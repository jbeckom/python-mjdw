import mjdb
import sgws
import config 
import pandas as pd
import sgHelpers as hlp
import common as cmn
import datetime as dt
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
FILE = 'PREMIUMS'
SCHEMA = 'sagitta'
LF = cmn.log_filer(LOGDIR,FILE)
CONFIGS = [
    {'tag':'Item','table':'premiums','func':'premium_row','rows':[]},
    {'tag':'TransactionCdInfo','table':'premiums_transaction_cd_info','func':'transaction_cd_info_row','rows':[]}
]

def premium_row(sagitem,soup):
    row = {
        'sagitem':int(sagitem),
        'utm_id':soup.find('UTMId').text if soup.find('UTMId') else None
    }
    for i in ('audit_entry_dt','audit_time_ind'):
        tag = hlp.col_tag_transform(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','installment_plan','day','deposit_amt','down','fee_amt','fee_pct','payment_plan_remark_text','payables_id','not_posted','purge_dt','invoice_new_ren'):
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def transaction_cd_info_row(sagitem,soup):
    row = {
        'sagitem':int(sagitem),
        'lis':int(soup.get('lis')),
        'ar_id':soup.find('ARId').text if soup.find('ARId') else None
    }
    for t in ('transaction_cd','transaction_effective_dt','transaction_entry_dt','transaction_amt','agency_pct','agency_amt','producer_cd','producer_pct','producer_amt','annualize_dt','invoice_number','sales_id','previous_posted_ind','staff_cd','department_cd','bill_to_cd','payee_cd','coverage_cd','insurer_cd'):
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    premiumRows = []
    tciRows = []
    try:
        lastEntry = mjdb.sg_last_entry(FILE)
    except Exception as e:
        LF.error(f"unable to fetch Last Entry for {FILE}:\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT {FILE} *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            # batchesStatement = f"SELECT PREMIUMS *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE 1/1/2023 AND LAST.ENTRY.DATE LE 1/31/2023"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            LF.error(f"unable to fetch batches for {FILE}:\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {FILE} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    LF.error(f"unable to fetch batch {batch}:\n{e}")
                else:
                    for f in batchResponse.find_all('File'):
                        sagitem = f.find('Item').get('sagitem')
                        for cfg in CONFIGS:
                            for x in f.find_all(cfg['tag']):
                                try:
                                    cfg['rows'].append(eval(f"{cfg['func']}(sagitem,x)"))
                                except Exception as e:
                                    LF.error(f"unable to parse {cfg['func']} for {sagitem}:\n{e}")
            for cfg in CONFIGS:
                if len(cfg['rows']) > 0:
                    try:
                        rcs = pd.DataFrame(cfg['rows']).to_sql(f"stg_{cfg['table']}",ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        LF.error(f"unable to stage records for {cfg['table']}:\n{e}")
                    else:
                        LF.info(f"{rcs} record(s) staged for {cfg['table']}")
                        try:
                            rcu = mjdb.upsert_stage('sagitta',cfg['table'],'upsert')
                        except Exception as e:
                            LF.error(f"unable to upsert record(s) for {cfg['table']}:\n{e}")
                        else:
                            LF.info(f"{rcu} record(s) affected for {cfg['table']}")
                    finally:
                        mjdb.drop_table('sagitta',f"stg_{cfg['table']}")
                else:
                    LF.info(f"no records to staged for {cfg['table']}")

if __name__ == '__main__':
    main()