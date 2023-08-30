import mjdb
import sgws
import config
import sgHelpers as hlp
import common as cmn
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
SCHEMA = 'sagitta'
FILES = [
    {
        'file':'CHART.OF.ACCOUNTS.MASTER',
        ### NEED TO RESOLVE MJDW-350 BEFORE WE'RE ABLE TO PULL DELTAS ###
        # 'lastEntry':'LAST.ENTRY.DATE',
        'configs':[
            {'tag':'Item','table':'chart_of_accounts_master','func':'coam_row','rows':[]},
            {'tag':'AllocationInfo','table':'chart_of_accounts_master_allocation_info','func':'coam_ai_row','rows':[]}
        ]
    }   
]

def coam_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'gl_account_number':soup.find('GLAccountNumber').text if soup.find('GLAccountNumber') else None
    }
    for i in ['audit_entry_dt','audit_time']:
        tag = hlp.col_tag_transform(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in ['audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','description','normal_balance_amt','ledger_type_cd','period_end_dt','close_to_account','report_cd','base_pct','allocated_by']:
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    for f in FILES:
        lf = cmn.log_filer(LOGDIR,f['file'])
        batchesStatement = f"SELECT {f['file']} *BATCH*"
        if 'lastEntry' in f:
            try:
                lastEntry = mjdb.sg_last_entry(f['file'])
            except Exception as e:
                lf.error(f"unable to fetch last entry data:\n{e}")
            else:
                lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
                lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
                batchesStatement += f" *CRITERIA.BATCH* WITH {f['lastEntry']} GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}" if lastEntry else " *BATCH*"
        try:
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {f['file']} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"unable to retrieve batch {batch}:\n{e}")
                else:
                    for fi in batchResponse.find_all('File'):
                        sagitem = fi.find('Item').get('sagitem')
                        for cfg in f['configs']:
                            for x in fi.find_all(cfg['tag']):
                                try:
                                    cfg['rows'].append(eval(f"{cfg['func']}(sagitem,x)"))
                                except Exception as e:
                                    lf.error(f"unable to parse {cfg['func']} for {sagitem}:\n{e}")
            for cfg in f['configs']:
                if len(cfg['rows']) > 0:
                    try:
                        rcs = pd.DataFrame(cfg['rows']).to_sql(f"stg_{cfg['table']}",ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        lf.error(f"unable to stage records for {cfg['table']}\n{e}")
                    else:
                        lf.info(f"{rcs} record(s) stage for {cfg['table']}")
                        try:
                            rcu = mjdb.upsert_stage('sagitta',cfg['table'],'upsert')
                        except Exception as e:
                            lf.error(f"unable to upsert record(s) for {cfg['table']}")
                        else:
                            lf.info(f"{rcu} record(s) affected for {cfg['table']}")
                    finally:
                        mjdb.drop_table('sagitta',f"stg_{cfg['table']}")
                else:
                    lf.info(f"no records to staged for {cfg['table']}")

if __name__ == '__main__':
    main()