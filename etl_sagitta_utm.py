import mjdb
import sgws
import config 
import datetime as dt
import common as cmn
import sgHelpers as hlp
import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
FILE = 'utm'
SCHEMA = 'sagitta'
LF = cmn.log_filer(LOGDIR,FILE)
CONFIGS = [
    {'tag':'Item','table':'utm','func':'utm_row','rows':[]}
]

def utm_row(sagitem,soup):
    row = {'sagitem':sagitem}
    for c,t in {'posting_date':'a2','period_end_date':'a3'}.items():
        row[c] = int(soup.find(t).text) if soup.find(t) else None
    for c,t in {'source_code':'a1','gl_acct_no':'a4','debit_amount':'a5','credit_amount':'a6','div':'a7','document_ref':'a8','vendor_code':'a9','client_code':'a10','transfer_journal':'a11','transfer_date':'a12','audit_info':'a13','tracking_client':'a15','ins':'a16','tracking_vendor':'a17','emp':'a18','program':'a19'}.items():
        row[c] = soup.find(t).text if soup.find(t) else None
    return row

def main():
    try:
        lastPeriod = mjdb.sg_accounting_last_period('utm')
        # last (a3) returns int but resolves to string date internally (abstract from standard) -- string date, rather than int representation, must be passed to filter
        # default None date to 9/1/2020
        lastPeriod = dt.datetime.strftime((dt.date(1967,12,31) + dt.timedelta(days=lastPeriod)),'%m/%d/%Y') if lastPeriod else '08/01/2019'
    except Exception as e:
        LF.error(f"unable to fetch last period data:\n{e}")
    else:
        try:
            batchesStatement = f"SELECT {FILE.replace('_','.').upper()} *CRITERIA.BATCH* WITH 3 GT {lastPeriod}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            LF.error(f"unable to fetch batches:\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {FILE.replace('_','.').upper()} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    LF.error(f"unable to fetch response for batch {batch}:\n{e}")
                else:
                    for f in batchResponse.find_all('File'):
                        sagitem = int(f.find('Item').get('sagitem'))
                        for cfg in CONFIGS:
                            for x in f.find_all(cfg['tag']):
                                try:
                                    cfg['rows'].append(eval(f"{cfg['func']}(sagitem,x)"))
                                except Exception as e:
                                    LF.error(f"unable to parse {cfg['func']} for {sagitem}:\n{e}")
                pass
            for cfg in CONFIGS:
                if len(cfg['rows']) > 0:
                    try:
                        rcs = pd.DataFrame(cfg['rows']).to_sql(f"stg_{cfg['table']}",ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        LF.error(f"unable to stage records for {cfg['table']}\n{e}")
                    else:
                        LF.info(f"{rcs} record(s) staged for {cfg['table']}")
                        try:
                            rcu = mjdb.upsert_stage(SCHEMA,cfg['table'],'insert')
                        except Exception as e:
                            LF.error(f"unable to insert from stage:\n{e}")
                        else:
                            LF.info(f"{rcu} row(s) affected for {cfg['table']}")

if __name__ == '__main__':
    main()