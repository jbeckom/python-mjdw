import sgws
import mjdb
import config 
import pandas as pd
import common as cmn
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
SCHEMA = 'sagitta'
FILE = 'CHART.OF.ACCOUNTS.DETAIL'
LF = cmn.log_filer(LOGDIR,FILE)

def coad_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':int(lis)}
    for t in ('journal_id','debit_balance','credit_balance','memo_amt'):
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    coad_rows = []
    try:
        batchesResponse = sgws.post_ptr_access_statement(f"SELECT {FILE} *BATCH*")
    except Exception as e:
        LF.error(f"unable to fetch batches:\n{e}")
    else:
        for batch in hlp.parse_batch_items(batchesResponse):
            batchStatement = f"SELECT {FILE} *GET.BATCH* {batch}"
            try:
                batchResponse = sgws.post_ptr_access_statement(batchStatement)
            except Exception as e:
                LF.error(f"unable to fetch batch {batch}:\n{e}")
            else:
                for f in batchResponse.find_all('File'):
                    sagitem = f.find('Item').get('sagitem')
                    for coad in f.find_all('ChartOfAccountsDetailInfo'):
                        lis = coad.get('lis')
                        if lis is not None:
                            try:
                                coad_rows.append(coad_row(sagitem,lis,coad))
                            except Exception as e:
                                LF.error(f"unable to parse coad_row for {sagitem}-{lis}:\n{e}")
        if len(coad_rows) > 0:
            try:
                rcs = pd.DataFrame(coad_rows).to_sql('stg_chart_of_accounts_detail',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
            except Exception as e:
                LF.error(f"unable to stage records:\n{e}")
            else:
                LF.info(f"{rcs} record(s) staged for chart_of_accounts_detail")
                if rcs > 0:
                    try:
                        rcu = mjdb.upsert_stage(SCHEMA,'chart_of_accounts_detail','upsert')
                    except Exception as e:
                        LF.error(f"unable to upsert from stage:\n{e}")
                    else:
                        LF.info(f"{rcu} record(s) affected for chart_of_accounts_detail")
        else:
            LF.info(f"no records to stage")

if __name__ == '__main__':
    main()