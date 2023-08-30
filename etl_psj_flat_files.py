import os
import mjdb
import config
import common as cmn
import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_psj'
SCHEMA = 'psj'
LF = cmn.log_filer(LOGDIR,'flat_files')
FILES = (
    {
        'dir':'O:\\Shared\\Employee Benefits\\01 General Information\\Analytics\\Finance Reporting',
        'wb':'Monthly Source Tables.xlsx',
        'sheets':('Retention','Ledger Categories','Producer Divisions')
    },
    {
        'dir':'O:\\Shared\\Employee Benefits\\01 General Information\\Analytics\\Finance Reporting\\Producer Success Journal',
        'wb':'PSJ - New Written by Producer.xlsx',
        'sheets':('new_written',)
    }
)

def main():
    for f in FILES:
        for sheet in f['sheets']:
            table = sheet.lower().replace(' ','_')
            try:
                df = pd.read_excel(os.path.join(f['dir'],f['wb']),sheet)
            except Exceptiona as e:
                LF.error(f"unable to read {f['wb']} {sheet}:\n{e}")
            else:
                try:
                    # reformat column names
                    df.columns = ['_'.join(col.replace(' & ','').lower().split()) for col in df.columns]
                    # get unique key for destination, drop rows without required data
                    keys = [x[0] for x in mjdb.get_tfn('public','tfn_table_unique_key',params=(SCHEMA,table))]
                    df.dropna(subset=keys,inplace=True)
                    # get numeric column types, cast dataframe types accordingly (attribute type id 23 = int4)
                    ints = [x[0] for x in mjdb.get_tfn('public','tfn_table_column_type',params=(SCHEMA,table,23))]
                    for i in ints:
                        df[i] = pd.to_numeric(df[i],downcast='unsigned')
                    pass
                except Exception as e:
                    LF.error(f"unable to format column names for {f['wb']} {sheet}:\n{e}")
                else:
                    try:
                        rcs = df.to_sql(f"stg_{table}",ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        LF.error(f"unable to stage records for {sheet}:\n{e}")
                    else:
                        LF.info(f"{rcs} record(s) staged for {sheet}")
                        if rcs > 0:
                            try:
                                rcu = mjdb.upsert_stage(SCHEMA,table,'upsert')
                            except Exception as e:
                                LF.error(f"unable to upsert stage for {sheet}:\n{e}")
                            else:
                                LF.info(f"{rcu} record(s) affected for {sheet}")
                    finally:
                        mjdb.drop_table(SCHEMA,f"stg_{table}")

if __name__ == '__main__':
    main()