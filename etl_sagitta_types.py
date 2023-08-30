import mjdb
import sgws
import config
import common as cmn
import pandas as pd
import datetime as dt
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR,'types')

def types_row(sagitem, soup):
    row = {'sagitem':sagitem}
    ints = {'audit_entry_dt':'s2','audit_time':'s3'}
    texts = {'audit_staff_cd':'s1','audit_cd':'s4','audit_history_record_number':'s5','audit_program':'s6','type_description':'a3','date_off':'a4','date_off_remarks':'a5'}
    for i in ints:
        row[i] = int(soup.find(ints[i]).text) if soup.find(ints[i]) else None
    for t in texts:
        row[t] = soup.find(texts[t]).text if soup.find(texts[t]) else None
    return row

def main():
    types = []
    try:
        typesResponse = sgws.post_ptr_access_statement('SELECT TYPES')
    except Exception as e:
        lf.error(f"sgws.post_ptr_access_statement('SELECT TYPES')\n{e}")
    else:
        for item in typesResponse.find_all('Item'):
            try:
                sagitem = item.get('sagitem')
                types.append(types_row(sagitem, item))
            except Exception as e:
                lf.error(f"types_row({sagitem}, <<item>>)")
        try:
            rcs = pd.DataFrame(types).to_sql('stg_types',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for types")
        else:
            if rcs > 0:
                lf.info(f"{rcs} record(s) staged for types")
                try:
                    rcu = mjdb.upsert_stage('sagitta', 'types','upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('sagitta', 'types','upsert')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('sagitta', 'types','upsert') affected {rcu} record(s)")
        finally:
            mjdb.drop_table('sagitta', 'stg_types')

if __name__ == '__main__':
    main()