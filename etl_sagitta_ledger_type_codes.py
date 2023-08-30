import mjdb
import sgws
import config
import common as cmn
import datetime as dt
import sgHelpers as hlp
import pandas as pd
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
FILE = 'LEDGER.TYPE.CODES'
LOGDIR = 'etl_sagitta'
LF = cmn.log_filer(LOGDIR,FILE)

def ledger_type_code_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('s2').text) if soup.find('s2') else None,
        'audit_time':int(soup.find('s3').text) if soup.find('s3') else None
    }
    for t,c in {'a1':'description','s1':'audit_staff_cd','s4':'audit_cd','s5':'audit_history_record_number','s6':'audit_program'}.items():
        row[c] = soup.find(t).text if soup.find(t) else None
    return row

def main():
    ltcRows = []
    try:
        response = sgws.post_ptr_access_statement(f"SELECT {FILE}")
    except Exception as e:
        LF.error(f"unable to fetch data:\n{e}")
    else:
        for item in response.find_all('Item'):
            sagitem = item.get('sagitem')
            ltcRows.append(ledger_type_code_row(sagitem,item))
    if len(ltcRows) > 0:
        try:
            rcs = pd.DataFrame(ltcRows).to_sql('stg_ledger_type_codes',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            LF.error(f"unable to stage records:]n{e}")
        else:
            LF.info(f"{rcs} record(s) staged for ledger_type_codes")
            if rcs > 0:
                try:
                    rcu = mjdb.upsert_stage('sagitta','ledger_type_codes','upsert')
                except Exception as e:
                    LF.error(f"unable to upsert stage:\n{e}")
                else:
                    LF.info(f"{rcu} record(s) affected for ledger_type_codes")
        finally:
            mjdb.drop_table('sagitta','stg_ledger_type_codes')
    else:
        LF.info("no records to stage for ledger_type_codes")


if __name__ == '__main__':
    main()