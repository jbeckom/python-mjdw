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

lf = cmn.log_filer(LOGDIR, 'staff_addlinfo')

def staff_addlinfo_row(sagitem, soup):
    intCols = {'audit_entry_dt':'s2','audit_time':'s3'}
    textCols = {
        'audit_staff_cd':'s1',
        'audit_cd':'s4',
        'audit_history_record_number':'s5',
        'audit_program':'s6',
        'only_staff_name':'a6',
        'associate_role':'a7'
    }
    row = {'sagitem':sagitem}
    for iCol in intCols:
        row[iCol] = int(soup.find(intCols[iCol]).text) if soup.find(intCols[iCol]) else None
    for tCol in textCols:
        row[tCol] = soup.find(textCols[tCol]).text if soup.find(textCols[tCol]) else None
    return row

def main():
    staffAddlinfoRows = []
    try:
        # parse response for individual items
        for item in sgws.post_ptr_access_statement('SELECT STAFF.ADDLINFO').find_all('Item'):
            # parse item into dictionary, append to list
            try:
                sagitem = item.get('sagitem')
                staffAddlinfoRows.append(staff_addlinfo_row(sagitem,item))
            except Exception as e:
                lf.error(f"unable to parse item {sagitem}\n{e}")
    except Exception as e:
        lf.error(f"unable to parse access statement\n{e}")
    else:
        try:
            # convert row list to dataframe, stage in database
            rcs = pd.DataFrame(staffAddlinfoRows).to_sql('stg_staff_addlinfo', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
        except Exception as e:
            lf.error(f"unable to stage dataframe\n{e}")
        else:
            if rcs > 0:
                lf.info(f"{rcs} row(s) staged for staff_addlinfo")
                try:
                    rcu = mjdb.upsert_stage('sagitta', 'staff_addlinfo', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('sagitta', 'staff_addlinfo')\n{e}")
                else:
                    mjdb.drop_table('sagitta', 'stg_staff_addlinfo')
                    lf.info(f"mjdb.upsert_stage('sagitta', 'staff_addlinfo') affected {rcu} row(s)")

if __name__ == '__main__':
    main()