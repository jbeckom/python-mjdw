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

lf = cmn.log_filer(LOGDIR,'coverages')

def coverages_row(sagitem, soup):
    row ={'sagitem':sagitem}
    ints = ('audit_entry_dt','audit_time','off_dt')
    texts = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','coverage_cd','coverage_description','major_lob_cd','acord_lob_cd','acord_sub_lob','coverage_type','personal_commercial','off_dt_remarks')
    for i in ints:
        tag = ''.join([x.capitalize() for x in i.split('_')])
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in texts:
        tag = ''.join([x.capitalize().replace('Lob','LOB') for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    coverages = []
    try:
        lastEntry = mjdb.sg_last_entry('coverages')
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
    except Exception as e:
        lf.error(f"unable to retrieve last entry data\n{e}")
    else:
        try:
            xmlResponse = sgws.post_ptr_access_statement(f"SELECT COVERAGES *CRITERIA* WITH PAX.AUDIT.DATE GE {dt.datetime.strftime(lastEntryDate,'%m-%d-%Y')}")
        except Exception as e:
            lf.error(f"unable to retrieve xml response\n{e}")
        else:
            for item in xmlResponse.find_all('Item'):
                try:
                    sagitem = int(item.get('sagitem'))
                    coverages.append(coverages_row(sagitem, item))
                except Exception as e:
                    lf.error(f"coverages_row({sagitem}, <<item>>)\n{e}")
            if coverages:
                try:
                    rcs = pd.DataFrame(coverages).to_sql('stg_coverages',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for coverages")
                else:
                    if rcs > 0:
                        lf.info(f"{rcs} record(s) staged for coverages")
                        try:
                            rcu = mjdb.upsert_stage('sagitta', 'coverages', 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('sagitta', 'coverages')\n{e}")
                        else:
                            lf.info(f"mjdb.upsert_stage('sagitta', 'coverages') affected {rcu} record(s)")
                finally:
                    mjdb.drop_table('sagitta', 'stg_coverages')

if __name__ == '__main__':
    main()