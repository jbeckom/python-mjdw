import os
import mjdb
import sgws
import config
import common as cmn
import pandas as pd
import datetime as dt
import sgHelpers as hlp
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR,'cp_blankets')

def cp_blankets_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','blanket_number','policy_agency_id','cause','coinsurance_pct','inflation_guard_pct','rate','total_blk_limit','total_blk_premium','blanket_type_cd','blanket_type_desc','valuation_cd','agree_amt_ind','ded_symbol','ded_amt','deductible_type_cd','deductible_basis_cd','off_dt','start_dt','second_amt_value_type','second_amt_value','end_dt'): 
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def detailed_rating_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('detailed_cause','coinsurance_pct','detailed_rate','detailed_ded_symbol','detailed_ded_amt','detailed_premium'): 
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    blankets = []
    detailedRatingInfo = []
    try:
        lastEntry = mjdb.sg_last_entry('cp_blankets')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('cp_blankets')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT CP.BLANKETS *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT CP.BLANKETS *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            blankets.append(cp_blankets_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"cp_blankets_row({sagitem},<<item>>)\n{e}")
                        else:
                            for drii in item.find_all('DetailedRatingInformationNInfo'):
                                if drii.get('lis'):
                                    try:
                                        lis = int(drii.get('lis'))
                                        detailedRatingInfo.append(detailed_rating_info_row(sagitem,lis,drii))
                                    except Exception as e:
                                        lf.error(f"detailed_rating_info_row({sagitem},{lis},<<drii>>)\n{e}")
        stages = {
            'cp_blankets':blankets if blankets else None,
            'cp_blankets_detailed_rating_information_info':detailedRatingInfo if detailedRatingInfo else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}\n{e}")
                else:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    if rcs > 0:
                        try:
                            rcu = mjdb.upsert_stage('sagitta',s, 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('sagitta',{s})\n{e}")
                        else:
                            lf.info(f"mjdb.upsert_stage('sagitta',{s}) affected {rcu} record(s)")
                finally:
                    mjdb.drop_table('sagitta', f'stg_{s}')

if __name__ == '__main__':
    main()