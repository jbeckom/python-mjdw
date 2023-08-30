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
FILE = 'gl_coverages'

lf = cmn.log_filer(LOGDIR,FILE)

def coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','general_liab_ind','claims_made_ind','claims_occurrence_ind','other_coverage_premium','per_claim_ind','per_occurrence_ind','ded_basis_cd','deductible','gen_aggregate','product_completed_ops_info','pers_advertising_injury','each_occurrence','fire_legal','medical_expense','premises_operation','products_premium_amt','total_premium_amt','off_dt','total_other_cov_premium_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def additional_coverage_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis,
        'gl_coverage_cd':soup.find('GLCoverageCd').text if soup.find('GLCoverageCd') else None
    }
    for t in ('hazard_number','form_number','edition_dt','cov_1_limit','cov_2_limit','deductible','cov_1_type_deductible','cov_1_basis_1_deductible','cov_1_basis_2_deductible','cov_rate','premium_amt','job_number','num_one','num_two','type_dt','dt','coverage_desc','coverage_2_deductible','coverage_2_ded_type','cov_2_basis_1_deductible','cov_basis_2_deductible','location_agency_id','state_prov_cd','line_1_remark_text','line_2_remark_text','building_num'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverages = []
    additionalCoverageInfo = []

    try:
        lastEntry = mjdb.sg_last_entry(FILE)
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry({FILE})\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT {FILE.replace('_','.').upper()} *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {FILE.replace('_','.').upper()} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            coverages.append(coverages_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"coverages_row({sagitem},<<item>>)\n{e}")
                        else:
                            try:
                                [additionalCoverageInfo.append(additional_coverage_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('AdditionalCoverageInfo') if (x.get('lis') and len(x.find_all()) > 0)]
                            except Exception as e:
                                lf.error(f"unable to parse AdditionalCoverageInfo for {sagitem}:\n{e}")
        stages = {
            'gl_coverages':coverages if coverages else None,
            'gl_coverages_additional_coverage_info':additionalCoverageInfo if additionalCoverageInfo else None
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
            else:
                lf.info(f"no records to stage for {s}")

if __name__ == '__main__':
    main()