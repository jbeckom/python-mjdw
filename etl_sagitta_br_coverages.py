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

lf = cmn.log_filer(LOGDIR,'br_coverages')

def coverage_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','builders_risk_yes_no_cd','installation_yes_no_cd','open_reporting_ind','job_specified_ind','completed_value','job_specific_location_limit','job_specific_temporary_limit','job_specific_transit_limit','job_specific_max_paid','first_any_one_location_limit','first_any_one_location_construction','second_any_one_location_limit','second_any_one_location_construction','per_disaster_limit','temporary_location_limit','transit_limit','addl_cov_ind','off_dt','reporting_annual_premium_amt','reporting_adj_period','reporting_deposit_amt','reporting_period'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None  
    return row

def completed_value_location_limit_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('completed_value_location_number','completed_value_sub_location_number','completed_value_site_limit'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None  
    return row

def loss_cause_cd_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('loss_cause','loss_cause_sub_limit','loss_cause_deductible_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None  
    return row

def additional_coverage_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('addl_cov_cd','first_addl_cov_limit','first_addl_cov_deductible_amt','second_addl_cov_limit','second_addl_cov_deductible_amt','addl_cov_desc','addl_cov_rate','addl_cov_endorse_form','addl_cov_endorse_form_date','addl_cov_premium_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    others = {
        'first_addl_cov_ded_desc':'FirstAddLCovDedDesc',
        'second_addl_cov_ded_desc':'SecondAddLCovDedDesc',
        'addl_cov_addl_info':'AddlCovAddLInfo'
    }
    for o in others:
        row[o] = soup.find(others[o]).text if soup.find(others[o]) else None
    return row

def main():
    coverageList = [] 
    completedValueLocationLimitInfo = [] 
    lossCauseCdInfo = [] 
    additionalCoverageInfo = [] 
    try:
        lastEntry = mjdb.sg_last_entry('br_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('br_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT BR.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT BR.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            coverageList.append(coverage_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"coverageList.append(coverages_row({sagitem},<<item>>))\n{e}")
                        else:
                            try:
                                for cvlli in item.find_all('CompletedValueLocationLimitInfo'):
                                    if cvlli.get('lis'):
                                        lis = int(cvlli.get('lis'))
                                        completedValueLocationLimitInfo.append(completed_value_location_limit_info_row(sagitem,lis,cvlli))
                            except Exception as e:
                                lf.error(f"completedValueLocationLimitInfo.append(completed_value_location_limit_info_row({sagitem},{lis},<<cvlli>>))\n{e}")
                            try:
                                for lcci in item.find_all('LossCauseCdInfo'):
                                    if lcci.get('lis'):
                                        lis = int(lcci.get('lis'))
                                        lossCauseCdInfo.append(loss_cause_cd_info_row(sagitem,lis,lcci))
                            except Exception as e:
                                lf.error(f"lossCauseCdInfo.append(loss_cause_cd_info_row({sagitem},{lis},<<lcci>>))\n{e}")
                            try:
                                for aci in item.find_all('AdditionalCoverageInfo'):
                                    if aci.get('lis') and len(aci.find_all()) > 0:
                                        lis = int(aci.get('lis'))
                                        additionalCoverageInfo.append(additional_coverage_info_row(sagitem,lis,aci))
                            except Exception as e:
                                lf.error(f"additionalCoverageInfo.append(additional_coverage_info_row({sagitem},{lis},<<aci>>))\n{e}")
    stages = {
        'br_coverages':coverageList if coverageList else None,
        'br_coverages_additional_coverage_info':additionalCoverageInfo if additionalCoverageInfo else None,
        'br_coverages_completed_value_location_limit_info':completedValueLocationLimitInfo if completedValueLocationLimitInfo else None,
        'br_coverages_loss_cause_cd_info':lossCauseCdInfo if lossCauseCdInfo else None
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