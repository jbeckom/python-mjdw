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

lf = cmn.log_filer(LOGDIR,'ba_coverages')

def coverage_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'count_doc_info':soup.find('CountDOCInfo').text if soup.find('CountDOCInfo') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','count_hired_info','count_comml_audit_addl_cov_info','total_premium','policy_agency_id','liability_deductible_amt','liability_premium_amt','property_damage_limit','property_damage_deductible_amt','property_damage_premium_amt','pip_limit','pip_deductible_amt','pip_premium_amt','apip_limit','apip_deductible_amt','apip_premium_amt','auto_medical_payments_deductible_amt','auto_medical_payments_premium_amt','underinsured_motorist_deductible_amt','underinsured_pd_limit','underinsured_deductible_pd_limit','underinsured_premium_pd_amt','towing_and_labor_limit','towing_and_labor_deductible_amt','towing_and_labor_premium_amt','comprehensive_limit','comprehensive_deductible_amt','comprehensive_premium_amt','specified_perils_limit','specified_perils_deductible_amt','specified_perils_premium_amt','collision_limit','collision_deductible_amt','collision_premium_amt','combined_physical_damage_limit','combined_physical_damage_deductible_amt','combined_physical_damage_premium_amt','off_dt','hired_physical_damage_cost_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None    
    others = {
        'obsolete_tag_*':2,
        'liability_symbol_*_cd':5,
        'liability_*_limit':2,
        'pip_no_fault_symbol_*_cd':3,
        'apip_symbol_*_cd':2,
        'auto_medical_payments_symbol_*_cd':3,
        'uninsured_motorist_symbol_*_cd':3,
        'underinsured_motorist_*_limit':2,
        'towing_and_labor_symbol_*_cd':3,
        'comprehensive_symbol_*_cd':4,
        'specified_perils_symbol_*_cd':4,
        'collision_symbol_*_cd':4,
        'combined_phsycial_damage_symbol_*_cd':4
    }
    for o in others:
        for n in range(others[o]):
            tag = ''.join([x.capitalize() for x in o.split('_')]).replace('*',str(n+1))
            row[o.replace('*',str(n+1))] = soup.find(tag).text if soup.find(tag) else None
    return row

def hired_info_row(sagitem, lis, soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for t in ('hired_liability_class_cd','hired_location_agency_id','hired_state_prov_cd','hired_liability_cost_amt','hired_liability_rate','hired_physical_damage_rate','hired_liability_minimum_yes_no_cd','hired_num_days','hired_num_vehs','hired_comprehensive_deductible_amt','hired_specified_perils_deductible_amt','hired_collision_deductible_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def non_owned_info_row(sagitem, lis, soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for t in ('non_owned_class_cd','non_owned_location_agency_id','non_owned_state_prov_cd','non_owned_group_type_cd','non_owned_num','non_owned_pct','non_owned_social_service_agency_yes_no_cd','non_owned_individual_liabilityfor_employees_yes_no_cd'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def doc_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for t in ('doc_class_cd','doc_location_agency_id','doc_state_prov_cd','doc_territory_cd','doc_num_employees','doc_num_individuals_covered','doc_fin_resp_doc_cert_yes_no_cd'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def comml_auto_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('comml_auto_addl_cov_coverage_cd','comml_auto_addl_cov_addl_coverage_desc','comml_auto_addl_cov_form_number','comml_auto_addl_cov_edition_dt','comml_auto_addl_cov_state_prov_cd','comml_auto_addl_cov_limit_1','comml_auto_addl_cov_limit_2','comml_auto_addl_cov_deductible_amt_1','comml_auto_addl_cov_deductible_amt_2','comml_auto_addl_cov_deductible_typ','comml_auto_addl_cov_credit_pct','comml_auto_addl_cov_addl_cov_rate_factor','comml_auto_addl_cov_addl_cov_coverage_premium_amt','comml_auto_addl_cov_veh_1','comml_auto_addl_cov_veh_2','comml_auto_addl_cov_veh_3','comml_auto_addl_cov_veh_4','comml_auto_addl_cov_veh_5','comml_auto_addl_cov_veh_6','comml_auto_addl_cov_veh_7','comml_auto_addl_cov_buyback_yes_no_cd','comml_auto_addl_cov_misc_options_1','comml_auto_addl_cov_misc_options_2','comml_auto_addl_cov_options_1','comml_auto_addl_cov_options_2','comml_auto_addl_cov_options_3','comml_auto_addl_cov_options_4','comml_auto_addl_cov_benefits_1','comml_auto_addl_cov_benefits_2','comml_auto_addl_cov_benefits_3','comml_auto_addl_cov_class_cd','comml_auto_addl_cov_hired_non_owned_doc','comml_auto_addl_cov_misc_factor'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverageList = []
    hiList = []
    noList = []
    docList = []
    caList = []
    try:
        lastEntry = mjdb.sg_last_entry('ba_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('ba_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT BA.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT BA.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = item.get('sagitem')
                            coverageList.append(coverage_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"coverageList.append(coverages_row({sagitem},<<item>>))\n{e}")
                        else:
                            for hi in item.find_all('HiredInfo'):
                                if hi.get('lis') and len(hi.find_all()) > 0:
                                    try:
                                        lis = int(hi.get('lis'))
                                        hiList.append(hired_info_row(sagitem, lis, hi))
                                    except Exception as e:
                                        lf.error(f"hiList.append(hired_info_row({sagitem}, {lis}, <<hi>>))\n{e}")
                            for no in item.find_all('NonOwnedInfo'):
                                if no.get('lis') and len(no.find_all()) > 0:
                                    try:
                                        lis = int(no.get('lis'))
                                        noList.append(non_owned_info_row(sagitem,lis,no))
                                    except Exception as e:
                                        lf.error(f"noList.append(non_owned_info_row({sagitem},{lis},<<no>>))\n{e}")
                            for doc in item.find_all('DOCInfo'):
                                if doc.get('lis') and len(doc.find_all()) > 0:
                                    try:
                                        lis = int(doc.get('lis'))
                                        docList.append(doc_info_row(sagitem,lis,doc))
                                    except Exception as e:
                                        lf.error(f"docList.append(doc_info_row({sagitem},{lis},<<doc>>))\n{e}")
                            for ca in item.find_all('CommlAutoAddlCovInfo'):
                                if ca.get('lis') and len(ca.find_all()) > 0:
                                    try:
                                        lis = int(ca.get('lis'))
                                        caList.append(comml_auto_info_row(sagitem, lis, ca))
                                    except Exception as e:
                                        lf.error(f"caList.append(comml_auto_info_row({sagitem}, {lis}, <<ca>>))\n{e}")
        # stage rows
        stages = {
            'ba_coverages':coverageList if coverageList else None,
            'ba_coverages_hired_info':hiList if hiList else None,
            'ba_coverages_non_owned_info':noList if noList else None,
            'ba_coverages_doc_info':docList if docList else None,
            'ba_coverages_comml_auto_addl_cov_info':caList if caList else None
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
                            rcu = mjdb.upsert_stage('sagitta', s, 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('sagitta', {s})\n{e}")
                        else:
                            lf.info(f"mjdb.upsert_stage('sagitta', {s}) affected {rcu} record(s)")
                finally:
                    mjdb.drop_table('sagitta',f'stg_{s}')
                    
if __name__ == '__main__':
    main()