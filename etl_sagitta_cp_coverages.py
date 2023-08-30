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

lf = cmn.log_filer(LOGDIR,'cp_coverages')

def cp_coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'lis_count_location_specific_coverage_info':soup.find('LISCountLocationSpecificCoverageInfo').text if soup.find('LISCountLocationSpecificCoverageInfo') else None,
        'lis_count_extension_excl_optional_info':soup.find('LISCountExtensionExclOptionalInfo').text if soup.find('LISCountExtensionExclOptionalInfo') else None,
        'lis_count_time_element_info':soup.find('LISCountTimeElementInfo').text if soup.find('LISCountTimeElementInfo') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','location_number','location_desc','off_dt','location_lower_level_coverage_slice'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def location_specific_coverage_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('building_number','subject_insurance_cd','subject_insurance_desc','insurance_amt','blanket_number','loss_cause_cd','coinsurance_pct','inflation_guard_pct','valuation_type_cd','agreed_value_id','deductible_symbol','deductible','deductible_type_cd','deductible_type_desc','deductible_basic_cd','premium_amt','building_desc','start_dt','end_dt','second_value_format_cd','second_value_amt','off_premises_power_dep_property_commercial_name','off_premises_power_dep_property_addr','off_premises_power_dep_property_addr_2','off_premises_power_dep_property_city','off_premises_power_dep_property_postal_code','off_premises_power_dep_property_postal_extension_code','off_premises_power_dep_property_county','off_premises_power_dep_property_state_prov_cd','off_premises_power_dep_property_country'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def extension_excl_optional_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('optional_coverages_cd','optional_limit','optional_deductible_ind','optional_deductible','optional_deductible_type_cd','optional_deductible_basis_cd','reporting_period_cd','optional_coverages_desc','optional_peak_season_start_dt','optional_peak_season_end_dt','option_form_number','option_edition_dt','optional_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def time_element_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('time_element_subject_insurance_cd','time_element_subject_insurance_desc','time_element_monthy_limit','time_element_num_days','time_element_floor_area_num_units','time_element_manufacturing_area_num_units','time_element_mercantile_area_num_units','time_element_option_cd','time_element_limit_on_loss_cd','time_element_payroll_cd','time_element_payroll_amt','time_element_num_extension_business_income_days','time_element_maximum_indemnity_period'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverages = []
    lsciList = []
    eeoiList = []
    teiList = []
    try:
        lastEntry = mjdb.sg_last_entry('cp_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('cp_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT CP.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT CP.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = item.get('sagitem')
                            coverages.append(cp_coverages_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"cp_blankets_row({sagitem},<<item>>)\n{e}")
                        else:
                            for lsci in item.find_all('LocationSpecificCoverageInfo'):
                                if lsci.get('lis'):
                                    try:
                                        lis = int(lsci.get('lis'))
                                        lsciList.append(location_specific_coverage_info_row(sagitem,lis,lsci))
                                    except Exception as e:
                                        lf.error(f"location_specific_coverage_info_row({sagitem},{lis},<<lsci>>)\n{e}")
                                    else:
                                        if lsci.find('ExtensionExclOptionalInfo'):
                                            try:
                                                eeoiList.append(extension_excl_optional_info_row(sagitem,lis,lsci.find('ExtensionExclOptionalInfo')))
                                            except Exception as e:
                                                lf.error(f"extension_excl_optional_info_row({sagitem},{lis},<<lsci.find('ExtensionExclOptionalInfo')>>)\n{e}")
                                        if lsci.find('TimeElementInfo'):
                                            try:
                                                teiList.append(time_element_info_row(sagitem,lis,lsci.find('TimeElementInfo')))
                                            except Exception as e:
                                                lf.error(f"time_element_info_row({sagitem},{lis},<<lsci.find('TimeElementInfo')>>)\n{e}")
    stages = {
        'cp_coverages':coverages if coverages else None,
        'cp_coverages_location_specific_coverage_info':lsciList if lsciList else None,
        'cp_coverages_lsci_extension_excl_optional_info':eeoiList if eeoiList else None,
        'cp_coverages_lsci_time_element_info':teiList if teiList else None
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