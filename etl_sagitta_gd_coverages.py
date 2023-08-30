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

lf = cmn.log_filer(LOGDIR,'gd_coverages')

def coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'auto_only_liability_bi_limit':soup.find('AutoOnlyLiabilityBILimit').text if soup.find('AutoOnlyLiabilityBILimit') else None,
        'auto_only_liability_csl_limit':soup.find('AutoOnlyLiabilityCSLLimit').text if soup.find('AutoOnlyLiabilityCSLLimit') else None,
        'other_than_auto_only_liability_bi_limit':soup.find('OtherThanAutoOnlyLiabilityBILimit').text if soup.find('OtherThanAutoOnlyLiabilityBILimit') else None
    }
    others = {
        'pip_no_fault_symbol_1_cd':'PIPNoFaultSymbol1Cd',
        'pip_no_fault_symbol_2_cd':'PIPNoFaultSymbol2Cd',
        'pip_no_fault_symbol_3_cd':'PIPNoFaultSymbol3Cd',
        'pip_no_fault_deductible_amt':'PIPNoFaultDeductibleAmt',
        'pip_no_fault_limit':'PIPNoFaultLimit',
        'pip_no_fault_premium_amt':'PIPNoFaultPremiumAmt',
        'apip_symbol_1_cd':'APIPSymbol1Cd',
        'apip_symbol_2_cd':'APIPSymbol2Cd',
        'apip_symbol_3_cd':'APIPSymbol3Cd',
        'apip_limit':'APIPLimit',
        'apip_deductible_amt':'APIPDeductibleAmt',
        'apip_premium_amt':'APIPPremiumAmt',
        'uninsured_motorist_liability_bi_limit':'UninsuredMotoristLiabilityBILimit',
        'uninsured_motorist_liability_csl_limit':'UninsuredMotoristLiabilityCSLLimit',
        'uninsured_motorist_liability_pd_limit':'UninsuredMotoristLiabilityPDLimit',
        'underinsured_motoist_pdt_limit':'UnderinsuredMotoistPDtLimit'
    }
    for o in others:
        row[o] = soup.find(others[o]).text if soup.find(others[o]) else None
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_seq_agency_id','liability_symbol_1_cd','liability_symbol_2_cd','liability_symbol_3_cd','liability_symbol_4_cd','liability_symbol_5_cd','auto_only_liability_pd_limit','other_than_auto_only_liability_limit','other_than_auto_only_liability_pd_limit','aggregate_other_than_auto_only_liability_limit','liability_deductible_amt','limited_liability_ind','unlimited_liability_ind','auto_only_premium_1_amt','auto_only_premium_2_amt','otherthan_auto_premium_1_amt','otherthan_auto_premium_2_amt','aggregate_other_than_auto_premium_1_amt','aggregate_other_than_auto_premium_2_amt','medical_payments_symbol_1_cd','medical_payments_symbol_2_cd','medical_payments_symbol_3_cd','medical_payments_limit','medical_payments_auto_ind','medical_payments_premises_operations_ind','medical_payments_deductible_amt','medical_payments_premium_1_amt','medical_payments_premium_2_amt','uninsured_motorist_symbol_1_cd','uninsured_motorist_symbol_2_cd','uninsured_motorist_symbol_3_cd','uninsured_motorist_deductible_amt','uninsured_motorist_premium_1_amt','uninsured_motorist_premium_2_amt','underinsured_motorist_symbol_1_cd','underinsured_motorist_symbol_2_cd','underinsured_motorist_symbol_3_cd','underinsured_motoist_per_person_limit','underinsured_motoist_eacj_accident_limit','underinsured_motorist_deductible_amt','underinsured_motorist_premium_1_amt','underinsured_motorist_premium_2_amt','off_dt','comprehensive_cov_ind','specified_perils_cov_ind','comprehensive_symbol_1_cd','comprehensive_symbol_2_cd','comprehensive_symbol_3_cd','comprehensive_symbol_4_cd','physical_damage_collision_symbol_1_cd','physical_damage_collision_symbol_2_cd','physical_damage_collision_symbol_3_cd','physical_damage_collision_symbol_4_cd','physical_damage_collision_limit','physical_damage_collision_deductible_amt','physical_damage_collision_premium_amt','gargage_keepers_legal_liability_ind','garagae_keepers_direct_basis_ind','gargage_keepers_primary_ind','gargage_keepers_excess_ind','garagekeeps_other_than_collision_comprehensive_ind','garagekeeps_other_than_collision_specified_perils_ind','garagekeeps_other_than_collision_symbol_cd','garagekeeps_collision_symbol_cd','towing_labor_symbol_1_cd','towing_labor_symbol_2_cd','towing_labor_symbol_3_cd','towing_labor_symbol_4_cd','towing_labor_limit','towing_labor_deductible_amt','towing_labor_premium_amt','combined_physical_damage_symbol_1_cd','combined_physical_damage_symbol_2_cd','combined_physical_damage_symbol_3_cd','combined_phyiscal_damage_symbol_4_cd','combined_phyisical_damage_limit','combined_phyisical_damage_deductible_amt','combined_phyisical_damage_premium_amt','reporting_types','physical_damage_reporting_period_cd','non_reporting_ind','temporary_location_limit','transit_limit','total_premium'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def plate_hoists_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('plates_hoists_location_number','num_dealer_plates','num_repairer_plates','num_transportation_plates','num_hoists'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def physical_damage_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('physical_damage_location_number','building_limit','standard_open_lot_limit','non_standard_open_lot_limit','miscellaneous_type_desc','miscellaneous_limit','total_limit','per_auto_deductible_amt','max_per_loss_deductible_amt','fire_coverage_ind','fire_theft_coverage_ind','fire_thef_wind_coverage_ind','limited_perils_coverage_ind','physical_damage_premium_amt','physical_damage_scheduled_noscheduled_ind'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def garage_keepers_otc_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('garagekeepers_other_than_collisions_loc_number','garagekeeps_other_than_collision_limit','garagekeeps_other_than_collision_deductible_amt','garagekeeps_other_than_collision_max_per_loss_deductible_amt','garagekeeps_other_than_collision_auto_num','garagekeeps_other_than_collision_premium_amt','garagekeeps_other_than_collision_fire_ind','garagekeeps_other_than_collision_fire_theft_ind','garagekeeps_other_than_collision_fire_theft_wind_ind','garagekeeps_other_than_collision_limited_perils_ind'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def additional_coverage_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('addl_coverage_cd', 'additional_state_coverage_info', 'state_prov_cd', 'buyback_yes_no_cd', 'addl_cov_form_num', 'addl_edition_dt', 'addl_coverage_1_limit', 'addl_coverage_2_limit', 'addl_deductible_1_amt', 'addl_deductible_2_amt', 'addl_deductible_factor', 'addl_cov_deductible_type', 'addl_cov_deductible_credit_pct', 'additional_coverage_info', 'addl_cov_1_vehicle_num', 'addl_cov_2_vehicle_num', 'addl_cov_3_vehicle_num', 'addl_cov_4_vehicle_num', 'addl_cov_5_vehicle_num', 'addl_cov_6_vehicle_num', 'addl_cov_7_vehicle_num', 'miscellaneous_options_info', 'miscellaneous_options_1_cd', 'miscellaneous_options_2_cd', 'addlcov_option_info', 'addl_cov_option_1_cd', 'addl_cov_option_2_cd', 'addl_cov_option_3_cd', 'addl_cov_option_4_cd', 'addl_cov_benefits_info', 'addl_cov_benefits_1_cd', 'addl_cov_benefits_2_cd', 'addl_cov_benefits_3_cd', 'addl_cov_rate_factor', 'addl_cov_premium_amt', 'addl_coverage_cd_desc'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def hired_borrowed_coverage_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('hired_liability_class_cd','hired_liability_location_number','hired_liability_state_prov_cd','hired_liability_cost_amt','hired_liability_rate','hired_physical_damage_rate','if_any_rating_basis_ind','num_days','num_vehs','hired_comprehensive_deductible_amt','hired_specified_perils_deductible_amt','hired_collision_deductible_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def non_owned_coverage_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('non_owned_class_cd', 'non_owned_location_number', 'non_owned_state_prov_cd', 'non_owned_group_type_cd', 'num_non_owned', 'non_owned_pct', 'social_service_agency_ind', 'non_owned_individual_yes_no_cd'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def doc_coverage_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    others = {
        'doc_class_cd':'DOCClassCd',
        'doc_location_number':'DOCLocationNumber', 
        'fin_resp_doc_cert_yes_no_cd':'FinRespDOCCertYesNoCd', 
        'doc_driver_info':'DOCDriverInfo' 
    }
    for o in others:
        row[o] = soup.find(others[o]).text if soup.find(others[o]) else None
    for t in ('state_prov_cd', 'rating_territory_desc', 'num_employees', 'num_individuals_covered', 'driver_1_number', 'driver_2_number', 'driver_3_number', 'driver_4_number', 'driver_5_number', 'driver_6_number', 'driver_7_number', 'driver_8_number', 'driver_9_number'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def garage_keepers_collision_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('garage_keepers_collision_location_number', 'garagekeeps_collision_limit', 'garagekeepers_collision_deductible_amt', 'garage_keepers_collision_num_autos', 'garage_keepers_collision_premium_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverages = [] 
    plateHoistsInfo = []
    physicalDamageInfo = [] 
    garageKeepersOTCInfo = [] 
    additionalCoverageInfo = [] 
    hiredBorrowedCoverageInfo = [] 
    nonOwnedCoverageInfo = [] 
    docCoverageInfo = []
    garageKeepersCollisionInfo = [] 
    try:
        lastEntry = mjdb.sg_last_entry('gd_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('gd_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT GD.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT GD.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        # if item.get('sagitem').isnumeric():
                        try:
                            sagitem = int(item.get('sagitem'))
                            coverages.append(coverages_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"coverages_row({sagitem},<<item>>)\n{e}")
                        else:
                            for phi in item.find_all('PlateHoistsInfo'):
                                if phi.get('lis'):
                                    try:
                                        plateHoistsInfo.append(plate_hoists_info_row(sagitem,int(phi.get('lis')),phi))
                                    except Exception as e:
                                        lf.error(f"plate_hoists_info_row({sagitem},{int(phi.get('lis'))},<<phi>>)\n{e}")
                            for pdi in item.find_all('PhysicalDamageInfo'):
                                if pdi.get('lis'):
                                    try:
                                        physicalDamageInfo.append(physical_damage_info_row(sagitem,int(pdi.get('lis')),pdi))
                                    except Exception as e:
                                        lf.error(f"physical_damage_info_row({sagitem},{int(pdi.get('lis'))},<<pdi>>)\n{e}")
                            for gkotci in item.find_all('GarageKeepersOTCInfo'):
                                if gkotci.get('lis') and len(gkotci.find_all()) > 0:
                                    try:
                                        garageKeepersOTCInfo.append(garage_keepers_otc_info_row(sagitem,int(gkotci.get('lis')),gkotci))
                                    except Exception as e:
                                        lf.effor(f"garage_keepers_otc_info_row({sagitem},{int(gkotci.get('lis'))},<<gkotci>>)\n{e}")
                            for aci in item.find_all('AdditionalCoverageInfo'):
                                if aci.get('lis'):
                                    try:
                                        additionalCoverageInfo.append(additional_coverage_row(sagitem,int(aci.get('lis')),aci))
                                    except Exception as e:
                                        lf.error(f"additional_coverage_row({sagitem},{int(aci.get('lis'))},<<aci>>)\n{e}")
                            for hbci in item.find_all('HiredBorrowedCoverageInfo'):
                                if hbci.get('lis') and len(hbci.find_all()) > 0:
                                    try:
                                        hiredBorrowedCoverageInfo.append(hired_borrowed_coverage_info_row(sagitem,int(hbci.get('lis')),hbci))
                                    except Exception as e:
                                        lf.error(f"hired_borrowed_coverage_info_row({sagitem},{int(hbci.get('lis'))},<<hbci>>)\n{e}")
                            for noci in item.find_all('NonOwnedCoverageNInfo'):
                                if noci.get('lis'):
                                    try:
                                        nonOwnedCoverageInfo.append(non_owned_coverage_info_row(sagitem,int(noci.get('lis')),noci))
                                    except Exception as e:
                                        lf.error(f"non_owned_coverage_info_row({sagitem},{int(noci.get('lis'))},<<noci>>)\n{e}")
                            for dci in item.find_all('DOCCoverageInfo'):
                                if dci.get('lis'):
                                    try:
                                        docCoverageInfo.append(doc_coverage_info_row(sagitem,int(dci.get('lis')),dci))
                                    except Exception as e:
                                        lf.error(f"doc_coverage_info_row({sagitem},{int(dci.get('lis'))},<<dci>>)\n{r}")
                            for gkci in item.find_all('GarageKeepersCollisionInfo'):
                                if gkci.get('lis'):
                                    try:
                                        garageKeepersCollisionInfo.append(garage_keepers_collision_info_row(sagitem,int(gkci.get('lis')),gkci))
                                    except Exception as e:
                                        lf.error(f"garage_keepers_collision_info_row({sagitem},{int(gkci.get('lis'))},<<gkci>>)\n{e}")

        stages = {
            'gd_coverages':coverages if coverages else None,
            'gd_coverages_additional_coverage_info':additionalCoverageInfo if additionalCoverageInfo else None,
            'gd_coverages_doc_coverage_info':docCoverageInfo if docCoverageInfo else None,
            'gd_coverages_garage_keepers_collision_info':garageKeepersCollisionInfo if garageKeepersCollisionInfo else None,
            'gd_coverages_garage_keepers_otc_info':garageKeepersOTCInfo if garageKeepersOTCInfo else None,
            'gd_coverages_hired_borrowed_coverage_info':hiredBorrowedCoverageInfo if hiredBorrowedCoverageInfo else None,
            'gd_coverages_non_owned_coverage_info':nonOwnedCoverageInfo if nonOwnedCoverageInfo else None,
            'gd_coverages_physical_damage_info':physicalDamageInfo if physicalDamageInfo else None,
            'gd_coverages_plate_hoists_info':plateHoistsInfo if plateHoistsInfo else None
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