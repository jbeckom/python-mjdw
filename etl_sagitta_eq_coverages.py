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

lf = cmn.log_filer(LOGDIR,'eq_coverages')

def coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time_ind':int(soup.find('AuditTimeInd').text) if soup.find('AuditTimeInd') else None,
        'aoi_seq_no':soup.find('AOISeqNo').text if soup.find('AOISeqNo') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','property_at_acquired_locations','property_covered_at_all_locations','property_in_transit','prop_not_at_your_premises_not_on_schedule','ded_flat_dollar_amt','ded_percentage_of_equipment_value','ded_percentage_minimum','ded_percentage_maximum','ded_annual_aggregate','catastrophe_limit_per_event_accident','eq_flood_catastrophe_loss_amt','equipment_blanket_amt','equipment_coins_pct','off_dt','schedule_type','location_no','building_no','building_desc','reporting','reporting_deposit_premium','reporting_minimum_ann_premium','reporting_reporting_period','reporting_adjustment_period','reporting_first_prem_base','reporting_second_prem_base','reporting_third_prem_base','reporting_first_rate','reporting_second_rate','non_reporting','non_reporting_first_rate','non_reporting_rate','non_reporting_premium'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def seini_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis,
        'aoi_seq_no':soup.find('AOISeqNo').text if soup.find('AOISeqNo') else None
    }
    for t in ('equipment_schedule_item_number','equipment_sch_model_yr','equip_sch_id_serial_number','equip_sch_purchase_dt','equip_sch_new_used_ind','equipment_schedule_deductible_amt','equip_sch_insurance_amt','equip_sch_desc','ded_type','ded_basis','limit_basis','model','item_value','item_value_type','schedule_equipment_limit_val_date','owned_leased','schedule','manufacturer'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def esi_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('location_agency_id','num_months_in_storage','max_value_in_building','max_value_out_building','equip_storage_security'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def uei_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('unsch_equip_desc','unscheduled_equip_coins_pct','num_unscheduled_equip_max_item','unscheduled_equipm_insurance_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def acci_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis,
        'addl_cov_limit':soup.find('AddlCovLImit').text if soup.find('AddlCovLImit') else None
    }
    for t in ('addl_cov_cd','addl_cov_item_number','addl_cov_ded_amt','addl_cov_ded_pct','addl_cov_rate','addl_cov_endt_form_number','addl_cov_edition_dt','addl_cov_premium','addl_cov_des','addl_cov_estimated_ann_rental_expense','item_limit_basis','item_limit_valuation_type','item_deductible_type','item_deductible_basis'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverageList = []
    seiniList = []
    esiList = []
    ueiList = []
    acciList = []
    try:
        lastEntry = mjdb.sg_last_entry('eq_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('eq_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT EQ.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT EQ.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        if item.get('sagitem').isnumeric():
                            try:
                                sagitem = int(item.get('sagitem'))
                                coverageList.append(coverages_row(sagitem,item))
                            except Exception as e:
                                lf.error(f"coverages_row({sagitem},<<item>>)\n{e}")
                            else:
                                for seini in item.find_all('ScheduledEquipmentItemNumberInfo'):
                                    if seini.get('lis') and len(seini.find_all()) > 0:
                                        try:
                                            lis = int(seini.get('lis'))
                                            seiniList.append(seini_row(sagitem,lis,seini))
                                        except Exception as e:
                                            lf.error(f"seini_row({sagitem},{lis},<<seini>>)\n{e}")
                                for esi in item.find_all('EquipmentStorageInfo'):
                                    if esi.get('lis') and len(esi.find_all()) > 0:
                                        try:
                                            lis = int(esi.get('lis'))
                                            esiList.append(esi_row(sagitem,lis,esi))
                                        except Exception as e:
                                            lf.error(f"esi_row({sagitem},{lis},<<esi>>)\n{e}")
                                for uei in item.find_all('UnscheduledEquipmentInfo'):
                                    if uei.get('lis') and len(uei.find_all()) > 0:
                                        try:
                                            lis = int(uei.get('lis'))
                                            ueiList.append(uei_row(sagitem,lis,uei))
                                        except Exception as e:
                                            lf.error(f"eui_row({sagitem},{lis},<<uei>>)\n{e}")
                                for acci in item.find_all('AddlCovCdInfo'):
                                    if acci.get('lis') and len(acci.find_all()) > 0:
                                        try:
                                            lis = int(acci.get('lis'))
                                            acciList.append(acci_row(sagitem,lis,acci))
                                        except Exception as e:
                                            lf.error(f"acci_row({sagitem},{lis},<<acci>>)\n{e}")
    stages = {
        'eq_coverages':coverageList if coverageList else None,
        'eq_coverages_scheduled_equipment_item_number_info':seiniList if seiniList else None,
        'eq_coverages_equipment_storage_info':esiList if esiList else None,
        'eq_coverages_unscheduled_equipment_info':ueiList if ueiList else None, 
        'eq_coverages_addl_cov_cd_info':acciList if acciList else None
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