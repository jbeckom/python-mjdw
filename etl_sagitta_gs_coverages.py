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
FILE = 'gs_coverages'

lf = cmn.log_filer(LOGDIR,FILE)

def coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','glass_deductible','glass_retention_pct','large_glass_option_ind','sign_full_form_coverage_ind','sign_deductible_clause_ind','off_dt','lis_count_glass_info','lis_count_sign_item_info'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def glass_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('glass_item_number','glass_location_agency_id','glass_sub_location','glass_type_cd','num_plates','glass_length_num_units','glass_width_num_units','glass_area_num_units','use_and_position_in_bldg_desc','item_desc','glass_limit_amt','safety_glass_ind','glass_position_and_use_in_bldg_cd','glass_linear_length_num_units','glass_tenants_exterior_ind','remark_text','glass_bldg_interior_ind','glass_bldg_interior_remark_text','num_large_replacement_plates'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def sign_item_info_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('sign_item_number','sign_location_agency_id','sign_sub_location','inside_outside_sign_cd','sign_limit_amt','sign_deductible','sign_descriptions','sign_1_desc_remark_text','sign_2_desc_remark_text'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverages = []
    glassInfo = [] 
    signItemInfo = []

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
                                [glassInfo.append(glass_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('GlassInfo') if (x.get('lis') and len(x.find_all()) > 0)]
                            except Exception as e:
                                lf.error(f"unable to parse GlassInfo for {sagitem}:\n{e}")
                            try:
                                [signItemInfo.append(sign_item_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('SignItemInfo') if (x.get('lis') and len(x.find_all()) > 0)]
                            except Exception as e:
                                lf.error(f"unable to parse SignItemInfo for {sagitem}:\n{e}")
        stages = {
            'gs_coverages':coverages if coverages else None,
            'gs_coverages_glass_info':glassInfo if glassInfo else None,
            'gs_coverages_sign_item_info':signItemInfo if signItemInfo else None
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