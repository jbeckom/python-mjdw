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
FILE = 'ol_coverages'

lf = cmn.log_filer(LOGDIR,FILE)

def coverage_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'lob_cd':soup.find('LOBCd').text if soup.find('LOBCd') else None,
        'lob_desc':soup.find('LOBDesc').text if soup.find('LOBDesc') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','rating_basis','valuation_code_1','valuation_code_2','cause_of_loss','cov_effective_date','cov_expiration_date','number_of_1_desc','number_of_2','number_of_2_desc','off_dt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def coverage_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('coverage_cd','state_prov_cd','location_number','building_number','form_number','addl_cov_edition_dt','rate','co_insurance','premium_amt','limit_1','limit_1_desc_code','deductible_1','ded_symbol_1','deductible_basis_cd_1','deductible_type_cd_1','limit_2','limit_2_desc_code','deductible_2','ded_symbol_2','deductible_basis_cd_2','deductible_type_cd_2','exposure_1','territory','coverage_desc','addl_info_ind','vehicle_no','rating_basis','valuation_code_1','valuation_code_2','cause_of_loss','cov_effective_date','cov_expiration_date','exposure_1basis','exposure_2','exposure_2basis','cov_type_code'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def discount_surcharge_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('discount_surcharge_code','discount_surcharge_desc','discount_surcharge_rate','discount_surcharge_pct','discount_surcharge_premium','discount_surcharge_remarks'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def main():
    coverages = []
    coverageInfo = []
    discountSurchargeInfo = []

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
                for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            coverages.append(coverage_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"coverages_row({sagitem},<<item>>)\n{e}")
                        else:
                            try:
                                [coverageInfo.append(coverage_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('CoverageInfo')]
                            except Exception as e:
                                lf.error(f"unable to parse CoverageInfo for {sagitem}:\n{e}")
                            try:
                                [discountSurchargeInfo.append(discount_surcharge_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('DiscountSurchargeInfo')]
                            except Exception as e:
                                lf.error(f"unable to parse DiscountSurchargeInfo for {sagitem}:\n{e}")
            stages = {
                'ol_coverages':coverages if coverages else None,
                'ol_coverages_coverage_info':coverageInfo if coverageInfo else None,
                'ol_coverages_discount_surcharge_info':discountSurchargeInfo if discountSurchargeInfo else None
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