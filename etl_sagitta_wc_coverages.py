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
FILE = 'wc_coverages'

lf = cmn.log_filer(LOGDIR,FILE)

def coverage_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'uslh_ind':soup.find('USLHInd').text if soup.find('USLHInd') else None,
        'uslh_premium_amt':soup.find('USLHPremiumAmt').text if soup.find('USLHPremiumAmt') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','empl_liab_acc_limit','disease_per_empl_limit','disease_policy_limit','empl_liab_premium_amt','voluntary_comp_ind','voluntary_comp_premium_amt','other_states_ind','other_states_premium_amt','off_dt','rate','statutory_limits_apply'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    for k,v in {'ExcludedStatesInfo':'excluded_states', 'IncludedStatesInfo':'included_states'}.items():
        row[v] = ', '.join([x.text for x in soup.find(k).find_all()]) if soup.find(k) else None
    return row

def coverage_extensions_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis,
        'first_ea_accident_bi_limit':soup.find('FirstEaAccidentBILimit').text if soup.find('FirstEaAccidentBILimit') else None,
        'second_bi_by_disease_limit':soup.find('SecondBIByDiseaseLimit').text if soup.find('SecondBIByDiseaseLimit') else None,
        'third_per_employee_by_bi_limit':soup.find('ThirdPerEmployeeByBILimit').text if soup.find('ThirdPerEmployeeByBILimit') else None
    }
    for t in ('coverage_cd','state_prov_cd','form_number','edition_dt','coverage_extention_annual_premium_amt','coverage_desc','location_number','type_cd','deductible_1_amt','deductible_2_amt','deductible_1_type_cd','deductible_2_type_cd','rate'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def main():
    coverages = []
    coverageExtensionsInfo = []

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
                                [coverageExtensionsInfo.append(coverage_extensions_info_row(sagitem, int(x.get('lis')), x)) for x in item.find_all('CoverageExtensionsInfo') if (x.get('lis') and len(x.find_all()) > 0)]
                            except Exception as e:
                                lf.error(f"unable to parse CoverageExtensionsInfo for {sagitem}:\n")
            stages = {
                'wc_coverages':coverages if coverages else None,
                'wc_coverages_coverage_extensions_info': coverageExtensionsInfo if coverageExtensionsInfo else None
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
                                rcu = mjdb.upsert_stage('sagitta',s,'upsert')
                            except Exception as e:
                                lf.error(f"mjdb.upsert_stage('sagitta',{s},'upsert')\n{e}")
                            else:
                                lf.info(f"mjdb.upsert_stage('sagitta',{s},'upsert') affected {rcu} record(s)")
                    finally:
                        mjdb.drop_table('sagitta', f'stg_{s}')
                else:
                    lf.info(f"no records to stage for {s}")

if __name__ == '__main__':
    main()