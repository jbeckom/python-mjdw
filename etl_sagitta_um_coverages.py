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

lf = cmn.log_filer(LOGDIR,'um_coverages')

def coverages_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','follow_umb_form','follow_form_excess','umbrella_excess','liab_each_occur_limit','liab_annual_aggregate','retained_limit','first_dollar_defense','current_retro_date','off_dt','desc_of_underlying_cov','garagekeepers_coverage','garagekeepers_exposure','aircraft_pass_coverage','aircraft_pass_exposure','care_custody_coverage','care_custody_exposure','professional_coverage','professional_exposure','foreign_liab_coverage','foreign_liab_exposure','malpractice_coverage','malpractive_exposure','aircraft_liab_coverage','aircraft_liab_exposure','add_int_coverage','add_int_exposure','emp_benefit_coverage','emp_benefit_exposure','liquor_coverage','liquor_exposure','pollution_coverage','pollution_exposure','vendors_liab_coverage','vendors_liab_exposure','watercraft_coverage','watercraft_exposure','first_other_description','first_other_coverage','first_other_exposure','second_other_description','second_other_coverage','second_other_exposure','third_other_description','third_other_coverage','third_other_exposure','fourth_other_description','fourth_other_coverage','fourth_other_exposure','retro_coverage_yes_no_cd','retro_proposed_date'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def employer_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('empl_pol_seq_number','empl_pol_number','employers_insurer','employers_insurer_name','employers_effective_date','employers_exp_date','employers_ann_prem','employers_each_occ_limit','employers_disease_each_employee','employers_disease_policy_limit','employers_first_rat_mod_info','employers_first_rate_mod_type','employers_first_rate_mod_amount','employers_second_rate_mod_info','employers_second_rate_mod_type','employers_second_rate_mod_amount','employers_third_rate_mod_info','employers_third_rate_mod_type','employers_third_rate_mod_amount','employers_fourth_rate_mod_info','employers_fourth_rate_mod_type','employers_fourth_rate_mod_amount'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row

def auto_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('auto_pol_seq','auto_pol_number','auto_insurer','auto_insurer_name','auto_effective_date','auto_exp_date','auto_any_auto_symbol','auto_rating_mod','auto_csl_limit'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    others = {
        'auto_bi_limit':'AutoBILimit',
        'auto_pd_limit':'AutoPDLimit',
        'auto_csl_ann_prem':'AutoCSLAnnPrem',
        'auto_bi_ann_prem':'AutoBIAnnPrem',
        'pd_ann_prem':'PDAnnPrem'
    }
    for o in others:
        row[o] = soup.find(others[o]).text if soup.find(others[o]) else None
    return row

def cov_extension_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('cov_extension','cov_extension_first_limit','cov_extension_first_ded','cov_extension_second_limit','cov_extension_second_ded','cov_extension_form','cov_extension_edition_date','cov_extension_premium','cov_extension_description'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def gl_pol_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    others = {
        'gl_pol_seq_number':'GLPolSeqNumber',
        'gl_pol_number':'GLPolNumber',
        'gl_claims_occur':'GLClaimsOccur',
        'gl_insurer':'GLInsurer',
        'gl_insurer_name':'GLInsurerName',
        'gl_effective_date':'GLEffectiveDate',
        'gl_exp_date':'GLExpDate',
        'gl_rate_mode':'GLRateMode',
        'gl_each_occur_limit':'GLEachOccurLimit',
        'gl_gen_agg_amount':'GLGenAggAmount',
        'gl_prod_compl_oper_limit':'GLProdComplOperLimit',
        'gl_fire_damage_limit':'GLFireDamageLimit',
        'gl_med_exp_limit':'GLMedExpLimit',
        'gl_pes_adv_inj_limit':'GLPesAdvInjLimit',
        'gl_prem_operations_ann_prem':'GLPremOperationsAnnPrem',
        'gl_products_ann_prem':'GLProductsAnnPrem',
        'gl_other_ann_prem':'GLOtherAnnPrem'
    }
    for o in others:
        row[o] = soup.find(others[o]).text if soup.find(others[o]) else None
    return row

def misc_pol_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('misc_pol_seq_number','misc_pol_number','misc_coverage_code','misc_insurer','misc_insurer_name','misc_effective_date','misc_exp_date','misc_rate_mod','misc_first_cov_info','misc_first_cov_descr','misc_first_cov_limit','misc_second_cov_info','misc_second_cov_descr','misc_second_cov_limit','acord_coverage_type'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def entity_merge(one, two):
    for a in one:
        one[a] = two[a] if one[a] is None else one[a]
    return one

def entity_dedup(dupList):
    z = []
    for x in dupList:
        for y in dupList:
            z.append(entity_merge(x,y)) if (x['lis']==y['lis'] and x!=y) else None
    return [dict(t) for t in {tuple(d.items()) for d in z}]

def main():
    coverages = []
    employerInfo = []
    autoInfo = []
    covExtensionInfo = []
    glPolInfo = []
    miscPolInfo = []

    try:
        lastEntry = mjdb.sg_last_entry('um_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('um_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT UM.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT UM.COVERAGES *GET.BATCH* {batch}"
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
                            ### EMPLOYER INFO ###
                            try:
                                [employerInfo.append(x) for x in entity_dedup([employer_info_row(sagitem, int(ei.get('lis')), ei) for ei in item.find_all('EmployerInfo') if (ei.get('lis') and len(ei.find_all()) > 0)]) if (x['empl_pol_seq_number'] and x['empl_pol_number'])]
                            except Exception as e:
                                lf.error(f"unable to parse EmployerInfo for {sagitem}:/n{e}")
                            ### AUTO INFO ###
                            try:
                                [autoInfo.append(x) for x in entity_dedup([auto_info_row(sagitem, int(ai.get('lis')), ai) for ai in item.find_all('AutoInfo') if (ai.get('lis') and len(ai.find_all()) > 0)]) if (x['auto_pol_seq'] and x['auto_pol_number'])]
                            except Exception as e:
                                lf.error(f"unable to parse AutoInfo for {sagitem}:\n{e}")
                            ### COV EXTENSION INFO ###
                            try:
                                [covExtensionInfo.append(cov_extension_info_row(sagitem, int(x.get('lis')), x)) for x in item.find_all('CovExtensionInfo') if (x.get('lis') and len(x.find_all()) > 0)]
                            except Exception as e:
                                lf.error(f"unable to parse CovExtensionInfo for {sagitem}:\n{e}")
                            ### GL POL INFO ###
                            try:
                                [glPolInfo.append(x) for x in entity_dedup([gl_pol_info_row(sagitem, int(gpi.get('lis')), gpi) for gpi in item.find_all('GLPolInfo') if (gpi.get('lis') and len(gpi.find_all()) > 0)]) if (x['gl_pol_seq_number'] and x['gl_pol_number'])]
                            except Exception as e:
                                lf.error(f"unable to parse GLPolInfo for {sagitem}:/n{e}")
                            ### MISC POL INFO ###
                            try:
                                [miscPolInfo.append(x) for x in entity_dedup([misc_pol_info_row(sagitem, int(mpi.get('lis')), mpi) for mpi in item.find_all('MiscPolInfo') if (mpi.get('lis') and len(mpi.find_all()) > 0)]) if (x['misc_pol_seq_number'] and x['misc_pol_number'])]
                            except Exception as e:
                                lf.error(f"unable to parse MiscPolInfo for {sagitem}:\n{e}")
        stages = {
            'um_coverages':coverages if coverages else None,
            'um_coverages_auto_info':autoInfo if autoInfo else None,
            'um_coverages_cov_extension_info':covExtensionInfo if covExtensionInfo else None,
            'um_coverages_employer_info':employerInfo if employerInfo else None,
            'um_coverages_gl_pol_info':glPolInfo if glPolInfo else None,
            'um_coverages_misc_pol_info':miscPolInfo if miscPolInfo else None
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