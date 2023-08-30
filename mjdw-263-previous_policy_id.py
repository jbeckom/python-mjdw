import os
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

lf = cmn.log_filer(LOGDIR,'mjdw-263-previous_policy_id')

def policy_row(sagitem, soup):
    row = {
        'sagitem':sagitem,
        'bor_effective_dt':soup.find('BOREffectiveDt').text if soup.find('BOREffectiveDt') else None,
        'bor_expiration_dt':soup.find('BORExpirationDt').text if soup.find('BORExpirationDt') else None
    }
    ints = ('audit_entry_dt','audit_time','client_cd','coverage_cd','policy_expiration_dt')
    texts = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_number','named_insured','bill_to_cd','policy_remark_text','insurer_name','canc_nonrenew_renew_ind','policy_contract_term_cd','policy_effective_dt','policy_effective_local_standard_time_ind','policy_expiration_local_standard_time_ind','policy_original_inception_dt','binder_effective_dt','binder_start_time_ind','binder_expiration_dt','binder_expiration_time_ind','bill_type_cd','binder_purpose_cd','cancellation_type_cd','canc_nonrenew_renew_dt','last_premium_amt','last_commission_pct','last_commission_amt','new_renew_ind','last_transaction_id','last_transaction_dt','producer_1_cd','producer_2_cd','producer_3_cd','written_premium_amt','written_agcy_commission_amt','written_producer_commission_amt','previous_policy_id','next_policy_id','annual_premium_amt','annual_agency_premium_amt','annual_producer_premium_amt','division_cd','state_prov_cd','last_letter','audit_term_cd','servicer_cd','department_cd','canc_dt','canc_reason_cd','canc_evidence','reinstate_dt','reinstate_reason_cd','countersignature_state_prov_cd','date_business_started','nature_business_cd','general_info_remark_text','num_current_addr_yrs','previous_addr_1','previous_addr_2','prevoius_postal_code','previous_postal_extension_code','previous_city','previous_state_prov_cd','current_residence_dt','previous_residence_dt','birth_dt','tax_id','num_residents_in_household','named_individuals','marital_stats_cd','occupation_class_cd','occupation_desc','length_time_employed','household_income_amt','commercial_name','length_time_with_previous_employee','length_time_with_current_occupation','num_vehs_in_household','length_time_known_by_agent_broker','auto_club_member_yes_no_cd','umpd_rejection_yes_ne_cd','underins_motorist_rejection_yes_no_cd','any_losses_accidents_convictions_ind_yes_no_cd','residence_owned_rented_cd','co_insured_birth_dt','co_insured_tax_id','co_insured_marital_status_cd','co_insured_occupation_class_cd','co_insured_occupation_desc','co_insured_length_time_with_current_employer','co_insured_commercial_name','co_insured_length_time_with_previous_employer','co_insured_length_time_current_occupation','business_income_type_business_cd','policy_type_cd','integration_policy_number','do_not_send_to_insurlink','payee_cd','canc_last_dt','policy_desc','block_download','block_archive','policy_source','carrier_producer_sub_code','est_prem_amt','est_comm_pct','est_comm_amt','payment_plan','insureds_title','co_insureds_title','insureds_first_name','co_insureds_first_name','insureds_middle_name','co_insureds_middle_name','insureds_last_name','co_insureds_last_name','insureds_suffix','co_insureds_suffix')
    for i in ints:
        tag = ''.join([x.capitalize() for x in i.split('_')])
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        if tag in ('PolicyRemarkText','NatureBusinessCd','CancLastDt','CountersignatureStateProvCd','GeneralInfoRemarkText','BinderExpirationDt'):
            row[t] = os.linesep.join([c.text for c in soup.find(tag).children]) if soup.find(tag) else None
        else:
            row[t] = soup.find(tag).text if soup.find(tag) else None
    row['sic_cd'] = int(soup.find('SICCd').text) if soup.find('SICCd') else None
    return row

def main():
    policies = []
    try:
        for batch in hlp.parse_batch_items(sgws.post_ptr_access_statement("SELECT POLICIES *BATCH*")):
            for item in sgws.post_ptr_access_statement(f"SELECT POLICIES *GET.BATCH* {batch}").find_all('Item'):
                sagitem = int(item.get('sagitem'))
                if item.find('PreviousPolicyId'):
                    policies.append(policy_row(sagitem,item))
    except Exception as e:
        lf.error(f"unable to retrieve batches:\n{e}")
    else:
        try:
            rcs = pd.DataFrame(policies).to_sql('stg_policies', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for policies")
        else:
            if rcs > 0:
                lf.info(f"{rcs} record(s) staged for policies")
                try:
                    rcu = mjdb.upsert_stage('sagitta', 'policies', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('sagitta', 'policies')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('sagitta', 'policies') affected {rcu} record(s)")
        finally:
            mjdb.drop_table('sagitta', 'stg_policies')

if __name__ == '__main__':
    main()