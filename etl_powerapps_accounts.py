import mjdb
import config
import common as cmn
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'accounts')

def raw_entity_df(table, schema, **kwargs):
    # limit to 10k records per pass, combine to dataframe
    df = pd.concat([chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]).reset_index(drop=True)
    df = df.query(kwargs['query']) if 'query' in kwargs else df
    df = df[kwargs['cols']] if 'cols' in kwargs else df
    return df

### used to accommodate varying SicCd data structure from Sagitta - there has to be a better way###
def sic_list_row (x):
    sicList = pd.Series('',index=[0,1,2])
    if x:
        sicList = pd.Series(x.strip().split())
    if 0 not in sicList:
        sicList[0] = None
    if 1 not in sicList:
        sicList[1] = None
    if 2 not in sicList:
        sicList[2] = None
    return sicList

def excluded_clients():
    # excluded clients include those outside of division 1 or personal lines (departments 04x, 24x, 54x)
    # Exclude the coverage type of CAP (lira-86770893r)
    query = (
        "division_cd!=1"
        " | coverage_cd==308"
    )
    cols=['client_cd','department_cd']
    df = raw_entity_df('policies', 'sagitta', query=query, cols=cols)
    return df[df.department_cd.str.startswith(('04','24','54'),na=False)].drop(['department_cd'],axis=1).drop_duplicates()

def sg_producers(policyDf, policyType):
    policyDf['policy_effective_dt'] = policyDf.policy_effective_dt.fillna(0).astype('int64')
    policyDf['rn'] = policyDf.sort_values(['policy_effective_dt'],ascending=False).groupby(['client_cd']).cumcount()+1
    policyDf = policyDf[policyDf.rn==1].drop(['rn','policy_effective_dt'],axis=1)
    policyDf['producer_cd'] = policyDf[['producer_1_cd','producer_2_cd','producer_3_cd']].bfill(axis=1).iloc[:,0]
    policyDf['pol_type'] = policyType
    return policyDf.drop(['producer_1_cd','producer_2_cd','producer_3_cd'],axis=1)

def sg_account_df(lastUpdate):
    accounts = raw_entity_df('clients','sagitta')[['sagitem','audit_entry_dt', 'audit_time', 'client_name', 'client_cd','business_nature', 'fein', 'web_site_link', 'status_cd', 'sic_cd','producer_1_cd','producer_2_cd','producer_3_cd']]
    # drop exluded client accounts
    accounts = accounts[~accounts.sagitem.isin(excluded_clients().client_cd)]
    # exclude clients that originate in Benefitpoint
    bpClients = raw_entity_df('account_integration_info', 'benefitpoint')['sagitta_client_id']
    accounts = accounts[~accounts.sagitem.isin(bpClients.astype('int64'))]
    # combine audit_entry_date and audit_time to datetime
    accounts['modify_dt'] = accounts.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row['audit_entry_dt'])), (dt.datetime.min + dt.timedelta(seconds=int(row['audit_time']))).time()), axis=1)
    accounts.drop(['audit_entry_dt','audit_time'],axis=1,inplace=True)
    # filter by last modified
    accounts = accounts[accounts['modify_dt'] >= lastUpdate]
    # set account active/inactive based on status codes, drop irrelevant columns
    accounts['status_code'] = accounts.apply(lambda x: 'INACTIVE' if 'DIP' in str(x['status_cd']) else 'ACTIVE', axis=1)
    accounts.drop(['status_cd'],axis=1,inplace=True)
    # join sic codes to sic code descriptions, drop irrelevant columns
    accounts[['sic_1','sic_2','sic_3']] = accounts.sic_cd.apply(lambda x: sic_list_row(x))
    sicCdDf = raw_entity_df('sic_codes', 'sagitta')[['sagitem', 'description']]
    for k,v in {'sic_1':'pc_primary_sic','sic_2':'pc_second_sic','sic_3':'pc_tertiary_sic'}.items():
        accounts = accounts.merge(sicCdDf.rename(columns={'sagitem':k,'description':v}), how='left', on=k)
    accounts.drop(['sic_cd','sic_1','sic_2','sic_3'], axis=1, inplace=True)
    # get additional info for client, apply logic, drop irrelevant columns
    accounts = accounts.merge(raw_entity_df('clients_addlinfo', 'sagitta')[['sagitem','budgeted_revenue']], how='left', on='sagitem')
    accounts['client_rev_25k'] = accounts.apply(lambda row: True if (int(row.fillna(0)['budgeted_revenue']) >= 25000) else False, axis=1)
    accounts.drop(['budgeted_revenue'], axis=1, inplace=True)
    # get account owner from producer codes
    accounts['ao_code'] = accounts[['producer_1_cd','producer_2_cd','producer_3_cd']].bfill(axis=1).iloc[:,0]
    accounts.drop(['producer_1_cd','producer_2_cd','producer_3_cd','ao_code'], axis=1, inplace=True)
    # get name of parent client 
    accounts = accounts.merge(accounts[['sagitem','client_name']].rename(columns={'client_name':'parent_account'}), how='left', on='sagitem')
    # strip unnecessary characters from website
    accounts['web_site_link'] = accounts['web_site_link'].replace('http://','')
    # set appropriate column names
    accounts.rename(columns={'sagitem':'source_key', 'client_name':'account_name', 'client_cd':'client_code', 'business_nature':'industry', 'fein':'ein','web_site_link':'web_url'},inplace=True)
    # add static value columns
    statics = {'account_source':'SAGITTA', 'pc_client_profile':True, 'eb_client_profile':False}
    for s in statics:
        accounts[s] = statics[s]
    # add None/NULL to pc irrelevant columns
    nones = ('client_category','medical_funding_type','enrolled_employees','bor_dt','erisa_info','stop_loss_renew_dt','dba','eb_primary_sic','other_subsidiaries','eb_account_classification','revenue_range','naics_description','primary_contact','status_code','status_reason','benefit_plan_change_effective_date', 'pc_client_category')
    for n in nones:
        accounts[n] = None    
    return accounts

def bp_account_df(lastUpdate): # re-order the operation of this function -- should start with accountSummary, filtered by lastupdate, then merge other dataframes in
    # entirety of custom_field_value data set
    customFieldValues = raw_entity_df('custom_field_value', 'benefitpoint')
    # filter custom field values to 'Client Level (A, B, C)' values
    clientLevel = ((customFieldValues[(customFieldValues.cfv_source=='ACCOUNT') & (customFieldValues.custom_field_id==85567) & (customFieldValues.value_text!='Select')])[['source_key','value_text']]).rename(columns={'source_key':'account_id','value_text':'client_category'})
    # stop loss only custom_field_values
    medicalFundingType = ((customFieldValues[(customFieldValues.cfv_source=='ACCOUNT') & (customFieldValues.custom_field_id==85388) & (customFieldValues.value_text!='Select')])[['source_key','value_text']]).rename(columns={'source_key':'account_id', 'value_text':'medical_funding_type'})
    # ERISA/5500 Info
    erisaInfo = ((customFieldValues[(customFieldValues.cfv_source=='ACCOUNT') & (customFieldValues.custom_field_id==89131)])[['source_key','value_text']]).rename(columns={'source_key':'account_id','value_text':'erisa_info'}).dropna()
    # renewal info
    stopLoss = ((customFieldValues[(customFieldValues.cfv_source=='ACCOUNT') & (customFieldValues.custom_field_id==85389)])[['source_key','value_text']]).rename(columns={'source_key':'account_id','value_text':'stop_loss_renew_dt'})
    # open enrollment/next due date
    openEnrollmentNextDueDate = (((customFieldValues[(customFieldValues.cfv_source=='ACCOUNT') & (customFieldValues.custom_field_id==89209)])[['source_key','value_text']]).dropna()).rename(columns={'source_key':'account_id','value_text':'benefit_plan_change_effective_date'})
    # broker of record -- change timestamp to date, string format appropriately
    borDf = raw_entity_df('brokerage_account_info', 'benefitpoint')[['account_id','broker_of_record_as_of']].dropna().rename(columns={'broker_of_record_as_of':'bor_dt'})
    borDf['bor_dt'] = borDf['bor_dt'].dt.strftime('%m/%d/%Y')
    # common account info
    accountsCommonInfo = raw_entity_df('common_group_account_info', 'benefitpoint')[['account_id','primary_industry','secondary_industry','tax_payer_id','website']].rename(columns={'tax_payer_id':'ein','website':'web_url'})
    accountsCommonInfo['industry'] = (accountsCommonInfo[['primary_industry','secondary_industry']].bfill(axis=1).iloc[:,0]).replace('None_Selected',None)
    accountsCommonInfo.drop(['primary_industry','secondary_industry'],axis=1,inplace=True)
    accountsCommonInfo['web_url'] = accountsCommonInfo['web_url'].map(lambda x: (x.lstrip('http://')) if x is not None else x)
    # group account info
    groupAccountInfo = raw_entity_df('group_account_info', 'benefitpoint')[['account_id','account_name','dba','sic_code','naics_code']].rename(columns={'sic_code':'eb_primary_sic','naics_code':'naics_description'})
    # account summary
    accountSummary = raw_entity_df('account', 'benefitpoint')[['account_id','account_classification','last_modified_on']].rename(columns={'account_classification':'eb_account_classification'})
    accountSummary = accountSummary[pd.to_datetime(accountSummary.last_modified_on).dt.tz_localize(None) >= lastUpdate]
    # build final account structure
    bpAccounts = pd.merge(accountSummary, groupAccountInfo, how='inner', on='account_id')
    for x in (clientLevel, medicalFundingType, erisaInfo, stopLoss, openEnrollmentNextDueDate, borDf, accountsCommonInfo):
        bpAccounts = bpAccounts.merge(x,how='left',on='account_id')
    bpAccounts.rename(columns={'account_id':'source_key', 'last_modified_on':'modify_dt'}, inplace=True)
    statics = {'account_source':'BENEFITPOINT','pc_client_profile':False, 'eb_client_profile':True}
    # re-use bp account id (source_key) for client_code
    bpAccounts['client_code'] = bpAccounts.source_key
    for s in statics:
        bpAccounts[s] = statics[s]
    nones = ('pc_client_category','enrolled_employees','pc_primary_sic','pc_second_sic','pc_tertiary_sic','client_rev_25k','revenue_range','primary_contact','parent_account','account_owner','status_code','status_reason','benefit_plan_change_effective_date','other_subsidiaries')
    for n in nones:
        bpAccounts[n] = None
    return bpAccounts

def current_clients():
    policies = raw_entity_df('policies', 'sagitta')[['sagitem','client_cd','division_cd','department_cd','policy_number','coverage_cd','canc_nonrenew_renew_ind','canc_nonrenew_renew_dt','policy_expiration_dt']].set_index('sagitem')
    # Client Level or Policy - Division 1 only; Departments not 04*, 24*, 54*; From Policy - Policy Number NE APP*; From Policy - Policy Number NE *RESERVE*, *CHARGE*; From Policy - Cov NE NOT, TAI, MVR (coverage_cd=102,205,211);
    policies = policies[(policies.division_cd=='1') & (~policies.department_cd.str[:2].isin(['04','24','54'])) & (~policies.policy_number.fillna('x').str.startswith('APP')) & (~policies.policy_number.fillna('x').str.contains('|'.join(['RESERVE','CHARGE']))) & (~policies.coverage_cd.isin([102,205,211]))].drop(['division_cd','department_cd','policy_number','coverage_cd'],axis=1)
    # CNR is blank or =I OR (C or N and a CNR date in the future); From Policy - Exp Date GE today;
    policies.loc[policies.canc_nonrenew_renew_ind.isin(['C','N']), 'cnr_date'] = policies.canc_nonrenew_renew_dt
    policies['actual_exp'] = policies[['cnr_date','policy_expiration_dt']].bfill(axis=1).iloc[:,0]
    policies = policies[policies.actual_exp.fillna(0).astype('int64')>=(dt.date.today() - dt.date(1967,12,31)).days].drop_duplicates(subset=['client_cd']).reset_index().drop(['sagitem','canc_nonrenew_renew_ind','canc_nonrenew_renew_dt','policy_expiration_dt','cnr_date','actual_exp'],axis=1)
    clients = raw_entity_df('clients', 'sagitta')[['sagitem','status_cd']]
    # From Client - Status 1,2,3 NE PRO, TES, AS
    clients = (clients[~clients.status_cd.fillna('').str.contains('|'.join(['PRO','TES','AS']))]).drop(['status_cd'], axis=1)
    # join to active policies to filter current clients
    clients = (clients.merge(policies,left_on='sagitem',right_on='client_cd')).drop_duplicates()
    bpAccounts = raw_entity_df('account_integration_info', 'benefitpoint')[['account_id','sagitta_client_id']].rename(columns={'sagitta_client_id':'sagitem'})
    bpAccounts['sagitem'] = bpAccounts.sagitem.astype('int64')
    sgClients = (clients[~clients.sagitem.isin(bpAccounts.sagitem)]).rename(columns={'sagitem':'source_key'})
    sgClients['account_source'] = 'SAGITTA'
    bpClients = (clients.merge(bpAccounts,on='sagitem')).drop(['sagitem'], axis=1).rename(columns={'account_id':'source_key'})
    bpClients['account_source'] = 'BENEFITPOINT'
    return pd.concat([bpClients,sgClients])

def current_producers():
    staff = raw_entity_df('staff', 'sagitta',query="~staff_name.str.contains('team',case=False)", cols=['sagitem','staff_name'])
    staffAddlInfo = raw_entity_df('staff_addlinfo','sagitta',cols=['sagitem','only_staff_name'])
    staff = staff.merge(staffAddlInfo,how='left',on='sagitem')
    staff['producer'] = staff.only_staff_name.combine_first(staff.staff_name)
    staff = staff.drop(['staff_name','only_staff_name'],axis=1)
    policies = raw_entity_df('policies', 'sagitta', cols=['client_cd','department_cd','coverage_cd','producer_1_cd','producer_2_cd','producer_3_cd','policy_effective_dt'])
    sgProducers = pd.DataFrame()
    pcProducers = policies[policies.department_cd.str.startswith(('01','21'),na=False)].drop(['department_cd','coverage_cd'], axis=1).drop_duplicates().copy()
    bdProducers = policies[policies.department_cd.str.startswith(('05','25'),na=False)].drop(['department_cd','coverage_cd'], axis=1).drop_duplicates().copy()
    for k,v in {'PC':pcProducers,'BD':bdProducers}.items():
        sgProducers = pd.concat([sgProducers,sg_producers(v,k)])
    trProducers = policies[policies.coverage_cd==413].drop(['department_cd','coverage_cd','producer_1_cd','producer_2_cd','producer_3_cd','policy_effective_dt'], axis=1).drop_duplicates().copy()
    bpInt = raw_entity_df('account_integration_info', 'benefitpoint', cols=['account_id','sagitta_client_id']).rename({'sagitta_client_id':'client_cd'},axis=1)
    bpInt.client_cd = bpInt.client_cd.astype('int64')
    trProducers = trProducers.merge(bpInt,how='inner',on='client_cd').drop(['client_cd'],axis=1).rename({'account_id':'client_cd'},axis=1)
    trProducers['producer_cd']='JULBI'
    trProducers['pol_type']='TR'
    sgProducers = pd.concat([sgProducers,trProducers])
    sgProducers = sgProducers.merge(staff.rename({'sagitem':'producer_cd'},axis=1),on='producer_cd').drop(['producer_cd'],axis=1)
    bpProducers = raw_entity_df('group_account_info', 'benefitpoint')[['account_id']]
    cfvQuery = "custom_field_id.isin([6618,6619,6620,18474]) and ~value_text.str.contains('|'.join(['Select','Team']),na=False)"
    cfvCols = ['source_key','custom_field_id','value_text']
    customFieldValues = raw_entity_df('custom_field_value', 'benefitpoint', query=cfvQuery, cols=cfvCols)
    for k,v in {'prod1':6618,'prod2':6619,'prod3':6620,'prod4':18474}.items():
        bpProducers = bpProducers.merge(customFieldValues[customFieldValues.custom_field_id==v].drop(['custom_field_id'],axis=1).rename({'source_key':'account_id','value_text':k},axis=1),how='left',on='account_id')
    bpProducers['producer'] = bpProducers[['prod1','prod2','prod3','prod4']].bfill(axis=1).iloc[:,0]
    bpProducers = (bpProducers.rename(columns={'account_id':'client_cd'}).dropna(subset=['producer'])).drop(['prod1','prod2','prod3','prod4'],axis=1)
    bpProducers['pol_type'] = 'EB'
    return pd.concat([sgProducers,bpProducers])
    
def main():
    try:
        sgLastUpdate = mjdb.entity_last_update('powerapps', 'account', ('SAGITTA',))
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'account', source='SAGITTA')\n{e}")
    try:
        bpLastUpdate = mjdb.entity_last_update('powerapps', 'account', ('BENEFITPOINT',))
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'account', source='BENEFITPOINT')\n{e}")
    try:
        sgAccounts = sg_account_df(sgLastUpdate if sgLastUpdate else dt.datetime(1900,1,1,0,0,0))
    except Exception as e:
        lf.error(f"sg_account_df({sgLastUpdate} if <<sgLastUpdate>> else {dt.datetime(1900,1,1,0,0,0)})\n{e}")
    try:
        bpAccounts = bp_account_df(bpLastUpdate if bpLastUpdate else dt.datetime(1900,1,1,0,0,0))
    except Exception as e:
        lf.error(f"bp_account_df({bpLastUpdate} if <<bpLastUpdate>> else {dt.datetime(1900,1,1,0,0,0)})\n{e}")
    try:
        rcs = pd.concat([sgAccounts,bpAccounts]).to_sql('stg_account', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage accounts\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} records staged for account")
            try:
                rcu = mjdb.upsert_stage('powerapps','account','upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps','account')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps','account') affected {rcu} row(s).")
    mjdb.drop_table('powerapps','stg_account')
    # update current/former clients
    try:
        rcscc = current_clients().to_sql('stg_current_clients', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for Current Clients\n{e}")
    else:
        if rcscc > 0:
            lf.info(f"{rcscc} record(s) staged for Current Clients")
            for x in ('current_clients_eb','current_clients_pc','former_clients'):
                try:
                    rcu = mjdb.function_execute('powerapps', f'sp_{x}_update')
                except Exception as e:
                    lf.error(f"unable to update record(s) for {x}:\n{e}")
                else:
                    lf.info(f'{rcu} record(s) updated for {x}')
        else:
            lf.info("no new records for Current/Former Clients")
    finally:
        mjdb.drop_table('powerapps', 'stg_current_clients')
    try:
        rcscp = current_producers().to_sql('stg_current_producers', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for Current Producers")
    else:
        if rcscp > 0:
            lf.info(f"{rcscp} record(s) staged for Current Producers")
            for x in ('eb','pc','bond','tr'):
                try:
                    rcu = mjdb.function_execute('powerapps', f'sp_{x}_producer_update')
                except Exception as e:
                    lf.error(f"unable to update Current Producers for {x.upper()}:\n{e}")
                else:
                    lf.info(f'{rcu} Current Producer record(s) updated for {x.upper()}')
        else:
            lf.info("no new records for Current Producers")
    finally:
        mjdb.drop_table('powerapps', 'stg_current_producers')

if __name__ == '__main__':
    main()