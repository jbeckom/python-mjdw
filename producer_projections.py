import config 
import pandas as pd 
import datetime as dt
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

def raw_entity_df(table, schema, **kwargs):
    # limit to 10k records per pass, combine to dataframe
    df = pd.concat([chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)])
    df = df.query(kwargs['query']) if 'query' in kwargs else df
    df = df[kwargs['cols']] if 'cols' in kwargs else df
    return df

def annual_commission(row):
    if row.bill_type_cd=='C':
        return row.est_comm_amt
    elif row.department_cd in ['050','056','250']:
        return row.written_agcy_commission_amt
    elif row.bill_type_cd=='A' and row.est_comm_amt is not None:
        return row.est_comm_amt
    else:
        return row.written_agcy_commission_amt

def convert_pct(x):
    x = x.rjust(4,'0')
    x = float('.'.join([x[:2],x[2:]]))
    return x

def sg_policies(expDate):
    ### DO WE NEED TO FILTER OUT PROCOURSE (FILTER BY ONLY DIVISION 1) ###
    query = (f"policy_expiration_dt.fillna({(dt.date.today()-dt.date(1967,12,31)).days}).astype('int') >= {(expDate-dt.date(1967,12,31)).days}" 
            " & (canc_nonrenew_renew_ind.isnull() | canc_nonrenew_renew_ind=='I')"
            " & ~policy_number.str.startswith('APP',na=False)"
            " & ~policy_number.str.contains('|'.join(['RESERVE','CHARGE']),na=False)"
            " & coverage_cd != 396"

    )
    cols = ['sagitem','policy_effective_dt','policy_expiration_dt','client_cd','servicer_cd','department_cd','coverage_cd','policy_desc','policy_number','payee_cd','annual_premium_amt','written_agcy_commission_amt','written_producer_commission_amt','est_comm_amt','bill_type_cd','canc_nonrenew_renew_ind']
    policies = raw_entity_df('policies','sagitta',query=query,cols=cols)
    # query parser doesn't like inline tuple, final filter needs to sliced a different way (work around--can this be fixed?)
    policies = policies[(policies.department_cd.str.startswith(('01','07','21','04','24','05','25'),na=False) | policies.department_cd.str.contains('|'.join(['080','510','110']),na=False))]
    # convert dates to proper format
    for x in ('policy_effective_dt','policy_expiration_dt'):
        policies[x] = policies[x].apply(lambda y: (dt.date(1967,12,31) + dt.timedelta(days=(int(y)))) if not pd.isnull(y) else None)
    # parse month from date
    policies['mo'] = pd.DatetimeIndex(policies.policy_expiration_dt).month
    # annual commission is logic based
    policies['annual_commission'] = policies.apply(lambda x: annual_commission(x),axis=1)
    # get coverage description from coverages table, based on coverage_cd    
    return pd.merge(policies,raw_entity_df('coverages','sagitta')[['sagitem','coverage_cd','coverage_description']].rename({'sagitem':'coverage_cd','coverage_cd':'cov_cd'},axis=1),how='left',on='coverage_cd')

def sg_policy_producer_splits():
    pps = raw_entity_df('policies_acct_prefill_mpci', 'sagitta', cols=['sagitem','lis','producer_cd','producer_new_pct','producer_renewal_pct'])
    for x in ('producer_new_pct','producer_renewal_pct'):
        pps[x] = pps[x].apply(lambda y: convert_pct(y))
    return pps

def sg_clients():
    # filter out ProCourse, Test, AS statuses
    query = "status_cd.fillna('x').str.contains('|'.join(['PRO','TES','AS']))"
    cols = ['sagitem','client_name','client_cd','parent_client','reference_cd','parent_rel_cd','relation_client','relation_cd','servicer_2_cd','servicer_3_cd','city','state_prov_cd','postal_code','postal_extension_code','producer_1_cd','producer_2_cd','producer_3_cd','sic_cd','business_nature','status_cd']
    clients = raw_entity_df('clients','sagitta',query=query,cols=cols)
    staff = sg_staff()
    # join postal_code, postal_extension_cd for zip
    clients['zip'] = clients[['postal_code','postal_extension_code']].fillna('').apply(lambda x: '-'.join(filter(None, x)),axis=1)
    # join AddlInfo for client executive
    clients = clients.merge(raw_entity_df('clients_addlinfo','sagitta',cols=['sagitem','client_exec']),how='left',on='sagitem')
    # strip only first parent client key, join to subset of 'parent' clients, assign parent client code
    clients['parent_key'] = pd.to_numeric(clients.parent_client.str.split().str[0])
    parents = clients[['sagitem','client_cd']].copy().rename({'sagitem':'parent_key','client_cd':'parent_client'},axis=1)
    clients = pd.merge(clients.drop(['parent_client'],axis=1),parents,how='left',on='parent_key')
    # map producer code to name
    for x in range(1,4):
        clients = pd.merge(clients,staff.astype({'sagitem':'object'}).rename({'sagitem':f'producer_{x}_cd','producer':f'producer_{x}_name'},axis=1),how='left',on=f'producer_{x}_cd').drop([f'producer_{x}_cd'],axis=1)
    return clients

def sg_staff():
    staff = raw_entity_df('staff','sagitta',cols=['sagitem','staff_name'])
    staffAddlInfo = raw_entity_df('staff_addlinfo','sagitta',cols=['sagitem','only_staff_name'])
    staff = staff.merge(staffAddlInfo,how='left',on='sagitem')
    staff['producer'] = staff.only_staff_name.combine_first(staff.staff_name)
    return staff.drop(['staff_name','only_staff_name'],axis=1)

def sg_coverages():
    coverages = raw_entity_df('coverages', 'sagitta', cols=['sagitem', 'coverage_cd', 'coverage_description'])
    return coverages

def sg_insurors():
    insurors = raw_entity_df('insurors', 'sagitta', cols=['sagitem','insurer_cd','insurer_name','naic_cd','group']).astype({'group':'Int64'})
    groups = insurors.copy()[['sagitem','insurer_cd','insurer_name']].rename({'sagitem':'group','insurer_cd':'group_cd','insurer_name':'group_name'},axis=1).astype({'group':'Int64'})
    return pd.merge(insurors,groups,how='left',on='group').drop(['group'],axis=1)

def sg_payees():
    payees = raw_entity_df('payees','sagitta',cols=['sagitem','payee_name','agency_cd'])
    return payees

def sg_sic_codes():
    sic = raw_entity_df('sic_codes', 'sagitta', cols=['sagitem','description','category'])
    return sic

def sg_prod_proj_raw(expDate):
    policies = sg_policies(expDate).rename({'client_cd':'client_key'},axis=1).drop(['sagitem'],axis=1)
    clients = sg_clients().rename({'sagitem':'client_key'},axis=1)
    ppr = pd.merge(policies,clients,how='left',on='client_key').drop(['client_key'],axis=1)
    return ppr

def main():
    foo = sg_policy_producer_splits()
    pass

if __name__ == '__main__':
    main()