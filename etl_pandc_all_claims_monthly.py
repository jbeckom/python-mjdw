import re
import config
import common as cmn
import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import datetime as dt
from numbers import Number
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine

LOGDIR = 'etl_pandc'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
SCHEMA = 'p_and_c'
MONEYPAT = r'[$,()]'

lf = cmn.log_filer(LOGDIR, 'accounts')

def raw_entity_df(table,schema,**kwargs):
    df = pd.concat([chunk for chunk in pd.read_sql_table(table,ENGINE,schema,chunksize=10000)])
    df = df[kwargs['cols']] if 'cols' in kwargs else df
    return df

def name_split(x, source): ### validate transformations
    if source==1:
        first = x.split(', ')[1].split()[0].title() if ',' in x else None
        middle = x.split()[2].title() if ',' in x  and len(x.split()) > 2 else None
        last = x.split(',')[0].title() if ',' in x else None
        return pd.Series([first,middle,last])
    if source==2:
        first = x.split()[0] if ',' not in x else None
        last = x.split()[1] if len(x.split()) > 1 else None
        return pd.Series([first,last])
    if source==3 and x is not None:
        first = x.split()[1] if len(x.split()) > 1 else None
        middle = x.split()[2] if len(x.split()) > 2 else None
        last = x.split(',')[0]
        return pd.Series([first,middle,last])
    if source==4:
        length = len(x.split(' '))
        splits = x.split()
        # if x.split() and len(x.split())<=2:
        if splits and length==2:
            return pd.Series([x.split()[1],x.split()[0]])
        else:
            return pd.Series([None,x]).fillna('')
    if source==5:
        if x is not None:
            first = x.split()[1] if x.split() and len(x.split()) > 1 else None
            middle = x.split()[2] if x.split() and len(x.split()) > 2 else None
            last = x.split()[0] if x.split() else None
        else:
            first, middle, last = None, None, None
        return pd.Series([first,middle,last])
    else: 
        return pd.Series([None,None,None])

def add_days(x):
    return pd.to_datetime('1900-01-01') + pd.Timedelta(days=int(x)+2)

def strip_money(x):
    """convert parenthetical notation, remove special characters, cast to smallest numeric type"""
    return x if isinstance(x,Number) else pd.to_numeric(re.sub(MONEYPAT,'',(('-' + x) if x.startswith('(') and x.endswith(')') else x))) if x is not None else 0

def sum_money(x):
    """ strips monetary notation(s) and sums all values in list(x)"""
    return sum([np.nan_to_num(strip_money(x[i])) for i in range(len(x))])

def days_employed(source,**kwargs):
    if source=='10000006':
        return (dt.datetime.strptime(kwargs['lost'],'%m/%d/%Y')-dt.datetime.strptime(kwargs['hired'],'%m/%d/%Y')).days 
    elif source=='acuity':
        num = int(kwargs['employed'].split()[0])
        unit = kwargs['employed'].split()[1].lower()
        return 365*num if 'yrs' in unit else 30*num if 'mon' in unit else num if 'days' in unit else np.nan

def claimant_age(loss,dob):
    return relativedelta(dt.datetime.strptime(loss,'%m/%d/%Y'),dt.datetime.strptime(dob,'%m/%d/%Y')).years

def parse_li_pdi(x):
    return 'Liability' if strip_money(x['Liability_Incurred']) > strip_money(x['Physical_Damage_Incurred']) else 'Physical Damage'

def carrier_coverage_type(source, row):
    if source=='cau':
        return 'Liability' if strip_money(row['Liability_Incurred']) > strip_money(row['Physical_Damage_Incurred']) else 'Physical Damage'
    elif source=='gli':
        return 'Prem Oper' if strip_money(row['Premises_Oper_Incurred']) > (sum([strip_money(row['Medical_Payment_Incurred']),strip_money(row['Personal_Injury_Incurred'])])) else 'Med Pay'

def normal_1000001():
    ### RAW DATA ###
    source_table = 'raw_1000001'
    raw = raw_entity_df(source_table,SCHEMA)
    ### RELEVANT SUPPLEMENTALS ###
    supp = raw_entity_df('raw_1000001_supp',SCHEMA,cols=['Claim Number','Claimant Hire Date','uploaded_at'])
    supp['Claimant Hire Date'].replace('',np.nan,inplace=True)
    supp.dropna(subset=['Claimant Hire Date'],inplace=True)
    supp.uploaded_at = pd.to_datetime(supp.uploaded_at, errors='coerce')
    # USE RANK TO RETURN FROM MOST RECENT UPLOAD DT
    supp['rank'] = supp.groupby('Claim Number')['uploaded_at'].rank(method='dense',ascending=False)
    supp = supp[supp['rank']==1].drop(['rank','uploaded_at'],axis=1)
    supp['Claim Number'] = supp['Claim Number'].str.split().str[1]
    ### JOIN RAW & SUPPS FOR CLAIMANT HIRE DATE ###
    raw = pd.merge(raw, supp, how='left', on='Claim Number')
    ### TRANSFORMATIONS ###
    for k,v in {
        'claim_number':'1000001-' + raw['Claim Number'],
        'claimant_number':raw['Claim Number'].str.split('-').str[1],
        'paid_expense':raw[['Legal PTD','Non-Legal PTD']].apply(lambda x: sum_money([x['Legal PTD'],x['Non-Legal PTD']]),axis=1),
        'reserve_expense':raw[['Legal Outsd','Non-Legal Outsd']].apply(lambda x: sum_money([x['Legal Outsd'],x['Non-Legal Outsd']]),axis=1),
        'claim_status':np.where(raw['Claim Status Description'] == 'Reopened', 'Reopen',np.where (raw['Claim Status Description'] == 'Cancelled', 'Closed',raw['Claim Status Description'])),
        'driver_last':raw['Insured Driver Last Name'].str.title(),
        'closed_date':np.where(raw['Claim Status Code']=='R',np.nan,raw['Closed/Reopened Date']),
        'reopen_date':np.where(raw['Claim Status Code']=='R',raw['Closed/Reopened Date'],np.nan),
        'litigation':np.where(raw['Litigation Ind']=='Yes',np.nan,'No'),
        'days_employed':np.where(raw['Claimant Hire Date']!='',(pd.to_datetime(raw['Loss Date']).dt.normalize() - pd.to_datetime(raw['Claimant Hire Date']).dt.normalize()).dt.days,raw['Claimant Hire Date']), ### validate calculations
        'claimant_gender':np.where(raw['Clmt Sex']=='M','Male',np.where(raw['Clmt Sex']=='F','Female',None)),
        'carrier_coverage_type':'1000001-' + raw['Minor Cov Description'],
        'carrier_policy_number':'1000001 - ' + raw['Level 7 Code'] + ' - ' + raw['Minor Cov Description'].str.split('-').str[0] + ' - ' + raw['Experience Period'].str.split('-').str[0] + ' - ' + raw['Policy Number'],
        'source_table':source_table,
        'carrier_cause':raw['Cause Description'] ### appears to be redundant -- is it necessary?
    }.items(): raw[k]=v
    ### SPLIT FULL NAME TO PARTS ###
    raw[['claimant_first','claimant_middle','claimant_last']] = raw['Claimant Name'].apply(name_split,args=(1,))
    ### NORMALIZE RELEVANT COLUMN NAMES,RETURN IN PREFERRED ORDER ###
    return (
        raw.drop_duplicates()
            .rename({'Occurrence Number':'occurrence_number','Ind/PD PTD':'paid_indemnity','Med/BI PTD':'paid_medical','Ind/PD Outsd':'reserve_indemnity','Med/BI Outsd':'reserve_medical','Total Recov':'subrogation','Cause Description':'description_text','Loss State':'claim_state','Location Code':'claim_site','Benefit State':'benefit_state','Garage State':'garage_state','Occupation':'occupation','NCCI Class Code':'class_code','Claimant Name':'claimant_name','Loss Date':'loss_date','Reported Date':'report_date','Clmt Age':'claimant_age','Body Part':'carrier_body_part','Injury Desc':'carrier_nature','NCCI Inj Code':'carrier_injury_code'},axis=1)
    )[['claim_number','occurrence_number','claimant_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','claim_status','description_text','claim_state','benefit_state','garage_state','occupation','class_code','claimant_name','claimant_first','claimant_middle','claimant_last','driver_last','loss_date','report_date','closed_date','reopen_date','litigation','days_employed','claimant_gender','claimant_age','carrier_body_part','carrier_cause','carrier_coverage_type','carrier_nature','carrier_injury_code','carrier_policy_number','source_table','uploaded_at']]

def normal_1000002():
    ### RAW DATA ###
    source_table = 'raw_1000002'
    raw = raw_entity_df(source_table,SCHEMA)
    ### TRANSFORMATIONS ###
    for k,v in {
        'claim_number':'1000002-' + raw['Claim Number'],
        'subrogation':'-' + raw['Collection'],
        'loss_date':raw['Date of Loss'].apply(add_days),
        'carrier_cause':'1000002-' + raw['Description'],
        'carrier_coverage_type':'1000002 - ' + raw['Type'],
        'carrier_policy_number':'1000002 - ' + raw[['Dept.','LOB','policy_year']].apply(lambda x: ' - '.join(x),axis=1),
        'source_table':source_table
    }.items(): raw[k]=v
    ### PARSE FULL NAME TO PARTS ###
    raw[['claimant_first','claimant_middle','claimant_last']] = raw['Claimant'].apply(name_split,args=(1,))
    ### NORMALIZE RELEVANT COLUMN NAMES,RETURN IN PREFERRED ORDER ###
    return (
        raw.drop_duplicates()
            .rename({'Claim Number':'occurrence_number','Paid':'paid_indemnity','Reserves':'reserve_indemnity','Description':'description_text','Claimant':'claimant_name','Type':'carrier_injury_code'},axis=1)
    )[['claim_number','occurrence_number','paid_indemnity','subrogation','description_text','claimant_name','claimant_first','claimant_middle','claimant_last','loss_date','carrier_cause','carrier_coverage_type','carrier_injury_code','carrier_policy_number','source_table','uploaded_at']]

def normal_1000005():
    ### RAW DATA, FILTERED BY CLAIM NUMBER ###
    source_table = 'raw_1000005'
    raw = raw_entity_df(source_table,SCHEMA)
    raw = raw[~raw['Claim_#'].str.startswith(('OD','MV','MT','MR'))]
    ### TRANSFORMATIONS ###
    for k,v in {
        'claim_number':'1000005-' + raw['Claim_#'],
        'paid_expense':raw[['Medical_Surplus_Cost','Indemnity_Surplus_Cost']].apply(lambda x: sum_money([x['Medical_Surplus_Cost'],x['Indemnity_Surplus_Cost']]),axis=1),
        'claim_status':np.where(pd.to_numeric(raw['MIRA_Reserves_Risk'].apply(strip_money)) > 0, 'Open', 'Closed'),
        'claim_state':'OH',
        'benefit_state':'OH',
        'loss_date':pd.to_datetime('1/1/' + raw['Claim_#'].str[:2]),
        'litigation':np.where(raw['Appealed_to_IC/Court']=='N','No','Yes'),
        'carrier_coverage_type':'1000005 - WC',
        'carrier_policy_number':'1000005 - ' + raw.filename + ' - WC - ' + raw["Claim_#"].str[:2],
        'source_table':source_table
    }.items(): raw[k]=v
    ### STRIP MONETARY NOTATION ###
    for k,v in {'Indemnity_Risk_Cost':'paid_indemnity','Medical_Risk_Cost':'paid_medical','MIRA_Indemnity_Reserves_Cost':'reserve_indemnity','MIRA_Medical_Reserves_Cost':'reserve_medical','Subrogation_Amount':'subrogation'}.items():
        raw[v] = raw[k].apply(strip_money)
    ### NORMALIZE RELEVANT COLUMN NAMES,RETURN IN PREFERRED ORDER ###
    return (
        raw.drop_duplicates()
            .rename({'MIRA_Reserves_Surplus':'reserve_expense','MIRA_Reserve_Code':'carrier_injury_code'},axis=1)
    )[['claim_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','claim_status','benefit_state','loss_date','litigation','carrier_coverage_type','carrier_injury_code','carrier_policy_number','source_table','uploaded_at']]

def normal_1000006(quincy=False,redgold=False):
    ### RAW DATA ###
    entity = 'raw_1000006' + ('_quincy' if quincy else '_redgold' if redgold else '') 
    raw = raw_entity_df(entity,SCHEMA)
    ### JOIN STATES REFERENCE FOR STATE CODE ###
    raw = pd.merge(raw,raw_entity_df('states',SCHEMA,cols=['name','code']).rename({'name':'Jurisdiction'},axis=1),how='left',on='Jurisdiction')
    ### GENERAL TRANSFORMATIONS ###
    for k,v in {
        'claim_number':entity + '-' + raw['Claim Number'],
        'occurrence_number':raw['Claim Number'].str.split('-').str[0],
        'claimant_number':raw['Claim Number'].str.split('-').str[1],
        'claim_status': raw['Claim Status'] if quincy else np.where(raw['Claim Status'].str.contains('incident',case=False),'Closed',raw['Claim Status'].str.split('-').str[0]),
        'claim_city':raw['Hierarchy Location Level 4'] if quincy else np.nan,
        'claim_state':raw.code,
        'claim_site':(raw[['Hierarchy Location Level 3','Hierarchy Location Level 4']].apply(lambda x: ' - '.join(x.fillna('')),axis=1)).replace(' - ',np.nan) if quincy else raw['Hierarchy Location Level 3'] if redgold else np.nan,
        'benefit_state':raw.code,
        'garage_state':raw.code,
        'closed_date':np.where(raw['Date Closed']=='-',np.nan,raw['Date Closed']),
        'days_employed':raw[['Date of Loss','Date Hired']].apply(lambda x: days_employed('1000006',lost=x['Date of Loss'],hired=x['Date Hired']) if x['Date Hired']!='-' else np.nan,axis=1),
        'claimant_gender':np.where(raw['Claimant Gender']=='Unknown',None,raw['Claimant Gender']),
        'claimant_age':raw[['Date of Loss','Date Claimant Born']].apply(lambda x: claimant_age(x['Date of Loss'],x['Date Claimant Born']) if x['Date Claimant Born']!='-' else np.nan, axis=1),
        'carrier_coverage_type':entity + ' - ' + raw['Coverage Minor'],
        'carrier_injury_code':raw['Coverage Minor'].str.split(' - ').str[1],
        'carrier_policy_number':entity.lstrip('raw_') + ' - ' + raw[['Hierarchy Location Level 2','Coverage Major','Policy Year']].apply(lambda x: ' - '.join(x),axis=1),
        'source_table':entity
    }.items(): 
        raw[k]=v
    ### MONETARY TRANSFORMATIONS ###
    for k,v in {
        'paid_expense':['Expense Paid','Legal Paid'],
        'reserve_expense':['Expense Balance','Legal Balance']
    }.items():
        raw[k] = raw[v].apply(lambda x: sum_money([x[0],x[1]]), axis=1)
    for k,v in {'Indemnity Paid':'paid_indemnity','Medical Paid':'paid_medical','Indemnity Balance':'reserve_indemnity','Medical Balance':'reserve_medical','Total Recovery':'subrogation'}.items():
        raw[v] = raw[k].apply(strip_money)
    ### PARSE FULL NAME TO PARTS ###
    raw[['claimant_first','claimant_last']] = raw['Claimant Name'].apply(name_split,args=(2,))
    ### NORMALIZE RELEVANT COLUMN NAMES, RETURN IN PREFERRED ORDER ###
    return (
        raw.drop_duplicates()
            .rename({'Accident Description':'description_text','Occupation':'occupation','Claimant Name':'claimant_name','Injury':'carrier_nature','Date of Loss':'loss_date','Date Reported to TPA':'report_date','Body Part':'carrier_body_part','Cause':'carrier_cause'},axis=1)
    )[['claim_number','occurrence_number','claimant_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','claim_status','description_text','claim_city','claim_state','claim_site','benefit_state','garage_state','occupation','claimant_name','claimant_first','claimant_last','loss_date','report_date','closed_date','days_employed','claimant_gender','claimant_age','carrier_body_part','carrier_cause','carrier_coverage_type','carrier_nature','carrier_injury_code','carrier_policy_number','source_table','uploaded_at']]

def normal_acuity(typ):
    """ typ: cau, gli, wco """
    entity = f'acuity_{typ}'
    source_table = f'raw_{entity}'
    ### RAW DATA ###
    raw = raw_entity_df(source_table,SCHEMA)
    ### TRANSFORMATIONS ###
    for k,v in {
        'claim_number':f'{entity.upper()}-' + raw['Claim_Nbr'],
        'claim_status':raw['Claim_Status'].str.title() if typ in ('cau','gli') else np.where(raw['Claim_Status'].str.contains('|'.join(['closed','rec only']),case=False),'Closed',raw['Claim_Status']),
        'claim_address_1':raw['Address'] if typ in ('cau','gli') else None,
        'claim_city':raw['City'] if typ in ('cau','gli') else None,
        'benefit_state':raw['State'],
        'garage_state':raw['State'] if typ=='cau' else None,
        'occupation':raw['Occupation'] if typ=='wco' else None,
        'claimant_name':raw['Name_of_Driver'] if typ=='cau' else raw['Clmt_Name'],
        'days_employed':raw['Employed'].apply(lambda x: days_employed('acuity',employed=x)) if typ=='wco' else np.nan,
        'claimant_gender':np.where(raw['Sex']=='F','Female',np.where(raw['Sex']=='M','Male',None)),
        'carrier_body_part':raw['Body_Part'] if typ=='wco' else None,
        'carrier_cause':f'ACUITY_CAU - ' + raw['Loss_Desc'] if typ=='cau' else raw[['Acc_Type','Cause_Type']].apply(lambda x: ' - '.join(x),axis=1) if typ=='WCO' else None,
        'carrier_coverage_type':f'{entity.upper()} - ' + raw.apply(lambda x: carrier_coverage_type(typ,x), axis=1) if typ in ('cau','gli') else f'{entity.upper()}-{typ.upper()}',
        'carrier_nature':raw['Nature_of_Inj'] if typ=='wco' else None,
        'carrier_policy_number':f'{entity.upper()} - ' + raw[['Insured','Pol_Eff_Date_Year']].apply(lambda x: ' - '.join(x),axis=1) + f' - {typ.upper()}',
        'source_table':source_table
    }.items(): 
        raw[k]=v
    ### TRANSFORM MONETARY TYPES ###
    for col,tran in {
        'paid_expense':{'gli':'Personal_Injury_Paid','wco':'Expense_Paid'},
        'reserve_expense':{'gli':'Personal_Injury_Reserve','wco':'Expense_Reserve'},
        'paid_indemnity':{'gli':'Premises_Oper_Paid','wco':'Indemnity_Paid','cau':['Total_Paid','Unsdundrsd_Mot_Paid']},
        'paid_medical':{'gli':'Medical_Payment_Paid','wco':'Medical_Paid'},
        'reserve_indemnity':{'gli':'Premises_Oper_Reserve','wco':'Indemnity_Reserve','cau':['Total_Reserve','Unsdundrsd_Mot_Reserve']},
        'reserve_medical':{'gli':'Medical_Payment_Reserve','wco':'Medical_Reserve'},
        'subrogation':{'cau':'Recover_Physical_Damage_Incurred','wco':'Other_Recovery_Paid'}
    }.items():
        if tran.get(typ) and isinstance(tran.get(typ),list):
            raw[col] = raw[tran.get(typ)].fillna('').apply(lambda x: sum_money([x[0],x[1]]),axis=1)
        else:
            raw[col] = raw[tran.get(typ)].apply(strip_money) if tran.get(typ) is not None else 0
    ### PARSE FULL NAME TO PARTS ###
    raw[['claimant_first','claimant_middle','claimant_last']] = raw['Name_of_Driver' if typ=='cau' else 'Clmt_Name'].apply(name_split,args=(3,))
    ### DRIVER IS COPY OF CLAIMANT ###
    raw[['driver_first','driver_middle','driver_last']] = raw[['claimant_first','claimant_middle','claimant_last']] if typ=='cau' else pd.Series([None,None,None])
    ### NORMALIZE RELEVANT COLUMN NAMES, RETURN IN PREFERRED ORDER ###
    return (
        raw.drop_duplicates()
            .rename({'Claim_Nbr':'occurrence_number','Loss_Desc':'description_text','State':'claim_state','Location':'claim_site','Loss_Date':'loss_date','Loss_Time':'loss_time','Received_Date':'report_date','Age':'claimant_age'},axis=1)
    )[['claim_number','occurrence_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','claim_status','description_text','claim_address_1','claim_city','claim_state','claim_site','benefit_state','garage_state','occupation','claimant_name','claimant_first','claimant_middle','claimant_last','driver_first','driver_middle','driver_last','loss_date','loss_time','report_date','days_employed','claimant_gender','claimant_age','carrier_body_part','carrier_cause','carrier_coverage_type','carrier_nature','carrier_policy_number','source_table','uploaded_at']]

def normal_ahict():
    entity = 'ahict'
    source_table = f'raw_{entity}'
    raw = raw_entity_df(source_table,SCHEMA)
    for k,v in {
        'claim_number':f"{entity.upper()}-" + raw['CLAIM_NBR'],
        'claim_status':raw['CLAIM_STATUS'].str.title(),
        'benefit_state':raw['INSURED_STATE'],
        'garage_state':raw['INSURED_STATE'],
        'litigation':np.where(raw['IS_LITIGATION']=='Y','Yes',np.where(raw['IS_LITIGATION']=='N','No',None)),
        'carrier_coverage_type':f'{entity.upper()} - ' + raw[['LOB','COVERAGE_CODE','POLICY_NBR']].apply(lambda x: ' - '.join(x),axis=1),
        'source_table':source_table
    }.items():
        raw[k]=v
    ### TRANSFORM MONETARY TYPES ###
    for k,v in {'EXP_PAID':'paid_expense','EXP_RESERVE':'reserve_expense','IND_PAID':'paid_indemnity','MED_PAID':'paid_medical','IND_RESERVE':'reserve_indemnity','MED_RESERVE':'reserve_medical','SUBRO_REC':'subrogation','SALVAGE_REC':'reimbursed'}.items():
        raw[v] = raw[k].apply(strip_money)
    ### PARSE FULL NAME TO PARTS ###
    raw[['claimant_first','claimant_last']] = raw['RESERVE_CLAIMANT_NAME'].apply(name_split,args=(4,))
    raw[['driver_first','driver_middle','driver_last']] = raw['DRIVER'].apply(name_split,args=(5,))
    return (
        raw.drop_duplicates()
            .rename({'CLAIM_NBR':'occurrence_number','ACC_DESC':'description_text','ACC_CITY':'claim_city','ACC_STATE':'claim_state','ACC_LOCATION':'claim_site','RESERVE_CLAIMANT_NAME':'claimant_name','ACC_DATE':'loss_date','DT_REPORTED':'report_date','DT_CLOSED':'closed_date','DT_REOPEN':'reopen_date','ACC_CODE':'carrier_cause'},axis=1)
    )[['claim_number','occurrence_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','reimbursed','claim_status','description_text','claim_city','claim_state','claim_site','benefit_state','garage_state','claimant_name','claimant_first','claimant_last','driver_first','driver_middle','driver_last','loss_date','report_date','closed_date','reopen_date','litigation','carrier_cause','carrier_coverage_type','source_table','uploaded_at']]

def main():
    print(f'start: {dt.datetime.now()}')
    all_claims = pd.concat([
        pd.DataFrame(columns=['claim_number','occurrence_number','claimant_number','paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','reimbursed','claim_status','description_text','claim_address_1','claim_address_2','claim_city','claim_state','claim_zip','claim_site','benefit_state','garage_state','occupation','class_code','claimant_name','claimant_first','claimant_middle','claimant_last','driver_first','driver_middle','driver_last','loss_date','loss_time','report_date','closed_date','reopen_date','litigation','days_employed','claimant_gender','claimant_age','claimant_shift','claimant_supervisor','carrier_body_part','carrier_cause','carrier_coverage_type','carrier_nature','carrier_injury_code','carrier_policy_number','source_table','uploaded_at']),
        normal_1000001(),
        normal_1000002(),
        normal_1000005(),
        normal_1000006(),
        normal_1000006(quincy=True),
        normal_1000006(redgold=True),
        normal_acuity('cau'),
        normal_acuity('gli'),
        normal_acuity('wco'),
        normal_ahict()
    ]).replace('',np.nan)
    for num in ('paid_expense','reserve_expense','paid_indemnity','paid_medical','reserve_indemnity','reserve_medical','subrogation','reimbursed'):
        all_claims[num] = pd.to_numeric(all_claims[num].replace(np.nan,0.00)) ### after everything is complied, does this numeric conversion need to be explicit?
    print(f"{all_claims.to_sql('poc_all_claims_monthly',ENGINE,'p_and_c','replace',chunksize=10000,method='multi')} row(s) affected")
    print(f'complete: {dt.datetime.now()}')

if __name__ == '__main__':
    main()
    # one = normal_1000001()
    # two = normal_1000002()
    # five = normal_1000005()
    # six = normal_1000006()
    # quincy = normal_1000006(quincy=True)
    # redgold = normal_1000006(redgold=True)
    # cau = normal_acuity('cau')
    # gli = normal_acuity('gli')
    # wco = normal_acuity('wco')
    # ahict = normal_ahict()
    pass
