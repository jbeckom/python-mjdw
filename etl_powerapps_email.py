import mjdb
import config
import common as cmn
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'email')

def raw_entity_df(table, schema):
    # read sagitta clients (limit to 10k records per pass), combine to dataframe
    entities = [chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]
    return pd.concat(entities)

def sg_client_email_df(lastUpdate):
    emails = raw_entity_df('clients', 'sagitta')[['sagitem','audit_entry_dt','audit_time','email_addr']].dropna(subset=['email_addr'])
    emails['modify_dt'] = emails.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    emails = (emails[emails.modify_dt >= lastUpdate]).drop(['audit_entry_dt','audit_time'],axis=1)
    statics = {'email_source':'SAGITTA','source_type':'CLIENT','source_email_type':'CLIENT','email_type':'Business'}
    for s in statics:
        emails[s] = statics[s]
    emails.rename(columns={'sagitem':'source_key','email_addr':'email'},inplace=True)
    return emails

def sg_contact_email_df(lastUpdate):
    emails = raw_entity_df('contacts_email_group','sagitta')[['sagitem','lis','type','email_address']]
    # merge to contact for audit info, combine to timestamp (modify_dt)
    emails = emails.merge(raw_entity_df('contacts','sagitta')[['sagitem','audit_entry_dt','audit_time']],how='inner',on='sagitem')
    emails['modify_dt'] = emails.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    # filter by modify_dt
    emails = (emails[emails.modify_dt >= lastUpdate]).drop(['audit_entry_dt','audit_time'],axis=1)
    # concat sagitem and lis for source key, remove irrelevant columns
    emails['source_key'] = emails[['sagitem','lis']].apply(lambda x: '-'.join(x.astype(str)), axis=1)
    emails.drop(['sagitem','lis'],axis=1,inplace=True)
    emails['source_email_type'] = emails.type
    # add statics and NoneTypes
    statics = {'email_source':'SAGITTA','source_type':'CONTACT'}
    for s in statics:
        emails[s] = statics[s]
    return emails.rename(columns={'type':'email_type','email_address':'email'})

def bp_email_df(lastUpdate):
    # emails don't exist at account level for Group accounts
    emails = raw_entity_df('contact','benefitpoint')[['contact_source','source_key','contact_id','email']].dropna(subset=['email']).rename(columns={'contact_source':'source_email_type','source_key':'account_id'})
    # join to account_contact for last_modified_on/modify_dt (also acts as filter to return only account_contacts)
    emails = emails.merge(raw_entity_df('account_contact','benefitpoint')[['account_id','contact_id','last_modified_on']],how='inner',on=['account_id','contact_id']).rename(columns={'last_modified_on':'modify_dt','contact_id':'source_key'}).drop(['account_id'],axis=1)
    # emails['source_key'] = emails['source_key'].astype(str)
    statics = {'email_source':'BENFITPOINT','source_type':'CONTACT','email_type':'Business'}
    for s in statics:
        emails[s] = statics[s]
    return emails

def account_email_df(lastUpdate):
    account = raw_entity_df('account', 'powerapps')[['account_source','source_key','guid']].rename(columns={'account_source':'source','guid':'account_guid'})
    email = raw_entity_df('email','powerapps')[['email_source','source_type','source_key','email_type','guid','modify_dt','status']].rename(columns={'email_source':'source','guid':'email_guid'})
    email = email[((pd.to_datetime(email.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (email.source_type.isin(['ACCOUNT','CLIENT']))]
    return email.merge(account,how='inner',on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def contact_email_df(lastUpdate):
    contact = raw_entity_df('vw_master_contacts','powerapps')[['contact_source','source_key','guid']].rename(columns={'contact_source':'source','guid':'contact_guid'})
    email = raw_entity_df('email','powerapps')[['email_source','source_type','source_key','email_type','guid','modify_dt','status']].rename(columns={'email_source':'source','guid':'email_guid'})
    email = email[((pd.to_datetime(email.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (email.source_type=='CONTACT')]
    # remove lis key from sagitta contact emails for matching
    email['source_key'] = email.source_key.str.split('-').str[0]
    return email.merge(contact,how='inner',on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def main():
    try:
        sgClientLastUpdate = mjdb.entity_last_update('powerapps', 'email', ('SAGITTA','CLIENT'))
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'email', ('SAGITTA','CLIENT'))\n{e}")
    try:
        sgContactLastUpdate = mjdb.entity_last_update('powerapps', 'email', ('SAGITTA','CONTACT'))
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'email', ('SAGITTA','CONTACT'))\n{e}")
    try:
        sgClientEmails = pd.DataFrame()
        sgClientEmails = sg_client_email_df(sgClientLastUpdate)
    except Exception as e:
        lf.error(f"sg_email_df({sgClientLastUpdate})\n{e}")
    try:
        bpLastUpdate = mjdb.entity_last_update('powerapps', 'email', ('BENEFITPOINT','CONTACT'))
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'email', ('BENEFITPOINT','CONTACT'))\n{e}")
    try:
        sgContactEmails = pd.DataFrame()
        sgContactEmails = sg_contact_email_df(sgContactLastUpdate)
    except Exception as e:
        lf.error(f"sg_contact_email_df(sgContactLastUpdate)\n{e}")
    try:
        bpEmails = pd.DataFrame()
        bpEmails = bp_email_df(bpLastUpdate)
    except Exception as e:
        lf.error(f"bp_email_df(bpLastUpdate)\n{e}")
    try:
        emails = pd.DataFrame()
        if not sgClientEmails.empty:
            emails = pd.concat([emails,sgClientEmails])
        if not sgContactEmails.empty:
            emails = pd.concat([emails,sgContactEmails])
        if not bpEmails.empty:
            emails = pd.concat([emails,bpEmails])
        rcs = emails.to_sql('stg_email',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for email\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} record(s) staged for email")
            try:
                rcu = mjdb.upsert_stage('powerapps','email', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps','email')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps','email') affected {rcu} record(s)")
    finally:
        mjdb.drop_table('powerapps', 'stg_email')
    # stage and upsert account_email relationships
    try:
        aercs = account_email_df(mjdb.entity_last_update('powerapps','account_email')).to_sql('stg_account_email',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for account_email")
    else:
        if aercs > 0:
            lf.info(f"{aercs} record(s) staged for account_email")
            try:
                aercu = mjdb.upsert_stage('powerapps', 'account_email', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps', 'account_email')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps', 'account_email') affected {aercu} record(s)")
    finally:
        mjdb.drop_table('powerapps','stg_account_email')
    # stage and upsert contact_email 
    try:
        cercs = contact_email_df(mjdb.entity_last_update('powerapps','contact_email')).to_sql('stg_contact_email',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for contact_email\n{e}")
    else:
        if cercs > 0:
            lf.info(f"{cercs} record(s) staged for contact_email")
            try:
                cercu = mjdb.upsert_stage('powerapps', 'contact_email', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps', 'contact_email')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps', 'contact_email') affected {cercu} record(s)")
    finally:
        mjdb.drop_table('powerapps','stg_contact_email')

if __name__ == '__main__':
    main()