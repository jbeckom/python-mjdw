import mjdb
import config
import common as cmn
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'faxes')

def raw_entity_df(table, schema):
    # read sagitta clients (limit to 10k records per pass), combine to dataframe
    entities = [chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]
    return pd.concat(entities)

def sg_client_fax_df(lastUpdate):
    faxes = raw_entity_df('clients', 'sagitta')[['sagitem','audit_entry_dt','audit_time','fax_number']].dropna(subset=['fax_number']).rename(columns={'sagitem':'source_key','fax_number':'fax'})
    faxes['modify_dt'] = faxes.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    faxes = (faxes[faxes.modify_dt >= lastUpdate]).drop(['audit_entry_dt','audit_time'],axis=1)
    statics = {'fax_source':'SAGITTA','source_type':'CLIENT','source_fax_type':'CLIENT','fax_type':'Business'}
    for s in statics:
        faxes[s] = statics[s]
    faxes = faxes.sort_values(by=['modify_dt'],ascending=False)
    return faxes.drop_duplicates(['source_key','fax'],keep='first')

def bp_account_fax_df(lastUpdate):
    # faxes don't exist at the account level for Group accounts
    return pd.DataFrame()

def sg_contact_fax_df(lastUpdate):
    faxes = raw_entity_df('contacts_phone_group', 'sagitta')[['sagitem','lis','type','fax']].dropna().rename(columns={'type':'fax_type'})
    # join with contacts for audit_info, contact audit date/time to timestamp, filter by lastUpdate
    faxes = faxes.merge(raw_entity_df('contacts', 'sagitta')[['sagitem','audit_entry_dt','audit_time']],how='inner',on='sagitem')
    faxes['modify_dt'] = faxes.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    faxes = faxes[faxes.modify_dt >= lastUpdate].drop(['audit_entry_dt','audit_time'],axis=1)
    # concat sagitem & lis to source_key, drop irrelevant columns
    faxes['source_key'] = faxes[['sagitem','lis']].apply(lambda x: '-'.join(x.astype(str)),axis=1)
    faxes['source_fax_type'] = faxes.fax_type
    faxes = (faxes.drop(['sagitem','lis'],axis=1)).sort_values(by=['modify_dt'],ascending=False)
    statics = {'fax_source':'SAGITTA','source_type':'CONTACT'}
    for s in statics:
        faxes[s] = statics[s]
    return faxes.drop_duplicates(['source_key','fax_type','fax'],keep='first')

def bp_contact_fax_df(lastUpdate):
    faxes = raw_entity_df('phone', 'benefitpoint').rename(columns={'source_type':'source_fax_type','type':'fax_type'})
    faxes = faxes[(faxes.fax_type=='Fax') & (faxes.phone_source=='CONTACT')].dropna(subset=['number']).drop(['phone_source','fax_type'],axis=1)
    faxes = faxes.merge(raw_entity_df('account_contact','benefitpoint')[['contact_id','last_modified_on']].rename(columns={'contact_id':'source_key','last_modified_on':'modify_dt'}),how='inner',on='source_key')
    faxes = faxes[pd.to_datetime(faxes.modify_dt).dt.tz_localize(None) >= lastUpdate]
    faxes['fax'] = (faxes[['area_code','number']].fillna('')).agg(lambda x: ''.join(x.values),axis=1)
    faxes = (faxes.drop(['area_code','number'],axis=1)).sort_values(by=['modify_dt'],ascending=False)
    statics = {'fax_source':'BENEFITPOINT','source_type':'CONTACT','fax_type':'Business'}
    for s in statics:
        faxes[s] = statics[s]
    return faxes.drop_duplicates(['source_key','fax_type','fax'],keep='first')

def account_fax_df(lastUpdate):
    account = raw_entity_df('account', 'powerapps')[['account_source','source_key','guid']].rename(columns={'account_source':'source','guid':'account_guid'})
    fax = raw_entity_df('fax','powerapps')[['fax_source','source_type','source_key','fax_type','guid','modify_dt']].rename(columns={'fax_source':'source','guid':'fax_guid'})
    fax = fax[((pd.to_datetime(fax.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (fax.source_type.isin(['ACCOUNT','CLIENT']))]
    return fax.merge(account, how='inner', on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def contact_fax_df(lastUpdate):
    contact = raw_entity_df('vw_master_contacts','powerapps')[['contact_source','source_key','guid']].rename(columns={'contact_source':'source','guid':'contact_guid'})
    fax = raw_entity_df('fax','powerapps')[['fax_source','source_type','source_key','fax_type','guid','modify_dt']].rename(columns={'fax_source':'source','guid':'fax_guid'})
    fax = fax[(pd.to_datetime(fax.modify_dt).dt.tz_localize(None) >= lastUpdate) & (fax.source_type=='CONTACT')]
    fax['source_key'] = fax.source_key.str.split('-').str[0]
    return fax.merge(contact, how='inner', on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def main():
    # STAGE & UPSERT FAXES
    try:
        faxes = pd.concat([
            sg_client_fax_df(mjdb.entity_last_update('powerapps', 'fax', ('SAGITTA','CLIENT'))),
            sg_contact_fax_df(mjdb.entity_last_update('powerapps', 'fax', ('SAGITTA','CONTACT'))),
            bp_contact_fax_df(mjdb.entity_last_update('powerapps', 'fax', ('BENEFITPOINT','CONTACT')))
        ])
        rcs = faxes.to_sql('stg_fax',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for fax\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} record(s) staged for fax")
            try:
                rcu = mjdb.upsert_stage('powerapps', 'fax', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps', 'fax')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps', 'fax') affected {rcu} record(s)")
    finally:
        mjdb.drop_table('powerapps', 'stg_fax')
    # STAGE & UPSERT ACCOUNT_FAX RELATIONSHIPS
    try:
        afLastUpdate = mjdb.entity_last_update('powerapps', 'account_fax')
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'account_fax')\n{e}")
    else:
        try:
            afrcs = account_fax_df(afLastUpdate).to_sql('stg_account_fax',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for account_fax")
        else:
            if afrcs > 0:
                lf.info(f"{afrcs} record(s) staged for account_fax")
                try:
                    afrcu = mjdb.upsert_stage('powerapps', 'account_fax', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps', 'account_fax')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('powerapps', 'account_fax') affected {afrcu} record(s)")
        finally:
            mjdb.drop_table('powerapps', 'stg_account_fax')
    # STAGE & UPSERT CONTACT_FAX RELATIONSHIPS
    try:
        cfrcs = contact_fax_df(mjdb.entity_last_update('powerapps','contact_fax')).to_sql('stg_contact_fax',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error("unable to stage records for contact_fax")
    else:
        if cfrcs > 0:
            lf.info(f"{cfrcs} record(s) staged for contact_fax")
            try:
                cfrcu = mjdb.upsert_stage('powerapps', 'contact_fax', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps', 'contact_fax')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps', 'contact_fax') affected {cfrcu} record(s)")
    finally:
        mjdb.drop_table('powerapps', 'stg_contact_fax')

if __name__ == '__main__':
    main()