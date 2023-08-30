import mjdb
import config
import common as cmn
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'phones')

def raw_entity_df(table, schema):
    # read source table (limit to 10k records per pass), combine to dataframe
    entities = [chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]
    return pd.concat(entities)

def sg_client_phone_df(lastUpdate):
    phones = raw_entity_df('clients', 'sagitta')[['sagitem','audit_entry_dt','audit_time','phone_1_number','phone_1_extension_number','phone_2_number','phone_2_extension_number']]
    phones['modify_dt'] = phones.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    phones = (phones[phones.modify_dt >= lastUpdate]).drop(['audit_entry_dt','audit_time'],axis=1)
    phoneOne = phones.dropna(subset=['phone_1_number'])[['sagitem','modify_dt','phone_1_number','phone_1_extension_number']]
    phoneOne['phone'] = ((phoneOne[['phone_1_number','phone_1_extension_number']].fillna('')).agg(lambda x: ' X. '.join(x.values), axis=1)).str.rstrip(' X. ')
    phoneOne.drop(['phone_1_number','phone_1_extension_number'],axis=1,inplace=True)
    phoneOne['source_phone_type'] = 'CLIENT-1'
    phoneTwo = phones.dropna(subset=['phone_2_number'])[['sagitem','modify_dt','phone_2_number','phone_2_extension_number']]
    phoneTwo['phone'] = ((phoneTwo[['phone_2_number','phone_2_extension_number']].fillna('')).agg(lambda x: ' X. '.join(x.values), axis=1)).str.rstrip(' X. ')
    phoneTwo.drop(['phone_2_number','phone_2_extension_number'],axis=1,inplace=True)
    phoneTwo['source_phone_type'] = 'CLIENT-2'
    sgPhones = pd.concat([phoneOne,phoneTwo]).rename(columns={'sagitem':'source_key'})
    statics = {'phone_source':'SAGITTA','source_type':'CLIENT','phone_type':'Business'}
    for s in statics:
        sgPhones[s] = statics[s]
    sgPhones = sgPhones.sort_values(by=['modify_dt'],ascending=False)
    return sgPhones.drop_duplicates(['source_key','phone'],keep='first')

def bp_account_phone_df(lastUpdate):
    # phones don't exist at the account level for Group accounts
    phones = raw_entity_df('phone', 'benefitpoint')
    phones = phones[(phones.phone_source=='ACCOUNT') & (phones.type!='Fax')]
    return phones

def sg_contact_phone_df(lastUpdate):
    allPhones = raw_entity_df('contacts_phone_group', 'sagitta').rename(columns={'type':'phone_type'})
    # join with contacts for audit_info, contact audit date/time to timestamp, filter by lastUpdate
    allPhones = allPhones.merge(raw_entity_df('contacts','sagitta')[['sagitem','audit_entry_dt','audit_time']],how='inner',on='sagitem')
    allPhones['modify_dt'] = allPhones.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    allPhones = allPhones[allPhones.modify_dt >= lastUpdate].drop(['audit_entry_dt','audit_time'],axis=1)
    # concat sagitem & lis to source_key, drop irrelevant columns
    allPhones['source_key'] = allPhones[['sagitem','lis']].apply(lambda x: '-'.join(x.astype(str)),axis=1)
    allPhones.drop(['sagitem','lis'],axis=1,inplace=True)
    # concat phone & extension to phone, drop irrelevant columns
    allPhones['phone'] = ((allPhones[['phone_number','extension']].fillna('')).agg(lambda x: ' X. '.join(x.values),axis=1)).str.rstrip(' X. ')
    allPhones.drop(['phone_number','extension'],axis=1,inplace=True)
    # break out phones by type, remove NULLs, concat relevant data back to final dataframe
    bp = allPhones[allPhones.phone_type=='Business'][['source_key','phone_type','phone','modify_dt']].replace('',np.nan).dropna(subset=['phone'])
    bp['source_phone_type'] = 'Business-Phone'
    bm = allPhones[allPhones.phone_type=='Business'][['source_key','phone_type','mobile_number','modify_dt']].replace('',np.nan).dropna(subset=['mobile_number']).rename(columns={'mobile_number':'phone'})
    bm['source_phone_type'] = 'Business-Mobile'
    hp = allPhones[allPhones.phone_type=='Home'][['source_key','phone_type','phone','modify_dt']].replace('',np.nan).dropna(subset=['phone'])
    hp['source_phone_type'] = 'Home-Phone'
    hm = allPhones[allPhones.phone_type=='Home'][['source_key','phone_type','mobile_number','modify_dt']].replace('',np.nan).dropna(subset=['mobile_number']).rename(columns={'mobile_number':'phone'})
    hm['source_phone_type'] = 'Home-Mobile'
    op = allPhones[allPhones.phone_type=='Other'][['source_key','phone_type','phone','modify_dt']].replace('',np.nan).replace('',np.nan).dropna(subset=['phone'])
    op['source_phone_type'] = 'Other-Phone'
    om = allPhones[allPhones.phone_type=='Other'][['source_key','phone_type','mobile_number','modify_dt']].replace('',np.nan).dropna(subset=['mobile_number']).rename(columns={'mobile_number':'phone'})
    om['source_phone_type'] = 'Other-Mobile'
    phones = [bp,bm,hp,hm,op,om]
    phonesDf = pd.concat(phones)
    statics = {'phone_source':'SAGITTA','source_type':'CONTACT'}
    for s in statics:
        phonesDf[s] = statics[s]
    phonesDf = phonesDf.sort_values(by=['modify_dt'],ascending=False)
    return phonesDf.drop_duplicates(['source_key','phone_type','phone'],keep='first')

def bp_contact_phone_df(lastUpdate):
    phones = raw_entity_df('phone', 'benefitpoint').rename(columns={'type':'phone_type'})
    phones = phones.merge(raw_entity_df('account_contact','benefitpoint')[['contact_id','last_modified_on']].rename(columns={'contact_id':'source_key','last_modified_on':'modify_dt'}),how='inner',on='source_key')
    phones = phones[(pd.to_datetime(phones.modify_dt).dt.tz_localize(None) >= lastUpdate) & (phones.phone_source=='CONTACT') & (phones.phone_type!='Fax')].drop(['phone_source','source_type'],axis=1)
    phones['phone'] = (phones[['area_code','number']].fillna('')).agg(lambda x: ''.join(x.values),axis=1) ### .str.replace(r'[^0-9]+','') ### strips relevant data (e.g. ext.)
    phones.drop(['area_code','number'],axis=1,inplace=True)
    phones['source_phone_type'] = phones.phone_type
    statics = {'phone_source':'BENEFITPOINT','source_type':'CONTACT'}
    for s in statics:
        phones[s] = statics[s]
    phones = phones.sort_values(by=['modify_dt'],ascending=False)
    return phones.drop_duplicates(['source_key','phone_type','phone'],keep='first')

def account_phone_df(lastUpdate):
    account = raw_entity_df('account', 'powerapps')[['account_source','source_key','guid']].rename(columns={'account_source':'source','guid':'account_guid'})
    phone = raw_entity_df('phone', 'powerapps')[['phone_source','source_type','source_key','phone_type','guid','modify_dt']].rename(columns={'phone_source':'source','guid':'phone_guid'})
    phone = phone[((pd.to_datetime(phone.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (phone.source_type.isin(['ACCOUNT','CLIENT']))]
    return phone.merge(account, how='inner', on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def contact_phone_df(lastUpdate):
    contact = raw_entity_df('vw_master_contacts', 'powerapps')[['contact_source','source_key','guid']].rename(columns={'contact_source':'source','guid':'contact_guid'})
    phone = raw_entity_df('phone', 'powerapps')[['phone_source','source_type','source_key','phone_type','guid','modify_dt']].rename(columns={'phone_source':'source','guid':'phone_guid'})
    phone = phone[(pd.to_datetime(phone.modify_dt).dt.tz_localize(None) >= lastUpdate) & (phone.source_type=='CONTACT')]
    phone['source_key'] = phone.source_key.str.split('-').str[0]
    return phone.merge(contact, how='inner', on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def main():
    # STAGE & UPSERT PHONES
    try:
        phones = pd.concat([
            sg_client_phone_df(mjdb.entity_last_update('powerapps', 'phone', ('SAGITTA','CLIENT'))),
            sg_contact_phone_df(mjdb.entity_last_update('powerapps', 'phone', ('SAGITTA','CONTACT'))),
            bp_contact_phone_df(mjdb.entity_last_update('powerapps', 'phone', ('BENEFITPOINT','CONTACT')))
        ])
        rcs = phones.to_sql('stg_phone', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for phone\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} record(s) staged for phone")
            try:
                rcu = mjdb.upsert_stage('powerapps','phone', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps','phone')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps','phone') affected {rcu} record(s)")
    finally:
        mjdb.drop_table('powerapps', 'stg_phone')
    # STAGE & UPSERT ACCOUNT_PHONE RELATIONSHIPS
    try:
        apLastUpdate = mjdb.entity_last_update('powerapps', 'account_phone')
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'account_phone')\n{e}")
    else:
        try:
            aprcs = account_phone_df(apLastUpdate).to_sql('stg_account_phone',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for account_phone")
        else:
            if aprcs > 0:
                lf.info(f"{aprcs} record(s) staged for account_phone")
                try:
                    aprcu = mjdb.upsert_stage('powerapps', 'account_phone', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps', 'account_phone')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('powerapps', 'account_phone') affected {aprcu} record(s)")
        finally:
            mjdb.drop_table('powerapps','stg_account_phone')
    # STAGE & UPSERT CONTACT_PHONE RELATIONSHIPS
    try:
        cpLastUpdate = mjdb.entity_last_update('powerapps', 'contact_phone')
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'contact_phone')\n{e}")
    else:
        try:
            cprcs = contact_phone_df(cpLastUpdate).to_sql('stg_contact_phone',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for contact_phone")
        else:
            if cprcs > 0:
                lf.info(f"{cprcs} record(s) staged for contact_phone")
                try:
                    cprcu = mjdb.upsert_stage('powerapps', 'contact_phone', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps', 'contact_phone')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('powerapps', 'contact_phone') affected {cprcu} record(s)")
        finally:
            mjdb.drop_table('powerapps','stg_contact_phone')
if __name__ == '__main__':
    main()