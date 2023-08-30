import mjdb
import config
import common as cmn
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'addresses')

def raw_entity_df(table, schema):
    # read raw table (limit to 10k records per pass), combine to dataframe
    entities = [chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]
    return pd.concat(entities)

def sg_client_address_df(lastUpdate):
    addresses = raw_entity_df('clients','sagitta')[['sagitem','audit_entry_dt','audit_time','addr_1','addr_2','city','postal_code','postal_extension_code','state_prov_cd']].dropna(subset=['addr_1'])
    addresses = addresses[addresses.addr_1 != 'DIP']  # THIS SHOULD NOT BE A PERMENANT FIX -- BAD DATA SHOULD BE CLEANED UP RATHER THAN COVERED UP
    addresses['modify_dt'] = addresses.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row['audit_entry_dt'])), (dt.datetime.min + dt.timedelta(seconds=int(row['audit_time']))).time()), axis=1)
    addresses = (addresses[addresses.modify_dt >= lastUpdate]).drop(['audit_entry_dt','audit_time'],axis=1).rename(columns={'state_prov_cd':'iso_2'})
    addresses = addresses.merge(raw_entity_df('iso_state_province','public')[['iso_2','state_province']],how='left',on='iso_2')
    addresses['zip_post_code'] = ((addresses[['postal_code','postal_extension_code']].fillna('')).agg(lambda x: '-'.join(x.values), axis=1)).str.rstrip('-')
    addresses.drop(['postal_code','postal_extension_code','iso_2'],axis=1,inplace=True)
    statics = {'address_source':'SAGITTA','source_type':'CLIENT','source_address_type':'CLIENT','address_type':'Business'}
    for x in statics:
        addresses[x] = statics[x]
    addresses['country_region'] = None
    return addresses.rename(columns={'sagitem':'source_key','addr_1':'street_1','addr_2':'street_2'})

def sg_contact_address_df(lastUpdate):
    addresses = raw_entity_df('contacts_address_group', 'sagitta')[['sagitem','lis','type','address','address_2','city','state','zip','zip_ext','country']].dropna(subset=['address']).rename(columns={'type':'address_type','address':'street_1','address_2':'street_2','state':'iso_2','country':'country_region'})
    addresses['source_address_type'] = addresses.address_type
    # merge to contact for audit info, combine to timestamp (modify_dt)
    addresses = addresses.merge(raw_entity_df('contacts','sagitta')[['sagitem','audit_entry_dt','audit_time']],how='inner',on='sagitem')
    addresses['modify_dt'] = addresses.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row.audit_entry_dt)), (dt.datetime.min + dt.timedelta(seconds=int(row.audit_time))).time()), axis=1)
    addresses['source_key'] = addresses[['sagitem','lis']].apply(lambda x: '-'.join(x.astype(str)), axis=1)
    addresses['zip_post_code'] = ((addresses[['zip','zip_ext']].fillna('')).agg(lambda x: '-'.join(x.values), axis=1)).str.rstrip('-')
    addresses.drop(['zip','zip_ext'],axis=1,inplace=True)
    addresses = (addresses.merge(raw_entity_df('iso_state_province','public')[['iso_2','state_province']],how='left',on='iso_2')).drop(['sagitem','lis','audit_entry_dt','audit_time','iso_2'],axis=1)
    statics = {'address_source':'SAGITTA','source_type':'CONTACT'}
    for s in statics:
        addresses[s] = statics[s]
    return addresses

def bp_account_address_df(lastUpdate):  ### addresses don't exists at account level for group accounts -- pull in related location addresses?
    accounts = (raw_entity_df('account', 'benefitpoint')[['account_id','account_classification','last_modified_on']]).rename(columns={'account_id':'source_key','last_modified_on':'modify_dt'})
    accounts = accounts[(pd.to_datetime(accounts.modify_dt).dt.tz_localize(None) >= lastUpdate) & (accounts.account_classification=='Group')]
    addresses = accounts.drop(['account_classification'],axis=1).merge(raw_entity_df('address','benefitpoint').dropna(subset=['street_1']),how='inner',on='source_key').rename(columns={'address_source':'source_type','source_type':'address_type','state':'state_province','zip':'zip_post_code','country':'country_region'})
    addresses = addresses[(addresses.source_type=='ACCOUNT') & (addresses.address_type!='LOCATION')]
    addresses['source_address_type'] = addresses.address_type
    addresses['address_type'] = addresses['address_type'].map(lambda x: x.title())
    addresses['address_source'] = 'BENEFITPOINT'
    return addresses

def bp_contact_address_df(lastUpdate):
    addresses = raw_entity_df('address', 'benefitpoint').dropna(subset=['street_1'])[['address_source','source_key','source_type','street_1','street_2','city','state','zip','country']].rename(columns={'address_source':'source_type','source_type':'source_address_type','state':'state_province','zip':'zip_post_code','country':'country_region'})
    addresses = addresses[addresses.source_type=='CONTACT']
    addresses = addresses.merge(raw_entity_df('account_contact', 'benefitpoint')[['contact_id','last_modified_on']].rename(columns={'contact_id':'source_key','last_modified_on':'modify_dt'}),how='inner',on='source_key')
    statics = {'address_source':'BENEFITPOINT','address_type':'Business'}
    for s in statics:
        addresses[s] = statics[s]
    return addresses

def account_address_df(lastUpdate):
    account = raw_entity_df('account', 'powerapps')[['account_source','source_key','guid']].rename(columns={'account_source':'source','guid':'account_guid'})
    address = raw_entity_df('address', 'powerapps')[['address_source','source_type','source_key','address_type','guid','modify_dt']].rename(columns={'address_source':'source','guid':'address_guid'})
    address = address[((pd.to_datetime(address.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (address.source_type.isin(['ACCOUNT','CLIENT']))]
    return address.merge(account, how='inner', on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def contact_address_df(lastUpdate):
    contact = raw_entity_df('vw_master_contacts', 'powerapps')[['contact_source','source_key','guid']].rename(columns={'contact_source':'source','guid':'contact_guid'})
    address = raw_entity_df('address', 'powerapps')[['address_source','source_type','source_key','address_type','guid','modify_dt']].rename(columns={'address_source':'source','guid':'address_guid'})
    address = address[((pd.to_datetime(address.modify_dt).dt.tz_localize(None) >= lastUpdate)) & (address.source_type=='CONTACT')]
    # remove lis key from sagitta contact emails for matching
    address['source_key'] = address.source_key.str.split('-').str[0]
    return address.merge(contact,how='inner',on=['source','source_key']).drop(['source','source_type','source_key'],axis=1)

def main():
    try:
        # consolidate address dataframes, stage in DB
        dfs = [
            sg_client_address_df(mjdb.entity_last_update('powerapps', 'address', ('SAGITTA', 'CLIENT'))),
            bp_account_address_df(mjdb.entity_last_update('powerapps', 'address', ('BENEFITPOINT', 'ACCOUNT'))),
            sg_contact_address_df(mjdb.entity_last_update('powerapps', 'address', ('SAGITTA', 'CONTACT'))),
            bp_contact_address_df(mjdb.entity_last_update('powerapps', 'address', ('BENEFITPOINT', 'CONTACT')))
        ]
        addresses = pd.concat(dfs)
        rcs = addresses.to_sql('stg_address', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for address\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} records staged for address")
            try:
                # upsert addresses from stage
                rcu = mjdb.upsert_stage('powerapps', 'address', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps', 'address')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('powerapps', 'address') affected {rcu} row(s).")
    finally:
        mjdb.drop_table('powerapps','stg_address')
    try:
        aaLastUpdate = mjdb.entity_last_update('powerapps', 'account_address')
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'account_address')\n{e}")
    else:
        try:
            # stage account_address records
            aarcs = account_address_df(aaLastUpdate).to_sql('stg_account_address',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for account_address\n{e}")
        else:
            if aarcs > 0:
                lf.info(f"{aarcs} record(s) staged for account_address")
                try:
                    # upsert account_address records from stage
                    aarcu = mjdb.upsert_stage('powerapps', 'account_address', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps', 'account_address')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('powerapps', 'account_address') affected {aarcu} row(s)")
    finally:
        mjdb.drop_table('powerapps','stg_account_address')

    try:
        caLastUpdate = mjdb.entity_last_update('powerapps', 'contact_address')
    except Exception as e:
        lf.error(f"mjdb.entity_last_update('powerapps', 'contact_address')\n{e}")
    else:
        try:
            # stage account_address records
            carcs = contact_address_df(caLastUpdate).to_sql('stg_contact_address',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for contact_address\n{e}")
        else:
            if aarcs > 0:
                lf.info(f"{aarcs} record(s) staged for contact_address")
                try:
                    # upsert account_address records from stage
                    carcu = mjdb.upsert_stage('powerapps', 'contact_address', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps', 'contact_address')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('powerapps', 'contact_address') affected {carcu} row(s)")
    finally:
        mjdb.drop_table('powerapps','stg_contact_address')

if __name__ == '__main__':
    main()