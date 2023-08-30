import mjdb
import config
import re
import common as cmn
import numpy as np
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine

LOGDIR = 'etl_powerapps'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'contacts')

def raw_entity_df(table, schema):
    # read sagitta clients (limit to 10k records per pass), combine to dataframe
    entities = [chunk for chunk in pd.read_sql_table(table, ENGINE, schema, chunksize=10000)]
    return pd.concat(entities)

def parent_account_df(source):
    parents = raw_entity_df('account', 'powerapps')[['account_source','source_key','guid']].rename(columns={'guid':'parent_account_guid'})
    parents = parents[parents.account_source==source]
    parents['source_key'] = parents.source_key.astype('int64')
    if source == 'SAGITTA':
        # refactor df to assist in merge
        parents.rename(columns={'source_key':'client_id'},inplace=True)
    return parents.drop(['account_source'],axis=1)

def sg_contact_df(lastUpdate):
    # convert last update date to Sagitta compliant date delta, filter return
    delta  = (lastUpdate.date() - dt.date(1967,12,31)).days
    contacts = raw_entity_df('contacts', 'sagitta').query("audit_entry_dt >= @delta and given_name.notnull() and surname.notnull()")[['sagitem','audit_entry_dt','audit_time','given_name','surname','title','sex_cd','spouse','birth_dt','contact_type_cd','client_id']]
    if not contacts.empty:
        # filter service center contacts ### MOVE THIS FILTER TO INITIAL QUERY ###
        contacts = contacts[(~contacts.given_name.str.contains('servcenter',flags=re.IGNORECASE,na=False)) & (~contacts.surname.str.contains('service center',flags=re.IGNORECASE,na=False))]
        # convert date and time integers to timestamp for modify_dt, drop unnecessary columns
        contacts['modify_dt'] = contacts.apply(lambda row: dt.datetime.combine(dt.date(1967,12,31) + dt.timedelta(days=int(row['audit_entry_dt'])), (dt.datetime.min + dt.timedelta(seconds=int(row['audit_time']))).time()), axis=1)
        contacts.drop(['audit_entry_dt','audit_time'],axis=1,inplace=True)
        # switch on sex_cd for gender
        contacts['gender'] = contacts.apply(lambda row: 'Female' if row['sex_cd']=='F' else ('Male' if row['sex_cd']=='M' else None),axis=1)
        # join types to get description for type_cd
        contacts = (contacts.merge(raw_entity_df('types', 'sagitta')[['sagitem','type_description']].rename(columns={'sagitem':'contact_type_cd','type_description':'contact_type'}), how='left', on='contact_type_cd')).drop(['contact_type_cd'],axis=1)
        # join to accounts for parent_account_guid
        contacts = (contacts.merge(parent_account_df('SAGITTA'),how='inner',on='client_id'))
        # format birth_dt int to date string, ignoring NoneType
        contacts['dob'] = contacts.apply(lambda row: dt.datetime.strftime((dt.date(1967,12,31)+dt.timedelta(days=row['birth_dt'])),'%m/%d/%Y') if np.all(pd.notnull(row['birth_dt'])) else None, axis=1)
        # set appropriate column names, static values, and NULLs
        contacts.drop(['sex_cd','birth_dt'],axis=1,inplace=True)
        for n in ('contact_source','preferred_contact','family_status','anniversary','allow_email','follow_email','allow_bulk_email','allow_phone','allow_fax','allow_mail','description','contact_owner','status','status_reason'):
            contacts[n] = None
        contacts['contact_source'] = 'SAGITTA'
        return contacts.rename({'sagitem':'source_key','given_name':'first_name','surname':'last_name','title':'job_title','spouse':'spouse_name'},axis=1)
    else:
        return pd.DataFrame()

def bp_contact_df(lastUpdate):
    contacts = raw_entity_df('account_contact', 'benefitpoint').query("last_modified_on.dt.date >= @lastUpdate.date()")[['account_id','contact_id','title','last_modified_on']]
    if not contacts.empty:
        # join to contacts for details
        contacts = contacts.merge(raw_entity_df('contact', 'benefitpoint')[['source_key','contact_id','first_name','last_name']], how='left', on='contact_id')
        # join to parent account guid (inner join also weeds out contact recrods associated to 'Individual' account types)
        contacts = contacts.merge(parent_account_df('BENEFITPOINT'),how='inner',on='source_key')
        contacts.drop(['source_key'],axis=1,inplace=True)    
        contacts['contact_source'] = 'BENEFITPOINT'
        for n in ('contact_type','preferred_contact','gender','family_status','spouse_name','dob','anniversary','allow_email','follow_email','allow_bulk_email','allow_phone','allow_fax','allow_mail','description','contact_owner','status','status_reason'):
            contacts[n] = None
        return contacts.rename({'contact_id':'source_key','title':'job_title','last_modified_on':'modify_dt'},axis=1) 
    else:
        return pd.DataFrame()

def contact_tags(lastUpdate):
    contacts = raw_entity_df('contact', 'powerapps')
    contacts = contacts[contacts.modify_dt >= lastUpdate]
    tags = raw_entity_df('tag_detail', 'powerapps')[['tag_name','tag_type','guid']].rename(columns={'guid':'tag_guid'})
    tags = tags[tags.tag_type=='Contact']
    tags['tag_name'] = tags.tag_name.str.lower()
    master_contacts = contacts[contacts.source_key==contacts.master_contact][['source_key','guid','modify_dt']].rename(columns={'source_key':'master_contact','guid':'contact_guid'})
    types = contacts[['master_contact','contact_type']].dropna(subset=['contact_type']).rename(columns={'contact_type':'tag_name'})
    types['tag_name'] = types.tag_name.str.lower()
    master_types = master_contacts.merge(types,how='inner',on='master_contact')
    return master_types.merge(tags,how='inner',on='tag_name')[['contact_guid','tag_guid','modify_dt']].drop_duplicates(['contact_guid','tag_guid'],keep='first')

def parent_child_contact(row,parents):
    row.fillna('',inplace=True)
    parents.fillna('',inplace=True)
    pc = parents.query("account_id==@row['account_id'] and first_name==@row['first_name'] and last_name==@row['last_name']").reset_index()['contact_id'][0]
    return pc

def master_contacts():
    # GET ALL SAGITTA CONTACTS, DROP ANY MISSING FIRST AND LAST NAME, SORT BY CLIENT/ACCOUNT & CONTACT IDs
    sg_contacts = raw_entity_df('contacts', 'sagitta').query("given_name.notnull() and surname.notnull()")[['sagitem','client_id','given_name','surname']].rename({'sagitem':'contact_id','client_id':'account_id','given_name':'first_name','surname':'last_name'},axis=1).sort_values(['account_id','contact_id'])
    # GROUP AND RANK
    sg_contacts['row_number'] = sg_contacts.fillna('x').groupby(['account_id','first_name','last_name']).cumcount()+1
    # SET MASTER CONTACT FOR "PARENTS"
    sg_parents = sg_contacts.loc[sg_contacts.row_number==1].copy()
    sg_parents['master_contact'] = sg_parents.contact_id
    # SET MASTER CONTACT FOR "CHILDREN"
    sg_children = sg_contacts.loc[sg_contacts.row_number>1].copy()
    sg_children['master_contact'] = sg_children.apply(lambda x: parent_child_contact(x,sg_parents),axis=1)
    # RESET SAGITTA CONTACTS DF WITH MASTER CONTACT INFO FOR PARENTS AND CHILDREN
    sg_contacts = pd.concat([sg_parents,sg_children]).drop(['row_number','account_id','first_name','last_name'],axis=1).rename({'contact_id':'source_key'},axis=1)
    # DEFINE SOURCE SYSTEM
    sg_contacts['contact_source'] = 'SAGITTA'

    # GET ALL BENEFITPOINT CONTACTS, SORT BY ACCOUNT & CONTACT IDs
    bp_contacts = pd.merge(
        left=raw_entity_df('account_contact', 'benefitpoint')[['account_id','contact_id']],
        right=raw_entity_df('contact', 'benefitpoint').query("contact_source=='ACCOUNT'")[['source_key','contact_id','first_name','last_name']].rename({'source_key':'account_id'},axis=1),
        how='inner',
        on=['account_id','contact_id']
    ).sort_values(['account_id','contact_id'])
    # GROUP AND RANK
    bp_contacts['row_number'] = bp_contacts.fillna('x').groupby(['account_id','first_name','last_name']).cumcount()+1
    # SET MASTER CONTACT FOR "PARENTS"
    bp_parents = bp_contacts.loc[bp_contacts.row_number==1].copy()
    bp_parents['master_contact'] = bp_parents.contact_id
    # SET MASTER CONTACT FOR "CHILDREN"
    bp_children = bp_contacts.loc[bp_contacts.row_number>1].copy()
    bp_children['master_contact'] = bp_children.apply(lambda x: parent_child_contact(x,bp_parents),axis=1)
    # RESET CONTACTS DF WITH MASTER CONTACT INFO FOR PARENTS AND CHILDREN
    bp_contacts = pd.concat([bp_parents,bp_children]).drop(['row_number','account_id','first_name','last_name'],axis=1).rename({'contact_id':'source_key'},axis=1)
    # DEFINE SOURCE SYSTEM
    bp_contacts['contact_source'] = 'BENEFITPOINT'
    
    # PUT EVERYTHING BACK TOGETHER AND RETURN
    return pd.concat([sg_contacts,bp_contacts])


def main():
    # CONTACTS
    defaultDt = dt.datetime(1967,12,31,0,0)
    try:
        sgLastUpdate = mjdb.entity_last_update('powerapps', 'contact', ('SAGITTA',))
        sgLastUpdate = defaultDt if sgLastUpdate is None else sgLastUpdate
    except Exception as e:
        lf.error(f"unable to retrieve last update from sagitta.contact:\n{e}")
    try:
        bpLastUpdate = mjdb.entity_last_update('powerapps', 'contact', ('BENEFITPOINT',))
        bpLastUpdate = defautlDt if bpLastUpdate is None else bpLastUpdate
    except Exception as e:
        lf.error(f"unable to retrieve last update from benefitpoint.contact:\n{e}")
    try:
        rcs = pd.concat([sg_contact_df(sgLastUpdate),bp_contact_df(bpLastUpdate)]).to_sql('stg_contact', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unbale to stage records for contact\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} record(s) staged for contact")
            try:
                rcu = mjdb.upsert_stage('powerapps','contact', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('powerapps','contact')\n{e}")
            else:
                lf.info(f"{rcu} record(s) affected for Contacts")
        else:
            lf.info('no records to stage for Contacts')
    finally:
        mjdb.drop_table('powerapps','stg_contact')
    
    # CONTACT TAGS
    try:
        tagLastUpdate = mjdb.entity_last_update('powerapps','contact_tag')
    except Exception as e:
        lf.error(f"unable to retrieve last update from powerapps.contact_tag:\n{e}")
    else:
        try:
            ctrcs = contact_tags(tagLastUpdate).to_sql('stg_contact_tag', ENGINE, 'powerapps', 'replace', index=False, chunksize=10000, method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for contact_tag\n{e}")
        else:
            if ctrcs > 0:
                lf.info(f"{ctrcs} record(s) staged for contact_tag")
                try:
                    ctrcu = mjdb.upsert_stage('powerapps','contact_tag', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('powerapps','contact_tag')\n{e}")
                else:
                    lf.info(f"{ctrcu} record(s) affected for Contact Tags")
            else:
                lf.info('no records to stage for Contact Tags')
        finally:
            mjdb.drop_table('powerapps','stg_contact_tag')

    # MASTER CONTACTS
    try:
        mcrcs = master_contacts().to_sql('stg_master_contacts',ENGINE,'powerapps','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage records for Master Contacts\n{e}")
    else:
        if mcrcs > 0:
            lf.info(f'staged {mcrcs} record(s) for Master Contacts')
            try:
                mcrcu = mjdb.function_execute('powerapps', 'sp_master_contact_update')
            except Exception as e:
                lf.error(f"mjdb.function_execute('powerapps', 'sp_master_contact_update')\n{e}")
            else:
                lf.info(f"{mcrcu} record(s) updated for Master Contact")
        else:
            lf.info('no records to stage for Master Contacts')
    finally:
        mjdb.drop_table('powerapps', 'stg_master_contacts')


if __name__ == '__main__':
    main()