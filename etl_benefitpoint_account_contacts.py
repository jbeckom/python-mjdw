import re
import bpws
import mjdb
import config
import common as cmn
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

LOGDIR = 'etl_benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR,'account_contacts')

def modified_contacts(lastMod):
    updates = []
    if (dt.datetime.now(dt.timezone.utc) - lastMod).days <= 30:
        try:
            fc = bpws.find_changes(sinceLastModifiedOn=lastMod, typesToInclude='Account_Contact')
        except Exception as e:
            raise ValueError(f"bpws.find_chanfges(sinceLastModifiedOn={lastMod}, typestoInclude='Account_Contact')\n{e}")
        else:
            try:
                for x in bs(fc.content, 'xml').find_all('modifications'):
                    if dt.datetime.strptime(x.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(bs(bpws.get_account_contact(x.find('entityID').text).content,'xml').find('contact'))
            except Exception as e:
                raise ValueError(f"unable to get findChanges: {e}")
    else:
        try:
            for accountId in mjdb.bp_account_ids():
                for contact in bs(bpws.find_account_contacts(accountId),'xml').find_all('contacts'):
                    if dt.datetime.strptime(contact.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(contact)
        except Exception as e:
            raise ValueError(f"unable to get findAccountContacts: {e}")
    return updates

def col_to_tag(col):
    c = col.split('_')
    return c[0] + ''.join(x.title() for x in c[1:])

def account_contact_row(accountID, contactID, soup):
    row = {'account_id':accountID, 'contact_id':contactID}
    for i in ('primary_location_id',):
        tag = col_to_tag(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in ('title','additional_info','notes'):
        tag = col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    for b in ('primary',):
        tag = col_to_tag(b)
        row[b] = bool(soup.find(tag).text) if soup.find(tag) else None
    for ts in ('last_modified_on',):
        tag = col_to_tag(ts)
        row[ts] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None
    row['location_ids'] = ', '.join([x.text for x in soup.find_all('locationIDs')]) if soup.find('locationIDs') else None
    row['responsibilities'] = ', '.join([x.text for x in soup.find_all('responsibilities')]) if soup.find('responsibilities') else None
    return row

def contact_row(sourceKey, contactID, soup):
    row = {'contact_source':'ACCOUNT','source_key':sourceKey,'contact_id':contactID}
    for t in ('first_name','last_name','email'):
        tag = col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def address_row(sourceKey, soup):
    row = {'address_source':'CONTACT', 'source_type':'ACCOUNT', 'source_key':sourceKey}
    for t in ('street_1','street_2','city','state','zip','country'):
        tag = col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def phone_row(sourceKey, soup):
    row = {'phone_source':'CONTACT', 'source_type':'ACCOUNT', 'source_key':sourceKey}
    for t in ('area_code','number','type'):
        tag = col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def custom_field_value_row(sourceKey, customFieldValueID, soup):
    row = {'cfv_source':'CONTACT', 'source_key':sourceKey, 'custom_field_value_id':customFieldValueID}
    for i in ('custom_field_id','option_value_ID'):
        tag = col_to_tag(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    row['value_text'] = soup.find('valueText').text if soup.find('valueText') else None
    return row    

def main():
    
    accountContacts = []
    contacts = []
    addresses = []
    phones = []
    customFieldValues = []
    lastMod = mjdb.bp_last_modified('account_contact') if mjdb.bp_last_modified('account_contact') else dt.datetime(1900,1,1,0,0,0,tzinfo=dt.timezone.utc)
    # lastMod = dt.datetime(2022,9,15,0,0,tzinfo=dt.timezone.utc) ### DEBUG ONLY ###
    for contact in modified_contacts(lastMod):
        accountId = int(contact.find('accountID').text)
        contactId = int(contact.find('contact').find('contactID').text)
        try:            
            accountContacts.append(account_contact_row(accountId, contactId, contact))
        except Exception as e:
            lf.error(f"account_contact_row({accountId}, {contactId}, <<accountContact>>)\n{e}")
        try:
            contacts.append(contact_row(accountId, contactId, contact.find('contact')))
        except Exception as e:
            lf.error(f"contact_row({contactId},<<contact>>)\n{e}")
        for address in contact.find_all('address'):
            if address.contents:
                try:
                    addresses.append(address_row(contactId,address))
                except Exception as e:
                    lf.error(f"address_row(contactID,contact.find(<<address>>))\n{e}")
        for p in contact.find_all('phones'):
            if not p.has_attr('xsi:nil'):
                try:
                    phones.append(phone_row(contactId,p))
                except Exception as e:
                    lf.error(f"phone_row({contactId},<<p>>)\n{e}")
        for cfv in contact.find_all('customFieldValues'):
            try:
                customFieldValueID = int(cfv.find('customFieldValueID').text)
                customFieldValues.append(custom_field_value_row(contactId, customFieldValueID, cfv))
            except Exception as e:
                lf.error(f"custom_field_value_row({contactId}, {customFieldValueID}, <<cfv>>)\n{e}")
    stages = {
        'account_contact':accountContacts if accountContacts else None,
        'contact':contacts if contacts else None,
        'address':addresses if addresses else None,
        'phone':phones if phones else None,
        'custom_field_value':customFieldValues if customFieldValues else None
    }
    for s in stages:
        if stages[s]:
            try:
                rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}', ENGINE, 'benefitpoint', 'replace', index=False, chunksize=10000, method='multi')
            except Exception as e:
                lf.error(f"unable to stage records for {s}")
            else:
                if rcs > 0:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    try:
                        rcu = mjdb.upsert_stage('benefitpoint', s, 'upsert')
                    except Exception as e:
                        lf.error(f"mjdb.upsert_stage('benefitpoint', {s})\n{e}")
                    else:
                        lf.info(f"mjdb.upsert_stage('benefitpoint', {s}) affected {rcu} record(s)")
            finally:
                mjdb.drop_table('benefitpoint',f'stg_{s}')

if __name__ == '__main__':
    main()