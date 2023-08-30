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

lf = cmn.log_filer(LOGDIR,'contacts')

def contacts_row(sagitem, soup):
    row = {'sagitem':sagitem}
    ints = ('audit_entry_dt','audit_time','client_id','birth_dt')
    texts = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','contact_type_cd','given_name','other_given_name','surname','suffix','salutation','spouse','profession','title','company','sex_cd','office_location','comments')
    for i in ints:
        tag = ''.join([x.capitalize() for x in i.split('_')])
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def contacts_address_group_row(sagitem, lis, soup):
    row = {'sagitem':sagitem,'lis':lis}
    for c in ('type','address','address_2','zip','zip_ext','city','state','country','primary','preferred'):
        tag = ''.join([x.capitalize() for x in c.split('_')])
        row[c] = soup.find(tag).text if soup.find(tag) else None
    return row

def contacts_category_group_row(sagitem, lis, soup):
    row = {'sagitem':sagitem,'lis':lis}
    for c in ('category_cd','category_desc'):
        tag = ''.join([x.capitalize() for x in c.split('_')])
        row[c] = soup.find(tag).text if soup.find(tag) else None
    return row

def contacts_email_group_row(sagitem, lis, soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for c in ('type','email_address', 'primary', 'preferred'):
        tag = ''.join([x.capitalize() for x in c.split('_')])
        row[c] = soup.find(tag).text if soup.find(tag) else None
    return row 

def contacts_phone_group_row(sagitem, lis, soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for c in ('type', 'phone_number', 'extension', 'primary_phone', 'mobile_number', 'primary_mobile', 'fax', 'preferred'):
        tag = ''.join([x.capitalize() for x in c.split('_')])
        row[c] = soup.find(tag).text if soup.find(tag) else None
    return row 

def contacts_website_group_row(sagitem, lis, soup):
    row = {'sagitem':sagitem, 'lis':lis}
    for c in ('type', 'website', 'primary', 'preferred'):
        tag = ''.join([x.capitalize() for x in c.split('_')])
        row[c] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    contacts = []
    contactsAddressGroups = []
    contactsCategoryGroups = []
    contactsEmailGroups = []
    contactsPhoneGroups = []
    contactsWebsiteGroups = []
    try:
        lastEntry = mjdb.sg_last_entry('contacts')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('contacts')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT CONTACTS *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate,'%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT CONTACTS *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            contacts.append(contacts_row(sagitem,item))
                            for ag in item.find_all('AddressGroup'):
                                try:
                                    lis = int(ag.get('lis'))
                                    contactsAddressGroups.append(contacts_address_group_row(sagitem,lis,ag))
                                except Exception as e:
                                    lf.error(f"contacts_address_group_row({sagitem},{lis},<<ag>>)\n{e}")
                            for cg in item.find_all('CategoryGroup'):
                                try:
                                    lis = int(cg.get('lis'))
                                    contactsCategoryGroups.append(contacts_category_group_row(sagitem,lis,cg))
                                except Exception as e:
                                    lf.error(f"contacts_category_group_row({sagitem},{lis},<<cg>>)\n{e}")
                            for eg in item.find_all('EmailGroup'):
                                try:
                                    lis = int(eg.get('lis'))
                                    contactsEmailGroups.append(contacts_email_group_row(sagitem,lis,eg))
                                except Exception as e:
                                    lf.error(f"contacts_email_group_row({sagitem},{lis},<<eg>>)\n{e}")
                            for pg in item.find_all('PhoneGroup'):
                                try:
                                    lis = int(pg.get('lis'))
                                    contactsPhoneGroups.append(contacts_phone_group_row(sagitem,lis,pg))
                                except Exception as e:
                                    lf.error(f"contacts_phone_group_row({sagitem},{lis},<<pg>>)\n{e}")
                            for wg in item.find_all('WebsiteGroup'):
                                try:
                                    lis = int(wg.get('lis'))
                                    contactsWebsiteGroups.append(contacts_website_group_row(sagitem,lis,wg))
                                except Exception as e:
                                    lf.error(f"contacts_website_group_row({sagitem},{lis},<<wg>>)\n{e}")
                        except Exception as e:
                            lf.error(f"contacts_row({sagitem},<<item>>)\n{e}")
        stages = [
            ('contacts',pd.DataFrame(contacts) if contacts else None),
            ('contacts_address_group',pd.DataFrame(contactsAddressGroups) if contactsAddressGroups else None),
            ('contacts_category_group',pd.DataFrame(contactsCategoryGroups) if contactsCategoryGroups else None),
            ('contacts_email_group',pd.DataFrame(contactsEmailGroups) if contactsEmailGroups else None),
            ('contacts_phone_group',pd.DataFrame(contactsPhoneGroups) if contactsPhoneGroups else None),
            ('contacts_website_group',pd.DataFrame(contactsWebsiteGroups) if contactsWebsiteGroups else None)
        ]
        for a,b in stages:
            if b is not None:
                try:
                    rcs = b.to_sql(f'stg_{a}', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {a}\n{e}")
                else:
                    if rcs > 0:
                        lf.info(f"{rcs} record(s) staged for {a}")
                        try:
                            rcu = mjdb.upsert_stage('sagitta', a, 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('sagitta', {a}\n{e})")
                        else:
                            lf.info(f"mjdb.upsert_stage('sagitta', {a}) affected {rcu} record(s)")
                finally:
                    mjdb.drop_table('sagitta', f"stg_{a}")

if __name__ == '__main__':
    main()