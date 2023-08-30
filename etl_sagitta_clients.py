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
FILE = 'clients'

lf = cmn.log_filer(LOGDIR,FILE)

def client_row(sagitem, soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'sic_cd':soup.find('SICCd').text if soup.find('SICCd') else None,
        'fein':soup.find('FEIN').text if soup.find('FEIN') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','client_cd','client_name','bill_to_code','addr_1','addr_2','postal_code','postal_extension_code','city','state_prov_cd','phone_1_number','phone_2_number','reference_cd','status_cd','producer_1_cd','producer_2_cd','producer_3_cd','servicer_1_cd','servicer_2_cd','servicer_3_cd','credit_terms','source_cd','source_dt','cat_1_cd','cat_2_cd','cat_3_cd','cat_4_cd','cat_5_cd','net_commission_pct','contact_method','collection_comments','remark_text','phone_1_extension_number','phone_2_extension_number','fax_number','pro_sus_cd','date_business_started','business_nature','inspection_contact','inspection_phone_number','inspection_phone_extension_number','accounting_contact','accounting_phone_number','accounting_phone_extension_number','legal_entity_cd','email_addr','web_site_link','division_number','parent_client','parent_rel_cd','relation_client','relation_cd','insp_email','acct_email','no_members','integration_client_name'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def main():
    clients = []
    try:
        lastEntry =  mjdb.sg_last_entry(FILE)
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry({FILE})\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT {FILE.replace('_','.').upper()} *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {FILE.replace('_','.').upper()} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statment({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            clients.append(client_row(sagitem, item))
                        except Exception as e:
                            lf.error(f"clients.append(client_row({sagitem}, <<item>>))\n{e}")
            stages = {
                'clients':clients if clients else None
            }
            for s in stages:
                if stages[s]:
                    try:
                        rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        lf.error(f"unable to stage records for {s}\n{e}")
                    else:
                        lf.info(f"{rcs} record(s) staged for {s}")
                        if rcs > 0:
                            try:
                                rcu = mjdb.upsert_stage('sagitta',s, 'upsert')
                            except Exception as e:
                                lf.error(f"mjdb.upsert_stage('sagitta',{s})\n{e}")
                            else:
                                lf.info(f"mjdb.upsert_stage('sagitta',{s}) affected {rcu} record(s)")
                    finally:
                        mjdb.drop_table('sagitta', f'stg_{s}')
                else:
                    lf.info(f"no records to stage for {s}")
if __name__ == '__main__':
    main()