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
FILE = 'payees'

lf = cmn.log_filer(LOGDIR,FILE)

def payees_row(sagitem,soup):
    row = {
        'sagitem':sagitem,
        'audit_entry_dt':int(soup.find('AuditEntryDt').text) if soup.find('AuditEntryDt') else None,
        'audit_time':int(soup.find('AuditTime').text) if soup.find('AuditTime') else None,
        'mga':soup.find('MGA').text if soup.find('MGA') else None
    }
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','payee_name','initial_dt','contact_name','addr_1','addr_2','postal_code','postal_extension_code','city','state_prov_cd','phone_1_number','phone_2_number','agency_cd','pay_method_cd','num_days','fax_number','phone_1_extention_number','phone_2_extention_number','off_dt','direct_bill_ind','release_ind','email_addr','description','global','payee_responsible_for_filing','tax_fee_payee'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def cov_ins_percent_info_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis,
        'db_new_pct':soup.find('DBNewPct').text if soup.find('DBNewPct') else None,
        'db_ren_pct':soup.find('DBRenPct').text if soup.find('DBRenPct') else None
    }
    for t in ('coverage_cd','agency_new_pct','agency_ren_pct','begin_dt','end_dt','standard_comm_ind','insurer_cd','grading_from_amt','grading_to_amt','commission_type_ind','comm_div','comm_dept'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def div_dept_designations_row(sagitem,lis,soup):
    row = {
        'sagitem':sagitem,
        'lis':lis
    }
    for t in ('valid_div','valid_dept','limit_new','limit_new_date','limit_renew','limit_renew_date'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None 
    return row 

def main():
    payees = []
    covInsPercentInfo = []
    divDeptDesignations = []

    try:
        lastEntry = mjdb.sg_last_entry(FILE)
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
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = item.get('sagitem')
                            payees.append(payees_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"payees_row({sagitem},<<item>>)\n{e}")
                        else:
                            try:
                                [covInsPercentInfo.append(cov_ins_percent_info_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('CovInsPercentInfo')]
                            except Exception as e:
                                lf.error(f"unable to parse CovInsPercentInfo for {sagitem}:\n{e}")
                            try:
                                [divDeptDesignations.append(div_dept_designations_row(sagitem,int(x.get('lis')),x)) for x in item.find_all('DivDeptDesignations')]
                            except Exception as e:
                                lf.error(f"unable to parse DivDeptDesignations for {sagitem}:\n{e}")
            stages = {
                'payees':payees if payees else None,
                'payees_cov_ins_percent_info':covInsPercentInfo if covInsPercentInfo else None,
                'payees_div_dept_designations':divDeptDesignations if divDeptDesignations else None
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