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

lf = cmn.log_filer(LOGDIR, 'staff')

def staff_row(sagitem, soup):
    textCols = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','staff_name','addr_1','addr_2','postal_code','postal_extension_code','city','state_prov_cd','work_phone_number','home_phone_number','dept','hired_dt','next_review_dt','termination_dt','annual_salary_amt','division_id','tax_id','emergency_phone_number','title','birth_dt','skip_flag','display_only','date_off','date_off_remark','fax_number','super_user_id','agency_cd','email_addr','role','national_producer_number','service_account','integration_staff_name','integration_staff_title','mobile_phone')
    intCols = ('audit_entry_dt','audit_time')
    row = {'sagitem':sagitem}
    for a in textCols:
        tag = ''.join([x.capitalize() for x in a.split('_')])
        row[a] = soup.find(tag).text if soup.find(tag) else None
    for b in intCols:
        tag = ''.join([x.capitalize() for x in b.split('_')])
        row[b] = int(soup.find(tag).text) if soup.find(tag) else None
    return row

def commission_group_row(sagitem, lis, soup):
    textCols = ('standard_commission','type','insurer_id','coverage_cd','from_amt','to_amt','ab_new_comm_percentage','ab_renewal_comm_percentage','cb_new_comm_percentage','cb_renew_comm_percentage','start_dt','end_dt','comm_div','comm_dept')
    row = {'sagitem':sagitem, 'lis':lis}
    for col in textCols:
        tag = ''.join([x.capitalize() for x in col.split('_')])
        row[col] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    staffRows = []
    commissionGroupRows = []
    try:
        lastEntry = mjdb.sg_last_entry('staff')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('staff')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        try:
            # STAFF dictionary does not have synonym for AuditTime (typically LAST.ENTRY.TIME)
            batchesStatement = f"SELECT STAFF *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate,'%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT STAFF *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = item.get('sagitem')
                            staffRows.append(staff_row(sagitem, item))
                        except Exception as e:
                            lf.error(f"staffRows.append(staff_row({sagitem}, <<item>>))\n{e}")
                        else:
                            for cg in item.find_all('CommissionGroup'):
                                try:
                                    lis = int(cg.get('lis'))
                                    commissionGroupRows.append(commission_group_row(sagitem,lis,cg))
                                except Exception as e:
                                    lf.error(f"commissionGroupRows.append(commission_group_row({sagitem},{lis},<<cg>>))\n{e}")
    # create datafram from row lists, stage in database
    try:
        rcss = pd.DataFrame(staffRows).to_sql('stg_staff', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage staff records\n{e}")
    else:
        if rcss > 0:
            lf.info(f"{rcss} row(s) staged for staff")
            try:
                rcscg = pd.DataFrame(commissionGroupRows).to_sql('stg_staff_commission_group', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
            except Exception as e:
                lf.error(f"unable to stage stff_commission_group records\n{e}")
            else:
                if rcscg > 0:
                    lf.info(f"{rcscg} row(s) stages for staff_commission_group") 
    # upsert from stage
    if rcss > 0:
        try:
            rcsu = mjdb.upsert_stage('sagitta', 'staff','upsert')
            mjdb.drop_table('sagitta', 'stg_staff')
        except Exception as e:
            lf.error(f"mjdb.upsert_stage('sagitta', 'staff','upsert')\n{e}")
        else:
            lf.info(f"mjdb.upsert_stage('sagitta', 'staff','upsert') affected {rcsu} row(s)")
    if rcscg > 0:
        try:
            rcucg = mjdb.upsert_stage('sagitta', 'staff_commission_group','upsert')
            mjdb.drop_table('sagitta', 'stg_staff_commission_group')
        except Exception as e:
            lf.error(f"mjdb.upsert_stage('sagitta', 'staff_commission_group','upsert')\n{e}")
        else:
            lf.info(f"mjdb.upsert_stage('sagitta', 'staff_commission_group','upsert') affected {rcucg} row(s)")

if __name__ == '__main__':
    main()