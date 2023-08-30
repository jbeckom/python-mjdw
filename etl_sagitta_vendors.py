import mjdb
import sgws
import config
import common as cmn
import datetime as dt 
import pandas as pd
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
FILE = 'vendors'
LF = cmn.log_filer(LOGDIR,FILE)
CONFIGS = [
    {'tag':'Item','table':'vendors','func':'vendors_row','rows':[]},
    {'tag':'VendorAPGLAccountInfo','table':'vendors_apgl_account_info','func':'vendors_apgl_account_info_row','rows':[]},
    {'tag':'VendorExpAcctInfo','table':'vendors_exp_acct_info','func':'vendors_exp_acct_info_row','rows':[]},
    {'tag':'ProducerDBAPAcctInfo','table':'vendors_producer_dbap_acct_info','func':'vendors_producer_dbap_acct_info_row','rows':[]},
    {'tag':'ProducerBExpAcctInof','table':'vendors_producer_b_exp_acct_info','func':'vendors_producer_b_exp_acct_info_row','rows':[]},
    {'tag':'InsDBARAcctInfo','table':'vendors_ins_dbar_acct_info','func':'vendors_ins_dbar_acct_info_row','rows':[]}
]

def vendors_row(sagitem,soup):
    row = {'sagitem':sagitem}
    for i in ['audit_entry_dt','audit_time']:
        tag = hlp.col_tag_transform(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in ['audit_staff_cd','audit_cd','audit_history_record_number','audit_program','vendor_name_1','vendor_name_2','vendor_addr_1','vendor_addr_2','postal_code','postal_extension_code','vendor_city','vendor_state','vendor_phone_number','vendor_fax_number','vendor_tax_id','vendor_require_1099_ind','vendor_credit_term','vendor_status','vendor_contact_name','bank_cd','vendor_type_cd','voucher_print_ind','vendor_off_dt','vendor_off_dt_remark_text','broker']:
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    # NON-STANDARD COLUMN NAME FORMATS
    for k,v in {'insurer_ap_void_ind':'InsurerAPVoidInd','insurer_db_rec_ind':'InsurerDBRecInd','producer_db_pay_ind':'ProducerDBPayInd','vendor_print_gl_number':'VendorPrintGLNumber','ins_db_method':'InsDBMethod','vendor_igl_ind':'VendorIGLInd','producer_ab_pay_ind':'ProducerABPayInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def vendors_apgl_account_info_row(sagitem,soup):
    row = {'sagitem':sagitem,'lis':int(soup.get('lis'))}
    for k,v in {'vendor_apgl_acct_number':'VendorAPGLAcctNumber','vendor_ap_dept_ind':'VendorAPDeptInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def vendors_exp_acct_info_row(sagitem,soup):
    row = {'sagitem':sagitem,'lis':int(soup.get('lis'))}
    for k,v in {'vendor_exp_acct_number':'VendorExpAcctNumber','vendor_exp_dept_ind':'VendorExpDeptInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def vendors_producer_dbap_acct_info_row(sagitem,soup):
    row = {'sagitem':sagitem,'lis':int(soup.get('lis'))}
    for k,v in {'producer_dbap_acct_number':'ProducerDBAPAcctNumber','producer_dbap_dept_ind':'ProducerDBAPDeptInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def vendors_producer_b_exp_acct_info_row(sagitem,soup):
    row = {'sagitem':sagitem,'lis':int(soup.get('lis'))}
    for k,v in {'producer_b_exp_acct_number':'ProducerDBAPAcctNumber','producer_db_exp_dept_ind':'ProducerDBAPDeptInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def vendors_ins_dbar_acct_info_row(sagitem,soup):
    row = {'sagitem':sagitem,'lis':int(soup.get('lis'))}
    for k,v in {'ins_dbar_acct_number':'InsDBARAcctNumber','ins_dbar_dept_ind':'InsDBARDeptInd'}.items():
        row[k] = soup.find(v).text if soup.find(v) else None
    return row

def main():    
    try:
        lastEntry = mjdb.sg_last_entry(FILE)
    except Exception as e:
        LF.error(f"unable to fetch last entry data for {FILE}:\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT {FILE.replace('_','.').upper()} *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            LF.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT {FILE.replace('_','.').upper()} *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    LF.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for f in batchResponse.find_all('File'):
                        sagitem = f.find('Item').get('sagitem')
                        for cfg in CONFIGS:
                            for x in f.find_all(cfg['tag']):
                                try:
                                    cfg['rows'].append(eval(f"{cfg['func']}(sagitem,x)"))
                                except Exception as e:
                                    LF.error(f"unable to parse {cfg['func']} for {sagitem}:\n{e}")
            for cfg in CONFIGS:
                if len(cfg['rows']) > 0:
                    try:
                        rcs = pd.DataFrame(cfg['rows']).to_sql(f"stg_{cfg['table']}",ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        LF.error(f"unable to stage records for {cfg['table']}\n{e}")
                    else:
                        LF.info(f"{rcs} record(s) staged for {cfg['table']}")
                        try:
                            rcu = mjdb.upsert_stage('sagitta',cfg['table'],'upsert')
                        except Exception as e:
                            LF.error(f"unable to upsert record(s) for {cfg['table']}")
                        else:
                            LF.info(f"{rcu} record(s) affected for {cfg['table']}")
                    finally:
                        mjdb.drop_table('sagitta',f"stg_{cfg['table']}")
                else:
                    LF.info(f"no records to staged for {cfg['table']}")

if __name__ == '__main__':
    main()