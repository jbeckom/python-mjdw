import sgws
import mjdb
import config 
from datetime import date, timedelta
import pandas as pd
import common as cmn
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
SCHEMA = 'sagitta'
FILE = 'COMPANY.MASTER'
LF = cmn.log_filer(LOGDIR,FILE)
CONFIGS = [
    {'tag':'Item','table':'company_master','func':'company_master_row','rows':[]},
    {'tag':'Department','table':'company_master_department','func':'department_row','rows':[]}
]

def company_master_row(sagitem,soup):
    row = {
        'sagitem':int(sagitem),
        'gl_format':soup.find('GLFormat').text if soup.find('GLFormat') else None,
        'division_gl_suspense_acct':soup.find('DivisionGLSuspenseAcct').text if soup.find('DivisionGLSuspenseAcct') else None,
        'prefill_acord_forms_yn_cd':soup.find('PrefillACORDFormsYNCd').text if soup.find('PrefillACORDFormsYNCd') else None,
        'ny_license_code':soup.find('NYLicenseCode').text if soup.find('NYLicenseCode') else None
    }
    for i in ('audit_entry_dt','audit_time'):
        tag = hlp.col_tag_transform(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','division_name','addr_1','addr_2','postal_code','postal_extension_code','phone_1_number','phone_2_number','fax_number'):
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def department_row(sagitem,soup):
    row = {
        'sagitem':int(sagitem),
        'lis':int(soup.get('lis')),
        'cb_acct_meth':soup.find('CBAcctMeth').text if soup.find('CBAcctMeth') else None,
        'create_ins_rec_yn_cd':soup.find('CreateInsRecYNCd').text if soup.find('CreateInsRecYNCd') else None
    }
    for t in ('dept_cd','dept_name','phone_number','addr_1','addr_2','postal_code','postal_extension_code','city','state_prov_cd'):
        tag = hlp.col_tag_transform(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    companyMasterRows = []
    departmentRows = []
    try:
        lastEntry = mjdb.sg_last_entry(FILE)
    except Exception as e:
        LF.error(f"unable to fetch lastEntry:\n{e}")
    else:
        lastEntryDate = (date(1967,12,31) + timedelta(days=lastEntry[0])) if lastEntry[0] else date(1967,12,31)
        batchesStatement = f"SELECT {FILE} *CRITERIA.BATCH* WITH PAX.AUDIT.DATE GE {lastEntryDate}" 
        try:
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            LF.error(f"unable to fetch batches:\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                batchStatement = f"SELECT {FILE} *GET.BATCH* {batch}"
                try:
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    LF.error(f"unable to fetch {batch}:\n{e}")
                else:
                    for f in batchResponse.find_all('File'):
                        sagitem = f.find('Item').get('sagitem')
                        for cfg in CONFIGS:
                            for x in f.find_all(cfg['tag']):
                                try:
                                    cfg['rows'].append(eval(f"{cfg['func']}(sagitem,x)"))
                                except Exception as e:
                                    LF.error(f"unable to parse {cfg['tag']} for {sagitem}:\n{e}")
            for cfg in CONFIGS:
                if len(cfg['rows']) > 0:
                    try:
                        rcs = pd.DataFrame(cfg['rows']).to_sql(f"stg_{cfg['table']}",ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        LF.error(f"unabel to stage records for {cfg['table']}:\n{e}")
                    else:
                        LF.info(f"{rcs} record(s) staged for {cfg['table']}")
                        if rcs > 0:
                            try:
                                rcu = mjdb.upsert_stage(SCHEMA,cfg['table'],'upsert')
                            except Exception as e:
                                LF.error(f"uanble to upsert from stage:\n{e}")
                            else:
                                LF.info(f"{rcu} row(s) affected for {cfg['table']}")
                    finally:
                        mjdb.drop_table(SCHEMA,f"stg_{cfg['table']}")
                else:
                    LF.info(f"no records to stage for {cfg['table']}")

if __name__ == '__main__':
    main()