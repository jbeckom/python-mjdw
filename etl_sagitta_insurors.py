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

lf = cmn.log_filer(LOGDIR,'insurors')

def insuror_row(sagitem,soup):
    row = {'sagitem':sagitem}
    for t in ('audit_staff_cd','audit_entry_dt','audit_time','audit_cd','audit_history_record_number','audit_program','insurer_cd','insurer_name','payee_cd','addr_1','addr_2','postal_code','postal_extension_code','city','state_prov_cd','telephone_1','telephone_2','group','agency_code','phone_extension_1_number','company_code','phone_extension_2_number','fax_number','type','obsolete_41','date_off','date_off_remark','email_addr','rounding_difference','description','global','bests_financial_strength','bests_financial_size','state_of_domicile','financial_strength_outlook','financial_strength_action'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    xx = {
        'naic_cd':'NAICCd',
        'am_best_number':'AMBestNumber',
        'amb_company_name':'AMBCompanyName',
        'amb_parent_number':'AMBParentNumber',
        'fein':'FEIN',
        'amb_last_update':'AMBLastUpdate',
        'auto_id_claim_reporting_phone_number':'AutoIDClaimReportingPhoneNumber',
        'auto_id_claim_reporting_phone_extension':'AutoIDClaimReportingPhoneExtension'
    }
    for x in xx:
        row[x] = soup.find(xx[x]).text if soup.find(xx[x]) else None
    return row

def admitted_states_row(sagitem,lis,soup):
    rows = []
    for sc in soup.find_all('StateCode'):
        if sc.find('s1'):
            for s in sc.find_all('s1'):
                rows.append({'sagitem':sagitem,'lis':lis,'state_code':s.text})
        else:
            rows.append({'sagitem':sagitem,'lis':lis,'state_code':sc.text})
    return rows

def alternate_contact_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('alternate_contact_type_cd','alternate_contact_name','alternate_contact_addr_1','alternate_contact_postal_code','alternate_contact_postal_extension_code','alternate_contact_city','alternate_contact_state_prov_cd','alternate_contact_salutation','alternate_contact_phone_number','alternate_contact_phone_extension_number','alternate_contact_addr_2','contact_fax_number','email_addr'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def div_dept_designation_row(sagitem,lis,soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('valid_div','valid_dept','limit_new','limit_new_date','limit_renew','limit_renew_date'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def state_specific_company_code_row(sagitem,lis,soup):
    row={'sagitem':sagitem,'lis':lis}
    for t in ('code_state','state_company_code'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    insurors = []
    admittedStates = []
    alternateContacts = []
    divDeptDesignations = []
    nonAdmittedStates = []
    stateSpecificCompanyCodes = []
    try:
        lastEntry = mjdb.sg_last_entry('insurors')
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
    except Exception as e:
        lf.error(f"unable to retrieve last entry data\n{e}")
    else:
        try:
            xmlResponse = sgws.post_ptr_access_statement(f"SELECT INSURORS *CRITERIA* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate,'%m-%d-%Y')}")
        except Exception as e:
            lf.error(f"unable to retrieve xml response\n{e}")
        else:
            for item in xmlResponse.find_all('Item'):
                try:
                    sagitem = int(item.get('sagitem'))
                    insurors.append(insuror_row(sagitem,item))
                except Exception as e:
                    lf.error(f"insuror_row({int(item.get('sagitem'))},<<item>>)\n{e}")
                else:
                    for ias in item.find_all('AdmittedStates'):
                        try:
                            lis = int(ias.get('lis')) if ias.get('lis') else 1
                            # unpack row level list, append to "master" list
                            [admittedStates.append(n) for n in admitted_states_row(sagitem,lis,ias)] 
                        except Exception as e:
                            lf.error(f"unable to parse Admitted States for {sagitem}, {lis}\n{e}")
                    for ac in item.find_all('AlternateContactInfo'):
                        try:
                            lis = int(ac.get('lis')) if ac.get('lis') else 1
                            alternateContacts.append(alternate_contact_row(sagitem,lis,ac))
                        except Exception as e:
                            lf.error(f"alternate_contact_row({sagitem},{lis},<<ac>>)\n{e}")
                    for ddd in item.find_all('DivDeptDesignations'):
                        try:
                            lis = int(ddd.get('lis'))
                            divDeptDesignations.append(div_dept_designation_row(sagitem,lis,ddd))
                        except Exception as e:
                            lf.error(f"div_dept_designation_row({sagitem},{lis},<<ddd>>)\n{e}")
                    for nas in item.find_all('NonAdmittedStates'):
                        try:
                            lis = int(nas.get('lis')) if nas.get('lis') else 1
                            # unpack "row-level" list, append to "master" list
                            [nonAdmittedStates.append(n) for n in admitted_states_row(sagitem,lis,nas)]
                        except Exception as e:
                            lf.error(f"unable to parse Non-Admitted States for {sagitem}, {lis}\n{e}")
                    for sscc in item.find_all('StateSpecificCompanyCodes'):
                        stateSpecificCompanyCodes.append(state_specific_company_code_row(sagitem,int(sscc.get('lis')),sscc))
    stages = {
        'insurors':insurors if insurors else None,
        'insurors_admitted_states':admittedStates if admittedStates else None,
        'insurors_alternate_contact_info':alternateContacts if alternateContacts else None,
        'insurors_div_dept_designations':divDeptDesignations if divDeptDesignations else None,
        'insurors_non_admitted_states':nonAdmittedStates if nonAdmittedStates else None,
        'insurors_state_specific_company_codes':stateSpecificCompanyCodes if stateSpecificCompanyCodes else None
    }
    for s in stages:
        if stages[s]:
            try:
                rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
            except Exception as e:
                lf.error(f"unable to stage records for {s}")
            else:
                lf.info(f"{rcs} record(s) staged for {s}")
                if rcs > 0:
                    # atypical function calls for admitted/non-admitted states entities
                    if s in ['insurors_admitted_states','insurors_non_admitted_states', 'insurors_state_specific_company_codes']:
                        try:
                            rci = mjdb.function_execute('sagitta', f'sp_{s}_insert')
                        except Exception as e:
                            lf.error(f"mjdb.function_execute('sagitta', 'sp_{s}_insert')\n{e}")
                        else:
                            lf.info(f"mjdb.function_execute('sagitta', 'sp_{s}_insert') affected {rci} record(s)")
                        try:
                            rcd = mjdb.function_execute('sagitta', f'sp_{s}_delete')
                        except Exception as e:
                            lf.error(f"mjdb.function_execute('sagitta', 'sp_{s}_delete')\n{e}")
                        else:
                            lf.info(f"mjdb.function_execute('sagitta', 'sp_{s}_delete') affected {rcd} record(s)")
                    # typical upsert function call for all other entities
                    else:
                        try:
                            rcu = mjdb.upsert_stage('sagitta', s, 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('sagitta', {s})\n{e}")
                        else:
                            lf.info(f"mjdb.upsert_stage('sagitta', {s}) affected {rcu} record(s)")
            finally:
                mjdb.drop_table('sagitta', f'stg_{s}')

if __name__ == '__main__':
    main()