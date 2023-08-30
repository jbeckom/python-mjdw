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
RESUB = r'(?<!^)(?=[A-Z])'

lf = cmn.log_filer(LOGDIR,'accounts')

def modified_accounts(lastMod):
    """Returns list of AccountID(s) that have been modified since provided lastMod"""
    updates = []
    if (dt.datetime.now(dt.timezone.utc) - lastMod).days <= 30:
        try:
            fc = bpws.find_changes(sinceLastModifiedOn=lastMod, typesToInclude='Account')
        except Exception as e:
            raise ValueError(f"bpws.find_changes(sinceLastModifiedOn={lastMod}, typesToInclude='Account')\n{e}")
        else:
            try:
                for x in bs(fc.content, 'xml').find_all('modifications'):
                    if dt.datetime.strptime(x.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(int(x.find('entityID').text))
            except Exception as e:
                raise ValueError(f"unable to parse findChangesResponse\n{e}")
            else:
                return updates
    else:
        try:
            fa = bpws.find_accounts()
        except Exception as e:
            raise ValueError(f"bpws.find_accounts()\n{e}")
        else:
            try:
                for x in bs(fa.content, 'xml').find_all('result'):
                    if dt.datetime.strptime(x.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(int(x.find('accountID').text))
            except Exception as e:
                raise ValueError(f"unable to parse findAccountsResponse\n{e}")
            else:
                return updates

def account_row(accountID, soup):
    row = {
        'account_id':accountID,
        'agency_account_id':', '.join([x.text for x in soup.find_all('agencyAccountId')])
    }
    for a,b in [('active', 'active'), ('excluded_purge','excludedPurge')]: 
        row[a] = cmn.bp_parse_bool(soup.find(b).text) if soup.find(b) else None        
    for c,d in [('inactive_as_of','inactiveAsOf'), ('last_reviewed_on','lastReviewedOn'), ('created_on','createdOn'), ('last_modified_on','lastModifiedOn')]:
        row[c] = dt.datetime.strptime(soup.find(d).text, WSTSFMT) if soup.find(d) else None
    for e,f in [('office_id','officeID'), ('department_id','departmentID'), ('administrator_user_id','administratorUserID'), ('primary_contact_user_id','primaryContactUserID'), ('primary_sales_lead_user_id','primarySalesLeadUserID'), ('primary_service_lead_user_id','primaryServiceLeadUserID'), ('last_reviewed_by_user_id','lastReviewedByUserID')]:
        row[e] = int(soup.find(f).text) if soup.find(f) else None
    for g,h in [('inactive_reason','inactiveReason'), ('account_classification','accountClassification'), ('account_type','accountType'), ('notes','notes')]:
        row[g] = soup.find(h).text if soup.find(h) else None
    return row

def address_row(addressSource, sourceType, sourceKey, soup):
    row = {
        'address_source':addressSource,
        'source_type':sourceType,
        'source_key':sourceKey
    }
    for a,b in [('street_1','street1'), ('street_2','street2'), ('city','city'), ('state','state'), ('zip','zip'), ('country','country')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def custom_field_value_row(cfvSource, sourceKey, soup):
    row = {
        'cfv_source':cfvSource,
        'source_key':sourceKey,
        'value_text':soup.find('valueText').text if soup.find('valueText') else None
    }
    for a,b in [('custom_field_value_id','customFieldValueID'), ('custom_field_id','customFieldID'), ('option_value_id','optionValueID')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    return row

def group_account_info_row(accountID, soup):
    row = {'account_id':accountID}    
    for a,b in [('account_name','accountName'), ('dba','DBA'), ('market_size','marketSize'), ('business_type','businessType'), ('sic_code','SICCode'), ('naics_code','NAICSCode'), ('locations_by_zip','locationsByZip'), ('affiliates','affiliates'), ('single_payroll_cycle','singlePayrollCycle')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    for c,d in [('number_of_ftes','numberOfFTEs'), ('number_of_full_time_equivalents','numberOfFullTimeEquivalents')]:
        row[c] = int(soup.find(d).text) if soup.find(d) else None
    for e,f in [('number_of_ftes_as_of','numberOfFTEsAsOf'), ('number_of_full_time_equivalents_as_of_date','numberOfFullTimeEquivalentsAsOfDate')]:
        row[e] = dt.datetime.strptime(soup.find(f).text, WSTSFMT) if soup.find(f) else None
    for g,h in [('coverage_subject_to_employer_mandate','coverageSubjectToEmployerMandate'), ('requires_5500','requires5500'), ('multiple_payroll_cycles','multiplePayrollCycles')]:
        row[g] = cmn.bp_parse_bool(soup.find(h).text.replace('None_Selected','')) if soup.find(h) else None
    for i,j in [('budgeted_total_annual_premium','budgetedTotalAnnualPremium'), ('budgeted_total_annual_revenue','budgetedTotalAnnualRevenue')]:
        row[i] = float(soup.find(j).text) if soup.find(j) else None
    row['multiple_payroll_cycles_differ_by'] = ', '.join([x.text for x in soup.find_all('multiplePayrollCyclesDifferBy')])
    return row

def common_group_account_info_row(accountID, soup):
    row = {
        'account_id':accountID,
        'number_of_retirees_as_of':dt.datetime.strptime(soup.find('numberOfRetireesAsOf').text, WSTSFMT) if soup.find('numberOfRetireesAsOf') else None
    }
    for a,b in [('number_of_retirees','numberOfRetirees'), ('year_established', 'yearEstablished')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    for c,d in [('account_funding_type','accountFundingType'), ('primary_industry','primaryIndustry'), ('secondary_industry','secondaryIndustry'), ('other_primary_industry','otherPrimaryIndustry'), ('other_secondary_industry','otherSecondaryIndustry'), ('tax_payer_id','taxpayerID'), ('website','website')]:
        row[c] = soup.find(d).text if soup.find(d) else None
    return row

def brokerage_account_info_row(accountID, accountType, soup):
    row = {
        'account_id':accountID,
        'account_type':accountType,
        'account_number':soup.find('accountNumber').text if soup.find('accountNumber') else None,
        'hipaa_required':cmn.bp_parse_bool(soup.find('HIPAARequired').text.replace('None_Selected','')) if soup.find('HIPAARequired') else None
    }
    for a,b in [('broker_of_record_as_of','brokerOfRecordAsOf'), ('hipaa_signed_on','HIPAASignedOn')]:
        row[a] = dt.datetime.strptime(soup.find(b).text, WSTSFMT) if soup.find(b) else None
    return row

def account_class_row(accountID, soup):
    row = {
        'account_id':accountID,
        'class_id':int(soup.find('classID').text) if soup.find('classID') else None
    }
    for a,b in [('code','code'), ('name','name'), ('payroll_cycle','payrollCycle')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row 

def account_division_row(accountID, soup):
    row = {
        'account_id':accountID,
        'division_id':int(soup.find('divisionID').text) if soup.find('divisionID') else None
    }
    for a,b in [('code','code'), ('name','name'), ('payroll_cycle','payrollCycle')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def account_location_row(accountID, soup):
    row = {
        'account_id':accountID,
        'location_id':int(soup.find('locationID').text) if soup.find('locationID') else None
    }
    for a,b in [('code','code'),('name','name'),('payroll_cycle','payrollCycle')]:
        row[a]=soup.find(b).text if soup.find(b) else None
    return row

def account_integration_row(accountID, soup):
    row = {
        'account_id':accountID,
        'ams_customer_number':int(soup.find('amsCustomerNumber').text) if soup.find('amsCustomerNumber') else None
    }
    for a,b in [('sagitta_client_id','sagittaClientID'), ('source_code','sourceCode'), ('primary_sales_lead_int_code','primarySalesLeadIntCode'), ('primary_service_lead_int_code','primaryServiceLeadIntCode'), ('ams_customer_id','amdCustomerID')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def tam_customer_row(accountID, soup):
    row = {
        'account_id':accountID,
        'office_id':int(soup.find('officeID').text) if soup.find('officeID') else None
    }
    for a,b in [('customer_code','customerCode'), ('customer_class_code','customerClassCode'), ('branch_code','branchCode'), ('agency_code','agencyCode'), ('branch_name','branchName'), ('agency_name','agencyName')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def ams360_account_row(accountID, soup):
    row = {'account_id':accountID}
    for a,b in [('is_benefits','isBenefits'),('is_commercial','isCommercial'),('is_financial','isFinancial'),('is_health','isHealth'),('is_life','isLife'),('is_non_property_and_casualty','isNonPropertyAndCasualty'),('is_personal','isPersonal')]:
        row[a] = cmn.bp_parse_bool(soup.find(b).text) if soup.find(b) else None
    return row

def employee_type_row(accountID, soup):
    row = {'account_id':accountID}
    for a,b in [('employee_type_id','employeeTypeID'),('value','value')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    for c,d in [('status','status'),('type','type'),('unit_of_measure','unitOfMeasure'),('frequency','frequency')]:
        row[c] = soup.find(d).text if soup.find(d) else None
    return row 

def aca_measurement_period_row(accountID, soup):
    row = {'account_id':accountID}
    for a,b in [('measurement_period','measurementPeriod'),('start_date','startDate'),('end_date','endDate')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def individual_account_info_row(accountID, soup):
    row = {
        'account_id':accountID,
        'email':soup.find('email').text if soup.find('email') else None,
        'affiliated_group_account_ID':int(soup.find('affiliatedGroupAccountID').text) if soup.find('affiliatedGroupAccountID') else None
    }
    return row

def person_info_row(personSource, sourceType, sourceKey, soup):
    row = {'person_source':personSource, 'source_type':sourceType, 'source_key':sourceKey}
    for a,b in [('first_name','firstName'),('middle_name','middleName'),('last_name','lastName'),('salutation','salutation'),('gender','gender'),('ssn','ssn'),('marital_status','maritalStatus')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    row['date_of_birth'] = dt.datetime.strptime(soup.find('dateOfBirth').text, '%Y-%m-%d') if soup.find('dateOfBirth') else None
    return row

def phone_row(phoneSource, sourceType, sourceKey, soup):
    row = {'phone_source':phoneSource, 'source_type':sourceType, 'source_key':sourceKey}
    for a,b in [('area_code','areaCode'),('number','number'),('type','type')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    return row

def dependent_row(accountID, soup):
    return {
            'account_id':accountID,
            'dependent_id':int(soup.find('dependentID').text) if soup.find('dependentID') else None,
            'relationship':soup.find('relationship').text if soup.find('relationship') else None
        }

def marketing_group_account_row(accountID, soup):
    row = {
        'account_id':accountID,
        'coverage_subject_to_employer_mandate':cmn.bp_parse_bool(soup.find('coverageSubjectToEmployerMandate').text) if soup.find('coverageSubjectToEmployerMandate') else None,
        'associated_account_ids':', '.join([x.text for x in soup.find_all('associatedAccountIDs')])
    }
    for a,b in [('marketing_group_name','marketingGroupName'),('marketing_group_type','marketingGroupType')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    for c,d in [('number_of_ftes','numberOfFTEs'),('number_of_full_time_equivalents','numberOfFullTimeEquivalents')]:
        row[c] = int(soup.find(d).text) if soup.find(d) else None
    for e,f in [('number_of_ftes_as_of','numberOfFTEsAsOf'),('number_of_full_time_equivalents_as_of_date','numberOfFullTimeEquivalentsAsOfDate')]:
        row[e] = dt.datetime.strptime(soup.find(f).text, WSTSFMT) if soup.find(f) else None
    return row

def agency_info_row(accountID, infoSource, soup):
    row = {
        'account_id':accountID,
        'associated_account_ids':', '.join([x.text for x in soup.find_all('associatedAccountIDs')]) if soup.find('associatedAccountIDs') else None
    }
    for a,b in [('email','email'),('tax_payer_id','taxPayerID')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    for c,d in [('budgeted_total_annual_premium','budgetedTotalAnnualPremium'),('budgeted_total_annual_revenue','budgetedTotalAnnualRevenue')]:
        row[c] = float(soup.find(d).text) if soup.find(d) else None
    return row

def license_row(accountID, soup):
    row = {
        'account_id':accountID,
        'license_id':int(soup.find('licenseID').text),
        'residence_license':cmn.bp_parse_bool(soup.find('residenceLicense').text) if soup.find('residenceLicense') else None
    }
    for a,b in [('state','state'),('license_number','licenseNumber')]:
        row[a] = soup.find(b).text if soup.find(b) else None
    for c,d in [('license_on','licenseOn'),('license_expires_on','licenseExpiresOn'),('e_and_o_expires_on','EAndOExpiresOn')]:
        row[c] = dt.datetime.strptime(soup.find(d).text, WSTSFMT) if soup.find(d) else None
    return row

def carrier_appointment_row(accountID, soup):
    row = {
        'account_id':accountID,
        'appointment_on':dt.datetime.strptime(soup.find('appointmentOn').text, WSTSFMT) if soup.find('appointmentOn') else None,
        'active':cmn.bp_parse_bool(soup.find('active').text) if soup.find('active') else None,
        'appointment_number':soup.find('appointmentNumber').text if soup.find('appointmentNumber') else None,
        'states':', '.join([x.text for x in soup.find_all('states')])
    }
    for a,b in [('carrier_appointment_id','carrierAppointmentID'),('carrier_id','carrierID')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    return row

def custom_field_option_value_row(cfovSource, sourceKey, soup):
    row = {'cfov_source':cfovSource, 'source_key':sourceKey}
    for a,b in [('custom_field_option_value_id','customFieldOptionValueID'), ('sort_order','sortOrder')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    for c,d in [('description','description'),('code','code')]:
        row[c] = soup.find(d).text if soup.find(d) else None
    for e,f in [('active','active'),('default_option_value','defaultOptionValue')]:
        row[e] = cmn.bp_parse_bool(soup.find(f).text) if soup.find(f) else None
    return row

def account_team_member_row(accountID, soup):
    row = {'account_id':accountID}
    for a,b in [('user_id','userID'),('broker_client_role_id','brokerClientRoleID')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    for c,d in [('first_name','firstName'),('last_name','lastName'),('email','email')]:
        row[c] = soup.find(d).text if soup.find(d) else None
    for e,f in [('administrator','administrator'),('primary_contact','primaryContact'),('team_role_primary','teamRolePrimary')]:
        row[e] = cmn.bp_parse_bool(soup.find(f).text) if soup.find(f) else None
    row['office'] = int(soup.find('office').find('officeID').text) if soup.find('office') else None
    return row

def main():
    accountLastMod = mjdb.bp_last_modified('account')
    accountLastMod = accountLastMod if accountLastMod else dt.datetime(1900,1,1,0,0,0,tzinfo=dt.timezone.utc)
    accounts = []
    addresses = []
    customFieldValues = []
    groupAccounts = []
    commonGroupAccounts = []
    brokerageAccounts = []
    accountClasses = []
    accountDivisions = []
    accountLocations = []
    accountIntegrations = []
    tamCustomers = []
    ams360Accounts = []
    employeeTypes = []
    acaMeasurementPeriods = []
    individualAccounts = []
    persons = []
    phones = []
    dependents = []
    marketingGroupAccounts = []
    agentAccounts = []
    agencies = []
    licenses = []
    carrierAppointments = []
    agencyAccounts = []
    customFieldOptionValues = []
    accountTeamMembers = []
    try:
        acctMods = modified_accounts(accountLastMod)
    except Exception as e:
        lf.error(f"modified_accounts({accountLastMod})\n{e}")
    else:
        for accountID in acctMods:
            try:
                accountSoup = bs(bpws.get_account(accountID).content, 'xml')
            except Exception as e:
                lf.error(f"bs(bpws.get_account({accountID}).content, 'xml')\n{e}")
            else:
                try:
                    accounts.append(account_row(accountID, accountSoup))
                    for a in ('main', 'billing', 'mailing'):
                        if accountSoup.find(f'{a}Address'):
                            addresses.append(address_row('ACCOUNT', a.upper(), accountID, accountSoup.find(f'{a}Address')))
                    for b in ('account','serviceInfo'):
                        for cfvSoup in accountSoup.find_all(f'{b}CustomFieldValues'):
                            customFieldValues.append(custom_field_value_row(re.sub(RESUB,' ',b).upper(),accountID, cfvSoup))
                    # GROUP ACCOUNTS
                    groupAccountSoup = accountSoup.find('groupAccountInfo')
                    if groupAccountSoup:
                        groupAccounts.append(group_account_info_row(accountID,groupAccountSoup))
                        commonGroupAccountSoup = groupAccountSoup.find('commonGroupAccountInfo')
                        if commonGroupAccountSoup:
                            commonGroupAccounts.append(common_group_account_info_row(accountID, commonGroupAccountSoup))
                            groupBrokerageAccountSoup = commonGroupAccountSoup.find('brokerageAccountInfo')
                            if groupBrokerageAccountSoup:
                                brokerageAccounts.append(brokerage_account_info_row(accountID, 'GROUP', groupBrokerageAccountSoup))
                        for acSoup in groupAccountSoup.find_all('accountClasses'):
                            accountClasses.append(account_class_row(accountID, acSoup)) 
                        for adSoup in groupAccountSoup.find_all('accountDivisions'):
                            accountDivisions.append(account_division_row(accountID, adSoup))
                        for alSoup in groupAccountSoup.find_all('accountLocations'):
                            accountLocations.append(account_location_row(accountID, alSoup))
                            addresses.append(address_row('ACCOUNT', 'LOCATION', int(alSoup.find('locationID').text), alSoup.find('address')))
                        groupAccountIntegrationSoup = groupAccountSoup.find('accountIntegrationInfo')
                        # xml structure allows empty node for accountIntegrationInfo, so need to test length of "child" list
                        if len(groupAccountIntegrationSoup.find_all()) > 0:
                            accountIntegrations.append(account_integration_row(accountID, groupAccountIntegrationSoup))
                            if groupAccountIntegrationSoup.find('TAMCustomer'):
                                tamCustomers.append(tam_customer_row(accountID, groupAccountIntegrationSoup.find('TAMCustomer')))
                            if groupAccountIntegrationSoup.find('ams360AccountBusinessType'):
                                ams360Accounts.append(ams360_account_row(accountID, groupAccountIntegrationSoup.find('ams360AccountBusinessType')))
                        for etSoup in groupAccountSoup.find_all('employeeTypes'):
                            employeeTypes.append(employee_type_row(accountID, etSoup))
                        if groupAccountSoup.find('ACAMeasurementPeriod'):
                            acaMeasurementPeriods.append(aca_measurement_period_row(accountID, groupAccountSoup.find('ACAMeasurementPeriod')))
                    # INDIVIDUAL ACCOUNTS
                    individualAccountSoup = accountSoup.find('individualAccountInfo')
                    if individualAccountSoup:
                        individualAccounts.append(individual_account_info_row(accountID, individualAccountSoup))
                        persons.append(person_info_row('ACCOUNT', 'INDIVIDUAL', accountID, individualAccountSoup.find('personInfo')))
                        if individualAccountSoup.find('phone'):
                            phones.append(phone_row('ACCOUNT', 'INDIVIDUAL', accountID, individualAccountSoup.find('phone')))
                        if individualAccountSoup.find('brokerageAccountInfo'):
                            brokerageAccounts.append(brokerage_account_info_row(accountID, 'INDIVIDUAL', individualAccountSoup.find('brokerageAccountInfo')))
                        individualAccountIntegrationSoup = individualAccountSoup.find('accountIntegrationInfo')
                        # xml structure allows empty node for accountIntegrationInfo, so need to test length of "child" list
                        if len(individualAccountIntegrationSoup.find_all()) > 0:
                            accountIntegrations.append(account_integration_row(accountID, individualAccountIntegrationSoup))
                            if individualAccountIntegrationSoup.find('TAMCustomer'):
                                tamCustomers.append(tam_customer_row(accountID, individualAccountIntegrationSoup.find('TAMCustomer')))
                            if individualAccountIntegrationSoup.find('ams360AccountBusinessType'):
                                ams360Accounts.append(ams360_account_row(accountID, individualAccountIntegrationSoup.find('ams360AccountBusinessType')))
                        for dependentSoup in individualAccountSoup.find_all('dependents'):
                            dependents.append(dependent_row(accountID, dependentSoup))
                            persons.append(person_info_row('ACCOUNT', 'DEPENDENT', int(dependentSoup.find('dependentID').text),dependentSoup.find('personInfo')))
                    # MARKETING GROUP ACCOUNTS
                    marketingGroupAccountSoup = accountSoup.find('marketingGroupAccountInfo')
                    if marketingGroupAccountSoup:
                        marketingGroupAccounts.append(marketing_group_account_row(accountID,marketingGroupAccountSoup))
                        marketingCommonGroupAccountSoup = marketingGroupAccountSoup.find('commonGroupAccountInfo')
                        if marketingCommonGroupAccountSoup:
                            commonGroupAccounts.append(common_group_account_info_row(accountID, marketingCommonGroupAccountSoup))
                            if marketingCommonGroupAccountSoup.find('brokerageAccountInfo'):
                                brokerageAccounts.append(brokerage_account_info_row(accountID, 'MARKETING', marketingCommonGroupAccountSoup.find('brokerageAccountInfo')))
                    # AGENT ACCOUNTS
                    agentAccountSoup = accountSoup.find('agentAccountInfo')
                    if agentAccountSoup:
                        agentAccounts.append(pd.DataFrame([{'account_id':accountID,'agency_account_id':int(agentAccountSoup.find('agencyAccountID').text)}]) if agentAccountSoup.find('agencyAccountID') else None)
                        persons.append(person_info_row('ACCOUNT','AGENT',accountID,agentAccountSoup.find('personInfo')))
                        agentSoup = agentAccountSoup.find('agentInfo')
                        if agentSoup:
                            agencies.append(agency_info_row(accountID, 'AGENT', agentSoup))
                            for p in ['1','2','3','4']:
                                if agentSoup.find(f'phone{p}'):
                                    phones.append(phone_row('ACCOUNT',f'AGENT-{p}',accountID, agentSoup.find(f'phone{p}')))
                            for licenseSoup in agentSoup.find_all('licenses'):
                                licenses.append(license_row(accountID,licenseSoup))
                            for caSoup in agentSoup.find_all('carrierAppointments'):
                                carrierAppointments.append(carrier_appointment_row(accountID, caSoup))
                    # AGENCY ACCOUNTS
                    agencyAccountSoup = accountSoup.find('agencyAccountInfo')
                    if agencyAccountSoup:
                        agencyAccounts.append(pd.DataFrame([{'account_id':accountID, 'associated_agent_account_ids':', '.join([x.text for x in agencyAccountSoup.find_all('associatedAgenctAccountIDs')]) if agencyAccountSoup.find('associatedAgenctAccountIDs') else None}]))
                        agencySoup = agentAccountSoup.find('agencyInfo')
                        if agencySoup:
                            agencies.append(agency_info_row(accountID, 'AGENCY', agencySoup))
                            for p in ['1','2','3','4']:
                                if agentSoup.find(f'phone{p}'):
                                    phones.append(phone_row('ACCOUNT',f'AGENCY-{p}',accountID, agentSoup.find(f'phone{p}')))
                            for licenseSoup in agentSoup.find_all('licenses'):
                                licenses.append(license_row(accountID,licenseSoup))
                            for caSoup in agentSoup.find_all('carrierAppointments'):
                                carrierAppointments.append(carrier_appointment_row(accountID, caSoup))
                    # ACCOUNT CUSTOM FIELD OPTION VALUES
                    for cfovSoup in accountSoup.find_all('accountCustomFieldOptionValues'):
                        customFieldOptionValues.append(custom_field_option_value_row('ACCOUNT',accountID,cfovSoup))
                    # ACCOUNT TEAM MEMBERS
                    for atmSoup in accountSoup.find_all('accountTeamMembers'):
                        accountTeamMembers.append(account_team_member_row(accountID, atmSoup))
                except Exception as e:
                    lf.error(f"unable to parse dataframe(s) for {accountID}\n{e}")
        # CONVERT DICTIONARY LISTS TO DATAFRAMES, STAGE IN DATABASE, UPSERT, DROP STAGE TABLE                    
        stages = {
            'account':pd.DataFrame(accounts) if accounts else None,
            'address':pd.DataFrame(addresses) if addresses else None,
            'custom_field_value':pd.DataFrame(customFieldValues) if customFieldValues else None,
            'group_account_info':pd.DataFrame(groupAccounts) if groupAccounts else None,
            'common_group_account_info':pd.DataFrame(commonGroupAccounts) if commonGroupAccounts else None,
            'brokerage_account_info':pd.DataFrame(brokerageAccounts) if brokerageAccounts else None,
            'account_class':pd.DataFrame(accountClasses) if accountClasses else None,
            'account_division':pd.DataFrame(accountDivisions) if accountDivisions else None,
            'account_location':pd.DataFrame(accountLocations) if accountLocations else None,
            'account_integration_info':pd.DataFrame(accountIntegrations) if accountIntegrations else None,
            'tam_customer':pd.DataFrame(tamCustomers) if tamCustomers else None,
            'ams360_account_business_type':pd.DataFrame(ams360Accounts) if ams360Accounts else None,
            'employee_type':pd.DataFrame(employeeTypes) if employeeTypes else None,
            'aca_measurement_period':pd.DataFrame(acaMeasurementPeriods) if acaMeasurementPeriods else None,
            'person_info':pd.DataFrame(persons) if persons else None,
            'phone':pd.DataFrame(phones) if phones else None,
            'dependent':pd.DataFrame(dependents) if dependents else None,
            'marketing_group_account_info':pd.DataFrame(marketingGroupAccounts) if marketingGroupAccounts else None,
            'agency_account_info':pd.DataFrame(agencyAccounts) if agencyAccounts else None,
            'agent_account_info':pd.DataFrame(agentAccounts) if agentAccounts else None,
            'agency_info':pd.DataFrame(agencies) if agencies else None,
            'license':pd.DataFrame(licenses) if licenses else None,
            'carrier_appointment':pd.DataFrame(carrierAppointments) if carrierAppointments else None,
            'custom_field_option_value':pd.DataFrame(customFieldOptionValues) if customFieldOptionValues else None,
            'account_team_member':pd.DataFrame(accountTeamMembers) if accountTeamMembers else None
        }
        for s in stages: 
            if stages[s] is not None:
                try:
                    rcs = stages[s].to_sql(f'stg_{s}', ENGINE, 'benefitpoint', 'replace', index=False, chunksize=10000, method='multi')
                except Exception as e:
                    lf.error(f"{s}.to_sql()\n{e}")
                else:
                    if rcs > 0:
                        lf.info(f"{rcs} rows staged in stg_{s}")
                        try:
                            rcu = mjdb.upsert_stage('benefitpoint',s, 'upsert')
                        except Exception as e:
                            lf.error(f"mjdb.upsert_stage('benefitpoint',{s})\n{e}")
                        else:
                            lf.info(f"mjdb.upsert_stage('benefitpoint',{s}) affected {rcu} row(s).")
                        if s == 'custom_field_value':
                            # remove deleted custom_field_value_ids from custom_field_value for account_id (source_key)
                            try:
                                rcd = mjdb.function_execute('benefitpoint','sp_custom_field_value_cleanup')
                            except Exception as e:
                                lf.error(f"unable to execute benefitpoint.sp_custom_field_value_cleanup()\n{e}")
                            else:
                                lf.info(f"{rcd} record(s) deleted for CustomFieldValues")
                finally:
                    mjdb.drop_table('benefitpoint', f"stg_{s}")
if __name__ == '__main__':
    main()