import os
import bpws
import mjdb
import common as cmn
from datetime import datetime
from xml.etree import ElementTree as ET

LOGDIR = 'benefitpoint_etl'
### INSTANTIATE LOGGER
lf = cmn.log_filer(LOGDIR, 'accounts')

def parse_account_response(xmldata):
    accountXml = ET.fromstring(xmldata)
    accountResponse = {'account':ET.tostring(accountXml, 'unicode', 'xml')}
    
    # Address Complex Type (one-to-one) -- main address for the account
    # changing tag name allows for less complex DML statements/logic
    if accountXml.find('mainAddress'):
        mainAddress = accountXml.find('mainAddress')
        mainAddress.tag = 'address'
        accountResponse['mainAddress'] = ET.tostring(mainAddress, 'unicode', 'xml')
    
    # Address Complex Type (one-to-one) -- billing address for the account
    if accountXml.find('billingAddress'):
        billingAddress = accountXml.find('billingAddress')
        billingAddress.tag = 'address'
        accountResponse['billingAddress'] = ET.tostring(billingAddress, 'unicode', 'xml')
    
    # Address Complex Type (one-to-one) -- mailing address for the account
    if accountXml.find('mailingAddress'):
        mailingAddress = accountXml.find('mailingAddress')
        mailingAddress.tag = 'address'
        accountResponse['mailingAddress'] = ET.tostring(mailingAddress, 'unicode', 'xml') 
    
    # CustomFieldValue Complex Type (one-to-many) -- custom field values for the account information section of the account
    accountCustomFieldValues = []
    for val in accountXml.findall('accountCustomFieldValues'):
        val.tag = val.tag.replace('account', '')
        accountCustomFieldValues.append(ET.tostring(val, 'unicode', 'xml'))
    accountResponse['accountCustomFieldValues'] = accountCustomFieldValues if accountCustomFieldValues else None
    
    # CustomFieldValue Complex Type (one-to-many) -- custom field values for the service information section of the account
    serviceInfoCustomFieldValues = []
    for val in accountXml.findall('serviceInfoCustomFieldValues'):
        val.tag = val.tag.replace('serviceInfo', '')
        serviceInfoCustomFieldValues.append(ET.tostring(val, 'unicode', 'xml'))
    accountResponse['serviceInfoCustomFieldValues'] = serviceInfoCustomFieldValues if serviceInfoCustomFieldValues else None
    
    # GroupAccountInfo Complex Type (optional, one-to-one) -- information about group accounts
    if accountXml.find('groupAccountInfo'):
        gaInfo = accountXml.find('groupAccountInfo')
        groupAccountInfo = {'gaInfo':ET.tostring(gaInfo, 'unicode', 'xml')}
        # CommonGroupAccountInfo Complex Type (optional, one-to-one) -- common group account information, applies to Group and Marketing Group accounts
        if gaInfo.find('commonGroupAccountInfo'):
            info = gaInfo.find('commonGroupAccountInfo')
            commonGroupAccountInfo = {
                'info':ET.tostring(info, 'unicode', 'xml'),
                'brokerageAccountInfo':(ET.tostring(info.find('brokerageAccountInfo'), 'unicode', 'xml') if info.find('brokerageAccountInfo') else None)
            }
            groupAccountInfo['commonGroupAccountInfo'] = commonGroupAccountInfo
        # multiplePayrollCyclesDifferBy (string, one-to-many) -- if the account has multiple payroll cycles, the values they differ by
        multiplePayrollCyclesDifferBy = []
        for each in gaInfo.findall('multiplePayrollCyclesDifferBy'):
            multiplePayrollCyclesDifferBy.append(each.text)
        groupAccountInfo['multiplePayrollCyclesDifferBy'] = multiplePayrollCyclesDifferBy if multiplePayrollCyclesDifferBy else None
        # AccountClass Complex Type (one-to-many) -- designated group of employees, split out for benefits purposes
        accountClasses = []
        for each in gaInfo.findall('accountClasses'):
            accountClasses.append({
                'id':each.find('classID').text,
                'xml':ET.tostring(each, 'unicode', 'xml')
            } )
        groupAccountInfo['accountClasses'] = accountClasses if accountClasses else None
        # AccountDivision Complex Type (one-to-many) -- designated units of the account's business, split out for benefits purposes
        accountDivisions = []
        for each in gaInfo.findall('accountDivisions'):
            accountDivisions.append({
                'id':each.find('divisionID').text,
                'xml':ET.tostring(each, 'unicode', 'xml')
            })
        groupAccountInfo['accountDivisions'] = accountDivisions if accountDivisions else None
        # AccountLocation Complex Type (one-to-many) -- addresses for the account
        accountLocations = []
        for each in gaInfo.findall('accountLocations'):
            accountLocations.append({
                'id':each.find('locationID').text,
                'xml':ET.tostring(each, 'unicode', 'xml'),
                'address':ET.tostring(each.find('address'), 'unicode', 'xml')
            })
        groupAccountInfo['accountLocations'] = accountLocations if accountLocations else None
        # AccountIntegrationInfo Complex Type (optional, one-to-one) -- account integration information
        if gaInfo.find('accountIntegrationInfo'):
            info = gaInfo.find('accountIntegrationInfo')
            accountIntegrationInfo = {
                'id':info.find('sagittaClientID').text,
                'xml':ET.tostring(info, 'unicode', 'xml')
                
            }
            if info.find('TAMCustomer'):
                accountIntegrationInfo['TAMCustomer'] = {
                    'id':info.find('customerCode').text,
                    'xml':(ET.tostring(info.find('TAMCustomer'),'unicode','xml') )
                }
            if info.find('ams360AccountBusinessType'):
                accountIntegrationInfo['ams360AccountBusinessType'] = ET.tostring(info.find('ams360AccountBusinessType'), 'unicode', 'xml')
            groupAccountInfo['accountIntegrationInfo'] = accountIntegrationInfo
        # EmployeeType Complex Type (one-to-many)
        employeeTypes = []
        for each in gaInfo.findall('employeeTypes'):
            employeeTypes.append(ET.tostring(each, 'unicode', 'xml'))        
        groupAccountInfo['employeeTypes'] = employeeTypes if employeeTypes else None
        # ACAMeasurementPeriodType Complex Type (optional), ACA Measurement Period for the account.
        if gaInfo.find('ACAMeasurementPeriod'):
            groupAccountInfo['ACAMeasurementPeriod'] = ET.tostring(gaInfo.find('ACAMeasurementPeriod'), 'unicode', 'xml')
        accountResponse['groupAccountInfo'] = groupAccountInfo if groupAccountInfo is not None else None
    
    # IndividualAccountInfo Complex Type (optional) -- information about individual accounts
    if accountXml.find('individualAccountInfo'):
        iaInfo = accountXml.find('individualAccountInfo')
        individualAccountInfo = {'iaInfo':ET.tostring(iaInfo, 'unicode', 'xml')}
        # PersonInfo Complex Type (one-to-one) -- personal information for the individual
        individualAccountInfo['personInfo'] = ET.tostring(iaInfo.find('personInfo'), 'unicode', 'xml')
        # Phone Complex Type (optional, one-to-one) -- phone number for the individual
        individualAccountInfo['phone'] = ET.tostring(iaInfo.find('phone'), 'unicode', 'xml') if iaInfo.find('phone') else None
        # BrokerageAccountInfo Complex Type (optional, one-to-one) -- common brokerage account information; applies to Group, Market Group, and Individual accounts
        individualAccountInfo['brokerageAccountInfo'] = ET.tostring(iaInfo.find('brokerageAccountInfo'), 'unicode', 'xml') if iaInfo.find('brokerageAccountInfo') else None
        # AccountIntegrationinfo Complex Type (optional, one-to-one) -- account integration info
        if iaInfo.find('accountIntegrationInfo'):
            info = iaInfo.find('accountIntegrationInfo')
            accountIntegrationInfo = {
                'info':ET.tostring(info, 'unicode', 'xml'),
                'tamCustomer':(ET.tostring(info.find('TAMCustomer'),'unicode','xml') if info.find('TAMCustomer') else None),
                'ams360AccountBusinessType':(ET.tostring(info.find('ams360AccountBusinessType'), 'unicode', 'xml') if info.find('ams360AccountBusinessType') else None)
            }
            individualAccountInfo['accountIntegrationInfo'] = accountIntegrationInfo
        # Dependent Complex Type (one-to-many) -- dependents associated to the individual
        dependents = []
        for each in iaInfo.findall('dependents'):
            dependents.append({
                'dependent':{
                    'id':each.find('dependentID').text,
                    'xml':ET.tostring(each, 'unicode', 'xml')
                },
                'person':ET.tostring(each.find('personInfo'), 'unicode', 'xml')
            })
        individualAccountInfo['dependents'] = dependents
        accountResponse['individualAccountInfo'] = individualAccountInfo
    
    # MarketingGroupAccountInfo Complex Type (optional, one-to-one) -- information about marketing group accounts
    if accountXml.find('marketingGroupAccountInfo'):
        mgaInfo = accountXml.find('marketingGroupAccountInfo')
        marketingGroupAccountInfo = {'mgaInfo':ET.tostring(mgaInfo, 'unicode', 'xml')}
        # associatedAccountIDs Element (one-to-many), integer -- IDs of accounts that are included in the marketing group.
        associatedAccounts = []
        for each in mgaInfo.findall('associatedAccountIDs'):
               associatedAccounts.append(each.text)
        marketingGroupAccountInfo['associatedAccounts'] = associatedAccounts if associatedAccounts else None
        # CommonGroupAccountInfo Complex Type (optional, one-to-one) -- common group account information; applies to Group and Marketing Group accounts
        if mgaInfo.find('commonGroupAccountInfo'):
            info = mgaInfo.find('commonGroupAccountInfo')
            commonGroupAccountInfo = {
                'info':ET.tostring(info, 'unicode', 'xml'),
                'brokerageAccountInfo':(ET.tostring(info.find('brokerageAccountInfo'), 'unicode', 'xml') if info.find('brokerageAccountInfo') else None)
            }
            marketingGroupAccountInfo['commonGroupAccountInfo'] = commonGroupAccountInfo
        accountResponse['marketingGroupAccountInfo'] = marketingGroupAccountInfo
    
    # AgentAccountInfo Complex Type (optional, one-to-one) -- information about agent accounts
    # required and only applicable for accounts with 'Agent' account classification
    if accountXml.find('agentAccountInfo'):
        aaInfo = accountXml.find('agentAccountInfo')
        agentAccountInfo = {'xml':ET.tostring(aaInfo, 'unicode', 'xml')}
        # PersonInfo Complex Type (one-to-one) -- personal information for agent
        agentAccountInfo['personInfo'] = ET.tostring(aaInfo.find('personInfo'), 'unicode', 'xml') if aaInfo.find('personInfo') else None
        # AgencyInfo Complex Type (optional, one-to-one) -- agent account details
        if aaInfo.find('agentInfo'):
            info = aaInfo.find('agentInfo')
            agentInfo = {'xml':ET.tostring(info, 'unicode', 'xml')}
            # iterate potential phone numbers
            phones = []
            for i in range(1,4):
                # Phone Complex Type
                if info.find(f'phone{i}'):
                    phone = info.find(f'phone{i}')
                    phone.tag = 'phone'
                    phones.append({
                        'id':i
                        ,'xml':ET.tostring(phone, 'unicode', 'xml')
                    })
            agentInfo['phones'] = phones if phones else None 
            # License Complex Type (one-to-many) -- Licenses for the agency.
            licenses = []
            for each in info.findall('licenses'):
                licenses.append({
                    'id':each.find('licenseID').text,
                    'xml':ET.tostring(each, 'unicode', 'xml')
                })
            agentInfo['licenses'] = licenses if licenses else None
            # CarrierAppointment Complex Type (one-to-many) -- Appointments made with carriers.
            carrierAppointments = []
            for each in info.findall('carrierAppointments'):
                carrierAppointments.append({
                    'carrierAppointmentId':each.find('carrierAppointmentID').text,
                    'xml':ET.tostring(each, 'unicode', 'xml')
                })
            agentInfo['carrierAppointments'] = carrierAppointments if carrierAppointments else None
            associatedAccounts = []
            for each in info.findall('associatedAccountIDs'):
                associatedAccounts.append(each.text)
            agentInfo['associatedAccounts'] = associatedAccounts if associatedAccounts else None
            agentAccountInfo['agentInfo'] = agentInfo
        accountResponse['agentAccountInfo'] = agentAccountInfo
    
    # AgencyAccountInfo Complex Type (optional, one-to-one) -- information about agency accounts
    # required and only applicable for accounts with the 'Agency' account classification
    if accountXml.find('agencyAccountInfo'):
        aaInfo = accountXml.find('agencyAccountInfo')
        agencyAccountInfo = {'xml':ET.tostring(aaInfo, 'unicode', 'xml')}
        # AgencyInfo Complex Type (optional, one-to-one) agency account details
        if aaInfo.find('agencyInfo'):
            info = aaInfo.find('agencyInfo')
            agencyInfo = {'xml':ET.tostring(info, 'unicode', 'xml')}
            # iterate potential phone numbers
            phones = []
            for i in range(1,4):
                # Phone Complex Type
                if info.find(f'phone{i}'):
                    phone = info.find(f'phone{i}')
                    phone.tag = 'phone'
                    phones.append({
                        'id':i,
                        'xml':ET.tostring(phone, 'unicode', 'xml')
                    })
            agencyInfo['phones'] = phones if phones else None
            # License Complex Type (one-to-many) -- Licenses for the agency
            licenses = []
            for each in info.findall('licenses'):
                licenses.append({
                    'id':each.find('licenseID').text,
                    'xml':ET.tostring(each, 'unicode', 'xml')
                })
            agencyInfo['licenses'] = licenses if licenses else None
            #CarrierAppointment Complex Type (one-to-many) -- Appointments made with carriers.
            carrierAppointments = []
            for each in info.findall('carrierAppointments'):
                carrierAppointments.append({
                    'id':each.find('carrierAppointmentID').text,
                    'xml':ET.tostring(each, 'unicode', 'xml')
                })
            agencyInfo['carrierAppointments'] = carrierAppointments if carrierAppointments else None
            associatedAccountIDs = []
            for each in info.findall('associatedAccountIDs'):
                associatedAccountIDs.append(each.text)
            agencyInfo['associatedAccountIDs'] = associatedAccountIDs if associatedAccountIDs else None
            agencyAccountInfo['agencyInfo'] = agencyInfo
        # associatedAgentAccountIDs (int, one-to-many) -- IDs of the agent accounts that are associated to the agency account
        associatedAgentAccountIDs = []
        for each in aaInfo.findall('associatedAgentAccountIDs'):
            associatedAgentAccountIDs.append(each.text)            
        agencyAccountInfo['associatedAgentAccountIDs'] = associatedAgentAccountIDs if associatedAgentAccountIDs else None
        accountResponse['agencyAccountInfo'] = agencyAccountInfo

    # CustomFieldOptionValue Complex Type (one-to-many) -- Custom field option values for the account
    accountCustomFieldOptionValues = []
    for each in accountXml.findall('accountCustomFieldOptionValues'):
        each.tag = 'customFieldOptionValue'
        accountCustomFieldOptionValues.append({
            'id':each.find('customFieldOptionValueID').text,
            'xml':ET.tostring(each, 'unicode', 'xml')
        })
    accountResponse['accountCustomFieldOptionValues'] = accountCustomFieldOptionValues if accountCustomFieldOptionValues else None

    # AccountRelationship Complex Type (one-to-many) -- information about account relationships
    accountRelationship = []
    for each in accountXml.findall('accountRelationship'):
        arInfo = {'info':ET.tostring(each, 'unicode', 'xml')}
        # AccountSummary Complex Type (one-to-many) -- The accounts that are part of the account relationship.
        accounts = []
        for account in each.findall('account'):
            accounts.append(ET.tostring(account, 'unicode', 'xml'))
        arInfo['accounts'] = accounts
    accountResponse['accountRelationship'] = accountRelationship if accountRelationship else None

    # AccountTeamMember Complex Type (one-to-many) -- Additional team members on the account.
    # Some roles may be required by brokerage customization.
    accountTeamMembers = []
    for each in accountXml.findall('accountTeamMembers'):
        atmInfo = {
            'id':each.find('userID').text,
            'xml':ET.tostring(each, 'unicode', 'xml')
        }
        # Office Complex Type (optional) -- The office associated to the team member
        # Read-only on createAccount() and updateAccount()
        if each.find('office'):
            atmInfo['office'] = {
                'id':each.find('office').find('officeID').text,
                'xml':ET.tostring(each.find('office'), 'unicode', 'xml')
            }
        accountTeamMembers.append(atmInfo)
    accountResponse['accountTeamMembers'] = accountTeamMembers if accountTeamMembers else None

    return accountResponse

def upsert_account_entities (accountId, account):
    try:
        rc = mjdb.bp_account_entity_upsert('account', accountId, account['account'])
    except Exception as e:
        raise ValueError(f"mjdb.bp_account_entity_upsert('account', {accountId}, <<account['account']>>)\n{e}")
    else:
        if rc > 0:
            lf.info(f"mjdb.bp_account_enitity_upsert('account', {accountId}, <<account['account']>>) successfully upserted {rc} row(s).")
        for address in ['mainAddress', 'billingAddress', 'mailingAddress']:
            if address in account.keys():
                try:
                    rc = mjdb.bp_address_upsert('ACCOUNT', address.replace('Address','').upper(), accountId, account[address])
                except Exception as e:
                    raise ValueError(f"mjdb.bp_address_upsert('ACCOUNT', {address.replace('Address','').upper()}, {accountId}, <<account[address]>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_address_upsert('ACCOUNT', '{address.replace('Address','').upper()}', {accountId}, <<account[address]>>) successfully upserted {rc} row(s).")
        if account['accountCustomFieldValues'] is not None:
            for cfv in account['accountCustomFieldValues']:
                try:
                    rc = mjdb.bp_custom_field_value_upsert('ACCOUNT', accountId, cfv)
                except Exception as e:
                    raise ValueError(f"mjdb.bp_custom_field_value_upsert('ACCOUNT', {accountId}, <<cfv>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_custom_field_value_upsert('ACCOUNT', {accountId}, <<cfv>>) successfully upserted {rc} row(s).")
        if account['serviceInfoCustomFieldValues'] is not None:
            for cfv in account['serviceInfoCustomFieldValues']:
                try:
                    rc = mjdb.bp_custom_field_value_upsert('SERVICE INFO', accountId, cfv)
                except Exception as e:
                    raise ValueError(f"mjdb.bp_custom_field_value_upsert('SERVICE INFO', {accountId}, <<cfv>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_custom_field_value_upsert('SERVICE INFO', {accountId}, <<cfv>>) successfully upserted {rc} row(s).")
        if 'groupAccountInfo' in account.keys():
            try:
                rc = mjdb.bp_account_entity_upsert('group_account_info', accountId, account['groupAccountInfo']['gaInfo'])
            except Exception as e:
                raise ValueError(f"mjdb.bp_account_entity_upsert('group_account_info', {accountId}, <<account['groupAccountInfo']['gaInfo']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('group_account_info', {accountId}, <<account['groupAccountInfo']['gaInfo']>>) successfully upserted {rc} row(s).")
                if account['groupAccountInfo']['commonGroupAccountInfo']:
                    try:
                        rc = mjdb.bp_account_entity_upsert('common_group_account_info', accountId, account['groupAccountInfo']['commonGroupAccountInfo']['info'])
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_entity_upsert('common_group_account_info', {accountId}, <<account['groupAccountInfo']['commonGroupAccountInfo']['info']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('common_group_account_info', {accountId}, <<account['groupAccountInfo']['commonGroupAccountInfo']['info']>>) successfully upserted {rc} row(s).")
                        if account['groupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo']:
                            try:
                                rc = mjdb.bp_account_entity_upsert('brokerage_account_info', accountId, account['groupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo'], accountType='GROUP')
                            except Exception as e:
                                raise ValueError(f"mjdb.bp_account_entity_upsert('brokerage_account_info', {accountId}, <<account['groupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo']>>, accountType='GROUP')\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_account_entity_upsert('brokerage_account_info', {accountId}, <<account['groupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo']>>, accountType='GROUP') successfully upserted {rc} row(s).")
                if account['groupAccountInfo']['accountClasses']:
                    for ac in account['groupAccountInfo']['accountClasses']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('account_class', accountId, ac['xml'], entityId=ac['id'])
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_entity_upsert('account_class', {accountId}, <<ac['xml']>>, classId={ac['id']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('account_class', {accountId}, <<ac['xml']>>, classId={ac['id']}) successfully upserted {rc} row(s).")
                if account['groupAccountInfo']['accountDivisions']:
                    for div in account['groupAccountInfo']['accountDivisions']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('account_division', accountId, div['xml'], entityId=div['id'])
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_entity_upsert('account_division', {accountId}, <<div['xml']>>, entityId={div['id']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('account_division', {accountId}, <<div['xml']>>, entityId={div['id']}) successfully upserted {rc} row(s).")
                if account['groupAccountInfo']['accountLocations']:
                    for loc in account['groupAccountInfo']['accountLocations']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('account_location', accountId, loc['xml'], entityId=loc['id'])
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_entity_upsert('account_location', {accountId}, <<loc['xml']>>, entityId={loc['id']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('account_location', {accountId}, <<loc['xml']>>, entityId={loc['id']}) successfully upserted {rc} rows(s).")
                            try:
                                rc = mjdb.bp_account_entity_upsert('address', loc['id'], loc['address'], addressSource='ACCOUNT', sourceType='LOCATION')
                            except Exception as e:
                                raise ValueError(f"mjdb.bp_account_entity_upsert('address', {loc['id']}, <<loc['address']>>, addressSource='ACCOUNT', sourceType='LOCATION')\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_account_entity_upsert('address', {loc['id']}, <<loc['address']>>, addressSource='ACCOUNT', sourceType='LOCATION') successfully upserted {rc} row(s).")
                if 'accountIntegrationInfo' in account['groupAccountInfo'].keys():
                    try:
                        rc = mjdb.bp_account_entity_upsert('account_integration_info', accountId, account['groupAccountInfo']['accountIntegrationInfo']['xml'])
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_entity_upsert('account_integration_info', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['xml']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('account_integration_info', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['xml']>>) successfully upserted {rc} row(s).")
                        if 'TAMCustomer' in account['groupAccountInfo']['accountIntegrationInfo'].keys():
                            try:
                                rc = mjdb.bp_account_entity_upsert('tam_cusomter', accountId, account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['xml'], entityId=account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['id'])
                            except Exception as e:
                                raise ValueError(f"mjdb.bp_account_entity_upsert('tam_cusomter', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['xml']>>, entityId={account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['id']})\n{e}")
                            else:
                                lf.info(f"mjdb.bp_account_entity_upsert('tam_cusomter', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['xml']>>, entityId={account['groupAccountInfo']['accountIntegrationInfo']['tamCustomer']['id']}) successfully upserted {rc} row(s).")
                        if 'ams360AccountBusinessType' in account['groupAccountInfo']['accountIntegrationInfo'].keys():
                            try:
                                rc = mjdw.bp_account_entity_upsert('ams360_account_business_type', accountId, account['groupAccountInfo']['accountIntegrationInfo']['ams360AccountBusinessType'])
                            except Exception as e:
                                raise ValueError(f"mjdw.bp_account_entity_upsert('ams360_account_business_type', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['ams360AccountBusinessType']>>)\n{e}")
                            else:
                                lf.info(f"mjdw.bp_account_entity_upsert('ams360_account_business_type', {accountId}, <<account['groupAccountInfo']['accountIntegrationInfo']['ams360AccountBusinessType']>>) successfully upserted {rc} rows(s).")
                if account['groupAccountInfo']['employeeTypes'] is not None:
                    for each in account['groupAccountInfo']['employeeTypes']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('employee_type', accountId, each)
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_entity_upsert('employee_type', {accountId}, <<each>>)\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('employee_type', {accountId}, <<each>>) successfully upserted {rc} row(s).")
                if 'ACAMeasurementPeriod' in account['groupAccountInfo'].keys():
                    try:
                        rc = mjdb.bp_account_entity_upsert('aca_measurement_period', accountId, account['groupAccountInfo']['ACAMeasurementPeriod'])
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_entity_upsert('aca_measurement_period', {accountId}, <<account['groupAccountInfo']['ACAMeasurementPeriod']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('aca_measurement_period', {accountId}, <<account['groupAccountInfo']['ACAMeasurementPeriod']>>) successfully upserted {rc} row(s).")
        if 'individualAccountInfo' in account.keys():
            try:
                rc = mjdb.bp_account_entity_upsert('individual_account_info', accountId, account['individualAccountInfo']['iaInfo'])
            except Exception as e:
                raise ValueError(f"mjdb.bp_account_entity_upsert('individual_account_info', {accountId}, <<account['individualAccountInfo']['iaInfo']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('individual_account_info', {accountId}, <<account['individualAccountInfo']['iaInfo']>>) successfully upserted {rc} row(s).")
            try:
                rc = mjdb.bp_account_entity_upsert('person_info', accountId, account['individualAccountInfo']['personInfo'], source='ACCOUNT', sourceType='INDIVIDUAL')
            except Exception as e:
                raise ValueError(f"mjdb.bp_account_entity_upsert('person_info', {accountId}, <<account['individualAccountInfo']['personInfo']>>, source='ACCOUNT', sourceType='INDIVIDUAL')\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('person_info', {accountId}, <<account['individualAccountInfo']['personInfo']>>, source='ACCOUNT', sourceType='INDIVIDUAL') successfully upserted {rc} row(s).")
            if account['individualAccountInfo']['phone']:
                try:
                    rc = mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'INDIVIDUAL', int(accountId)))
                except Exception as e:
                    raise ValueError(f"mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'INDIVIDUAL', {int(accountId)}))\n{e}")
                try:
                    rc = mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT','INDIVIDUAL', int(accountId), account['individualAccountInfo']['phone']))
                except Exception as e:
                    raise ValueError(f"mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT','INDIVIDUAL', {int(accountId)}, <<account['individualAccountInfo']['phone']>>))\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT','INDIVIDUAL', {int(accountId)}, <<account['individualAccountInfo']['phone']>>)) affected {rc} row(s).")
            if account['individualAccountInfo']['brokerageAccountInfo']:
                try:
                    rc = mjdb.bp_account_entity_upsert('brokerage_account_info', int(accountId), account['individualAccountInfo']['brokerageAccountInfo'], accountType='INDIVIDUAL')
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_entity_upsert('brokerage_account_info', {int(accountId)}, <<account['individualAccountInfo']['brokerageAccountInfo']>>, accountType='INDIVIDUAL')\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('brokerage_account_info', {int(accountId)}, <<account['individualAccountInfo']['brokerageAccountInfo']>>, accountType='INDIVIDUAL') successfully upserted {rc} row(s).")
            if account['individualAccountInfo']['dependents']:
                for each in account['individualAccountInfo']['dependents']:
                    try:
                        rc = mjdb.bp_account_entity_upsert('dependent', int(accountId), each['dependent']['xml'])
                    except Exception as e:
                        raise ValueError (f"mjdb.bp_account_entity_upsert('dependent', {int(accountId)}, <<each['dependent']['xml']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('dependent', {int(accountId)}, <<each['dependent']['xml']>>) successfully upserted {rc} row(s).")
                        try:
                            rc = mjdb.bp_account_entity_upsert('person_info', each['dependent']['id'], each['person'], source='ACCOUNT', sourceType='DEPENDENT')
                        except Exception as e:
                            raise ValueError (f"mjdb.bp_account_entity_upsert('person_info', {each['dependent']['id']}, <<each['person']>>, source='ACCOUNT', sourceType='DEPENDENT')\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('person_info', {each['dependent']['id']}, <<each['person']>>, source='ACCOUNT', sourceType='DEPENDENT') successfully upserted {rc} row(s).")
        if 'marketingGroupAccountInfo' in account.keys():
            try:
                rc = mjdb.bp_account_entity_upsert('marketing_group_account_info', accountId, account['marketingGroupAccountInfo']['mgaInfo'])
            except Exception as e:
                raise ValueError(f"mjdb.bp_account_entity_upsert('marketing_group_account_info', {accountId}, <<account['marketingGroupAccountInfo']['mgaInfo']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('marketing_group_account_info', accountId, account['marketingGroupAccountInfo']['mgaInfo']) successfully upserted {rc} rows(s).")
            if 'associatedAccounts' in account['marketingGroupAccountInfo'].keys():
                # delete exsiting relationships first -- avoids orphaned and/or duplicated relationships
                try:
                    rc = mjdb.bp_account_relationship_delete('marketing_group_account_associated_accounts', accountId)
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_relationship_delete('marketing_group_account_associated_accounts', {accountId})\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_relationship_delete('marketing_group_account_associated_accounts', accountId) successfully deleted {rc} row(s).")
                for each in account['marketingGroupAccountInfo']['associatedAccounts']:
                    try:
                        rc = mjdb.bp_account_realtionship_insert('marketing_group_account_associated_accounts', accountId, each)
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_relationship_insert('marketing_group_account_associated_accounts', {accountId}, {each})\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_relationship_insert('marketing_group_account_associated_accounts', {accountId}, {each}) successfully inserted {rc} row(s).")
            if 'commonGroupAccountInfo' in account['marketingGroupAccountInfo'].keys():
                try:
                    rc = mjdb.bp_account_entity_upsert('common_group_account_info', accountId, account['marketingGroupAccountInfo']['commonGroupAccountInfo']['info'])
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_entity_upsert('common_group_account_info', {accountId}, <<account['marketingGroupAccountInfo']['commonGroupAccountInfo']['info']>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('common_group_account_info', accountId, account['marketingGroupAccountInfo']['commonGroupAccountInfo']['info']) successfully upserted {rc} row(s).")
                if 'brokerageAccountInfo' in account['marketingGroupAccountInfo']['commonGroupAccountInfo'].keys():
                    try:
                        rc = mjdb.bp_account_entity_upsert('brokerage_account_info', accountId, account['marketingGroupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo'], accountType='MARKETING GROUP')
                    except Exception as e:
                        raise ValueError (f"mjdb.bp_account_entity_upsert('brokerage_account_info', {accountId}, <<account['marketingGroupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo']>>, accountType='MARKETING GROUP')\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('brokerage_account_info', {accountId}, <<account['marketingGroupAccountInfo']['commonGroupAccountInfo']['brokerageAccountInfo']>>, accountType='MARKETING GROUP') successully upserted {rc} row(s).")
        if 'agentAccountInfo' in account.keys():
            try:
                rc = mjdb.bp_account_entity_upsert('agent_account_info', accountId, account['agentAccountInfo']['xml'])
            except Exception as e:
                raise ValueError (f"mjdb.bp_account_entity_upsert('agent_account_info', {accountId}, <<account['agentAccountInfo']['xml']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('agent_account_info', {accountId}, <<account['agentAccountInfo']['xml']>>) successfully upserted {rc} row(s).")
            if account['agentAccountInfo']['personInfo'] is not None:
                try:
                    rc = mjdb.bp_account_entity_upsert('person_info', accountId, account['agentAccountInfo']['personInfo'], source='ACCOUNT', sourceType='AGENT')
                except Exception as e:
                    raise ValueError (f"mjdb.bp_account_entity_upsert('person_info', {accountId}, <<account['agentAccountInfo']['personInfo']>>, source='ACCOUNT', sourceType='AGENT')\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('person_info', {accountId}, <<account['agentAccountInfo']['personInfo']>>, source='ACCOUNT', sourceType='AGENT') successfully upserted {rc} row(s).")
            if 'agentInfo' in account['agentAccountInfo'].keys():
                try:
                    rc = mjdb.bp_account_entity_upsert('agent_info', accountId, account['agentAccountInfo']['agentInfo']['xml'])
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_entity_upsert('agent_info', {accountId}, <<account['agentAccountInfo']['agentInfo']['xml']>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('agent_info', {accountId}, <<account['agentAccountInfo']['agentInfo']['xml']>>) successfully upserted {rc} row(s).")
                if account['agentAccountInfo']['agentInfo']['phones'] is not None:
                    try:
                        rc = mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'AGENT', int(accountId)))
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'AGENT', {int(accountId)}))\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'AGENT', {int(accountId)})) affected {rc} row(s)")
                    for each in account['agentAccountInfo']['agentInfo']['phones']:
                        try:
                            rc = mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENT', int(accountId), each['xml']))
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENT', {int(accountId)}, <<each['xml']>>))\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENT', {int(accountId)}, <<each['xml']>>)) affected {rc} row(s).")
                if account['agentAccountInfo']['agentInfo']['licenses'] is not None:
                    for each in account['agentAccountInfo']['agentInfo']['licenses']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('license', int(accountId), each['xml'], entityId=each['id'])
                        except Exception as e:
                            raise ValueError (f"mjdb.bp_account_entity_upsert('license', {int(accountId)}, <<each['xml']>>, entityId={each['id']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('license', {int(accountId)}, <<each['xml']>>, entityId={each['id']}) successfully upserted {rc} row(s).")
                if account['agentAccountInfo']['agentInfo']['carrierAppointments'] is not None:
                    for each in account['agentAccountInfo']['agentInfo']['carrierAppointments']:
                        try:
                            rc = mjdb.bp_account_entity_upsert('carrier_appointment', int(accountId), each['xml'], entityId=each['carrierAppointmentId'])
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_entity_upsert('carrier_appointment', {int(accountId)}, <<each['xml']>>, entityId={each['carrierAppointmentId']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_entity_upsert('carrier_appointment', {int(accountId)}, <<each['xml']>>, entityId={each['carrierAppointmentId']}) successfully upserted {rc} row(s).")
                if account['agentAccountInfo']['agentInfo']['associatedAccounts'] is not None:
                        try:
                            rc = mjdb.bp_account_relationship_delete('agent_account_associated_accounts', int(accountId))
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_relationship_delete('agent_account_associated_accounts', {int(accountId)})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_relationship_delete('agent_account_associated_accounts', {int(accountId)}) successfully deleted {rc} row(s).")
                            for each in account['agentAccountInfo']['agentInfo']['associatedAccounts']:
                                try:
                                    rc = mjdb.bp_account_realtionship_insert('agent_account_associated_accounts', int(accountId), each)
                                except Exception as e:
                                    raise ValueError(f"mjdb.bp_account_realtionship_insert('agent_account_associated_accounts', {int(accountId)}, {each})")
                                else:
                                    if rc > 0:
                                        lf.info(f"mjdb.bp_account_realtionship_insert('agent_account_associated_accounts', {int(accountId)}, {each}) successfully inserted {rc} row(s).")
        if 'agencyAccountInfo' in account.keys():
            try:
                rc = mjdb.bp_account_entity_upsert('agency_account_info', int(accountId), account['agencyAccountInfo']['xml'])
            except Exception as e:
                raise ValueError(f"mjdb.bp_account_entity_upsert('agency_account_info', {int(accountId)}, <<account['agencyAccountInfo']['xml']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('agency_account_info', {int(accountId)}, <<account['agencyAccountInfo']['xml']>>) successfully upserted {rc} row(s).")
                if 'agencyInfo' in account['agencyAccountInfo'].keys():
                    try:
                        rc = mjdb.bp_account_entity_upsert('agency_info', int(accountId), account['agencyAccountInfo']['agencyInfo']['xml'])
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_entity_upsert('agency_info', {int(accountId)}, <<account['agencyAccountInfo']['agencyInfo']['xml']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('agency_info', {int(accountId)}, <<account['agencyAccountInfo']['agencyInfo']['xml']>>) successfully upserted {rc} row(s).")
                    if account['agencyAccountInfo']['agencyInfo']['phones'] is not None:
                        try:
                            rc = mjdb.bp_entity_action('phone','delete',('ACCOUNT', 'AGENCY', int(accountId)))
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_entity_action('phone','delete',('ACCOUNT', 'AGENCY', {int(accountId)}))\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_entity_action('phone','delete',('ACCOUNT', 'AGENCY', {int(accountId)})) affected {rc} row(s).")
                        for each in account['agencyAccountInfo']['agencyInfo']['phones']:
                            try:
                                rc = mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENCY', int(accountId), each['xml']))
                            except Exception as e:
                                raise ValueError(f"mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENCY', {int(accountId)}, <<each['xml']>>))\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_entity_action('phone','insert',('ACCOUNT', 'AGENCY', {int(accountId)}, <<each['xml']>>)) affected {rc} row(s).")
                    if account['agencyAccountInfo']['agencyInfo']['licenses'] is not None:
                        for each in account['agencyAccountInfo']['agencyInfo']['licenses']:
                            try:
                                rc = mjdb.bp_account_entity_upsert('license', int(accountId), each['xml'], entityId=each['id'])
                            except Exception as e:
                                raise ValueError (f"mjdb.bp_account_entity_upsert('license', {int(accountId)}, <<each['xml']>>, entityId={each['id']})\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_account_entity_upsert('license', {int(accountId)}, <<each['xml']>>, entityId={each['id']}) successfully upserted {rc} row(s).")
                    if account['agencyAccountInfo']['agencyInfo']['carrierAppointments'] is not None:
                        for each in account['agencyAccountInfo']['agencyInfo']['carrierAppointments']:
                            try:
                                rc = mjdb.bp_account_entity_upsert('carrier_appointment', int(accountId), each['xml'], entityId=each['id'])
                            except Exception as e:
                                raise ValueError(f"mjdb.bp_account_entity_upsert('carrier_appointment', {int(accountId)}, <<each['xml']>>, entityId={each['id']})\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_account_entity_upsert('carrier_appointment', {int(accountId)}, <<each['xml']>>, entityId={each['id']}) successfully upserted {rc} row(s).")
                    if account['agencyAccountInfo']['agencyInfo']['associatedAccountIDs'] is not None:
                        try:
                            rc = mjdb.bp_account_relationship_delete('agency_account_associated_accounts', int(accountId))
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_relationship_delete('agency_account_associated_accounts', {int(accountId)})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_relationship_delete('agency_account_associated_accounts', {int(accountId)}) successfully deleted {rc} row(s).")
                            for each in account['agencyAccountInfo']['agencyInfo']['associatedAccountIDs']:
                                try:
                                    rc = mjdb.bp_account_realtionship_insert('agency_account_associated_accounts', int(accountId), each)
                                except Exception as e:
                                    raise ValueError(f"mjdb.bp_account_realtionship_insert('agency_account_associated_accounts', {int(accountId)}, {each})")
                                else:
                                    if rc > 0:
                                        lf.info(f"mjdb.bp_account_realtionship_insert('agency_account_associated_accounts', {int(accountId)}, {each}) successfully inserted {rc} row(s).")
                if account['agencyAccountInfo']['associatedAgentAccountIDs'] is not None:
                    try:
                        rc = mjdb.bp_account_relationship_delete('agency_account_associated_agent_accounts', int(accountId))
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_relationship_delete('agency_account_associated_agent_accounts', {int(accountId)})\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_relationship_delete('agency_account_associated_agent_accounts', {int(accountId)}) successfully deleted {rc} row(s).")
                    for each in account['agencyAccountInfo']['associatedAgentAccountIDs']:
                        try:
                            rc = mjdb.bp_account_realtionship_insert('agency_account_associated_agent_accounts', int(accountId), each)
                        except Exception as e:
                            raise ValueError(f"mjdb.bp_account_relationship_insert('agency_account_associated_agent_accounts', {int(accountId)}, {each})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_account_relationship_insert('agency_account_associated_agent_accounts', {int(accountId)}, {each}) successfully inserted {rc} row(s).")
        if account['accountCustomFieldOptionValues'] is not None:
            for each in account['accountCustomFieldOptionValues']:
                try:
                    rc = mjdb.bp_account_entity_upsert('custom_field_option_value', accountId, each['xml'], entityId=each['id'], source='ACCOUNT')
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_entity_upsert('custom_field_option_value', {accountId}, <<each['xml']>>, entityId={each['id']}, source='ACCOUNT')\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('custom_field_option_value', {accountId}, <<each['xml']>>, entityId={each['id']}, source='ACCOUNT') successfully upserted {rc} row(s).")
        if account['accountTeamMembers'] is not None:
            for atm in account['accountTeamMembers']:
                try:
                    rc = mjdb.bp_account_entity_upsert('account_team_member', accountId, atm['xml'], entityId=atm['id'])
                except Exception as e:
                    raise ValueError(f"mjdb.bp_account_entity_upsert('account_team_member', {accountId}, <<atm['xml']>>, entityId={atm['id']})\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_account_entity_upsert('account_team_member', {accountId}, <<atm['xml']>>, entityId={atm['id']}) affected {rc} row(s).")
                if atm['office'] is not None:
                    try:
                        rc = mjdb.bp_account_entity_upsert('office', atm['id'], atm['office']['xml'], source='ACCOUNT TEAM MEMBER',entityId=atm['office']['id'])
                    except Exception as e:
                        raise ValueError(f"mjdb.bp_account_entity_upsert('office', {atm['id']}, <<atm['office']['xml']>>, entityId={atm['office']['id']})\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_account_entity_upsert('office', {atm['id']}, <<atm['office']['xml']>>, entityId={atm['office']['id']}) affected {rc} row(s).")
        
def main():
    ### GET LAST MODIFIED DATE FROM DB
    lastMod = mjdb.bp_last_modified ('account')
    ### FIND ACCOUNTS CHANGED SINCE LAST UPDATE (30 DAYS MAX FOR WS-findChanges)
    if lastMod is not None and ((datetime.now()-lastMod).days <= 30):
        records = bpws.find_changes(sinceLastModifiedOn=datetime.strftime(lastMod, '%Y-%m-%dT%H:%M:%S.%f'), typesToInclude='Account')
    else:
        records = bpws.find_accounts()
    ### ITERATE RECORDS, CALL FOR SPECIFIC DETAIL, UPSERT ENTITIES
    for record in records:
        try:
            account = bpws.get_account(record['entityID'])
        except Exception as e:
            lf.error(f"bpws.get_account({record['entityID']})\n{e}")
        else:
            try:
                parsedAccount = parse_account_response(account)
            except Exception as e:
                lf.error(f"parse_account_response(<<account>>) for {record['entityID']}\n{e}")
            else:
                try:
                    upsert_account_entities(record['entityID'], parsedAccount)
                except Exception as e:
                    lf.error(e)

if __name__ == '__main__':
    main()