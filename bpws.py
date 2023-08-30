import config
import requests
from datetime import datetime as dt
from xml.etree import ElementTree as ET

USERNAME = config.config('config.ini','bpws')['username']
PASSWORD = config.config('config.ini', 'bpws')['password']
SOAPNS = 'http://schemas.xmlsoap.org/soap/envelope/'
LOGINWSDL = config.config('config.ini', 'bpws')['login']
LOGINNS = 'http://ws.benefitpoint.com/aptusconnect/login/v2'
BROKERCONNECTWSDL = config.config('config.ini', 'bpws')['brokerconnect']
BROKERCONNECTNS = 'http://ws.benefitpoint.com/aptusconnect/broker/v4_3'
HEADERS = {'content-type':'text/xml'}    

def post_request(wsdl, requestXml):
    return requests.post(wsdl, requestXml, headers=HEADERS)
    # DEV ONLY -- use above for Test/Prod environments
    # return requests.post(wsdl, requestXml, headers=HEADERS, verify=False)

def login_echo(sessionID):
    """
    Utility method to verify login.
    """
    # build xml for request
    envelope = ET.Element('Envelope')
    envelope.set('xmlns',SOAPNS)
    body = ET.SubElement(envelope,'Body')
    echo = ET.SubElement(body, 'echo')
    echo.set('xmlns',LOGINNS)
    sessionId = ET.SubElement(echo, 'sessionID')
    sessionId.set('xmlns','')
    sessionId.text = sessionID
    # post request
    response = post_request (LOGINWSDL, ET.tostring(envelope, 'unicode', 'xml'))
    # parse response
    if response.status_code == 200:
        sessionIDResp = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/login/v2}echoResponse').find('sessionID').text
    elif response.status_code == 500:
        raise ValueError(ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://www.w3.org/2003/05/soap-envelope}Fault').find('faultstring').text)
    if sessionId == sessionIDResp:
        return True
    else:
        return False

def login_session():
    """
    Attempts to login using the given username and password and create a sessionID.
    """
    # build xml for request
    envelope = ET.Element('Envelope')
    envelope.set('xmlns',SOAPNS)
    body = ET.SubElement(envelope,'Body')
    login = ET.SubElement(body, 'login')
    login.set('xmlns',LOGINNS)
    username = ET.SubElement(login, 'username')
    username.set('xmlns','')
    username.text = USERNAME
    password = ET.SubElement(login,'password')
    password.set('xmlns','')
    password.text = PASSWORD
    # post request
    response = post_request(LOGINWSDL, ET.tostring(envelope, 'unicode', 'xml'))
    # parse response for sessionID
    if response.ok == True:
        return ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/login/v2}loginResponse').find('result').find('sessionID').text
    else:
        fault = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault')
        raise ValueError(f"{response.status_code} => {fault.find('faultcode').text} => {fault.find('faultstring').text}")

def logout_session(sessionID):
    """
    Terminates the session indicated by the supplied sessionID
    """
    # build xml for request
    envelope = ET.Element('Envelope')
    envelope.set('xmlns', SOAPNS)
    body = ET.SubElement(envelope, 'Body')
    logout = ET.SubElement(body, 'logout')
    logout.set('xmlns', LOGINNS)
    sessionId = ET.SubElement(logout, 'sessionID')
    sessionId.set('xmlns','')
    sessionId.text = sessionID
    #post request
    post_request(LOGINWSDL, ET.tostring(envelope, 'unicode', 'xml'))

def session_header_stub(sessionID):
    envelope = ET.Element('Envelope')
    envelope.set('xmlns',SOAPNS)
    header = ET.SubElement(envelope,'Header')
    sessionIdHeader = ET.SubElement(header, 'SessionIdHeader')
    sessionIdHeader.set('xmlns',BROKERCONNECTNS)
    sessionId = ET.SubElement(sessionIdHeader, 'sessionId')
    sessionId.set('xmlns','')
    sessionId.text = sessionID
    return envelope

def find_changes(**kwargs):
    """
    Finds records that have been created, updated, or deleted since a given time.
    EntityTypes: Account, Account_Contact, Activity_Log_Record, Benefit_Summary, Payee, Split, Product, User, Request, Plan_Design, Invitation, Response, Plan_Design_Alternate, Rate
    kwargs: sinceLastModifiedOn (datetime), typesToInclude (string of EntityType)
    """
    # check validity of session token
    sessionID = login_session()
    try:
        # build xml for request
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findChanges = ET.SubElement(body, 'findChanges')
        findChanges.set('xmlns', BROKERCONNECTNS)
        criteria = ET.SubElement(findChanges, 'criteria')
        criteria.set('xmlns','')
        if 'sinceLastModifiedOn' in kwargs:
            sinceLastModifiedOn = ET.SubElement(criteria, 'sinceLastModifiedOn')
            # sinceLastModifiedOn.text = dt.strftime(kwargs['sinceLastModifiedOn'], '%Y-%m-%dT%H:%M:%S.%f%z')
            sinceLastModifiedOn.text = str(kwargs['sinceLastModifiedOn']).replace(' ','T') ### don't think this is a permenant fix
        if 'typesToInclude' in kwargs:
            typesToInclude = ET.SubElement(criteria, 'typesToInclude')
            typesToInclude.text = kwargs['typesToInclude']
        response = post_request(BROKERCONNECTWSDL, ET.tostring(envelope, 'unicode', 'xml'))        
        if response.ok == True:
            return response
        else:
            faultstring = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault').find('faultstring').text
            raise ValueError(f"[{response.status_code}] {faultstring}")
    finally:
        logout_session(sessionID)

def find_accounts(**kwargs):
    """
    Finds and returns summary information about accounts based on name, type, status, or team member.
    More detailed information can be obtained by invoking getAccount() on individual accounts.
    """
    # check validity of session token
    sessionID = login_session()
    try:
        # build request xml
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope, 'Body')
        findAccounts = ET.SubElement(body, 'findAccounts')
        findAccounts.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findAccounts, 'criteria')
        criteria.set('xmlns','')
        if 'teamMemberID' in kwargs:
            teamMemberId = ET.SubElement(criteria, 'teamMemberID')
            teamMemberId.text = kwargs['teamMemberId']
        if 'accountNameMatch' in kwargs:
            accountNameMatch = ET.SubElement(criteria, 'accountNameMatch')
            accountNameMatch.text = kwargs['accountNameMatch']
        if 'accountClassifications' in kwargs:
            accountClassifications = ET.SubElement(criteria,'accountClassifications')
            accountClassifications.text = kwargs['accountClassifications']
        if 'accountTypes' in kwargs:
            accountTypes = ET.SubElement(criteria, 'accountTypes')
            accountTypes.text = kwargs['accountTypes']
        if 'active' in kwargs:
            active = ET.SubElement(criteria, 'active')
            active.text = kwargs['active']        

        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope, 'unicode', 'xml'))
    finally:
        logout_session(sessionID)

def find_account_contacts(accountId):
    """
    Finds and returns contacts for an account based on the account ID.
    """
    # get session token
    sessionID = login_session()

    try:
        # build request xml
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope, 'Body')
        findAccountContacts = ET.SubElement(body, 'findAccountContacts')
        findAccountContacts.set('xmlns', BROKERCONNECTNS)
        criteria = ET.SubElement(findAccountContacts, 'criteria')
        criteria.set('xmlns','')
        accountID = ET.SubElement(criteria, 'accountID')
        accountID.text = str(accountId)
        # post request
        response = post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
        # parse response
        if response.ok == True:
            return response.content
        else:
            faultstring = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault').find('faultstring').text
            raise ValueError(f"[{response.status_code}] {faultstring}")
    finally:
        logout_session(sessionID)

def find_carrier_contacts(**kwargs):
    """
    Finds and returns carrier contacts that match the given search criteria.
    """
    # get session token
    sessionID = login_session()

    try:
        # build request xml
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope, 'Body')
        findCarrierContacts = ET.SubElement(body, 'findCarrierContacts')
        findCarrierContacts.set('xmlns', BROKERCONNECTNS)
        criteria = ET.SubElement(findCarrierContacts, 'criteria')
        criteria.set('xmlns','')
        if 'carrierIDs' in kwargs:
            for each in kwargs['carrierIDs']:
                carrierIDs = ET.SubElement(criteria, 'carrierIDs')
                carrierIDs.text = each
        if 'userID' in kwargs:
            userID = ET.SubElement(criteria, 'userID')
            userID.text = kwargs['userID']
        if 'productID' in kwargs:
            productID = ET.SubElement(criteria, 'productID')
            productID.text = kwargs['productID']
        if 'officeIDs' in kwargs:
            for each in kwargs['officeIDs']:
                officeIDs = ET.SubElement(criteria, 'officeIDs')
                officeIDs.text = each
        if 'departmentIDs' in kwargs:
            for each in kwargs['departmentIDs']:
                departmentIDs = ET.SubElement(criteria,'departmentIDs')
                departmentIDs.text = each
        if 'assignmentTypes' in kwargs: 
            for each in kwargs['assignmentTypes']:
                assignmentTypes = ET.SubElement(critera, 'assignmentTypes')
                assignmentTypes.text = each
        if 'productTypeIDs' in kwargs:
            for each in kwargs['productTypeIDs']:
                productTypeIDs = ET.SubElement(criteria, 'productTypeIDs')
                productTypeIDs.text = each
        if 'marketSizes' in kwargs:
            for each in kwargs['marketSizes']:
                marketSizes = ET.SubElement(criteria, 'marketSizes')
                marketSizes.text = each
        if 'territories' in kwargs:
            for each in kwargs['territories']:
                territories = ET.SubElement(criteria, 'territories')
                territories.text = each

        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
        
    finally:
        logout_session(sessionID)

def find_offices(**kwargs):
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findOffices = ET.SubElement(body, 'findOffices')
        findOffices.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findOffices, 'criteria')
        criteria.set('xmlns','')
        for x in kwargs:
            y = ET.SubElement(criteria, x)
            t.text = str(kwargs[x])
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)
            

def find_posting_records(productID,**kwargs):
    """
    Finds and returns information for posting records.
    Any posting records that match the criteria specified will be returned.
    """
    sessionID = login_session()
    try:
        # build request XML
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body') 
        findPostingRecords = ET.SubElement(body,'findPostingRecords2')
        findPostingRecords.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findPostingRecords,'criteria')
        criteria.set('xmlns','')
        productId = ET.SubElement(criteria,'productID')
        productId.text = str(productID)
        for x in kwargs:
            y = ET.SubElement(criteria,x)
            y.text = str(kwargs[x])
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_payees(**kwargs):
    """
    Finds and returns summary information about payees based on a payee's name.
    More detailed information can be obtained by invoking getPayee() on individual payees.
    """
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findPayees = ET.SubElement(body,'findPayees')
        findPayees.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findPayees,'criteria')
        criteria.set('xmlns','')
        for kw in kwargs:
            x = ET.SubElement(criteria,kw)
            x.text = str(kwargs[kw])
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_products(accountID,**kwargs):
    """
    Finds and returns summary information for plans or additional products based on specifying an Account ID and any of the following optional criteria: Last Modified Date, Effective On Date
    """
    # get session token 
    sessionID = login_session()
    try:
        # build request XML
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body') 
        findProducts = ET.SubElement(body,'findProducts')
        findProducts.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findProducts,'criteria')
        criteria.set('xmlns','')
        accountId = ET.SubElement(criteria, 'accountID')
        accountId.text = str(accountID)
        for x in ('sinceLastModifiedOn','effectiveOn','brokerOfRecordAccountID'):
            if x in kwargs:
                y = ET.SubElement(criteria,x)
                y.text = kwargs[x]
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_statements(**kwargs):
    """
    Finds and returns information for statements based on specifying a status, user ID who created the statement, billing Carrier ID, or an entry date range.
    Any statements that match the criteria specified will be returned.
    kwargs: statementStatus, createdByUserID, billingCarrierID, officeID, entryFromDate, entryToDate, lastModifiedDateAfter, lastModifiedDateBefore, lastPostedDateAfter, lastPostedDateBefore, accountingMonthDateAfter, accountingMonthDateBefore
    """
    sessionID = login_session()

    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findStatements = ET.SubElement(body,'findStatements')
        findStatements.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findStatements, 'criteria')
        criteria.set('xmlns','')
        for x in kwargs:
            y = ET.SubElement(criteria, x)
            y.text = kwargs[x]
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_rates(**kwargs):
    """
    Finds and returns summary information for rates based on specifying a product ID or response ID.
    """
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findRates = ET.SubElement(body,'findRates')
        findRates.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findRates,'rateSearchCriteria')
        criteria.set('xmlns','')
        for x in kwargs:
            y = ET.SubElement(criteria, x)
            y.text = str(kwargs[x])
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_splits(productID):
    """
    Finds and returns information for splits based on specifying a product ID.
    Any splits that are associated to the Plan or Additional Product will be returned.
    """
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        findSplits = ET.SubElement(body,'findSplits')
        findSplits.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findSplits,'criteria')
        criteria.set('xmlns','')
        productId = ET.SubElement(criteria,'productID')
        productId.text = str(productID)
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def find_users(**kwargs):
    """
    Finds and returns summary information about users based username, roles and permissions. 
    More detailed information can be obtained by invoking getUser() on individual users. 
    """
    # get session token 
    sessionID = login_session()
    try:
        # build request XML
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body') 
        findUsers = ET.SubElement(body,'findUsers')
        findUsers.set('xmlns',BROKERCONNECTNS)
        criteria = ET.SubElement(findUsers,'criteria')
        criteria.set('xmlns','')
        for tag in ('usernameMatch','firstNameMatch','lastNameMatch','active','roles','sinceLastModifiedOn'):
            if tag in kwargs:
                node = ET.SubElement(criteria,tag)
                node.text = kwargs[tag]
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_account(accountId):
    """
    Retrieves detailed information about a single account.
    """
    # get session token
    sessionID = login_session()
    # build request xml
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope, 'Body')
        getAccount = ET.SubElement(body, 'getAccount')
        getAccount.set('xmlns', BROKERCONNECTNS)
        accountID = ET.SubElement(getAccount, 'accountID')
        accountID.set('xmlns','')
        accountID.text = str(accountId)
        # post request
        response = post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
        # parse response
        if response.ok == True:
            return response 
        else:
            faultstring = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault').find('faultstring').text
            raise ValueError(f"[{response.status_code}] {faultstring}")
    finally:
        logout_session(sessionID)

def get_account_contact(contactID):
    """
    Retrieves an account contact for the given contact ID.
    """
    sessionID  = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope, 'Body')
        getAccountContact = ET.SubElement(body,'getAccountContact')
        getAccountContact.set('xmlns',BROKERCONNECTNS)
        contactId = ET.SubElement(getAccountContact,'contactID')
        contactId.set('xmlns','')
        contactId.text = contactID

        response = post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
        if response.ok == True:
            return response
        else: 
            faultstring = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault').find('faultstring').text
            raise ValueError(f"[{response.status_code}] {faultstring}")
    finally:
        logout_session(sessionID)

def get_available_carriers():
    sessionID = login_session() 
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getAvailableCarriers = ET.SubElement(body, 'getAvailableCarriers')
        getAvailableCarriers.set('xmlns',BROKERCONNECTNS)
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_custom_field_structure(customArea):
    """
    returns metadata which describes a customization area is structured;
    customArea: Account_Summary, Activity_Log, Carrier_Contact, Home_Page_Tabs, Service_Info, Plan_Info, Account_Contact
    """
    # check validity of session token
    sessionId = login_session()
    try:
        # build request xml
        envelope = session_header_stub(sessionId)
        body = ET.SubElement(envelope, 'Body')
        getCustomFieldStructure = ET.SubElement(body, 'getCustomFieldStructure')
        getCustomFieldStructure.set('xmlns', BROKERCONNECTNS)
        customizationArea = ET.SubElement(getCustomFieldStructure, 'customizationArea')
        customizationArea.set('xmlns', '')
        customizationArea.text = customArea

        response = post_request(BROKERCONNECTWSDL, ET.tostring(envelope, 'unicode', 'xml'))
        if response.ok == True:
            return response
        else:
            faultstring = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://schemas.xmlsoap.org/soap/envelope/}Fault').find('faultstring').text
            raise ValueError(f"[{response.status_code}] {faultString}")
    finally:
        logout_session(sessionId)

def get_posting_record(postingRecordID):
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getPostingRecord = ET.SubElement(body,'getPostingRecord')
        getPostingRecord.set('xmlns',BROKERCONNECTNS)
        postingRecordId = ET.SubElement(getPostingRecord, 'postingRecordID')
        postingRecordId.set('xmlns','')
        postingRecordId.textb = postingRecordID
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_rate(rateID):
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getRate = ET.SubElement(body,'getRate')
        getRate.set('xmlns',BROKERCONNECTNS)
        rateId = ET.SubElement(getRate,'rateID')
        rateId.set('xmlns','')
        rateId.text = str(rateID)
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_split(splitID):
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getSplit = ET.SubElement(body,'getSplit')
        getSplit.set('xmlns',BROKERCONNECTNS)
        splitId = ET.SubElement(getSplit,'splitID')
        splitId.set('xmlns','')
        splitId.text = str(splitID)
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_user(userID):
    """
    Retrieves detailed information for a specific User, including roles, permissions and office access.
    """
    # get session token
    sessionID = login_session()
    try:
        # build request XML
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getUser = ET.SubElement(body,'getUser')
        getUser.set('xmlns',BROKERCONNECTNS)
        userId = ET.SubElement(getUser,'userID')
        userId.set('xmlns','')
        userId.text = userID
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_product(productID):
    """
    Retrieves detailed information for a specific plan or additional product.
    NOTES: The values in Commission Info will only be populated if the Web Services User has an RTM role.
    The totalEstimatedMonthlyRevenue and totalEstimatedMonthlyPremium values in the Product will only be populated in the Web Services User has the 'View Financial Info' permission.
    """
    # get session token
    sessionID = login_session()
    try:
        # build request XML
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getProduct = ET.SubElement(body, 'getProduct')
        getProduct.set('xmlns',BROKERCONNECTNS)
        productId = ET.SubElement(getProduct, 'productID')
        productId.set('xmlns','')
        productId.text = productID
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_product_types(returnAll):
    sessionID = login_session() 

    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getProductTypes = ET.SubElement(body, 'getProductTypes')
        getProductTypes.set('xmlns',BROKERCONNECTNS)
        returnAllTypes = ET.SubElement(getProductTypes, 'returnAllTypes')
        returnAllTypes.set('xmlns','')
        returnAllTypes.text = str(returnAll)
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_payee(payeeID):
    """
    Retrieves detailed information for a specific Payee.
    """
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getPayee = ET.SubElement(body,'getPayee')
        getPayee.set('xmlns',BROKERCONNECTNS)
        payeeId = ET.SubElement(getPayee, 'payeeID')
        payeeId.set('xmlns','')
        payeeId.text = str(payeeID)
        return post_request(BROKERCONNECTWSDL, ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

def get_statement(statementID):
    """
    Retrieves detailed information for a specific Statement.
    """
    sessionID = login_session()
    try:
        envelope = session_header_stub(sessionID)
        body = ET.SubElement(envelope,'Body')
        getStatement = ET.SubElement(body,'getStatement')
        getStatement.set('xmlns',BROKERCONNECTNS)
        statementId = ET.SubElement(getStatement,'statementID')
        statementId.set('xmlns','')
        statementId.text = statementID
        return post_request(BROKERCONNECTWSDL,ET.tostring(envelope,'unicode','xml'))
    finally:
        logout_session(sessionID)

### DEBUG ONLY ###
if __name__ == '__main__':
    pass