import config
import requests
import datetime as dt
from bs4 import BeautifulSoup as bs
from xml.etree import ElementTree as ET

ACCOUNT = config.config('config.ini','sgws')['account']
USERNAME = config.config('config.ini','sgws')['username']
PASSWORD = config.config('config.ini','sgws')['password']
ONLINE = config.config('config.ini','sgws')['online']
WSDL = config.config('config.ini','sgws')['wsdl']
HEADERS = {'content-type':'text/xml'}
AMSNS = 'http://amsservices.com/'

def authentication_header():
    envelope = ET.Element('Envelope')
    envelope.set('xmlns','http://www.w3.org/2003/05/soap-envelope')
    header = ET.SubElement(envelope, 'Header')
    auth_header = ET.SubElement(header, 'AuthenticationHeader')
    auth_header.set('xmlns', AMSNS)
    account = ET.SubElement(auth_header, 'Account')
    account.text = ACCOUNT
    username = ET.SubElement(auth_header, 'Username')
    username.text = USERNAME
    password = ET.SubElement(auth_header, 'Password')
    password.text = PASSWORD 
    online = ET.SubElement(auth_header, 'Online')
    online.text = ONLINE
    return envelope

def ptr_xml(**kwargs):
    """build XML SOAP envelope"""
    env = ET.Element('Envelope')
    env.set('xmlns','http://www.w3.org/2003/05/soap-envelope')
    body = ET.SubElement(env,'Body')
    ptr = ET.SubElement(body,'PassThroughReq')
    ptr.set('xmlns', AMSNS)
    xmlinput = ET.SubElement(ptr, 'XMLinput')
    input = ET.SubElement(xmlinput, 'INPUT')
    account = ET.SubElement(input, 'Account')
    account.set('value', ACCOUNT)
    username = ET.SubElement(input, 'Username')
    username.set('value', USERNAME)
    password = ET.SubElement(input, 'Password')
    password.set('value', PASSWORD)
    online = ET.SubElement(input,'Online')
    online.set('value',ONLINE)

    if 'fileName' in kwargs:
        files = ET.SubElement(input, 'Files')
        items = ET.SubElement(input, 'Items')
        file = ET.SubElement(files, 'File')
        file.set('name', kwargs['fileName'])
        item = ET.SubElement(items, 'Item')
        item.set('key', kwargs['fileKey'])
    elif 'accessStatement' in kwargs:
        access = ET.SubElement(input, 'Access')
        access.set('statement', kwargs['accessStatement'])
    return ET.tostring(env, encoding='unicode', method='xml')

def post_ptr_access_statement(accessStatement):
    """return response content from PassThroughRequest"""
    xmlStr = ptr_xml(accessStatement=accessStatement)
    response = requests.post(WSDL,xmlStr,headers=HEADERS).content
    ptrSoup = bs(bs(response, 'xml').find('PassThroughReqResult').text,'xml')
    if ptrSoup.find('File').get('sagfile') == 'WEBSERVICE.ERRORS':
        raise ValueError(ptrSoup.find('a1').text)
    else:
        return ptrSoup

def file_record_count (file, **kwargs):
    """returns number of records for specified file and criteria (optional)"""
    accessStmt = f"COUNT {file}"
    if 'criteria' in kwargs:
        accessStmt += f" *CRITERIA* WITH {kwargs['criteria']} GE {kwargs['lastEntry']}"
    xmlStr = ptr_xml(accessStatement=accessStmt)
    response = requests.post(WSDL,xmlStr,headers=HEADERS).content
    ptrSoup = bs(bs(response,'xml').find('PassThroughReqResult').text,'xml')
    return ptrSoup.find('a1').text.split(' ')[0]

# temporary quick fix for fein cleanup, needs to be added to for full support of WS call
def client_update(guid, sagittaId, **kwargs):
    envelope = authentication_header()
    body = ET.SubElement(envelope, 'Body')
    clientUpdate = ET.SubElement (body, 'clientUpdate')
    clientUpdate.set('xmlns', AMSNS)
    clientUpdateRecord = ET.SubElement(clientUpdate, 'ClientUpdateRecord')
    guidTag = ET.SubElement(clientUpdateRecord, 'Guid')
    guidTag.text = guid
    sagittaIdTag = ET.SubElement(clientUpdateRecord, 'SagittaId')
    sagittaIdTag.text = sagittaId
    if 'fein' in kwargs:
        fein = ET.SubElement(clientUpdateRecord, 'FEIN')
        fein.text = kwargs['fein']
    response = requests.post(WSDL,ET.tostring(envelope,'unicode','xml'),headers=HEADERS)
    if response.ok == False:
        faultText = ET.fromstring(response.text).find('{http://www.w3.org/2003/05/soap-envelope}Body').find('{http://www.w3.org/2003/05/soap-envelope}Fault').find('{http://www.w3.org/2003/05/soap-envelope}Reason').find('{http://www.w3.org/2003/05/soap-envelope}Text').text
        raise ValueError(f"{response.reason}({response.status_code})[[{faultText}]]")

### DEBUG ONLY ###
if __name__ == '__main__':
    pass