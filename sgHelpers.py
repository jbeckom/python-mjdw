import os
import datetime as dt
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape, unescape
import sgws

def parse_clients_file(itemXml):
    item = ET.fromstring(itemXml.replace('&','&amp;'))
    statusCd = item.find('StatusCd')
    if statusCd is not None:
        if len(statusCd) > 0:
            for each in statusCd:
                each.tag = f"Status{each.tag[each.tag.find('v')+1:len(each.tag)]}Cd"
        else:
            new = ET.SubElement(statusCd,'Status1Cd')
            new.text = statusCd.text
            statusCd.text = None

    sicCd = item.find('SICCd')
    if sicCd is not None:
        if len(sicCd) > 0:
            for each in sicCd:
                each.tag = f"SIC{each.tag[each.tag.find('v')+1:len(each.tag)]}Cd"
        else:
            new = ET.SubElement(sicCd,'SIC1Cd')
            new.text = sicCd.text
            sicCd.text = None

    businessNature = item.find('BusinessNature')
    if businessNature is not None:
        bnText = ''
        if len(businessNature) > 0:
            for each in businessNature:
                bnText += each.text + os.linesep
            item.remove(businessNature)
            bn = ET.SubElement(item,'BusinessNature')
            bn.text = bnText

    phoneTypes = item.find('a69')
    if phoneTypes is not None:
        if len(phoneTypes) > 0:
            oneType = phoneTypes.find('v1')
            if oneType is not None:
                phone1Type = ET.SubElement(item,'Phone1Type')
                phone1Type.text = oneType.text
            twoType = phoneTypes.find('v2')
            if twoType is not None:
                phone2Type = ET.SubElement(item,'Phone2Type')
                phone2Type.text = twoType.text
            threeType = phoneTypes.find('v3')
            if threeType is not None:
                inspPhoneType = ET.SubElement(item,'InspectionPhoneType')
                inspPhoneType.text = threeType.text
            fourType = phoneTypes.find('v4')
            if fourType is not None:
                acctPhoneType = ET.SubElement(item,'AccountingPhoneType')
                acctPhoneType.text = fourType.text
        else:
            phone1Type = ET.SubElement(item,'Phone1Type')
            phone1Type.text = phoneTypes.text
        item.remove(phoneTypes)

    parentClient = item.find('ParentClient')
    if parentClient and len(parentClient) > 0:
        parentClient.text = parentClient.find('v1').text.strip()
        for child in [x for x in iter(parentClient)]:
            parentClient.remove(child)

    return ET.tostring(item,'unicode','xml')

def parse_contacts_file(itemXml):
    contactItem = ET.fromstring(itemXml.replace('&','&amp;'))
    groups = {}
    for group in ['address','category','email','phone','website']:
        groups[group] = [{'lis':x.attrib['lis'],'xml':ET.tostring(x,'unicode','xml')} for x in contactItem.findall(f'{group.title()}Group')]
    return {'itemXml':ET.tostring(contactItem,'unicode','xml'),'groups':groups}

def parse_policies_file(itemXml):
    item = ET.fromstring(itemXml.replace('&','&amp;'))
    for tag in ['PolicyRemarkText','NatureBusinessCd', 'CancLastDt', 'CountersignatureStateProvCd', 'GeneralInfoRemarkText', 'BinderExpirationDt']:
        tagElement = item.find(tag)
        if tagElement:
            tagElement.text = os.linesep.join(map(str,[v.text.strip() for v in tagElement])).strip()
            for v in [x for x in iter(tagElement)]:
                tagElement.remove(v)
    return ET.tostring(item, 'unicode', 'xml')
    
def parse_file_group(group, itemXml):
    groups = []
    for each in  ET.fromstring(itemXml).findall(group):
        groups.append({
            'lis':each.attrib['lis'],
            'xml':ET.tostring(each, 'unicode', 'xml')
        })
    return groups

def parse_file_item_list(fileType, respContent, sinceLastMod):
    """return dictionary list of file/item(s) from responseXml"""
    items = []
    auditTag = 'AuditInd' if fileType in ['CLIENTS','POLICIES'] else 'AuditInfo'
    for file in ET.fromstring(ET.fromstring(respContent).find('.//{http://amsservices.com/}PassThroughReqResult').text).find('.//Files').findall('File'):
        item = file.find('Item')
        # extract date and time integer values, combine to timestamp, compare to most recently modified record in DB, filter out those with less recent timestamps
        auditTimestamp = dt.datetime.combine((dt.date(1967,12,31) + dt.timedelta(days=int(item.find(auditTag).find('AuditDetail').find('AuditEntryDt').text))),(dt.datetime(1,1,1) + dt.timedelta(seconds=int(item.find(auditTag).find('AuditDetail').find('AuditTime').text))).time())
        if auditTimestamp >= sinceLastMod:
            items.append({
                'sagitem':int(item.attrib['sagitem']),
                'auditTimestamp':auditTimestamp,
                'xml':unescape(ET.tostring(item,'unicode','xml'))
            })
    items = sorted(items, key=lambda x: x['auditTimestamp'])
    return items

def parse_batch_items(batchResponse):
    """parse webservice response to list of batches for further processing"""
    return [x.text for x in batchResponse.find('Item').children if '*BATCH*' in x.text]

def col_tag_transform(col):
    return ''.join([x.capitalize() for x in col.split('_')])
    
### DEBUG ONLY ###
if __name__ == '__main__':
    foo = col_tag_transform('audit_change_agency_id')
    pass