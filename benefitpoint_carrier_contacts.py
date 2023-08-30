import mjdb
import bpws
import common as cmn
import datetime as dt
from xml.etree import ElementTree as ET

LOGDIR = 'benefitpoint_carrier_contacts'
# INSTANTIATE LOGGER
lf = cmn.log_filer(LOGDIR, 'carrier_contacts')

def parse_phone_xml(phoneXml):
    # db function expects 'phone' tag rather than 'phones'
    phoneXml.tag = 'phone'
    return ET.tostring(phoneXml, 'unicode', 'xml')

def entity_upsert(entity, entityID, xmlStr, **kwargs):
    try:
        rc = mjdb.bp_entity_upsert(entity, entityID, xmlStr, **kwargs)
    except Exception as e:
        lf.error(f"mjdb.bp_entity_upsert({entity}, {entityID}, <<xmlStr>>)\n{e}")
    else:
        if rc > 0:
            lf.info(f"mjdb.bp_entity_upsert({entity}, {entityID}, <<xmlStr>>) affected {rc} row(s).")

def entity_action(entity, action, params, relationship=None):
    try:
        rc = mjdb.bp_entity_action(entity, action, params, relationship)
    except Exception as e:
        lf.error(f"mjdb.bp_entity_action({entity}, {action}, <<params>>, {relationship})\n{e}")
    else:
        if rc > 0:
            lf.info(f"mjdb.bp_entity_action({entity}, {action}, <<params>>, {relationship}) affected {rc} row(s).")

def upsert_carrier_contacts(xmlData, sinceLastMod):
    for cc in ET.fromstring(xmlData.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/broker/v4_3}findCarrierContactsResponse').findall('contacts'):
        # filter records by latest modification date
        if dt.datetime.strptime(cc.find('lastModifiedOn').text, '%Y-%m-%dT%H:%M:%S.%f%z') >= sinceLastMod:
            carrierID = cc.find('carrierID').text
            contact = cc.find('contact')
            contactID = contact.find('contactID').text
            entity_upsert('carrier_contact', carrierID, ET.tostring(cc,'unicode','xml'))
            entity_upsert('contact', contactID, ET.tostring(contact,'unicode','xml'), source='CARRIER', sourceKey=carrierID)
            entity_upsert('address', contactID, ET.tostring(contact.find('address'), 'unicode', 'xml'), source='CARRIER', type='CONTACT')
            # source data structure does not enforce uniqueness for phone records -- remove existing records, by source/type, to avoid duplication
            entity_action('phone', 'delete', ('CARRIER', 'CONTACT', contactID))
            # phone elements without data have attribute of xsi:mil="true", filter out instances with no data
            phones = (x for x in contact.findall('phones') if len(x.attrib) == 0)
            # iterate & insert phone records
            for phone in phones:
                entity_action('phone', 'insert', ('CARRIER','CONTACT',contactID, parse_phone_xml(phone)))
            # iterate entity relationships
            entities = [
                {'tag':'officeIDs','rel':'offices'},
                {'tag':'departmentIDs','rel':'departments'},
                {'tag':'contactAssignments','rel':'contact_assignments'},
                {'tag':'productTypeIDs','rel':'product_types'},
                {'tag':'supportedTerritories','rel':'supported_territories'},
                {'tag':'userIDs','rel':'users'},
                {'tag':'productIDs','rel':'products'}
            ]
            for entity in entities:
                # delete existing relationship records first -- prevents duplication
                entity_action('carrier_contact', 'delete', (int(carrierID), int(contactID)), entity['rel'])
                # iterate all relationship instances & upsert relationship entities
                for entityXml in cc.findall(entity['tag']):
                    entity_action('carrier_contact', 'insert', (int(carrierID), int(contactID), entityXml.text), entity['rel'])
            for cfv in cc.findall('customFieldValues'):
                entity_upsert('custom_field_value', carrierID, ET.tostring(cfv,'unicode','xml'), source='CARRIER CONTACT')

def main():
    lastMod = mjdb.bp_last_modified('carrier_contact')
    lastMod = lastMod if lastMod is not None else dt.datetime(1900,1,1,0,0, tzinfo=dt.timezone.utc)
    upsert_carrier_contacts(bpws.find_carrier_contacts(), lastMod)

if __name__ == '__main__':
    main()