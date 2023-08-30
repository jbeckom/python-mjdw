import mjdb 
import sgws 
import common as cmn
import datetime as dt
import sgHelpers as hlp
from xml.etree import ElementTree as ET

entities = [
    {
        'file':'CLIENTS',
        'criteria':'LAST.ENTRY.DATE'
    },
    {
        'file':'CONTACTS',
        'criteria':'LAST.ENTRY.DATE'
    },
    {
        'file':'POLICIES',
        'criteria':'LAST.ENTRY.DATE'
    }
]

def upsert_file_item(sagfile, sagitem, fileXml, logger):
    try:
        rc = mjdb.sg_file_upsert(sagfile, sagitem, fileXml)
    except Exception as e:
        logger.error(f"mjdb.sg_file_upsert({sagfile}, {sagitem}, <<fileXml>>)\n{e}")
    else: 
        if rc > 0: logger.info(f"mjdb.sg_file_upsert({sagfile}, {sagitem}, <<fileXml>>) affected {rc} row(s).")

def file_group_action(file, group, action, logger, **kwargs):
    if action == 'delete': params = (kwargs['sagitem'],)
    elif action == 'insert': params = (kwargs['sagitem'], kwargs['lis'], kwargs['xml'])
    try:
        rc = mjdb.sg_file_group_action(action, file, group, params)
    except Exception as e:
        logger.error(f"mjdb.sg_file_group_action({action}, {file}, {group}, <<params>>) for {kwargs['sagitem']}\n{e}")
    else:
        if rc > 0: logger.info(f"mjdb.sg_file_group_action({action}, {file}, {group}, <<params>>) for {kwargs['sagitem']} affected {rc} row(s).")

def process_file_item(sagfile, item, logger):
    if sagfile == 'CLIENTS':
        try: 
            clientXml = hlp.parse_clients_file(item['xml'])
        except Exception as e: 
            logger.error(f"hlp.parse_clients_file(<<item['xml']>>) for {item['sagitem']}\n{e}")
        else: 
            upsert_file_item(sagfile, item['sagitem'], clientXml, logger)
    elif sagfile == 'CONTACTS':
        try:
            contactItem = hlp.parse_contacts_file(item['xml'])
        except Exception as e:
            logger.error(f"hlp.parse_contacts_file(item['xml']) for {item['sagitem']}\n{e}")
        else:
            upsert_file_item(sagfile, item['sagitem'], contactItem['itemXml'], logger)
            for key, values in contactItem['groups'].items():
                try:
                    file_group_action(sagfile, key, 'delete', logger, sagitem=item['sagitem'])
                except Exception as e:
                    logger.error(f"file_group_action({sagfile}, {key}, 'delete', <<logger>>, sagitem={item['sagitem']})")
                else:
                    for groupItem in values:
                        file_group_action(sagfile, key, 'insert', logger, sagitem=item['sagitem'], lis=groupItem['lis'], xml=groupItem['xml'])
    elif sagfile == 'POLICIES':
        try:
            policyItem = hlp.parse_policies_file(item['xml'])
        except Exception as e:
            logger.error(f"hlp.parse_policies_file(<<item['xml']>>) for {item['sagitem']}\n{e}")
        else:
            upsert_file_item(sagfile, item['sagitem'], policyItem, logger)

def main():
    for entity in entities:
        lf = cmn.log_filer('etl_sagitta', entity['file'])
        lastEntry = mjdb.sg_last_entry(entity['file'].lower())
        # last modified date and time are stored as integers -- convert each to applicable date part, then concatenate to timestamp for comparison
        lastEntryTimestamp = dt.datetime.combine((dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])), (dt.datetime(1967,12,31) + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[0] is not None else dt.datetime(1967,12,31,0,0)
        fileCount = sgws.file_record_count(entity['file'], criteria=entity['criteria'], lastEntry=dt.datetime.strftime(lastEntryTimestamp.date(), '%#m-%#d-%Y'))
        # don't proceed if there are no changes to process
        if fileCount > 0:
            accessStatement = f"SELECT {entity['file']} *CRITERIA* WITH {entity['criteria']} GE {dt.datetime.strftime(lastEntryTimestamp.date(), '%#m-%#d-%Y')}"
            if fileCount >= 10000:
                # modify access statement to return request in batches, parse batch response to list, iterate batch list
                for batch in hlp.parse_batch_items(sgws.post_ptr_access_statement(accessStatement.replace('*CRITERIA*','*CRITERIA.BATCH*'))):
                    batchRequest = sgws.post_ptr_access_statement(f"SELECT {entity['file']} *GET.BATCH* {batch}")
                    for item in hlp.parse_file_item_list(entity['file'], batchRequest.content, lastEntryTimestamp):
                        process_file_item(entity['file'], item, lf)
            else:
                for item in hlp.parse_file_item_list(entity['file'], sgws.post_ptr_access_statement(accessStatement).content, lastEntryTimestamp):
                    process_file_item(entity['file'], item, lf)

if __name__ == '__main__':
    main()