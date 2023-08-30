import sgws
import mjdb
import common as cmn
import datetime as dt
import sfeHelpers as hlp
from datetime import date, timedelta

cfgs = [
    {
        'file':'ACORD.LEGAL.ENTITY.CODES'
    },
    {
        'file':'CLIENTS',
        'criteria':'PAX.AUDIT.DATE'
    },
    {
        'file':'CLIENTS.ADDLINFO',
    },
    {
        'file':'CONTACTS',
        'criteria':'LAST.ENTRY.DATE GE',
        'groups':[
            {
                'full':'AddressGroup',
                'abbrv':'address_group'
            },
            {
                'full':'CategoryGroup',
                'abbrv':'category_group'
            },
            {
                'full':'EmailGroup',
                'abbrv':'email_group'
            },
            {
                'full':'PhoneGroup',
                'abbrv':'phone_group'                
            },
            {
                'full':'WebsiteGroup',
                'abbrv':'website_group'
            }
        ]
    },
    {
        'file':'CREDIT.TERMS'
    },
    {
        'file':'TYPES',
    },
    {
        'file':'STAFF',
        'criteria':'LAST.ENTRY.DATE GE',
        'groups':[
            {
                'full':'CommissionGroup',
                'abbrv':'commission_group'
            }
        ]
    },
    {
        'file':'STAFF.ADDLINFO'
    },
    # {
    #     'file':'POLICIES'
    # },
    # {
    #     'file':'POLICIES.ACCT.PREFILL',
    #     'criteria':'LAST.ENTRY.DATE GE',
    #     'groups':[
    #         {
    #             'full':'MultipleBilltoProdAddTranInfo',
    #             'abbrv':'mbpati'
    #         },
    #         {
    #             'full':'MultipleProducerCdInfo',
    #             'abbrv':'mpci'
    #         },
    #         {
    #             'full':'AdditionalTransactionInfo',
    #             'abbrv':'ati'
    #         }
    #     ]
    # },
    # {
    #     'file':'SIC.CODES'
    # },
    # {
    #     'file':'COVERAGES'
    # }
]

def process_files_response (cfg, filesResp, lf):
    for item in sgws.parse_file_item_response(filesResp):
        try:
            if cfg['file'] == 'CLIENTS':
                item['xml'] = hlp.parse_clients_file(item['xml'])
            elif cfg['file'] == 'POLICIES':
                item['xml'] = hlp.parse_policies_file(item['xml'])
        except Exception as e:
            lf.error(f"unable to parse {cfg['file']} file\n{e}")
        # INSERT/UPDATE INDIVIDUAL FILE/ITEM
        try:
            rc = mjdb.sg_file_extract_upsert(cfg['file'], item['sagitem'], item['xml'])
        except Exception as e:
            lf.error(f"mjdb.sg_file_extract_upsert({cfg['file']}, {item['sagitem']}, <<item['xml']>>)\n{e}")
        else:
            if rc > 0:
                lf.info(f"mjdb.sg_file_extract_upsert({cfg['file']}, {item['sagitem']}, <<item['xml']>>) affected {rc} row(s).")
            # PARSE & INSERT/UPDATE CHILD GROUPS FOR FILE
            if 'groups' in cfg:
                for group in cfg['groups']:
                    try:
                        groups = hlp.parse_file_group(group['full'], item['xml'])
                    except Exception as e:
                        lf.error(f"hlp.parse_file_group({group['full']}, <<item['xml']>>)\n{e}")
                    else:
                        if len(groups) > 0:
                            try:
                                rc = mjdb.sg_file_group_delete(cfg['file'], group['abbrv'], item['sagitem'])
                            except Exception as e:
                                lf.error(f"mjdb.sg_file_group_delete({cfg['file']}, {group['abbrv']}, {item['sagitem']})\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.sg_file_group_delete({cfg['file']}, {group['abbrv']}, {item['sagitem']}) affected {rc} row(s).")
                                for grp in groups:
                                    try:                                    
                                        rc = mjdb.sg_file_group_upsert(cfg['file'], group['abbrv'], item['sagitem'], grp['lis'], grp['xml'])
                                    except Exception as e:
                                        lf.error(f"mjdb.sg_file_group_upsert({cfg['file']}, {group['abbrv']}, {item['sagitem']}, {grp['lis']}, <<grp['xml']>>\n{e})")
                                    else:
                                        if rc > 0:
                                            lf.info(f"mjdb.sg_file_group_upsert({cfg['file']}, {group['abbrv']}, {item['sagitem']},{ grp['lis']}, <<grp['xml']>>) affected {rc} row(s).")
def main():
    for cfg in cfgs:
        lf = cmn.log_filer('sagittaFileExtract', cfg['file'])
        try:
            # WHAT IS THE LAST IMPORT DATE?
            lastEntry = mjdb.sg_last_entry(cfg['file']) if 'criteria' in cfg else None
            lastEntryDt = (date(1967,12,31) + timedelta(days=lastEntry)) if lastEntry is not None else None
        except Exception as e:
            lf.error(f"mjdb.sg_last_entry({cfg['file']})\n{e}")
        else:
            try:
                # HOW MANY RECORDS TO IMPORT?
                fileCount = sgws.file_record_count(cfg['file'], criteria=cfg['criteria'], lastEntry=lastEntryDt) if lastEntryDt is not None and cfg['criteria'] is not None else sgws.file_record_count(cfg['file'])
            except Exception as e:
                lf.error(f"unable to obtain file count for {cfg['file']}\n{e}")
            else:
                if fileCount > 0:  ### log 0 records found?
                    # BUILD ACCESS STATEMENT FOR PASSTHROUGHREQ
                    accessStmt  = f"SELECT {cfg['file']}"
                    if fileCount >= 10000:
                        accessStmt += f" *CRITERIA.BATCH* WITH {cfg['criteria']} {dt.datetime.strftime(lastEntryDt,'%m/%d/%Y')}" if lastEntryDt is not None and 'criteria' in cfg else " *BATCH*"
                    else:
                        accessStmt += f" *CRITERIA* WITH {cfg['criteria']} {dt.datetime.strftime(lastEntryDt,'%m/%d/%Y')}" if lastEntryDt is not None and cfg['criteria'] is not None else ''

                    # BUILD & POST PASSTHROUGHREQ
                    try:
                        soapResp = sgws.post_ptr_access_statement(accessStmt)
                    except Exception as e:
                        lf.error(f"sgws.post_ptr_access_statement({accessStmt})\n{e}")
                    else:
                        # RETRIEVE INDIVIDUAL BATCHES OF FILES
                        if '*BATCH*' in soapResp.text:
                            for batch in sgws.parse_batch_response(soapResp):
                                try:
                                    accessStmt = f"SELECT {cfg['file']} *GET.BATCH* {batch}"
                                    batchResp = sgws.post_ptr_access_statement(accessStmt)
                                except Exception as e:
                                    lf.error(f"sgws.post_ptr_access_statement({accessStmt})\n{e}")
                                else:
                                    process_files_response(cfg, batchResp, lf)
                        else:
                            process_files_response(cfg, soapResp, lf)
                else:
                    lf.info(f"No {cfg['file']} updated since {lastEntryDt}.")

if __name__ == '__main__':
    main()