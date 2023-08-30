import mjdb
import bpws
import config
import common as cmn
from sqlalchemy import create_engine

LOGDIR = 'maintenance'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

lf = cmn.log_filer(LOGDIR, 'bp_contact_cleanup')

bp = {
    'contact':'contact_id=cid',
    'account_contact':'contact_id=cid',
    'address':"address_source='CONTACT' AND source_type='ACCOUNT' AND source_key=cid",
    'phone':"phone_source='CONTACT' AND source_type='ACCOUNT' and source_key=cid"
}

pa = {
    'contact':"contact_source='BENEFITPOINT' AND source_key=cid",
    'address':"address_source='BENEFITPOINT' AND source_type='CONTACT' AND source_key=cid",
    'email':"email_source='BENEFITPOINT' AND source_type='CONTACT' AND source_key=cid",
    'fax':"fax_source='BENEFITPOINT' AND source_type='CONTACT' AND source_key=cid",
    'phone':"phone_source='BENEFITPOINT' AND source_type='CONTACT' AND source_key=cid",
    'contact_address':"contact_guid=c_guid",
    'contact_email':"contact_guid=c_guid",
    'contact_fax':"contact_guid=c_guid",
    'contact_phone':"contact_guid=c_guid",
    'contact_tag':"contact_guid=c_guid"
}

def main():
    for cid in mjdb.get_table('benefitpoint', 'contact', cols=['contact_id'], clause="contact_source='ACCOUNT'"):
        cid=cid[0]
        try:
            bpws.get_account_contact(str(cid))
        except Exception as e:
            if e.args[0] == '[500] You are not authorized to access the requested information.':
                for k,v in bp.items():
                    try:
                        rc = mjdb.delete_table_record('benefitpoint',k,v.replace('cid',str(cid)))
                    except Exception as e:
                        lf.error(f"unable to delete benefitpoint.{k} record for {cid}\n{e}")
                    else:
                        lf.info(f"{rc} benefitpoint.{k} record(s) deleted for {cid}")
                c_guid = mjdb.get_table('powerapps','contact',cols=['guid'],clause=f"contact_source='BENEFITPOINT' AND source_key='{cid}'")[0][0]
                for k,v in pa.items():
                    try:
                        rc = mjdb.delete_table_record('powerapps',k,v.replace('cid',f"'{cid}'").replace('c_guid',f"'{c_guid}'"))
                    except Exception as e:
                        lf.error(f"unable to delete powerapps.{k} record for {cid},{c_guid}:\n{e}")
                    else:
                        lf.info(f"{rc} powerapps.{k} record(s) deleted for {cid},{c_guid}")                
            else:
                lf.error(f"{cid}: {e}")

if __name__ == '__main__':
    main()