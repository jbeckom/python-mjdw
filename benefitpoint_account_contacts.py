import mjdb
import bpws
import common as cmn

LOGDIR = 'benefitpoint_account_contacts'
### INSTANTIATE LOGGER
lf = cmn.log_filer(LOGDIR, 'account_contacts')

def main():
    for accountId in  mjdb.bp_account_ids():
        for ac in bpws.find_account_contacts(accountId):
            try:
                rc = mjdb.bp_entity_upsert('account_contact', accountId, ac['xml'])
            except Exception as e:
                lf.error(f"mjdb.bp_account_entity_upsert('account_contact', {accountId}, <<ac['xml']>>)\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_account_entity_upsert('account_contact', {accountId}, <<ac['xml']>>) affected {rc} row(s).")
            if ac['locationIDs'] is not None:
                try:
                    rc = mjdb.bp_entity_action('account_contact', 'delete', (accountId,), relationship='locations')
                except Exception as e:
                    lf.error(f"mjdb.bp_entity_action('account_contact', 'delete', ({accountId},), relationship='locations')\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_entity_action('account_contact', 'delete', ({accountId},), relationship='locations') affected {rc} row(s).")
                for lid in ac['locationIDs']:
                    try:
                        rc= mjdb.bp_entity_action('account_contact', 'insert', (accountId, lid), relationship='locations')
                    except Exception as e:
                        lf.error(f"mjdb.bp_entity_action('account_contact', 'insert', ({accountId}, {lid}), relationship='locations')\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_entity_action('account_contact', 'insert', ({accountId}, {lid}), relationship='locations') affected {rc} row(s).")
            if ac['customFieldValues'] is not None:
                for cfv in ac['customFieldValues']:
                    try:
                        rc = mjdb.bp_custom_field_value_upsert('ACCOUNT_CONTACT', accountId, cfv)
                    except Exception as e:
                        lf.error(f"mjdb.bp_custom_field_value_upsert('ACCOUNT_CONTACT', {accountId}, <<cfv>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_custom_field_value_upsert('ACCOUNT_CONTACT', {accountId}, <<cfv>>) affected {rc} row(s).")
            try:
                rc = mjdb.bp_entity_upsert('contact', ac['contactId'], ac['contact']['xml'], source='ACCOUNT', sourceKey=accountId)
            except Exception as e:
                lf.error(f"mjdb.bp_entity_upsert('contact', {ac['contactId']}, <<ac['contact']['xml']>>, source='ACCOUNT', sourceKey={accountId})\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.bp_entity_upsert('contact', {ac['contactId']}, <<ac['contact']['xml']>>, source='ACCOUNT', sourceKey={accountId}) affected {rc} row(s).")
            if ac['contact']['address'] is not None:
                try:
                    rc = mjdb.bp_entity_upsert('address', ac['contactId'], ac['contact']['address'], source='ACCOUNT', type='CONTACT')
                except Exception as e:
                    lf.error(f"mjdb.bp_entity_upsert('address', {ac['contactId']}, <<ac['contact']['address']>>, source='ACCOUNT', type='CONTACT')\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_entity_upsert('address', {ac['contactId']}, <<ac['contact']['address']>>, source='ACCOUNT', type='CONTACT') affected {rc} row(s).")
            if ac['contact']['phones'] is not None:
                try:
                    rc = mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'CONTACT', ac['contactId']))
                except Exception as e:
                    lf.error(f"mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'CONTACT', {ac['contactId']}))\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_entity_action('phone', 'delete', ('ACCOUNT', 'CONTACT', {ac['contactId']})) affected {rc} row(s).")
                for phone in ac['contact']['phones']:
                    try:
                        rc = mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT', 'CONTACT', ac['contactId'], phone))
                    except Exception as e:
                        lf.error(f"mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT', 'CONTACT', {ac['contactId']}, <<phone>>))\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_entity_action('phone', 'insert', ('ACCOUNT', 'CONTACT', {ac['contactId']}, <<phone>>)) affected {rc} row(s).")

if __name__ == '__main__':
    main()