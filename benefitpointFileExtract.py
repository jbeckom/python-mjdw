import bpws
import mjdb
import config
import pandas as pd
import common as cmn
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

cfgs = [
    {
        'entity':'CustomFieldStructure',
        'criteria':'customizationArea',
        'criteriaVals':'Account_Summary,Activity_Log,Carrier_Contact,Service_Info,Plan_Info,Account_Contact'
    }
]

# def reference_dataframe(obj):
#     return pd.read_sql(f"SELECT * FROM benefitpoint.{obj};", ENGINE)

def main():
    sessionId = None
    # caDf = reference_dataframe('vw_customization_areas')
    # accDf = reference_dataframe('vw_account_customization_categories')
    for cfg in cfgs:
        lf = cmn.log_filer('benefitpointFileExtract', cfg['entity'])
        # use existing session token if still valid, else reqeust a new one
        sessionId = bpws.login_session(sessionId)
        # iterate each Customization Area
        for ca in cfg['criteriaVals'].split(','):
            # iterate sections in customizationArea xml
            for section in bpws.get_custom_field_structure(sessionId, ca):
                # upsert custom_field_section
                try:
                    # caId = int(caDf.loc[caDf['value'] == ca, 'ca_id'].iloc[0])
                    rc = mjdb.bp_custom_section_upsert(ca, section['label'], section['xml'])
                except Exception as e:
                    lf.error(f"mjdb.bp_custom_section_upsert({ca}, {section['label']}, <<section['xml']>>)\n{e}")
                else:
                    if rc > 0:
                        lf.info(f"mjdb.bp_custom_section_upsert({ca}, {section['label']}, <<section['xml']>>) successfully upserted {rc} record(s)")
                
                # get id for custom section
                # try:
                #     csId = int(mjdb.bp_custom_section_id(section['label']))
                # except Exception as e:
                #     lf.error(f"mjdb.bp_custom_section_id({section['label']})\n{e}")
                
                # iterate account customization categories
                for acc in section['accountCustomizationCategories']:
                    try:
                        # insert custom section/account classification type link
                        # accId = int(accDf.loc[accDf['value'] == acc, 'act_id'].iloc[0])
                        rc = mjdb.bp_custom_section_account_customization_categories_link(section['label'], acc)
                    except Exception as e:
                        lf.error(f"mjdb.bp_custom_section_account_customization_categories_link({section['label']}, {acc})\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_custom_section_account_customization_categories_link({section['label']}, {acc}) successfully inserted {rc} record(s).")

                # iterate sections/customFields
                for cf in section['customFields']:
                    # upsert custom_fields
                    try:
                        rc = mjdb.bp_custom_fields_upsert(section['label'], cf['customFieldID'], cf['xml'])
                    except Exception as e:
                        lf.error(f"mjdb.bp_custom_fields_upsert({section['label']}, {cf['customFieldID']}, <<cf['xml']>>)\n{e}")
                    else:
                        if rc > 0:
                            lf.info(f"mjdb.bp_custom_fields_upsert({section['label']}, {cf['customFieldID']}, <<cf['xml']>>) successfully upserted {rc} record(s).")
                    
                    # iterate optionValues for customFields
                    for ov in cf['optionValues']:
                        # upsert custom_field_option_option_value
                        try:
                            rc = mjdb.bp_custom_field_option_value_upsert(ov['id'], ov['xml'])
                        except Exception as e:
                            lf.error(f"mjdb.bp_custom_field_option_value_upsert({cf['customFieldID']}, {ov['id']}, <<ov['xml']>>)\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_custom_field_option_value_upsert({cf['customFieldID']}, {ov['id']}, <<ov['xml']>>) successfully upserted {rc} record(s).")
                        
                        # insert custom_field/custom_field_option_value relationship record
                        try:
                            rc = mjdb.bp_custom_field_custom_option_values_link(cf['customFieldID'], ov['id'])
                        except Exception as e:
                            lf.error(f"mjdb.bp_custom_field_custom_option_values_link({cf['customFieldID']}, {ov['id']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_custom_field_custom_option_values_link({cf['customFieldID']}, {ov['id']}) successfully inserted {rc} record(s).")
   
                    # iterate customFields/dependentFields
                    for df in cf['dependentFields']:
                        if df['triggerId'] is not None:
                            # upsert dependentTrigger (as customFieldOptionValue)
                            try:
                                rc = mjdb.bp_custom_field_dependent_trigger_upsert(df['triggerId'], df['triggerXml'])
                            except Exception as e:
                                lf.error(f"mjdb.bp_custom_field_dependent_trigger_upsert({df['triggerId']}, <<df['triggerXml']>>)\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.bp_custom_field_dependent_trigger_upsert({df['triggerId']}, <<df['triggerXml']>>) successfully upserted {rc} record(s).")

                        # upsert dependentFields records (as customFields)
                        try:
                            rc = mjdb.bp_custom_field_dependent_fields_upsert(df['fieldId'], df['fieldXml'])
                        except Exception as e:
                            lf.error(f"mjdb.bp_custom_field_dependent_fields_upsert({df['fieldId']}, <<df['fieldXml']>>)\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_custom_field_dependent_fields_upsert({df['fieldId']}, <<df['fieldTrigger']>>) successfully upserted {rc} record(s).")

                        # link dependentField to customField
                        try:
                            rc = mjdb.bp_custom_field_dependent_fields_link(cf['customFieldID'], df['fieldId'])
                        except Exception as e:
                            lf.error(f"mjdb.bp_custom_field_dependent_fields_link({cf['customFieldID']}, {df['fieldId']})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.bp_custom_field_dependent_fields_link({cf['customFieldID']}, {df['fieldId']}) successfully inserted {rc} row(s).")
    
    bpws.logout_session(sessionId)

if __name__ == '__main__':
    main()