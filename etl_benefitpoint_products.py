import mjdb
import bpws
import config
import common as cmn
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

LOGDIR = 'etl_benefitpoint'
SCHEMA = 'benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'products')

def get_product_ids(lastMod):
    productIDs = []
    if (dt.datetime.now(dt.timezone.utc) - lastMod).days > 30:
        try:
            accounts = mjdb.get_table(SCHEMA, 'account', cols=['account_id'])
        except Exception as e:
            raise ValueError(f"unable to retrieve account IDs:\n{e}")
        else:
            for account in accounts:
                try:
                    findProductsResp = bpws.find_products(account[0],sinceLastModifiedOn=str(lastMod).replace(' ','T'))
                    findProductsXml = bs(findProductsResp.content,'xml')
                    if findProductsResp.ok == False:
                        raise ValueError(f"status_code: {findProductsResp.status_code}, faultCode: {findProductsXml.find('faultcode').text}, faultString: {findProductsXml.find('faultstring').text}")
                except Exception as e:
                    lf.error(f"unable to parse findProducts for accountID {account[0]}:\n{e}")
                else:
                    [productIDs.append(ps.find('productID').text) for ps in findProductsXml.find_all('summaries') if dt.datetime.strptime(ps.find('lastModifiedOn').text,WSTSFMT) >= lastMod]
    else:
        try:
            findChangesResp = bpws.find_changes(sinceLastModifiedOn=str(lastMod),typesToInclude='Product')
            findChangesXml = bs(findChangesResp.content,'xml')
            if findChangesResp.ok == False:
                raise ValueError(f"status_code: {findChangesResp.status_code}, faultCode: {findChangesXml.find('faultcode').text}, faultString: {findChangesXml.find('faultstring').text}")
            else:
                [productIDs.append(mod.find('entityID').text) for mod in findChangesXml.find_all('modifications')]
        except Exception as e:
            raise ValueError(f"unable to parse findChanges, sinceLastModifiedOn={str(lastMod)}:\n{e}")
    return productIDs

def product_row(accountID, productID, soup):
    row = {'account_id':accountID, 'product_id':productID}
    for b in ('is_additional_product','continuous_policy','voluntary_product','union_product','non_payable','non_revenue'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for f in ('total_estimated_monthly_revenue','total_estimated_monthly_premium'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    for i in ('product_type_id','carrier_id','broker_of_record_account_id','parent_product_id','number_of_eligible_employees','billing_carrier_id','office_id','department_id','primary_sales_lead_user_id','primary_servicer_lead_user_id','aca_sage_harbor_reporting_year','custom_cancellation_reason_id'):
        tag = (cmn.bp_col_to_tag(i)).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('product_status','name','policy_number','policy_origination_reason','policy_origination_reason_qualifier_id','cancellation_reason','cancellation_additional_information','reinstatement_reason','reinstatement_additional_information','premium_payment_frequency','billing_type','billing_carrier_type','metal_level_type','aca_safe_harbor_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('broker_of_record_as_of','effective_as_of','renewal_on','original_effective_as_of','cancellation_on','reinstatement_on','last_modified_on','created_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text, WSTSFMT) if soup.find(tag) else None
    return row

def msa_info_row(accountID, productID, soup):
    return {
        'account_id':accountID,
        'product_id':productID,
        'nation_wide':cmn.bp_parse_bool(soup.find('nationWide').text) if soup.find('nationWide') else None,
        'msa_ids':', '.join([x.text for x in soup.find_all('msaIDs')]) if len(soup.find_all('msaIDs')) > 0 else None
    }

def custom_field_value_row(sourceKey, customFieldValueID, soup):
    row = {
        'cfv_source':'PRODUCT',
        'source_key':sourceKey,
        'custom_field_value_id':customFieldValueID,
        'value_text':soup.find('valueText').text if soup.find('valueText') else None
    }
    for i in ('custom_field_id','option_value_id'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    return row

def additional_product_info_row(accountID, productID, soup):
    row = {
        'account_id':accountID,
        'product_id':productID,
        'requires_5500':cmn.bp_parse_bool(soup.find('requires5500').text) if soup.find('requires5500') else None,
        'estimated_commission':float(soup.find('estimatedCommission').text) if soup.find('estimatedCommission') else None
    }
    for i in ('erisa_plan_year_end_month','erisa_plan_year_end_day','alternative_plan_id'):
        tag = cmn.bp_col_to_tag(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('commission_period_type','notes','erisa_plan'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def additional_product_attribute_row(accountID, productID, attributeID, soup):
    row = {
        'account_id':accountID,
        'product_id':productID,
        'attribute_id':attributeID,
        'value_num':float(soup.find('valueNum').text) if soup.find('valueNum') else None,
        'option_value_id':int(soup.find('optionValueID').text) if soup.find('optionValueID') else None,
        'value_date':dt.datetime.strptime(soup.find('valueDate').text, WSTSFMT) if soup.find('valueDate') else None
    }
    for s in ('name','field_value_type','value_text'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def plan_info_row(accountID, productID, soup):
    row = {
        'account_id':accountID,
        'product_id':productID,
        'frozen_enrollment_effective_as_of':dt.datetime.strptime(soup.find('frozenEnrollmentEffectiveAsOf').text,WSTSFMT) if soup.find('frozenEnrollmentEffectiveAsOf') else None
    }
    for b in ('frozen_enrollment','requires_5500'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for i in ('erisa_plan_year_end_month','erisa_plan_year_end_day','maxiumum_group_size','minimum_group_size','market_group_plan_id'):
        tag = cmn.bp_col_to_tag(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('alternative_plan_id','erisa_plan','notes','market_size','exclusions_limitations','customizations','participation_requirements','participation_requirements_vol','state_list'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def retirement_plan_info_row(accountID, productID, soup):
    row = {
        'account_id':accountID,
        'product_id':productID,
        'audit_required':cmn.bp_parse_bool(soup.find('auditRequired').text) if soup.find('auditRequired') else None,
        'plan_adoption_on':dt.datetime.strptime(soup.find('planAdoptionOn').text, WSTSFMT) if soup.find('planAdoptionOn') else None
    }
    for t in ('record_keeper_plan_number','fiscal_year_from','fiscal_year_to','auditor','trustee','custodian'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def commission_info_row(accountID, productID, soup):
    row = {'account_id':accountID, 'product_id':productID}
    for s in ('alternative_policy_number','notes'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('new_business_until','commission_start_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text, WSTSFMT) if soup.find(tag) else None
    return row

def policy_integration_info_row(accountID, productID, soup):
    row = {'account_id':accountID, 'product_id':productID}
    for s in ('sagitta_policy_id','ams_policy_id','tam_policy_id','vision_policy_id'):
        tag = cmn.bp_col_to_tag(s).replace('Id','ID')
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row        

def main():
    # get date of last modified product record from EDW
    try:
        lastMod = mjdb.bp_last_modified('product') if mjdb.bp_last_modified('product') else dt.datetime(1900,1,1,0,0,tzinfo=dt.timezone.utc)
    except Exception as e:
        lf.error(f"unable to get last modified date from EDW:\n{e}")
    else:
        # instantiate list(s) for staging
        product = []
        msaInfo = []
        customFieldValue = []
        additionalProductInfo = []
        additionalProductAttribute = []
        planInfo = []
        retirementPlanInfo = []
        commissionInfo = []
        policyIntegrationInfo = []
        # iterate list of productIDs, get detail from Benefitpoint WS
        for productID in get_product_ids(lastMod):
            try:
                productResp = bpws.get_product(productID)
                if productResp.ok == False:
                    pXml = bs(productResp.content, 'xml')
                    raise ValueError(f"status_code: {productResp.status_code}, faultCode: {pXml.find('faultcode').text}, faultString: {pXml.find('faultstring').text}")
                else:
                    productXml = bs(productResp.content,'xml').find('getProductResponse')
                    accountID = int(productXml.find('accountID').text)
                    productID = int(productID)
                    try:
                        product.append(product_row(accountID, productID, productXml))
                    except Exception as e:
                        lf.error(f"unable to parse Product for accountID: {accountID}, productID: {productID}\n{e}")
                    else:
                        try:
                            [msaInfo.append(msa_info_row(accountID, productID, mi)) for mi in productXml.find_all('msaInfo') if len(mi.find_all()) > 0]
                        except Exception as e:
                            lf.error(f"unable to parse ProductMSAInfo for accountID: {accountID}, productID: {productID}\n{e}")
                        try:
                            [customFieldValue.append(custom_field_value_row(productID, int(cfv.find('customFieldValueID').text), cfv)) for cfv in productXml.find_all('customFieldValues')]
                        except Exception as e:
                            lf.error(f"unable to parse CustomFieldValue(s) for accountID: {accountID}, productID: {productID}\n{e}")
                        if productXml.find('additionalProductInfo'):
                            try:
                                [additionalProductInfo.append(additional_product_info_row(accountID, productID, api)) for api in productXml.find_all('additionalProductInfo') if len(api.find_all()) > 0]
                            except Exception as e:
                                lf.error(f"unable to parse AdditionalProductInfo for for accountID: {accountID}, productID: {productID}\n{e}")
                            else:
                                for apa in productXml.find('additionalProductInfo').find_all('attributes'):
                                    apa = bs(str(apa),'xml')
                                    additionalProductAttribute.append(additional_product_attribute_row(accountID, productID, int(apa.find('attributeID').text), apa))
                        try:
                            [planInfo.append(plan_info_row(accountID, productID, pi)) for pi in productXml.find_all('planInfo') if len(pi.find_all()) > 0]
                        except Exception as e:
                            lf.error(f"unable to parse PlanInfo for accountID: {accountID}, productID: {productID}\n{e}")
                        else:
                            try:
                                [retirementPlanInfo.append(retirement_plan_info_row(accountID, productID, rpi)) for rpi in productXml.find_all('retirementPlanInfo') if len(rpi.find_all()) > 0]
                            except Exception as e:
                                lf.error(f"unable to parse RetirementPlanInfo for accountID: {accountID}, productID: {productID}\n{e}")
                        try:
                            [commissionInfo.append(commission_info_row(accountID, productID, ci)) for ci in productXml.find_all('commissionInfo') if len(ci.find_all()) > 0]
                        except Exception as e:
                            lf.error(f"unable to parse CommissionInfo for accountID: {accountID}, productID: {productID}\n{e}")
                        try:
                            [policyIntegrationInfo.append(policy_integration_info_row(accountID, productID, pii)) for pii in productXml.find_all('policyIntegrationInfo') if len(pii.find_all()) > 0]
                        except Exception as e:
                            lf.error(f"unable to parse PolicyIntegrationInfo for accountID: {accountID}, productID: {productID}\n{e}")                                                
            except Exception as e:
                lf.error(f"unable to parse getProduct for {productID}:\n{e}")
        stages = {
            'product':product if product else None,
            'product_msa_info':msaInfo if msaInfo else None,
            'custom_field_value':customFieldValue if customFieldValue else None,
            'additional_product_info':additionalProductInfo if additionalProductInfo else None,
            'additional_product_attribute':additionalProductAttribute if additionalProductAttribute else None,
            'plan_info':planInfo if planInfo else None,
            'retirement_plan_info':retirementPlanInfo if retirementPlanInfo else None,
            'commission_info':commissionInfo if commissionInfo else None,
            'policy_integration_info':policyIntegrationInfo if policyIntegrationInfo else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}:\n{e}")
                else:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    if rcs > 0:
                        try:
                            rcu = mjdb.upsert_stage(SCHEMA, s, 'upsert')
                        except Exception as e:
                            lf.error(f"unable to upsert from stage to {s}:\n{e}")
                        else:
                            lf.info(f"{rcu} record(s) affected for {s}")
                    else:
                        lf.info(f"no records to stage for {s}")
                finally:
                    mjdb.drop_table(SCHEMA, f'stg_{s}')

if __name__ == '__main__':
    main()