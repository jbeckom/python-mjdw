import os
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

lf = cmn.log_filer(LOGDIR, 'rates')

def get_rate_ids(lastMod):
    rate_ids = []
    if (dt.datetime.now() - lastMod).days > 30:
        for product in mjdb.get_table(SCHEMA, 'product', cols=['product_id']):
            try:        
                frResp = bpws.find_rates(productID=product[0])
                frSoup = bs(frResp.content,'xml')
                if frResp.ok==False:
                    raise ValueError(f"status_code: {frResp.status_code}, faultCode: {frSoup.find('faultcode').text}, faultString: {frSoup.find('faultstring').text}")
                else:
                    [rate_ids.append(fr.find('rateID').text) for fr in frSoup.find_all('rates')]
            except Exception as e:
                lf.error(f"unable to parse find_rates for productID {product[0]}:\n{e}")
    else:
        try:
            fcResp = bpws.find_changes(sinceLastModifiedOn=lastMod,typesToInclude='Rate')
            fcSoup = bs(fcResp.content,'xml')
            if fcResp.ok==False:
                raise ValueError(f"status_code: {fcResp.status_code}, faultCode: {fcSoup.find('faultcode').text}, faultString: {fcSoup.find('faultstring').text}")
            else:
                [rate_ids.append(mod.find('entityID').text) for mod in fcSoup.find_all('modifications')]
        except Exception as e:
            lf.error(f"unable to parse findChanges, sinceLastModification={lastMod}, typesToInclude='Rate':\n{e}")
    return rate_ids

def rate_row(rateID,soup):
    row = {'rate_id':int(rateID)}
    for b in ('include_ee','age_banded','age_banded_gender_specific'):
        tag = cmn.bp_col_to_tag(b).replace('Ee','EE')
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for f in ('estimated_monthly_premium','estimated_monthly_revenue','renewal_percentage_change'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    for i in ('product_id','response_id','rate_type_id','rate_type_tier_id','age_banded_start_on','age_banded_end_on','age_banded_interval','rate_guarantee','payment_cycle'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('description','rating_method','additional_info','rate_guarantee_uom'):
        tag = cmn.bp_col_to_tag(s).replace('Uom','UOM')
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('effective_as_of','expiration_on','number_of_lives_as_of','last_modified_on','created_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None
    return row

def benefit_summary_description_row(rateID,soup):
    return {
        'rate_id':int(rateID),
        'benefit_summary_id':int(soup.find('benefitSummaryID').text),
        'product_id':int(soup.find('productID').text) if soup.find('productID') else None,
        'description':soup.find('description').text if soup.find('description') else None
    }

def commission_row(rateID,soup):
    row = {
        'rate_id':int(rateID),
        'commission_id':int(soup.find('commissionID').text),
        'estimated_number_of_members':int(soup.find('estimatedNumberOfMembers').text) if soup.find('estimatedNumberOfMembers') else None
    }
    for f in ('estimated_monthly_revenue','flat_percentage_of_premium','premium_override_amount'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    for s in ('commission_type','commission_paid_by','description','additional_info'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def flat_fee_row(commissionID,soup):
    row = {
        'commission_id':int(commissionID),
        'fee':float(soup.find('fee').text),
        'estimated_number_of_claims':int(soup.find('estimatedNumberOfClaims').text) if soup.find('estimatedNumberOfClaims') else None
    }
    for s in ('estimated_number_of_claims_interval_type','fee_interval_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def capitated_fee_row(commissionID,soup):
    row = {
        'commission_id':int(commissionID),
        'fee':float(soup.find('fee').text) if soup.find('fee') else None,
        'estimated_number_of_employees':int(soup.find('estimatedNumberOfEmployees').text) if soup.find('estimatedNumberOfEmployees') else None
    }
    for s in ('fee_lives_type','fee_interval_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def commission_range_row(sourceType,commissionID,soup):
    row = {
        'source_type':sourceType,
        'commission_id':int(commissionID),
        'value_unit_of_measure':soup.find('valueUnitOfMeasure').text if soup.find('valueUnitOfMeasure') else None,
        'sort_order':int(soup.find('sortOrder').text)
    }
    for f in ('value','from_range','to_range'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    return row

def rate_field_value_row(rateID,soup):
    row = {
        'rate_id':int(rateID),
	    'rate_field_id':int(soup.find('rateFieldID').text),
        'value_num':float(soup.find('valueNum').text) if soup.find('valueNum') else None
    }
    for i in ('multi_value_index','age_band_index'):
        tag = cmn.bp_col_to_tag(i)
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('value_text','age_band_gender'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def rate_field_row(soup):
    row = {
        'rate_field_id':int(soup.find('rateFieldID').text),
        'rate_field_group':int(soup.find('rateFieldGroupID').text) if soup.find('rateFieldGroupID') else None
    }
    for b in ('tiered','calculated'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for s in ('label','field_type','field_value_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def rate_option_value_row(rateFieldID, soup):
    return {
        'rate_field_id':int(rateFieldID),
	    'rate_option_value_id':int(soup.find('rateOptionValueID').text),
        'description':soup.find('description').text if soup.find('description') else None
    }

def rate_field_group_row(soup):
    row = {
        'rate_field_group_id':int(soup.find('rateFieldGroupID').text),
        'max_values_allowed':int(soup.find('maxValuesAllowed').text) if soup.find('maxValuesAllowed') else None
    }
    for s in ('description','rate_field_group_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def rate_tier_row(rateFieldID, soup):
    return {
        'rate_field_id':int(rateFieldID),
	    'rate_tier_id':int(soup.find('rateTierID').text),
        'description':soup.find('description').text if soup.find('description') else None,
        'allow_include_ee':cmn.bp_parse_bool(soup.find('allowIncludeEE').text) if soup.find('allowIncludeEE') else None
    }

def response_rate_info_row(rateID,soup):
    row = {
        'rate_id':int(rateID),
	    'rate_version':int(soup.find('rateVersion').text),
        'parent_rate_id':int(soup.find('parentRateID').text) if soup.find('parentRateID') else None,
        'rate_version_reasons':', '.join([rvr.text for rvr in soup.find_all('rateVersionReasons')]) if soup.find('rateVersionReasons') else None,
        'quote_valid_through':dt.datetime.strptime(soup.find('quoteValueThrough').text, WSTSFMT) if soup.find('quoteValueThrough') else None
    }
    for b in ('selected','created_by_broker'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    return row

def main():
    rates = []
    benefitSummaryDescriptions = []
    commissions = []
    flatFees = []
    capitatedFees = []
    commissionRanges = []
    rateFieldValues = []
    rateFields = []
    rateOptionValues = []
    rateFieldGroups = []
    rateTiers = []
    responseRateInfos = []
    try:
        lastMod = mjdb.bp_last_modified('rate') if mjdb.bp_last_modified('rate') else dt.datetime(1900,1,1,0,0)
    except Exception as e:
        lf.error(f"unable to get last modified date from EDW:\n{e}")
    else:
        for rateID in get_rate_ids(lastMod):
            try:
                grResp = bpws.get_rate(rateID)
                grSoup = bs(grResp.content,'xml')
                if grResp.ok == False:
                    raise ValueError(f"status_code: {grResp.status_code}, faultCode: {grSoup.find('faultcode').text}, faultString: {grSoup.find('faultstring').text}")
                else:
                    try:
                        rates.append(rate_row(rateID,grSoup))
                    except Exception as e:
                        lf.error(f"unable to parse rate_row for {rateID}:\n{e}")
                    else:
                        try:
                            [benefitSummaryDescriptions.append(benefit_summary_description_row(rateID, bsd)) for bsd in grSoup.find_all('associatedBenefitSummaries')]
                        except Exception as e:
                            lf.error(f"unable to parse benefit_summary_description_row for rateID {rateID}:\n{e}")
                        for c in grSoup.find_all('commissions'):
                            try:
                                commissionID = c.find('commissionID').text
                                commissions.append(commission_row(rateID,c))
                            except Exception as e:
                                lf.error(f"unable to parse commission_row for rateID {rateID}:\n{e}")
                            try:
                                flatFees.append(flat_fee_row(commissionID,c.find('flatFee'))) if c.find('flatFee') else None
                            except Exception as e:
                                lf.error(f"unable to parse flat_fee_row for rateID {rateID}:\n{e}")
                            try:
                                capitatedFees.append(capitated_fee_row(commissionID,c.find('capitatedFee'))) if c.find('capitatedFee') else None
                            except Exception as e:
                                lf.error(f"unable to parse capitated_fee_row for rateID {rateID}:\n{e}")
                            for x in {'gradedPercentageOfPremium':'GRADED PERCENTAGE OF PREMIUM','memberBasedSlidingSchedule':'MEMBER BASED SLIDING SCHEDULE'}.items():
                                try:
                                    [commissionRanges.append(commission_range_row(x[1],commissionID,cr)) for cr in c.find(x[0]).find_all('commissionRanges')] if c.find(x[0]) else None
                                except Exception as e:
                                    lf.error(f"unable to parse commission_rage_row({x[1]}) for rateID {rateID}:\n{e}")
                        for rfv in grSoup.find_all('rateFieldValues'):
                            try:
                                rateFieldValues.append(rate_field_value_row(rateID,rfv))
                            except Exception as e:
                                lf.error(f"unable to parse rate_field_value_row for rateID {rateID}:\n{e}")
                            try:
                                rf = rfv.find('rateField')
                                rateFields.append(rate_field_row(rf))
                            except Exception as e:
                                lf.error(f"unable to parse rate_field_row for rateID {rateID}:\n{e}")
                            try:
                                rateFieldGroups.append(rate_field_group_row(rf.find('rateFieldGroup')))
                            except Exception as e:
                                lf.error(f"unable to parse rate_field_group for rateID {rateID}:\n{e}")
                            try:
                                [rateOptionValues.append(rate_option_value_row(rf.find('rateFieldID').text,rvo)) for rvo in rf.find_all('optionValues')]
                            except Exception as e:
                                lf.error(f"unable to parse rate_option_value_row for rateID {rateID}:\n{e}")
                            try:
                                rateTiers.append(rate_tier_row(rfv.find('rateFieldID').text, rfv.find('rateTier'))) if rfv.find('rateTier') else None
                            except Exception as e:
                                lf.error(f"unable to parse rate_tier_row for rateID {rateID}:\n{e}")
                        try:
                            responseRateInfos.append(response_rate_info_row(rateID,grSoup.find('responseRateInfo'))) if grSoup.find('responseRateInfo') else None
                        except Exception as e:
                            lf.error(f"unable to parse response_rate_info_row for rateID {rateID}:\n{e}")
            except Exception as e:
                lf.error(f"unable to parse bpws.get_rate for {rateID}:\n{e}")
        stages = {
            'rate':rates if rates else None,
            'benefit_summary_description':benefitSummaryDescriptions if benefitSummaryDescriptions else None,
            'commission':commissions if commissions else None,
            'flat_fee':flatFees if flatFees else None,
            'capitated_fee':capitatedFees if capitatedFees else None,
            'commission_range':commissionRanges if commissionRanges else None,
            'rate_field_value':rateFieldValues if rateFieldValues else None,
            'rate_field':rateFields if rateFields else None,
            'rate_option_value':rateOptionValues if rateOptionValues else None,
            'rate_field_group':rateFieldGroups if rateFieldGroups else None,
            'rate_tier':rateTiers if rateTiers else None,
            'response_rate_info':responseRateInfos if responseRateInfos else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).drop_duplicates().to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}:\n{e}")
                else:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    if rcs > 0:
                        if s == 'rate_field_value':
                            try:
                                rcd = mjdb.function_execute(SCHEMA,f'sp_{s}_delete_staged')
                            except Exception as e:
                                lf.error(f"unable to delete for {s}:\n{e}")
                            else:
                                lf.info(f"{rcd} record(s) deleted for {s}")
                                try:
                                    rci = mjdb.upsert_stage(SCHEMA, s, 'insert')
                                except Exception as e:
                                    lf.error(f"unable to upsert from stage to {s}:\n{e}")
                                else:
                                    lf.info(f"{rci} record(s) affected for {s}")
                        else:
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