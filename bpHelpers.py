import re
import config
import datetime as dt
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import create_engine
from xml.etree import ElementTree as ET

PATTERN = re.compile(r'(?<!^)(?=[A-Z])')
ENGINE = create_engine(config.config('config.ini', 'postgres_alchemy')['url'])

def stage_custom_field_structure(customArea, response):
    # PARSE BPWS RESPONSE TEXT TO ETREE ELEMENT
    customFieldStructure = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/broker/v4_3}getCustomFieldStructureResponse').find('customFieldStructure')
    
    # DECLARE EMPTY DATAFRAMES FOR EACH ENTITY
    customSectionsDf = pd.DataFrame()
    customFieldsDf = pd.DataFrame()
    optionValuesDf = pd.DataFrame()
    
    # PARSE XML TO DATAFRAME(S) -- (dependentField=customField, dependentTrigger=customFieldOptionValue)
    for section in customFieldStructure.findall('sections'):
        csdf = pd.DataFrame({a.tag:a.text for a in section.findall('./') if a.text is not None and a.tag != 'accountCustomizationCategories'}, index=['label'])
        csdf['customizationArea'] = customArea
        csdf['accountCustomizationCategories'] = ', '.join(acc.text for acc in section.findall('accountCustomizationCategories'))
        customSectionsDf = customSectionsDf.append(csdf)
        for cf in section.findall('customFields'):
            cfdf = pd.DataFrame({b.tag:b.text for b in cf.findall('./') if b.text is not None and b.text not in ('optionValues', 'dependentTrigger')}, index=['customFieldID'])
            cfdf['customSection'], cfdf['dependentFieldParentID'] = (customArea, pd.NA)
            customFieldsDf = customFieldsDf.append(cfdf)
            for ov in cf.findall('optionValues'):
                ovdf = pd.DataFrame({c.tag:c.text for c in ov.findall('./') if c.text is not None}, index=['customFieldOptionValueID'])
                ovdf['customFieldID'], ovdf['dependentTrigger'] = (cf.find('customFieldID').text, False)
                optionValuesDf = optionValuesDf.append(ovdf)
            # dependentField shares data structure of customField
            for df in cf.findall('dependentFields'):
                dfdf = pd.DataFrame({d.tag:d.text for d in df.findall('./') if d.text is not None}, index=['customFieldID'])
                dfdf['customSection'], dfdf['dependentFieldParentID'] = (customArea, cf.find('customFieldID').text)
                customFieldsDf = customFieldsDf.append(dfdf)
            # dependentTrigger shares data structure of customFieldOptionValue
            for dt in cf.findall('dependentTrigger'):
                dtdf = pd.DataFrame({e.tag:e.text for e in dt.finall('./') if e.text is not None}, index=['customFieldOptionValueID'])
                dtdf['customFieldID'], dtdf['dependentTrigger'] = (cf.find('customFieldID'), True)
                optionValuesDf = optionValuesDf.append(dtdf)

    # RESET COLUMN NAMES (for better behavior w/ postgresql), DUMP DATAFRAME TO STAGING TABLE(S) IN DATABASE
    if customSectionsDf.empty == False:
        customSectionsDf.columns = [x.lower() for (x,y) in customSectionsDf.iteritems()]
        customSectionsDf.to_sql('stg_custom_section', ENGINE, 'benefitpoint', 'append', False)
    if customFieldsDf.empty == False:
        customFieldsDf.columns = [x.lower() for (x,y) in customFieldsDf.iteritems()]
        customFieldsDf.to_sql('stg_custom_field', ENGINE, 'benefitpoint', 'append', False)
    if optionValuesDf.empty == False:
        optionValuesDf.columns = [x.lower() for (x,y) in optionValuesDf.iteritems()]
        optionValuesDf.to_sql('stg_custom_field_option_value', ENGINE, 'benefitpoint', 'append', False)

def parse_change_summary(changeResponse):
    # traverse xml, list modifications node(s)
    modifications = ET.fromstring(changeResponse.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/broker/v4_3}findChangesResponse').find('changes').findall('modifications')
    # iterate modifications xml, create dictionary
    modifications = [{'entityID':mod.find('entityID').text,'entityType':mod.find('entityType').text,'lastModifiedOn':dt.datetime.strptime(mod.find('lastModifiedOn').text, '%Y-%m-%dT%H:%M:%S.%f%z')} for mod in modifications]
    # return dictionary, sorted by last modification date
    return sorted(modifications, key=lambda x: x['lastModifiedOn'])

def parse_account_response(response):
    accountXml = ET.fromstring(response.text).find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://ws.benefitpoint.com/aptusconnect/broker/v4_3}getAccountResponse').find('account')
    # create dictionary for master Account Complex Type
    account = {a.tag:a.text for a in accountXml.findall('./') if a.text is not None}
    # create individual dict for each Address Complex Type
    for address in ['main', 'billing', 'mailing']:
        account[f'{address}Address'] = {b.tag:b.text for b in accountXml.find(f'{address}Address').findall('./') if b.text is not None} if accountXml.find(f'{address}Address') else None
    # iterate each CustomFieldValue Complex Type, create list of dicts
    for cfvType in ['account', 'serviceInfo']:
        account[f'{cfvType}CustomFieldValues'] = []
        for e in [d.findall('./') for d in [c for c in accountXml.findall(f'{cfvType}CustomFieldValues')]]:
            account[f'{cfvType}CustomFieldValues'].append({f.tag:f.text for f in e if f.text is not None})
    return account

# DEBUG ONLY
if __name__ == '__main__':
    # PATTERN.sub('_',a.tag).lower()
    # PATTERN.sub('_',b.tag).lower()
    pass