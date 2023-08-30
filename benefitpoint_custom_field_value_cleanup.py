import re
import mjdb
import bpws
import config
import common as cmn
import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

LOGDIR = 'maintenance'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
# WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'
RESUB = r'(?<!^)(?=[A-Z])'

lf = cmn.log_filer(LOGDIR,'custom_field_value_cleanup')

def custom_field_value_row(cfvSource, sourceKey, soup):
    row = {
        'cfv_source':cfvSource,
        'source_key':sourceKey,
        'value_text':soup.find('valueText').text if soup.find('valueText') else None
    }
    for a,b in [('custom_field_value_id','customFieldValueID'), ('custom_field_id','customFieldID'), ('option_value_id','optionValueID')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    return row

def main():
    customFieldValues = []
    for account in mjdb.get_table('benefitpoint','account',cols=['account_id']):
        accountID = account[0]
        soup = bs(bpws.get_account(accountID).content,'xml')
        for x in ('account','serviceInfo'):
            for cfvSoup in soup.find_all(f'{x}CustomFieldValues'):
                try:
                    customFieldValues.append(custom_field_value_row(re.sub(RESUB,' ',x).upper(),accountID, cfvSoup))
                except Exception as e:
                    lf.error(f"unable to parse {x}CustomFieldValues for {accountID}:\n{e}")
    try:
        rcs = pd.DataFrame(customFieldValues).to_sql('stg_custom_field_value',ENGINE,'benefitpoint','replace',index=False,chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage record(s) for CustomFieldValues:\n{e}")
    else:
        lf.info(f"{rcs} record(s) staged for CustomFieldValues")
        try:
            rcd = mjdb.function_execute('benefitpoint','sp_custom_field_value_cleanup')
        except Exception as e:
            lf.error(f"unable to execute benefitpoint.sp_custom_field_value_cleanup()\n{e}")
        else:
            lf.info(f"{rcd} record(s) deleted for CustomFieldValues")
    finally:
        mjdb.drop_table('benefitpoint', f"stg_custom_field_value")

if __name__ == '__main__':
    main()