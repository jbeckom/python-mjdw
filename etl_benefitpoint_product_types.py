import mjdb 
import bpws 
import config 
import common as cmn 
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
SCHEMA = 'benefitpoint'
LOGDIR = 'etl_benefitpoint'
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'product_types')

def product_type_row(soup):
    return {
        'product_type_id':int(soup.find('productTypeID').text),
        'short_description':soup.find('shortDescription').text if soup.find('shortDescription') else None,
        'long_description':soup.find('longDescription').text if soup.find('longDescription') else None,
        'line_of_coverage':soup.find('lineOfCoverage').text if soup.find('lineOfCoverage') else None,
        'rate_types':', '.join([rt.find('rateTypeID').text for rt in soup.find_all('rateTypes')]) if len(soup.find_all('rateTypes')) > 0 else None
    }

def rate_type_row(soup):
    return {
        'rate_type_id':int(soup.find('rateTypeID').text),
        'description':soup.find('description').text if soup.find('description') else None,
        'funding_type':soup.find('fundingType').text if soup.find('fundingType') else None
    }

def main():
    productTypes = []
    rateTypes = []

    try:
        gpt = bpws.get_product_types('true')
        gptSoup = bs(gpt.content,'xml')
        if gpt.ok==False:
            raise ValueError(f"status_code: {gpt.status_code}, faultCode: {gptSoup.find('faultcode').text}, faultString: {gptSoup.find('faultstring').text}")
        else:
            for productType in gptSoup.find_all('productTypes'):
                try:
                    productTypes.append(product_type_row(productType))
                except Exception as e:
                    lf.error(f"unable to parse producType:\n{e}")
                else:
                    try:
                        [rateTypes.append(rate_type_row(rt)) for rt in productType.find_all('rateTypes')]
                    except Exception as e:
                        lf.error(f"unable to parse rateType:\n{e}")
    except Exception as e:
        lf.error(f"unable to parse getProducTypes:\n{e}")

    stages = {
        'product_type':productTypes if productTypes else None,
        'rate_type':rateTypes if rateTypes else None
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