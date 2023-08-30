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

lf = cmn.log_filer(LOGDIR, 'carrier_summaries')

def carrier_summary_row(soup):
    return {
        'carrier_id':int(soup.find('carrierID').text),
        'carrier_name':soup.find('carrierName').text if soup.find('carrierName') else None,
        'carrier_alias':soup.find('carrierAlias').text if soup.find('carrierAlias') else None
    }

def main():
    carrierSummaries = []
    try:
        gac = bpws.get_available_carriers()
        gacSoup = bs(gac.content,'xml')
        if gac.ok==False:
            raise ValueError(f"status_code: {gac.status_code}, faultCode: {gacSoupSoup.find('faultcode').text}, faultString: {gacSoup.find('faultstring').text}")
        else:
            try:
                [carrierSummaries.append(carrier_summary_row(cs)) for cs in gacSoup.find_all('carriers')]
            except Exception as e:
                lf.error(f"unable to parse carrier_summary_row:\n{e}")
    except Exception as e:
        lf.error(f"unable to parse getAvailableCarriers:\n{e}")
    else:
        try:
            rcs = pd.DataFrame(carrierSummaries).drop_duplicates().to_sql(f'stg_carrier_summary',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for carrier_summary:\n{e}")
        else:
            lf.info(f"{rcs} record(s) staged for carrier_summary")
            if rcs > 0:
                try:
                    rcu = mjdb.upsert_stage(SCHEMA, 'carrier_summary', 'upsert')
                except Exception as e:
                    lf.error(f"unable to upsert from stage to carrier_summary:\n{e}")
                else:
                    lf.info(f"{rcu} record(s) affected for carrier_summary")
            else:
                lf.info(f"no records to stage for carrier_summary")
        finally:
            mjdb.drop_table(SCHEMA, 'stg_carrier_summary')

if __name__ == '__main__':
    main()