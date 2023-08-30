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

lf = cmn.log_filer(LOGDIR, 'offices')

def office_row(soup):
    return {
        'office_id':int(soup.find('officeID').text),
        'office_name':soup.find('officeName').text if soup.find('officeName') else None,
        'region_name':soup.find('regionName').text if soup.find('regionName') else None
    }

def main():
    offices = []
    try:
        fo = bpws.find_offices()
        foSoup = bs(fo.content,'xml')
        if fo.ok==False:
            raise ValueError(f"status_code: {fo.status_code}, faultCode: {foSoup.find('faultcode').text}, faultString: {foSoup.find('faultstring').text}")
        else:
            try:
                [offices.append(office_row(o)) for o in foSoup.find_all('offices')]
            except Exception as e:
                lf.error(f"unable to parse office_row:\n{e}")
    except Exception as e:
        lf.error(f"unable to parse findOffices:\n{e}")
    else:
        try:
            rcs = pd.DataFrame(offices).drop_duplicates().to_sql('stg_office',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"unable to stage records for office:\n{e}")
        else:
            lf.info(f"{rcs} record(s) staged for office")
            if rcs > 0:
                try:
                    rcu = mjdb.upsert_stage(SCHEMA, 'office', 'upsert')
                except Exception as e:
                    lf.error(f"unable to upsert from stage to office:\n{e}")
                else:
                    lf.info(f"{rcu} record(s) affected for office")
            else:
                lf.info("no records to stage for office")
        finally:
            mjdb.drop_table(SCHEMA, 'stg_office')

if __name__ == '__main__':
    main()