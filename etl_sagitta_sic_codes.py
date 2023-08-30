import mjdb 
import sgws 
import config 
import common as cmn 
import pandas as pd 
from sqlalchemy import create_engine 

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR, 'sic_codes')

def sic_code_row(sagitem, soup):
    row = {'sagitem':sagitem}
    cols = {'uc':'a2','description':'a3','category':'a4'}
    for k in cols:
        row[k] = soup.find(cols[k]).text if soup.find(cols[k]) else None
    return row

def main():
    sicCodes = []
    try:
        # parse response for individual items
        for item in sgws.post_ptr_access_statement("SELECT SIC.CODES").find_all('Item'):
            try:
                # parse item into dictionary, append to list
                sagitem = item.get('sagitem')
                sicCodes.append(sic_code_row(sagitem,item))
            except Exception as e:
                lf.error(f"unable to parse item {sagitem}\n{e}")
    except:
        lf.error(f"unable to parse access statement\n{e}")
    try:
        # convert list of rows to dataframe, stage in database
        rcs = pd.DataFrame(sicCodes).to_sql('stg_sic_codes', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
    except Exception as e:
        lf.error(f"unable to stage dataframe\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} row(s) staged for sic_codes")
            try:
                rcu = mjdb.upsert_stage('sagitta', 'sic_codes', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('sagitta', 'sic_codes')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('sagitta', 'sic_codes') affected {rcu} row(s).")
                mjdb.drop_table('sagitta', 'stg_sic_codes')

if __name__ == '__main__':
    main()