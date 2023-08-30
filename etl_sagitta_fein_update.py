import mjdb
import sgws
import config
import common as cmn
import pandas as pd
import datetime as dt
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR,'fein_update')

def main():
    feins = []
    try:
        batches = sgws.post_ptr_access_statement("SELECT CLIENTS *BATCH*")
    except Exception as e:
        lf.error(f'sgws.post_ptr_access_statement("SELECT CLIENTS *BATCH*")\n{e}')
    else:
        for batch in hlp.parse_batch_items(batches):
            try:
                batchStatement = f"SELECT CLIENTS *GET.BATCH* {batch}"
                clientXml = sgws.post_ptr_access_statement(batchStatement)
            except Exception as e:
                lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
            else:
                for item in clientXml.find_all('Item'):
                    try:
                        if item.find('FEIN'):
                            feins.append({'sagitem':int(item.get('sagitem')),'fein':item.find('FEIN').text})
                    except Exception as e:
                        lf.error(f"feins.append({'sagitem':{int(item.get('sagitem'))},'fein':{item.find('FEIN').text}})\n{e}")
        # stage updates
        try:
            rcs = pd.DataFrame(feins).to_sql('stg_fein_update',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
        except Exception as e:
            lf.error(f"pd.DataFrame(<<feins>>).to_sql('stg_fein_update',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')\n{e}")
        else:
            lf.info(f"{rcs} record(s) staged for fein_update")

if __name__ == '__main__':
    main()