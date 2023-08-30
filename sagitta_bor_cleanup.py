import sgws 
import config
import common as cmn 
import sgHelpers as hlp 
from psycopg2 import pool 
from sqlalchemy import create_engine 

config_file = R'config.ini'

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'
HOST = config.config(config_file,'pgdb')['host']
DATABASE = config.config(config_file,'pgdb')['database']
USER = config.config(config_file,'pgdb')['user']
PASSWORD = config.config(config_file,'pgdb')['password']

scp = pool.SimpleConnectionPool(1,25,host=HOST,database=DATABASE,user=USER,password=PASSWORD)
lf = cmn.log_filer(LOGDIR,'bor_cleanup')

def sql_update(sagitem, borEff, borExp):
    sql = f"UPDATE sagitta.policies"
    if borEff and borExp is None:
        sql += f" SET  bor_effective_dt = {borEff} WHERE sagitem = {sagitem}"
    elif borExp and borEff is None: 
        sql += f" SET bor_expiration_dt = {borExp} WHERE sagitem = {sagitem}"
    elif borEff and borExp:
        sql += f" SET  bor_effective_dt = {borEff}, bor_expiration_dt = {borExp} WHERE sagitem = {sagitem}"
    else:
        raise ValueError("no BOR data available")
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
    except Exception as e:
        raise ValueError(f"unable to update: {e}")
    finally:
        scp.putconn(conn)

def main():
    items = []
    try:
        batches = sgws.post_ptr_access_statement("SELECT POLICIES *BATCH*")
    except Exception as e:
        lf.error(f'sgws.post_ptr_access_statement("SELECT POLICIES *BATCH*")\n{e}')
    else:
        for batch in hlp.parse_batch_items(batches):
            for item in sgws.post_ptr_access_statement(f"SELECT POLICIES *GET.BATCH* {batch}").find_all('Item'):
                try:
                    if (item.find('BOREffectiveDt') or item.find('BORExpirationDt')):
                        items.append({
                            'sagitem':int(item.get('sagitem')),
                            'bor_effective_dt':item.find('BOREffectiveDt').text if item.find('BOREffectiveDt') else None,
                            'bor_expiration_dt':item.find('BORExpirationDt').text if item.find('BORExpirationDt') else None,
                        })
                except Exception as e:
                    lf.error(f"unable to parse item {item.get('sagitem')}\n{e}")
    for x in items:
        try:
            sql_update(x['sagitem'],x['bor_effective_dt'],x['bor_expiration_dt'])
        except Exception as e:
            lf.error(f"sagitem: {x['sagitem']}\n{e}")
        else:
            lf.info(f"{x['sagitem']} successfully updated")

if __name__ == '__main__':
    main()