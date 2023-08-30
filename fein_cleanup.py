import uuid
import sgws
import config
import common as cmn
from psycopg2 import pool
from xml.etree import ElementTree as ET

# ad hoc solution uses legacy DB, no need to add to config file
scp = pool.SimpleConnectionPool(1,25,host='mj_db',database='mj_central',user='mj_postgres_user',password='MJ4Data2Analytics1*')

# instantiate logger
lf = cmn.log_filer('fein_cleanup', 'fein_cleanup')

def account_source_fein():
    sql = "SELECT account_id, source, fein FROM p_and_c.fein_cleanup;"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                # convert tuple (returned by fetchall) to list, remove special character from fein element, return list
                return [(x[0],x[1],x[2].replace('\ufeff','')) for x in [list(n) for n in cur.fetchall()]]
    finally:
        scp.putconn(conn)

def main():    
    # filter list for applicable data, send update request
    for each in [x for x in account_source_fein() if x[2] != 'NA' and x[1] == 'sagitta']:
        try:
            sgws.client_update(str(uuid.uuid1()), each[0], fein=each[2].replace('-',''))
        except Exception as e:
            lf.error(f"sgws.client_update({str(uuid.uuid1())}, {each[0]}, fein={each[2].replace('-','')})\n{e}")
        else:
            lf.info(f"sgws.client_update({str(uuid.uuid1())}, {each[0]}, fein={each[2].replace('-','')}) successful.")

if __name__ == '__main__':
    main()