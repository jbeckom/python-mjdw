import mjdb 
import sgws 
import config 
import common as cmn 
import pandas as pd 
from sqlalchemy import create_engine 

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR, 'clients_addlinfo')

def clients_addlinfo_row(sagitem, soup):
    # instantiate return value
    row = {'sagitem':sagitem}
    # map db columns to xml tags
    ints = {'audit_entry_dt':'s2','audit_time':'s3'}
    texts = {
        'audit_staff_cd':'s1',
        'audit_cd':'s4',
        'audit_history_record_number':'s5',
        'audit_program':'s6',
        'serv_4':'a6',
        'serv_5':'a7',
        'client_exec':'a16',
        'prod_4':'a14',
        'prod_5':'a15',
        'budgeted_revenue':'a8',
        'invoice_contact':'a12',
        'rewrite':'a9',
        'invoice_email_address':'a10',
        'invoice_fax_number':'a11',
        'industry_segment':'a13',
        'mc_number':'a17',
        'dot_number':'a18'
    }
    for a in ints:
        row[a] = int(soup.find(ints[a]).text) if soup.find(ints[a]) else None
    for b in texts:
        row[b] = soup.find(texts[b]).text if soup.find(texts[b]) else None
    return row

def main():
    clientsAddlinfos = []
    try:
        # parse reponse for individual items
        for item in sgws.post_ptr_access_statement("SELECT CLIENTS.ADDLINFO").find_all('Item'):
            try:
                # parse item to dictionary, append to list
                sagitem = int(item.get('sagitem'))
                clientsAddlinfos.append(clients_addlinfo_row(sagitem,item))
            except Exception as e:
                lf.error(f"unable to parse item {sagitem}\n{e}")
    except:
        lf.error(f"unable to parse access statement\n{e}")
    else:
        try:
            # convert list of rows to dataframe, stage in database
            rcs = pd.DataFrame(clientsAddlinfos).to_sql('stg_clients_addlinfo', ENGINE, 'sagitta', 'replace', index=False, chunksize=10000, method='multi')
        except Exception as e:
            lf.error(f"unable to stage dataframe\n{e}")
        else:
            if rcs > 0:
                lf.info(f"{rcs} row(s) staged for clients_addlinfo")
                try:
                    rcu = mjdb.upsert_stage('sagitta', 'clients_addlinfo', 'upsert')
                except Exception as e:
                    lf.error(f"mjdb.upsert_stage('sagitta', 'clients_addlinfo')\n{e}")
                else:
                    lf.info(f"mjdb.upsert_stage('sagitta', 'clients_addlinfo') affected {rcu} row(s)")
                    mjdb.drop_table('sagitta', 'stg_clients_addlinfo')

if __name__ == '__main__':
    main()