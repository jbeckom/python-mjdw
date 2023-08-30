import mjdb 
import bpws 
import config 
import common as cmn 
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_benefitpoint'
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'users')

def modified_users(lastMod):
    """Returns list of UserID(s) that have been modified since provided last modified date"""
    updates = []
    if (dt.datetime.now(dt.timezone.utc) - lastMod).days <= 30:
        try:
            fc = bpws.find_changes(sinceLastModifiedOn=lastMod, typesToInclude='User')
        except Exception as e:
            raise ValueError(f"bpws.find_changes(sinceLastModifiedOn={lastMod}, typesToInclude='User')\n{e}")
        else:
            try:
                for x in bs(fc.content,'xml').find_all('modifications'):
                    if dt.datetime.strptime(x.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(int(x.find('entityID').text))
            except Exception as e:
                raise ValueError(f"unable to parse findChangesResponse\n{e}")
            else:
                return updates
    else:
        try:
            fu = bpws.find_users(strsinceLastModifiedOn=dt.datetime.strftime(lastMod, WSTSFMT))
        except Exception as e:
            raise ValueError(f"bpws.find_users({lastMod})\n{e}")
        else:
            try:
                for x in bs(fu.content, 'xml').find_all('summaries'):
                    if dt.datetime.strptime(x.find('lastModifiedOn').text, WSTSFMT) > lastMod:
                        updates.append(int(x.find('ID').text))
            except Exception as e:
                raise ValueError(f"unable to parse findUsersResponse\n{e}")
            else:
                return updates

def col_to_tag(col):
    c = col.split('_')
    return c[0] + ''.join(x.title() for x in c[1:])

def user_row(userID, soup):
    textCols = ('username','first_name','last_name','title','notes')
    tsCols = ('last_modified_on','last_login_on','last_lockout_on','created_on')
    row = {
        'user_id':int(userID),
        'office_id':int(soup.find('officeID').text) if soup.find('officeID') else None
    }
    for a in textCols:
        tag = col_to_tag(a)
        row[a] = soup.find(tag).text if soup.find(tag) else None
    for b in tsCols:
        tag = col_to_tag(b)
        row[b] = dt.datetime.strptime(soup.find(tag).text, WSTSFMT) if soup.find(tag) else None
    row['roles'] = ', '.join([x.find('role').text for x in soup.find_all('roles')])
    return row
    

def main():
    userRows = []
    lastMod = mjdb.bp_last_modified('user') if mjdb.bp_last_modified('user') else dt.datetime(1900,1,1,0,0,0,tzinfo=dt.timezone.utc)
    # get user detail for each userID
    for userID in modified_users(lastMod):
        try:
            getUser = bpws.get_user(str(userID))
            if getUser.ok == False:
                raise ValueError(f"status_code: {getUser.status_code}, faultCode: {userSoup.find('faultcode').text}, faultString: {userSoup.find('faultstring').text}")
            else:
                # parse user detail into dictionary, append to list
                userRows.append(user_row(userID,bs(getUser.content,'xml')))
        except Exception as e:
            lf.error(f"bpws.get_user({userID})\n{e}")
    try:
        # convert row list to dataframe, stage in DB
        rcs = pd.DataFrame(userRows).to_sql('stg_user',ENGINE,'benefitpoint','replace',chunksize=10000,method='multi')
    except Exception as e:
        lf.error(f"unable to stage dataframe\n{e}")
    else:
        if rcs > 0:
            lf.info(f"{rcs} rows staged for users")
            try:
                rcu = mjdb.upsert_stage('benefitpoint', 'user', 'upsert')
            except Exception as e:
                lf.error(f"mjdb.upsert_stage('benefitpoint', 'user')\n{e}")
            else:
                lf.info(f"mjdb.upsert_stage('benefitpoint', 'user') affected {rcu} row(s)")
                mjdb.drop_table('benefitpoint', 'stg_user')

if __name__ == '__main__':
    main()