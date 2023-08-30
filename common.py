import os
import logging
import pandas as pd
import datetime as dt
from functools import reduce

def log_filer (dir, file):
    """returns file specific logging instance"""
    logDir = os.path.join(f"C:\Python\logs\{dir}", str(dt.datetime.now().year), str(dt.datetime.now().month), str(dt.datetime.now().day))
    if not os.path.exists(logDir):
        os.makedirs(logDir)
    logger = logging.getLogger(file)
    logger.setLevel(logging.DEBUG)
    lf = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    fh = logging.FileHandler(os.path.join(logDir,f'{file.replace(".","_").lower()}_{dt.datetime.strftime(dt.datetime.today().date(), "%Y%m%d")}.log'))
    fh.setFormatter(lf)
    logger.addHandler(fh)
    return logger

def csv_dataframe(filePath, fileCols, **kwargs):
    df = pd.read_csv(filePath, usecols=fileCols, dtype='string')
    if 'miedge_eb' in filePath:
        df = df.loc[:,df.columns.str.find('.') < 0]
    df.columns = kwargs['targetCols'] if 'targetCols' in kwargs else df.columns
    return df

def merge_dataframes(dataframes, mergeOn, mergeHow, targetCols):
     df = reduce(lambda left,right: pd.merge(left,right,on=mergeOn,how=mergeHow), dataframes)
     df.columns = targetCols
     return df

def move_file(directory, file, action):
    fileNow = dt.datetime.strftime(dt.datetime.now(), '%Y%m%d-%H%M%S')
    destination = os.path.join(directory, action.title(), str(dt.date.today().year), f"{dt.date.today().month:02d}", f"{dt.date.today().day:02d}")
    if not os.path.exists(destination):
        os.makedirs(destination)
    os.rename(os.path.join(directory, file), os.path.join(destination, f"{fileNow}-{file}"))

def bp_col_to_tag(col):
    c = col.split('_')
    return c[0] + ''.join(x.title() for x in c[1:])

def bp_parse_bool(x):
    return True if x.lower() == 'true' else False

def bp_contact_row(contactSource, sourceKey, contactID, soup):
    row = {'contact_source':contactSource,'source_key':int(sourceKey),'contact_id':int(contactID)}
    for t in ('first_name','last_name','email'):
        tag = bp_col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def bp_address_row(addressSource, sourceType, sourceKey, soup):
    row = {'address_source':addressSource, 'source_type':sourceType, 'source_key':sourceKey}
    for t in ('street_1','street_2','city','state','zip','country'):
        tag = bp_col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def bp_phone_row(phoneSource, sourceType, sourceKey, soup):
    row = {'phone_source':phoneSource, 'source_type':sourceType, 'source_key':sourceKey}
    for t in ('area_code','number','type'):
        tag = bp_col_to_tag(t)
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def bp_carrier_appointment_row(carrierAppointmentSourse, sourceKey, carrierAppointmentID, soup):
    row = {
        'account_id':accountID,
        'appointment_on':dt.datetime.strptime(soup.find('appointmentOn').text, WSTSFMT) if soup.find('appointmentOn') else None,
        'active':bool(soup.find('active').text) if soup.find('active') else None,
        'appointment_number':soup.find('appointmentNumber').text if soup.find('appointmentNumber') else None,
        'states':', '.join([x.text for x in soup.find_all('states')])
    }
    for a,b in [('carrier_appointment_id','carrierAppointmentID'),('carrier_id','carrierID')]:
        row[a] = int(soup.find(b).text) if soup.find(b) else None
    return row

def bp_license_row(licenseSource, sourceKey, licenseID, soup):
    row = {
        'license_source':licenseSource,
        'source_key':int(sourceKey),
        'license_id':int(licenseID),
        'residence_license':bp_parse_bool(soup.find('residenceLicense').text) if soup.find('residenceLicense') else None
    }
    for s in ('state','license_number'):
        tag = bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('license_on','license_expires_on','e_and_o_expires_on'):
        tag = bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text, WSTSFMT) if soup.find(tag) else None
    return row

def raw_entity_df(table, conn, schema, **kwargs):
    # limit to 10k records per pass, combine to dataframe
    df = pd.concat([chunk for chunk in pd.read_sql_table(table, conn, schema, chunksize=10000)]).reset_index(drop=True)
    if df.empty:
        return df 
    else:
        df = df.query(kwargs['query']) if 'query' in kwargs else df
        df = df[kwargs['cols']] if 'cols' in kwargs else df
        return df

if __name__ == '__main__':
    lf = log_filer('test', 'test')
    pass