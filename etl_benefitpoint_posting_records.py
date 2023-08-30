import mjdb
import bpws
import config
import common as cmn
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
from dateutil import relativedelta as rd

LOGDIR = 'etl_benefitpoint'
SCHEMA = 'benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'posting_records')

def posting_record_row(postingRecordID, accountingMonth, soup):
    row = {
        'posting_record_id':postingRecordID,
        'accounting_month':accountingMonth,
        'posted_amount':float(soup.find('postedAmount').text) if soup.find('postedAmount') else None
    }
    for b in ('voided_record','accept_tolerance','statement_split'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for i in ('statement_id','voided_posting_record_id', 'statement_entry_id'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('split_column_type','split_basis_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('effective_as_of','posted_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text, WSTSFMT) if soup.find(tag) else None
    return row

def payee_amount_row(postingRecordID, payeeID, soup):
    row = {
        'posting_record_id':int(postingRecordID),
        'payee_id':payeeID,
        'team_owner_payee_id':int(soup.find('teamOwnerPayeeID').text) if soup.find('teamOwnerPayeeID') else None,
        'check_number':soup.find('checkNumber').text if soup.find('checkNumber') else None,
        'check_date':dt.datetime.timestrptime(soup.find('checkDate').text,WSTSFMT) if soup.find('checkDate') else None
    }
    for f in ('percentage','amount'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    return row
    
def main():
    try:
        lastPeriod = mjdb.bp_accounting_last_period('posting_record') if mjdb.bp_accounting_last_period('posting_record') else dt.date(2021,8,1)
    except Exception as e:
        lf.error(f"unable to get most recent accounting month:\n{e}")
    else:
        while lastPeriod < (dt.datetime.today().replace(day=1)-dt.timedelta(days=1)).replace(day=1,hour=0,minute=0,second=0,microsecond=0).date():
            postingRecords = []
            payeeAmounts = []
            accountingMonth = lastPeriod + rd.relativedelta(months=1)
            statements = mjdb.bp_statement_entry_per_accounting_month(accountingMonth)
            if len(statements) > 0:
                for statementID,productID in statements: 
                    fprResponse = bpws.find_posting_records(productID, statementID=statementID)
                    fprSoup = bs(fprResponse.content,'xml')
                    if fprResponse.ok==False:
                        raise ValueError(f"status_code: {fprResponse.status_code}, faultCode: {fprSoup.find('faultcode').text}, faultString: {fprSoup.find('faultstring').text}")
                    else:
                        for pr in fprSoup.find_all('postingRecords'):
                            postingRecordID = int(pr.find('postingRecordID').text)
                            postingRecords.append(posting_record_row(postingRecordID,accountingMonth,pr))
                            [payeeAmounts.append(payee_amount_row(postingRecordID,int(pa.find('payeeID').text),pa)) for pa in pr.find_all('payeeAmounts')]
            else:
                lf.info(f"no statements for {accountingMonth}")
                break
            stages = {
                'posting_record':postingRecords if postingRecords else None,
                'payee_amount':payeeAmounts if payeeAmounts else None
            }
            for s in stages:
                if stages[s]:
                    try:
                        rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        lf.error(f"unable to stage records for {s}:\n{e}")
                    else:
                        lf.info(f"{rcs} record(s) staged for {s}, accountingMonth: {accountingMonth}")
                        if rcs > 0:
                            try:
                                rcu = mjdb.upsert_stage(SCHEMA, s, 'insert')
                            except Exception as e:
                                lf.error(f"unable to upsert from stage to {s}:\n{e}")
                            else:
                                lf.info(f"{rcu} record(s) affected for {s}")
                        else:
                            lf.info(f"no records to stage for {s}")
                    finally:
                        mjdb.drop_table(SCHEMA, f'stg_{s}')
            try:
                lastPeriod = mjdb.bp_accounting_last_period('posting_record')
            except Exception as e:
                lf.error(f"unable to update lastPeriod:\n{e}")

if __name__ == '__main__':
    main()