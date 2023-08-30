import mjdb
import bpws
import config
import common as cmn
import pandas as pd
import datetime as dt
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
from calendar import monthrange
from dateutil import relativedelta as rd

LOGDIR = 'etl_benefitpoint'
SCHEMA = 'benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'statements')

def statement_row(statementID, soup):
    row = {
        'statement_id':statementID,
        'accounting_month':dt.datetime.strptime(soup.find('accountingMonth').text, '%Y-%m-%d%z') if soup.find('accountingMonth') else None,
        'statement_total':float(soup.find('statementTotal').text) if soup.find('statementTotal') else None,
        'ams360_gl_date':dt.datetime.strptime(soup.find('ams360GLDate').text,WSTSFMT) if soup.find('ams360GLDate') else None
    }
    for b in ('override_only','use_estimated_premium'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for i in ('billing_carrier_id','office_id','override_payee_id','created_by_user_id'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for s in ('statement_status','payement_method','notes'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('entry_date','statement_date','range_start_on','range_end_on','last_posted_on','voided_on','last_modified_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None    
    return row

def check_row(statementID, soup):
    row = {
        'statement_id':statementID,
        'amount':float(soup.find('amount').text) if soup.find('amount') else None
    }
    for s in ('check_number','payable_to','issued_by'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    for t in ('check_date','deposit_date'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None
    return row

def statement_entry_row(statementID, statementEntryID, soup):
    row = {
        'statement_id':statementID,
        'statement_entry_id':statementEntryID,
        'num_of_lives':int(soup.find('numOfLIves').text) if soup.find('numOfLIves') else None,
        'apply_to_date':dt.datetime.strptime(soup.find('applyToDate').text,WSTSFMT) if soup.find('applyToDate') else None
    }
    for b in ('posted','override'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for i in ('product_id','activity_log_record_id','statement_split_id'):
        tag = cmn.bp_col_to_tag(i).replace('Id','ID')
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for f in ('revenue_amount','premium_amount'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    for s in ('split_column_type','sagitta_transaction_code'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    try:
        lastPeriod = mjdb.bp_accounting_last_period('statement') if mjdb.bp_accounting_last_period('statement') else dt.date(2021,8,1)
    except Exception as e:
        lf.error(f"unable to get most recent accounting month:\n{e}")
    else:
        while lastPeriod < (dt.datetime.today().replace(day=1)-dt.timedelta(days=1)).replace(day=1,hour=0,minute=0,second=0,microsecond=0).date():
            statements = []
            checks = []
            statementEntries = []
            try:
                accountingMonth = f"{lastPeriod+rd.relativedelta(months=1)}T00:00:00"
                statementSummaryResp = bpws.find_statements(statementStatus='Closed',accountingMonthDateAfter=accountingMonth, accountingMonthDateBefore=accountingMonth)
                statementSummaryXml = bs(statementSummaryResp.content,'xml')
                if statementSummaryResp.ok==False:
                    raise ValueError(f"status_code: {statementSummaryResp.status_code}, faultCode: {statementSummaryXml.find('faultcode').text}, faultString: {statementSummaryXml.find('faultstring').text}")
            except Exception as e:
                lf.error(f"unable to parse findStatements for {accountingMonth}\n{e}")
            else:
                try:
                    for ss in statementSummaryXml.find_all('summaries'):
                        statementID = int(ss.find('statementID').text)
                        try:
                            statementResp = bpws.get_statement(str(statementID))
                            statementXml = bs(statementResp.content,'xml')
                            if statementResp.ok==False:
                                raise ValueError(f"status_code: {statementResp.status_code}, faultCode: {statementXml.find('faultcode').text}, faultString: {statementXml.find('faultstring').text}")
                            else:
                                try:
                                    [statements.append(statement_row(statementID,s)) for s in statementXml.find_all('statement')]
                                except Exception as e:
                                    lf.error(f"unable to parse statement_row for statementID {statementID}:\n{e}")
                                try:
                                    [checks.append(check_row(statementID,c)) for c in statementXml.find_all('check')]
                                except Exception as e:
                                    lf.error(f"unable to parse check_row for statementID {statementID}:\n{e}")
                                try:
                                    [statementEntries.append(statement_entry_row(statementID,int(se.find('statementEntryID').text),se)) for se in statementXml.find_all('statementEntries')]
                                except Exception as e:
                                    lf.error(f"unable to parse statement_entry_row for statementID {statementID}:\n{e}")
                        except Exception as e:
                            lf.error(f"unable to parse getStatement for statementID {statementID}\n{e}")
                except Exception as e:
                    lf.error("unable to parse Statement Summarys:\n{e}")
            stages = {
                'statement':statements if statements else None,
                'check':checks if checks else None,
                'statement_entry':statementEntries if statementEntries else None
            }
            for s in stages:
                if stages[s]:
                    try:
                        rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        lf.error(f"unable to stage records for {s}:\n{e}")
                    else:
                        lf.info(f"{rcs} record(s) staged for {s}, {accountingMonth}")
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
                lastPeriod = mjdb.bp_accounting_last_period('statement')
            except Exception as e:
                lf.error(f"unable to update lastPeriod:\n{e}")
    

if __name__ == '__main__':
    main()