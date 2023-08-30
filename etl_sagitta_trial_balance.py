import mjdb
import common as cmn
import config
import pandas as pd
import datetime as dt
from calendar import monthrange
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
SCHEMA = 'sagitta'
FILE = 'trial_balance'
LOGDIR = 'etl_sagitta'
LF = cmn.log_filer(LOGDIR,FILE)

def string_to_number(x):
    x = x.rjust(3,'0') if '-' not in x else '-' + x.strip('-').rjust(3,'0')
    return round(float(x[:-2] + '.' + x[-2:]),2)

def main():
    dayOne = dt.date(1967,12,31)
    today = dt.date.today()
    # build "template" dataframe for Chart of Accounts -- alleviates having to build this with every iteration
    try:
        coa = cmn.raw_entity_df('chart_of_accounts_master',ENGINE,SCHEMA,cols=['sagitem','description','ledger_type_cd']).rename({'sagitem':'ledger'},axis=1)
    except Exception as e:
        LF.error(f"unable to fetch chart of accounts master:\n{e}")
    else:
        try:
            coa = coa.merge(cmn.raw_entity_df('ledger_type_codes',ENGINE,SCHEMA,cols=['sagitem','description']).rename({'sagitem':'ledger_type_cd','description':'ledger_type'},axis=1),how='left',on='ledger_type_cd').drop(['ledger_type_cd'],axis=1)
        except Exception as e:
            LF.error(f"unable to merge Chart Of Accounts Master and Ledger Type Codes:\n{e}")
        else:
            try:
                currentPeriod = mjdb.function_execute(SCHEMA,'fn_trial_balance_current_period')
            except Exception as e:
                LF.error(f"unable to fetch current period:\n{e}")
            else:
                # get most recent period for Trial Balance
                # re-calculate prior month to ensure unposted transactions, after EOM, are accounted for
                # starting balance for all accounts are as of 8/31/2020, default start is 9/30/2020
                currentPeriodDate = (currentPeriod - relativedelta(months=1)) if currentPeriod and currentPeriod >= dt.date(2019,10,31) else dt.date(2019,9,30) 
                while currentPeriodDate <= dt.date(today.year,today.month,monthrange(today.year,today.month)[1]):    # last day of current period
                    currentPeriod = (currentPeriodDate - dt.date(1967,12,31)).days
                    lastPeriodDate = (currentPeriodDate - relativedelta(months=1))
                    lastPeriodDate = dt.date(lastPeriodDate.year,lastPeriodDate.month,monthrange(lastPeriodDate.year,lastPeriodDate.month)[1])   # ensure period date is last day of month
                    # prior period is current period month from last year (???)
                    priorPeriodDate = dt.date(currentPeriodDate.year-1, currentPeriodDate.month, monthrange(currentPeriodDate.year-1,currentPeriodDate.month)[1])
                    # starting balance for current period is ending balance from previous period
                    try:
                        currentTB = pd.read_sql_query(f"SELECT ledger, end_balance AS start_bal FROM sagitta.trial_balance WHERE period = '{lastPeriodDate}'::date", ENGINE)
                    except Exception as e:
                        LF.error(f"unable to fetch starting balance for {currentPeriodDate}:\n{e}")
                    else:
                        try:
                            priorTB = pd.read_sql_query(f"SELECT ledger, start_bal AS prior_beg_bal, this_period AS prior_period, end_balance AS prior_end_bal FROM sagitta.trial_balance WHERE period = '{priorPeriodDate}'::date", ENGINE)
                            currentTB = currentTB.merge(priorTB,how='left',on='ledger')
                        except Exception as e:
                            LF.error(f"unable to fetch prior period data for {currentPeriodDate}:\n{e}")
                        else:
                            try:
                                utm = pd.read_sql_query(f"SELECT gl_acct_no AS ledger, debit_amount, credit_amount FROM sagitta.utm WHERE period_end_date = {currentPeriod}", ENGINE)
                            except Exception as e:
                                LF.error(f"unable to fetch UTM for period {currentPeriodDate}:\n{e}")
                            else:
                                if not utm.empty:
                                    for x in ('debit_amount','credit_amount'):
                                        utm[x] = utm[x].apply(string_to_number)
                                    utm = utm.groupby('ledger')[['credit_amount','debit_amount']].sum()
                                    utm['this_period'] = utm[['credit_amount','debit_amount']].sum(axis=1).round(2)
                                    currentTB = currentTB.merge(coa,how='right',on='ledger')
                                    currentTB = currentTB.merge(utm.reset_index()[['ledger','this_period']],how='left',on='ledger').fillna(0)   # if there are no UTM records for a ledger in a period, this_period will be NULL/NaN, resulting in bad calculations... defaulting to 0 should account for this
                                    currentTB['period'] = pd.to_datetime(currentPeriodDate)
                                    currentTB['end_balance'] = currentTB.start_bal + currentTB.this_period
                                    for x in ('start_bal','this_period','end_balance','prior_beg_bal','prior_period','prior_end_bal'):
                                        currentTB[x] = pd.to_numeric(currentTB[x]).round(2).fillna(0)
                                    cols = ['period','ledger','description','ledger_type','start_bal','this_period','end_balance','prior_beg_bal','prior_period','prior_end_bal']
                                    try:
                                        rcs = currentTB[cols].to_sql('stg_trial_balance',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                                    except Exception as e:
                                        LF.error(f"unable to stage records for {currentPeriodDate}:\n{e}")
                                    else:
                                        LF.info(f"{rcs} record(s) staged for {currentPeriodDate}")
                                        if rcs > 0:
                                            try:
                                                rcu = mjdb.upsert_stage(SCHEMA,FILE,'upsert')
                                            except Exception as e:
                                                LF.error(f"{rcu} record(s) affected for {currentPeriodDate}")
                                            else:
                                                LF.info(f"{rcu} record(s) affected for {currentPeriodDate}")
                                    finally:
                                        mjdb.drop_table(SCHEMA,'stg_trial_balance')
                    # advance period, ensure last day of month is used
                    nextPeriodDate = (currentPeriodDate + relativedelta(months=1))
                    currentPeriodDate = dt.date(nextPeriodDate.year,nextPeriodDate.month,monthrange(nextPeriodDate.year,nextPeriodDate.month)[1])

if __name__ == '__main__':
    main()