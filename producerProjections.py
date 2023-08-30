import config
import pandas as pd
import common as cmn
import datetime as dt
from sqlalchemy import create_engine

LF = cmn.log_filer('benefitpointFileImport','benefitpointFileImport')
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

def bp_splits():
    usecols = ['account_id','plan_id','policy_group_nbr','plan_type','payee_name','bob_pct','split_eff_date']
    splits = pd.read_sql_table('splits', ENGINE, 'benefitpoint', columns=usecols)
    # drop rows where split_eff_date is greater than today (runtime)
    splits = splits[pd.to_datetime(splits.split_eff_date).dt.date <= dt.date.today()]
    # drop rows where bob_pct equals 0
    splits = splits[splits.bob_pct !='0']
    # re-arrange stored name to display name
    splits.payee_name = splits.payee_name.str.split(',').str[1] + ' ' + splits.payee_name.str.split(',').str[0]
    # combine payee_name and bob_pct to single column
    splits['splits'] = splits.payee_name + ' (' + splits.bob_pct + ')'
    # remove payee_name and bob_pct columns
    splits.drop(['payee_name', 'bob_pct'],axis=1,inplace=True)
    # aggregate all splits (pipe del) and group by remaining columns
    splits = splits.groupby(['account_id', 'plan_id', 'policy_group_nbr', 'plan_type', 'split_eff_date']).agg(lambda splits:' | '.join(splits)).reset_index()
    # sort by split_eff_date and drop all rows except most recent/current
    splits['rank'] = splits.groupby(['account_id', 'plan_id', 'policy_group_nbr', 'plan_type'])['split_eff_date'].transform(lambda x: x.sort_values(ascending=False).rank(ascending=False, method='first')).astype(int)
    splits = splits[splits['rank'] == 1]

    return splits

def main():
    bpSplits = bp_splits()
    pass

if __name__ == '__main__':
    main()