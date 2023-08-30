
import os
import mjdb
import config
import pandas as pd
import common as cmn
from sqlalchemy import create_engine

DIRECTORY = "C:\PETL\Benefitpoint"
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

cfgs = [
    {
        'file':'bp_splits.csv',
        'target':'splits',
        'targetSchema':'benefitpoint',
        'targetCols':'office,department,account,account_id,acct_primary_sales_lead,acct_primary_service_lead,billing_carrier,plan_type,plan_name,plan_office,plan_department,plan_id,policy_group_nbr,plan_eff_date,plan_renewal_date,split_eff_date,payee_name,commission_pct,commission_split_type,override_pct,override_split_type,bonus_pct,bonus_split_type,bob_pct'
    }
]

def main():
    for cfg in cfgs:
        if os.path.exists(os.path.join(DIRECTORY,cfg['file'])):
            df = cmn.csv_dataframe(os.path.join(DIRECTORY,cfg['file']), None, targetCols=cfg['targetCols'].split(','))
            df.to_sql(cfg['target'], ENGINE, cfg['targetSchema'], 'replace', False)
            cmn.move_file(DIRECTORY, cfg['file'], 'archive')

if __name__ == '__main__':
    main()