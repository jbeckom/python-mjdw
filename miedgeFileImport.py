
import os
import mjdb
import config
import pandas as pd
import common as cmn
from sqlalchemy import create_engine

DIRECTORY = "C:\PETL\MiEdge"
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

cfgs = [
    {
        'file':'miedge_pc_',
        'target':'prospects_pc',
        'targetSchema':'miedge',
        'targetCols':'name,state,industry,web,social,employees,revenue_range,fidelity_bond,pc_self_funded,eb_self_funded,summary,ex_mod,ex_mod_changed_in_last_30_days,est_annual_prem,assigned_risk,workers_compensation_carrier,workers_compensation_renewal,bipd_carrier,bipd_renewal,bipd_excess_carrier,bipd_excess_renewal,cargo_carrier,cargo_renewal,bond_carrier,bond_renewal,business_travel_carrier,business_travel_renewal,pc_broker_normalized,pc_broker,benefits_broker,benefits_broker_revenue,searched_benefits_broker,searched_benefits_broker_revenue,peo_normalized,peo,accounting_firm_normalized,accounting_firm,actuary_name,actuary_firm_name,motor_carrier_operation,drivers,vehicles,mileage,checks,dot,osha,whd,address,city,county,zip,phone_number,miedge_smart_id,ein,activity_date'
    },
    {
        'file':'miedge_eb_',
        'target':'prospects_eb',
        'targetSchema':'miedge',
        'targetCols':'name,state,employees,eligible_employees,revenue_range,kpis,fidelity_bond,filing_status,premium,premium_per_employee,revenue,commission,fees,benefits_renewal,benefits_broker_normalized,benefits_broker,broker_share,broker_affiliation,broker_revenue,broker_revenue_pct,broker_commission,broker_commission_pct,broker_fees,broker_fees_pct,largest_carrier_normalized,carrier_premium,tpa,tpa_compensation,retirees,pc_broker_normalized,pc_broker,peo_normalized,peo,accounting_firm_normalized,accounting_firm,actuary_name,actuary_firm_name,advisor,service_provider,plan_assets,no_of_plans,corrective_distributions,participant_loans,web,social,primary_naics,naics_description,address,city,county,zip,phone_number,msid,ein,activity_date'
    }
]

def main():
    for cfg in cfgs:
        for file in os.listdir(DIRECTORY):
            if file.startswith(cfg['file']):
                df = cmn.csv_dataframe(os.path.join(DIRECTORY,file), None, targetCols=cfg['targetCols'].split(','))
                df.to_sql(f"stg_{cfg['target']}", ENGINE, cfg['targetSchema'], 'replace', False)
                mjdb.file_upsert(cfg['targetSchema'], cfg['target'])
                cmn.move_file(DIRECTORY, file, 'archive')

if __name__ == '__main__':
    main()