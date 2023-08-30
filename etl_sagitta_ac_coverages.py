import os
import mjdb
import sgws
import config
import common as cmn
import pandas as pd
import datetime as dt
import sgHelpers as hlp
from sqlalchemy import create_engine

ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
LOGDIR = 'etl_sagitta'

lf = cmn.log_filer(LOGDIR,'ac_coverages')

def ac_coverages_row(sagitem,soup):
    ints = ('audit_entry_dt','audit_time')
    texts = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_agency_id','state_prov_cd','named_customer_name','reporting','non_reporting','accts_rec_dep_premium_amt','accts_rec_min_premium_amt','accts_rec_reporting_period','accts_rec_prem_adjustment_period_desc','accts_rec_first_rate','accts_re_second_rate','accts_rec_rate_premium_amt','not_at_your_premises','all_covered_prop_all_location','accts_rec_in_transit_limit','accts_rec_collapse','accts_rec_removal','libraries_endorsment_applies','val_papers_lmt_away_from_premises','val_papers_blanket','val_papers_blanket_amt','val_papers_specified','val_papers_specified_amt','val_papers_collapse','val_papers_occurence_ded_amt','val_papers_removal','val_papers_removal_limit','off_dt','accts_rec_reporting_period_cd')
    row = {'sagitem':sagitem}
    for i in ints:
        tag = ''.join([x.capitalize() for x in i.split('_')])
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def arcli_row(sagitem, lis, soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('accts_rec_location_id','accts_rec_sub','accts_rec_cov_sub','accts_rec_your_premises_limit'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def vpcli_row(sagitem, lis, soup):
    row = {'sagitem':sagitem,'lis':lis}
    for t in ('val_papers_location_id','val_papers_sub','val_papers_your_premises_amt'):
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    acCoverages = []
    arcliList = []
    vpcliList = []
    try:
        lastEntry = mjdb.sg_last_entry('ac_coverages')
    except Exception as e:
        lf.error(f"mjdb.sg_last_entry('ac_coverages')\n{e}")
    else:
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
        try:
            batchesStatement = f"SELECT AC.COVERAGES *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT AC.COVERAGES *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            acCoverages.append(ac_coverages_row(sagitem,item))
                        except Exception as e:
                            lf.error(f"acCoverages.append(ac_coverages_row({sagitem},<<item>>))\n{e}")
                        for arcli in item.find_all('AcctsRecCovLimitInfo'):
                            try:
                                if arcli.get('lis'):
                                    lis = int(arcli.get('lis'))
                                    arcliList.append(arcli_row(sagitem,lis,arcli))
                            except Exception as e:
                                lf.error(f"arcliList.append(arcli_row({sagitem},{lis},<<arcli>>))\n{e}")
                        for vpcli in item.find_all('ValPapersCovLimitInfo'):
                            try:
                                if vpcli.get('lis'):
                                    lis = int(vpcli.get('lis'))
                                    vpcliList.append(vpcli_row(sagitem,lis,vpcli))
                            except Exception as e:
                                lf.error(f"vpcliList.append(vpcli_row({sagitem},{lis},<<vpcli>>))\n{e}")
            stages = {
                'ac_coverages':acCoverages if acCoverages else None,
                'ac_coverages_arcli':arcliList if arcliList else None,
                'ac_coverages_vpcli':vpcliList if vpcliList else None
            }
            for s in stages:
                if stages[s]:
                    try:
                        rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                    except Exception as e:
                        lf.error(f"unable to stage records for {s}\n{e}")
                    else:
                        if rcs > 0:
                            lf.info(f"{rcs} record(s) staged for {s}")
                            try:
                                rcu = mjdb.upsert_stage('sagitta', s, 'upsert')
                            except Exception as e:
                                lf.error(f"mjdb.upsert_stage('sagitta', {s})\n{e}")
                            else:
                                lf.info(f"mjdb.upsert_stage('sagitta', {s}) affected {rcu} record(s)")
                    finally:
                        mjdb.drop_table('sagitta',f'stg_{s}')

if __name__ == '__main__':
    main()
