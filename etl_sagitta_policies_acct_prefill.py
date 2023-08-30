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

lf = cmn.log_filer(LOGDIR,'policies_acct_prefill')

def policies_acct_prefill_row(sagitem, soup):
    ints = ('audit_entry_dt','audit_time')
    texts = ('audit_staff_cd','audit_cd','audit_history_record_number','audit_program','audit_effective_dt','audit_change_agency_id','policy_number','client_cd','net_commission_pct','create_producer_payable','filing_state')
    row = {'sagitem':sagitem}
    for i in ints:
        tag = ''.join([x.capitalize() for x in i.split('_')])
        row[i] = int(soup.find(tag).text) if soup.find(tag) else None
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def additional_transaction_info_row(sagitem, lis, soup):
    texts = ('transaction_cd','transaction_cov_cd','transaction_payee_cd','transaction_amt','transaction_prorated_repeated','transaction_admitted_yes_no_ind','transaction_percentage','transaction_basis','transaction_rounding','transaction_taxable','transaction_indicator')
    row = {'sagitem':sagitem,'lis':lis}
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def multiple_billto_prod_add_tran_info_row(sagitem, lis, soup):
    texts = ('billto','billto_percent','billto_amount','ins','payee','payee_percent','payee_amt','agency_pct','agency_amt')
    row = {'sagitem':sagitem,'lis':lis}
    for t in texts:
        tag = ''.join([x.capitalize() for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def multiple_producer_cd_info_row(sagitem, lis, soup):
    texts = ('producer_cd','producer_new_pct','ig_rel_ind','producer_renewal_pct')
    row = {'sagitem':sagitem,'lis':lis}
    for t in texts:
        tag = ''.join([x.capitalize().replace('Ig','IG') for x in t.split('_')])
        row[t] = soup.find(tag).text if soup.find(tag) else None
    return row

def main():
    policiesAcctPrefill = []
    additionalTransactionInfo = []
    multipleBilltoProdAddTranInfo = []
    multipleProducerCdInfo = []
    try:
        lastEntry = mjdb.sg_last_entry('policies_acct_prefill')
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
    except Exception as e:
        lf.error(f"unable to retrieve last entry data\n{e}")
    else:
        try:
            batchesStatement = f"SELECT POLICIES.ACCT.PREFILL *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate,'%m-%d-%Y')}"
            batchesResponse = sgws.post_ptr_access_statement(batchesStatement)
        except Exception as e:
            lf.error(f"sgws.post_ptr_access_statement({batchesStatement})\n{e}")
        else:
            for batch in hlp.parse_batch_items(batchesResponse):
                try:
                    batchStatement = f"SELECT POLICIES.ACCT.PREFILL *GET.BATCH* {batch}"
                    batchResponse = sgws.post_ptr_access_statement(batchStatement)
                except Exception as e:
                    lf.error(f"sgws.post_ptr_access_statement({batchStatement})\n{e}")
                else:
                    for item in batchResponse.find_all('Item'):
                        try:
                            sagitem = int(item.get('sagitem'))
                            policiesAcctPrefill.append(policies_acct_prefill_row(sagitem, item))
                        except Exception as e:
                            lf.error(f"policies_acct_prefill_row({sagitem}, <<item>>)\n{e}")
                        for ati in item.find_all('AdditionalTransactionInfo'):
                            try:
                                lis = int(ati.get('lis'))
                                additionalTransactionInfo.append(additional_transaction_info_row(sagitem, lis, ati))
                            except Exception as e:
                                lf.error(f"additional_transaction_info_row({sagitem}, {lis}, <<ati>>)\n{e}")
                        for mbpati in item.find_all('MultipleBilltoProdAddTranInfo'):
                            try:
                                lis = int(mbpati.get('lis'))
                                multipleBilltoProdAddTranInfo.append(multiple_billto_prod_add_tran_info_row(sagitem, lis, mbpati))
                            except Exception as e:
                                lf.error(f"multiple_billto_prod_add_tran_info_row({sagitem}, {lis}, <<mbpati>>)\n{e}")
                        for mpci in item.find_all('MultipleProducerCdInfo'):
                            try:
                                lis = int(mpci.get('lis'))
                                multipleProducerCdInfo.append(multiple_producer_cd_info_row(sagitem, lis, mpci))
                            except Exception as e:
                                lf.error(f"multiple_producer_cd_info_row(sagitem, lis, mpci)\n{e}")
        stages = {
            'policies_acct_prefill':policiesAcctPrefill if policiesAcctPrefill else None,
            'policies_acct_prefill_ati':additionalTransactionInfo if additionalTransactionInfo else None,
            'policies_acct_prefill_mbpati':multipleBilltoProdAddTranInfo if multipleBilltoProdAddTranInfo else None,
            'policies_acct_prefill_mpci':multipleProducerCdInfo if multipleProducerCdInfo else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).to_sql(f'stg_{s}',ENGINE,'sagitta','replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}")
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
                    mjdb.drop_table('sagitta', f'stg_{s}')

if __name__ == '__main__':
    main()