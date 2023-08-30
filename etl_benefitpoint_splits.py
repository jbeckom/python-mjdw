import mjdb
import bpws
import config
import common as cmn
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine

LOGDIR = 'etl_benefitpoint'
SCHEMA = 'benefitpoint'
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
WSTSFMT = '%Y-%m-%dT%H:%M:%S.%f%z'

lf = cmn.log_filer(LOGDIR, 'splits')

def split_row(splitID,soup):
    row = {
        'split_id':int(splitID),
        'notes':soup.find('notes').text if soup.find('notes') else None
    }
    # compile list of productIDs, sort by integer value, return comma delimited string (allows for deduplication down stream)
    if len(soup.find_all('productIDs')):
        productIDs = [int(x.text) for x in soup.find_all('productIDs')]
        productIDs.sort()
        row['product_ids'] = ', '.join([str(y) for y in productIDs])
    else:
        row['product_ids'] = None
    for t in ('effective_as_of','last_modified_on','created_on'):
        tag = cmn.bp_col_to_tag(t)
        row[t] = dt.datetime.strptime(soup.find(tag).text,WSTSFMT) if soup.find(tag) else None
    return row

def split_column_row(splitID, splitColumnID, soup):
    row = {'split_id':int(splitID),'split_column_id':int(splitColumnID)}
    for s in ('split_basis_type','split_column_type'):
        tag = cmn.bp_col_to_tag(s)
        row[s] = soup.find(tag).text if soup.find(tag) else None
    return row

def split_payee_row(splitID, payeeID, soup):
    row = {
        'split_id':int(splitID),
        'payee_id':int(payeeID),
        'payee_role_type':soup.find('payeeRoleType').text if soup.find('payeeRoleType') else None,
        'sort_order':int(soup.find('sortOrder').text) if soup.find('sortOrder') else None
    }
    for b in ('ignore_split_team','round_to'):
        tag = cmn.bp_col_to_tag(b)
        row[b] = cmn.bp_parse_bool(soup.find(tag).text) if soup.find(tag) else None
    for f in ('commission','override','bonus','bob'):
        tag = cmn.bp_col_to_tag(f)
        row[f] = float(soup.find(tag).text) if soup.find(tag) else None
    return row

def main():
    splits = []
    splitColumns = []
    splitPayees = []
    try:
        lastMod = mjdb.bp_last_modified('split') if mjdb.bp_last_modified('split')  else dt.datetime(1900,1,1,0,0)
    except Exception as e:
        lf.error(f"unable to get last modified date from EDW:\n{e}")
    else:
        if (dt.datetime.now() - lastMod).days > 30:
            try:
                products = mjdb.get_table(SCHEMA, 'product', cols=['product_id'])
            except Exception as e:
                raise ValueError(f"unable to retrieve product IDs:\n{e}")
            else:
                for product in products:
                    try:
                        fsResp = bpws.find_splits(product[0])
                        fsSoup = bs(fsResp.content,'xml')
                        if fsResp.ok == False:
                            raise ValueError(f"status_code: {fsResp.status_code}, faultCode: {fsSoup.find('faultcode').text}, faultString: {fsSoup.find('faultstring').text}")
                        else:
                            for splitSoup in fsSoup.find_all('splits'):
                                try:
                                    splitID = splitSoup.find('splitID').text
                                    splits.append(split_row(splitID,splitSoup))
                                except Exception as e:
                                    lf.error(f"unable to parse Split for Split {splitID}:\n{e}")
                                else:
                                    try:
                                        [splitColumns.append(split_column_row(splitID,sc.find('splitColumnID').text,sc)) for sc in splitSoup.find_all('splitColumns')]
                                    except Exception as e:
                                        lf.error(f"unable to parse SplitColumns for Split {splitID}:\n{e}")
                                    try:
                                        [splitPayees.append(split_payee_row(splitID,sp.find('payeeID').text,sp)) for sp in splitSoup.find_all('payees')]
                                    except Exception as e:
                                        lf.error(f"unable to parse SplitPayees for Split {splitID}:\n{e}")
                    except Exception as e:
                        lf.error(f"unable to parse findSplits for Product {product[0]}:\n{e}")
        else:
            try:
                fcResp = bpws.find_changes(sinceLastModifiedOn=lastMod,typesToInclude='Split')
                fcSoup = bs(fcResp.content,'xml')
                if fcResp.ok==False:
                    raise ValueError(f"status_code: {fcResp.status_code}, faultCode: {fcSoup.find('faultcode').text}, faultString: {fcSoup.find('faultstring').text}")
                else:
                    for mod in fcSoup.find_all('modifications'):
                        splitID = mod.find('entityID').text
                        try:
                            gsResp = bpws.get_split(splitID)
                            gsSoup = bs(gsResp.content,'xml')
                            if gsResp.ok==False:
                                raise ValueError(f"status_code: {gsResp.status_code}, faultCode: {gsSoup.find('faultcode').text}, faultString: {gsSoup.find('faultstring').text}")
                            else:
                                try:
                                    splits.append(split_row(splitID,gsSoup))
                                except Exception as e:
                                    lf.error(f"unable to parse split_row for {splitID}:\n{e}")
                                else:
                                    try:
                                        [splitColumns.append(split_column_row(splitID,sc.find('splitColumnID').text,sc)) for sc in gsSoup.find_all('splitColumns')]
                                    except Exception as e:
                                        lf.error(f"unable to parse split_column_row for Split {splitID}:\n{e}")
                                    try:
                                        [splitPayees.append(split_payee_row(splitID,sp.find('payeeID').text,sp)) for sp in gsSoup.find_all('payees')]
                                    except Exception as e:
                                        lf.error(f"unable to parse split_payee_row for Split {splitID}:\n{e}")
                        except Exception as e:
                            lf.error(f"unable to parse getSplit for {splitID}:\n{e}")
            except Exception as e:
                lf.error(f"unable to parse findChanges for Splits sinceLastModifiedOn {lastMod}:\n{e}")
        stages = {
            'split':splits if splits else None,
            'split_column':splitColumns if splitColumns else None,
            'split_payee':splitPayees if splitPayees else None
        }
        for s in stages:
            if stages[s]:
                try:
                    rcs = pd.DataFrame(stages[s]).drop_duplicates().to_sql(f'stg_{s}',ENGINE,SCHEMA,'replace',index=False,chunksize=10000,method='multi')
                except Exception as e:
                    lf.error(f"unable to stage records for {s}:\n{e}")
                else:
                    lf.info(f"{rcs} record(s) staged for {s}")
                    if rcs > 0:
                        try:
                            rcu = mjdb.upsert_stage(SCHEMA, s, 'upsert')
                        except Exception as e:
                            lf.error(f"unable to upsert from stage to {s}:\n{e}")
                        else:
                            lf.info(f"{rcu} record(s) affected for {s}")
                    else:
                        lf.info(f"no records to stage for {s}")
                finally:
                    mjdb.drop_table(SCHEMA, f'stg_{s}')

if __name__ == '__main__':
    main()