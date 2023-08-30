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
FILE = 'policies_addlinfo'

lf = cmn.log_filer(LOGDIR,FILE)

def main():
    rows = []
    try:
        lastEntry = mjdb.sg_last_entry(FILE)
        lastEntryDate = (dt.date(1967,12,31) + dt.timedelta(days=lastEntry[0])) if lastEntry[0] else dt.date(1967,12,31)
        lastEntryTime = ((dt.datetime.min + dt.timedelta(seconds=lastEntry[1])).time()) if lastEntry[1] else dt.time(0,0,0)
    except Exception as e:
        lf.error(f"unable to obtain last entry:\n{e}")
    else:
        try:
            batches = sgws.post_ptr_access_statement(f"SELECT {FILE.replace('_','.').upper()} *CRITERIA.BATCH* WITH LAST.ENTRY.DATE GE {dt.datetime.strftime(lastEntryDate, '%m-%d-%Y')}")
        except Exception as e:
            lf.error(f"unable to obtain batches:\n{e}")
        else:
            pass

if __name__ == '__main__':
    main()