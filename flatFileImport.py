import os
import mjdb
import config
import common as cmn
from sqlalchemy import create_engine

ROOT = R"C:\PETL"
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])

cfgs = [
    {
        'fileName':'departments',
        'fileExt':'csv',
        'fileDir':'Sagitta\Departments',
        'sourceCols':'Division,Department #,Department Name,Active,Notes',
        'destination':'stg_departments',
        'destSchema':'sagitta',
        'destCols':'division,dept_cd,department,active,notes'

    }
]

def main():
    for cfg in cfgs:
        departments = cmn.csv_dataframe(os.path.join(ROOT, cfg['fileDir'], f"{cfg['fileName']}.{cfg['fileExt']}"), cfg['sourceCols'].split(','), targetCols=cfg['destCols'].split(','))
        departments.to_sql(cfg['destination'], ENGINE, cfg['destSchema'], 'replace', False)
        mjdb.file_upsert(cfg['destSchema'], cfg['fileName'])
        # add drop table to mjdb, remove from indivuidual upsert functions
        # move file
        # log
        pass

if __name__ == '__main__':
    main()