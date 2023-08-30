import os
import mjdb
import config
import common as cmn
import pandas as pd
from functools import reduce
from sqlalchemy import create_engine

LF = cmn.log_filer('benefitpointFileImport','benefitpointFileImport')
BPROOT = os.path.join(config.config('config.ini','general')['root'],'Benefitpoint')
ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
TARGETSCHEMA = 'benefitpoint'

def main():
    try:
        cfgs = mjdb.bp_file_import_cfgs()
    except Exception as e:
        LF.error(f"mjdb.bp_file_import_cfgs()\n{e}")
    else:
        clients = []
        for cfg in cfgs:
            cfg = list(cfg)
            filePath = os.path.join(BPROOT,cfg[1])
            if os.path.exists(filePath):
                try:
                    usecols = cfg[2].split(' | ') if cfg[2].strip() != '' else None
                    df = cmn.csv_dataframe(filePath, usecols, targetCols=cfg[4].split(','))
                except Exception as e:
                    cmn.move_file(BPROOT, cfg[1], 'error')
                    LF.error(f"csv_dataframe({filePath}, <<cfg[2].split(' | ')>>, <<targetCols=cfg[4].split(',')>>)\n{e}")
                else:
                    if cfg[0] == 'clients':
                        cfg.append(df)
                        clients.append(cfg)
                    else:
                        try:
                            df.to_sql(cfg[3], ENGINE, TARGETSCHEMA, 'replace', False)
                        except Exception as e:
                            cmn.move_file(BPROOT, cfg[1], 'error')
                            LF.error(f"df.to_sql({cfg[3]}, <<engine>>, {TARGETSCHEMA}, 'replace', False)")
                        else:
                            try:
                                mjdb.file_upsert(TARGETSCHEMA, cfg[0])
                            except Exception as e:
                                cmn.move_file(BPROOT, cfg[1], 'error')
                                LF.error(f"mjdb.file_upsert({TARGETSCHEMA}, {cfg[0]})")
                            else:
                                try:
                                    cmn.move_file(BPROOT, cfg[1], 'archive')
                                except Exception as e:
                                    LF.error(f"cmn.move_file(<<BPROOT>>, {cfg[1]}, 'archive')\n{e}")
                                else:
                                    LF.info(f"cmn.move_file(<<BPROOT>>, {cfg[1]}, 'archive') successful.")
                if len(clients) == 3:
                    clientDataframes = []
                    targetCols = ''
                    for client in clients:
                        clientDataframes.append(client[6])
                        targetCols += f"{client[4]},"
                    # remove multiple occurences of account_id column and trailing separator
                    targetCols = targetCols[::-1].replace(',di_tnuocca','',2).replace(',','',1)[::-1]
                    try:
                        clientDf = cmn.merge_dataframes(clientDataframes, clients[0][5], 'outer', targetCols.split(','))
                    except Exception as e:
                        LF.error(f"merge_dataframes(<<clientDataframes>>, <<clients[0][5]>>, 'outer', <<targetCols.split(',')>>)\n{e}")
                    else:
                        try:
                            clientDf.to_sql(clients[0][3], ENGINE, TARGETSCHEMA, 'replace', False)
                        except Exception as e:
                            cmn.move_file(BPROOT, client[1], 'error')
                            LF.error(f"clientDf.to_sql({clients[0][3]}, <<engine>>, {TARGETSCHEMA}, 'replace', False)")
                        else:
                            try:
                                mjdb.file_upsert(TARGETSCHEMA, clients[0][0])
                            except Exception as e:
                                cmn.move_file(BPROOT, client[1], 'error')
                                LF.error(f"mjdb.file_upsert({TARGETSCHEMA}, {clients[0][0]})\n{e}")
                            else:
                                LF.info(f"mjdb.file_upsert({TARGETSCHEMA}, {clients[0][0]}) successful.")
                                for client in clients:
                                    try:
                                        cmn.move_file(BPROOT, client[1], 'archive')
                                    except Exception as e:
                                        LF.error(f"cmn.move_file({BPROOT}, {client[1]}, 'archive')\n{e}")
                                    else:
                                        LF.info(f"cmn.move_file({BPROOT}, {client[1]}, 'archive') successful")

if __name__ == '__main__':
    main()