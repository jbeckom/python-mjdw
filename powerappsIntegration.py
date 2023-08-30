import mjdb
import common as cmn
import datetime as dt

configs = [
    {
         'source':'sagitta'
        ,'sourceEntity':'clients'
        ,'destination':'powerapps'
        ,'destEntity':'account'
        ,'attributes':['address', 'email', 'fax', 'phone']
    },
    {
         'source':'sagitta'
        ,'sourceEntity':'contacts'
        ,'destination':'powerapps'
        ,'destEntity':'contact'
        ,'attributes':['address', 'email', 'fax', 'phone']
    }
]

def main():
    for cfg in configs:
        lf = cmn.log_filer('powerappsIntegration', cfg['destEntity'])
        lastEntry = mjdb.dest_entity_last_update(cfg['destination'], cfg['destEntity'], cfg['source'])
        lastEntry = lastEntry if lastEntry is not None else dt.datetime.strptime('1900-01-01', '%Y-%m-%d')
        lisList = None

        for each in mjdb.source_entity_deltas(cfg['source'], cfg['sourceEntity'], lastEntry):
            # insert/update entity record
            try:
                rc = mjdb.dest_entity_upsert(cfg['destination'], cfg['source'], cfg['destEntity'], each[0])
            except Exception as e:
                lf.error(f"mjdb.dest_entity_upsert({cfg['destination']}, {cfg['source']}, {cfg['destEntity']}, {each[0]})\n{e}")
            else:
                if rc > 0:
                    lf.info(f"mjdb.dest_entity_upsert({cfg['destination']}, {cfg['source']}, {cfg['destEntity']}, {each[0]}) successfully upserted {rc} row(s)")
                
                    #insert update entity attribute record(s) -- only when an entity was affected
                    for att in cfg['attributes']:
                        #must delete contact attributes for sagitta clients, to accomodate it's entity relationship data structure
                        if cfg['source'] == 'sagitta' and cfg['sourceEntity'] == 'contacts':
                            try:
                                rc = mjdb.dest_entity_attribute_delete(cfg['destination'], cfg['destEntity'], att, cfg['source'], each[0])
                            except Exception as e:
                                lf.error(f"mjdb.dest_entity_attribute_delete({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]})\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.dest_entity_attribute_delete({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]}) successfully deleted {rc} record(s)")
                            
                            try:
                                lisList = mjdb.sg_entity_attribute_group_lis (cfg['sourceEntity'], att, each[0])
                            except Exception as e:
                                lf.error(f"mjdb.sg_entity_attribute_group_lis ({cfg['sourceEntity']}, {att}, {each[0]})\n{e}")

                        try:
                            rc = mjdb.dest_entity_attribute_upsert(cfg['destination'], cfg['source'], cfg['destEntity'], att, each[0])
                        except Exception as e:
                            lf.error(f"mjdb.dest_entity_attribute_upsert({cfg['destination']}, {cfg['source']}, {cfg['destEntity']}, {att}, {each[0]})\n{e}")
                        else:
                            if rc > 0:
                                lf.info(f"mjdb.dest_entity_attribute_upsert({cfg['destination']}, {cfg['source']}, {cfg['destEntity']}, {att}, {each[0]}) successfully upserted {rc} row(s)")

                        #link attribute record(s) to entity record -- only when an entity attribute was affected
                        if lisList is not None:
                            for lis in lisList.split(','):
                                attSrcKey = f"{each[0]}-{lis}"
                                try:
                                    rc = mjdb.dest_entity_attribute_link(cfg['destination'], cfg['destEntity'], att, cfg['source'], each[0], attSrcKey=attSrcKey)
                                except Exception as e:
                                    lf.error(f"mjdb.dest_entity_attribute_link({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]})\n{e}")
                                else:
                                    if rc > 0:
                                        lf.info(f"mjdb.dest_entity_attribute_link({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]}) successfully upserted {rc} row(s)")
                        elif not (cfg['source'] == 'sagitta' and cfg['sourceEntity'] == 'contacts'):
                            try:
                                rc = mjdb.dest_entity_attribute_link(cfg['destination'], cfg['destEntity'], att, cfg['source'], each[0])
                            except Exception as e:
                                lf.error(f"mjdb.dest_entity_attribute_link({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]})\n{e}")
                            else:
                                if rc > 0:
                                    lf.info(f"mjdb.dest_entity_attribute_link({cfg['destination']}, {cfg['destEntity']}, {att}, {cfg['source']}, {each[0]}) successfully upserted {rc} row(s)")

if __name__ == '__main__':
    main()