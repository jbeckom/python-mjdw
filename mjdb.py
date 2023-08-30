import config as config
from psycopg2 import pool
import datetime as dt

config_file = R'config.ini'

HOST = config.config(config_file,'pgdb')['host']
DATABASE = config.config(config_file,'pgdb')['database']
USER = config.config(config_file,'pgdb')['user']
PASSWORD = config.config(config_file,'pgdb')['password']

scp = pool.SimpleConnectionPool(1,25,host=HOST,database=DATABASE,user=USER,password=PASSWORD)

def sg_file_upsert(sagfile, sagitem, xml):
    sql = f"SELECT sagitta.sp_{sagfile.lower().replace('.','_')}_upsert(%s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(sagitem, xml))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def sg_file_group_action(action, file, group, params):
    sql = f"SELECT sagitta.sp_{file.replace('.','_').lower()}_{group.lower()}_group_{action}"
    if action == 'delete': sql += "(%s);"
    elif action == 'insert': sql += "(%s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)                
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_file_import_cfgs():
    sql = f"SELECT * FROM config.vw_bp_file_imports;"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
    finally: scp.putconn(conn)

def file_upsert(schema, file):
    sql = f"SELECT {schema}.sp_{file}_upsert();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
        conn.commit()
    finally: scp.putconn(conn)

def sg_last_entry(file):
    sql = f"SELECT last_mod_date, last_mod_time FROM sagitta.tfn_{file.replace('.','_').lower()}_last_entry();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()[0]
    finally: scp.putconn(conn)

def source_entity_deltas (source, sourceEntity, lastEntry):
    sql = f"SELECT {source.lower()}.tfn_{sourceEntity.lower()}_deltas(%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(lastEntry,))
                return cur.fetchall()
    finally: scp.putconn(conn)

def dest_entity_upsert(destination, source, destEntity, sourceKey):
    sql = f"SELECT {destination.lower()}.sp_{source.lower()}_{destEntity.lower()}_upsert(%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(sourceKey,))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def dest_entity_attribute_upsert(destination, source, destEntity, destAttribute, sourceKey):
    sql = f"SELECT {destination.lower()}.sp_{source.lower()}_{destEntity.lower()}_{destAttribute.lower()}_upsert(%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(sourceKey,))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def dest_entity_attribute_link(destination, destEntity, attribute, source, sourceKey, **kwargs):
    if 'attSrcKey' in kwargs:
        sql = f"SELECT {destination.lower()}.sp_{destEntity.lower()}_{attribute.lower()}_link(%s,%s,%s);"
        parms = (source, str(sourceKey), kwargs['attSrcKey'])
    else:
        sql = f"SELECT {destination.lower()}.sp_{destEntity.lower()}_{attribute.lower()}_link(%s,%s);"
        parms = (source, str(sourceKey))
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,parms)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def dest_entity_attribute_delete (destination, destEntity, attribute, source, sourceKey):
    sql = f"SELECT {destination.lower()}.sp_{source.lower()}_{destEntity.lower()}_{attribute.lower()}_delete(%s)"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute (sql, (sourceKey,))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def sg_entity_attribute_group_lis (entity, attribute, entityKey):
    sql = f"SELECT sagitta.fn_{entity.lower()}_{attribute.lower()}_group_lis (%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(entityKey,))
                return cur.fetchone()[0]
    finally: scp.putconn(conn)

def bp_custom_section_upsert(ca, csLabel, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_section_upsert(%s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(ca, csLabel, xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_custom_fields_upsert(customSectionId, customFieldId, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_fields_upsert(%s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(customSectionId, customFieldId, xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_custom_field_option_value_upsert(cfovId, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_field_option_value_upsert(%s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(cfovId, xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_custom_field_dependent_fields_upsert(customFieldID, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_field_dependent_fields_upsert(%s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(customFieldID, xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_custom_field_dependent_trigger_upsert (customFieldID, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_field_dependent_trigger_upsert(%s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(customFieldID, xmldata))
                rc = cur.fetchone()[0]
        conn.commit() 
        return rc 
    finally: scp.putconn(conn)

def bp_custom_section_account_customization_categories_link(csId, actId):
    sql = f"SELECT benefitpoint.sp_custom_section_account_customization_categories_link(%s,%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(csId, actId))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc 
    finally: scp.putconn(conn)

def bp_custom_field_custom_option_values_link(cfId, cfovId):
    sql = f"SELECT benefitpoint.sp_custom_field_option_values_link(%s,%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(cfId, cfovId))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc 
    finally: scp.putconn(conn)

def bp_custom_field_dependent_fields_link(cfId, dfId):
    sql = f"SELECT benefitpoint.sp_custom_field_dependent_fields_link(%s,%s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(cfId, dfId))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc 
    finally: scp.putconn(conn)

def bp_last_modified (entity):
    sql = f"SELECT benefitpoint.fn_{entity}_last_modified();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0]
    finally: scp.putconn(conn)

def bp_address_upsert(addressSource, sourceType, sourceKey, xmldata):
    sql = f"SELECT benefitpoint.sp_address_upsert(%s, %s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(addressSource, sourceType, int(sourceKey), xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_custom_field_value_upsert(cfvSource, sourceKey, xmldata):
    sql = f"SELECT benefitpoint.sp_custom_field_value_upsert(%s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,(cfvSource, int(sourceKey), xmldata))
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_account_entity_upsert(entity, accountId, xmldata, **kwargs):
    if entity == 'address':
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s, %s, %s);"
        params = (kwargs['addressSource'], kwargs['sourceType'], accountId, xmldata)
    elif entity in ['account_class', 'account_division', 'account_location', 'carrier_appointment', 'license', 'location', 'account_team_member']:
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s, %s);"
        params = (accountId, int(kwargs['entityId']), xmldata)
    elif entity in ['person_info', 'phone']:
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s, %s, %s);"
        params = (kwargs['source'], kwargs['sourceType'], accountId, xmldata)
    elif entity == 'brokerage_account_info':
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s, %s);"
        params = (accountId, kwargs['accountType'], xmldata)
    elif entity in ['custom_field_option_value', 'office']:
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s, %s, %s);"
        params = (kwargs['source'], accountId, int(kwargs['entityId']), xmldata)
    else:
        sql = f"SELECT benefitpoint.sp_{entity}_upsert(%s, %s);"
        params = (accountId, xmldata)
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_account_relationship_delete(entity, accountId):
    sql = f"SELECT benefitpoint.sp_{entity}_delete(%s);"
    params = (int(accountId),)
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_account_realtionship_insert(entity, accountId, relationshipId):
    sql = f"SELECT benefitpoint.sp_{entity}_insert(%s, %s);"
    params = (int(accountId), int(relationshipId))
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_account_ids():
    sql = "SELECT account_id FROM benefitpoint.vw_account_ids;"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [r[0] for r in cur.fetchall()]
    finally: scp.putconn(conn)

def bp_entity_upsert(entity, entityId, xmldata, **kwargs):
    sql = f"SELECT benefitpoint.sp_{entity}_upsert"
    if entity == 'contact':
        sql += "(%s, %s, %s, %s);"
        params = (kwargs['source'], kwargs['sourceKey'], entityId, xmldata)
    elif entity in ('address','phone'):
        sql += "(%s, %s, %s, %s);"
        params = (kwargs['source'], kwargs['type'], entityId, xmldata)
    else:
        sql += "(%s, %s);"
        params = (entityId, xmldata)
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def bp_entity_action (entity, action, params, relationship=None):
    sql = f"SELECT benefitpoint.sp_{entity}"
    sql += f"_{relationship}_{action}" if relationship is not None else f"_{action}"
    if action == 'delete':
        sql += "(%s, %s, %s);" if entity == 'phone' else "(%s, %s);"
    elif action == 'insert':
        sql += "(%s, %s, %s, %s)" if entity == 'phone' else "(%s, %s, %s);"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute (sql, params)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally: scp.putconn(conn)

def upsert_stage(schema, table, action):
    sql = f"SELECT {schema}.sp_{table}_{action}();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally:
        scp.putconn(conn)        

def drop_table(scehma, table):
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {scehma}.{table};")
        conn.commit()
    finally:
        scp.putconn(conn)

def entity_last_update(schema, entity, params=None):
    pRef = ('%s,'*len(params)).rstrip(',') if params else ''
    sql = f"SELECT {schema}.fn_{entity}_last_update({pRef});"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(sql,params)
                else:
                    cur.execute(sql) 
                lu = cur.fetchone()[0]
                return lu if lu else dt.datetime(1900,1,1,0,0,0)
    finally:
        scp.putconn(conn)

def bp_accounting_last_period(entity):
    sql = f"SELECT benefitpoint.fn_{entity}_last_period();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0]
    finally:
        scp.putconn(conn)

def sg_accounting_last_period(entity):
    sql = f"SELECT sagitta.fn_{entity}_last_period();"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchone()[0]
    finally:
        scp.putconn(conn)

def function_execute(schema, function, params=None):
    sql = f"SELECT {schema}.{function}();"
    if params is not None:
        placeHolder = ''
        for _ in params:
            placeHolder += '%s,'
        sql = sql.replace('()',f"({placeHolder.rstrip(',')})")
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,params) if params is not None else cur.execute(sql)
                rc = cur.fetchone()[0]
        conn.commit()
        return rc
    finally:
        scp.putconn(conn)

def get_table(schema,table,**kwargs):
    sql = f"SELECT * FROM {schema}.{table};"
    sql = sql.replace('*',', '.join(kwargs['cols'])) if 'cols' in kwargs else sql
    sql = sql.replace(';',f" WHERE {kwargs['clause']};") if 'clause' in kwargs else sql
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
    finally:
        scp.putconn(conn)

def delete_table_record(schema,table,clause):
    sql = f"DELETE FROM {schema}.{table} WHERE {clause};"
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rc = cur.rowcount
        conn.commit()
        return rc
    finally:
        scp.putconn(conn)

def bp_statement_entry_per_accounting_month(accountingMonth):
    try:
        with scp.getconn() as conn:
            with conn.cursor ()as cur:
                cur.execute(f"SELECT statement_id, product_id FROM benefitpoint.tfn_statement_entry_per_accounting_month('{accountingMonth}');")
                return cur.fetchall()
    finally:
        scp.putconn(conn)

def get_tfn(schema,function,**kwargs):
    params = ','.join(['%s' for _ in range(len(kwargs['params']))])
    try:
        with scp.getconn() as conn:
            with conn.cursor() as cur:
                sql = f"SELECT {schema}.{function}({params});"
                cur.execute(sql,kwargs['params'])
                return cur.fetchall()
    finally:
        scp.putconn(conn)

### DEBUG ONLY ###
if __name__ == '__main__':
    pass