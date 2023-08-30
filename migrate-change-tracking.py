import os
import config
import xml.etree.ElementTree as ET
from psycopg2 import pool
# from sqlalchemy import create_engine

# ENGINE = create_engine(config.config('config.ini','postgres_alchemy')['url'])
HOST = config.config('config.ini','pgdb')['host']
DATABASE = config.config('config.ini','pgdb')['database']
USER = config.config('config.ini','pgdb')['user']
PASSWORD = config.config('config.ini','pgdb')['password']
ROOT = r"c:\psql\mjdw\schemas"
SCP = pool.SimpleConnectionPool(1,25,host=HOST,database=DATABASE,user=USER,password=PASSWORD)
CHANGELOGCONFIGS = {
    'xmlns':'http://www.liquibase.org/xml/ns/dbchangelog',
    'xmlns:ext':'http://www.liquibase.org/xml/ns/dbchangelog-ext',
    'xmlns:pro':'http://www.liquibase.org/xml/ns/pro',
    'xmlns:xsi':'http://www.w3.org/2001/XMLSchema-instance',
    'xsi:schemaLocation':'http://www.liquibase.org/xml/ns/dbchangelog-ext http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd http://www.liquibase.org/xml/ns/pro http://www.liquibase.org/xml/ns/pro/liquibase-pro-4.6.xsd http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-4.6.xsd'
}

def query_exec(query):
    try:
        with SCP.getconn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
    finally:
        SCP.putconn(conn)

def main():
    schemaQuery = (
        "SELECT s.oid,s.nspname "
        "FROM pg_catalog.pg_namespace AS s "
        "INNER JOIN pg_catalog.pg_roles AS r "
        "ON r.oid = s.nspowner "
        "WHERE r.rolname = 'mj_admin';"
    )
    for schemaId,schema in [x for x in query_exec(schemaQuery)]:
        fileCounter = 1
        ### GATHER METADATA FOR AUDIT TABLE(S) ###
        tableQuery = (
            "SELECT relname "
            "FROM pg_catalog.pg_class "
            f"WHERE relnamespace = {schemaId} AND relkind = 'r' AND relname LIKE 'audit_%';"
        )
        auditTables = [x[0] for x in query_exec(tableQuery)]
        ### DON'T CREATE FILE STRUCTURES IF THERE ARE NO TABLES TO MODIFY ###
        if auditTables:
            ### CREATE DIRECTORY FOR CHANGE FILE(S) ###
            csDir = os.path.join(ROOT,schema,'Change Scripts','mjdw-357')
            os.makedirs(csDir) if not os.path.exists(csDir) else None
            ### INSTANTIATE ELEMENTTREE (XML) FOR LIQUIBASE CHANGELOG SCRIPT -- FOR TABLE MODIFICATIONS AND AUDIT TABLE DROPS ###
            changelogDir = os.path.join(ROOT,schema,'_liquibase')
            os.makedirs(changelogDir) if not os.path.exists(changelogDir) else None
            tableDatabaseChangeLog = ET.Element('databaseChangeLog')
            dropDatabaseChangeLog = ET.Element('databaseChangeLog')
            for x in (tableDatabaseChangeLog,dropDatabaseChangeLog):
                for k,v in CHANGELOGCONFIGS.items():
                    x.set(k,v)    
            dropScript = ''    
        
        for auditTable in auditTables:
            ### BUILD SQL SCRIPT TO DROP AUDIT TABLES ###
            dropScript += f"DROP TABLE IF EXISTS {auditTable};\nGO{os.linesep}"
            
            table = auditTable.replace('audit_','')
            outFile = os.path.join(csDir,f"mjdw-357-{table}.sql")
            columnQuery = f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{auditTable}' and column_name LIKE 'old_%' ORDER BY ordinal_position"
            columns = [x[0] for x in query_exec(columnQuery)]
            pkQuery = (
                "SELECT a.attname "
                "FROM pg_catalog.pg_index AS i "
                "INNER JOIN pg_catalog.pg_attribute AS a "
                "ON a.attrelid = i.indrelid "
                "AND a.attnum = ANY(i.indkey) "
                f"WHERE i.indrelid = '{schema}.{table}'::regClass "
                "AND i.indisprimary; "
            )
            pk = query_exec(pkQuery)
            ### BUILD SQL FILE FOR TABLE MODIFICATIONS ###
            script = ''
            if pk:
                pkStr = ','.join([x for x in pk[0]])
                script += (
                    "/*** DROP EXISTING CONSTRAINT(S), ADD UNIQUE CONSTRAINT ON SOURCE IDENTIFIER, CREATE NEW/CONSISTENT ID FIELD AS PRIMARY KEY ***/\n"
                    f"ALTER TABLE {schema}.{table} \n"
                    f"\t DROP CONSTRAINT IF EXISTS {table}_pkey \n"
                    f"\t,ADD CONSTRAINT {table}_{pkStr.replace(',','_')}_uq \tUNIQUE ({pkStr})\n"
                    f"\t,ADD COLUMN id \t\t\tBIGINT \t\t\tNOT NULL \t\t\tGENERATED ALWAYS AS IDENTITY \t\t\tPRIMARY KEY; \n"
                    "GO\n"
                )
            else:
                script += (
                    "/*** CREATE NEW/CONSISTENT ID FIELD AS PRIMARY KEY ***/\n"
                    f"ALTER TABLE {schema}.{table} \n"
                    f"\tADD COLUMN id \t\t\tBIGINT \t\t\tNOT NULL \t\t\tGENERATED ALWAYS AS IDENTITY \t\t\tPRIMARY KEY; \n"   
                    "GO\n"
                )
            script += f"{os.linesep}/*** REMOVE LEGACY TRIGGER(S) ***/\n"
            for _ in ('d','i','u'):
                script += f"DROP TRIGGER IF EXISTS audit_{_} ON {schema}.{table}; \nGO\n"
            script += (
                f"{os.linesep}/*** DROP AND RE-CREATE CONSISTENT LOG CHANGE TRIGGER  ***/\n"
                f"DROP TRIGGER IF EXISTS log_change_iud ON {schema}.{table}; \nGO \n"
                f"CREATE TRIGGER log_change_iud AFTER INSERT OR UPDATE OR DELETE ON {schema}.{table} FOR EACH ROW EXECUTE FUNCTION audit.tr_{schema}_log_change(); \nGO\n "
            )
            script += (                
                f"{os.linesep}/*** MOVE AUDIT DATA FROM LEGACY TABLE(S) TO NEW AUDIT SCHEMA ***/\n"
                f"INSERT INTO audit.{schema}_log (audit_time, audit_user, audit_client, operation, table_name, table_key, new_record) \n"
                f"SELECT \t x.audit_timestamp \t\t\tAS audit_time \n"
                f"\t\t,x.audit_user \t\t\tAS audit_user \n"
                f"\t\t,'0.0.0.0'::inet \t\t\tAS audit_client \n"
                f"\t\t,CASE \n"
                f"\t\t\tWHEN x.audit_action = 'D' THEN 'DELETE' \n"
                f"\t\t\tWHEN x.audit_action = 'I' THEN 'INSERT' \n"
                f"\t\t\tWHEN x.audit_action = 'U' THEN 'UPDATE' \n"
                f"\t\tEND \t\t\tAS operation \n"
                f"\t\t,'{table}' \t\t\tAS table_name \n"
                f"\t\t,y.id \t\t\tAS table_key \n"
                "\t\t,( \n"
                "\t\t\tSELECT \trow_to_json(_) \n"
                "\t\t\tFROM ( \n"
                f"\t\t\t\tSELECT \t "
            )
            script += '\t\t\t\t\t\t,'.join(f"x.{x[0]}\n" for x in pk) if pk is not None else None
            for column in columns:
                script += f"\t\t\t\t\t\t,x.{column} \t\t\tAS {column.replace('old_','')} \n"
            script += (
                "\t\t\t) \tAS _ \n"
                "\t\t) \t\t\tAS new_record \n"
                f"FROM {schema}.{auditTable} \tAS x \n"
                f"\tINNER JOIN \t{schema}.{table} \tAS y \n"
                f"\t\tON "
            )
            script += "\n\t\t\t AND ".join(f"x.{x[0]} = y.{x[0]}" for x in pk) if pk is not None else None
            script += '; \nGO \n'
            with open(outFile,'w') as f:
                f.write(script)
            ### APPEND TABLE CHANGESET TO CHANGELOG SCRIPT ###
            tableChangeSet = ET.SubElement(tableDatabaseChangeLog,'changeSet')
            tableChangeSet.set('author','jbeckom')
            tableChangeSet.set('id',f'mjdw-357-{fileCounter}')
            tableSqlFile= ET.SubElement(tableChangeSet,'sqlFile')
            tableSqlFile.set('dbms','postgresql')
            tableSqlFile.set('encoding','UTF-8')
            tableSqlFile.set('endDelimiter','\\nGO')
            tableSqlFile.set('path',f'..\\Change Scripts\\mjdw-357\\mjdw-357-{table}.sql')
            tableSqlFile.set('relativeToChangelogFile','true')
            tableSqlFile.set('splitStatements','true')
            tableSqlFile.set('stripComments','true')
            ### INCREMENT FILE COUNTER ###
            fileCounter+=1

        ### DON'T CREATE CHANGE SCRIPTS IF THERE ARE NO CHANGE FILES ###
        if fileCounter > 1:
            ### WRITE DROP SCRIPT TO FILE ###
            with open(os.path.join(csDir,'mjdw-357-drop-audit-tables.sql'),'w') as drops:
                drops.write(dropScript)

            ### APPEND DROP CHANGESET TO DROP CHANGELOG ###
            dropChangeSet = ET.SubElement(dropDatabaseChangeLog,'changeSet')
            dropChangeSet.set('author','jbeckom')
            dropChangeSet.set('id',f'mjdw-357-drop-audit-tables-1')
            dropSqlFile = ET.SubElement(dropChangeSet,'sqlFile')
            dropSqlFile.set('dbms','postgresql')
            dropSqlFile.set('encoding','UTF-8')
            dropSqlFile.set('endDelimiter','\\nGO')
            dropSqlFile.set('path',f'..\\Change Scripts\\mjdw-357\\mjdw-357-drop-audit-tables.sql')
            dropSqlFile.set('relativeToChangelogFile','true')
            dropSqlFile.set('splitStatements','true')
            dropSqlFile.set('stripComments','true')

            ### WRITE TABLE CHANGE SCRIPT TO FILE ###
            tableET = ET.ElementTree(tableDatabaseChangeLog)
            with open(os.path.join(changelogDir,'mjdw-357.xml'),'wb') as cl:
                tableET.write(cl)

            ### WRITE DROP CHANGE SCRIPT TO FILE ###
            dropET = ET.ElementTree(dropDatabaseChangeLog)
            with open(os.path.join(changelogDir,'mjdw-357-drop-audit-tables.xml'),'wb') as d:
                dropET.write(d)            

if __name__ == '__main__':
    main()