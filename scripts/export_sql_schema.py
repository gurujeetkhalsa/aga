"""Export SQL Server definitions using SELECTs against live system catalogs only.
No application table rows, passwords, credential secrets or sequence current values.
JSON companions preserve catalog detail alongside readable DDL, including unsupported
DDL features. SQL is evidence, not an automatically executable deployment script.
"""
import datetime,json,re
from pathlib import Path
import pyodbc

def q(s):return '['+str(s).replace(']',']]')+']'
def rows(c,sql):
    cur=c.execute(sql);names=[x[0] for x in cur.description]
    return [dict(zip(names,r)) for r in cur.fetchall()]
def dump(p,v):p.write_text(json.dumps(v,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')
def export_schema(connection_string,out):
    c=pyodbc.connect(connection_string,timeout=40,autocommit=False)
    db,permission=c.execute("SELECT DB_NAME(), HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION')").fetchone()
    if not permission:raise RuntimeError('VIEW DEFINITION required for complete export')
    if out.exists() and any(out.iterdir()):raise RuntimeError('Refusing to mix databases in an existing SQL export')
    out.mkdir(parents=True,exist_ok=True)
    objects=rows(c,"SELECT o.object_id, SCHEMA_NAME(o.schema_id) schema_name,o.name,o.type,o.type_desc,o.parent_object_id,o.is_ms_shipped FROM sys.objects o WHERE o.is_ms_shipped=0 ORDER BY schema_name,o.name")
    modules=rows(c,"SELECT object_id,definition,uses_ansi_nulls,uses_quoted_identifier,is_schema_bound,uses_database_collation,is_recompiled,null_on_null_input,execute_as_principal_id FROM sys.sql_modules")
    tables=rows(c,"SELECT * FROM sys.tables WHERE is_ms_shipped=0")
    columns=rows(c,"SELECT c.*,TYPE_NAME(c.user_type_id) type_name,SCHEMA_NAME(t.schema_id) type_schema,t.is_user_defined FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id JOIN sys.objects o ON o.object_id=c.object_id WHERE o.is_ms_shipped=0 ORDER BY c.object_id,c.column_id")
    catalogs={
      'defaults':"SELECT * FROM sys.default_constraints WHERE is_ms_shipped=0",
      'computed':"SELECT * FROM sys.computed_columns",
      'identity':"SELECT object_id,column_id,CONVERT(nvarchar(100),seed_value) seed_value,CONVERT(nvarchar(100),increment_value) increment_value,is_not_for_replication FROM sys.identity_columns",
      'checks':"SELECT * FROM sys.check_constraints WHERE is_ms_shipped=0",
      'keys':"SELECT * FROM sys.key_constraints WHERE is_ms_shipped=0",
      'foreign_keys':"SELECT * FROM sys.foreign_keys WHERE is_ms_shipped=0",
      'foreign_key_columns':"SELECT * FROM sys.foreign_key_columns ORDER BY constraint_object_id,constraint_column_id",
      'indexes':"SELECT * FROM sys.indexes WHERE object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0)",
      'index_columns':"SELECT * FROM sys.index_columns WHERE object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0) ORDER BY object_id,index_id,key_ordinal,index_column_id",
      'partitions':"SELECT object_id,index_id,partition_number,data_compression,data_compression_desc FROM sys.partitions WHERE object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0)",
      'triggers':"SELECT * FROM sys.triggers WHERE is_ms_shipped=0",
      'sequences':"SELECT object_id,SCHEMA_NAME(schema_id) schema_name,name,TYPE_NAME(user_type_id) type_name,CONVERT(nvarchar(100),start_value) start_value,CONVERT(nvarchar(100),increment) increment,CONVERT(nvarchar(100),minimum_value) minimum_value,CONVERT(nvarchar(100),maximum_value) maximum_value,is_cycling,is_cached,cache_size,precision,scale FROM sys.sequences WHERE is_ms_shipped=0",
      'synonyms':"SELECT object_id,SCHEMA_NAME(schema_id) schema_name,name,base_object_name FROM sys.synonyms",
      'types':"SELECT * FROM sys.types WHERE is_user_defined=1",
      'table_types':"SELECT * FROM sys.table_types WHERE is_user_defined=1",
      'parameters':"SELECT * FROM sys.parameters WHERE object_id IN (SELECT object_id FROM sys.objects WHERE is_ms_shipped=0)",
      'dependencies':"SELECT * FROM sys.sql_expression_dependencies",
      'schemas':"SELECT schema_id,name,USER_NAME(principal_id) owner_name FROM sys.schemas",
      'principals':"SELECT principal_id,name,type,type_desc,default_schema_name,owning_principal_id,is_fixed_role,authentication_type_desc FROM sys.database_principals WHERE principal_id>4 AND name NOT LIKE '##%'",
      'permissions':"SELECT p.class,p.class_desc,p.major_id,p.minor_id,p.permission_name,p.state_desc,USER_NAME(p.grantee_principal_id) grantee,USER_NAME(p.grantor_principal_id) grantor,OBJECT_SCHEMA_NAME(p.major_id) object_schema,OBJECT_NAME(p.major_id) object_name,COL_NAME(p.major_id,p.minor_id) column_name FROM sys.database_permissions p",
      'role_members':"SELECT USER_NAME(role_principal_id) role_name,USER_NAME(member_principal_id) member_name FROM sys.database_role_members",
      'partition_functions':"SELECT * FROM sys.partition_functions",
      'partition_parameters':"SELECT * FROM sys.partition_parameters",
      'partition_ranges':"SELECT function_id,boundary_id,parameter_id,CONVERT(nvarchar(max),value) value,CONVERT(nvarchar(128),SQL_VARIANT_PROPERTY(value,'BaseType')) base_type FROM sys.partition_range_values",
      'partition_schemes':"SELECT * FROM sys.partition_schemes",
      'data_spaces':"SELECT * FROM sys.data_spaces",
      'security_policies':"SELECT * FROM sys.security_policies",
      'security_predicates':"SELECT * FROM sys.security_predicates",
      'fulltext_indexes':"SELECT object_id,unique_index_id,fulltext_catalog_id,is_enabled,change_tracking_state_desc,stoplist_id,property_list_id,data_space_id FROM sys.fulltext_indexes",
      'fulltext_columns':"SELECT * FROM sys.fulltext_index_columns",
      'fulltext_catalogs':"SELECT fulltext_catalog_id,name,is_default,is_accent_sensitivity_on,USER_NAME(principal_id) owner_name FROM sys.fulltext_catalogs",
      'fulltext_stoplists':"SELECT stoplist_id,name,USER_NAME(principal_id) owner_name FROM sys.fulltext_stoplists",
      'fulltext_stopwords':"SELECT stoplist_id,stopword,language_id FROM sys.fulltext_stopwords",
      'xml_indexes':"SELECT * FROM sys.xml_indexes",
      'external_tables':"SELECT * FROM sys.external_tables",
    }
    cat={}
    for k,v in catalogs.items():
        try:cat[k]=rows(c,v)
        except Exception as e:raise RuntimeError('Catalog '+k+' failed: '+str(e)) from e
    byid={o['object_id']:o for o in objects};mod={m['object_id']:m for m in modules}
    colmap={(x['object_id'],x['column_id']):x for x in columns}
    def fullname(oid):
        o=byid[oid];return q(o['schema_name'])+'.'+q(o['name'])
    def typename(x):
        t=x['type_name']
        if x['is_user_defined']:return q(x['type_schema'])+'.'+q(t)
        if t in ('varchar','char','varbinary','binary','nvarchar','nchar'):
            length=x['max_length'];length='max' if length==-1 else length//(2 if t.startswith('n') else 1)
            return t+'('+str(length)+')'
        if t in ('decimal','numeric'):return f"{t}({x['precision']},{x['scale']})"
        if t in ('datetime2','datetimeoffset','time'):return f"{t}({x['scale']})"
        return t
    def idxcols(oid,idx,include=False):
        xs=[x for x in cat['index_columns'] if x['object_id']==oid and x['index_id']==idx and bool(x['is_included_column'])==include and (include or x['key_ordinal']>0)]
        return ', '.join(q(colmap[(oid,x['column_id'])]['name'])+('' if include else (' DESC' if x['is_descending_key'] else ' ASC')) for x in xs)
    index=[]; encrypted=[]
    for o in objects:
        oid=o['object_id'];schema=o['schema_name'];typ=o['type'].strip()
        # Child constraints are rendered in their parent table, preserving all metadata.
        if typ in ('D','C','F','PK','UQ'):continue
        folder=out/schema;folder.mkdir(exist_ok=True);name=o['name']
        if '/' in name or '\\' in name:raise RuntimeError('Unsafe SQL object name')
        dest=folder/(name+'.sql');lines=['-- Live Azure SQL metadata; database '+db+'; '+o['type_desc'], '-- Review dependencies before use. Not a migration to PostgreSQL.']
        detail={'object':o,'columns':[x for x in columns if x['object_id']==oid]}
        for k,vs in cat.items():
            relevant=[x for x in vs if x.get('object_id')==oid or x.get('parent_object_id')==oid or x.get('referencing_id')==oid]
            if relevant:detail[k]=relevant
        if oid in mod:
            m=mod[oid];detail['moduleProperties']={k:v for k,v in m.items() if k!='definition'}
            if m['definition'] is None:encrypted.append(schema+'.'+name);lines.append('-- UNKNOWN: module definition encrypted or unavailable')
            else:lines+=['SET ANSI_NULLS '+('ON' if m['uses_ansi_nulls'] else 'OFF')+';','GO','SET QUOTED_IDENTIFIER '+('ON' if m['uses_quoted_identifier'] else 'OFF')+';','GO',m['definition'].replace('\r\n','\n'),'GO']
        elif typ=='U':
            detail['tableProperties']=next(t for t in tables if t['object_id']==oid)
            parts=[]
            for x in detail['columns']:
                computed=next((z for z in cat['computed'] if z['object_id']==oid and z['column_id']==x['column_id']),None)
                identity=next((z for z in cat['identity'] if z['object_id']==oid and z['column_id']==x['column_id']),None)
                default=next((z for z in cat['defaults'] if z['parent_object_id']==oid and z['parent_column_id']==x['column_id']),None)
                if computed:s=q(x['name'])+' AS '+computed['definition']+(' PERSISTED' if computed['is_persisted'] else '')
                else:
                    s=q(x['name'])+' '+typename(x)
                    if x.get('collation_name'):s+=' COLLATE '+x['collation_name']
                    if identity:s+=f" IDENTITY({identity['seed_value']},{identity['increment_value']})"+(' NOT FOR REPLICATION' if identity['is_not_for_replication'] else '')
                    if x.get('is_rowguidcol'):s+=' ROWGUIDCOL'
                    if x.get('is_sparse'):s+=' SPARSE'
                    if x.get('generated_always_type'):s+=' GENERATED ALWAYS AS '+x['generated_always_type_desc'].replace('_',' ')+(' HIDDEN' if x.get('is_hidden') else '')
                    s+=' NULL' if x['is_nullable'] else ' NOT NULL'
                    if default:s+=' CONSTRAINT '+q(default['name'])+' DEFAULT '+default['definition']
                parts.append('    '+s)
            lines+=['CREATE TABLE '+fullname(oid)+' (',',\n'.join(parts),' );','GO']
            for x in cat['indexes']:
                if x['object_id']!=oid or x['index_id']==0:continue
                cols=idxcols(oid,x['index_id'])
                if x['type'] not in (1,2):lines+=['-- Specialized index '+q(x['name'])+': see metadata JSON'];continue
                key=next((k for k in cat['keys'] if k['parent_object_id']==oid and k['unique_index_id']==x['index_id']),None)
                if key:s='ALTER TABLE '+fullname(oid)+' ADD CONSTRAINT '+q(key['name'])+' '+('PRIMARY KEY' if key['type']=='PK' else 'UNIQUE')+' '+x['type_desc']+' ('+cols+')'
                else:s='CREATE '+('UNIQUE ' if x['is_unique'] else '')+x['type_desc']+' INDEX '+q(x['name'])+' ON '+fullname(oid)+' ('+cols+')'
                inc=idxcols(oid,x['index_id'],True)
                if inc:s+=' INCLUDE ('+inc+')'
                if x.get('filter_definition'):s+=' WHERE '+x['filter_definition']
                s+=f" WITH (PAD_INDEX = {'ON' if x['is_padded'] else 'OFF'}, FILLFACTOR = {x['fill_factor']}, IGNORE_DUP_KEY = {'ON' if x['ignore_dup_key'] else 'OFF'}, ALLOW_ROW_LOCKS = {'ON' if x['allow_row_locks'] else 'OFF'}, ALLOW_PAGE_LOCKS = {'ON' if x['allow_page_locks'] else 'OFF'});"
                lines += [s,'GO']
                if x['is_disabled']:lines+=['ALTER INDEX '+q(x['name'])+' ON '+fullname(oid)+' DISABLE;','GO']
            for x in cat['checks']:
                if x['parent_object_id']==oid:
                    lines+=['ALTER TABLE '+fullname(oid)+(' WITH NOCHECK' if x['is_not_trusted'] else ' WITH CHECK')+' ADD CONSTRAINT '+q(x['name'])+' CHECK '+('NOT FOR REPLICATION ' if x['is_not_for_replication'] else '')+x['definition']+';','GO']
                    if x['is_disabled']:lines+=['ALTER TABLE '+fullname(oid)+' NOCHECK CONSTRAINT '+q(x['name'])+';','GO']
            for x in cat['foreign_keys']:
                if x['parent_object_id']!=oid:continue
                fc=[z for z in cat['foreign_key_columns'] if z['constraint_object_id']==x['object_id']]
                left=', '.join(q(colmap[(oid,z['parent_column_id'])]['name']) for z in fc)
                right=', '.join(q(colmap[(x['referenced_object_id'],z['referenced_column_id'])]['name']) for z in fc)
                lines+=['ALTER TABLE '+fullname(oid)+(' WITH NOCHECK' if x['is_not_trusted'] else ' WITH CHECK')+' ADD CONSTRAINT '+q(x['name'])+' FOREIGN KEY ('+left+') REFERENCES '+fullname(x['referenced_object_id'])+' ('+right+') ON DELETE '+x['delete_referential_action_desc'].replace('_',' ')+' ON UPDATE '+x['update_referential_action_desc'].replace('_',' ')+(' NOT FOR REPLICATION' if x['is_not_for_replication'] else '')+';','GO']
                if x['is_disabled']:lines+=['ALTER TABLE '+fullname(oid)+' NOCHECK CONSTRAINT '+q(x['name'])+';','GO']
        elif typ=='SO':
            x=next(x for x in cat['sequences'] if x['object_id']==oid)
            lines+=['CREATE SEQUENCE '+fullname(oid)+' AS '+x['type_name']+' START WITH '+str(x['start_value'])+' INCREMENT BY '+str(x['increment'])+' MINVALUE '+str(x['minimum_value'])+' MAXVALUE '+str(x['maximum_value'])+(' CYCLE' if x['is_cycling'] else ' NO CYCLE')+(' CACHE '+str(x['cache_size']) if x['is_cached'] and x['cache_size'] else ' NO CACHE')+';','GO']
        elif typ=='SN':lines+=['CREATE SYNONYM '+fullname(oid)+' FOR '+next(x['base_object_name'] for x in cat['synonyms'] if x['object_id']==oid)+';','GO']
        else:lines+=['-- Definition represented by catalog metadata JSON; review type '+typ]
        dest.write_text('\n'.join(lines)+'\n',encoding='utf-8');dump(dest.with_suffix('.metadata.json'),detail)
        index.append({'schema':schema,'name':name,'type':typ,'path':str(dest.relative_to(out)).replace('\\','/'),'classification':'tooling/replication' if schema in ('dbrepl_meta','DataSync','dss','TaskHosting') or name.startswith(('sp_MS','dbrepl_dirty_')) or name in ('sp_upgraddiagrams','sp_helpdiagrams','sp_helpdiagramdefinition','sp_creatediagram','sp_renamediagram','sp_alterdiagram','sp_dropdiagram','fn_diagramobjects','sysdiagrams') else 'application'})
    meta=out/'_database';meta.mkdir(exist_ok=True)
    # Stable catalog ordering; do not include capture timestamps in definition files.
    for k,vs in cat.items():dump(meta/(k+'.json'),sorted(vs,key=lambda v:json.dumps(v,sort_keys=True,default=str)))
    dump(meta/'objects.json',objects)
    permissions=['-- Application and database permissions; credentials must be provisioned separately.']
    for x in cat['role_members']:permissions.append('ALTER ROLE '+q(x['role_name'])+' ADD MEMBER '+q(x['member_name'])+';')
    for x in cat['permissions']:
        target=''
        if x['class']==1:
            if not x['object_name']:continue
            target=' ON OBJECT::'+q(x['object_schema'])+'.'+q(x['object_name'])
            if x['minor_id']:target+=' ('+q(x['column_name'])+')'
        elif x['class']==3:target=' ON SCHEMA::'+q(next(z['name'] for z in cat['schemas'] if z['schema_id']==x['major_id']))
        elif x['class']!=0:
            permissions.append('-- Additional permission class recorded in permissions.json: '+x['class_desc']);continue
        state=x['state_desc'];cmd='GRANT' if state=='GRANT_WITH_GRANT_OPTION' else state
        permissions.append(cmd+' '+x['permission_name']+target+(' FROM ' if cmd=='REVOKE' else ' TO ')+q(x['grantee'])+(' WITH GRANT OPTION' if state=='GRANT_WITH_GRANT_OPTION' else '')+';')
    (meta/'permissions.sql').write_text('\n'.join(permissions)+'\n',encoding='utf-8')
    principals=['-- Identity provisioning definitions; credentials and Entra resolution supplied separately.']
    for x in cat['principals']:
        if x['type']=='R' and not x['is_fixed_role']:principals.append('CREATE ROLE '+q(x['name'])+';')
        elif x['type'] in ('E','X'):principals.append('CREATE USER '+q(x['name'])+' FROM EXTERNAL PROVIDER;')
        elif x['type']=='S':
            principals.append('CREATE USER '+q(x['name'])+(' WITH PASSWORD = \'<supply securely>\'' if x['authentication_type_desc']=='DATABASE' else ' WITHOUT LOGIN')+('; -- Review original authentication_type_desc in principals.json'))
        if x.get('default_schema_name') and x['type']!='R':principals.append('ALTER USER '+q(x['name'])+' WITH DEFAULT_SCHEMA = '+q(x['default_schema_name'])+';')
    (meta/'principals.sql').write_text('\n'.join(principals)+'\n',encoding='utf-8')
    (meta/'schemas.sql').write_text('\n'.join('CREATE SCHEMA '+q(s['name'])+' AUTHORIZATION '+q(s['owner_name'])+';\nGO' for s in cat['schemas'] if s['schema_id']<16384 and s['name'] not in ('dbo','guest','sys','INFORMATION_SCHEMA'))+'\n',encoding='utf-8')
    ft=[]
    for x in cat['fulltext_catalogs']:
        ft+=['CREATE FULLTEXT CATALOG '+q(x['name'])+' WITH ACCENT_SENSITIVITY = '+('ON' if x['is_accent_sensitivity_on'] else 'OFF')+(' AS DEFAULT' if x['is_default'] else '')+' AUTHORIZATION '+q(x['owner_name'])+';','GO']
    for x in cat['fulltext_indexes']:
        oid=x['object_id'];cols=[]
        for z in cat['fulltext_columns']:
            if z['object_id']!=oid:continue
            s=q(colmap[(oid,z['column_id'])]['name'])
            if z['type_column_id']:s+=' TYPE COLUMN '+q(colmap[(oid,z['type_column_id'])]['name'])
            s+=' LANGUAGE '+str(z['language_id'])
            if z.get('statistical_semantics'):s+=' STATISTICAL_SEMANTICS'
            cols.append(s)
        key=next(z['name'] for z in cat['indexes'] if z['object_id']==oid and z['index_id']==x['unique_index_id'])
        catalog=next(z['name'] for z in cat['fulltext_catalogs'] if z['fulltext_catalog_id']==x['fulltext_catalog_id'])
        stop='SYSTEM' if x['stoplist_id']==0 else ('OFF' if x['stoplist_id'] is None else q(next(z['name'] for z in cat['fulltext_stoplists'] if z['stoplist_id']==x['stoplist_id'])))
        ft+=['CREATE FULLTEXT INDEX ON '+fullname(oid)+' ('+', '.join(cols)+') KEY INDEX '+q(key)+' ON '+q(catalog)+' WITH (CHANGE_TRACKING = '+x['change_tracking_state_desc']+', STOPLIST = '+stop+');','GO']
        if not x['is_enabled']:ft+=['ALTER FULLTEXT INDEX ON '+fullname(oid)+' DISABLE;','GO']
    (meta/'fulltext.sql').write_text('\n'.join(ft)+'\n',encoding='utf-8')
    summary={'database':db,'status':'exported' if not encrypted else 'incomplete encrypted definitions','objects':index,'unavailableDefinitions':encrypted,'catalogCounts':{k:len(v) for k,v in cat.items()},'capturedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'method':'live sys catalog SELECTs; no application rows'}
    dump(out/'index.json',summary);c.rollback();c.close();print('SQL',db,len(index),'definitions',flush=True);return summary
