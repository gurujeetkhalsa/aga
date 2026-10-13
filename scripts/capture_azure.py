"""Read-only AGA source/schema capture. Never deploys, invokes functions, or reads rows.

Install requests, azure-storage-blob, dissect.squashfs and pyodbc; install ODBC Driver 18 and Azure CLI.
Run with --output pointing to a NEW directory outside the checkout. Credentials stay
in memory. Output is a REVIEW CANDIDATE, never automatically commit it.
"""
import argparse, datetime, hashlib, io, json, os, re, subprocess, zipfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit,parse_qsl,urlencode
import requests
from azure.storage.blob import BlobServiceClient

SUB = '4dcce4a0-c902-4012-91fe-6dbbddfd6fb7'
TENANT = '5f1a8692-caac-4040-9a1b-858e98f03e4b'
EXCLUDE_DIRS = {'.python_packages','site-packages','__pycache__','.git','.venv','venv','node_modules','data','test-results','.test-deps'}
SOURCE_SUFFIXES = {'.py','.html','.htm','.js','.cjs','.mjs','.css','.json','.txt','.md','.sql','.ps1','.sh','.toml','.yaml','.yml','.svg','.png','.jpg','.jpeg','.gif','.ico','.woff','.woff2','.ttf','.map','.xml','.in','.cfg','.ini','.rst'}
SECRET_PATTERN = re.compile(r'(?:AccountKey\s*=\s*[A-Za-z0-9+/]{30,}|(?:[?&](?:sig|code|access_token)=)[^\s"\'<>]+|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|gh[pousr]_[A-Za-z0-9]{25,}|eyJ[A-Za-z0-9_-]{20,}\.eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+)', re.I)

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def write_json(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2,sort_keys=True,default=str)+'\n',encoding='utf-8')
def az(*args):
    p=subprocess.run(['az.cmd' if os.name=='nt' else 'az',*args,'-o','json'],capture_output=True,text=True)
    if p.returncode: raise RuntimeError('Azure CLI failed (credentials/errors intentionally suppressed): '+ ' '.join(args[:3]))
    return json.loads(p.stdout)
def clean_url(value):
    if not value:return ''
    p=urlsplit(value); return p._replace(query='',fragment='').geturl()
def safe_setting(k,v):
    # Default deny. No addresses, identities, recipient lists, OAuth or SQL credentials.
    if re.search(r'(?i)(ENABLED|DISABLED)$',k) and v.lower() in {'true','false','1','0'}:return v
    if k=='EVENTS_EMAIL_MODE' and v.lower() in {'live','test','disabled','production','dry-run'}:return v
    if k in {'AzureWebJobsStorage','EVENTS_ALLOWED_SENDERS','EVENTS_ZOOM_ACCOUNT_ID','GOOGLE_WORKSPACE_MAILBOX'} or re.search(r'(SECRET|PASSWORD|TOKEN|KEY|CONNECTION|RECIPIENT|EMAIL|USER|CLIENT_ID|TENANT_ID|INSTRUMENTATION)',k,re.I): return '<supplied securely>'
    if k in {'AzureFunctionsWebHost__hostid','CLUBEXPRESS_MAILBOX_FOLDER','CLUBEXPRESS_PROCESSED_CATEGORY','GOOGLE_WORKSPACE_QUERY','CLUBEXPRESS_CLUB_DOMAIN','APPLICATIONINSIGHTS_AUTHENTICATION_STRING','NEWS_TRANSLATION_RESUME_AT'}:return v
    if re.fullmatch(r'(?i:true|false|yes|no|on|off|disabled|enabled|none|python|~4|~1|\d+(?:\.\d+)?)',v): return v
    if re.search(r'(SCHEDULE|CRON|TIME_ZONE|TIMEZONE|CONTAINER|QUEUE|BLOB_PREFIX|BLOB_NAME|DATABASE|SQL_SERVER|WORKER_RUNTIME|EXTENSION_VERSION)',k,re.I) and not SECRET_PATTERN.search(v): return v
    if re.search(r'(URL|URI|ENDPOINT)',k) and v.startswith('https://'):
        u=urlsplit(v);query=[(a,'<supplied securely>' if re.search(r'(?i)^(sig|code|token|key|access_token)$',a) else b) for a,b in parse_qsl(u.query)]
        return u._replace(query=urlencode(query),fragment='').geturl()
    return '<supplied securely; review behavior value privately>'

class Capture:
    def __init__(self,sub):
        self.sub=sub
        a=az('account','show','--subscription',sub)
        if sub!=SUB or a['tenantId']!=TENANT: raise RuntimeError('Expected explicitly authorized AGA subscription/tenant')
        self.token=az('account','get-access-token','--subscription',sub)['accessToken']
        self.storage={}
    def arm(self,path,method='GET',version='2024-04-01'):
        allowed=('/config/appsettings/list','/config/connectionstrings/list','/host/default/listkeys','/listKeys')
        if method!='GET' and not (method=='POST' and path.endswith(allowed)): raise ValueError('Non-read ARM operation refused')
        r=requests.request(method,'https://management.azure.com'+path+'?api-version='+version,headers={'Authorization':'Bearer '+self.token},timeout=90)
        if not r.ok: raise RuntimeError('ARM HTTP '+str(r.status_code)+' '+path)
        return json.JSONDecoder().raw_decode(r.text)[0]
    def collection(self,path,version='2024-04-01'):
        # ARM lists used here have no continuation in this subscription. Fail closed if that changes.
        d=self.arm(path,version=version)
        if d.get('nextLink'): raise RuntimeError('Pagination requires review: '+path)
        return d.get('value',[])
    def blob(self,account,group):
        if account not in self.storage:
            key=self.arm(f'/subscriptions/{self.sub}/resourceGroups/{group}/providers/Microsoft.Storage/storageAccounts/{account}/listKeys','POST','2023-05-01')['keys'][0]['value']
            self.storage[account]=BlobServiceClient(f'https://{account}.blob.core.windows.net',credential=key)
        return self.storage[account]

def extract_source(data,dest):
    files={}; exclusions=[]; alerts=[]
    archive_format='zip'
    if data[:4]==b'hsqs':
        from dissect.squashfs import SquashFS
        fs=SquashFS(io.BytesIO(data));converted=io.BytesIO();archive_format='squashfs'
        with zipfile.ZipFile(converted,'w') as output:
            def walk(node,prefix=''):
                for child in node.iterdir():
                    n=prefix+child.name
                    if child.is_dir():
                        if child.name not in EXCLUDE_DIRS:walk(child,n+'/')
                    elif child.is_file():output.writestr(n,child.open().read())
                    else:raise ValueError('Source symlink requires explicit review: '+n)
            walk(fs.root)
        data=converted.getvalue()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for entry in z.infolist():
            n=entry.filename.replace('\\','/'); p=PurePosixPath(n)
            if entry.is_dir():continue
            if p.is_absolute() or '..' in p.parts or ':' in n: raise ValueError('Unsafe ZIP path')
            reason=None
            if set(p.parts)&EXCLUDE_DIRS or p.suffix in {'.pyc','.pyo','.log'}: reason='installed dependency/cache/runtime data'
            elif p.suffix=='.json' and p.name.startswith('legacy_bayrate_'): reason='generated rating experiment output; production data excluded'
            elif p.name in {'local.settings.json','.env'} or 'credential' in p.name.lower() or p.suffix in {'.pem','.pfx','.key'}: reason='local configuration/credential'
            elif p.suffix.lower() not in SOURCE_SUFFIXES and p.name not in {'.funcignore','.gitignore','COPYING','LICENSE','NOTICE','Dockerfile'}: reason='not source allowlist; review if required asset'
            if reason:
                if not set(p.parts)&EXCLUDE_DIRS: exclusions.append({'path':n,'reason':reason})
                continue
            b=z.read(entry)
            if SECRET_PATTERN.search(b.decode('utf-8',errors='ignore')):alerts.append(n)
            f=dest.joinpath(*p.parts);f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b)
            files[n]=hashlib.sha256(b).hexdigest()
    return {'files':files,'excluded':exclusions,'secret_review_required':alerts,'archiveFormat':archive_format}

def capture_app(c,a,out):
    p=a['properties']; name=a['name']; rid=a['id']; group=rid.split('/')[4]
    record={'name':name,'resourceGroup':group,'state':p['state'],'hostname':p['defaultHostName'],'capturedAt':now(),'status':'unrecovered'}
    fcfg=p.get('functionAppConfig') or {}
    record['runtime']=fcfg.get('runtime') or {'linuxFxVersion':(p.get('siteConfig') or {}).get('linuxFxVersion')}
    settings=c.arm(rid+'/config/appsettings/list','POST')['properties']
    contract={'settings':{k:safe_setting(k,str(v)) for k,v in sorted(settings.items())},'identity':a.get('identity'),'deploymentStorageAuthentication':((fcfg.get('deployment') or {}).get('storage') or {}).get('authentication')}
    connections=c.arm(rid+'/config/connectionstrings/list','POST').get('properties',{})
    contract['connectionStrings']={k:{'type':v.get('type'),'value':'<supplied securely>'} for k,v in connections.items()}
    contract['storageAccounts']=sorted(set(re.findall(r'AccountName=([^;]+)', ';'.join(settings.values()))))
    contract['sqlTargets']=sorted(set(re.findall(r'(?:Server|Data Source)\s*=\s*([^;]+)', ';'.join(settings.values()),re.I)))
    fs=c.collection(rid+'/functions')
    contract['functions']=[{'name':f['name'].split('/')[-1],'config':f['properties'].get('config'),'isDisabled':f['properties'].get('isDisabled')} for f in fs]
    for cfg in ['authsettingsV2','authsettings','web']:
        try:
            config=c.arm(rid+'/config/'+cfg)['properties']
            if cfg=='web': config={k:config.get(k) for k in ['linuxFxVersion','alwaysOn','http20Enabled','ftpsState','minTlsVersion','cors','ipSecurityRestrictions','scmIpSecurityRestrictions','appCommandLine']}
            def scrub(x):
                if isinstance(x,dict):return {k:('<supplied securely>' if re.search(r'(?i)(secret|password|token|key)$',k) and not k.endswith('SettingName') and isinstance(v,str) else scrub(v)) for k,v in x.items()}
                if isinstance(x,list):return [scrub(v) for v in x]
                return x
            contract[cfg]=scrub(config)
        except RuntimeError as e: contract[cfg]={'unavailable':str(e)}
    write_json(out/'config'/f'{name}.json',contract)
    try:
        storage=(fcfg.get('deployment') or {}).get('storage')
        if storage:
            uri=urlsplit(storage['value']); account=uri.hostname.split('.')[0];container=uri.path.strip('/')
            svc=c.blob(account,group)
            state=json.loads(svc.get_blob_client(container,'kudu-state.json').download_blob().readall())
            active=state.get('ActiveDeploymentId'); release=state.get('Deployments',{}).get(active,{})
            if not active or release.get('Status')!=4 or not release.get('Complete'): raise RuntimeError('No successful active deployment in kudu-state')
            record['activeDeployment']={k:release.get(k) for k in ['Id','Status','Complete','EndTime','UploadedPackageName','RemoteBuild']}
            record['activeDeployment']['sourcePackage']=clean_url(release.get('SourcePackageUri',''))
            blobname=release.get('UploadedPackageName') or 'released-package.zip'
            if '/' in blobname or '\\' in blobname:raise RuntimeError('Unexpected active uploaded package path')
            blob=svc.get_blob_client(container,blobname)
            props=blob.get_blob_properties(); data=blob.download_blob(max_concurrency=8,etag=props.etag,match_condition=__import__('azure.core',fromlist=['MatchConditions']).MatchConditions.IfNotModified).readall()
            state2=json.loads(svc.get_blob_client(container,'kudu-state.json').download_blob().readall())
            if state2.get('ActiveDeploymentId')!=active:raise RuntimeError('Deployment changed during capture; recapture')
            record['packageLocation']=clean_url(blob.url)
            record['etag']=props.etag;record['lastModified']=str(props.last_modified)
            record['status']='recovered active deployment'
        else:
            # Linux Consumption stores its current SCM package in scm-releases.
            run=settings.get('WEBSITE_RUN_FROM_PACKAGE','')
            if run.startswith('https://'):
                r=requests.get(run,timeout=120);r.raise_for_status();data=r.content
                record['packageLocation']=clean_url(run);record['status']='recovered configured package'
            else:
                match=re.search(r'AccountName=([^;]+)',settings.get('AzureWebJobsStorage',''))
                if not match:raise RuntimeError('Classic storage account unknown')
                svc=c.blob(match.group(1),group);blob=svc.get_blob_client('scm-releases',f'scm-latest-{name}.zip')
                props=blob.get_blob_properties();data=blob.download_blob(max_concurrency=8,etag=props.etag,match_condition=__import__('azure.core',fromlist=['MatchConditions']).MatchConditions.IfNotModified).readall()
                record['packageLocation']=clean_url(blob.url);record['etag']=props.etag;record['lastModified']=str(props.last_modified)
                record['status']='recovered SCM current package; runtime verification pending'
        record['packageSha256']=hashlib.sha256(data).hexdigest();record['packageBytes']=len(data)
        record.update(extract_source(data,out/'azure-baseline'/name))
        # Authenticate only to the host's read-only VFS; never invoke an app route.
        try:
            key=c.arm(rid+'/host/default/listkeys','POST')['masterKey']
            r=requests.get(f"https://{p['defaultHostName']}/admin/vfs/home/site/wwwroot/function_app.py",headers={'x-functions-key':key},timeout=45)
            record['runtimeVfsStatus']=r.status_code
            if r.ok:
                record['runtimeEntrySha256']=hashlib.sha256(r.content).hexdigest()
                record['runtimeEntryMatches']=record['files'].get('function_app.py')==record['runtimeEntrySha256']
                if not record['runtimeEntryMatches']:record['status']='CONFLICT runtime entry differs from captured package'
                elif not storage:record['status']='recovered current package; runtime entry verified'
        except Exception as e:record['runtimeVfsStatus']=type(e).__name__
    except Exception as e:
        record['status']='unrecovered';record['error']=type(e).__name__+': '+str(e).split('?')[0][:300]
    write_json(out/'manifests'/f'{name}.json',record)
    print(name,record['status'],len(record.get('files',{})),flush=True)
    return record,settings

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--output',required=True,type=Path);ap.add_argument('--subscription',default=SUB);ap.add_argument('--skip-sql',action='store_true');args=ap.parse_args()
    out=args.output.resolve()
    if out.exists():raise SystemExit('Output must be a new directory; baseline/development files are never overwritten')
    repo=Path(__file__).resolve().parents[1]
    if out==repo or repo in out.parents:raise SystemExit('Output must be outside the repository')
    out.mkdir(parents=True);c=Capture(args.subscription)
    apps=c.collection(f'/subscriptions/{c.sub}/providers/Microsoft.Web/sites')
    apps=[a for a in apps if 'functionapp' in a.get('kind','').lower()]
    inv={'startedAt':now(),'subscription':c.sub,'tenant':TENANT,'apps':[]}
    sqlconnections={}
    for a in sorted(apps,key=lambda a:a['name']):
        try:
            rec,settings=capture_app(c,a,out);inv['apps'].append(rec)
            for k,v in settings.items():
                if 'SQL' in k and re.search(r'(Server|Data Source)\s*=',v,re.I):sqlconnections[v]=True
        except Exception as e:inv['apps'].append({'name':a['name'],'status':'failed','error':type(e).__name__})
    inv['sqlServers']=az('sql','server','list','--subscription',c.sub,'--query','[].{name:name,resourceGroup:resourceGroup,host:fullyQualifiedDomainName}')
    inv['storageAccounts']=az('storage','account','list','--subscription',c.sub,'--query','[].{name:name,resourceGroup:resourceGroup,kind:kind}')
    storage_contracts=[]
    for storage in inv['storageAccounts']:
        svc=c.blob(storage['name'],storage['resourceGroup'])
        storage_contracts.append({'account':storage['name'],'containers':[x.name for x in svc.list_containers()], 'queues':az('storage','queue','list','--subscription',c.sub,'--account-name',storage['name'],'--auth-mode','key','--query','[].name'),'contentsCaptured':False})
    write_json(out/'storage-contract.json',storage_contracts)
    for s in inv['sqlServers']:s['databases']=az('sql','db','list','--subscription',c.sub,'-g',s['resourceGroup'],'-s',s['name'],'--query','[].{name:name,status:status}')
    if not args.skip_sql:
        from export_sql_schema import export_schema
        # Multiple app credentials can target the same database; export once after validation.
        done=set();inv['sqlExports']=[]
        for connstr in sqlconnections:
            target=re.search(r'(?:Database|Initial Catalog)\s*=\s*([^;]+)',connstr,re.I)
            db=target.group(1) if target else 'unknown'
            if db in done:continue
            try:
                result=export_schema(connstr,out/'sql'/'azure-baseline');inv['sqlExports'].append(result);done.add(db)
            except Exception as e:inv['sqlExports'].append({'database':db,'status':'failed','error':type(e).__name__})
    inv['finishedAt']=now();write_json(out/'inventory.json',inv)
    print('Capture complete; inspect all manifests and review secrets before publishing.',flush=True)
if __name__=='__main__':main()
