"""Static verification only: never import application modules or use credentials."""
import argparse,ast,hashlib,io,json,sys,zipfile
from pathlib import Path

ALIASES={'azure':'azure-functions','PIL':'pillow','pytds':'python-tds','OpenSSL':'pyopenssl','bs4':'beautifulsoup4','dateutil':'python-dateutil','google':'google-api-python-client','google_auth_oauthlib':'google-auth-oauthlib','yaml':'pyyaml','jwt':'pyjwt','Crypto':'pycryptodome','lxml':'lxml','fitz':'pymupdf'}
def verify(repo):
    reports=[]
    for root in sorted((repo/'azure-baseline').iterdir()):
        if not root.is_dir():continue
        manifest=json.loads((repo/'docs/azure-capture/manifests'/f'{root.name}.json').read_text())
        result={'app':root.name,'hashMismatches':[],'missingRequiredFiles':[],'syntaxErrors':[],'externalImports':[],'importsNeedingReview':[],'sourceFiles':len(manifest['files'])}
        for n,h in manifest['files'].items():
            p=root/n
            if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=h:result['hashMismatches'].append(n)
        for n in ['function_app.py','requirements.txt','host.json']:
            if not (root/n).is_file():result['missingRequiredFiles'].append(n)
        trees={}
        for p in root.rglob('*.py'):
            try:trees[p.relative_to(root).as_posix()]=ast.parse(p.read_text(encoding='utf-8-sig'),filename=str(p))
            except SyntaxError as e:result['syntaxErrors'].append({'path':str(p.relative_to(root)),'line':e.lineno,'message':e.msg})
        requirements=(root/'requirements.txt').read_text().lower() if (root/'requirements.txt').exists() else ''
        visited=set();external=set()
        def walk(name):
            if name in visited or name not in trees:return
            visited.add(name)
            for node in ast.walk(trees[name]):
                imports=[]
                if isinstance(node,ast.Import):imports=[x.name for x in node.names]
                elif isinstance(node,ast.ImportFrom):
                    base=(node.module or '')
                    if node.level:
                        parent=Path(name).parent
                        for _ in range(node.level-1):parent=parent.parent
                        base='.'.join([*parent.parts,base]).strip('.')
                    imports=[base]+[(base+'.'+x.name).strip('.') for x in node.names]
                for module in imports:
                    stem=module.replace('.','/');local=next((x for x in [stem+'.py',stem+'/__init__.py'] if x in trees),None)
                    if local:walk(local)
                    elif module and module.split('.')[0] not in sys.stdlib_module_names and not (root/module.split('.')[0]).is_dir() and not (root/(module.split('.')[0]+'.py')).is_file():external.add(module.split('.')[0])
        walk('function_app.py')
        result['reachableApplicationModules']=sorted(visited)
        result['externalImports']=sorted(external)
        for module in external:
            dist=ALIASES.get(module,module).lower().replace('_','-')
            if dist not in requirements.replace('_','-'):result['importsNeedingReview'].append(module)
        package=io.BytesIO()
        with zipfile.ZipFile(package,'w',zipfile.ZIP_DEFLATED) as z:
            for n in sorted(manifest['files']):
                if (root/n).is_file():z.writestr(n,(root/n).read_bytes())
        with zipfile.ZipFile(io.BytesIO(package.getvalue())) as z:
            result['packageCrcCheck']=z.testzip();result['packagedFiles']=len(z.namelist())
        reports.append(result)
    return reports
if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--output',type=Path,required=True);args=ap.parse_args();repo=Path(__file__).resolve().parents[1]
    results=verify(repo);args.output.write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8')
    for r in results:print(r['app'],r['sourceFiles'],'files; review imports:',r['importsNeedingReview'],'errors:',r['hashMismatches']+r['missingRequiredFiles']+r['syntaxErrors'])
    sys.exit(any(r['hashMismatches'] or r['missingRequiredFiles'] or r['syntaxErrors'] or r['packageCrcCheck'] for r in results))
