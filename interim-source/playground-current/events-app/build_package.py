"""Build a source package from an explicit allowlist; never include local secrets."""
from pathlib import Path
import hashlib
import json
import zipfile

ROOT=Path(__file__).resolve().parent
FILES=[
    'function_app.py','event_domain.py','sql_repository.py','workshop_domain.py',
    'workshop_repository.py','workshop_messages.py','workshop_providers.py','workshop_worker.py',
    'event_registration.html','event_manage.html','workshop_admin.js','workshop_qr_page.html','workshop_qr_share.js','workshop_chapter_qr.html',
    'host.json','requirements.txt','.funcignore','README.md','OPERATIONS.md','VERIFICATION.md','build_package.py',
    'auth/app-roles.json','auth/authsettings.json',
    'workshop_qr.py', 'downloads/free-beginners-workshop-qr.svg', 'downloads/free-beginners-workshop-qr.png',
]
FILES += [str(p.relative_to(ROOT)).replace('\\','/') for folder,pattern in [('sql','*.sql'),('tests','*.py'),('tests','*.cjs')] for p in sorted((ROOT/folder).glob(pattern))]

if __name__=='__main__':
    target=ROOT/'test-results'/'aga-workshop-package.zip';target.parent.mkdir(exist_ok=True)
    manifest={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in FILES}
    with zipfile.ZipFile(target,'w',zipfile.ZIP_DEFLATED) as package:
        for name in FILES:package.write(ROOT/name,name)
        package.writestr('MANIFEST.json',json.dumps(manifest,indent=2))
    print(f'Created {target} ({len(FILES)} source files; no credentials or bundled dependencies).')
