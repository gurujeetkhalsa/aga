"""Create a source-only ZIP from the reviewed, hash-checked captured manifest."""
import argparse,hashlib,json,zipfile
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('app');p.add_argument('--output',required=True,type=Path);args=p.parse_args()
    repo=Path(__file__).resolve().parents[1]
    if args.output.exists():raise SystemExit('Refusing to overwrite output')
    manifest_path=repo/'docs/azure-capture/manifests'/f'{args.app}.json'
    m=json.loads(manifest_path.read_text());root=repo/'azure-baseline'/args.app
    for n,h in m['files'].items():
        if hashlib.sha256((root/n).read_bytes()).hexdigest()!=h:raise SystemExit('Captured source differs: '+n)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(args.output,'w',zipfile.ZIP_DEFLATED) as z:
        for n in sorted(m['files']):
            info=zipfile.ZipInfo(n,date_time=(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,(root/n).read_bytes())
    print(f'{args.app}: {len(m["files"])} reviewed source files packaged. Dependencies/data are supplied separately; no deployment performed.')
if __name__=='__main__':main()
