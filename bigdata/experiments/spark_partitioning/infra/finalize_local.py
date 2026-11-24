"""Local postprocessing of the already running experiment. Never calls an AWS API."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from node_bootstrap import extract
from deploy import LAB,REPO


def main():
    p=argparse.ArgumentParser();p.add_argument('state',type=Path);args=p.parse_args()
    deadline=time.time()+11*3600
    while not (args.state/'DESTROYED.json').exists():
        if any((args.state/name).exists() for name in ['RUN_FAILED.json','RECOVERY_REQUIRED.json']):
            raise RuntimeError('Experiment failed or recovery is required; incomplete data will not be analyzed as 540 successful runs')
        if time.time()>deadline:raise TimeoutError('Collection did not complete within the approved experiment window')
        time.sleep(15)
    if (args.state/'RUN_FAILED.json').exists():raise RuntimeError('Failed experiment was cleaned up; no successful full report will be fabricated')
    receipt=json.loads((args.state/'DESTROYED.json').read_text());archive=Path(receipt['archive'])
    if hashlib.sha256(archive.read_bytes()).hexdigest()!=receipt['sha256']:raise ValueError('Collected archive checksum differs from receipt')
    owner=json.loads((args.state/'ownership.json').read_text())
    output=LAB/'results'/('ec2-'+owner['stack'])
    if output.exists():raise ValueError('Fresh extraction directory required')
    extract(archive,output)
    environment=dict(os.environ,PYTHONPATH=str(REPO/'.codex-tmp/clustering-analysis'))
    subprocess.run([sys.executable,str(LAB/'apps/report_reliability.py'),str(output/'results'),'--output',str(output/'analysis')],check=True,env=environment)
    (args.state/'ANALYSIS_COMPLETE.json').write_text(json.dumps(dict(report=str(output/'analysis/REPORT.md'),raw=str(output/'results'),measurements=540),indent=2))
    print(output/'analysis/REPORT.md',flush=True)


if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(type(exc).__name__+': '+str(exc),file=sys.stderr,flush=True)
        raise
