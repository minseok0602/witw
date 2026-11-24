"""Read-only progress of exactly the owned experiment; never starts or stops resources."""
import argparse
import json
import shlex
import subprocess
from pathlib import Path
from deploy import aws


def main():
    parser=argparse.ArgumentParser();parser.add_argument('state',type=Path);args=parser.parse_args()
    for name,status in [('RUN_FAILED.json','failed'),('RECOVERY_REQUIRED.json','recovery_required'),('DESTROYED.json','collected_and_destroyed')]:
        if (args.state/name).exists():
            print(json.dumps(dict(controller_state=status,marker=name)));return
    owner=json.loads((args.state/'ownership.json').read_text());cluster=json.loads((args.state/'cluster.json').read_text())
    response=aws(['ec2','describe-instances','--instance-ids',cluster['HeadId']],owner.get('profile'))
    instance=response['Reservations'][0]['Instances'][0]
    if {t['Key']:t['Value'] for t in instance.get('Tags',[])}.get('OwnerStackId')!=owner['stack_id']:raise ValueError('Ownership mismatch')
    if instance['State']['Name']!='running':
        print(json.dumps(dict(head_state=instance['State']['Name'])));return
    code="""import csv,json,pathlib,time
root=pathlib.Path('/opt/witw/results');runs=[]
for path in sorted(root.glob('*/runs.csv')):
 rows=list(csv.DictReader(path.open()));measured=[r for r in rows if r['phase']!='warmup']
 validated=path.parent/'VALIDATED.json'
 runs.append(dict(run=path.parent.name,phase=measured[-1]['phase'] if measured else 'warmup',measured=len(measured),
  equal=all(r['equal']=='True' for r in measured),validated=validated.exists() and json.loads(validated.read_text())['passed'],
  latest_condition=measured[-1]['scenario'] if measured else None,latest_rep=measured[-1]['rep'] if measured else None))
deadline=pathlib.Path('/opt/witw/state/deadline_epoch')
print(json.dumps(dict(main_completed=sum(r['measured'] for r in runs if r['phase']=='full'),quick_completed=sum(r['measured'] for r in runs if r['phase']=='quick'),
 runs=runs,deadline_remaining_h=(float(deadline.read_text())-time.time())/3600)))
"""
    cmd=['ssh','-i',owner['ssh_key_file'],'-o','ConnectTimeout=10','-o','UserKnownHostsFile='+str(args.state/'known_hosts'),
         'ubuntu@'+instance['PublicIpAddress'],'sudo python3 -c '+shlex.quote(code)]
    subprocess.run(cmd,check=True)


if __name__=='__main__':main()
