"""Approved 540-run controller, two stop/start sessions, one absolute deadline."""
import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from deploy import aws


def owned(owner,cluster):
    ids=[cluster[k] for k in ['HeadId','Worker1Id','Worker2Id']]
    r=aws(['ec2','describe-instances','--instance-ids',*ids],owner.get('profile'))
    instances=[i for reservation in r['Reservations'] for i in reservation['Instances']]
    if len(instances)!=3 or any({t['Key']:t['Value'] for t in i.get('Tags',[])}.get('OwnerStackId')!=owner['stack_id'] for i in instances):
        raise ValueError('Exact stack ownership verification failed')
    return instances


def main():
    p=argparse.ArgumentParser();p.add_argument('state',type=Path);p.add_argument('--execute',action='store_true')
    a=p.parse_args();owner=json.loads((a.state/'ownership.json').read_text())
    if owner.get('created_by')!='spark_partitioning/infra/deploy.py':raise ValueError('Not an owned experiment')
    print(json.dumps(dict(scope='reliability',sessions=2,main_measurements=540,conditions=['NATURAL','MODERATE','SKEW'],
                         repetitions_per_session=15,approved_budget_krw=10000,execute=a.execute),indent=2),flush=True)
    if not a.execute:return
    cluster=json.loads((a.state/'cluster.json').read_text());ids=[cluster[k] for k in ['HeadId','Worker1Id','Worker2Id']]
    options=['-i',owner['ssh_key_file'],'-o','BatchMode=yes','-o','ConnectTimeout=15',
             '-o','StrictHostKeyChecking=accept-new','-o','UserKnownHostsFile='+str(a.state/'known_hosts')]
    head=None;failure=None;collection_ok=False
    def remote(command):
        return subprocess.run(['ssh',*options,head,command],check=True)
    def head_ready():
        instances=owned(owner,cluster)
        destination='ubuntu@'+next(i['PublicIpAddress'] for i in instances if i['InstanceId']==cluster['HeadId'])
        for _ in range(60):
            if subprocess.run(['ssh',*options,destination,'true'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:return destination
            time.sleep(5)
        raise RuntimeError('Head SSH did not become ready')
    try:
        head=head_ready()
        for session in [1,2]:
            (a.state/'SESSION_STATUS.json').write_text(json.dumps(dict(session=session,status='running',epoch=time.time())))
            remote('sudo systemctl start witw-host-sampler.service && sudo systemctl is-active witw-budget.timer')
            for mode in ['quick','full']:
                if mode=='full':remote('sudo python3 /opt/witw/workspace/bigdata/experiments/spark_partitioning/infra/check_budget.py --remaining-reps '+str(30 if session==1 else 15))
                for app in ['A','B']:
                    command='sudo python3 /opt/witw/workspace/bigdata/experiments/spark_partitioning/infra/run_ec2.py --execute --app '+app+' --mode '+mode
                    command+=' --scenarios NATURAL MODERATE SKEW --session '+str(session)
                    command+=' --run-id '+app+'-'+mode+'-session'+str(session)
                    if mode=='full':command+=' --reps 15 --rep-start '+str((session-1)*15)
                    remote(command)
            (a.state/f'SESSION{session}_COMPLETE.json').write_text(json.dumps(dict(epoch=time.time(),main_runs=270)))
            if session==1:
                subprocess.run([sys.executable,str(Path(__file__).with_name('collect_destroy.py')),str(a.state),'--execute'],check=True)
                receipt=json.loads((a.state/'collection-receipt.json').read_text())
                archive=Path(receipt['archive']);copy=archive.with_name(archive.name.replace('.tar.gz','-session1.tar.gz'))
                shutil.copy2(archive,copy)
                (a.state/'SESSION1_RECEIPT.json').write_text(json.dumps(dict(receipt,session1_copy=str(copy)),indent=2))
                owned(owner,cluster)
                aws(['ec2','stop-instances','--instance-ids',*ids],owner.get('profile'))
                aws(['ec2','wait','instance-stopped','--instance-ids',*ids],owner.get('profile'))
                time.sleep(30)
                aws(['ec2','start-instances','--instance-ids',*ids],owner.get('profile'))
                aws(['ec2','wait','instance-status-ok','--instance-ids',*ids],owner.get('profile'))
                head=head_ready()
        (a.state/'ALL_MEASUREMENTS_COMPLETE.json').write_text(json.dumps(dict(main_runs=540,epoch=time.time())))
    except BaseException as exc:
        failure=exc
        (a.state/'RUN_FAILED.json').write_text(json.dumps(dict(type=type(exc).__name__,message=str(exc))))
    finally:
        command=[sys.executable,str(Path(__file__).with_name('collect_destroy.py')),str(a.state),'--execute','--destroy']
        collection_ok=subprocess.run(command).returncode==0
        if not collection_ok:
            instances=owned(owner,cluster)
            running=[i['InstanceId'] for i in instances if i['State']['Name'] in ['running','pending']]
            if running:aws(['ec2','stop-instances','--instance-ids',*running],owner.get('profile'))
            (a.state/'RECOVERY_REQUIRED.json').write_text(json.dumps(dict(results_preserved_on_ebs=True)))
    if failure:raise failure
    if not collection_ok:raise RuntimeError('Collection failed; EBS preserved on stopped owned nodes')


if __name__=='__main__':main()
