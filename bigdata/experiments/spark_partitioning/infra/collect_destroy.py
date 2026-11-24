"""Preserve results before deleting only the exact CloudFormation stack in ownership.json."""
import argparse
import atexit
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path
from deploy import aws,LAB


def main():
    p=argparse.ArgumentParser();p.add_argument('state',type=Path)
    p.add_argument('--execute',action='store_true');p.add_argument('--destroy',action='store_true')
    p.add_argument('--recover-stopped',action='store_true',help='Briefly start only owned stopped nodes to recover results; uses contingency budget')
    a=p.parse_args();owner=json.loads((a.state/'ownership.json').read_text())
    if owner.get('created_by')!='spark_partitioning/infra/deploy.py' or not owner['stack'].startswith('witw-partitioning-'):
        raise ValueError('Not an owned experiment')
    print(json.dumps(dict(action='collect then delete owned stack' if a.destroy else 'collect only',stack_id=owner['stack_id'],execute=a.execute),indent=2))
    if not a.execute:return
    stack=aws(['cloudformation','describe-stacks','--stack-name',owner['stack_id']],owner.get('profile'))['Stacks'][0]
    if stack['StackId']!=owner['stack_id']:raise ValueError('Ownership mismatch')
    outputs={x['OutputKey']:x['OutputValue'] for x in stack['Outputs']}
    ids=[outputs[k] for k in ['HeadId','Worker1Id','Worker2Id']]
    response=aws(['ec2','describe-instances','--instance-ids',*ids],owner.get('profile'))
    instances=[i for r in response['Reservations'] for i in r['Instances']]
    for instance in instances:
        tags={t['Key']:t['Value'] for t in instance.get('Tags',[])}
        if tags.get('OwnerStackId')!=owner['stack_id']:raise ValueError('Instance does not belong to this exact stack')
    stopped=[i['InstanceId'] for i in instances if i['State']['Name']=='stopped']
    if stopped:
        if not a.recover_stopped:raise RuntimeError('Nodes are stopped; results remain on EBS. Use --recover-stopped for bounded result recovery before deletion.')
        aws(['ec2','start-instances','--instance-ids',*stopped],owner.get('profile'))
        recovered={'deleted':False}
        def stop_recovered():
            if not recovered['deleted']:
                aws(['ec2','stop-instances','--instance-ids',*stopped],owner.get('profile'))
        atexit.register(stop_recovered)
        aws(['ec2','wait','instance-running','--instance-ids',*stopped],owner.get('profile'))
        aws(['ec2','wait','instance-status-ok','--instance-ids',*stopped],owner.get('profile'))
        response=aws(['ec2','describe-instances','--instance-ids',*ids],owner.get('profile'))
        instances=[i for r in response['Reservations'] for i in r['Instances']]
    head_instance=next(i for i in instances if i['InstanceId']==outputs['HeadId'])
    if 'PublicIpAddress' not in head_instance:raise RuntimeError('Owned Head has no current public IP; do not delete before recovery')
    head='ubuntu@'+head_instance['PublicIpAddress']
    options=['-i',owner['ssh_key_file'],'-o','StrictHostKeyChecking=accept-new','-o','UserKnownHostsFile='+str(a.state/'known_hosts')]
    remote='sudo systemctl stop witw-host-sampler.service; sudo tar -czf /tmp/witw-results.tar.gz -C /opt/witw results state && sudo chmod 644 /tmp/witw-results.tar.gz'
    subprocess.run(['ssh',*options,head,remote],check=True)
    target=LAB/'results'/('ec2-'+owner['stack']+'.tar.gz')
    subprocess.run(['scp',*options,head+':/tmp/witw-results.tar.gz',str(target)],check=True)
    with tarfile.open(target) as archive:
        if not any(n.startswith('results/') for n in archive.getnames()):raise ValueError('Results archive is empty')
    receipt=dict(stack_id=owner['stack_id'],archive=str(target.resolve()),sha256=hashlib.sha256(target.read_bytes()).hexdigest(),bytes=target.stat().st_size)
    (a.state/'collection-receipt.json').write_text(json.dumps(receipt,indent=2))
    if a.destroy:
        aws(['cloudformation','delete-stack','--stack-name',owner['stack_id']],owner.get('profile'))
        aws(['cloudformation','wait','stack-delete-complete','--stack-name',owner['stack_id']],owner.get('profile'))
        (a.state/'DESTROYED.json').write_text(json.dumps(receipt,indent=2))
        if stopped:recovered['deleted']=True
    print(json.dumps(receipt,indent=2))


if __name__=='__main__':main()
