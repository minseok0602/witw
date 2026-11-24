"""Controller for an already approved/created stack. No AWS operations without --execute."""
import argparse
import json
import subprocess
from pathlib import Path
from deploy import aws


def main():
    p=argparse.ArgumentParser()
    p.add_argument('state',type=Path)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--keep-resources',action='store_true')
    p.add_argument('--scope',choices=['original','deliberate-skew'],default='original')
    a=p.parse_args()
    owner=json.loads((a.state/'ownership.json').read_text())
    if owner.get('created_by')!='spark_partitioning/infra/deploy.py':raise ValueError('Not an owned experiment stack')
    plan=[(app,mode) for mode in ['quick','full'] for app in ['A','B']]
    print(json.dumps(dict(stack_id=owner['stack_id'],runs=plan,scope=a.scope,
                         main_measurements=180 if a.scope=='deliberate-skew' else 720,
                         additional_driver_baselines=0 if a.scope=='deliberate-skew' else 240,execute=a.execute),indent=2))
    if not a.execute:return
    cluster=json.loads((a.state/'cluster.json').read_text())
    response=aws(['ec2','describe-instances','--instance-ids',cluster['HeadId'],cluster['Worker1Id'],cluster['Worker2Id']],owner.get('profile'))
    instances=[i for r in response['Reservations'] for i in r['Instances']]
    if any({t['Key']:t['Value'] for t in i.get('Tags',[])}.get('OwnerStackId')!=owner['stack_id'] for i in instances):
        raise ValueError('Instance ownership mismatch')
    head='ubuntu@'+next(i['PublicIpAddress'] for i in instances if i['InstanceId']==cluster['HeadId'])
    options=['-i',owner['ssh_key_file'],'-o','StrictHostKeyChecking=accept-new','-o','UserKnownHostsFile='+str(a.state/'known_hosts')]
    failure=None
    collection_failed=False
    try:
        for app,mode in plan:
            if app=='A' and mode=='full':
                subprocess.run(['ssh',*options,head,'sudo python3 /opt/witw/workspace/bigdata/experiments/spark_partitioning/infra/check_budget.py'],check=True)
            command='sudo python3 /opt/witw/workspace/bigdata/experiments/spark_partitioning/infra/run_ec2.py --execute --app '+app+' --mode '+mode
            if a.scope=='deliberate-skew':command+=' --scenarios SKEW'
            subprocess.run(['ssh',*options,head,command],check=True)
    except BaseException as exc:
        failure=exc
        (a.state/'RUN_FAILED.json').write_text(json.dumps(dict(type=type(exc).__name__,message=str(exc))))
    finally:
        collector=Path(__file__).with_name('collect_destroy.py')
        command=['python',str(collector),str(a.state),'--execute']
        if not a.keep_resources:command+=['--destroy']
        result=subprocess.run(command)
        collection_failed=result.returncode!=0
        if collection_failed:
            (a.state/'COLLECTION_FAILED.json').write_text(json.dumps(dict(returncode=result.returncode,stack_id=owner['stack_id'],ebs_preserved=True)))
        if result.returncode or a.keep_resources:
            # Preserve EBS and ownership receipt if recovery failed; stop only these verified nodes.
            aws(['ec2','stop-instances','--instance-ids',*[i['InstanceId'] for i in instances]],owner.get('profile'))
    if failure:raise failure
    if collection_failed:raise RuntimeError('Results collection failed; owned nodes stopped with EBS preserved, recovery required')


if __name__=='__main__':main()
