"""Default is a local plan. --execute requires an approved estimate and explicit SSH inputs."""
import argparse
import atexit
import getpass
import datetime
import hashlib
import ipaddress
import json
import re
import subprocess
import tarfile
import time
import urllib.request
from pathlib import Path
from make_template import template

LAB=Path(__file__).resolve().parents[1]
REPO=LAB.parents[2]
REGION='ap-northeast-2'


def aws(args,profile=None):
    command=['aws','--region',REGION,'--output','json']
    if profile:command+=['--profile',profile]
    result=subprocess.run(command+args,check=True,capture_output=True,text=True)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def bundle(destination):
    files=list((REPO/'bigdata/spark/apps').glob('*.py'))+[REPO/'bigdata/spark/Dockerfile',REPO/'bigdata/spark/resolve_kafka_dependencies.py']
    files+=list((REPO/'bigdata/spark/conf').glob('*'))
    for path in LAB.rglob('*'):
        relative=path.relative_to(LAB)
        if path.is_file() and not any(p in {'results','.runtime','__pycache__'} for p in relative.parts) and path.suffix not in {'.pem','.pyc'}:
            files.append(path)
    audit=json.loads((LAB/'source_audit.json').read_text())
    for name,value in audit.items():
        if hashlib.sha256((REPO/name).read_bytes().replace(b'\r\n',b'\n')).hexdigest()!=value:raise ValueError('Production source differs from audited baseline: '+name)
    manifest={}
    with tarfile.open(destination,'w:gz') as archive:
        for path in sorted(set(files)):
            if not path.resolve().is_relative_to(REPO.resolve()) or path.is_symlink():raise ValueError('Source path outside repository')
            name=path.relative_to(REPO).as_posix();manifest[name]=hashlib.sha256(path.read_bytes()).hexdigest()
            archive.add(path,arcname=name,recursive=False)
    destination.with_suffix('.manifest.json').write_text(json.dumps(manifest,indent=2))
    return manifest


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--execute',action='store_true');p.add_argument('--key-name');p.add_argument('--ssh-key-file',type=Path)
    p.add_argument('--ssh-cidr');p.add_argument('--profile');p.add_argument('--estimate',type=Path)
    p.add_argument('--auto-key',action='store_true',help='Create a local experiment-only SSH key and import only its public material with the owned stack')
    p.add_argument('--stack',default='witw-partitioning-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S'))
    p.add_argument('--budget-hours',type=int,default=10);p.add_argument('--max-budget-krw',type=int,default=30000)
    p.add_argument('--contingency-krw',type=int,default=3000)
    a=p.parse_args()
    if not re.fullmatch(r'witw-partitioning-[A-Za-z0-9-]{1,100}',a.stack):p.error('Only an experiment-prefixed stack can be created')
    if not 1<=a.budget_hours<=20:p.error('Budget deadline must be 1..20 hours')
    snapshot=json.loads((LAB/'infra/pricing_snapshot.json').read_text())
    hourly=float(snapshot['instances']['m7i.xlarge']['price'])+2*float(snapshot['instances']['m7i.large']['price'])+80*float(snapshot['gp3']['price'])/720+.005
    cap=hourly*a.budget_hours*1400*1.1
    if a.contingency_krw<0:p.error('Contingency cannot be negative')
    if cap+a.contingency_krw>a.max_budget_krw:p.error('Deadline cost plus contingency exceeds approved budget')
    plan=dict(region=REGION,stack=a.stack,instances={'m7i.xlarge':1,'m7i.large':2},same_az=True,
        public_ipv4=1,nat_gateways=0,ebs_gp3_gb=80,total_executor_cores=4,executors=2,
        deadline_hours=a.budget_hours,estimated_deadline_krw_including_vat=cap,
        contingency_krw=a.contingency_krw,max_budget_krw=a.max_budget_krw,approval_required=True,executed=a.execute)
    print(json.dumps(plan,indent=2))
    if not a.execute:return
    if not a.estimate:p.error('Execution requires a reviewed estimate')
    if not a.auto_key and not all([a.key_name,a.ssh_key_file]):p.error('Provide SSH key name/file or --auto-key')
    if not a.auto_key and not a.ssh_key_file.is_file():p.error('SSH private key file does not exist')
    if not a.ssh_cidr:
        with urllib.request.urlopen('https://checkip.amazonaws.com',timeout=15) as response:
            a.ssh_cidr=response.read().decode().strip()+'/32'
    network=ipaddress.ip_network(a.ssh_cidr)
    if network.version!=4 or network.prefixlen!=32:p.error('SSH source must be IPv4 /32')
    estimate=json.loads(a.estimate.read_text(encoding='utf-8'))
    upper=estimate.get('estimated_upper_krw',max(estimate.get('estimated_with_contingency_krw',[0])))
    if upper>a.max_budget_krw:p.error('Measured-based estimate exceeds approved budget')
    state=LAB/'.runtime'/a.stack
    state.mkdir(parents=True,exist_ok=False)
    if a.auto_key:
        import os
        if os.name=='nt':
            user=os.environ.get('USERDOMAIN','.')+'\\'+getpass.getuser()
            subprocess.run(['icacls',str(state),'/inheritance:r','/grant:r',user+':(OI)(CI)F'],check=True,capture_output=True)
        else:state.chmod(0o700)
        a.ssh_key_file=state/'lab-key'
        subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(a.ssh_key_file)],check=True)
        public=a.ssh_key_file.with_suffix('.pub').read_text().strip()
    else:public=''
    (state/'plan.json').write_text(json.dumps(plan,indent=2))
    parameters=[dict(ParameterKey='KeyName',ParameterValue=a.key_name or ''),dict(ParameterKey='PublicKeyMaterial',ParameterValue=public),dict(ParameterKey='SshCidr',ParameterValue=a.ssh_cidr),
                dict(ParameterKey='BudgetHours',ParameterValue=str(a.budget_hours))]
    (state/'parameters.json').write_text(json.dumps(parameters))
    template_file=state/'cloudformation.json';template_file.write_text(json.dumps(template(),indent=2))
    source=state/'source.tar.gz';bundle(source)
    created=aws(['cloudformation','create-stack','--stack-name',a.stack,'--template-body','file://'+str(template_file),
                 '--parameters','file://'+str(state/'parameters.json'),'--capabilities','CAPABILITY_IAM'],a.profile)
    ownership=dict(stack_id=created['StackId'],stack=a.stack,profile=a.profile,region=REGION,ssh_key_file=str(a.ssh_key_file.resolve()),
                   budget_hours=a.budget_hours,created_by='spark_partitioning/infra/deploy.py',source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    (state/'ownership.json').write_text(json.dumps(ownership,indent=2))
    provisioning={'complete':False}
    def stop_failed_provisioning():
        if provisioning['complete']:return
        (state/'PROVISIONING_FAILED.json').write_text(json.dumps(dict(stack_id=created['StackId'],results_preserved=True)))
        try:
            response=aws(['ec2','describe-instances','--filters','Name=tag:OwnerStackId,Values='+created['StackId'],
                          'Name=instance-state-name,Values=pending,running'],a.profile)
            ids=[i['InstanceId'] for r in response['Reservations'] for i in r['Instances']
                 if {t['Key']:t['Value'] for t in i.get('Tags',[])}.get('OwnerStackId')==created['StackId']]
            if ids:aws(['ec2','stop-instances','--instance-ids',*ids],a.profile)
        except Exception as exc:
            (state/'STOP_RECOVERY_REQUIRED.txt').write_text(str(exc))
    atexit.register(stop_failed_provisioning)
    aws(['cloudformation','wait','stack-create-complete','--stack-name',created['StackId']],a.profile)
    stack=aws(['cloudformation','describe-stacks','--stack-name',created['StackId']],a.profile)['Stacks'][0]
    outputs={x['OutputKey']:x['OutputValue'] for x in stack['Outputs']}
    outputs.update(experiment=a.stack,owner_stack_id=created['StackId'],budget_hours=a.budget_hours)
    (state/'cluster.json').write_text(json.dumps(outputs,indent=2))
    response=aws(['ec2','describe-instances','--instance-ids',outputs['HeadId'],outputs['Worker1Id'],outputs['Worker2Id']],a.profile)
    instances=[i for r in response['Reservations'] for i in r['Instances']]
    if len(instances)!=3 or sorted(i['InstanceType'] for i in instances)!=['m7i.large','m7i.large','m7i.xlarge']:
        raise RuntimeError('Actual EC2 instance allocation differs from the reviewed plan')
    if len({i['Placement']['AvailabilityZone'] for i in instances})!=1:raise RuntimeError('Instances are not in the same AZ')
    if any({t['Key']:t['Value'] for t in i.get('Tags',[])}.get('OwnerStackId')!=created['StackId'] for i in instances):
        raise RuntimeError('Actual instance ownership mismatch')
    volume_ids=[b['Ebs']['VolumeId'] for i in instances for b in i['BlockDeviceMappings']]
    volumes=aws(['ec2','describe-volumes','--volume-ids',*volume_ids],a.profile)['Volumes']
    if sorted(v['Size'] for v in volumes)!=[20,20,40] or any(v['VolumeType']!='gp3' or v['Iops']!=3000 or v['Throughput']!=125 or not v['Encrypted'] for v in volumes):
        raise RuntimeError('Actual EBS allocation differs from fixed gp3 settings')
    (state/'actual_infrastructure.json').write_text(json.dumps(dict(instances=instances,volumes=volumes),indent=2))
    head='ubuntu@'+outputs['HeadPublicIp']
    options=['-i',str(a.ssh_key_file),'-o','StrictHostKeyChecking=accept-new','-o','UserKnownHostsFile='+str(state/'known_hosts')]
    for _ in range(60):
        if subprocess.run(['ssh',*options,'-o','ConnectTimeout=10',head,'true'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
        time.sleep(10)
    else:raise RuntimeError('SSH did not become ready; preserve ownership state and clean up the owned stack')
    subprocess.run(['ssh',*options,head,'sudo cloud-init status --wait && sudo systemctl is-active witw-budget.timer witw-publish.service'],check=True)
    subprocess.run(['scp',*options,str(source),head+':/tmp/witw-source.tar.gz'],check=True)
    subprocess.run(['scp',*options,str(LAB/'infra/node_bootstrap.py'),head+':/tmp/witw-node-bootstrap.py'],check=True)
    subprocess.run(['scp',*options,str(state/'cluster.json'),head+':/tmp/witw-cluster.json'],check=True)
    command='sudo python3 /tmp/witw-node-bootstrap.py --role head --experiment '+a.stack+' --source /tmp/witw-source.tar.gz'
    subprocess.run(['ssh',*options,head,command],check=True)
    subprocess.run(['ssh',*options,head,'sudo cp /tmp/witw-cluster.json /opt/witw/state/cluster.json'],check=True)
    provisioning['complete']=True
    print('Provisioned. Run remote quick checks before full measurement; collect and destroy using this ownership directory.')


if __name__=='__main__':main()
