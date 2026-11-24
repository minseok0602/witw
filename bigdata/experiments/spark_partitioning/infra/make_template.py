"""Emit a reviewable CloudFormation template; no AWS calls."""
import json
from pathlib import Path


def ref(name):return {'Ref':name}
def sub(text):return {'Fn::Sub':text}
def get(name,attr):return {'Fn::GetAtt':[name,attr]}


def template():
    tags=[{'Key':'ExperimentId','Value':ref('AWS::StackName')},{'Key':'OwnerStackId','Value':ref('AWS::StackId')},
          {'Key':'Purpose','Value':'witw-spark-partitioning-only'}]
    resources={
      'Vpc':{'Type':'AWS::EC2::VPC','Properties':{'CidrBlock':'10.91.0.0/16','EnableDnsSupport':True,'EnableDnsHostnames':True,'Tags':tags}},
      'Subnet':{'Type':'AWS::EC2::Subnet','Properties':{'VpcId':ref('Vpc'),'CidrBlock':'10.91.1.0/24',
          'AvailabilityZone':{'Fn::Select':[0,{'Fn::GetAZs':''}]},'MapPublicIpOnLaunch':False,'Tags':tags}},
      'Gateway':{'Type':'AWS::EC2::InternetGateway','Properties':{'Tags':tags}},
      'GatewayAttachment':{'Type':'AWS::EC2::VPCGatewayAttachment','Properties':{'VpcId':ref('Vpc'),'InternetGatewayId':ref('Gateway')}},
      'Routes':{'Type':'AWS::EC2::RouteTable','Properties':{'VpcId':ref('Vpc'),'Tags':tags}},
      'InternetRoute':{'Type':'AWS::EC2::Route','DependsOn':'GatewayAttachment','Properties':{'RouteTableId':ref('Routes'),
          'DestinationCidrBlock':'0.0.0.0/0','GatewayId':ref('Gateway')}},
      'RouteAssociation':{'Type':'AWS::EC2::SubnetRouteTableAssociation','Properties':{'SubnetId':ref('Subnet'),'RouteTableId':ref('Routes')}},
      'LabSecurityGroup':{'Type':'AWS::EC2::SecurityGroup','Properties':{'GroupDescription':'Only this disposable experiment',
          'VpcId':ref('Vpc'),'SecurityGroupIngress':[{'IpProtocol':'tcp','FromPort':22,'ToPort':22,'CidrIp':ref('SshCidr')}],'Tags':tags}},
      'LabInternal':{'Type':'AWS::EC2::SecurityGroupIngress','Properties':{'GroupId':ref('LabSecurityGroup'),
          'SourceSecurityGroupId':ref('LabSecurityGroup'),'IpProtocol':'tcp','FromPort':1,'ToPort':65535}},
      'HeadRole':{'Type':'AWS::IAM::Role','Properties':{
          'AssumeRolePolicyDocument':{'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'ec2.amazonaws.com'},'Action':'sts:AssumeRole'}]},
          'Policies':[{'PolicyName':'StopOnlyOwnedExperiment','PolicyDocument':{'Version':'2012-10-17','Statement':[
              {'Effect':'Allow','Action':['ec2:DescribeInstances'],'Resource':'*'},
              {'Effect':'Allow','Action':['ec2:StopInstances'],'Resource':sub('arn:${AWS::Partition}:ec2:${AWS::Region}:${AWS::AccountId}:instance/*'),
               'Condition':{'StringEquals':{'aws:ResourceTag/OwnerStackId':ref('AWS::StackId')}}}]}}],'Tags':tags}},
      'HeadProfile':{'Type':'AWS::IAM::InstanceProfile','Properties':{'Roles':[ref('HeadRole')]}},
    }
    head_init="#!/bin/bash\nset -euo pipefail\nexec > >(tee /var/log/witw-head-init.log) 2>&1\napt-get update\nDEBIAN_FRONTEND=noninteractive apt-get install -y python3-boto3 curl ca-certificates\ninstall -d -m 755 /opt/witw/publish /opt/witw/results /opt/witw/state\ncat >/opt/witw/stop_owned.py <<'PY'\nimport boto3,json,os,subprocess,time\nfrom pathlib import Path\nstack='${AWS::StackId}'\nec2=boto3.client('ec2',region_name='${AWS::Region}')\nr=ec2.describe_instances(Filters=[{'Name':'tag:OwnerStackId','Values':[stack]},{'Name':'instance-state-name','Values':['pending','running']}])\nids=[i['InstanceId'] for x in r['Reservations'] for i in x['Instances']]\nPath('/opt/witw/results/BUDGET_STOP.json').write_text(json.dumps({'epoch':time.time(),'stack':stack,'instances':ids}))\ntry:\n p=subprocess.run(['docker','ps','-q','--filter','label=witw.experiment=${AWS::StackName}'],capture_output=True,text=True)\n if p.returncode==0 and p.stdout.split():subprocess.run(['docker','stop','--timeout','30',*p.stdout.split()],timeout=50)\nexcept Exception as e:print('Container stop failed; still stopping owned EC2:',str(e),flush=True)\nos.sync()\nif ids:ec2.stop_instances(InstanceIds=ids)\nPY\ncat >/etc/systemd/system/witw-budget.service <<'UNIT'\n[Unit]\nDescription=Preserve data and stop only this experiment at budget deadline\n[Service]\nType=oneshot\nExecStart=/usr/bin/python3 /opt/witw/stop_owned.py\nUNIT\ncat >/etc/systemd/system/witw-budget.timer <<'UNIT'\n[Unit]\nDescription=Owned experiment budget deadline\n[Timer]\nOnBootSec=${BudgetHours}h\nUnit=witw-budget.service\n[Install]\nWantedBy=timers.target\nUNIT\ncat >/etc/systemd/system/witw-publish.service <<'UNIT'\n[Unit]\nDescription=Private artifact publisher for experiment workers\n[Service]\nExecStart=/usr/bin/python3 -m http.server 8090 --bind 0.0.0.0 --directory /opt/witw/publish\nRestart=always\n[Install]\nWantedBy=multi-user.target\nUNIT\nsystemctl daemon-reload\nsystemctl enable --now witw-budget.timer witw-publish.service\n"
    # Keep one absolute deadline across stop/start sessions; never grant another ten hours on reboot.
    head_init=head_init.replace("cat >/etc/systemd/system/witw-budget.timer <<'UNIT'\n",
        'deadline=$(date -u -d "+${BudgetHours} hours" "+%Y-%m-%d %H:%M:%S UTC")\n'
        'date -u -d "$deadline" +%s >/opt/witw/state/deadline_epoch\n'
        'cat >/etc/systemd/system/witw-budget.timer <<UNIT\n')
    head_init=head_init.replace('OnBootSec=${BudgetHours}h\n','OnCalendar=$deadline\nPersistent=true\n')
    resources['OwnedKeyPair']={'Type':'AWS::EC2::KeyPair','Condition':'CreateOwnedKey',
        'Properties':{'KeyName':ref('AWS::StackName'),'PublicKeyMaterial':ref('PublicKeyMaterial'),'Tags':tags}}
    def instance(role,kind,size,init,profile=False):
        properties=dict(ImageId=ref('Ami'),InstanceType=kind,KeyName={'Fn::If':['CreateOwnedKey',ref('OwnedKeyPair'),ref('KeyName')]},
            NetworkInterfaces=[{'DeviceIndex':'0','SubnetId':ref('Subnet'),'AssociatePublicIpAddress':profile,'GroupSet':[ref('LabSecurityGroup')]}],
            BlockDeviceMappings=[{'DeviceName':'/dev/sda1','Ebs':{'VolumeSize':size,'VolumeType':'gp3','Iops':3000,
                'DeleteOnTermination':True,'Encrypted':True}}],
            MetadataOptions={'HttpTokens':'required','HttpEndpoint':'enabled'},
            Tags=tags+[{'Key':'NodeRole','Value':role}],UserData={'Fn::Base64':sub(init)})
        if profile:properties['IamInstanceProfile']=ref('HeadProfile')
        return {'Type':'AWS::EC2::Instance','DependsOn':['InternetRoute','RouteAssociation'],'Properties':properties}
    resources['Head']=instance('head','m7i.xlarge',40,head_init,True)
    for name,role in [('Worker1','worker-1'),('Worker2','worker-2')]:
        init="#!/bin/bash\nset -euo pipefail\nexec > >(tee /var/log/witw-worker-init.log) 2>&1\npython3 - <<'PY'\nimport time,urllib.request,subprocess\nurl='http://${Head.PrivateIp}:8090/node_bootstrap.py'\nfor _ in range(720):\n try:\n  data=urllib.request.urlopen(url,timeout=10).read()\n  open('/tmp/witw-node-bootstrap.py','wb').write(data)\n  subprocess.run(['python3','/tmp/witw-node-bootstrap.py','--role','ROLE','--head','${Head.PrivateIp}','--experiment','${AWS::StackName}'],check=True)\n  break\n except Exception as e:\n  print(type(e).__name__,str(e),flush=True);time.sleep(10)\nelse:raise RuntimeError('Private bootstrap did not become ready within two hours')\nPY\n"
        resources[name]=instance(role,'m7i.large',20,init.replace('ROLE',role))
    return dict(AWSTemplateFormatVersion='2010-09-09',Description='Disposable WITW A/B partitioning experiment; fixed 4 executor cores',
        Parameters={'Ami':{'Type':'AWS::SSM::Parameter::Value<AWS::EC2::Image::Id>',
            'Default':'/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id'},
            'KeyName':{'Type':'String','Default':''},'PublicKeyMaterial':{'Type':'String','Default':''},
            'SshCidr':{'Type':'String','AllowedPattern':r'(\d{1,3}\.){3}\d{1,3}/32','Description':'Only controller public IPv4 /32'},
            'BudgetHours':{'Type':'Number','Default':10,'MinValue':1,'MaxValue':20}},
        Conditions={'CreateOwnedKey':{'Fn::Not':[{'Fn::Equals':[ref('PublicKeyMaterial'),'']}]}},
        Resources=resources,Outputs={'HeadPublicIp':{'Value':get('Head','PublicIp')},'HeadPrivateIp':{'Value':get('Head','PrivateIp')},
            'Worker1PrivateIp':{'Value':get('Worker1','PrivateIp')},'Worker2PrivateIp':{'Value':get('Worker2','PrivateIp')},
            'HeadId':{'Value':ref('Head')},'Worker1Id':{'Value':ref('Worker1')},'Worker2Id':{'Value':ref('Worker2')},
            'AvailabilityZone':{'Value':get('Head','AvailabilityZone')}})


if __name__=='__main__':
    path=Path(__file__).with_name('cloudformation.json')
    path.write_text(json.dumps(template(),indent=2));print(path)
