"""Run only on newly approved EC2 nodes. Worker bootstrap uses private Head HTTP, no NAT/public IPv4."""
import argparse
import gzip
import hashlib
import json
import os
import shutil
import socket
import subprocess
import tarfile
import time
import urllib.request
from pathlib import Path

ROOT=Path('/opt/witw')
DOCKER_VERSION='28.5.1'
DOCKER_URL=f'https://download.docker.com/linux/static/stable/x86_64/docker-{DOCKER_VERSION}.tgz'
IMAGE='witw-spark-partitioning:ec2'
THREADS=['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS']


def run(args,**kwargs):
    print(' '.join(map(str,args)),flush=True)
    return subprocess.run([str(a) for a in args],check=True,**kwargs)


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def download(url,path):
    with urllib.request.urlopen(url,timeout=180) as source,Path(path).open('wb') as target:shutil.copyfileobj(source,target)


def extract(archive,target):
    target=Path(target).resolve();target.mkdir(parents=True,exist_ok=True)
    with tarfile.open(archive,'r:*') as tar:
        for member in tar.getmembers():
            candidate=(target/member.name).resolve()
            if not candidate.is_relative_to(target) or member.issym() or member.islnk():raise ValueError('Unsafe archive member')
        tar.extractall(target,filter='data')


def private_ip():
    request=urllib.request.Request('http://169.254.169.254/latest/api/token',method='PUT',headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    token=urllib.request.urlopen(request,timeout=5).read().decode()
    request=urllib.request.Request('http://169.254.169.254/latest/meta-data/local-ipv4',headers={'X-aws-ec2-metadata-token':token})
    return urllib.request.urlopen(request,timeout=5).read().decode()


def service(name,command):
    text='[Unit]\nAfter=network-online.target\n[Service]\nExecStart='+command+'\nRestart=on-failure\n[Install]\nWantedBy=multi-user.target\n'
    Path('/etc/systemd/system/'+name+'.service').write_text(text)
    run(['systemctl','daemon-reload']);run(['systemctl','enable','--now',name+'.service'])


def install_docker(archive):
    extract(archive,ROOT/'docker-dist')
    for file in (ROOT/'docker-dist/docker').iterdir():
        if file.is_file():shutil.copy2(file,Path('/usr/local/bin')/file.name)
    Path('/etc/docker').mkdir(exist_ok=True)
    Path('/etc/docker/daemon.json').write_text(json.dumps({'iptables':False,'bridge':'none','ip-forward':False}))
    service('docker','/usr/local/bin/dockerd')
    for _ in range(60):
        if subprocess.run(['docker','info'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:return
        time.sleep(2)
    raise RuntimeError('Docker did not start')


def docker_base(experiment):
    return ['docker','run','-d','--network','host','--user','root','--label','witw.experiment='+experiment,
            '-v',str(ROOT/'workspace/bigdata/experiments/spark_partitioning')+':/experiment:ro',
            '-v',str(ROOT/'workspace/bigdata/spark/apps')+':/source:ro','-v',str(ROOT/'results')+':/results']


def record_environment(role,ip):
    info=dict(role=role,private_ip=ip,epoch=time.time(),docker_version=subprocess.check_output(['docker','version','--format','{{.Server.Version}}'],text=True).strip())
    for name,args in [('cpu',['lscpu']),('memory',['free','-b']),('disk',['df','-B1','/']),('ntp',['timedatectl','show','-p','NTPSynchronized'])]:
        info[name]=subprocess.run(args,capture_output=True,text=True).stdout
    (ROOT/'results'/f'node-{role}.json').write_text(json.dumps(info,indent=2))


def main():
    p=argparse.ArgumentParser();p.add_argument('--role',choices=['head','worker-1','worker-2'],required=True)
    p.add_argument('--experiment',required=True);p.add_argument('--head');p.add_argument('--source',type=Path)
    a=p.parse_args()
    if os.geteuid()!=0:raise PermissionError('Bootstrap requires root on disposable EC2 only')
    if not a.experiment.startswith('witw-partitioning-'):raise ValueError('Unexpected experiment prefix')
    for path in ['publish','results','state','workspace','tmp']:(ROOT/path).mkdir(parents=True,exist_ok=True)
    ip=private_ip()
    if a.role=='head':
        if not a.source:raise ValueError('Source archive required')
        extract(a.source,ROOT/'workspace')
        lab=ROOT/'workspace/bigdata/experiments/spark_partitioning'
        archive=ROOT/'publish/docker.tgz';download(DOCKER_URL,archive);install_docker(archive)
        environment=dict(os.environ,DOCKER_BUILDKIT='0')
        run(['docker','build','--network','host','-t','news-spark:4.2.0-kafka',str(ROOT/'workspace/bigdata/spark')],env=environment)
        run(['docker','build','--network','host','-t',IMAGE,'-f',str(lab/'Dockerfile'),str(lab)],env=environment)
        image_file=ROOT/'publish/image.tar.gz'
        with image_file.open('wb') as raw:
            proc=subprocess.Popen(['docker','save',IMAGE],stdout=subprocess.PIPE)
            with gzip.GzipFile(fileobj=raw,mode='wb') as compressed:shutil.copyfileobj(proc.stdout,compressed)
            if proc.wait()!=0:raise RuntimeError('Cannot archive Docker image')
        source_file=ROOT/'publish/source.tar.gz';shutil.copy2(a.source,source_file)
        manifest=dict(experiment=a.experiment,head_private_ip=ip,docker_version=DOCKER_VERSION,image=IMAGE,
            image_id=subprocess.check_output(['docker','image','inspect','--format','{{.Id}}',IMAGE],text=True).strip(),
            files={f.name:sha(f) for f in [archive,image_file,source_file]})
        # Publish bootstrap only after all artifacts and their hashes are ready.
        (ROOT/'publish/bootstrap.json').write_text(json.dumps(manifest,indent=2))
        shutil.copy2(__file__,ROOT/'publish/node_bootstrap.py')
        run(docker_base(a.experiment)+['--name','witw-lab-master','--restart','unless-stopped','--label','witw.role=master','--cpus','.5','--memory','512m',
            '-e','SPARK_DAEMON_MEMORY=256m',IMAGE,'/opt/spark/bin/spark-class','org.apache.spark.deploy.master.Master',
            '--host',ip,'--port','7077'])
    else:
        base='http://'+a.head+':8090/'
        for _ in range(720):
            try:
                manifest=json.loads(urllib.request.urlopen(base+'bootstrap.json',timeout=10).read());break
            except Exception:time.sleep(10)
        else:raise RuntimeError('Head artifacts unavailable')
        if manifest['experiment']!=a.experiment:raise ValueError('Wrong experiment manifest')
        for name,checksum in manifest['files'].items():
            if name not in ['docker.tgz','image.tar.gz','source.tar.gz']:raise ValueError('Unexpected artifact')
            destination=ROOT/'tmp'/name;download(base+name,destination)
            if sha(destination)!=checksum:raise ValueError('Artifact checksum mismatch: '+name)
        install_docker(ROOT/'tmp/docker.tgz');extract(ROOT/'tmp/source.tar.gz',ROOT/'workspace')
        run(['docker','load','-i',ROOT/'tmp/image.tar.gz'])
        actual=subprocess.check_output(['docker','image','inspect','--format','{{.Id}}',IMAGE],text=True).strip()
        if actual!=manifest['image_id']:raise ValueError('Worker image differs from Head')
        args=docker_base(a.experiment)+['--name','witw-lab-worker','--restart','unless-stopped','--label','witw.role='+a.role,'--cpus','2','--memory','4g','-e','SPARK_LOCAL_HOSTNAME='+ip,
            '-e','SPARK_DAEMON_MEMORY=256m']
        for key in THREADS:args+=['-e',key+'=1']
        run(args+[IMAGE,'python3','/experiment/apps/resource_monitor.py','--output','/results/resources-worker.csv','--',
            '/opt/spark/bin/spark-class','org.apache.spark.deploy.worker.Worker','spark://'+a.head+':7077',
            '--host',ip,'--cores','2','--memory','3g','--webui-port','8081'])
        (ROOT/'results/worker-container.json').write_bytes(subprocess.check_output(['docker','inspect','witw-lab-worker']))
        service('witw-results','/usr/bin/python3 -m http.server 8091 --bind 0.0.0.0 --directory /opt/witw/results')
        # Remove only downloaded temporary archives after successful load, not results or source.
        for name in ['docker.tgz','image.tar.gz','source.tar.gz']:(ROOT/'tmp'/name).unlink()
    lab=ROOT/'workspace/bigdata/experiments/spark_partitioning'
    service('witw-host-sampler','/usr/bin/python3 '+str(lab/'apps/resource_monitor.py')+' --output '+str(ROOT/'results'/('server-'+a.role+'.csv'))+' -- /usr/bin/sleep 604800')
    record_environment(a.role,ip)
    (ROOT/'results'/('READY-'+a.role+'.json')).write_text(json.dumps(dict(experiment=a.experiment,private_ip=ip,role=a.role)))


if __name__=='__main__':main()
