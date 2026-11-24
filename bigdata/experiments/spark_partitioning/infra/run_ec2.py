"""Run on the approved Head. The default prints a plan and performs no operations."""
import argparse
import csv
import io
import json
import subprocess
import time
import urllib.request
from pathlib import Path
from node_bootstrap import ROOT,IMAGE,THREADS,docker_base


def request(url):
    return urllib.request.urlopen(url,timeout=30).read()


def main():
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true')
    p.add_argument('--app',required=True,choices=['A','B']);p.add_argument('--mode',required=True,choices=['quick','explore','full'])
    p.add_argument('--scenarios',nargs='+',choices=['D1','D2','D3','D4','NATURAL','MODERATE','SKEW'],default=['D1','D2','D3','D4'])
    p.add_argument('--reps',type=int);p.add_argument('--rep-start',type=int,default=0);p.add_argument('--session',type=int,default=0)
    p.add_argument('--run-id');a=p.parse_args()
    print(json.dumps(dict(app=a.app,mode=a.mode,scenarios=a.scenarios,executor_count=2,executor_cores=2,total_cores=4,partitions=4,execute=a.execute)))
    if not a.execute:return
    cluster=json.loads((ROOT/'state/cluster.json').read_text());experiment=cluster['experiment']
    lab=ROOT/'workspace/bigdata/experiments/spark_partitioning'
    output_name=a.run_id or f'{a.app}-{a.mode}-{int(time.time())}'
    if '/' in output_name or '..' in output_name:raise ValueError('Invalid run ID')
    out=ROOT/'results'/output_name
    if out.exists():raise ValueError('Use a fresh run ID')
    for role,key in [('worker-1','Worker1PrivateIp'),('worker-2','Worker2PrivateIp')]:
        for _ in range(120):
            try:
                ready=json.loads(request('http://'+cluster[key]+':8091/READY-'+role+'.json'))
                assert ready['experiment']==experiment;break
            except Exception:time.sleep(5)
        else:raise RuntimeError('Worker not ready: '+role)
    for _ in range(90):
        try:
            master=json.loads(request('http://'+cluster['HeadPrivateIp']+':8080/json/'))
            live=[w for w in master['workers'] if w['state']=='ALIVE']
            if len(live)==2 and sorted(w['cores'] for w in live)==[2,2]:break
        except Exception:pass
        time.sleep(2)
    else:raise RuntimeError('Master does not expose exactly four worker cores')
    model=None;passed=[]
    for path in (ROOT/'results').glob(a.app+'-quick-*/VALIDATED.json'):
        validation=json.loads(path.read_text())
        environment=json.loads((path.parent/'environment.json').read_text())
        if validation['passed'] and environment['scenarios']==a.scenarios:passed.append(path.parent)
    if a.mode=='full':
        if not passed:raise RuntimeError('A validated EC2 Quick Test is required before full measurements')
    if a.app=='B' and passed:model=sorted(passed)[0]/'cost_model.json'
    name='witw-lab-driver'
    if subprocess.run(['docker','inspect',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
        raise RuntimeError('Previous driver container still exists; preserve and remove it before another run')
    ip=cluster['HeadPrivateIp']
    command=docker_base(experiment)+['--name',name,'--label','witw.role=driver','--cpus','1','--memory','3g','-e','SPARK_LOCAL_HOSTNAME='+ip]
    for key in THREADS:command+=['-e',key+'=1']
    command += [IMAGE,'python3','/experiment/apps/resource_monitor.py','--output','/results/'+output_name+'/resources-driver.csv','--',
        '/opt/spark/bin/spark-submit','--master','spark://'+ip+':7077','--driver-memory','1536m','--executor-memory','2g',
        '--conf','spark.cores.max=4','--conf','spark.executor.cores=2','--conf','spark.driver.host='+ip,
        '--conf','spark.driver.bindAddress=0.0.0.0','--conf','spark.driver.port=7078','--conf','spark.blockManager.port=7080',
        '--conf','spark.executor.processTreeMetrics.enabled=true','--conf','spark.executor.metrics.pollingInterval=250',
        '--conf','spark.eventLog.logStageExecutorMetrics=true','--conf','spark.metrics.conf=/experiment/conf/metrics.properties',
        '/experiment/apps/benchmark.py','--source','/source','--app',a.app,'--mode',a.mode,'--output','/results/'+output_name]
    command+=['--scenarios',*a.scenarios]
    if a.app=='B' and 'SKEW' not in a.scenarios:command+=['--include-baselines']
    if model:command+=['--cost-model','/results/'+model.parent.name+'/cost_model.json']
    if a.reps:command+=['--reps',str(a.reps)]
    command+=['--rep-start',str(a.rep_start),'--session',str(a.session)]
    subprocess.run(command,check=True)
    exit_code=subprocess.check_output(['docker','wait',name],text=True).strip()
    time.sleep(1)
    with (ROOT/'results'/(output_name+'.console.log')).open('wb') as f:
        subprocess.run(['docker','logs',name],stdout=f,stderr=subprocess.STDOUT)
    containers=json.loads(subprocess.check_output(['docker','inspect',name,'witw-lab-master']))
    for role,key in [('worker-1','Worker1PrivateIp'),('worker-2','Worker2PrivateIp')]:
        base='http://'+cluster[key]+':8091/'
        (out/('resources-'+role+'.csv')).write_bytes(request(base+'resources-worker.csv'))
        (out/('server-'+role+'.csv')).write_bytes(request(base+'server-'+role+'.csv'))
        (out/('node-'+role+'.json')).write_bytes(request(base+'node-'+role+'.json'))
        containers+=json.loads(request(base+'worker-container.json'))
    (ROOT/'results'/(output_name+'.containers.json')).write_text(json.dumps(containers,indent=2))
    (ROOT/'results'/(output_name+'.final-state.json')).write_text(json.dumps(containers,indent=2))
    subprocess.run(['docker','rm',name],check=True)
    if exit_code!='0':raise RuntimeError('Failed run preserved: '+output_name)
    subprocess.run(['python3',str(lab/'apps/validate.py'),str(out)],check=True)
    print(json.dumps(dict(validated_run=output_name,full_main_runs=3*len(a.scenarios)*(a.reps or 30) if a.mode=='full' else None)))


if __name__=='__main__':main()
