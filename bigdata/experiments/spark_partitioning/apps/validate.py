import argparse
import collections
import csv
import json
import re
from pathlib import Path
from event_metrics import parse


def read(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f:return list(csv.DictReader(f))


def validate(root):
    status=json.loads((root/'SUCCESS.json').read_text());env=json.loads((root/'environment.json').read_text())
    assert status['complete'] and status['equal'] and status['failures']==0
    assert len({r['host'] for r in env['executors']})==2
    allocation=[];removed=[]
    for path in (root/'spark-events').rglob('*'):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):continue
        for line in path.open(encoding='utf-8'):
            e=json.loads(line)
            if e.get('Event')=='SparkListenerExecutorAdded':allocation.append(e['Executor Info']['Total Cores'])
            if e.get('Event')=='SparkListenerExecutorRemoved':removed.append(e)
    assert sorted(allocation)==[2,2],f'Actual allocation must be two executors with two cores each: {allocation}'
    assert not removed,'Executor was lost or replaced'
    manifest=root.parent/(root.name+'.containers.json')
    if manifest.exists():
        containers=json.loads(manifest.read_text(encoding='utf-8-sig'))
        limits={'driver':(10**9,3*2**30),'worker-1':(2*10**9,4*2**30),
                'worker-2':(2*10**9,4*2**30),'master':(5*10**8,512*2**20)}
        assert len({c['Image'] for c in containers})==1
        for c in containers:
            labels=c['Config'].get('Labels') or {}
            role=labels.get('witw.role') or labels.get('com.docker.compose.service')
            assert (c['HostConfig']['NanoCpus'],c['HostConfig']['Memory'])==limits[role]
            for key in ['OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS']:
                assert key+'=1' in c['Config']['Env']
    rows=read(root/'runs.csv');parts=read(root/'partition_runs.csv')
    plans={p['group']:p for p in json.loads((root/'plans.json').read_text())} if (root/'plans.json').exists() else {}
    measured=[r for r in rows if r['phase']!='warmup']
    assert len(measured)==status['measured_runs']==len(env['scenarios'])*env['reps']*len(status['modes'])
    for r in rows:
        assert r['equal']=='True' and r['reference_sha256']==r['result_sha256']
        assert int(r['assigned'])+int(r['remaining'])==int(r['n'])
        assert float(r['wall_s'])>0 and float(r['plan_s'])>=0 and float(r['extra_aggregate_s'])>=0
        if r['phase']!='warmup':assert int(r['n'])==(4000 if r['app']=='A' else 2000)
    blocks=collections.defaultdict(list)
    semantic_blocks=collections.defaultdict(set)
    for r in read(root/'inputs.csv'):
        if r.get('logical_input_sha256') and r['scenario'] in ['NATURAL','MODERATE','SKEW']:
            semantic_blocks[(r['phase'],r['seed'])].add(r['logical_input_sha256'])
    assert all(len(values)==1 for values in semantic_blocks.values()),'Logical input changed between placement conditions'
    for r in measured:blocks[(r['scenario'],r['seed'],r['rep'])].append(r)
    for block in blocks.values():
        assert len(block)==len(status['modes'])
        assert {r['mode'] for r in block}==set(status['modes'])
        assert len({(r['input_sha256'],r['result_sha256']) for r in block})==1
        assert {int(r['order']) for r in block}==set(range(len(block)))
    events={r['group']:r for r in parse(root)}
    tasks=read(root/'tasks.csv');task_ids={int(r['task_id']):r for r in tasks}
    runtime_failures=dict(failed_tasks=sum(int(float(e['failed_tasks'])) for e in events.values()),
                          retry_attempts=sum(int(t['task_attempt'] or 0)>0 for t in tasks),executor_removals=len(removed))
    (root/'RUNTIME_FAILURES.json').write_text(json.dumps(runtime_failures,indent=2))
    for r in rows:
        assert events[r['group']]['failed_tasks']==0
        pp=[p for p in parts if p['group']==r['group']]
        if int(r['cluster_stage_id'])<0:
            assert not pp
            continue
        assert len(pp)==4 and {int(p['partition']) for p in pp}=={0,1,2,3}
        assert sum(int(p['n']) for p in pp)==int(r['n'])
        if r['phase']!='warmup' and r['scenario'] in ['MODERATE','SKEW']:
            if r['mode']=='A0':
                assert all(int(p['n'])==1000 for p in pp)
                loads=[float(p['predicted_cost']) for p in pp]
                expected=[15000,40000,90000,175000] if r['scenario']=='SKEW' else [41000,56000,86000,137000]
                assert sorted(round(x/768) for x in loads)==expected
            elif r['mode']=='B0':
                expected=[150,150,200,1500] if r['scenario']=='SKEW' else [200,300,650,850]
                assert sorted(int(p['n']) for p in pp)==expected
        tt=[t for t in tasks if t['group']==r['group'] and t['stage_id']==r['cluster_stage_id']]
        assert len(tt)==4
        for p in pp:
            t=task_ids[int(p['task_id'])]
            assert t['group']==r['group'] and t['task_index']==p['partition']
        if r['app']=='B':
            seen=[]
            for p in pp:seen+=list(json.loads(p['themes']))
            assert len(seen)==len(set(seen)),'A theme was split across tasks'
        elif r['mode']=='A1':
            assert max(int(p['n']) for p in pp)-min(int(p['n']) for p in pp)<=1
        elif r['mode']=='A2' and plans:
            quotas=plans[r['group']]['article_quotas']
            for p in pp:
                pid=int(p['partition']);actual=json.loads(p['themes'])
                for theme,cumulative in quotas.items():
                    expected=cumulative[pid]-(cumulative[pid-1] if pid else 0)
                    assert int(actual.get(theme,0))==expected,'Article cost quota was not applied to the destination partition'
    if env['app']=='A':
        expected={r['group']:int(r['n']) for r in rows}
        completed={};current=None;ids=set();count=0
        with (root/'matching_results.csv').open(encoding='utf-8',newline='') as file:
            for result in csv.DictReader(file):
                if current is not None and result['group']!=current:
                    assert len(ids)==count==expected[current]
                    completed[current]=count;ids=set();count=0
                current=result['group'];ids.add(result['article_id']);count+=1
        if current is not None:
            assert len(ids)==count==expected[current];completed[current]=count
        assert set(completed)==set(expected)
    else:
        model=json.loads((root/'cost_model.json').read_text())
        assert model['benchmark_holdout_used'] is False
        assert not set(model['calibration_seeds']).intersection(int(r['seed']) for r in rows)
    stages={s['stage_id']:s['group'] for s in read(root/'stages.csv')}
    console=root.parent/(root.name+'.console.log')
    warnings=re.findall(r'Stage (\d+) contains a task of very large size',console.read_text(encoding='utf-8-sig'))
    assert all(stages.get(s,'unknown').startswith('prepare-') for s in warnings), 'Synthetic payload in timed stage'
    for role in ('driver','worker-1','worker-2'):
        samples=read(root/f'resources-{role}.csv')
        assert float(samples[0]['epoch_s'])<=min(float(r['epoch_start']) for r in measured)
        assert float(samples[-1]['epoch_s'])>=max(float(r['epoch_end']) for r in measured)
    summary=dict(passed=True,app=env['app'],mode=env['mode'],measured_runs=len(measured),
        allocation=allocation,target_stage_partitions=4,all_results_equal=True,failed_tasks=0,
        dynamic_routing=False,oversized_timed_task_warnings=0,performance_conclusions_allowed=env['mode']=='full' and env['reps']>=30)
    (root/'VALIDATED.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary))
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path);validate(p.parse_args().directory)
