import argparse
import collections
import csv
import json
import re
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from event_metrics import parse, write_csv


def read(path):
    with Path(path).open(encoding='utf-8-sig',newline='') as f:
        return list(csv.DictReader(f))


def validate(root):
    status=json.loads((root/'SUCCESS.json').read_text())
    env=json.loads((root/'environment.json').read_text())
    assert status['complete'] and status['equal'] and not status['full_experiment_run']
    assert env['no_dynamic_driver_executor_routing']
    assert all(r==env['driver'] for r in env['executors'])
    assert env['master'].startswith('spark://')
    allocated=[]
    for path in (root/'spark-events').rglob('*'):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):
            continue
        for line in path.open(encoding='utf-8'):
            event=json.loads(line)
            if event.get('Event')=='SparkListenerExecutorAdded':
                allocated.append(event['Executor Info']['Total Cores'])
    assert allocated==[env['cores']], f'Unexpected actual Executor allocation: {allocated}'
    records=read(root/'runs.csv');parts=read(root/'partition_runs.csv')
    measured=[r for r in records if r['phase']!='warmup']
    assert len(measured)==status['measured_runs']==(98 if env['cores']==4 else 90)
    events={r['group']:r for r in parse(root)}
    tasks=read(root/'tasks.csv');task_by_id={int(r['task_id']):r for r in tasks}
    for r in records:
        assert r['equal']=='True' and r['digest']==r['reference_digest']
        assert int(r['assigned'])+int(r['noise_or_filtered'])==int(r['selected_articles'])
        assert abs(float(r['epoch_end'])-float(r['epoch_start'])-float(r['wall_s']))<.1
        assert events[r['group']]['failed_tasks']==0
        if r['mode'].startswith('to_be_'):
            pp=[p for p in parts if p['group']==r['group']]
            assert len(pp)==int(r['partitions'])
            assert sorted(int(p['partition']) for p in pp)==list(range(int(r['partitions'])))
            assert len({p['task_id'] for p in pp})==len(pp)
            counts=json.loads(r['counts'])
            seen=[]
            for p in pp:
                tt=task_by_id[int(p['task_id'])]
                assert tt['group']==r['group'] and int(tt['task_index'])==int(p['partition'])
                assert int(tt['stage_id'])==int(r['cluster_stage_id'])
                seen += p['themes'].split(';') if p['themes'] else []
            assert len(seen)==len(set(seen))==len(counts) and set(seen)==set(counts)
            tt=[t for t in tasks if t['group']==r['group'] and t['stage_id']==r['cluster_stage_id']]
            assert len(tt)==int(r['partitions']), 'Clustering stage must have P actual tasks, including empty partitions'
    for scenario in ('single','balanced4','moderate8','severe16','balanced32'):
        rr=[r for r in measured if r['phase']=='mini' and r['scenario']==scenario]
        assert len(rr)==17
        assert {r['mode'] for r in rr}=={'as_is','control','to_be_1','to_be_2','to_be_3'}
        assert len({(r['input_digest'],r['digest']) for r in rr})==1
        for mode in ('to_be_1','to_be_2','to_be_3'):
            assert {int(r['partitions']) for r in rr if r['mode']==mode}=={1,2,4,8,16}
    singles=[r for r in measured if r['scenario']=='single' and r['mode'].startswith('to_be_')]
    assert all(int(r['empty_partitions'])==int(r['partitions'])-1 for r in singles)
    assert any(int(r['clusters'])>0 and int(r['noise_or_filtered'])>0 for r in measured)
    model=json.loads((root/'cost_model.json').read_text())
    assert model['test_data_used'] is False
    assert not set(model['calibration_seeds']).intersection(int(r['seed']) for r in records)
    if env['cores']==4:
        edge=[r for r in measured if r['scenario']=='cap-and-ineligible']
        assert len(edge)==5 and all(int(r['selected_articles'])==2025 for r in edge)
        collision=[r for r in measured if r['scenario']=='hash-collision']
        assert next(int(r['empty_partitions']) for r in collision if r['mode']=='to_be_1')==3
        assert all(int(r['empty_partitions'])==0 for r in collision if r['mode']!='to_be_1')
    console=root.parent/(root.name+'.console.log')
    stage_groups={r['stage_id']:r['group'] for r in read(root/'stages.csv')}
    warned=re.findall(r'Stage (\d+) contains a task of very large size',console.read_text(encoding='utf-8-sig'))
    assert all(stage_groups.get(s,'unknown').startswith('prepare-') for s in warned)
    containers=json.loads((root.parent/(root.name+'.containers.json')).read_text(encoding='utf-8-sig'))
    limits={'driver':(10**9,3*2**30),'worker':(env['cores']*10**9,6*2**30),'master':(5*10**8,512*2**20)}
    assert len({c['Image'] for c in containers})==1
    for c in containers:
        role=c['Config']['Labels']['com.docker.compose.service']
        assert (c['HostConfig']['NanoCpus'],c['HostConfig']['Memory'])==limits[role]
    for role in ('driver','worker'):
        samples=read(root/f'resources-{role}.csv')
        assert float(samples[0]['epoch_s'])<=min(float(r['epoch_start']) for r in measured)
        assert float(samples[-1]['epoch_s'])>=max(float(r['epoch_end']) for r in measured)
    summary=dict(passed=True,design=env['design'],cores=env['cores'],measured_runs=len(measured),
                 all_results_equal=True,whole_theme_placement=True,actual_clustering_tasks_equal_partitions=True,
                 allocated_executor_cores=allocated,
                 independent_cost_calibration=True,resource_limits_verified=True,
                 performance_conclusions_allowed=False,full_experiment_run=False)
    (root/'VALIDATED.json').write_text(json.dumps(summary,indent=2))
    print(json.dumps(summary))
    return summary


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('directory',type=Path)
    validate(p.parse_args().directory)
