"""Independent post-run gate: pairing, counts, resources, event attribution, source parity."""
import argparse
import collections
import csv
import hashlib
import json
import re
from pathlib import Path
from event_metrics import parse


def read_csv(path):
    with Path(path).open(encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))


def validate(root):
    status = json.loads((root/'SUCCESS.json').read_text())
    assert status['complete'] and status['equal']
    env = json.loads((root/'environment.json').read_text())
    assert env['master'].startswith('spark://'), 'Separate executor process required'
    assert env['driver']==env['executors'][0]
    assert all(w==env['driver'] for w in env['executors'])
    assert env['params']==dict(min_cluster_size=8,min_samples=4,representative_count=4,
                              min_membership_confidence=.8,min_centroid_similarity=.92)
    records = read_csv(root/'runs.csv')
    measured = [r for r in records if r['phase'] in ('calibration','holdout')]
    assert len(measured)==status['measured_runs']
    groups = collections.defaultdict(list)
    conditions = collections.Counter()
    for r in records:
        assert r['equal']=='True' and r['digest']==r['reference_digest']
        assert float(r['wall_s'])>0 and float(r['route_s'])>=0
        assert int(r['assigned'])+int(r['noise_or_filtered'])==int(r['selected_articles'])
        assert abs(float(r['epoch_end'])-float(r['epoch_start'])-float(r['wall_s']))<.1
        groups[(r['phase'],r['scenario'],r['seed'],r['rep'])].append(r)
        if r['phase'] in ('calibration','holdout'):
            conditions[(r['phase'],r['scenario'],r['mode'])] += 1
    for key, rr in groups.items():
        modes={'as_is','control','to_be_1'}|({'to_be_2'} if key[0]=='holdout' else set())
        assert {r['mode'] for r in rr}==modes and len(rr)==len(modes)
        assert len({(r['input_digest'],r['selected_digest'],r['digest']) for r in rr})==1
        assert {int(r['order']) for r in rr}==set(range(len(modes)))
    assert len(conditions)==42, 'Six calibration and six holdout profiles expected'
    assert all(n==status['required_repetitions'] for n in conditions.values())
    assert any(int(r['clusters'])>0 and int(r['noise_or_filtered'])>0 for r in measured)
    if status['quick']:
        edge=[r for r in records if r['phase']=='edge']
        assert len(edge)==3 and all(int(r['selected_articles'])==2025 for r in edge)
    cal_seeds={r['seed'] for r in records if r['phase']=='calibration'}
    test_seeds={r['seed'] for r in records if r['phase']=='holdout'}
    assert not cal_seeds.intersection(test_seeds)
    assert json.loads((root/'policy.json').read_text())['holdout_used'] is False
    event_rows = parse(root)
    events={r['group']:r for r in event_rows}
    console=root.parent/(root.name+'.console.log')
    oversized_timed_tasks=0
    if console.exists():
        stage_groups={r['stage_id']:r['group'] for r in read_csv(root/'stages.csv')}
        warned=re.findall(r'Stage (\d+) contains a task of very large size',console.read_text(encoding='utf-8-sig'))
        oversized_timed_tasks=sum(not stage_groups.get(s,'unknown').startswith('prepare-') for s in warned)
        if env.get('input_backend')=='untimed_parquet_then_cached_dataframe':
            assert oversized_timed_tasks==0, 'Synthetic literal task payload leaked into measured execution'
    manifest=root.parent/(root.name+'.containers.json')
    if manifest.exists():
        containers=json.loads(manifest.read_text(encoding='utf-8-sig'))
        limits={'driver':(10**9,3*2**30),'worker':(env['cores']*10**9,6*2**30),'master':(5*10**8,512*2**20)}
        assert len({c['Image'] for c in containers})==1
        for c in containers:
            role=c['Config']['Labels']['com.docker.compose.service']
            assert (c['HostConfig']['NanoCpus'],c['HostConfig']['Memory'])==limits[role]
            for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
                assert key+'=1' in c['Config']['Env']
    for r in records:
        e=events[r['group']]
        assert e['jobs']>=1 and e['tasks']>=1
        assert e['failed_tasks']==0
    allocated=[]
    for path in (root/'spark-events').rglob('*'):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):
            continue
        for line in path.open(encoding='utf-8'):
            e=json.loads(line)
            if e.get('Event')=='SparkListenerExecutorAdded':
                allocated.append(e['Executor Info']['Total Cores'])
    assert allocated==[env['cores']], f'Unexpected allocation {allocated}'
    samples={}
    for role in ('driver','worker'):
        rr=read_csv(root/f'resources-{role}.csv')
        assert len(rr)>2
        assert float(rr[0]['epoch_s'])<=min(float(r['epoch_start']) for r in measured)
        assert float(rr[-1]['epoch_s'])>=max(float(r['epoch_end']) for r in measured)
        assert float(rr[-1]['cpu_usage_s'])>float(rr[0]['cpu_usage_s'])
        samples[role]=len(rr)
    checks=dict(passed=True,measured_runs=len(measured),conditions=len(conditions),
                repetitions_per_condition=status['required_repetitions'],allocated_executor_cores=allocated,
                all_results_equal=True,resource_samples=samples,calibration_holdout_disjoint=True,
                event_groups_complete=True,oversized_timed_tasks=oversized_timed_tasks)
    (root/'VALIDATED.json').write_text(json.dumps(checks,indent=2))
    print(json.dumps(checks))
    return checks


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('result_directory')
    validate(Path(p.parse_args().result_directory))
