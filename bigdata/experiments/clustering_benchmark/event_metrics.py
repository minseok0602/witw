"""Parse uncompressed Spark event logs without treating task sums as elapsed time."""
import argparse
import collections
import csv
import json
import statistics
from pathlib import Path
def write_csv(path, rows):
    if not rows:
        return
    with Path(path).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(dict.fromkeys(k for r in rows for k in r)))
        w.writeheader()
        w.writerows(rows)


def parse(root):
    stage_groups = {}
    stage_attempts = {}
    metrics = collections.defaultdict(lambda:collections.defaultdict(float))
    durations = collections.defaultdict(list)
    tasks, stages, jobs, executor_metrics = [], [], [], []
    stage_task_durations=collections.defaultdict(list)
    for path in sorted((root/'spark-events').rglob('*')):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):
            continue
        with path.open(encoding='utf-8') as f:
            for line in f:
                try:
                    e = json.loads(line)
                except (ValueError,UnicodeError):
                    continue
                kind = e.get('Event')
                if kind=='SparkListenerJobStart':
                    group = (e.get('Properties') or {}).get('spark.jobGroup.id','ungrouped')
                    metrics[group]['jobs'] += 1
                    jobs.append(dict(group=group,job_id=e['Job ID'],submission_ms=e.get('Submission Time'),
                                     completion_ms=None,result=None))
                    for sid in e.get('Stage IDs',[]):
                        stage_groups[sid] = group
                elif kind=='SparkListenerJobEnd':
                    for j in reversed(jobs):
                        if j['job_id']==e['Job ID']:
                            j.update(completion_ms=e.get('Completion Time'),result=(e.get('Job Result') or {}).get('Result'))
                            break
                elif kind=='SparkListenerStageSubmitted':
                    info = e['Stage Info']
                    key = (info['Stage ID'],info.get('Stage Attempt ID',0))
                    group = (e.get('Properties') or {}).get('spark.jobGroup.id',stage_groups.get(key[0],'ungrouped'))
                    stage_groups[key[0]] = group
                    stage_attempts[key] = group
                elif kind=='SparkListenerStageCompleted':
                    info = e['Stage Info']
                    key = (info['Stage ID'],info.get('Stage Attempt ID',0))
                    group = stage_attempts.get(key,stage_groups.get(key[0],'ungrouped'))
                    stages.append(dict(group=group,stage_id=key[0],attempt=key[1],tasks=info.get('Number of Tasks'),
                        submission_ms=info.get('Submission Time'),completion_ms=info.get('Completion Time'),
                        failure=info.get('Failure Reason','')))
                elif kind=='SparkListenerStageExecutorMetrics':
                    sid,attempt=e.get('Stage ID'),e.get('Stage Attempt ID',0)
                    values=e.get('Executor Metrics',{})
                    peak=dict(group=stage_attempts.get((sid,attempt),stage_groups.get(sid,'ungrouped')),
                              stage_id=sid,stage_attempt=attempt,executor=e.get('Executor ID'),metrics_json=json.dumps(values))
                    for key in ('JVMHeapMemory','JVMOffHeapMemory','ProcessTreeJVMRSSMemory','ProcessTreePythonRSSMemory'):
                        peak[key]=values.get(key,0)
                    executor_metrics.append(peak)
                if kind!='SparkListenerTaskEnd':
                    continue
                sid,attempt = e.get('Stage ID'),e.get('Stage Attempt ID',0)
                group = stage_attempts.get((sid,attempt),stage_groups.get(sid,'ungrouped'))
                t,i = e.get('Task Metrics') or {}, e.get('Task Info') or {}
                elapsed = i.get('Finish Time',0)-i.get('Launch Time',0)
                row = dict(group=group,stage_id=sid,stage_attempt=attempt,task_id=i.get('Task ID'),
                    task_index=i.get('Index'),task_attempt=i.get('Attempt'),executor=i.get('Executor ID'),
                    host=i.get('Host'),launch_ms=i.get('Launch Time'),finish_ms=i.get('Finish Time'),duration_ms=elapsed,
                    failed=int((e.get('Task End Reason') or {}).get('Reason')!='Success'))
                for name,key in [('executor_run_ms','Executor Run Time'),('executor_cpu_ns','Executor CPU Time'),
                    ('deserialize_ms','Executor Deserialize Time'),('deserialize_cpu_ns','Executor Deserialize CPU Time'),
                    ('serialize_result_ms','Result Serialization Time'),('gc_ms','JVM GC Time'),
                    ('result_bytes','Result Size'),('memory_spill_bytes','Memory Bytes Spilled'),
                    ('disk_spill_bytes','Disk Bytes Spilled'),('peak_execution_memory_bytes','Peak Execution Memory')]:
                    row[name] = t.get(key,0) or 0
                r,w = t.get('Shuffle Read Metrics') or {},t.get('Shuffle Write Metrics') or {}
                row.update(shuffle_read_bytes=r.get('Remote Bytes Read',0)+r.get('Local Bytes Read',0),
                           shuffle_write_bytes=w.get('Shuffle Bytes Written',0),
                           shuffle_fetch_wait_ms=r.get('Fetch Wait Time',0),
                           shuffle_write_ns=w.get('Shuffle Write Time',0))
                getting_result = i.get('Getting Result Time',0)
                row['task_type'] = e.get('Task Type','unknown')
                row['getting_result_ms'] = max(0,i.get('Finish Time',0)-getting_result) if getting_result else 0
                row['scheduler_residual_ms'] = max(0,elapsed-row['executor_run_ms']-row['deserialize_ms']
                                                  -row['serialize_result_ms']-row['getting_result_ms'])
                tasks.append(row)
                stage_task_durations[(sid,attempt)].append(elapsed)
                m = metrics[group]
                m['tasks'] += 1
                m['failed_tasks'] += row['failed']
                m['task_duration_sum_ms'] += elapsed
                durations[group].append(elapsed)
                for k in list(row)[12:]:
                    if isinstance(row[k],(float,int)):
                        if k=='peak_execution_memory_bytes':
                            m[k]=max(m[k],row[k])
                        else:
                            m[k] += row[k]
    out = []
    for group,m in metrics.items():
        for key in ('jobs','tasks','failed_tasks'):
            m.setdefault(key,0)
        v = durations[group]
        avg = statistics.mean(v) if v else 0
        out.append(dict(group=group,**m,task_median_ms=statistics.median(v) if v else 0,
                        task_max_ms=max(v,default=0),task_max_mean_ratio=max(v)/avg if avg else 0,
                        task_cv=statistics.pstdev(v)/avg if avg else 0))
    if not tasks:
        raise RuntimeError('No task events found')
    for stage in stages:
        tt=stage_task_durations[(stage['stage_id'],stage['attempt'])]
        avg=statistics.mean(tt) if tt else 0
        stage['duration_ms']=(stage['completion_ms']-stage['submission_ms']) if stage['completion_ms'] is not None and stage['submission_ms'] is not None else None
        stage['task_max_mean_ratio']=max(tt)/avg if avg else 0
        stage['task_cv']=statistics.pstdev(tt)/avg if avg else 0
        stage['task_duration_sum_ms']=sum(tt)
        stage['task_max_ms']=max(tt,default=0)
    write_csv(root/'event_metrics.csv',out)
    write_csv(root/'tasks.csv',tasks)
    write_csv(root/'stages.csv',stages)
    write_csv(root/'jobs.csv',jobs)
    write_csv(root/'executor_metrics.csv',executor_metrics)
    return out


if __name__=='__main__':
    p = argparse.ArgumentParser()
    p.add_argument('result_directory')
    parse(Path(p.parse_args().result_directory))
