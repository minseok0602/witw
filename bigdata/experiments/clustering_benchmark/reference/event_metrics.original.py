"""Aggregate raw Spark event logs by job group; all task durations are sums, not wall time."""
import argparse,csv,json,collections,statistics
from pathlib import Path

def main():
    p=argparse.ArgumentParser();p.add_argument('result_directory');a=p.parse_args();root=Path(a.result_directory);out=[]
    for path in sorted((root/'spark-events').rglob('*')):
        if not path.is_file() or path.name.startswith('.') or path.name.startswith('appstatus'):continue
        stage_group={};metrics=collections.defaultdict(lambda:collections.defaultdict(float));durations=collections.defaultdict(list)
        try:
            with path.open(encoding='utf-8') as f:
                for line in f:
                    try:e=json.loads(line)
                    except (ValueError,UnicodeError):continue
                    if e.get('Event')=='SparkListenerJobStart':
                        group=(e.get('Properties') or {}).get('spark.jobGroup.id','ungrouped')
                        metrics[group]['jobs']+=1
                        for st in e.get('Stage IDs',[]):stage_group[st]=group
                    if e.get('Event')!='SparkListenerTaskEnd':continue
                    group=stage_group.get(e.get('Stage ID'),'ungrouped');m=metrics[group];t=e.get('Task Metrics') or {};i=e.get('Task Info') or {};elapsed=i.get('Finish Time',0)-i.get('Launch Time',0)
                    m['tasks']+=1; m['failed_tasks']+=int((e.get('Task End Reason') or {}).get('Reason')!='Success');m['task_duration_sum_ms']+=elapsed;durations[group].append(elapsed)
                    for name,key in [('executor_run_ms','Executor Run Time'),('executor_cpu_ns','Executor CPU Time'),('deserialize_ms','Executor Deserialize Time'),('serialize_result_ms','Result Serialization Time'),('gc_ms','JVM GC Time'),('result_bytes','Result Size'),('memory_spill_bytes','Memory Bytes Spilled'),('disk_spill_bytes','Disk Bytes Spilled')]:m[name]+=t.get(key,0) or 0
                    r=t.get('Shuffle Read Metrics') or {};w=t.get('Shuffle Write Metrics') or {};m['shuffle_read_bytes']+=r.get('Remote Bytes Read',0)+r.get('Local Bytes Read',0);m['shuffle_write_bytes']+=w.get('Shuffle Bytes Written',0);m['shuffle_fetch_wait_ms']+=r.get('Fetch Wait Time',0)
        except UnicodeError:continue
        for group,m in metrics.items():
            v=sorted(durations[group]);out.append(dict(event_log=path.name,group=group,**m,task_median_ms=statistics.median(v) if v else 0,task_max_ms=max(v,default=0)) )
    if not out:raise SystemExit('No task events found')
    fields=list(dict.fromkeys(k for r in out for k in r));
    with (root/'event_metrics.csv').open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(out)
if __name__=='__main__':main()
