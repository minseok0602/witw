"""Mini correctness test for whole-theme partitioning, never dynamically routes execution."""
import os
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
    os.environ[key]='1'
import argparse
import collections
import hashlib
import json
import random
import shutil
import statistics
import sys
import time
from pathlib import Path
from functools import partial
import numpy as np
from kernel import CAP,PARAMS,fixture,kernel,fingerprint,runtime
from placement import placement,predicted_cost

MODES=['as_is','control','to_be_1','to_be_2','to_be_3']
PARTITIONS=[1,2,4,8,16]
PROFILES={'single':[96],'balanced4':[32]*4,'moderate8':[96]+[24]*7,
          'severe16':[480]+[16]*15,'balanced32':[25]*32}


def write_csv(path,rows):
    import csv
    if not rows:
        return
    with Path(path).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(dict.fromkeys(k for r in rows for k in r)))
        w.writeheader();w.writerows(rows)


def partition_kernel(pid,groups):
    from pyspark import TaskContext
    ctx=TaskContext.get()
    t,cpu=time.perf_counter(),time.process_time()
    results=[];counts={}
    for theme,values in groups:
        rows=list(values)
        counts[int(theme)]=len(rows)
        results.append(kernel(int(theme),rows))
    yield dict(partition=pid,stage_id=ctx.stageId(),task_id=ctx.taskAttemptId(),task_attempt=ctx.attemptNumber(),
               counts=counts,results=results,python_wall_s=time.perf_counter()-t,
               python_cpu_s=time.process_time()-cpu)


def execute(df,counts,mode,partitions,model):
    from pyspark.sql import functions as F
    start=time.perf_counter();plan_start=time.perf_counter()
    assignment=placement(counts,partitions,mode,model) if mode.startswith('to_be_') else {}
    plan_s=time.perf_counter()-plan_start
    selected=df.filter(F.col('theme_id').isin(sorted(counts)))
    if mode=='as_is':
        results=[]
        for theme in sorted(counts):
            rows=selected.filter(F.col('theme_id')==theme).orderBy('unclassified_at','article_id').limit(CAP).collect()
            results.append(kernel(theme,rows))
        reports=[]
    elif mode=='control':
        groups=collections.defaultdict(list)
        for row in selected.collect():
            groups[row.theme_id].append(row)
        results=[kernel(t,rows) for t,rows in sorted(groups.items())]
        reports=[]
    else:
        keyed=selected.rdd.map(lambda r:(int(r.theme_id),tuple(r)))
        if mode=='to_be_1':
            grouped=keyed.groupByKey(numPartitions=partitions)
        else:
            grouped=keyed.groupByKey(numPartitions=partitions,partitionFunc=lambda t:assignment[int(t)])
        reports=grouped.mapPartitionsWithIndex(partition_kernel).collect()
        results=[item for report in reports for item in report['results']]
    wall=time.perf_counter()-start
    return wall,results,reports,assignment,plan_s


def calibrate_cost(out):
    from scipy.optimize import nnls
    kernel(1,fixture([32],700000))
    records=[]
    sizes=[25,64,128,256,512,1024,1600,2000]
    for i,n in enumerate(sizes):
        for repeat in range(2):
            seed=700100+i*10+repeat
            result=kernel(1,fixture([n],seed))
            records.append(dict(n=n,seed=seed,repeat=repeat,kernel_wall_s=result[2],kernel_cpu_s=result[3]))
    x=np.array([n/1000 for n in sizes])
    y=np.array([statistics.median(r['kernel_cpu_s'] for r in records if r['n']==n) for n in sizes])
    coefficients,_=nnls(np.column_stack([x*x,x,np.ones_like(x)]),y)
    model=dict(kind='nonnegative_quadratic_plus_linear_cpu',dimension=768,cap=CAP,
               coefficients_quadratic_linear_constant=coefficients.tolist(),
               calibration_seeds=[r['seed'] for r in records],test_data_used=False,
               training_sizes=sizes,training_cpu_medians_s=y.tolist(),
               limitation='Synthetic density only; includes existing HDBSCAN and Python postprocessing; not a complexity guarantee')
    write_csv(out/'cost_calibration.csv',records)
    (out/'cost_model.json').write_text(json.dumps(model,indent=2))
    return model


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--source',required=True);p.add_argument('--output',required=True)
    p.add_argument('--cores',type=int,choices=[1,2,4],required=True)
    p.add_argument('--cost-model',type=Path)
    a=p.parse_args()
    sys.path.insert(0,a.source)
    from pyspark.sql import SparkSession,types as T,functions as F
    from pyspark import StorageLevel
    out=Path(a.output).resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'runs.csv').exists():
        raise RuntimeError('Output already used')
    (out/'spark-events').mkdir(exist_ok=True)
    builder=(SparkSession.builder.appName('whole-theme-partitioning-mini')
       .config('spark.eventLog.enabled','true').config('spark.eventLog.dir',(out/'spark-events').as_uri())
       .config('spark.eventLog.compress','false').config('spark.eventLog.rolling.enabled','false')
       .config('spark.sql.shuffle.partitions','4').config('spark.default.parallelism','4')
       .config('spark.sql.adaptive.enabled','false').config('spark.dynamicAllocation.enabled','false')
       .config('spark.speculation','false').config('spark.python.worker.reuse','true').config('spark.task.cpus','1'))
    for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
        builder=builder.config('spark.executorEnv.'+k,'1')
    spark=builder.getOrCreate();sc=spark.sparkContext;sc.setLogLevel('WARN')
    for path in [Path(a.source)/'issue_clustering.py',Path(__file__).with_name('kernel.py'),
                 Path(__file__).with_name('placement.py'),Path(__file__)]:
        sc.addPyFile(str(path.resolve()))
    driver=list(runtime(None))[0]
    workers=sc.parallelize(range(a.cores),a.cores).mapPartitions(runtime).collect()
    assert all(r==driver for r in workers)
    assert (driver['numpy'],driver['sklearn'],spark.version)==('1.26.4','1.6.1','4.2.0')
    if a.cost_model:
        model=json.loads(a.cost_model.read_text())
        (out/'cost_model.json').write_text(json.dumps(model,indent=2))
    else:
        model=calibrate_cost(out)
    env=dict(design='partitioning-v3-mini',quick=True,driver=driver,executors=workers,cores=a.cores,
             spark=spark.version,master=sc.master,config=dict(sc.getConf().getAll()),params=PARAMS,
             modes=MODES,partitions=PARTITIONS,input_partitions=4,input_backend='untimed_parquet_then_cached_dataframe',
             benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
             placement_sha256=hashlib.sha256(Path(__file__).with_name('placement.py').read_bytes()).hexdigest(),
             cost_model_sha256=hashlib.sha256(json.dumps(model,sort_keys=True).encode()).hexdigest(),
             no_dynamic_driver_executor_routing=True,repetitions=1,embedding_dimension=768)
    (out/'environment.json').write_text(json.dumps(env,indent=2))
    schema=T.StructType([T.StructField('article_id',T.StringType()),T.StructField('theme_id',T.LongType()),
       T.StructField('embedding',T.ArrayType(T.DoubleType())),T.StructField('title',T.StringType()),
       T.StructField('published_at',T.StringType()),T.StructField('unclassified_at',T.StringType()),T.StructField('summary',T.StringType())])
    records=[];part_records=[];theme_records=[];inputs=[]
    def run_case(phase,name,counts,seed,conditions,theme_ids=None,eligible=None):
        rows=fixture(counts,seed)
        ids=theme_ids or list(range(1,len(counts)+1))
        rows=[(r[0],ids[r[1]-1],*r[2:]) for r in rows]
        theme_counts={t:n for t,n in zip(ids,counts) if eligible is None or t in eligible}
        groups=collections.defaultdict(list)
        for r in rows:
            if r[1] in theme_counts:
                groups[r[1]].append(r)
        reference=[kernel(t,rs) for t,rs in sorted(groups.items())]
        expected=fingerprint(reference)
        selected=sum(min(CAP,n) for n in theme_counts.values())
        input_digest=hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()
        inputs.append(dict(phase=phase,scenario=name,seed=seed,counts=json.dumps(theme_counts),
                           input_articles=len(rows),selected_articles=selected,input_digest=input_digest))
        write_csv(out/'inputs.csv',inputs)
        sc.setJobGroup(f'prepare-{phase}-{name}','untimed fixture/cache')
        path=out/'fixtures'/f'{phase}-{name}'
        spark.createDataFrame(sc.parallelize(rows,4),schema).write.parquet(str(path))
        df=(spark.read.schema(schema).parquet(str(path)).repartition(4,F.xxhash64('article_id'))
            .persist(StorageLevel.MEMORY_ONLY))
        assert df.count()==len(rows)
        order=list(conditions);random.Random(seed).shuffle(order)
        try:
            for position,(mode,partitions) in enumerate(order):
                group=f'{phase}-{name}-c{a.cores}-p{partitions}-{mode}'
                sc.setJobGroup(group,group)
                epoch=time.time()
                wall,res,reports,assignment,plan_s=execute(df,theme_counts,mode,partitions,model)
                epoch_end=time.time()
                digest,clusters,assigned=fingerprint(res)
                assert (digest,clusters,assigned)==expected,f'Candidate mismatch: {group}'
                if reports:
                    assert sorted(r['partition'] for r in reports)==list(range(partitions))
                    seen={}
                    for report in reports:
                        for theme,n in report['counts'].items():
                            assert theme not in seen and n==theme_counts[theme]
                            assert assignment[theme]==report['partition']
                            seen[theme]=report['partition']
                        part_records.append(dict(group=group,phase=phase,scenario=name,cores=a.cores,mode=mode,
                            partitions=partitions,partition=report['partition'],stage_id=report['stage_id'],
                            task_id=report['task_id'],task_attempt=report['task_attempt'],
                            themes=';'.join(map(str,sorted(report['counts']))),theme_count=len(report['counts']),
                            raw_articles=sum(report['counts'].values()),selected_articles=sum(min(CAP,n) for n in report['counts'].values()),
                            python_wall_s=report['python_wall_s'],python_cpu_s=report['python_cpu_s'],
                            kernel_wall_s=sum(r[2] for r in report['results']),kernel_cpu_s=sum(r[3] for r in report['results']),
                            predicted_cpu_s=sum(predicted_cost(n,model) for n in report['counts'].values())))
                        for theme,_,kt,kc in report['results']:
                            theme_records.append(dict(group=group,theme_id=theme,partition=report['partition'],
                                articles=theme_counts[theme],kernel_wall_s=kt,kernel_cpu_s=kc))
                    assert set(seen)==set(theme_counts)
                records.append(dict(group=group,phase=phase,scenario=name,seed=seed,order=position,cores=a.cores,
                    mode=mode,partitions=partitions,themes=len(theme_counts),counts=json.dumps(theme_counts),
                    input_articles=len(rows),selected_articles=selected,input_digest=input_digest,
                    wall_s=wall,plan_s=plan_s,kernel_sum_s=sum(r[2] for r in res),kernel_cpu_s=sum(r[3] for r in res),
                    epoch_start=epoch,epoch_end=epoch_end,digest=digest,reference_digest=expected[0],equal=True,
                    clusters=clusters,assigned=assigned,noise_or_filtered=selected-assigned,
                    empty_partitions=sum(not r['counts'] for r in reports),
                    cluster_stage_id=reports[0]['stage_id'] if reports else -1))
                write_csv(out/'runs.csv',records);write_csv(out/'partition_runs.csv',part_records)
                write_csv(out/'theme_runs.csv',theme_records)
            print(f'{phase} {name}: {len(order)} combinations equal',flush=True)
        finally:
            sc.setLocalProperty('spark.jobGroup.id',None);df.unpersist(blocking=True);shutil.rmtree(path)
    distributed=[(m,p) for p in PARTITIONS for m in MODES[2:]]
    try:
        run_case('warmup','all-workers',[32]*8,17,[(m,4 if m.startswith('to_be_') else 0) for m in MODES])
        run_case('hash_smoke','task-count',[25]*8,800001,[('to_be_1',p) for p in PARTITIONS])
        for i,(name,counts) in enumerate(PROFILES.items()):
            run_case('mini',name,counts,900001+i,[('as_is',0),('control',0)]+distributed)
        if a.cores==4:
            run_case('edge','cap-and-ineligible',[2011,25,7],910001,
                [(m,4 if m.startswith('to_be_') else 0) for m in MODES],eligible={1,2})
            run_case('edge','hash-collision',[32]*4,910002,[(m,4) for m in MODES[2:]],theme_ids=[1,17,33,49])
        measured=[r for r in records if r['phase']!='warmup']
        assert len(measured)==(98 if a.cores==4 else 90)
        (out/'SUCCESS.json').write_text(json.dumps(dict(design='partitioning-v3-mini',complete=True,
            equal=True,measured_runs=len(measured),performance_conclusions_allowed=False,full_experiment_run=False),indent=2))
    finally:
        spark.stop()


if __name__=='__main__':
    main()
