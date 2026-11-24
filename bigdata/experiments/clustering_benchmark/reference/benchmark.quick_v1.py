"""Isolated Spark comparison; imports the unchanged production clustering function."""
import os
for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'BLIS_NUM_THREADS'):
    os.environ[key] = '1'
import argparse
import csv
import hashlib
import json
import math
import platform
import random
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
import numpy as np

BASE_MODES = ['as_is', 'control', 'to_be_1']
CAL = {'tiny': [25]*4, 'balanced': [400]*4, 'skew': [1450,50,50,50],
       'single': [1600], 'many': [100]*16, 'large': [1000]*8}
TEST = {'tiny': [32]*4, 'balanced': [300]*4, 'skew': [1080,40,40,40],
        'single': [1200], 'many': [100]*12, 'large': [800]*8}
PARAMS = dict(min_cluster_size=8, min_samples=4, representative_count=4,
              min_membership_confidence=.80, min_centroid_similarity=.92)
CAP = 2000


def fixture(counts, seed):
    rng = np.random.default_rng(seed)
    rows = []
    for theme, n in enumerate(counts, 1):
        centers = rng.normal(size=(max(2,min(6,n//12)),768))
        centers /= np.linalg.norm(centers,axis=1,keepdims=True)
        for i in range(n):
            v = centers[i%len(centers)] + rng.normal(0,.004 if i%10 else .012,size=768)
            v /= np.linalg.norm(v)
            stamp = f'2026-11-24T00:{(i//60)%60:02d}:{i%60:02d}Z'
            rows.append((f'{theme:03d}-{i:06d}', theme, v.tolist(), 'synthetic title', stamp, stamp, None))
    random.Random(seed).shuffle(rows)
    return rows


def kernel(theme, rows):
    from issue_clustering import cluster_articles, UnclassifiedArticle
    t = time.perf_counter()
    cpu = time.process_time()
    selected = sorted(rows, key=lambda r:(r[5],r[0]))[:CAP]
    articles = [UnclassifiedArticle(article_id=r[0],theme_id=r[1],embedding=tuple(r[2]),
                title=r[3],published_at=r[4],unclassified_at=r[5],summary=r[6]) for r in selected]
    clusters = cluster_articles(articles, **PARAMS)
    return theme, clusters, time.perf_counter()-t, time.process_time()-cpu


def group_kernel(item):
    return kernel(*item)


def fingerprint(results):
    out = []
    for theme, clusters, _, _ in sorted(results,key=lambda x:x[0]):
        for c in clusters:
            out.append([theme, sorted(a.article_id for a in c.articles),
                        [a.article_id for a in c.representatives],
                        [round(float(x),10) for x in c.centroid], round(c.membership_confidence,10)])
    return hashlib.sha256(json.dumps(out).encode()).hexdigest(), len(out), sum(len(x[1]) for x in out)


def features(counts):
    counts = [min(CAP,n) for n in counts]
    return np.array([math.log1p(sum(counts)), math.log1p(len(counts)),
                     math.log1p(max(counts)), max(counts)/sum(counts)])


def route(counts, calibration):
    if not calibration:
        return 'control'
    xs = np.array([features(c['counts']) for c in calibration])
    scale = np.maximum(xs.std(axis=0), .1)
    idx = int(np.argmin(np.linalg.norm((xs-features(counts))/scale,axis=1)))
    medians = calibration[idx]['medians']
    # Require 10% improvement over CONTROL to attribute selection to parallelism.
    return 'to_be_1' if medians['to_be_1'] <= .9*medians['control'] else 'control'


def probe_kernel(theme, rows):
    return theme, [r[0] for r in sorted(rows,key=lambda r:(r[5],r[0]))[:CAP]]


def probe_group(item):
    return probe_kernel(*item)


def execute(df, counts, mode, partitions, probe=False):
    from pyspark.sql import functions as F
    t = time.perf_counter()
    fn = probe_kernel if probe else kernel
    if mode == 'as_is':
        results = []
        for theme in sorted(counts):
            rows = df.filter(F.col('theme_id')==theme).orderBy('unclassified_at','article_id').limit(CAP).collect()
            results.append(fn(theme, rows))
    elif mode == 'control':
        # Exactly one article collect, then the same per-theme ordering/cap/kernel.
        rows = df.filter(F.col('theme_id').isin(sorted(counts))).collect()
        groups = defaultdict(list)
        for r in rows:
            groups[r.theme_id].append(r)
        results = [fn(theme, groups[theme]) for theme in sorted(counts)]
    elif mode == 'to_be_1':
        results = (df.filter(F.col('theme_id').isin(sorted(counts))).rdd
                   .map(lambda r:(r.theme_id,tuple(r))).groupByKey(numPartitions=partitions)
                   .map(probe_group if probe else group_kernel).collect())
    else:
        raise ValueError(mode)
    return time.perf_counter()-t, results


def write_csv(path, rows):
    if not rows:
        return
    with Path(path).open('w',newline='') as f:
        w = csv.DictWriter(f,fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def runtime(_):
    import issue_clustering, pyspark, sklearn, scipy, joblib, threadpoolctl
    yield dict(python=platform.python_version(),numpy=np.__version__,sklearn=sklearn.__version__,
               scipy=scipy.__version__,pyspark=pyspark.__version__,joblib=joblib.__version__,
               threadpoolctl=threadpoolctl.__version__,
               source_sha256=hashlib.sha256(Path(issue_clustering.__file__).read_bytes()).hexdigest())


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source',required=True)
    p.add_argument('--master')
    p.add_argument('--cores',type=int,default=4)
    p.add_argument('--reps',type=int,default=10)
    p.add_argument('--seeds',type=int,default=3)
    p.add_argument('--output',required=True)
    p.add_argument('--quick',action='store_true')
    a = p.parse_args()
    if min(a.reps,a.seeds,a.cores)<1:
        p.error('positive counts required')
    if not a.quick and a.reps*a.seeds<30:
        p.error('full experiment requires at least 30 repetitions per condition')
    sys.path.insert(0,str(Path(a.source).resolve()))
    import issue_clustering
    from pyspark.sql import SparkSession, types as T
    from pyspark import StorageLevel
    out = Path(a.output).resolve()
    out.mkdir(parents=True,exist_ok=True)
    if (out/'runs.csv').exists():
        raise RuntimeError('Refusing to mix results; use a new output directory')
    eventdir = out/'spark-events'
    eventdir.mkdir(exist_ok=True)
    builder = (SparkSession.builder.appName('isolated-clustering-benchmark')
       .config('spark.eventLog.enabled','true').config('spark.eventLog.dir',eventdir.as_uri())
       .config('spark.eventLog.compress','false').config('spark.eventLog.rolling.enabled','false')
       .config('spark.sql.shuffle.partitions',str(a.cores)).config('spark.default.parallelism',str(a.cores))
       .config('spark.speculation','false').config('spark.dynamicAllocation.enabled','false')
       .config('spark.sql.adaptive.enabled','false').config('spark.python.worker.reuse','true')
       .config('spark.task.cpus','1').config('spark.scheduler.minRegisteredResourcesRatio','1.0')
       .config('spark.scheduler.maxRegisteredResourcesWaitingTime','60s'))
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
        builder = builder.config('spark.executorEnv.'+key,'1')
    if a.master:
        builder = builder.master(a.master)
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    sc.addPyFile(str(Path(a.source,'issue_clustering.py').resolve()))
    sc.addPyFile(str(Path(__file__).resolve()))
    driver_runtime = list(runtime(None))[0]
    workers = sc.parallelize(range(a.cores),a.cores).mapPartitions(runtime).collect()
    if any(w!=driver_runtime for w in workers):
        raise RuntimeError('Driver/executor runtime or source mismatch')
    if driver_runtime['numpy']!='1.26.4' or driver_runtime['sklearn']!='1.6.1' or spark.version!='4.2.0':
        raise RuntimeError('Project version mismatch')
    environment = dict(driver=driver_runtime,executors=workers,spark=spark.version,master=sc.master,
                       cores=a.cores,config=dict(sc.getConf().getAll()),params=PARAMS,embedding_dimension=768,
                       benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       stream_sha256=hashlib.sha256(Path(a.source,'unclassified_issue_stream.py').read_bytes()).hexdigest(),
                       quick=a.quick,seed_repetitions=2 if a.quick else a.reps,seeds=1 if a.quick else a.seeds,
                       input_partitions=4,policy_margin=.10,
                       timing='cached input ready -> returned full candidate objects; includes routing; excludes digest/resource aggregation')
    (out/'environment.json').write_text(json.dumps(environment,indent=2))
    schema = T.StructType([T.StructField('article_id',T.StringType()),T.StructField('theme_id',T.LongType()),
              T.StructField('embedding',T.ArrayType(T.DoubleType())),T.StructField('title',T.StringType()),
              T.StructField('published_at',T.StringType()),T.StructField('unclassified_at',T.StringType()),
              T.StructField('summary',T.StringType())])
    records, calibration, probes, inputs = [], [], [], []

    def run_profile(phase,name,counts,seed,reps,eligible=None):
        rows = fixture(counts,seed)
        # Columnar cached DataFrame; fixed partition layout across core counts.
        df = spark.createDataFrame(sc.parallelize(rows,4),schema).persist(StorageLevel.MEMORY_ONLY)
        sc.setJobGroup(f'prepare-{phase}-{name}-{seed}', 'untimed cache preparation')
        assert df.count()==len(rows)
        theme_counts = {i+1:n for i,n in enumerate(counts) if eligible is None or i+1 in eligible}
        effective = [min(CAP,n) for n in theme_counts.values()]
        selected = []
        for theme in theme_counts:
            selected += sorted((r for r in rows if r[1]==theme),key=lambda r:(r[5],r[0]))[:CAP]
        input_digest = hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()
        selected_digest = hashlib.sha256(json.dumps([r[0] for r in selected]).encode()).hexdigest()
        inputs.append(dict(phase=phase,scenario=name,seed=seed,input_digest=input_digest,
                           selected_digest=selected_digest,input_articles=len(rows),selected_articles=len(selected)))
        write_csv(out/'inputs.csv',inputs)
        try:
            # Independent Driver reference excluded from timing, never a timed strategy's own answer.
            groups = defaultdict(list)
            for r in selected:
                groups[r[1]].append(r)
            reference = [kernel(t,rs) for t,rs in sorted(groups.items())]
            baseline, reference_clusters, reference_assigned = fingerprint(reference)
            times = defaultdict(list)
            modes = BASE_MODES + (['to_be_2'] if phase=='holdout' else [])
            for rep in range(reps):
                order = modes.copy()
                random.Random(seed+rep*31).shuffle(order)
                for position,mode in enumerate(order):
                    group = f'{phase}-{name}-{seed}-{rep}-{mode}'
                    sc.setJobGroup(group,f'{mode} {name}')
                    epoch_start = time.time()
                    start = time.perf_counter()
                    route_start = time.perf_counter()
                    actual = route(effective,calibration) if mode=='to_be_2' else mode
                    route_s = time.perf_counter()-route_start
                    _,res = execute(df,theme_counts,actual,a.cores)
                    wall = time.perf_counter()-start
                    epoch_end = time.time()
                    dg,n,assigned = fingerprint(res)
                    equal = dg==baseline and n==reference_clusters and assigned==reference_assigned
                    records.append(dict(group=group,phase=phase,scenario=name,seed=seed,rep=rep,order=position,
                        cores=a.cores,mode=mode,actual=actual,input_articles=len(rows),selected_articles=len(selected),
                        counts=';'.join(map(str,counts)),wall_s=wall,articles_per_s=len(selected)/wall,
                        kernel_sum_s=sum(x[2] for x in res),kernel_max_s=max(x[2] for x in res),
                        kernel_cpu_s=sum(x[3] for x in res),route_s=route_s,epoch_start=epoch_start,epoch_end=epoch_end,
                        input_digest=input_digest,selected_digest=selected_digest,digest=dg,reference_digest=baseline,
                        equal=equal,clusters=n,assigned=assigned,noise_or_filtered=len(selected)-assigned))
                    write_csv(out/'runs.csv',records)
                    if not equal:
                        raise RuntimeError(f'Candidate mismatch: {group}')
                    times[mode].append(wall)
            if phase in ('holdout','edge'):
                for rep in range(1 if a.quick else 3):
                    order = BASE_MODES.copy()
                    random.Random(seed+rep+900).shuffle(order)
                    for mode in order:
                        group = f'probe-{name}-{seed}-{rep}-{mode}'
                        sc.setJobGroup(group,'no-clustering structural probe')
                        wall,res = execute(df,theme_counts,mode,a.cores,probe=True)
                        ids = sorted(i for _,v in res for i in v)
                        assert ids==sorted(r[0] for r in selected)
                        probes.append(dict(group=group,scenario=name,seed=seed,rep=rep,mode=mode,wall_s=wall,selected=len(ids)))
                write_csv(out/'overhead_probe.csv',probes)
            print(phase,name,seed,{m:round(statistics.median(v),3) for m,v in times.items()},flush=True)
            return times
        finally:
            sc.setLocalProperty('spark.jobGroup.id',None)
            df.unpersist(blocking=True)
    try:
        # All fixed strategies, all Python worker slots and JVM paths warmed before measurement.
        run_profile('warmup','workers',[40]*max(8,a.cores*2),17,3)
        if a.quick:
            run_profile('edge','cap-and-ineligible',[2011,25,7],23,1,eligible={1,2})
        for i,(name,counts) in enumerate(CAL.items()):
            agg = defaultdict(list)
            for s in range(1 if a.quick else a.seeds):
                values = run_profile('calibration',name,counts,1000+i*100+s,2 if a.quick else a.reps)
                for m,v in values.items():
                    agg[m] += v
            calibration.append(dict(scenario=name,counts=counts,medians={m:statistics.median(agg[m]) for m in BASE_MODES}))
        (out/'policy.json').write_text(json.dumps(dict(features=['log_total','log_themes','log_max_theme','max_share'],
            calibration=calibration,rule='nearest standardized profile; executor only if >=10% faster than CONTROL',
            holdout_used=False),indent=2))
        for i,(name,counts) in enumerate(TEST.items()):
            for s in range(1 if a.quick else a.seeds):
                run_profile('holdout',name,counts,9000+i*100+s,2 if a.quick else a.reps)
        if a.quick:
            required = {('calibration',n,m):2 for n in CAL for m in BASE_MODES}
            required.update({('holdout',n,m):2 for n in TEST for m in BASE_MODES+['to_be_2']})
        else:
            required = {('calibration',n,m):a.reps*a.seeds for n in CAL for m in BASE_MODES}
            required.update({('holdout',n,m):a.reps*a.seeds for n in TEST for m in BASE_MODES+['to_be_2']})
        for (phase,name,mode),n in required.items():
            assert sum(r['phase']==phase and r['scenario']==name and r['mode']==mode for r in records)==n
        (out/'SUCCESS.json').write_text(json.dumps(dict(equal=all(r['equal'] for r in records),
            measured_runs=sum(r['phase'] in ('calibration','holdout') for r in records),quick=a.quick,
            required_repetitions=2 if a.quick else a.reps*a.seeds,complete=True),indent=2))
    finally:
        spark.stop()


if __name__=='__main__':
    main()
