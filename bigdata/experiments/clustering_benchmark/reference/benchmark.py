"""Run from witw root using its Spark image; see README. Never writes production state."""
import os
for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
    os.environ[key]='1'
import argparse, csv, hashlib, json, math, random, statistics, sys, time
from pathlib import Path
from collections import defaultdict
import numpy as np

CAL={'tiny':[25]*4,'medium':[120]*4,'large':[400]*4,'skew':[1450,50,50,50],'single':[800],'many_small':[25]*12}
TEST={'small':[32]*4,'balanced':[300]*4,'skew':[1080,40,40,40],'single':[600],'many_small':[40]*12,'mixed':[400,200,100,50,25,25]}
PARAMS=dict(min_cluster_size=8,min_samples=4,representative_count=4,min_membership_confidence=.80,min_centroid_similarity=.92)

def fixture(counts, seed):
    rng=np.random.default_rng(seed); rows=[]
    for theme,n in enumerate(counts,1):
        centers=rng.normal(size=(max(2,min(6,n//12)),768)); centers/=np.linalg.norm(centers,axis=1,keepdims=True)
        for i in range(n):
            v=centers[i%len(centers)]+rng.normal(0,.004 if i%10 else .012,size=768); v/=np.linalg.norm(v)
            rows.append((f'{theme:03d}-{i:06d}',theme,v.tolist(),'synthetic title','2026-11-24T00:00:00Z','2026-11-24T00:00:00Z',None))
    random.Random(seed).shuffle(rows); return rows

def kernel(theme, rows):
    from issue_clustering import cluster_articles,UnclassifiedArticle
    t=time.perf_counter(); cpu=time.process_time()
    selected=sorted(rows,key=lambda r:(r[5],r[0]))[:2000]
    articles=[UnclassifiedArticle(article_id=r[0],theme_id=r[1],embedding=tuple(r[2]),title=r[3],published_at=r[4],unclassified_at=r[5],summary=r[6]) for r in selected]
    clusters=cluster_articles(articles,**PARAMS)
    return theme,clusters,time.perf_counter()-t,time.process_time()-cpu

def group_kernel(item):
    theme,rows=item; return kernel(theme,rows)

def fingerprint(results):
    out=[]
    for theme,clusters,_,_ in sorted(results,key=lambda x:x[0]):
        for c in clusters:
            out.append([theme,sorted(a.article_id for a in c.articles),[a.article_id for a in c.representatives],[round(float(x),10) for x in c.centroid],round(c.membership_confidence,10)])
    return hashlib.sha256(json.dumps(out).encode()).hexdigest(),len(out),sum(len(x[1]) for x in out)

def features(counts):
    return np.array([math.log1p(sum(counts)),math.log1p(len(counts)),math.log1p(max(counts)),max(counts)/sum(counts)])

def route(counts, calibration):
    if not calibration: return 'as_is'
    xs=np.array([features(c) for c,_,_ in calibration]); scale=np.maximum(xs.std(axis=0),.1)
    idx=int(np.argmin(np.linalg.norm((xs-features(counts))/scale,axis=1)))
    _,a,b=calibration[idx]
    return 'to_be_1' if b<=.9*a else 'as_is'

def probe_kernel(theme, rows):
    return theme, [r[0] for r in sorted(rows,key=lambda r:(r[5],r[0]))[:2000]]

def probe_group(item):
    return probe_kernel(*item)

def execute(df,counts,mode,partitions,probe=False):
    from pyspark.sql import functions as F
    t=time.perf_counter()
    if mode=='as_is':
        results=[]
        for theme in sorted(counts):
            rows=df.filter(F.col('theme_id')==theme).orderBy('unclassified_at','article_id').limit(2000).collect()
            results.append((probe_kernel if probe else kernel)(theme,rows))
    else:
        # All data for one theme must remain together. Multiple themes can share a partition.
        results=df.rdd.map(lambda r:(r.theme_id,tuple(r))).groupByKey(numPartitions=partitions).map(probe_group if probe else group_kernel).collect()
    return time.perf_counter()-t,results

def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True,help='witw bigdata/spark/apps directory');p.add_argument('--master',default=None);p.add_argument('--cores',type=int,default=4);p.add_argument('--reps',type=int,default=5);p.add_argument('--seeds',type=int,default=2);p.add_argument('--output',default='results');p.add_argument('--quick',action='store_true');a=p.parse_args()
    if min(a.reps,a.seeds,a.cores)<1: p.error('positive counts required')
    sys.path.insert(0,str(Path(a.source).resolve())); from issue_clustering import cluster_articles
    from pyspark.sql import SparkSession,types as T
    from pyspark import StorageLevel
    out=Path(a.output).resolve();out.mkdir(parents=True,exist_ok=True)
    eventdir=out/'spark-events';eventdir.mkdir(exist_ok=True)
    builder=SparkSession.builder.config('spark.eventLog.enabled','true').config('spark.eventLog.dir',eventdir.as_uri()).config('spark.eventLog.compress','false').config('spark.eventLog.rolling.enabled','false').appName('isolated-clustering-benchmark').config('spark.sql.shuffle.partitions',str(a.cores)).config('spark.speculation','false').config('spark.dynamicAllocation.enabled','false').config('spark.sql.adaptive.enabled','false').config('spark.python.worker.reuse','true').config('spark.task.cpus','1').config('spark.executorEnv.OMP_NUM_THREADS','1').config('spark.executorEnv.OPENBLAS_NUM_THREADS','1').config('spark.executorEnv.MKL_NUM_THREADS','1').config('spark.executorEnv.NUMEXPR_NUM_THREADS','1')
    if a.master: builder=builder.master(a.master)
    spark=builder.getOrCreate();sc=spark.sparkContext;sc.setLogLevel('WARN');sc.addPyFile(str(Path(a.source,'issue_clustering.py').resolve()));sc.addPyFile(str(Path(__file__).resolve()))
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    import sklearn
    (out/'environment.json').write_text(json.dumps(dict(spark=spark.version,sklearn=sklearn.__version__,python=sys.version,master=sc.master,cores=a.cores,config=dict(sc.getConf().getAll()),source_sha256=hashlib.sha256(Path(a.source,'issue_clustering.py').read_bytes()).hexdigest(),params=PARAMS),indent=2))
    schema=T.StructType([T.StructField('article_id',T.StringType()),T.StructField('theme_id',T.LongType()),T.StructField('embedding',T.ArrayType(T.DoubleType())),T.StructField('title',T.StringType()),T.StructField('published_at',T.StringType()),T.StructField('unclassified_at',T.StringType()),T.StructField('summary',T.StringType())])
    records=[];calibration=[];probes=[]
    def run_profile(phase,name,counts,seed,reps):
        rows=fixture(counts,seed); df0=spark.createDataFrame(sc.parallelize(rows,a.cores),schema)
        cached=df0.rdd.persist(StorageLevel.MEMORY_AND_DISK);cached.count();df=spark.createDataFrame(cached,schema)
        try:
            # Counts are already available to the production driver at trigger selection.
            theme_counts={i+1:n for i,n in enumerate(counts)}
            baseline=None
            modes=['as_is','to_be_1']+(['to_be_2'] if phase=='holdout' else [])
            times=defaultdict(list)
            for rep in range(reps):
                order=modes.copy();random.Random(seed+rep).shuffle(order)
                for mode in order:
                    start=time.perf_counter();actual=route(counts,calibration) if mode=='to_be_2' else mode;route_s=time.perf_counter()-start
                    sc.setJobGroup(f'{phase}-{name}-{seed}-{rep}-{mode}',f'{mode} {name}')
                    wall,res=execute(df,theme_counts,actual,a.cores);wall+=route_s
                    dg,n,assigned=fingerprint(res);baseline=baseline or dg
                    records.append(dict(phase=phase,scenario=name,seed=seed,counts=';'.join(map(str,counts)),rep=rep,mode=mode,actual=actual,wall_s=wall,kernel_sum_s=sum(x[2] for x in res),kernel_max_s=max(x[2] for x in res),kernel_cpu_s=sum(x[3] for x in res),route_s=route_s,digest=dg,equal=dg==baseline,clusters=n,assigned=assigned));times[mode].append(wall)
                    with (out/'runs.csv').open('w',newline='') as f:
                        w=csv.DictWriter(f,fieldnames=list(records[0]));w.writeheader();w.writerows(records)
                    if dg!=baseline: raise RuntimeError(f'Candidate mismatch: {phase} {name} {seed} {mode}')
            if phase=='holdout':
                for rep in range(2 if a.quick else 5):
                    order=['as_is','to_be_1'];random.Random(seed+rep+900).shuffle(order)
                    for mode in order:
                        sc.setJobGroup(f'probe-{name}-{seed}-{rep}-{mode}',f'no-clustering probe {mode}')
                        wall,res=execute(df,theme_counts,mode,a.cores,probe=True)
                        assert sum(len(x[1]) for x in res)==sum(counts)
                        probes.append(dict(scenario=name,seed=seed,rep=rep,mode=mode,wall_s=wall,selected=sum(len(x[1]) for x in res)))
                with (out/'overhead_probe.csv').open('w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=list(probes[0]));w.writeheader();w.writerows(probes)
            print(phase,name,seed,{m:round(statistics.median(v),3) for m,v in times.items()},flush=True)
            return times
        finally:cached.unpersist(blocking=True)
    try:
        run_profile('warmup','warmup',[25]*max(4,a.cores),17,2)
        for i,(name,counts) in enumerate(CAL.items()):
            agg=defaultdict(list)
            for s in range(1 if a.quick else a.seeds):
                times=run_profile('calibration',name,counts,1000+i*100+s,2 if a.quick else a.reps)
                for m,v in times.items():agg[m]+=v
            calibration.append((counts,statistics.median(agg['as_is']),statistics.median(agg['to_be_1'])))
        (out/'policy.json').write_text(json.dumps(calibration,indent=2))
        for i,(name,counts) in enumerate(TEST.items()):
            for s in range(1 if a.quick else a.seeds):run_profile('holdout',name,counts,9000+i*100+s,2 if a.quick else a.reps)
        summary=[]
        for name in TEST:
            rows=[r for r in records if r['phase']=='holdout' and r['scenario']==name];base=statistics.median(r['wall_s'] for r in rows if r['mode']=='as_is')
            for mode in ['as_is','to_be_1','to_be_2']:
                rr=[r for r in rows if r['mode']==mode];v=[r['wall_s'] for r in rr];med=statistics.median(v)
                summary.append(dict(scenario=name,mode=mode,n=len(v),median_s=med,p95_s=float(np.percentile(v,95)),speedup=base/med,reduction_pct=100*(1-med/base),route=rr[0]['actual'],equal=all(r['equal'] for r in rr)))
        with (out/'summary.csv').open('w',newline='') as f:
            w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
    finally:spark.stop()
if __name__=='__main__':main()
