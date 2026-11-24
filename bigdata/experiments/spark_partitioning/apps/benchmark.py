"""Independent A/B runs; fixed two executors x two cores and four target partitions."""
import os
for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
    os.environ[k]='1'
import argparse
import collections
import dataclasses
import hashlib
import json
import platform
import random
import shutil
import sys
import time
from pathlib import Path
import numpy as np
from event_metrics import write_csv
from data import A_COUNTS,B_COUNTS,CANDIDATES,STAMP,rotate_counts,digest,issue_snapshot,matching_rows,clustering_rows
import matching
import clustering
from profiles import RELIABILITY_SCENARIOS,B_SIZES,case_counts,theme_aliases,place_matching_row
from cost_model import calibrate,predict


def runtime(_):
    import sklearn,scipy,issue_clustering,issue_matching
    yield dict(host=__import__('socket').gethostname(),python=platform.python_version(),numpy=np.__version__,
        sklearn=sklearn.__version__,scipy=scipy.__version__,
        cluster_sha256=hashlib.sha256(Path(issue_clustering.__file__).read_bytes()).hexdigest(),
        match_sha256=hashlib.sha256(Path(issue_matching.__file__).read_bytes()).hexdigest())


def append_csv(path,rows):
    import csv
    if not rows:return
    exists=path.exists() and path.stat().st_size>0
    with path.open('a',newline='') as file:
        writer=csv.DictWriter(file,fieldnames=list(rows[0]))
        if not exists:writer.writeheader()
        writer.writerows(rows)


def persistent_ids(sc):
    iterator=sc._jsc.sc().getPersistentRDDs().keysIterator();result=[]
    while iterator.hasNext():result.append(int(iterator.next()))
    return set(result)


def prepare_df(spark,rows,schema,app,forced_theme_partitions=None):
    from pyspark.sql import functions as F
    before=persistent_ids(spark.sparkContext)
    raw=spark.createDataFrame(spark.sparkContext.parallelize(rows,4),schema)
    # Local checkpoint truncates serialized parallelize payloads. No shared filesystem is needed on EC2.
    if forced_theme_partitions is not None:
        scenario,rotation=forced_theme_partitions
        pairs=raw.rdd.map(lambda r:place_matching_row(r,scenario,rotation))
        placed=pairs.partitionBy(4,lambda pid:int(pid)).values()
        df=spark.createDataFrame(placed,schema).localCheckpoint(eager=True)
    else:
        df=raw.repartition(4,F.xxhash64('article_id')).localCheckpoint(eager=True)
    assert df.rdd.getNumPartitions()==4
    assert df.count()==len(rows)
    return df,persistent_ids(spark.sparkContext)-before


def unpersist(sc,ids):
    for rid in ids:
        option=sc._jsc.sc().getPersistentRDDs().get(rid)
        if option.isDefined():option.get().unpersist(True)


def matching_input(counts,seed,issues,namespace=False):
    from pyspark.sql import Row
    from enriched_article_stream import _transform_kafka_partition
    expected=matching_rows(counts,seed,issues)
    if namespace:
        expected=[(f'{r[0].rsplit("-",1)[0]}-{seed}-{r[0].rsplit("-",1)[1]}',*r[1:]) for r in expected]
    kafka=[]
    for i,(aid,theme,v,title,summary,published,occurred) in enumerate(expected):
        nlp=dict(country_code='KR',theme_id=theme,theme_relevance_score=.99,article_embedding=v,
                 summary=summary,sentiment={'score':50},mentioned_countries=[],processed_at=occurred)
        if i%2==0:
            nlp['article_embedding']=[-x for x in v];nlp['summary_embedding']=v
        message=dict(article_id=aid,source_name='Fixture',article_url='https://fixture.invalid/'+aid,
                     title=title,discovered_at=STAMP,published_at=published,nlp_worker_data=nlp)
        kafka.append(Row(topic='fixture.enriched',partition=i%4,offset=i,value=json.dumps(message)))
    rows=list(_transform_kafka_partition(iter(kafka)))
    assert len(rows)==len(expected)
    assert all(row[4]==original[2] for row,original in zip(rows,expected)), 'Embedding selection changed'
    return rows


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--source',required=True);p.add_argument('--output',required=True)
    p.add_argument('--app',choices=['A','B'],default='B')
    p.add_argument('--mode',choices=['quick','explore','full','calibrate'],default='quick')
    p.add_argument('--reps',type=int);p.add_argument('--cost-model',type=Path)
    p.add_argument('--scenarios',nargs='+',choices=['D1','D2','D3','D4',*RELIABILITY_SCENARIOS],default=['D1','D2','D3','D4'])
    p.add_argument('--rep-start',type=int,default=0);p.add_argument('--session',type=int,default=0)
    p.add_argument('--include-baselines',action='store_true')
    a=p.parse_args();reps=a.reps or {'quick':1,'explore':3,'full':30,'calibrate':1}[a.mode]
    if reps<1 or (a.mode=='full' and reps<30 and not (a.session in [1,2] and reps==15 and a.rep_start in [0,15])):
        p.error('Full requires 30 blocks or one explicitly identified half-session of 15 blocks')
    sys.path.insert(0,str(Path(a.source).resolve()))
    from pyspark.sql import SparkSession,types as T
    out=Path(a.output).resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'runs.csv').exists():raise RuntimeError('Use a fresh output directory')
    (out/'spark-events').mkdir(exist_ok=True)
    (out/'code').mkdir(exist_ok=True)
    for codefile in Path(__file__).resolve().parent.glob('*.py'):
        shutil.copyfile(codefile,out/'code'/codefile.name)
    builder=(SparkSession.builder.appName('partitioning-'+a.app+'-'+a.mode)
       .config('spark.cores.max','4').config('spark.executor.cores','2').config('spark.task.cpus','1')
       .config('spark.sql.shuffle.partitions','4').config('spark.default.parallelism','4')
       .config('spark.dynamicAllocation.enabled','false').config('spark.speculation','false')
       .config('spark.sql.adaptive.enabled','false').config('spark.python.worker.reuse','true')
       .config('spark.scheduler.minRegisteredResourcesRatio','1.0')
       .config('spark.scheduler.maxRegisteredResourcesWaitingTime','60s')
       .config('spark.eventLog.enabled','true').config('spark.eventLog.dir',(out/'spark-events').as_uri())
       .config('spark.eventLog.compress','false').config('spark.eventLog.rolling.enabled','false')
       .config('spark.ui.prometheus.enabled','true'))
    for k in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','BLIS_NUM_THREADS'):
        builder=builder.config('spark.executorEnv.'+k,'1')
    spark=builder.getOrCreate();sc=spark.sparkContext;sc.setLogLevel('WARN')
    sources=['issue_clustering.py','issue_matching.py','article_event.py','event_identity.py','enriched_article_stream.py','streaming_metrics.py']
    for name in sources:sc.addPyFile(str(Path(a.source,name).resolve()))
    for path in Path(__file__).resolve().parent.glob('*.py'):sc.addPyFile(str(path))
    workers=sc.parallelize(range(8),8).mapPartitions(runtime).collect()
    versions=[{k:v for k,v in r.items() if k!='host'} for r in workers]
    assert all(r==versions[0] for r in versions)
    assert (versions[0]['numpy'],versions[0]['sklearn'],spark.version)==('1.26.4','1.6.1','4.2.0')
    driver={k:v for k,v in next(runtime(None)).items() if k!='host'}
    assert driver==versions[0]
    env=dict(app=a.app,mode=a.mode,reps=reps,rep_start=a.rep_start,session=a.session,scenarios=a.scenarios,spark=spark.version,driver=driver,executors=workers,
        master=sc.master,config=dict(sc.getConf().getAll()),parts=4,cores=4,executor_cores=2,expected_executors=2,
        params=clustering.PARAMS,matching_threshold=.93,embedding_dimension=768,
        input_backend='eager localCheckpoint, fixed hash of article_id, four partitions',
        timer='ready input -> strategy/extra aggregate/plan/shuffle -> original function -> same collect',
        matching_telemetry='original cosine wrapped to capture per-article maximum; equal overhead in A0/A1/A2',
        cost_model_independent=True,rounding_similarity=12,rounding_centroid_confidence=10,
        intentional_skew='SKEW' in a.scenarios,
        source_sha256={n:hashlib.sha256(Path(a.source,n).read_bytes()).hexdigest() for n in sources},
        benchmark_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        code_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).resolve().parent.glob('*.py')})
    if a.app=='A' and 'SKEW' in a.scenarios:
        env['input_backend']='eager localCheckpoint, explicit identity partitioning of theme groups, equal article counts and unequal candidate work'
    (out/'environment.json').write_text(json.dumps(env,indent=2))
    model=None
    if a.app=='B':
        if a.cost_model:
            model=json.loads(a.cost_model.read_text());(out/'cost_model.json').write_text(json.dumps(model,indent=2))
        else:model=calibrate(out,sc=sc)
    if a.mode=='calibrate':
        (out/'SUCCESS.json').write_text(json.dumps(dict(complete=True,calibration_only=True)))
        spark.stop();return
    issues=issue_snapshot() if a.app=='A' else None
    broadcast=sc.broadcast(issues) if issues else None
    if issues:
        snapshot={t:[dataclasses.asdict(i) for i in candidates] for t,candidates in issues.items()}
        (out/'issue_snapshot.json').write_text(json.dumps(snapshot,separators=(',',':')))
        env['issue_snapshot_sha256']=digest(snapshot)
        (out/'environment.json').write_text(json.dumps(env,indent=2))
        from enriched_article_stream import ARTICLE_SCHEMA
        schema=ARTICLE_SCHEMA
    else:
        schema=T.StructType([T.StructField('article_id',T.StringType()),T.StructField('theme_id',T.LongType()),
          T.StructField('embedding',T.ArrayType(T.DoubleType())),T.StructField('title',T.StringType()),
          T.StructField('published_at',T.StringType()),T.StructField('unclassified_at',T.StringType()),T.StructField('summary',T.StringType())])
    records=[];part_records=[];inputs=[];failures=[];common=[];plans=[]
    main_modes=['A0','A1','A2'] if a.app=='A' else ['B0','B1','B2']
    modes=main_modes+(['AS_IS','CONTROL'] if a.app=='B' and (a.mode=='quick' or a.include_baselines) else [])
    def run_case(phase,scenario,counts,seed,rep,warmup=False):
        sc.setJobGroup(f'prepare-{phase}-{scenario}-{seed}','untimed fixture and local checkpoint')
        if a.app=='B' and scenario in RELIABILITY_SCENARIOS:
            logical_rows=clustering_rows(dict(enumerate(B_SIZES,1)),seed)
            logical_rows=[(f'{r[0].rsplit("-",1)[0]}-{seed}-{r[0].rsplit("-",1)[1]}',*r[1:]) for r in logical_rows]
            aliases=theme_aliases(scenario,seed,rep)
            rows=[(r[0],aliases[r[1]],*r[2:]) for r in logical_rows]
        else:
            rows=matching_input(counts,seed,issues,scenario in RELIABILITY_SCENARIOS) if a.app=='A' else clustering_rows(counts,seed)
            logical_rows=rows
        input_hash=digest(rows)
        inputs.append(dict(app=a.app,phase=phase,scenario=scenario,seed=seed,rep=rep,
            counts=json.dumps(counts),n=len(rows),input_sha256=input_hash,logical_input_sha256=digest(logical_rows)))
        write_csv(out/'inputs.csv',inputs)
        forced=None
        if a.app=='A' and scenario in ['MODERATE','SKEW']:
            # Equal 1,000 articles per input partition, deliberately unequal candidate work.
            # Rotate physical destination between blocks to avoid fixing the expensive Task to one worker.
            forced=(scenario,rep%4)
        t=time.perf_counter();df,cached=prepare_df(spark,rows,schema,a.app,forced);prepare_s=time.perf_counter()-t
        if scenario in RELIABILITY_SCENARIOS:
            metadata=df.select('theme_id').rdd.mapPartitionsWithIndex(matching.metadata_partition).collect()
            if a.app=='A':
                loads=[sum(CANDIDATES[t-1]*n for t,n in c.items()) for _,c in metadata]
                if scenario!='NATURAL':assert all(sum(c.values())==1000 for _,c in metadata)
                if scenario=='SKEW':assert sorted(loads)==[15000,40000,90000,175000]
                if scenario=='MODERATE':assert sorted(loads)==[41000,56000,86000,137000]
            else:
                hashloads=[sum(n for t,n in counts.items() if t%4==pid) for pid in range(4)]
                if scenario=='SKEW':assert sorted(hashloads)==[150,150,200,1500]
                if scenario=='MODERATE':assert sorted(hashloads)==[200,300,650,850]
            (out/f'placement-input-{scenario}-{seed}.json').write_text(json.dumps(dict(app=a.app,scenario=scenario,seed=seed,
                logical_input_sha256=digest(logical_rows),theme_aliases=aliases if a.app=='B' else None,
                actual_initial_partition_theme_counts=metadata,forced_theme_partitions=forced,
                expected_a_candidate_comparisons=loads if a.app=='A' else None,
                expected_b_hash_articles=hashloads if a.app=='B' else None),indent=2))
        t=time.perf_counter()
        # Common work done by the current production driver before entering the measured strategy.
        if a.app=='A':
            actual={int(r.theme_id) for r in df.select('theme_id').distinct().collect()}
            assert actual==set(counts)
        else:
            actual={int(r.theme_id):int(r['count']) for r in df.groupBy('theme_id').count().collect()}
            assert actual==counts
        common.append(dict(phase=phase,scenario=scenario,seed=seed,prepare_s=prepare_s,common_aggregation_s=time.perf_counter()-t))
        write_csv(out/'common_costs.csv',common)
        try:
            sc.setJobGroup(f'reference-{phase}-{scenario}-{seed}','untimed production reference')
            if a.app=='A':
                _,reference_reports,_=matching.execute(df,'A0',broadcast,dict(enumerate(CANDIDATES,1)))
                reference=matching.canonical(reference_reports)
                assert {r[0] for r in reference}=={r[2] for r in rows}
            else:
                groups=collections.defaultdict(list)
                for r in rows:groups[r[1]].append(r)
                reference=clustering.canonical([clustering.kernel(t,rs) for t,rs in groups.items()])
            reference_hash=digest(reference)
            order=modes.copy();random.Random(seed+rep*31).shuffle(order)
            for position,mode in enumerate(order):
                group=f'{a.app}-{phase}-{scenario}-s{seed}-r{rep}-{mode}'
                sc.setJobGroup(group,group);start=time.time()
                try:
                    if a.app=='A':
                        wall,reports,meta=matching.execute(df,mode,broadcast,dict(enumerate(CANDIDATES,1)))
                        results=matching.canonical(reports)
                        assigned=sum(r[1]=='fixture.event' for r in results)
                        # Save output IDs, issue assignments, assigned similarity and actual maximum; never drop duplicates.
                        matches=[]
                        for aid,topic,key,payload,maximum in results:
                            matches.append(dict(group=group,article_id=aid,topic=topic,
                                issue_id=payload.get('payload',{}).get('issue_id'),
                                similarity=payload.get('payload',{}).get('similarity'),max_similarity=maximum))
                        kernel_wall=sum(r['kernel_wall_s'] for r in reports);kernel_cpu=sum(r['kernel_cpu_s'] for r in reports)
                    else:
                        wall,kernels,reports,meta=clustering.execute(df,counts,mode,model)
                        results=clustering.canonical(kernels)
                        assigned=sum(len(c['ids']) for r in results for c in r['clusters'])
                        kernel_wall=sum(r['wall_s'] for r in kernels);kernel_cpu=sum(r['cpu_s'] for r in kernels)
                    # Materialization has completed before canonical comparison, which is excluded from the timer.
                    # execute() captures the monotonic wall clock. Use that exact interval for sampling.
                    end=start+wall
                    result_hash=digest(results)
                    assert result_hash==reference_hash,f'Result mismatch {group}'
                    if reports:
                        assert sorted(r['partition'] for r in reports)==[0,1,2,3]
                    plan=meta.get('assignment',{})
                    plans.append(dict(group=group,mode=mode,assignment=plan,
                        article_quotas=(meta.get('plan') or {}).get('quotas',{}),theme_counts=counts))
                    (out/'plans.json').write_text(json.dumps(plans,indent=2))
                    for report in reports:
                        if a.app=='B':
                            for theme in report['counts']:assert plan[theme]==report['partition']
                        part_records.append(dict(group=group,app=a.app,phase=phase,scenario=scenario,mode=mode,
                            partition=report['partition'],stage_id=report['stage_id'],task_id=report['task_id'],
                            executor_host=report['executor_host'],n=sum(report['counts'].values()),themes=json.dumps(report['counts']),
                            predicted_cost=sum((CANDIDATES[t-1]*768*n if a.app=='A' else predict(n,model)) for t,n in report['counts'].items()),
                            kernel_wall_s=report['kernel_wall_s'],kernel_cpu_s=report['kernel_cpu_s'],
                            python_rss_peak_kib=report['python_rss_peak_kib'],empty=not report['counts']))
                    records.append(dict(group=group,app=a.app,phase=phase,scenario=scenario,seed=seed,rep=rep,order=position,
                        mode=mode,n=len(rows),input_sha256=input_hash,reference_sha256=reference_hash,result_sha256=result_hash,
                        equal=True,wall_s=wall,articles_per_s=len(rows)/wall,plan_s=meta['plan_s'],extra_aggregate_s=meta['aggregate_s'],
                        kernel_sum_s=kernel_wall,kernel_cpu_s=kernel_cpu,epoch_start=start,epoch_end=end,
                        assigned=assigned,remaining=len(rows)-assigned,cluster_stage_id=reports[0]['stage_id'] if reports else -1))
                    write_csv(out/'runs.csv',records);write_csv(out/'partition_runs.csv',part_records)
                    if a.app=='A':append_csv(out/'matching_results.csv',matches)
                except Exception as exc:
                    failures.append(dict(group=group,type=type(exc).__name__,message=str(exc),epoch=time.time(),retry=0))
                    write_csv(out/'failures.csv',failures)
                    raise
            print(f'{a.app} {phase} {scenario} rep={rep}: {len(order)} strategies equal',flush=True)
        finally:
            sc.setLocalProperty('spark.jobGroup.id',None);unpersist(sc,cached)
    try:
        warm_counts={i+1:32 for i in range(8)}
        for rep in range(2):run_case('warmup','warmup',warm_counts,700000+rep,rep,True)
        profiles=A_COUNTS if a.app=='A' else B_COUNTS
        cases=[]
        for rep in range(a.rep_start,a.rep_start+reps):
            order=a.scenarios.copy();random.Random(420000+rep).shuffle(order)
            cases.extend((scenario,rep) for scenario in order)
        for scenario,rep in cases:
            i=a.scenarios.index(scenario)
            offset={'quick':0,'explore':1000000,'full':2000000}[a.mode]
            seed=(810000 if a.app=='A' else 910000)+offset+(0 if scenario in RELIABILITY_SCENARIOS else i*100)+rep
            if scenario in RELIABILITY_SCENARIOS:
                counts=case_counts(a.app,scenario,seed,rep)
            else:
                counts=rotate_counts(profiles[scenario],seed)
            assert sum(counts.values())==(4000 if a.app=='A' else 2000)
            run_case(a.mode,scenario,counts,seed,rep)
        measured=[r for r in records if r['phase']!='warmup']
        assert len(measured)==len(a.scenarios)*reps*len(modes)
        (out/'SUCCESS.json').write_text(json.dumps(dict(complete=True,app=a.app,mode=a.mode,
            measured_runs=len(measured),equal=True,failures=len(failures),retries=0,
            performance_conclusions_allowed=a.mode=='full' and reps>=30,main_modes=main_modes,modes=modes),indent=2))
    finally:
        if broadcast is not None:broadcast.destroy()
        spark.stop()


if __name__=='__main__':
    main()
