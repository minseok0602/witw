import resource
import socket
import time
from placement import CAP,theme_lpt

PARAMS=dict(min_cluster_size=8,min_samples=4,representative_count=4,
            min_membership_confidence=.80,min_centroid_similarity=.92)


def kernel(theme,rows):
    from issue_clustering import cluster_articles,UnclassifiedArticle
    start=time.perf_counter();cpu=time.process_time()
    selected=sorted(rows,key=lambda r:(r[5],r[0]))[:CAP]
    articles=[UnclassifiedArticle(article_id=r[0],theme_id=r[1],embedding=tuple(r[2]),
        title=r[3],published_at=r[4],unclassified_at=r[5],summary=r[6]) for r in selected]
    result=cluster_articles(articles,**PARAMS)
    return dict(theme=theme,clusters=result,input_ids=[r[0] for r in selected],
                wall_s=time.perf_counter()-start,cpu_s=time.process_time()-cpu)


def partition_kernel(pid,groups):
    from pyspark import TaskContext
    ctx=TaskContext.get();start=time.perf_counter();cpu=time.process_time();results=[];counts={}
    for theme,values in groups:
        rows=list(values);counts[int(theme)]=len(rows)
        results.append(kernel(int(theme),rows))
    yield dict(partition=pid,stage_id=ctx.stageId(),task_id=ctx.taskAttemptId(),executor_host=socket.gethostname(),
        results=results,counts=counts,kernel_wall_s=sum(r['wall_s'] for r in results),
        kernel_cpu_s=sum(r['cpu_s'] for r in results),python_wall_s=time.perf_counter()-start,
        python_cpu_s=time.process_time()-cpu,python_rss_peak_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def execute(df,counts,mode,model):
    from pyspark.sql import functions as F
    import collections
    from cost_model import predict
    start=time.perf_counter();t=time.perf_counter();assignment={}
    if mode=='B1':
        assignment=theme_lpt(counts,{t:min(CAP,n) for t,n in counts.items()})
    elif mode=='B2':
        assignment=theme_lpt(counts,{t:predict(n,model) for t,n in counts.items()})
    plan_s=time.perf_counter()-t
    if mode=='AS_IS':
        results=[kernel(theme,df.filter(F.col('theme_id')==theme).orderBy('unclassified_at','article_id').limit(CAP).collect())
                 for theme in sorted(counts)]
        reports=[]
    elif mode=='CONTROL':
        groups=collections.defaultdict(list)
        for row in df.collect():groups[int(row.theme_id)].append(row)
        results=[kernel(t,r) for t,r in sorted(groups.items())];reports=[]
    else:
        keyed=df.rdd.map(lambda r:(int(r.theme_id),tuple(r)))
        grouped=keyed.groupByKey(4) if mode=='B0' else keyed.groupByKey(4,lambda t:assignment[int(t)])
        reports=grouped.mapPartitionsWithIndex(partition_kernel).collect()
        results=[r for p in reports for r in p['results']]
        if mode=='B0':assignment={t:int(t)%4 for t in counts}
    wall=time.perf_counter()-start
    return wall,results,reports,dict(plan_s=plan_s,aggregate_s=0.,assignment=assignment)


def canonical(results):
    out=[]
    for r in sorted(results,key=lambda r:r['theme']):
        clusters=[];assigned=set()
        for c in r['clusters']:
            ids=sorted(a.article_id for a in c.articles);assigned.update(ids)
            clusters.append(dict(ids=ids,representatives=[a.article_id for a in c.representatives],
                centroid=[round(float(v),10) for v in c.centroid],confidence=round(c.membership_confidence,10)))
        clusters.sort(key=lambda c:c['ids'])
        out.append(dict(theme=r['theme'],input_ids=r['input_ids'],clusters=clusters,
                        remaining_ids=sorted(set(r['input_ids'])-assigned)))
    return out
