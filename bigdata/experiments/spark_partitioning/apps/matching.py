"""Call the production partition matcher. Only telemetry wraps the original cosine function."""
import collections
import resource
import time
from functools import partial
from placement import article_plan,destination


def metadata_partition(pid,rows):
    counts=collections.Counter(int(r.theme_id) for r in rows)
    yield pid,dict(counts)


def keyed_partition(pid,rows,plan):
    local=collections.Counter()
    for i,row in enumerate(rows):
        theme=int(row.theme_id)
        target=destination(plan,pid,i,theme,local[theme])
        local[theme]+=1
        yield target,row


def match_partition(pid,rows,issues):
    from pyspark import TaskContext
    from enriched_article_stream import _match_partition
    import issue_matching
    original=issue_matching.cosine_similarity
    capture=[None,0]
    def measured_cosine(a,b):
        score=original(a,b)
        capture[1]+=1
        if score is not None and (capture[0] is None or score>capture[0]):
            capture[0]=score
        return score
    issue_matching.cosine_similarity=measured_cosine
    results=[];counts=collections.Counter();comparisons=0
    ctx=TaskContext.get();start=time.perf_counter();cpu=time.process_time()
    try:
        for row in rows:
            capture[:]=[None,0]
            # The exact existing article conversion, theme filtering, threshold, tie policy and event IDs.
            result=next(_match_partition(iter([row]),issues,.93,'fixture.event','fixture.unclassified'))
            results.append((row.article_id,*result,capture[0]))
            comparisons+=capture[1];counts[int(row.theme_id)]+=1
    finally:
        issue_matching.cosine_similarity=original
    yield dict(partition=pid,stage_id=ctx.stageId(),task_id=ctx.taskAttemptId(),executor_host=__import__('socket').gethostname(),
        results=results,counts=dict(counts),kernel_wall_s=time.perf_counter()-start,
        kernel_cpu_s=time.process_time()-cpu,comparisons=comparisons,
        python_rss_peak_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)


def execute(df,mode,broadcast,candidate_counts):
    start=time.perf_counter();plan_s=0.;aggregate_s=0.;plan=None
    rdd=df.rdd
    if mode!='A0':
        t=time.perf_counter()
        metadata=df.select('theme_id').rdd.mapPartitionsWithIndex(metadata_partition).collect()
        aggregate_s=time.perf_counter()-t
        t=time.perf_counter()
        plan=article_plan(metadata,candidate_counts,mode=='A2')
        plan_s=time.perf_counter()-t
        # Identity partition function: the integer destination is not hashed a second time.
        rdd=(rdd.mapPartitionsWithIndex(partial(keyed_partition,plan=plan))
             .partitionBy(4,lambda target:int(target)).values())
    reports=rdd.mapPartitionsWithIndex(partial(match_partition,issues=broadcast)).collect()
    return time.perf_counter()-start,reports,dict(plan_s=plan_s,aggregate_s=aggregate_s,plan=plan)


def canonical(reports):
    result=[]
    for report in reports:
        for aid,topic,key,value,maximum in report['results']:
            payload=__import__('json').loads(value)
            # Output similarity is preserved and compared, even when no event was assigned.
            result.append((aid,topic,key,payload,None if maximum is None else round(maximum,12)))
    result.sort(key=lambda r:r[0])
    ids=[r[0] for r in result]
    assert len(ids)==len(set(ids)),'Duplicated article output'
    return result
