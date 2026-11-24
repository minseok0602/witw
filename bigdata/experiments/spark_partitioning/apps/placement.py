"""Partition plans contain counts and costs only, never article payloads or article IDs."""
import bisect
import heapq

PARTITIONS=4
CAP=2000


def theme_lpt(counts,weights):
    loads=[(0.,p) for p in range(PARTITIONS)]
    heapq.heapify(loads)
    assignment={}
    for theme in sorted(counts,key=lambda t:(-weights[t],t)):
        load,p=heapq.heappop(loads)
        assignment[theme]=p
        heapq.heappush(loads,(load+weights[theme],p))
    return assignment


def article_plan(metadata,candidate_counts,weighted):
    totals={}
    offsets={}
    global_offsets={}
    offset=0
    for pid,counts in sorted(metadata):
        global_offsets[pid]=offset
        offset+=sum(counts.values())
        for theme,n in sorted(counts.items()):
            offsets[pid,theme]=totals.get(theme,0)
            totals[theme]=totals.get(theme,0)+n
    quotas={}
    if weighted:
        # The input to this loop is eight aggregated counts, not 4,000 collected articles.
        costs={t:max(1,candidate_counts.get(t,0))*768 for t in totals}
        loads=[(0.,p) for p in range(PARTITIONS)]
        heapq.heapify(loads)
        for theme in sorted(totals,key=lambda t:(-costs[t],t)):
            q=[0]*PARTITIONS
            for _ in range(totals[theme]):
                load,p=heapq.heappop(loads)
                q[p]+=1
                heapq.heappush(loads,(load+costs[theme],p))
            cumulative=[];s=0
            for n in q:
                s+=n;cumulative.append(s)
            quotas[theme]=cumulative
    return dict(global_offsets=global_offsets,theme_offsets=offsets,quotas=quotas,
                totals=totals,weighted=weighted)


def destination(plan,pid,index,theme,local_theme_index):
    if not plan['weighted']:
        return (plan['global_offsets'][pid]+index)%PARTITIONS
    rank=plan['theme_offsets'][pid,theme]+local_theme_index
    return bisect.bisect_right(plan['quotas'][theme],rank)
