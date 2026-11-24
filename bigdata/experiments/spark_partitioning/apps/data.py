import hashlib
import json
import random
import numpy as np

DIM=768
STAMP='2026-11-24T00:00:00Z'
CANDIDATES=[200,100,50,10,150,80,30,20]
A_COUNTS={'D1':[500]*8,'D2':[1200,800,600,400,300,300,200,200],
          'D3':[3000,400,200,100,100,100,50,50],'D4':[4000,0,0,0,0,0,0,0]}
B_COUNTS={'D1':[250]*8,'D2':[600,400,300,200,150,150,100,100],
          'D3':[1500,200,100,50,50,50,25,25],'D4':[2000,0,0,0,0,0,0,0]}


def rotate_counts(counts,seed):
    slots=list(range(1,9));random.Random(seed).shuffle(slots)
    return {t:n for t,n in zip(slots,counts) if n}


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def issue_snapshot():
    from issue_matching import IssueCandidate
    rng=np.random.default_rng(510001)
    result={}
    for t,n in enumerate(CANDIDATES,1):
        vectors=rng.normal(size=(n,DIM));vectors/=np.linalg.norm(vectors,axis=1,keepdims=True)
        vectors[1]=vectors[0]  # deterministic same-score tie: the lower issue ID must win.
        result[t]=[IssueCandidate(t*1000+i+1,t,tuple(v)) for i,v in enumerate(vectors)]
    return result


def matching_rows(counts,seed,issues):
    rng=np.random.default_rng(seed);rows=[]
    for theme,n in sorted(counts.items()):
        for i in range(n):
            candidate=issues[theme][i%len(issues[theme])].embedding
            if i%5==0:
                v=rng.normal(size=DIM) # unassigned: random unit vector, checked by real matching.
            else:
                v=np.array(candidate)+rng.normal(0,.001,size=DIM)
            v/=np.linalg.norm(v)
            aid=f'a-{theme:02d}-{i:05d}'
            rows.append((aid,theme,v.tolist(),'fixture title','fixture summary',STAMP,STAMP))
    random.Random(seed+1).shuffle(rows)
    return rows


def clustering_rows(counts,seed):
    rng=np.random.default_rng(seed);rows=[]
    for theme,n in sorted(counts.items()):
        k=max(2,min(6,n//12));centers=rng.normal(size=(k,DIM))
        centers/=np.linalg.norm(centers,axis=1,keepdims=True)
        for i in range(n):
            v=centers[i%k]+rng.normal(0,.004 if i%10 else .012,size=DIM)
            v/=np.linalg.norm(v)
            stamp=f'2026-11-24T00:{(i//60)%60:02d}:{i%60:02d}Z'
            rows.append((f'b-{theme:02d}-{i:05d}',theme,v.tolist(),'fixture title',stamp,stamp,None))
    random.Random(seed+1).shuffle(rows)
    return rows
