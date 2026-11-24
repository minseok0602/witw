import os, hashlib, json, platform, random, sys, time
from pathlib import Path
import numpy as np
CAP=2000
PARAMS=dict(min_cluster_size=8,min_samples=4,representative_count=4,min_membership_confidence=.80,min_centroid_similarity=.92)

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

def fingerprint(results):
    out = []
    for theme, clusters, _, _ in sorted(results,key=lambda x:x[0]):
        for c in clusters:
            out.append([theme, sorted(a.article_id for a in c.articles),
                        [a.article_id for a in c.representatives],
                        [round(float(x),10) for x in c.centroid], round(c.membership_confidence,10)])
    return hashlib.sha256(json.dumps(out).encode()).hexdigest(), len(out), sum(len(x[1]) for x in out)

def runtime(_):
    import issue_clustering, pyspark, sklearn, scipy, joblib, threadpoolctl
    yield dict(python=platform.python_version(),numpy=np.__version__,sklearn=sklearn.__version__,
               scipy=scipy.__version__,pyspark=pyspark.__version__,joblib=joblib.__version__,
               threadpoolctl=threadpoolctl.__version__,
               source_sha256=hashlib.sha256(Path(issue_clustering.__file__).read_bytes()).hexdigest())
