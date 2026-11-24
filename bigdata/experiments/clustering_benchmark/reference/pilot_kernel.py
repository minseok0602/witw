# Computational subset transcribed from develop issue_clustering.py (blob 1c34ad0447569e9ce3c24697bf6e2dbc7a44e81d).
from dataclasses import dataclass
import math
@dataclass(frozen=True)
class UnclassifiedArticle:
    article_id:str; theme_id:int; embedding:tuple; title:str; published_at:str; unclassified_at:str; summary:str|None=None
@dataclass(frozen=True)
class ClusterCandidate:
    theme_id:int; articles:tuple; centroid:tuple; representatives:tuple; membership_confidence:float

def cosine_similarity(left,right):
    l=tuple(float(x) for x in left);r=tuple(float(x) for x in right)
    if not l or len(l)!=len(r):return None
    dot=sum(a*b for a,b in zip(l,r));ln=math.sqrt(sum(x*x for x in l));rn=math.sqrt(sum(x*x for x in r))
    return max(-1.,min(1.,dot/(ln*rn))) if ln and rn else None

def _normalized_centroid(embeddings):
    c=tuple(sum(e[i] for e in embeddings)/len(embeddings) for i in range(len(embeddings[0])));n=math.sqrt(sum(x*x for x in c));return tuple(x/n for x in c)

def cluster_articles(articles,min_cluster_size,min_samples=None,representative_count=8,min_membership_confidence=0.,min_centroid_similarity=-1.):
    if len(articles)<min_cluster_size:return ()
    from sklearn.cluster import HDBSCAN
    model=HDBSCAN(min_cluster_size=min_cluster_size,min_samples=min_samples,metric='euclidean',cluster_selection_method='leaf',allow_single_cluster=False)
    labels=model.fit_predict([a.embedding for a in articles]);probabilities=model.probabilities_;clusters=[]
    for label in sorted(set(int(v) for v in labels if int(v)>=0)):
        member_indices=[i for i,(al,p) in enumerate(zip(labels,probabilities)) if int(al)==label and float(p)>=min_membership_confidence]
        if len(member_indices)<min_cluster_size:continue
        while True:
            centroid=_normalized_centroid([articles[i].embedding for i in member_indices]);cohesive_indices=[i for i in member_indices if (s:=cosine_similarity(articles[i].embedding,centroid)) is not None and s>=min_centroid_similarity]
            if len(cohesive_indices)<min_cluster_size:member_indices=[];break
            if cohesive_indices==member_indices:break
            member_indices=cohesive_indices
        if not member_indices:continue
        members=tuple(articles[i] for i in member_indices);confidence=sum(float(probabilities[i]) for i in member_indices)/len(member_indices)
        def representative_key(a):
            s=cosine_similarity(a.embedding,centroid);return (-(s if s is not None else -1.),a.article_id)
        representatives=tuple(sorted(members,key=representative_key)[:representative_count]);clusters.append(ClusterCandidate(members[0].theme_id,members,centroid,representatives,confidence))
    return tuple(sorted(clusters,key=lambda item:item.articles[0].article_id))
