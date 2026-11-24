"""Controlled initial placement profiles. Cost/semantic payloads stay fixed across conditions."""
import random

RELIABILITY_SCENARIOS=['NATURAL','MODERATE','SKEW']
B_SIZES=[450,400,350,300,150,150,100,100]
SEVERE_A={t:pid for pid,pair in enumerate([(1,5),(2,6),(3,7),(4,8)]) for t in pair}


def matching_destination(theme,article_id,scenario,rotation):
    index=int(article_id.rsplit('-',1)[1])
    base=SEVERE_A[int(theme)]
    if scenario=='MODERATE' and index>=300:base=(index-300)//50
    return (base+rotation)%4


def place_matching_row(row,scenario,rotation):
    return matching_destination(row.theme_id,row.article_id,scenario,rotation),tuple(row)


def theme_aliases(scenario,seed,rotation):
    if scenario=='NATURAL':
        ids=list(range(1,9));random.Random(seed+63).shuffle(ids)
    elif scenario=='MODERATE':ids=[1,5,2,6,3,7,4,8]
    else:ids=[1,5,9,13,2,3,4,8]
    return {semantic:physical+rotation%4 for semantic,physical in enumerate(ids,1)}


def case_counts(app,scenario,seed,rotation):
    if app=='A':return {t:500 for t in range(1,9)}
    aliases=theme_aliases(scenario,seed,rotation)
    return {aliases[t]:n for t,n in enumerate(B_SIZES,1)}
