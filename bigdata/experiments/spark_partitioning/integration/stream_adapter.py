"""Experiment-only static adapters. Production files are never edited."""
import argparse
import ast
import hashlib
import inspect
import json
import os
import sys
from pathlib import Path


def rewrite_clustering_function(source):
    tree=ast.parse(source);changed={'selected':0,'clusters':0,'injection':0}
    class Transformer(ast.NodeTransformer):
        def visit_Assign(self,node):
            names=[t.id for t in node.targets if isinstance(t,ast.Name)]
            if names==['theme_names'] and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id=='_load_theme_names':
                changed['injection']+=1
                extra=ast.parse("partitioned_clusters = _experiment_compute(pending, triggered_theme_ids, pending_counts)").body[0]
                return [node,extra]
            if names==['selected']:
                changed['selected']+=1;return ast.copy_location(ast.Pass(),node)
            if names==['clusters'] and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id=='cluster_articles':
                changed['clusters']+=1
                node.value=ast.parse('partitioned_clusters.get(theme_id, ())',mode='eval').body
            return self.generic_visit(node)
    tree=Transformer().visit(tree)
    if changed!={'selected':1,'clusters':1,'injection':1}:raise ValueError('Unexpected production AST; refuse an unsafe adapter rewrite: '+str(changed))
    ast.fix_missing_locations(tree)
    return tree


def fixed_llm(config,theme_name,cluster):
    from issue_clustering import GeneratedIssue,GeneratedIssueDecision,cluster_membership_key
    ids=tuple(a.article_id for a in cluster.articles)
    name='fixture-'+hashlib.sha256(cluster_membership_key(cluster.theme_id,ids).encode()).hexdigest()[:16]
    return GeneratedIssueDecision(GeneratedIssue(name),ids,())


def install_matching(stream,strategy):
    from functools import partial
    import matching
    from placement import article_plan
    original=stream.write_event_batch
    source=inspect.getsource(original)
    tree=ast.parse(source)
    changed=0
    for node in ast.walk(tree):
        if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='match_rows' for t in node.targets):
            node.value=ast.parse('_experiment_matching_rdd(article_df, issues_by_theme)',mode='eval').body;changed+=1
    if changed!=1:raise ValueError('Matching source shape changed')
    def computation(df,broadcast):
        rdd=df.rdd
        if strategy!='A0':
            metadata=df.select('theme_id').rdd.mapPartitionsWithIndex(matching.metadata_partition).collect()
            candidates={t:len(items) for t,items in broadcast.value.items()}
            plan=article_plan(metadata,candidates,strategy=='A2')
            rdd=(rdd.mapPartitionsWithIndex(partial(matching.keyed_partition,plan=plan))
                 .partitionBy(4,lambda target:int(target)).values())
        fn=stream._match_partition
        threshold,event_topic,unclassified_topic=stream.ISSUE_MATCH_THRESHOLD,stream.KAFKA_EVENT_TOPIC,stream.KAFKA_UNCLASSIFIED_TOPIC
        return rdd.mapPartitions(lambda rows:fn(rows,broadcast,threshold,event_topic,unclassified_topic))
    stream._experiment_matching_rdd=computation
    ast.fix_missing_locations(tree);exec(compile(tree,'<experiment matching adapter>','exec'),stream.__dict__)
    return ast.unparse(tree)


def install_clustering(stream,strategy,model):
    import clustering
    if strategy!='AS_IS':
        tree=rewrite_clustering_function(inspect.getsource(stream.write_cluster_batch))
        def computation(pending,theme_ids,count_rows):
            counts={int(r.theme_id):int(r['count']) for r in count_rows if int(r.theme_id) in theme_ids}
            columns=['article_id','theme_id','embedding','title','published_at','unclassified_at','summary']
            selected=pending.filter(pending.theme_id.isin(theme_ids)).select(*columns)
            _,results,_,_=clustering.execute(selected,counts,strategy,model)
            return {r['theme']:r['clusters'] for r in results}
        stream._experiment_compute=computation
        exec(compile(tree,'<experiment clustering adapter>','exec'),stream.__dict__)
        expanded=ast.unparse(tree)
    else:expanded=inspect.getsource(stream.write_cluster_batch)
    stream.generate_issue=fixed_llm
    return expanded


def main():
    p=argparse.ArgumentParser();p.add_argument('--app',choices=['A','B'],required=True)
    p.add_argument('--strategy',required=True);p.add_argument('--source',required=True);p.add_argument('--cost-model',type=Path)
    p.add_argument('--audit-directory',required=True,type=Path);p.add_argument('--execute',action='store_true')
    a=p.parse_args()
    # This executable is a stage-C adapter, not a substitute for Kafka/DB/HDFS deployment and reset validation.
    if not a.execute:
        print(json.dumps(dict(app=a.app,strategy=a.strategy,execute=False,cores_per_app=2,executor_cores=1,total_pipeline_cores=4)))
        return
    if os.getenv('WITW_INTEGRATION_ISOLATED')!='yes':raise RuntimeError('Explicit isolated integration namespace required')
    if not os.getenv('KAFKA_BOOTSTRAP_SERVERS') or 'kafka:29092'==os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
        raise ValueError('Explicit test Kafka endpoint required')
    sys.path.insert(0,a.source);sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'apps'))
    if a.app=='A':
        if a.strategy not in ['A0','A1','A2']:raise ValueError('Invalid matching strategy')
        import enriched_article_stream as stream
        expanded=install_matching(stream,a.strategy)
    else:
        if a.strategy not in ['AS_IS','B0','B1','B2']:raise ValueError('Invalid clustering strategy')
        import unclassified_issue_stream as stream
        if stream.ISSUE_CLUSTER_MAX_ARTICLES_PER_THEME!=2000:raise ValueError('Unexpected cluster cap')
        model=json.loads(a.cost_model.read_text()) if a.cost_model else None
        if a.strategy=='B2' and model is None:raise ValueError('Frozen EC2 cost model required')
        expanded=install_clustering(stream,a.strategy,model)
    a.audit_directory.mkdir(parents=True,exist_ok=True)
    (a.audit_directory/(a.app+'-expanded-adapter.py')).write_text(expanded,encoding='utf-8')
    stream.main()


if __name__=='__main__':main()
