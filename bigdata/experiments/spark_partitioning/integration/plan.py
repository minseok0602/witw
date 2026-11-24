"""Generate C0..C3 only from completed full A/B results and a reviewed fixed choice."""
import argparse
import json
from pathlib import Path


def main():
    p=argparse.ArgumentParser();p.add_argument('--analysis',type=Path,required=True)
    p.add_argument('--a-choice',choices=['A0','A1','A2'],required=True)
    p.add_argument('--b-choice',choices=['B0','B1','B2'],required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    manifest=json.loads((a.analysis/'analysis_manifest.json').read_text())
    if manifest['mode']!='full' or not manifest['performance_conclusions_allowed']:
        raise ValueError('Quick/exploration cannot select integration winners')
    selection=json.loads((a.analysis/'strategy_selection.json').read_text())
    if {r['app'] for r in selection}!={'A','B'}:raise ValueError('Both full experiments required')
    plan=dict(stage='C',executed=False,total_executor_cores=4,
        application_resources={'A':{'cores_max':2,'executor_cores':1},'B':{'cores_max':2,'executor_cores':1}},
        cases={'C0':['A0','AS_IS'],'C1':[a.a_choice,'AS_IS'],'C2':['A0',a.b_choice],'C3':[a.a_choice,a.b_choice]},
        llm='fixed deterministic response for every case',
        reset_requirements=['fresh isolated Kafka broker/storage per case; DB Writer requires literal news.event.v1; identical topic partitions and producer schedule',
          'restore the same PostgreSQL snapshot including issue IDs and sequences',
          'restore identical HDFS pending/attempt/trigger state and fresh per-case checkpoints'],
        required_metrics=['DB commit timestamp minus producer send timestamp','Kafka lag','microbatch duration',
          'pending backlog','issue-create delay','server CPU/memory/NIC','assignment semantic comparison'],
        caution='Pipeline batching can change eligible clustering membership; keep per-operation equivalence separate from pipeline outcomes',
        reviewed_choices={'A':a.a_choice,'B':a.b_choice})
    a.output.write_text(json.dumps(plan,indent=2));print(a.output)


if __name__=='__main__':main()
