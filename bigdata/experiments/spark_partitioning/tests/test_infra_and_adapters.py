import ast
import importlib.util
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'infra'))
from make_template import template
from node_bootstrap import extract
spec=importlib.util.spec_from_file_location('stream_adapter',ROOT/'integration/stream_adapter.py')
adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)


class InfrastructureContractTest(unittest.TestCase):
    def test_only_three_instances_same_subnet_and_only_head_public(self):
        resources=template()['Resources']
        nodes={n:r for n,r in resources.items() if r['Type']=='AWS::EC2::Instance'}
        self.assertEqual(set(nodes),{'Head','Worker1','Worker2'})
        self.assertEqual([nodes[n]['Properties']['InstanceType'] for n in nodes],['m7i.xlarge','m7i.large','m7i.large'])
        for name,r in nodes.items():
            nic=r['Properties']['NetworkInterfaces'][0]
            self.assertEqual(nic['SubnetId'],{'Ref':'Subnet'})
            self.assertEqual(nic['AssociatePublicIpAddress'],name=='Head')
            self.assertTrue(r['Properties']['BlockDeviceMappings'][0]['Ebs']['DeleteOnTermination'])
        self.assertFalse(any(r['Type']=='AWS::EC2::NatGateway' for r in resources.values()))

    def test_budget_role_cannot_terminate_or_stop_unowned_resources(self):
        role=template()['Resources']['HeadRole']['Properties']
        policies=role['Policies'][0]['PolicyDocument']['Statement']
        writes=[p for p in policies if p['Action']!=['ec2:DescribeInstances']]
        self.assertEqual(len(writes),1)
        self.assertEqual(writes[0]['Action'],['ec2:StopInstances'])
        self.assertEqual(writes[0]['Condition']['StringEquals']['aws:ResourceTag/OwnerStackId'],{'Ref':'AWS::StackId'})

    def test_bootstrap_rejects_archive_traversal(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);archive=root/'bad.tar'
            with tarfile.open(archive,'w') as tar:
                info=tarfile.TarInfo('../escape');info.size=0;tar.addfile(info)
            with self.assertRaises(ValueError):extract(archive,root/'destination')
            self.assertFalse((root/'escape').exists())

    def test_stream_adapter_changes_only_compute_placement(self):
        source=(ROOT.parents[1]/'spark/apps/unclassified_issue_stream.py').read_text(encoding='utf-8-sig')
        original=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='write_cluster_batch')
        old=ast.unparse(original)
        result=adapter.rewrite_clustering_function(old)
        expanded=ast.unparse(result)
        for name in ['_write_pending_state','_write_trigger_state','_write_attempt_state','generate_issue','decide_cluster_attempt']:
            self.assertEqual(old.count(name),expanded.count(name))
        self.assertIn('_experiment_compute(pending, triggered_theme_ids, pending_counts)',expanded)
        self.assertIn('partitioned_clusters.get(theme_id, ())',expanded)
        with self.assertRaises(ValueError):adapter.rewrite_clustering_function('def changed(): pass')


if __name__=='__main__':unittest.main()
