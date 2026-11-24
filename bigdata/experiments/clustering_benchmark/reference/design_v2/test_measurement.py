import json
import tempfile
import unittest
from pathlib import Path
from benchmark import route
from event_metrics import parse


class MeasurementTest(unittest.TestCase):
    def test_policy_requires_advantage_over_control(self):
        cal=[dict(counts=[100]*4,medians={'as_is':10,'control':2,'to_be_1':3})]
        self.assertEqual(route([100]*4,cal),'control')
        cal[0]['medians']['to_be_1']=1.7
        self.assertEqual(route([100]*4,cal),'to_be_1')

    def test_stage_submission_owns_tasks_and_nanoseconds_stay_nanoseconds(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            (root/'spark-events').mkdir()
            events=[
                {'Event':'SparkListenerJobStart','Job ID':1,'Properties':{'spark.jobGroup.id':'a'},'Stage IDs':[1]},
                {'Event':'SparkListenerStageSubmitted','Stage Info':{'Stage ID':1},'Properties':{'spark.jobGroup.id':'a'}},
                # Later job references the same stage. Task ownership remains the submitted attempt.
                {'Event':'SparkListenerJobStart','Job ID':2,'Properties':{'spark.jobGroup.id':'b'},'Stage IDs':[1]},
                {'Event':'SparkListenerTaskEnd','Stage ID':1,'Task Info':{'Task ID':1,'Launch Time':10,'Finish Time':110},
                 'Task End Reason':{'Reason':'Success'},'Task Metrics':{'Executor Run Time':60,
                   'Executor CPU Time':123000000,'Executor Deserialize Time':10,'Result Serialization Time':5,
                   'Shuffle Read Metrics':{'Remote Bytes Read':100,'Local Bytes Read':20},
                   'Shuffle Write Metrics':{'Shuffle Bytes Written':90}}},
            ]
            (root/'spark-events/events').write_text('\n'.join(map(json.dumps,events)))
            rows={r['group']:r for r in parse(root)}
            self.assertEqual(rows['a']['tasks'],1)
            self.assertEqual(rows['a']['executor_run_ms'],60)
            self.assertEqual(rows['a']['executor_cpu_ns'],123000000)
            self.assertEqual(rows['a']['shuffle_read_bytes'],120)
            self.assertEqual(rows['a']['scheduler_residual_ms'],25)
            self.assertEqual(rows['b']['jobs'],1)
            self.assertEqual(rows['b']['tasks'],0)

    def test_empty_logs_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(RuntimeError):
                parse(Path(temp))


if __name__=='__main__':
    unittest.main()
