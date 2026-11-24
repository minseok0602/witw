import collections
import sys
import unittest
from pathlib import Path

LAB=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(LAB/'apps'));sys.path.insert(0,str(LAB/'infra'))
from profiles import case_counts,matching_destination,RELIABILITY_SCENARIOS
from make_template import template


class ReliabilityProfilesTest(unittest.TestCase):
    def test_matching_equal_counts_with_distinct_workload_skew(self):
        candidates=[200,100,50,10,150,80,30,20]
        for rotation in range(4):
            for scenario,expected in [('MODERATE',[41000,56000,86000,137000]),('SKEW',[15000,40000,90000,175000])]:
                counts=[0]*4;costs=[0]*4
                for theme in range(1,9):
                    for index in range(500):
                        pid=matching_destination(theme,f'a-{theme:02d}-{index:05d}',scenario,rotation)
                        counts[pid]+=1;costs[pid]+=candidates[theme-1]
                self.assertEqual(counts,[1000]*4);self.assertEqual(sorted(costs),expected)

    def test_clustering_retains_sizes_but_changes_hash_collisions(self):
        for rotation in range(4):
            for scenario in RELIABILITY_SCENARIOS:
                counts=case_counts('B',scenario,910000,rotation)
                self.assertEqual(sorted(counts.values()),[100,100,150,150,300,350,400,450])
                loads=[sum(n for t,n in counts.items() if t%4==pid) for pid in range(4)]
                if scenario=='MODERATE':self.assertEqual(sorted(loads),[200,300,650,850])
                if scenario=='SKEW':self.assertEqual(sorted(loads),[150,150,200,1500])

    def test_watchdog_has_absolute_deadline_and_public_key_only(self):
        t=template();head=t['Resources']['Head']['Properties']['UserData']['Fn::Base64']['Fn::Sub']
        self.assertIn('OnCalendar=$deadline',head);self.assertNotIn('OnBootSec=',head)
        self.assertIn('state/deadline_epoch',head)
        self.assertEqual(t['Resources']['OwnedKeyPair']['Properties']['PublicKeyMaterial'],{'Ref':'PublicKeyMaterial'})


if __name__=='__main__':unittest.main()
