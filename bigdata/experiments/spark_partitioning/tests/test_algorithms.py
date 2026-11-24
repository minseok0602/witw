import collections
import json
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'apps'))
sys.path.insert(0,str(ROOT.parents[1]/'spark/apps'))
from placement import article_plan,destination,theme_lpt
from data import A_COUNTS,B_COUNTS,rotate_counts,CANDIDATES
from issue_matching import IssueCandidate,find_best_issue,build_issue_result


class PartitionContractTest(unittest.TestCase):
    def test_article_quotas_are_applied_without_collecting_article_ids(self):
        metadata=[(0,{1:1300,2:100}),(1,{3:180,4:120}),(2,{5:650,6:850}),(3,{7:400,8:400})]
        candidates=dict(enumerate(CANDIDATES,1))
        for weighted in (False,True):
            plan=article_plan(metadata,candidates,weighted)
            actual=[collections.Counter() for _ in range(4)]
            for pid,counts in metadata:
                index=0
                for theme,n in counts.items():
                    for local in range(n):
                        target=destination(plan,pid,index,theme,local)
                        self.assertIn(target,range(4));actual[target][theme]+=1;index+=1
            self.assertEqual(sum(sum(c.values()) for c in actual),4000)
            if not weighted:self.assertEqual([sum(c.values()) for c in actual],[1000]*4)
            else:
                costs=[sum(candidates[t]*768*n for t,n in c.items()) for c in actual]
                self.assertLessEqual(max(costs)-min(costs),max(CANDIDATES)*768)
                for t,cumulative in plan['quotas'].items():
                    self.assertEqual([c[t] for c in actual],[cumulative[i]-(cumulative[i-1] if i else 0) for i in range(4)])

    def test_whole_theme_lpt_does_not_hash_destination_again(self):
        counts={1:25,5:25,9:25,13:25}
        plan=theme_lpt(counts,counts)
        self.assertEqual(set(plan),set(counts))
        self.assertEqual(set(plan.values()),{0,1,2,3})
        self.assertEqual({t%4 for t in counts},{1})

    def test_distribution_rotation_preserves_totals(self):
        for profiles,total in [(A_COUNTS,4000),(B_COUNTS,2000)]:
            for counts in profiles.values():
                for seed in range(5):
                    actual=rotate_counts(counts,seed)
                    self.assertEqual(sum(actual.values()),total)
                    self.assertEqual(sorted(actual.values()),sorted(n for n in counts if n))
                    self.assertEqual(actual,rotate_counts(counts,seed))

    def test_original_tie_policy_zero_candidate_and_threshold(self):
        candidates=[IssueCandidate(2,1,(1.,0.)),IssueCandidate(1,1,(1.,0.))]
        self.assertEqual(find_best_issue((1.,0.),candidates,.93).issue_id,1)
        self.assertIsNone(find_best_issue((0.,1.),candidates,.93))
        article=dict(article_id='a',theme_id=1,embedding=[1.,0.],title='t',summary=None,
                     published_at='2026-11-24T00:00:00Z',occurred_at='2026-11-24T00:00:00Z')
        result=build_issue_result(article,[],.93,'event','unclassified')
        self.assertEqual(result[0],'unclassified')
        self.assertEqual(json.loads(result[2])['article_id'],'a')


if __name__=='__main__':unittest.main()
