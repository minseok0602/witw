import unittest
from placement import placement, predicted_cost


class PlacementTest(unittest.TestCase):
    def test_hash_collision_and_count_balancing(self):
        counts = {1: 25, 17: 25, 33: 25, 49: 25}
        self.assertEqual(set(placement(counts,4,'to_be_1').values()), {1})
        self.assertEqual(set(placement(counts,4,'to_be_2').values()), {0,1,2,3})

    def test_cost_is_not_just_article_count(self):
        counts = dict(enumerate([9,8,7,6,5],1))
        model = {'coefficients_quadratic_linear_constant': [1.,0.,0.]}
        self.assertNotEqual(placement(counts,2,'to_be_2'), placement(counts,2,'to_be_3',model))

    def test_cap_empty_partitions_and_deterministic_ties(self):
        counts = {1: 2500, 2: 2000, 3: 25}
        result = placement(counts,16,'to_be_2')
        self.assertEqual(result, placement(dict(reversed(list(counts.items()))),16,'to_be_2'))
        self.assertEqual(len(set(result.values())),3)
        model = {'coefficients_quadratic_linear_constant': [1.,1.,1.]}
        self.assertEqual(predicted_cost(2500,model), predicted_cost(2000,model))
        self.assertEqual(set(result), set(counts))


if __name__ == '__main__':
    unittest.main()
