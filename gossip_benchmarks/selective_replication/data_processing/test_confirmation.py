"""Optional owner-run exact-DP checks; no data downloads or measured workloads."""

import unittest

import budget_planner as planner
import confirmation


class ConfirmationChecks(unittest.TestCase):
    def setUp(self):
        self.stages = [
            {"name": "large", "deps": [], "compute_us": 9, "write_us": 1, "read_us": 1, "bytes": 6},
            {"name": "small_a", "deps": [], "compute_us": 6, "write_us": 1, "read_us": 1, "bytes": 5},
            {"name": "small_b", "deps": [], "compute_us": 6, "write_us": 1, "read_us": 1, "bytes": 5},
            {"name": "sink", "deps": [0, 1, 2], "compute_us": 1, "write_us": 1, "read_us": 1, "bytes": 1}]

    def test_dp_finds_one_for_two_exchange(self):
        faults = [{"stage": 3, "fraction": 0}]
        table = planner.make_table(self.stages, faults, 1000)
        self.assertEqual(planner.greedy(table, 10, 3, swaps=True), 1)
        self.assertEqual(confirmation.exact_knapsack(self.stages, faults, 1000, 10), 6)

    def test_dp_matches_independent_replay_for_progress_faults_and_budgets(self):
        faults = [{"stage": i, "fraction": .4} for i in range(4)]
        for probability in (0, 10, 100, 300, 1000):
            table = planner.make_table(self.stages, faults, probability)
            for budget in range(17):
                expected = min(score for score in table.values() if score[1] <= budget)
                mask = confirmation.exact_knapsack(self.stages, faults, probability, budget)
                self.assertEqual(table[mask], expected, (probability, budget))

    def test_dp_rejects_dependent_producers(self):
        self.stages[1]["deps"] = [0]
        with self.assertRaises(ValueError):
            confirmation.exact_knapsack(self.stages, [{"stage": 3, "fraction": 0}], 300, 10)


if __name__ == "__main__":
    unittest.main()
