"""Optional owner-run solver checks; constructed fixtures, NOT benchmark evidence."""

from pathlib import Path
import tempfile
import unittest

import budget_planner as planner


class BudgetPlannerChecks(unittest.TestCase):
    def setUp(self):
        # Three independent producers, followed by a consumer. The large
        # producer wins every single-choice comparison, but two smaller saves
        # together beat it. These are abstract unit-test costs, not real timings.
        self.stages = [
            {"name": "large", "deps": [], "compute_us": 9, "write_us": 1, "read_us": 1, "bytes": 6},
            {"name": "small_a", "deps": [], "compute_us": 6, "write_us": 1, "read_us": 1, "bytes": 5},
            {"name": "small_b", "deps": [], "compute_us": 6, "write_us": 1, "read_us": 1, "bytes": 5},
            {"name": "sink", "deps": [0, 1, 2], "compute_us": 1, "write_us": 1, "read_us": 1, "bytes": 1}]
        self.faults = [{"stage": 3, "fraction": 0}]

    def solve(self, budget):
        with tempfile.TemporaryDirectory() as directory:
            return planner.solve_z3(self.stages, self.faults, 1000, budget, 5000,
                                    Path(directory) / "check.smt2")

    def test_one_for_two_exchange_is_not_a_one_for_one_swap(self):
        table = planner.make_table(self.stages, self.faults, 1000)
        self.assertEqual(planner.greedy(table, 10, 3), 1)
        self.assertEqual(planner.greedy(table, 10, 3, swaps=True), 1)
        # Exact imperative totals: large-only takes 37 units, two-small takes 36.
        self.assertEqual(planner.simulate(self.stages, 1, self.faults[0]), 37)
        self.assertEqual(planner.simulate(self.stages, 6, self.faults[0]), 36)
        solved = self.solve(10)
        self.assertTrue(solved["optimality_proven"])
        self.assertEqual(solved["mask"], 6)
        self.assertEqual(solved["objective"], table[6][0])

    def test_zero_budget_forces_recomputation(self):
        solved = self.solve(0)
        self.assertTrue(solved["optimality_proven"])
        self.assertEqual(solved["mask"], 0)
        self.assertEqual(solved["objective"], 44000)


if __name__ == "__main__":
    unittest.main()
