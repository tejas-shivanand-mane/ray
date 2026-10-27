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
        self.assertTrue(all(c["strict_improvement"] == "unsat"
                            for c in solved["certificates"].values()))

    def test_zero_budget_forces_recomputation(self):
        solved = self.solve(0)
        self.assertTrue(solved["optimality_proven"])
        self.assertEqual(solved["mask"], 0)
        self.assertEqual(solved["objective"], 44000)

    def test_uploaded_taxi_zone_planning_case(self):
        # Quantized coefficients from the owner's failed 2026-10-26 run.
        # Keep the multiple roots, joins, and branch reuse that its audit saw.
        coefficients = [
            (140631, 6213, 4290, 12226281, []),
            (3048, 1327, 83, 10027, []),
            (76924, 9309, 6002, 18945580, [0, 1]),
            (6436, 973, 91, 17064, [2]),
            (12585, 1193, 161, 612219, [2]),
            (4346, 941, 73, 25212, [2]),
            (6990, 1249, 176, 700076, [4]),
            (2460, 1083, 74, 23316, [3]),
            (5334, 935, 56, 2002, [2]),
            (22708, 1075, 18, 410, [5, 6, 7, 8])]
        stages = [{"name": str(i), "compute_us": c, "write_us": w,
                   "read_us": r, "bytes": b, "deps": deps}
                  for i, (c, w, r, b, deps) in enumerate(coefficients)]
        faults = planner.fault_points(stages, (.15, .35, .55, .75, .95))
        table = planner.make_table(stages, faults, 100)
        optimum = min(score for score in table.values() if score[1] <= 325618)
        self.assertEqual(optimum[2], 0)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "taxi.smt2"
            solved = planner.solve_z3(stages, faults, 100, 325618, 5000, path)
            self.assertTrue(path.with_suffix(".json").exists())
            self.assertTrue(path.with_name("taxi-better-objective.smt2").exists())
        self.assertTrue(solved["optimality_proven"], solved)
        self.assertEqual((solved["objective"], solved["bytes"], solved["mask"]), optimum)
        self.assertEqual(solved["healthy_us"], planner.simulate(stages, solved["mask"]))
        self.assertEqual(solved["failed_us"], [planner.simulate(stages, solved["mask"], f) for f in faults])

    def test_exhausted_budget_is_saved_without_optimality_claim(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "timeout.smt2"
            solved = planner.solve_z3(self.stages, self.faults, 1000, 10, 0, path)
            self.assertTrue(path.with_suffix(".json").exists())
        self.assertFalse(solved["optimality_proven"])
        self.assertEqual(solved["status"], "unknown")


if __name__ == "__main__":
    unittest.main()
