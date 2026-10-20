"""Hand-constructed optimizer checks; prepared for the project owner to run."""

import unittest

import joint_checkpoint_sweep as study


def table(costs, probabilities=None):
    return [
        dict(
            schedule_id=schedule,
            checkpoint_id=checkpoint,
            expected_cost=cost,
            expected_elapsed_s=1.0,
            expected_worker_busy_s=1.0,
            success_by_deadline=[
                1.0 if probabilities is None else probabilities[schedule][checkpoint]
            ],
        )
        for schedule, values in enumerate(costs)
        for checkpoint, cost in enumerate(values)
    ]


class OptimizerChecks(unittest.TestCase):
    def test_alternating_can_stop_at_a_strict_local_optimum(self):
        # Starting from column 0: (0,0)->(0,1), then neither block improves.
        # Reaching (1,2) requires changing BOTH decisions together.
        rows = table([[5, 4, 8], [6, 7, 1]])
        results = study.solve_methods(rows, [0], 0, 0.95)
        self.assertEqual(results["joint_checkpoint"]["row_index"], 5)
        self.assertEqual(results["alternating_multistart"]["row_index"], 1)
        self.assertEqual(results["alternating_multistart"]["trajectories"], [[0, 1]])

    def test_a_second_start_eliminates_that_apparent_joint_advantage(self):
        rows = table([[5, 4, 8], [6, 7, 1]])
        results = study.solve_methods(rows, [0, 2], 0, 0.95)
        self.assertEqual(results["joint_checkpoint"]["row_index"], 5)
        self.assertEqual(results["sequential_multistart"]["row_index"], 5)
        self.assertEqual(results["alternating_multistart"]["row_index"], 5)

    def test_feasibility_dominates_cost_and_fallback_recovers(self):
        rows = table([[1, 10], [2, 3]], [[0.94, 1.0], [0.8, 0.9]])
        results = study.solve_methods(rows, [0], 0, 0.95)
        self.assertEqual(results["uniform_checkpoint"]["row_index"], 0)
        for method in results.keys() - {"uniform_checkpoint"}:
            self.assertEqual(results[method]["row_index"], 1)
        pair = study.comparison(rows[0], rows[1], 0, 0.95)
        self.assertEqual(pair["verdict"], "only_baseline_feasible")
        self.assertIsNone(pair["cost_gain_fraction"])

    def test_all_ties_terminate_at_the_same_canonical_plan(self):
        results = study.solve_methods(table([[1, 1], [1, 1]]), [0, 1], 0, 0.95)
        self.assertTrue(all(r["row_index"] == 0 for r in results.values()))

    def test_held_out_reversal_is_reported_as_a_loss(self):
        planning = table([[1, 2]])
        held_out = table([[3, 2]])
        self.assertEqual(
            study.comparison(planning[0], planning[1], 0, 0.95)["verdict"],
            "joint_cheaper",
        )
        self.assertEqual(
            study.comparison(held_out[0], held_out[1], 0, 0.95)["verdict"],
            "baseline_cheaper",
        )


if __name__ == "__main__":
    unittest.main()
