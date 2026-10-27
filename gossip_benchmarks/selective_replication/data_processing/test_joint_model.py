"""Owner-run checks for node-correlated loss and exact planners."""

import copy
from pathlib import Path
import tempfile
import time
import unittest

import joint_model as model


def fixture():
    config = {"nodes": [
        {"name": "spot", "kind": "spot", "failure_probability": .5, "usd_per_hour": .03,
         "speed": 1., "restart_s": 2., "transfer_mib_s": 100},
        {"name": "regular", "kind": "regular", "failure_probability": .25, "usd_per_hour": .1,
         "speed": 1.2, "restart_s": 1., "transfer_mib_s": 100}],
        "checkpoint_mib_s": 50, "reducer_usd_per_hour": .1, "storage_fraction": .65,
        "deadline_factor": 2., "minimum_deadline_success": .5}
    profile = {"tasks": [
        {"compute_us": 100000 * i, "write_us": 100, "read_us": 50, "bytes": 100 * i,
         "input_bytes": 200 * i} for i in (1, 2, 3)], "reduce_us": 10000}
    return model.make_case(profile, config, {"name": "test", "spot_multiplier": 1.}, 2.)


class JointModelTests(unittest.TestCase):
    def test_node_loss_is_shared_and_unused_node_loss_has_no_effect(self):
        case = fixture()
        value = model.evaluate(case, [0, 0, 0])
        lost_work = case["restart_us"][0] + sum(opts[0]["recover_us"] for opts in case["options"])
        for scenario, duration in zip(case["scenarios"], value["scenario_us"]):
            self.assertEqual(duration, value["healthy_us"] + (lost_work if scenario["lost_nodes"] & 1 else 0))
        self.assertEqual(sum(s["weight"] for s in case["scenarios"]), case["weight_total"])
        self.assertEqual(value["objective"] * 2,
                         case["weight_total"] * (2 * value["healthy_us"] + lost_work))

    def test_checkpoints_restore_outputs_but_still_pay_restart(self):
        case = fixture()
        value = model.evaluate(case, [1, 1, 1])
        extra = case["restart_us"][0] + sum(opts[1]["recover_us"] for opts in case["options"])
        self.assertEqual(max(value["scenario_us"]) - value["healthy_us"], extra)
        self.assertEqual(value["cost_score"], value["objective"] * (case["rates"][0] + case["reducer_rate"]))

    def test_dp_and_z3_match_exhaustive_under_constraints(self):
        with tempfile.TemporaryDirectory() as directory:
            for budget in (0, 200, 600):
                case = fixture()
                case["storage_budget"] = budget
                oracle = model.exhaustive(case, time.perf_counter() + 10)
                optimum = None if oracle is None else model.evaluate(case, oracle)["objective"]
                dp = model.exact_dp(case, time.perf_counter() + 10, 100000)
                z3 = model.solve_z3(case, time.perf_counter() + 10, Path(directory) / "case.smt2")
                for result in (dp, z3):
                    self.assertTrue(result["optimality_proven"], result)
                    actual = None if result["plan"] is None else model.evaluate(case, result["plan"])["objective"]
                    self.assertEqual(actual, optimum)

    def test_resource_limit_does_not_claim_optimality(self):
        case = fixture()
        result = model.exact_dp(case, time.perf_counter() + 10, 1)
        self.assertEqual(result["status"], "state_limit")
        self.assertFalse(result["optimality_proven"])
        impossible = copy.deepcopy(case)
        impossible["cost_budget"] = 0
        result = model.exact_dp(impossible, time.perf_counter() + 10, 100000)
        self.assertEqual(result["status"], "infeasible")
        self.assertTrue(result["optimality_proven"])


if __name__ == "__main__":
    unittest.main()
