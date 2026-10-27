"""User-run checks; these tests do not launch Ray, solvers, or cloud jobs."""

import unittest
from ranked_simulation import DEFAULTS, checkpoints, generate_case, placement
from joint_model import evaluate
import random


class RankedSimulationChecks(unittest.TestCase):
    def test_checkpoint_expected_cost_matches_hand_calculation(self):
        # One task on a node with 50% loss; compute=10s, write=1s, restore=2s.
        case = dict(nodes=[{}], probabilities_per_thousand=[500],
                    options=[[dict(node=0, checkpoint=False, bytes=0,
                                   transfer_us=0, map_us=10_000_000, recover_us=10_000_000),
                              dict(node=0, checkpoint=True, bytes=100,
                                   transfer_us=0, map_us=11_000_000, recover_us=2_000_000)]],
                    scenarios=[dict(lost_nodes=0, weight=1), dict(lost_nodes=1, weight=1)],
                    weight_total=2, restart_us=[0], rates=[100_000], reducer_rate=0,
                    reduce_us=0, storage_budget=100, deadline_us=100_000_000,
                    required_success_weight=0, cost_budget=10**60)
        self.assertEqual(evaluate(case, [0])["expected_s"], 15.0)
        self.assertEqual(checkpoints(case, [0], "selective", 0.5), [1])
        self.assertEqual(evaluate(case, [1])["expected_s"], 12.0)
        case["storage_budget"] = 99
        self.assertEqual(checkpoints(case, [0], "selective", 0.5), [0])
        # Cheap recomputation and expensive writes must reject protection.
        case["storage_budget"] = 100
        case["options"][0][1]["map_us"] = 20_000_000
        self.assertEqual(checkpoints(case, [0], "selective", 0.5), [0])

    def test_case_generation_and_shared_placement(self):
        for index in range(5):
            record = generate_case(index, DEFAULTS)
            self.assertEqual(record, generate_case(index, DEFAULTS))
            case = record["case"]
            self.assertEqual(sum(s["weight"] for s in case["scenarios"]), case["weight_total"])
            for kind in ("random", "ranked", "balanced"):
                base = placement(case, kind, random.Random(7), DEFAULTS["weights"])
                for policy in ("none", "uniform", "selective"):
                    plan = checkpoints(case, base, policy, 0.25)
                    self.assertEqual([x // 2 for x in plan], [x // 2 for x in base])
                    self.assertLessEqual(evaluate(case, plan)["bytes"], case["storage_budget"])
                    # The common post-map failure model never finishes sooner
                    # after losing a node than in its no-loss scenario.
                    metrics = evaluate(case, plan)
                    self.assertTrue(all(t >= metrics["healthy_us"] for t in metrics["scenario_us"]))


if __name__ == "__main__":
    unittest.main()
