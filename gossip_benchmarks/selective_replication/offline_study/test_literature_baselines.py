"""Independent small checks for the owner to run; never auto-run by the harness."""

import itertools
import math
import unittest

import joint_checkpoint_sweep as study
import literature_baselines as literature


class PublishedKernelChecks(unittest.TestCase):
    def test_checkfreq_paper_one_unit_example(self):
        result = literature.checkfreq_algorithm1(1, 1, 1, 1, 0, 1, 0, 0, 0.05)
        self.assertEqual(result["interval"], 20)
        self.assertEqual(result["mode"], "CPU")

    def test_checkfreq_gpu_capacity_and_persistence_limit(self):
        result = literature.checkfreq_algorithm1(1, 0.5, 2, 0.1, 20, 1, 0, 2, 0.05)
        self.assertEqual(result["mode"], "GPU")
        self.assertEqual(result["interval"], 22)
        # Exactly enough space does not pass the paper's strict > condition.
        result = literature.checkfreq_algorithm1(1, 0.5, 2, 0.1, 20, 1, 1, 2, 0.05)
        self.assertEqual(result["mode"], "CPU")
        self.assertEqual(result["interval"], 30)

    def test_han_skip_edge_io_is_counted(self):
        edges = [(-1, 0, 2), (0, 1, 3), (0, 2, 7), (1, 2, 5),
                 (1, 3, 11), (2, 3, 13), (3, 4, 17)]
        times = literature.han_segment_times([1, 2, 3, 4], edges, 0)
        self.assertEqual(times[1, 3], 10 + 5 + 24)
        self.assertEqual(times[0, 4], 2 + 10 + 17)
        self.assertEqual(literature.han_checkpoint_dp(times, 4)["checkpoint_after"], [3])

    def test_han_repeated_failures_include_io_and_downtime(self):
        times = literature.han_segment_times([3], [(-1, 0, 2), (0, 1, 5)], 0.1, 7)
        self.assertAlmostEqual(times[0, 1], 17 * (math.e - 1))

    def test_han_dp_matches_independent_enumeration_of_checkpoint_subsets(self):
        work = [3, 21, 4, 12]
        edges = [(-1, 0, 1), (0, 1, 2), (0, 2, 3), (1, 2, 1),
                 (1, 3, 4), (2, 3, 2), (3, 4, 1)]
        times = literature.han_segment_times(work, edges, 0.03, 2)
        for required in ((), (2,)):
            costs = []
            for mask in itertools.product((False, True), repeat=3):
                cuts = [0] + [i + 1 for i, chosen in enumerate(mask) if chosen] + [4]
                if not set(required) <= set(cuts):
                    continue
                costs.append(sum(times[a, b] for a, b in zip(cuts, cuts[1:])))
            dp = literature.han_checkpoint_dp(times, 4, required)
            self.assertAlmostEqual(dp["expected_time_s"], min(costs))
            self.assertTrue(set(required) <= {i + 1 for i in dp["checkpoint_after"]})

    def test_fork_join_uses_four_tasks_and_valid_orders(self):
        spec = next(s for s in study.family_grid("literature") if s["workload"] == "forkjoin4")
        tasks, _, profiles = study.make_problem(spec)
        schedules, checkpoints, _ = study.plan_space(tasks, (2, 4))
        self.assertEqual(len(tasks), 4)
        self.assertEqual(len(schedules), 162)
        self.assertTrue(all(s["order"][0] == "t1" and s["order"][-1] == "t4" for s in schedules))
        self.assertTrue(all(set(c) == set(tasks) for c in checkpoints))
        self.assertEqual(set(profiles), set(tasks))

    def test_interval_adapters_return_shared_candidates_and_record_clipping(self):
        spec = study.family_grid("short")[0]
        problem = study.make_problem(spec)
        schedules, checkpoints, _ = study.plan_space(problem[0], (2, 4))
        scenarios = [dict(probability=1.0, failures={})]
        rules = literature.interval_candidates(problem, schedules, checkpoints, scenarios, 100)
        self.assertTrue(all(rate == 0 for rate in rules["equivalent_rates_per_s"].values()))
        for method in literature.INTERVAL_METHODS:
            for rid in rules["candidates"][method]:
                self.assertLess(rid, len(schedules) * len(checkpoints))
        for rid in rules["candidates"]["young_interval_adapted"]:
            checkpoint = checkpoints[rid % len(checkpoints)]
            self.assertEqual(checkpoint, {t: v["epochs"] for t, v in problem[0].items()})


if __name__ == "__main__":
    unittest.main()
