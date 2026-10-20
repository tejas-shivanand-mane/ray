"""Checks for evidence integrity in the selective-checkpoint experiment."""

import argparse
import itertools

import pytest
import state_protection_experiment as study
from workload import write_json


def config():
    return study.configuration(
        argparse.Namespace(target_seconds=1.0, repeats=3, smoke=False, preset="native")
    )


@pytest.mark.parametrize("preset", study.PRESETS)
def test_failure_distribution_and_held_out_times(preset):
    planning = study.failure_scenarios(preset, [0.2, 0.5, 0.8], 10)
    replay = study.failure_scenarios(preset, [0.35, 0.65], 10)
    assert sum(s["probability"] for s in replay) == pytest.approx(1)
    assert sum(s["probability"] for s in planning) == pytest.approx(1)
    assert replay[0]["probability"] == 0.5
    assert {t for s in planning for t in s["failures"].values()}.isdisjoint(
        {t for s in replay for t in s["failures"].values()}
    )
    assert any(set(s["failures"]) == {"spot_a", "spot_b"} for s in replay)
    assert any(set(s["failures"]) == {"safe"} for s in replay)


def row(time, cost, success=1.0):
    return dict(
        expected_elapsed_s=time,
        expected_cost=cost,
        expected_worker_busy_s=time,
        success_probability=success,
        plan={"id": f"{time}-{cost}-{success}"},
    )


def test_frontier_and_selection_respect_reliability():
    candidates = [row(1, 1, 0.5), row(2, 5), row(3, 3), row(4, 4), row(5, 2)]
    best, feasible = study.select(candidates, 0.95)
    assert feasible and best == candidates[1]
    assert study.pareto(candidates, 0.95) == [candidates[i] for i in (1, 2, 4)]
    best, feasible = study.select([row(1, 1, 0.5), row(2, 2, 0.8)], 0.95)
    assert not feasible and best["success_probability"] == 0.8


def test_no_speed_claim_for_reliability_loss_or_reused_plan():
    def summary(key, elapsed, success):
        return dict(
            plan_id=key,
            all_correct=True,
            weighted=dict(elapsed_s=elapsed, assumed_cost=1.0, met_deadline=success),
            per_repeat=[dict(elapsed_s=elapsed)] * 3,
        )

    baseline = summary("b", 10, 1.0)
    assert (
        study.comparison(summary("a", 5, 0.9), baseline, config())["verdict"]
        == "no_claim_deadline_or_correctness"
    )
    assert (
        study.comparison(summary("b", 10, 1), baseline, config())["verdict"]
        == "identical_plan"
    )
    assert (
        study.comparison(summary("a", 9, 1), baseline, config())["verdict"]
        == "candidate_time_advantage"
    )


def test_small_search_has_nested_baselines_and_frozen_replay(tmp_path):
    tasks = {
        name: dict(
            epochs=16 if name == "t1" else 8,
            checkpoint_every=2,
            depends_on=["t2"] if name == "t3" else [],
        )
        for name in ("t1", "t2", "t3")
    }
    machines = {
        m: dict(cost_per_hour=1, accounting={"bill_warm_pool": True})
        for m in ("safe", "spot_a", "spot_b")
    }
    estimates = {
        t: {m: dict(init_s=0.01, epoch_s=0.1, save_s=0.15) for m in machines}
        for t in tasks
    }
    folder = tmp_path / "native"
    folder.mkdir()
    write_json(
        folder / "profile.json",
        dict(tasks=tasks, machines=machines, estimates=estimates),
    )
    study.plan_case(tmp_path, config(), "native")
    search = study.read(folder / "search.json")
    assert [c["multiplier"] for c in search["deadline_cases"]] == [1.25, 1.5, 2.0]
    assert search["case"]["deadline_s"] == pytest.approx(1.5 * search["window_s"])
    uniform, choices = study.menus(tasks)
    for intervals in uniform:
        assert all(intervals[t] in choices[t] for t in tasks)
    for case in search["deadline_cases"]:
        joint = case["policies"]["joint_checkpoint"]
        for name, other in case["policies"].items():
            if other["feasible"]:
                assert joint["feasible"]
                assert (
                    joint["row"]["expected_elapsed_s"]
                    <= other["row"]["expected_elapsed_s"] + 1e-12
                )
    for entry in search["plans"].values():
        assert all(len(ms) == 1 for ms in entry["plan"]["copies"].values())
        assert entry["plan"]["order"].index("t2") < entry["plan"]["order"].index("t3")
    schedules = []
    for plan_id in search["plans"]:
        schedules.append(
            sorted(
                (j["repeat"], j["scenario"]["name"], j["scenario"]["failures"])
                for j in search["jobs"]
                if j["plan_id"] == plan_id
            )
        )
    assert all(a == b for a, b in itertools.combinations(schedules, 2))


def test_native_has_no_artificial_delay_and_controls_share_work():
    tasks = {"t1": dict(epochs=16)}
    native = study.case_config(config(), "native", tasks)
    storage = study.case_config(config(), "storage", tasks)
    assert native["tasks"] == storage["tasks"]
    for machine in native["machines"]:
        assert (
            native["machines"][machine]["execution_by_task"]["t1"]["checkpoint_delay_s"]
            == 0
        )
        assert (
            storage["machines"][machine]["execution_by_task"]["t1"][
                "checkpoint_delay_s"
            ]
            == 0.15
        )
