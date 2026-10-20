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


def asymmetric_config(**overrides):
    options = dict(
        target_seconds=None, repeats=1, smoke=False, preset=None,
        design="asymmetric", comparison=None, checkpoint_delay_s=None,
    )
    return study.configuration(argparse.Namespace(**{**options, **overrides}))


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


@pytest.mark.parametrize("design", ["original", "asymmetric"])
def test_small_search_has_nested_baselines_and_frozen_replay(tmp_path, design):
    c = config() if design == "original" else asymmetric_config()
    counts = study.epoch_counts(c, 8)
    tasks = {
        name: dict(
            epochs=counts[name],
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
    study.plan_case(tmp_path, c, "native")
    search = study.read(folder / "search.json")
    assert [entry["multiplier"] for entry in search["deadline_cases"]] == c["deadline_multipliers"]
    assert search["case"]["deadline_s"] == pytest.approx(
        c["replay_deadline_multiplier"] * search["window_s"]
    )
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
        if any(name in study.MATCHED_POLICIES for name in entry["policies"]):
            assert entry["plan"]["copies"] == {
                "t1": ["spot_a"], "t2": ["safe"], "t3": ["safe"]
            }
            assert entry["plan"]["order"] == ["t1", "t2", "t3"]
    if design == "asymmetric":
        assert {
            name for entry in search["plans"].values() for name in entry["policies"]
        } == set(study.MECHANISM_POLICIES)
        assert search["reference_policy"] == "matched_selective"
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


def test_default_late_faults_and_protected_task_coverage():
    c = config()
    assert max(c["replay_fractions"]) > 1
    assert set(c["replay_fractions"]).isdisjoint(c["planning_fractions"])
    result = dict(
        elapsed_s=1.0,
        worker_busy_s=2.0,
        assumed_cost=1.0,
        met_deadline=True,
        correct=True,
        checkpoint_bytes=100,
        fault_evidence={"spot_a": dict(active_task="t3")},
        restorations=[dict(task="t3", restored_epoch=0)],
    )
    record = dict(
        job=dict(
            repeat=0,
            scenario=dict(name="late", probability=1.0, failures={"spot_a": 1.05}),
        ),
        result=result,
    )
    summary = study.summarize_records([record], 1, ["t3"])
    assert summary["protection_exposure"] == "protected_task_interrupted_but_no_restore"
    result["restorations"][0]["restored_epoch"] = 2
    summary = study.summarize_records([record], 1, ["t3"])
    assert summary["protection_exposure"] == "checkpoint_restore_observed"
    assert summary["protected_task_restores"] == {"t3": 1}
    assert (
        study.summarize_records([record], 1, ["t1"])["protection_exposure"]
        == "protected_tasks_not_interrupted"
    )
    assert (
        study.summarize_records([record], 1)["protection_exposure"]
        == "no_intermediate_checkpoints"
    )


def test_asymmetric_compute_and_checkpoint_cost_are_independent():
    small = asymmetric_config(target_seconds=6)
    large = asymmetric_config(target_seconds=12)
    assert study.checkpoint_delay(small, "storage") == 0.5
    assert study.checkpoint_delay(large, "storage") == 0.5
    assert study.checkpoint_delay(small, "native") == 0
    assert study.epoch_counts(small, 6) == dict(t1=9, t2=6, t3=6)
    assert small["presets"] == ["native", "storage"]
    assert small["width"] == 256
    for preset in study.PRESETS:
        rows = study.failure_scenarios(
            preset, [0.3, 0.7], 10, small["failure_weights"][preset]
        )
        assert sum(r["probability"] for r in rows) == pytest.approx(1)
    with pytest.raises(ValueError):
        asymmetric_config(checkpoint_delay_s=-1)


def test_risky_task_has_its_own_shared_held_out_failure_clock():
    c = asymmetric_config()
    measured = dict(
        tasks={"t1": dict(epochs=9)},
        estimates={"t1": {"spot_a": dict(init_s=0, epoch_s=1, save_s=0.5)}},
    )
    timing = study.failure_timing(c, measured, 13)
    assert timing["risk_reference_s"] == 9.5
    replay_times = [f * 13 for f in timing["replay_fractions"]]
    assert replay_times == pytest.approx([3.8, 7.6, 9.025, 13.65])
    assert set(timing["replay_fractions"]).isdisjoint(timing["planning_fractions"])
    assert study.failure_timing(c, measured, 13) == timing
