import copy
import itertools

import pytest
from planner import Schedule, lease_cost, search, simulate


@pytest.fixture
def problem():
    tasks = {
        "a": {"epochs": 4, "checkpoint_every": 2},
        "b": {"epochs": 2, "checkpoint_every": 2},
    }
    machines = {m: {"cost_per_hour": 3600} for m in ("safe", "spot")}
    profiles = {
        t: {m: {"init_s": 0, "epoch_s": 1, "save_s": 0} for m in machines}
        for t in tasks
    }
    return tasks, machines, profiles


def test_checkpoint_retry_and_loss_boundary(problem):
    tasks, machines, profiles = problem
    tasks = {"a": tasks["a"]}
    plan = {"order": ["a"], "copies": {"a": ["spot"]}}
    # At 2.5, two epochs survive; at exactly 2 the loss wins the tie.
    recovered = simulate(plan, tasks, machines, profiles, {"spot": 2.5}, 20)
    tie = simulate(plan, tasks, machines, profiles, {"spot": 2}, 20)
    assert recovered["elapsed_s"] == 4.5
    assert tie["elapsed_s"] == 6
    assert recovered["attempts"] == 2


def test_replica_survives_and_first_result_cancels(problem):
    tasks, machines, profiles = problem
    tasks = {"a": tasks["a"]}
    plan = {"order": ["a"], "copies": {"a": ["spot", "safe"]}}
    row = simulate(plan, tasks, machines, profiles, {"spot": 1}, 20)
    assert row["complete"] and row["elapsed_s"] == 4
    assert row["cost"] == 5


def test_busy_machine_is_not_oversubscribed(problem):
    tasks, machines, profiles = problem
    plan = {"order": ["a", "b"], "copies": {"a": ["safe"], "b": ["safe"]}}
    assert simulate(plan, tasks, machines, profiles, {}, 20)["elapsed_s"] == 6


def test_failure_before_dispatch_and_correlated_loss(problem):
    tasks, machines, profiles = problem
    plan = {"order": ["a", "b"], "copies": {"a": ["spot"], "b": ["spot"]}}
    assert simulate(plan, tasks, machines, profiles, {"spot": 0}, 20)["elapsed_s"] == 6
    row = simulate(plan, tasks, machines, profiles, {"safe": 0, "spot": 0}, 20)
    assert not row["complete"] and row["cost"] == 0


def test_cancelled_backups_do_not_commit_twice(problem):
    _, machines, _ = problem
    schedule = Schedule(
        {"order": ["a", "b"], "copies": {"a": ["safe", "spot"], "b": ["spot"]}},
        machines,
    )
    assert schedule.dispatch() == [("a", "safe"), ("b", "spot")]
    schedule.finish("a")
    schedule.finish("b")
    assert schedule.dispatch() == []


def test_lease_charges_idle_and_stops_at_loss(problem):
    _, machines, _ = problem
    assert lease_cost(machines, {"safe": 0, "spot": 2}, {"spot": 3}, 10) == 11


def test_joint_contains_every_baseline_and_reports_infeasible(problem):
    tasks, machines, profiles = problem
    scenarios = [
        {"probability": 0.5, "failures": {}},
        {"probability": 0.5, "failures": {"spot": 1}},
    ]
    result = search(tasks, machines, profiles, scenarios, 7, 1)
    assert result["candidate_count"] == 32  # four placements per task, two orders
    joint = result["policies"]["joint_enumeration"]
    for row in result["policies"].values():
        if row:
            assert joint["expected_cost"] <= row["expected_cost"] + 1e-12
    assert all(
        row is None
        for row in search(tasks, machines, profiles, scenarios, 0.1, 1)[
            "policies"
        ].values()
    )


def test_small_exhaustive_model_matches_independent_no_failure_schedule(problem):
    tasks, machines, profiles = problem
    for placement in itertools.product(machines, repeat=2):
        plan = {
            "order": ["a", "b"],
            "copies": {t: [m] for t, m in zip(tasks, placement)},
        }
        expected = max(
            sum(
                tasks[t]["epochs"]
                for t, assigned in zip(tasks, placement)
                if assigned == m
            )
            for m in machines
        )
        assert (
            simulate(plan, tasks, machines, profiles, {}, 20)["elapsed_s"] == expected
        )


@pytest.mark.parametrize(
    "scenarios",
    [
        [{"probability": 0.2, "failures": {}}],
        [{"probability": 1, "failures": {"missing": 1}}],
        [{"probability": 1, "failures": {"spot": -1}}],
    ],
)
def test_reject_invalid_failure_models(problem, scenarios):
    with pytest.raises(ValueError):
        search(*problem, scenarios, 10)


@pytest.mark.parametrize("dtype", ["float32", "float64"])
def test_real_checkpoint_restores_optimizer_and_input_state(tmp_path, dtype):
    import numpy as np
    import torch
    from workload import train, validate_state

    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    rng = np.random.default_rng(4)
    data = dict(
        x=rng.normal(size=(32, 64)).astype("float32"),
        y=np.arange(32, dtype="int64") % 10,
        ids=np.arange(32),
        test_x=rng.normal(size=(8, 64)).astype("float32"),
        test_y=np.arange(8, dtype="int64"),
    )
    task = dict(
        width=8,
        seed=4,
        lr=0.01,
        epochs=4,
        checkpoint_every=2,
        batch_size=8,
        dtype=dtype,
    )
    events = []
    train(task, data, None, tmp_path / "healthy", events.append, lambda: False)
    full = events[-1]["path"]
    checkpoint = next(
        e["path"] for e in events if e["kind"] == "checkpoint" and e["epoch"] == 2
    )
    restored = []
    train(task, data, checkpoint, tmp_path / "restored", restored.append, lambda: False)
    assert validate_state(restored[-1]["path"], full) == 0
    # Failure after persisting the final checkpoint but before committing result.
    final_retry = []
    train(task, data, full, tmp_path / "final-retry", final_retry.append, lambda: False)
    assert validate_state(final_retry[-1]["path"], full) == 0
    wrong = copy.deepcopy(task)
    wrong["lr"] *= 2
    with pytest.raises(ValueError):
        train(
            wrong, data, checkpoint, tmp_path / "wrong", lambda _: None, lambda: False
        )


def test_constructed_controls_have_expected_analytic_outcomes(problem):
    tasks, machines, profiles = problem
    tasks = {"a": dict(tasks["a"], checkpoint_every=4)}
    single = {"order": ["a"], "copies": {"a": ["spot"]}}
    double = {"order": ["a"], "copies": {"a": ["spot", "safe"]}}
    retry = simulate(single, tasks, machines, profiles, {"spot": 3}, 20)
    replica = simulate(double, tasks, machines, profiles, {"spot": 3}, 20)
    assert retry["elapsed_s"] == 7 and replica["elapsed_s"] == 4
    healthy_single = simulate(single, tasks, machines, profiles, {}, 20)
    healthy_double = simulate(double, tasks, machines, profiles, {}, 20)
    assert healthy_single["elapsed_s"] == healthy_double["elapsed_s"] == 4
    assert healthy_double["cost"] == 2 * healthy_single["cost"]


def test_dependency_waits_for_parent_even_when_priority_is_reversed(problem):
    tasks, machines, profiles = problem
    tasks["b"]["depends_on"] = ["a"]
    plan = {"order": ["b", "a"], "copies": {"a": ["safe"], "b": ["spot"]}}
    assert simulate(plan, tasks, machines, profiles, {}, 20)["elapsed_s"] == 6
    schedule = Schedule(plan, machines, tasks)
    assert schedule.dispatch() == [("a", "safe")]
    schedule.finish("a")
    assert schedule.dispatch() == [("b", "spot")]


def test_invalid_dependencies_rejected(problem):
    tasks, machines, profiles = problem
    tasks["a"]["depends_on"] = ["b"]
    tasks["b"]["depends_on"] = ["a"]
    with pytest.raises(ValueError, match="cyclic"):
        search(tasks, machines, profiles, [{"probability": 1, "failures": {}}], 10)


def test_cancellation_ack_fences_machine_reuse(problem):
    tasks, machines, _ = problem
    schedule = Schedule(
        {"order": ["a", "b"], "copies": {"a": ["safe", "spot"], "b": ["spot"]}},
        machines,
        tasks,
    )
    schedule.dispatch()
    schedule.finish("b")
    schedule.blocked.add("spot")
    assert schedule.dispatch() == []
    schedule.blocked.remove("spot")
    assert schedule.dispatch() == [("a", "spot")]


def test_cost_includes_idle_reserve_and_shared_storage(problem):
    _, machines, _ = problem
    machines = {
        m: dict(v, accounting={"bill_warm_pool": True, "shared_storage_per_hour": 360})
        for m, v in machines.items()
    }
    assert lease_cost(machines, {"safe": 0}, {}, 10) == 21


def test_conventional_plan_and_replication_ablation_share_primaries(problem):
    tasks, machines, profiles = problem
    result = search(tasks, machines, profiles, [{"probability": 1, "failures": {}}], 20)
    plain = result["policies"]["round_robin_retry"]["plan"]
    protected = result["policies"]["fixed_placement_replication"]["plan"]
    assert plain["order"] == protected["order"]
    assert all(plain["copies"][t][0] == protected["copies"][t][0] for t in tasks)
    assert all(
        len(ms) == 1
        for ms in result["policies"]["joint_enumeration"]["plan"]["copies"].values()
    )


def test_fault_injector_runs_while_controller_is_busy():
    import multiprocessing as mp
    import time
    from types import SimpleNamespace

    from replay_runtime import FaultInjector

    context = mp.get_context("spawn")
    worker = context.Process(target=time.sleep, args=(10,))
    worker.start()
    workers = SimpleNamespace(processes={"spot": worker})
    schedule = SimpleNamespace(active={"spot": "a"})
    begin = time.monotonic()
    injector = FaultInjector(workers, {"spot": 0.1}, begin, schedule)
    try:
        time.sleep(0.4)  # Controller does no polling or fault processing.
        evidence = injector.snapshot()["spot"]
        worker.join(timeout=2)
        assert worker.exitcode == -9
        assert evidence["active_task"] == "a"
        assert 0.1 <= evidence["observed_s"] < 0.35
    finally:
        injector.close()
        if worker.is_alive():
            worker.kill()
        worker.join(timeout=2)


def test_shard_merge_finetune_resume_matches_reference(tmp_path):
    import numpy as np
    import torch
    from workload import train, validate_state

    torch.set_num_threads(1)
    rng = np.random.default_rng(3)
    data = dict(
        x=rng.normal(size=(32, 64)).astype("float32"),
        y=np.arange(32, dtype="int64") % 10,
        ids=np.arange(32),
        test_x=rng.normal(size=(8, 64)).astype("float32"),
        test_y=np.arange(8, dtype="int64"),
    )
    task = dict(width=8, seed=3, lr=0.01, epochs=4, checkpoint_every=2, batch_size=8)
    parents = []
    for i in range(2):
        events = []
        train(
            dict(task, shard=[i, 2]),
            data,
            None,
            tmp_path / f"shard-{i}",
            events.append,
            lambda: False,
        )
        parents.append(events[-1]["path"])
    full = []
    train(task, data, None, tmp_path / "merge", full.append, lambda: False, parents)
    restored = []
    checkpoint = next(
        e["path"] for e in full if e["kind"] == "checkpoint" and e["epoch"] == 2
    )
    train(
        task,
        data,
        checkpoint,
        tmp_path / "resume-merge",
        restored.append,
        lambda: False,
        parents,
    )
    assert validate_state(full[-1]["path"], restored[-1]["path"]) == 0
    initial = []
    train(task, data, None, tmp_path / "without-merge", initial.append, lambda: False)
    with pytest.raises(AssertionError):
        validate_state(full[-1]["path"], initial[-1]["path"])
