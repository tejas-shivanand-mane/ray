"""Source regressions for the local Train coverage acceptance criteria."""

import copy
from pathlib import Path

import pytest


@pytest.fixture
def coverage(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[3] / "gossip_benchmarks"))
    import run_fixed_r_train_coverage

    return run_fixed_r_train_coverage


def completed_run():
    worker = {"actor_id": "worker", "node_id": "executor", "worker_id": "worker-process", "pid": 1}
    controller = {"actor_id": "controller", "node_id": "coordinator", "worker_id": "control-process", "pid": 2}
    task = {
        "name": "ReadParquet", "tasks_submitted": 4, "tasks_finished": 4,
        "tasks_failed": 0, "active_enrolled_tasks": [], "fixed_r_recovery_errors": [],
        "fixed_r_closed_streams": 4, "output_rows": 100,
        "fixed_r_recovered_tasks": 1,
    }
    actors = {
        "name": "MapBatches(XGBoostPredictor)", "tasks_submitted": 4, "tasks_finished": 4,
        "tasks_failed": 0, "active_enrolled_tasks": [], "output_rows": 100,
        "fixed_r_actor_mode": "surviving_coordinator_owned",
    }
    write = {
        **task, "name": "Write", "output_rows": 4, "fixed_r_survivor_only": True,
        "fixed_r_enrolled_tasks": 0, "fixed_r_recovered_tasks": 0, "fixed_r_survivor_tasks": 4,
    }
    return copy.deepcopy({
        "trigger": {"head_failure_requested": True},
        "lifecycle": {
            "workers_before": [worker], "workers_after": [worker],
            "controller_before": controller, "controller_after": controller,
        },
        "executions": {
            "training": {"state": "finished", "coordinator": controller, "operators": [task]},
            "inference": {"state": "finished", "coordinator": controller, "operators": [actors, write]},
        },
    })


@pytest.mark.parametrize("gap", ["read_replay", "worker_restart", "unsafe_write", "missing_predictions"])
def test_train_coverage_rejects_false_success(coverage, gap):
    result = completed_run()
    coverage.validate_observations(result, 100, ("executor",), "coordinator")
    if gap == "read_replay":
        result["executions"]["training"]["operators"][0]["fixed_r_recovered_tasks"] = 0
    elif gap == "worker_restart":
        result["lifecycle"]["workers_after"] = [{**result["lifecycle"]["workers_before"][0], "pid": 3}]
    elif gap == "unsafe_write":
        result["executions"]["inference"]["operators"][1]["fixed_r_enrolled_tasks"] = 1
    else:
        result["executions"]["inference"]["operators"][0]["output_rows"] = 99
    with pytest.raises(ValueError):
        coverage.validate_observations(result, 100, ("executor",), "coordinator")
