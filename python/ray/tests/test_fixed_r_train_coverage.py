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
    worker = {"actor_id": "worker", "node_id": "executor", "worker_id": "worker-process", "pid": 1,
              "world_rank": 0, "world_size": 1}
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


def test_v2_parquet_listing_is_not_training_read_recovery(coverage):
    result = completed_run()
    reads = result["executions"]["training"]["operators"][0]
    reads["name"] = "ReadFilesParquetV2"
    listing = {**reads, "name": "ListFiles", "output_rows": 32,
               "fixed_r_recovered_tasks": 0}
    result["executions"]["training"]["operators"].insert(0, listing)
    assert coverage.parquet_read_stage(["ListFiles", "ReadFilesParquetV2"]) == 1
    coverage.validate_observations(result, 100, ("executor",), "coordinator")
    listing["fixed_r_recovered_tasks"] = 1
    reads["fixed_r_recovered_tasks"] = 0
    with pytest.raises(ValueError, match="exercise replay"):
        coverage.validate_observations(result, 100, ("executor",), "coordinator")


@pytest.mark.parametrize("names", [
    ["ListFiles"], ["ReadCSV"], ["ReadParquet", "ReadFilesParquetV2"],
])
def test_parquet_stage_requires_one_actual_reader(coverage, names):
    with pytest.raises(ValueError, match="one Parquet ingestion stage"):
        coverage.parquet_read_stage(names)


@pytest.mark.parametrize("gap", [
    "missing_worker", "duplicate_rank", "wrong_world_size", "restarted_worker",
    "packed_workers", "coordinator_worker",
])
def test_multi_worker_coverage_requires_both_ranks_to_survive(coverage, gap):
    result = completed_run()
    lifecycle = result["lifecycle"]
    first = {**lifecycle["workers_before"][0], "world_size": 2}
    second = {**first, "actor_id": "worker-2", "worker_id": "process-2",
              "node_id": "executor-2", "pid": 3, "world_rank": 1}
    lifecycle["workers_before"] = [first, second]
    lifecycle["workers_after"] = copy.deepcopy([first, second])
    executors = ("executor", "executor-2")
    coverage.validate_observations(result, 100, executors, "coordinator", 2)
    if gap == "missing_worker":
        lifecycle["workers_before"] = [first]
        lifecycle["workers_after"] = [dict(first)]
    elif gap == "restarted_worker":
        lifecycle["workers_after"][1]["pid"] = 4
    else:
        field, value = {
            "duplicate_rank": ("world_rank", 0),
            "wrong_world_size": ("world_size", 1),
            "packed_workers": ("node_id", "executor"),
            "coordinator_worker": ("node_id", "coordinator"),
        }[gap]
        second[field] = value
        lifecycle["workers_after"] = copy.deepcopy(lifecycle["workers_before"])
    with pytest.raises(ValueError):
        coverage.validate_observations(result, 100, executors, "coordinator", 2)


@pytest.mark.parametrize("gap", [
    "no_replacement", "late_replacement", "unfinished_rounds", "missing_checkpoint",
    "missing_report", "wrong_trigger",
])
def test_boosting_failure_requires_progress_after_replacement(coverage, gap):
    result = completed_run()
    # Ingestion finished before the injected fault; replay is not expected.
    result["executions"]["training"]["operators"][0]["fixed_r_recovered_tasks"] = 0
    result["trigger"] = {
        "failure_phase": "training_boosting", "completed_boosting_rounds": 50,
        "reporting_workers": 1,
    }
    result["training_progress"] = {
        "reports": 101, "boosting_rounds": 100, "final_checkpoint_reported": True,
        "rounds_at_head_replacement": 52,
    }
    kwargs = dict(failure_phase="boosting", num_boost_round=100, failure_after_round=50)
    coverage.validate_observations(result, 100, ("executor",), "coordinator", **kwargs)
    if gap == "wrong_trigger":
        result["trigger"]["failure_phase"] = "training_data_ingestion"
    else:
        field, value = {
            "no_replacement": ("rounds_at_head_replacement", None),
            "late_replacement": ("rounds_at_head_replacement", 98),
            "unfinished_rounds": ("boosting_rounds", 99),
            "missing_checkpoint": ("final_checkpoint_reported", False),
            "missing_report": ("reports", 100),
        }[gap]
        result["training_progress"][field] = value
    with pytest.raises(ValueError, match="progress after head replacement"):
        coverage.validate_observations(result, 100, ("executor",), "coordinator", **kwargs)
