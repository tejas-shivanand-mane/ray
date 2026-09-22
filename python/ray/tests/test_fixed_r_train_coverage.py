"""Source regressions for the local Train coverage acceptance criteria."""

import copy
from pathlib import Path
from types import SimpleNamespace

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


@pytest.mark.parametrize("gap", [
    "no_kill", "no_restart", "wrong_checkpoint", "wrong_path", "restart_from_zero",
    "missing_round", "extra_rounds", "no_final_checkpoint",
])
def test_checkpoint_recovery_rejects_false_resume(coverage, gap):
    result = completed_run()
    lifecycle = result["lifecycle"]
    before = {**lifecycle["workers_before"][0], "restored_checkpoint_rounds": 0,
              "restored_checkpoint_path": None}
    after = {**before, "actor_id": "replacement", "worker_id": "replacement-process", "pid": 3,
             "restored_checkpoint_rounds": 5, "restored_checkpoint_path": "/checkpoint-5"}
    lifecycle["workers_before"] = [before]
    lifecycle["workers_after"] = [
        {k: v for k, v in after.items() if not k.startswith("restored_checkpoint_")}
    ]
    result["trigger"] = {"failure_phase": "training_checkpoint_restart", "checkpoint_rounds": 5,
                         "checkpoint_path": "/checkpoint-5"}
    reports = [
        {"attempt": 2, "boosting_rounds": i, "restored_checkpoint_rounds": 5,
         "reporting_workers": 1, "checkpoint": {"rounds": 10} if i == 10 else None}
        for i in range(6, 11)
    ]
    recovery = {"worker_groups": [[before], [after]], "worker_failure_injected": True, "reports": reports}
    result["checkpoint_recovery"] = recovery
    result["executions"]["training"]["operators"][0]["fixed_r_recovered_tasks"] = 0
    kwargs = dict(failure_phase="checkpoint", num_boost_round=10, failure_after_round=5)
    coverage.validate_observations(result, 100, ("executor",), "coordinator", **kwargs)
    if gap == "no_kill":
        recovery["worker_failure_injected"] = False
    elif gap == "no_restart":
        after["actor_id"] = before["actor_id"]
    elif gap == "wrong_checkpoint":
        after["restored_checkpoint_rounds"] = 4
    elif gap == "wrong_path":
        after["restored_checkpoint_path"] = "/unrelated-checkpoint"
    elif gap == "restart_from_zero":
        reports[0]["restored_checkpoint_rounds"] = 0
    elif gap == "missing_round":
        reports.pop(0)
    elif gap == "extra_rounds":
        reports.append({**reports[-1], "boosting_rounds": 11})
    else:
        reports[-1]["checkpoint"] = None
    with pytest.raises(ValueError):
        coverage.validate_observations(result, 100, ("executor",), "coordinator", **kwargs)


def test_checkpoint_monitor_requires_persisted_checkpoint_and_all_worker_agreement(coverage):
    monitor = coverage.CheckpointMonitor(10, 5, 2)
    monitor.worker_group_started([{}, {}], ["first", "second"])
    with pytest.raises(ValueError, match="disagree"):
        monitor.checkpoint_report([
            {"boosting_rounds": 1, "restored_checkpoint_rounds": 0},
            {"boosting_rounds": 2, "restored_checkpoint_rounds": 0},
        ], None)
    for i in range(1, 5):
        monitor.checkpoint_report([{"boosting_rounds": i, "restored_checkpoint_rounds": 0}] * 2, None)
    with pytest.raises(ValueError, match="persisted"):
        monitor.checkpoint_report([{"boosting_rounds": 5, "restored_checkpoint_rounds": 0}] * 2, None)


@pytest.mark.parametrize("restored_rounds", [0, 5, 10, 11])
def test_xgboost_resume_preserves_model_and_total_round_budget(coverage, monkeypatch, restored_rounds):
    import train_batch_inference_benchmark as benchmark

    checkpoint = object() if restored_rounds else None
    model = SimpleNamespace(num_boosted_rounds=lambda: restored_rounds)
    monkeypatch.setattr(benchmark.ray.train, "get_checkpoint", lambda: checkpoint)
    monkeypatch.setattr(benchmark.XGBoostReportCallback, "get_model", lambda checkpoint: model)
    monkeypatch.setattr(benchmark.ray.train, "get_context", lambda: SimpleNamespace(get_world_rank=lambda: 0))
    reports, training_calls, reads = [], [], []
    monkeypatch.setattr(benchmark.ray.train, "report", lambda metrics, **kw: reports.append((metrics, kw)))
    frame = benchmark.pd.DataFrame({"feature": [0.0, 1.0], "labels": [0, 1]})

    def get_shard(name):
        reads.append(name)
        return SimpleNamespace(materialize=lambda: SimpleNamespace(to_pandas=lambda: frame))

    monkeypatch.setattr(benchmark.ray.train, "get_dataset_shard", get_shard)
    monkeypatch.setattr(benchmark.xgb, "DMatrix", lambda *a, **kw: object())
    monkeypatch.setattr(benchmark.xgb, "train", lambda params, **kw: training_calls.append(kw))
    config = {**benchmark._FRAMEWORK_PARAMS["xgboost"]["train_loop_config"],
              "num_boost_round": 10, "checkpoint_frequency": 5}
    if restored_rounds > 10:
        with pytest.raises(ValueError, match="more rounds"):
            benchmark.xgboost_train_loop_function(config)
        assert not reads and not training_calls and not reports
        return
    benchmark.xgboost_train_loop_function(config)
    if restored_rounds == 10:
        assert not reads and not training_calls
        assert reports == [({"boosting_rounds": 10, "restored_checkpoint_rounds": 10},
                            {"checkpoint": checkpoint})]
    else:
        assert len(training_calls) == 1
        assert training_calls[0]["num_boost_round"] == 10 - restored_rounds
        assert training_calls[0]["xgb_model"] is (model if restored_rounds else None)
