"""Report regressions; distributed acceptance uses the comparison runner."""

import copy
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.fixture
def modules(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    monkeypatch.syspath_prepend(str(root / "release/train_tests/xgboost_lightgbm"))
    import run_fixed_r_train_comparison
    import train_comparison

    return train_comparison, run_fixed_r_train_comparison


def test_comparison_payload_deserializes_without_benchmark_import_paths(modules, tmp_path):
    from ray import cloudpickle
    import train_batch_inference_benchmark as benchmark

    case, _ = modules
    previous = cloudpickle.list_registry_pickle_by_value()
    names = (case.__name__, case.coverage.__name__, benchmark.__name__, "streaming_recovery_progress")
    try:
        case.register_for_worker_serialization(benchmark)
        # Capture the same module globals and callback class as the Job actor.
        payload = cloudpickle.dumps((case.ComparisonProbe, case.coverage, benchmark,
                                     case.capture_node_execution("test-monitor", "training")))
    finally:
        for name in names:
            if name not in previous:
                cloudpickle.unregister_pickle_by_value(sys.modules[name])
    code = """
import importlib.abc
import sys
from ray import cloudpickle

class NoBenchmarkImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in {
            'train_comparison', 'run_fixed_r_train_coverage',
            'train_batch_inference_benchmark', 'streaming_recovery_progress',
        }:
            raise ModuleNotFoundError('Worker cannot import ' + fullname)

sys.meta_path.insert(0, NoBenchmarkImports())
probe, coverage, benchmark, capture = cloudpickle.loads(sys.stdin.buffer.read())
assert probe.__name__ == 'ComparisonProbe'
assert callable(coverage.checkpoint_worker_identity)
assert callable(benchmark.xgboost_train_loop_function)
assert callable(capture.before_execution_starts)
"""
    result = subprocess.run([sys.executable, "-c", code], input=payload,
                            cwd=tmp_path, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stderr.decode(errors="replace")


def options():
    return {"scenario": "worker", "num_train_workers": 2, "num_boost_round": 10,
            "failure_after_round": 5, "gated": True}


def workers(attempt, rounds=0, path=None):
    return [{"actor_id": f"actor-{attempt}-{rank}", "worker_id": f"worker-{attempt}-{rank}",
             "pid": attempt * 10 + rank, "node_id": f"executor-{rank}",
             "world_rank": rank, "world_size": 2,
             "restored_checkpoint_rounds": rounds, "restored_checkpoint_path": path}
            for rank in range(2)]


def observation():
    first, second = workers(1), workers(2, 5, "/checkpoint-5")
    reports = [{"attempt": 1, "rounds": 5, "checkpoint_path": "/checkpoint-5", "at_ns": 10**9},
               {"attempt": 1, "rounds": 7, "checkpoint_path": None, "at_ns": 2 * 10**9},
               {"attempt": 2, "rounds": 6, "checkpoint_path": None, "at_ns": 5 * 10**9},
               {"attempt": 2, "rounds": 10, "checkpoint_path": "/checkpoint-10", "at_ns": 8 * 10**9}]
    controller = {"node_id": "coordinator", "actor_id": "controller"}
    return {
        "worker_groups": [{"attempt": 1, "at_ns": 0, "workers": first},
                          {"attempt": 2, "at_ns": 4 * 10**9, "workers": second}],
        "controller": {"controller_started": controller, "controller_finished": dict(controller)},
        "finished_workers": [{k: v for k, v in w.items() if not k.startswith("restored_checkpoint_")}
                             for w in second],
        "trigger": {"rounds": 5}, "fault_done": True,
        "fault": {"request_ns": 2 * 10**9, "last_reported_round": 5,
                  "last_registered_checkpoint": {"rounds": 5, "path": "/checkpoint-5"},
                  "worker_group_attempt": 1, "worker_request_ns": 3 * 10**9,
                  "last_reported_round_before_worker_failure": 7},
        "reports": reports,
        "events": [{"name": name, "at_ns": at * 10**9} for name, at in (
            ("worker_failure_requested", 3), ("controller_failure_detected", 3.5),
            ("training_finished", 8), ("pipeline_finished", 9))],
        "stages": [{"worker_id": w["worker_id"], "world_rank": w["world_rank"],
                    "restored_checkpoint_rounds": w["restored_checkpoint_rounds"], "name": name,
                    "started_ns": 0, "finished_ns": 10**9, "duration_s": 1.0}
                   for w in first + second for name in ("checkpoint_load", "data_ingestion", "dmatrix")],
    }


def test_monitor_matches_progress_and_final_only_checkpoint(modules):
    case, _ = modules
    monitor = case.ComparisonMonitor({**options(), "scenario": "none"})
    monitor.group_started(workers(1), ["target", "other"], 1)
    for round_count in range(1, 11):
        monitor.report([{"boosting_rounds": round_count, "restored_checkpoint_rounds": 0}] * 2,
                       None, round_count + 1)
    final_metrics = [{"boosting_rounds": 10, "restored_checkpoint_rounds": 0}] * 2
    monitor.report(final_metrics, "/checkpoint-final", 12)
    assert monitor.trigger is None
    assert monitor.checkpoint["rounds"] == 10
    with pytest.raises(ValueError, match="skipped, repeated"):
        monitor.report(final_metrics, "/checkpoint-final", 13)


def test_monitor_snapshots_actual_worker_fault_after_head_progress(modules):
    case, _ = modules
    monitor = case.ComparisonMonitor({**options(), "scenario": "head-worker", "gated": False})
    monitor.group_started(workers(1), ["target", "other"], 1)
    for count in range(1, 6):
        monitor.report([{"boosting_rounds": count, "restored_checkpoint_rounds": 0}] * 2,
                       "/checkpoint-5" if count == 5 else None, count + 1)
    monitor.begin_fault(7)
    monitor.report([{"boosting_rounds": 6, "restored_checkpoint_rounds": 0}] * 2, None, 8)
    monitor.worker_failure_requested(9)
    assert monitor.fault["last_reported_round"] == 5
    assert monitor.fault["last_reported_round_before_worker_failure"] == 6
    with pytest.raises(ValueError):
        monitor.worker_failure_requested(10)


@pytest.mark.parametrize("metrics", [
    [{"boosting_rounds": 2, "restored_checkpoint_rounds": 0}] * 2,
    [{"boosting_rounds": 1, "restored_checkpoint_rounds": 1}] * 2,
    [{"boosting_rounds": 1, "restored_checkpoint_rounds": 0},
     {"boosting_rounds": 2, "restored_checkpoint_rounds": 0}],
])
def test_monitor_rejects_missing_progress_and_disagreement(modules, metrics):
    case, _ = modules
    monitor = case.ComparisonMonitor(options())
    monitor.group_started(workers(1), ["target", "other"], 1)
    with pytest.raises(ValueError):
        monitor.report(metrics, None, 2)


@pytest.mark.parametrize("gap", ["checkpoint", "worker_id", "controller", "stage", "stage_rank", "duration", "final"])
def test_observation_rejects_false_recovery(modules, gap):
    case, _ = modules
    result = observation()
    case.validate_observation(result, options(), "coordinator", ("executor-0", "executor-1"))
    if gap == "checkpoint":
        result["worker_groups"][1]["workers"][0]["restored_checkpoint_path"] = "/wrong"
    elif gap == "worker_id":
        result["worker_groups"][1]["workers"][0]["worker_id"] = "worker-1-0"
    elif gap == "controller":
        result["controller"]["controller_finished"]["actor_id"] = "replacement-controller"
    elif gap == "stage":
        result["stages"].pop()
    elif gap == "stage_rank":
        result["stages"][0]["world_rank"] = 1
    elif gap == "duration":
        result["stages"][0]["duration_s"] = float("nan")
    else:
        result["reports"][-1]["checkpoint_path"] = None
    with pytest.raises(ValueError):
        case.validate_observation(result, options(), "coordinator", ("executor-0", "executor-1"))


def test_newer_inflight_checkpoint_must_be_used_on_restart(modules):
    case, _ = modules
    result = observation()
    result["reports"].insert(1, {"attempt": 1, "rounds": 6, "checkpoint_path": "/checkpoint-6",
                                 "at_ns": 1.5 * 10**9})
    with pytest.raises(ValueError, match="wrong checkpoint"):
        case.validate_observation(result, options(), "coordinator", ("executor-0", "executor-1"))
    for worker in result["worker_groups"][1]["workers"]:
        worker.update(restored_checkpoint_rounds=6, restored_checkpoint_path="/checkpoint-6")
    for stage in result["stages"]:
        if stage["worker_id"].startswith("worker-2"):
            stage["restored_checkpoint_rounds"] = 6
    case.validate_observation(result, options(), "coordinator", ("executor-0", "executor-1"))


def test_recovery_latency_excludes_remaining_work_and_reports_rollback(modules):
    case, _ = modules
    recovery = case.recovery_metrics(observation())
    assert recovery["fault_request_to_first_resumed_round_s"] == 3
    assert recovery["worker_failure_to_detection_s"] == .5
    assert recovery["detection_to_worker_group_ready_s"] == .5
    assert recovery["fault_request_to_training_completion_s"] == 6
    assert recovery["rollback_reported_rounds"] == 2
    assert recovery["preserved_worker_ids"] == []
    assert len(recovery["replacement_worker_ids"]) == 2


def sample(mode, pair=1):
    return {"scenario": "none", "mode": mode, "pair": pair, "status": "passed",
            "training_s": 10 if mode == "off" else 11, "pipeline_s": 10 if mode == "off" else 11,
            "prediction_s": None,
            "provenance": {key: "same" for key in (
                "source_sha256", "native_extension_sha256", "python", "platform", "ray_version",
                "xgboost_version", "numpy_version", "pyarrow_version", "pandas_version")}}


def test_summary_excludes_timeouts_and_incomplete_pairs(modules):
    _, runner = modules
    failed = {**sample("on", 2), "status": "failed", "timeout": True, "training_s": 1}
    rows = runner.summarize([sample("off"), sample("on"), sample("off", 2), failed, sample("on", 3)])
    assert {r["metric"] for r in rows} == {"training_s", "pipeline_s"}
    assert all(r["pairs"] == 1 and r["on_vs_off_pct_stdev"] is None for r in rows)
    assert all(r["on_vs_off_pct_mean"] == pytest.approx(10) for r in rows)


@pytest.mark.parametrize("key", ["source_sha256", "native_extension_sha256", "xgboost_version"])
def test_summary_refuses_mixed_builds(modules, key):
    _, runner = modules
    changed = copy.deepcopy(sample("on"))
    changed["provenance"][key] = "different"
    with pytest.raises(ValueError, match=key):
        runner.summarize([sample("off"), changed])
