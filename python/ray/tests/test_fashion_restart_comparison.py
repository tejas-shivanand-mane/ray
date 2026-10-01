"""Check real attempt accounting and same-cluster restart without running Ray."""

from contextlib import contextmanager
import copy
import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest


@pytest.fixture
def modules(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    return NS(harness=importlib.import_module("train_restart"),
              runner=importlib.import_module("run_fashion_restart_comparison"),
              plot=importlib.import_module("plot_fashion_restart"))


@pytest.mark.parametrize("second_succeeds", [False, True])
def test_restart_uses_same_driver_and_repaired_cluster(modules, monkeypatch, tmp_path, second_succeeds):
    import ray
    from ray.experimental.recovery import _local

    clusters, calls = [], []
    monkeypatch.setattr(ray, "get_runtime_context", lambda: NS(get_node_id=lambda: "driver-node", get_job_id=lambda: "job"))

    @contextmanager
    def cluster(args, **kwargs):
        assert kwargs["recovery_enabled"] is False
        clusters.append(args)
        yield (NS(owner_node_id="head", executor_node_ids=("a", "b", "c", "d")), "crash-head", "crash-worker")

    def workload(options, path, evidence, existing_cluster):
        calls.append((copy.deepcopy(options), path, existing_cluster))
        if len(calls) == 1:
            evidence.update(workload_completed=False, data_owner_fault={"head_replacement": {"replacement_head_node_id": "new-head"}})
            raise RuntimeError("owner lost")
        assert existing_cluster[0].owner_node_id == "new-head"
        assert path.name == "attempt-1" and calls[0][1].name == "attempt-0"
        assert options["scenario"] == "none" and options["failure_point"] == "none"
        assert not options["owner_progress_plan"]["inject"]
        if not second_succeeds:
            raise RuntimeError("restart failed too")
        evidence["workload_completed"] = True
        return {"validation_status": "passed", "training_s": 3}

    monkeypatch.setattr(_local, "local_head_failure_cluster", cluster)
    monkeypatch.setattr(modules.harness, "run_workload", workload)
    monkeypatch.setattr(modules.harness, "verify_owner_loss", lambda evidence: None)
    options = {"mode": "off", "scenario": "data-owner", "failure_point": "late", "restart_scope": "full"}
    diagnostics = {"provenance": {"source_sha256": "same"}}
    if second_succeeds:
        assert modules.harness.run_case(options, tmp_path, diagnostics)["validation_status"] == "passed"
        assert diagnostics["whole_workload_restarts"] == 1
    else:
        with pytest.raises(RuntimeError, match="restart failed too"):
            modules.harness.run_case(options, tmp_path, diagnostics)
    assert len(clusters) == 1 and len(calls) == 2
    attempts = json.loads((tmp_path / "restart-attempts.json").read_text())["attempts"]
    assert attempts[0]["status"] == "failed" and attempts[0]["expected_owner_loss"]
    assert attempts[1]["status"] == ("passed" if second_succeeds else "failed")
    assert attempts[0]["driver_identity"] == attempts[1]["driver_identity"]
    assert all(call[0]["restart_scope"] == "full" for call in calls)


@pytest.fixture
def owner_failure():
    owner = {"task_id": "task", "owner_node_id": "head", "owner_worker_id": "owner", "object_ref_hex": "object", "recorded_ns": 2}
    plan = {"map_count": 4, "target_index": 0, "inject": True}
    return {"mode": "off", "status": "failed", "scenario": "data-owner", "failure_point": "early",
            "workload_completed": False, "workload_started_ns": 1, "map_progress": [],
            "owner_progress_plan": plan, "shuffle_owner": owner, "selected_owner_node_id": "head",
            "data_owner_fault": {
                "completed": True, "stage": "RandomShuffle.map", "submission_batch_settled": True,
                "ownership": owner, "request_ns": 4, "replacement_ready_ns": 5,
                "completed_maps_before_failure": [], "progress_plan": plan,
                "target": {"task_id": "task", "map_index": 0, "node_id": "executor", "blocked_ns": 3},
                "head_replacement": {"original_head_processes_exited": True,
                                     "failure_scope": "all_head_processes_with_surviving_gcs_storage",
                                     "gcs_storage_backend": "rocksdb", "original_head_node_id": "head",
                                     "replacement_head_node_id": "new-head", "original_gcs_pid": 10,
                                     "replacement_gcs_pid": 20, "surviving_node_ids": ["executor"]},
            },
            "ordinary_owner_loss": {"error_type": "OwnerDiedError", "source": "shuffle_metadata_fetch",
                                    "object_ref_hex": "object", "owner_worker_id": "owner", "owner_node_id": "head", "observed_ns": 6}}


def test_only_exact_owner_loss_qualifies_for_restart(modules, owner_failure):
    modules.harness.verify_owner_loss(owner_failure)
    for change in ("timeout", "object", "training", "head"):
        sample = copy.deepcopy(owner_failure)
        if change == "timeout":
            sample["error_type"] = "TimeoutError"
        elif change == "object":
            sample["ordinary_owner_loss"]["object_ref_hex"] = "unrelated"
        elif change == "training":
            sample["reports"] = [{"time_ns": 10}]
        else:
            sample["data_owner_fault"]["head_replacement"]["original_head_processes_exited"] = False
        with pytest.raises(ValueError):
            modules.harness.verify_owner_loss(sample)


def test_comparison_uses_total_measured_time_and_final_predictions(modules, monkeypatch):
    initial_off, final_off, initial_on = {"status": "failed"}, {"status": "passed"}, {"status": "passed"}
    off = {"mode": "off", "attempts": [initial_off, final_off], "whole_workload_restarts": 1, "observation_wall_s": 200}
    on = {"mode": "on", "attempts": [initial_on], "whole_workload_restarts": 0, "observation_wall_s": 150}
    monkeypatch.setattr(modules.runner, "check_trial", lambda sample, control: sample["observation_wall_s"])

    def owner_pair(a, b):
        assert a is initial_off and b is initial_on
        return {"owner_loss_demonstrated": True, "on_replayed_tasks": 4}

    def predictions(a, b):
        assert a is final_off and b is initial_on
        return 0

    monkeypatch.setattr(modules.runner, "compare_owner_pair", owner_pair)
    monkeypatch.setattr(modules.runner, "predictions_match", predictions)
    result = modules.runner.compare_trials(off, on)
    assert result["off_completion_wall_s"] == 200
    assert result["on_vs_off_pct"] == -25
    assert initial_off["status"] == "failed"


def test_plot_uses_original_clock_including_restart_and_cleanup(modules):
    sample = {"status": "passed", "workload_completed": True, "training_epochs": 2,
              "observation_started_ns": 10**9, "observation_wall_s": 20,
              "attempts": [
                  {"status": "failed", "workload_started_ns": 3 * 10**9, "workload_finished_ns": 6 * 10**9,
                   "data_owner_fault": {"request_ns": 4 * 10**9}, "reports": []},
                  {"status": "passed", "workload_completed": True, "training_epochs": 2,
                   "attempt_started_ns": 7 * 10**9, "workload_started_ns": 8 * 10**9,
                   "workload_finished_ns": 18 * 10**9,
                   "reports": [{"time_ns": t * 10**9, "metrics": [{"epoch": i}]} for i, t in ((1, 12), (2, 16))]},
              ]}
    trace = modules.plot.trial_trace(sample)
    assert trace["faults"] == [3]
    assert trace["restarts"] == [6]
    assert trace["seconds"][-1] == 20
    assert trace["epochs"][-1] == 2
    sample.update(status="failed", timeout=True)
    assert modules.plot.trial_trace(sample)["seconds"][-1] == 17


def test_feature_identity_rejects_changed_weights(modules, tmp_path):
    content = b"test-weight-bytes"
    path = tmp_path / "mobilenet-v3-small.pt"
    path.write_bytes(content)
    manifest = {"model": "MobileNet_V3_Small_Weights.IMAGENET1K_V1", "features": 576,
                "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert modules.runner.feature_identity(tmp_path) == manifest
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="weights"):
        modules.runner.feature_identity(tmp_path)


def test_timeout_plot_keeps_preprocessing_and_separate_measured_stop(modules):
    sample = {"status": "failed", "timeout": True,
              "observation_started_ns": 10**9, "observation_wall_s": 610,
              "attempts": [{"status": "failed", "timeout": True,
                            "workload_started_ns": 10 * 10**9,
                            "workload_finished_ns": 601 * 10**9,
                            "feature_progress": {"feature_ready_ns": 524 * 10**9},
                            "map_progress": [{"time_ns": 527 * 10**9}]}]}
    trace = modules.plot.trial_trace(sample)
    assert trace["seconds"] == [0, 9, 523, 526]
    assert trace["epochs"] == [0, 0, 0, 0]
    assert trace["stopped_s"] == 610
    assert not trace["faults"] and not trace["restarts"]
    assert sample["status"] == "failed"
    sample["observation_wall_s"] = 500
    with pytest.raises(ValueError, match="before its last observed progress"):
        modules.plot.trial_trace(sample)


@pytest.mark.parametrize("invalid", [{"status": "failed"}, {"timeout": True}, {"workload_completed": False}])
def test_incomplete_trials_never_produce_completion_time(modules, invalid):
    sample = {"status": "passed", "workload_completed": True, "observation_wall_s": 100, **invalid}
    with pytest.raises(ValueError, match="verified completion"):
        modules.runner.check_trial(sample)
