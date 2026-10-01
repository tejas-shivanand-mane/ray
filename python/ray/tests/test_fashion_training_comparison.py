"""Check measured evidence and fail-closed pairing without starting a cluster."""

import copy
import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest


@pytest.fixture
def modules(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    return NS(checks=importlib.import_module("fashion_comparison"),
              runner=importlib.import_module("run_fashion_training_comparison"),
              workload=importlib.import_module("train_workload"))


def test_failure_points_leave_training_after_every_fault(modules):
    assert modules.runner.failure_epochs(8, ["early", "middle", "late"]) == {
        "early": 1, "middle": 4, "late": 7,
    }
    assert modules.runner.failure_epochs(4, ["middle", "middle"]) == {"middle": 2}
    with pytest.raises(ValueError):
        modules.runner.failure_epochs(3, ["late"])


@pytest.fixture
def later_checkpoint(tmp_path):
    def worker(rank, actor):
        return {"rank": rank, "actor_id": actor, "pid": 10 + rank}

    old = [worker(0, "old-0"), worker(1, "old-1")]
    new = [worker(0, "new-0"), worker(1, "old-1")]
    timeline = {"groups": [old, new],
                "reports": [{"time_ns": i * 10**9, "checkpoint": {"model.pt": f"epoch-{i}"}}
                            for i in (1, 2, 3, 4)],
                "fault": {"report_number": 3, "request_ns": 3100000000}}
    (tmp_path / "timeline.json").write_text(json.dumps(timeline))
    (tmp_path / "starts").mkdir()
    for i, w in enumerate(old + new):
        start = {**w, "checkpoint": None if i < 2 else {"model.pt": "epoch-3"},
                 "time_ns": 500000000 if i < 2 else 3500000000}
        (tmp_path / "starts" / f"{i}.json").write_text(json.dumps(start))
    return tmp_path


def test_retry_restores_selected_later_checkpoint(modules, later_checkpoint):
    result = modules.workload.validate(later_checkpoint, selective=True, inject=True)
    recovery = result["recoveries"][0]
    assert recovery["committed_reports_before_failure"] == 3
    assert recovery["retained_ranks"] == [1]
    assert recovery["failure_to_next_report_s"] == pytest.approx(0.9)
    assert recovery["failure_to_all_workers_invoked_s"] == pytest.approx(0.4)


@pytest.mark.parametrize("corruption", ["old_checkpoint", "wrong_rank", "early_report", "missing_progress", "fallback"])
def test_retry_rejects_wrong_recovery_evidence(modules, later_checkpoint, corruption):
    timeline_path = later_checkpoint / "timeline.json"
    timeline = json.loads(timeline_path.read_text())
    start_path = later_checkpoint / "starts/3.json"
    start = json.loads(start_path.read_text())
    if corruption == "old_checkpoint":
        start["checkpoint"] = {"model.pt": "epoch-1"}
    elif corruption == "wrong_rank":
        start["rank"] = 0
    elif corruption == "early_report":
        timeline["reports"][-1]["time_ns"] = 3200000000
    elif corruption == "missing_progress":
        timeline["reports"].pop()
    else:
        timeline["groups"][1][1]["actor_id"] = "new-1"
    start_path.write_text(json.dumps(start))
    timeline_path.write_text(json.dumps(timeline))
    with pytest.raises(ValueError):
        modules.workload.validate(later_checkpoint, selective=True, inject=True)


def test_later_report_gate_does_not_block_earlier_epochs(modules, monkeypatch, tmp_path):
    from ray.train.v2._internal.execution import context

    current = NS(report_call_index=0)
    monkeypatch.setattr(context, "get_train_context", lambda: current)
    gate = modules.workload.ReportGate(str(tmp_path), report_number=3)
    monkeypatch.setattr(modules.workload.time, "sleep", lambda _: pytest.fail("Unexpected wait"))
    with gate.on_report():
        current.report_call_index += 1
    # The third report must use its own release marker, not the first report's.
    (tmp_path / "release-report-3.json").write_text("{}")
    current.report_call_index = 2
    with gate.on_report():
        current.report_call_index += 1


@pytest.fixture
def matched_pair(tmp_path):
    pair = []
    for arm, mode, scope in (("ordinary", "off", "full"), ("integrated", "on", "selective")):
        directory = tmp_path / arm
        directory.mkdir()
        np.save(directory / "predictions.npy", np.ones((10000, 10), dtype=np.float32))
        pair.append({
            "arm": arm, "mode": mode, "restart_scope": scope, "directory": str(directory),
            "status": "passed", "workload_completed": True, "scenario": "none", "pair": 1,
            "failure_point": "none", "fault_after_epoch": 0, "training_epochs": 4,
            "input_identity": {"dataset": "Fashion-MNIST"}, "workload_sha256": "script",
            "torch_version": "torch", "owner_placement": "default", "model_parameters": 235146,
            "training_rows_per_epoch": 60000, "validation_rows_per_epoch": 10000,
            "checkpoint_policy": "application-every-epoch", "workload_s": 10, "training_s": 8,
            "native_settings": {"enable_streaming_recovery": mode == "on", "replicas": 2},
            "provenance": {key: "same" for key in (
                "source_sha256", "native_extension_sha256", "python", "platform", "ray_version",
                "numpy_version", "pyarrow_version", "pandas_version", "xgboost_version")},
        })
    return pair


def test_combined_pair_preserves_baseline_retry(modules, matched_pair):
    result = modules.checks.compare_pair(*matched_pair)
    assert result["workload_s_change_pct"] == 0
    assert result["predictions_max_abs_difference"] == 0


@pytest.mark.parametrize("corruption", ["mode", "retry", "input", "failure_epoch", "native", "timeout", "failure", "predictions"])
def test_combined_pair_rejects_unmatched_or_incorrect_runs(modules, matched_pair, corruption):
    ordinary, integrated = matched_pair
    if corruption == "mode":
        integrated["mode"] = "off"
    elif corruption == "retry":
        integrated["restart_scope"] = "full"
    elif corruption == "input":
        integrated["input_identity"] = {"dataset": "different"}
    elif corruption == "failure_epoch":
        integrated["fault_after_epoch"] = 3
    elif corruption == "native":
        integrated["native_settings"]["replicas"] = 3
    elif corruption == "timeout":
        ordinary["timeout"] = True
    elif corruption == "failure":
        ordinary["status"] = "failed"
    else:
        np.save(Path(integrated["directory"]) / "predictions.npy", np.zeros((10000, 10)))
    with pytest.raises((ValueError, AssertionError)):
        modules.checks.compare_pair(ordinary, integrated)


def test_control_failure_skips_expensive_fault_trials(modules, monkeypatch, tmp_path, matched_pair):
    calls = []
    monkeypatch.setattr(modules.runner, "input_identity", lambda path: {})

    def observation(options, pair, directory, provenance):
        calls.append(options)
        sample = copy.deepcopy(matched_pair[options["mode"] == "on"])
        sample.update(status="failed", error="control error")
        return sample

    monkeypatch.setattr(modules.runner, "run_observation", observation)
    args = NS(epochs=8, failure_point=["early", "middle", "late"], repeats=1,
              data_directory=tmp_path, timeout_s=420, output=tmp_path / "report.json")
    assert modules.runner.run_comparison(args, tmp_path, {}) == 1
    assert len(calls) == 2
    assert {o["mode"] for o in calls} == {"off", "on"}
    assert all(o["scenario"] == "none" for o in calls)
    report = json.loads(args.output.read_text())
    assert report["skipped_failure_points"] == ["early", "middle", "late"]
    assert not report["pairs"]


def test_entire_suite_keeps_consistent_arms_and_checks_controls(modules, monkeypatch, tmp_path, matched_pair):
    calls = []
    monkeypatch.setattr(modules.runner, "input_identity", lambda path: {})

    def observation(options, pair, directory, provenance):
        calls.append(options)
        sample = copy.deepcopy(matched_pair[options["mode"] == "on"])
        sample.update(scenario=options["scenario"], training_epochs=8,
                      reports=[{"time_ns": i * 10**9} for i in range(1, 9)])
        if options["scenario"] == "worker":
            sample["recoveries"] = [{"failure_to_next_report_s": 2}]
        return sample

    monkeypatch.setattr(modules.runner, "run_observation", observation)
    args = NS(epochs=8, failure_point=["early", "middle", "late"], repeats=1,
              data_directory=tmp_path, timeout_s=420, output=tmp_path / "report.json")
    assert modules.runner.run_comparison(args, tmp_path, {}) == 0
    assert len(calls) == 8
    assert {(o["mode"], o["restart_scope"]) for o in calls} == {("off", "full"), ("on", "selective")}
    assert [o["fault_after_epoch"] for o in calls] == [0, 0, 1, 1, 4, 4, 7, 7]
    report = json.loads(args.output.read_text())
    assert len(report["pairs"]) == 4
    for sample in report["samples"][2:]:
        assert sample["matches_no_failure_predictions"]
        assert sample["recoveries"][0]["next_report_excess_vs_control_s"] == 1
        assert sample["recoveries"][0]["lost_uncommitted_optimizer_steps"] is None


def test_input_fingerprint_rejects_changed_dataset(modules, tmp_path):
    files = {}
    for filename, rows in (("train.parquet", 60000), ("test.parquet", 10000)):
        content = filename.encode()
        (tmp_path / filename).write_bytes(content)
        files[filename] = {"rows": rows, "bytes": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    manifest = {"dataset": "Fashion-MNIST", "split": "official", "files": files}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    assert modules.checks.input_identity(tmp_path) == manifest
    (tmp_path / "train.parquet").write_bytes(b"changed input")
    with pytest.raises(ValueError, match="identity mismatch"):
        modules.checks.input_identity(tmp_path)
