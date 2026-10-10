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
    args = NS(epochs=8, failure_point=["early", "middle", "late"], failure_kind=["head-node", "worker-node"], repeats=1,
              data_directory=tmp_path, timeout_s=420, output=tmp_path / "report.json")
    assert modules.runner.run_comparison(args, tmp_path, {}) == 1
    assert len(calls) == 2
    assert {o["mode"] for o in calls} == {"off", "on"}
    assert all(o["scenario"] == "none" for o in calls)
    report = json.loads(args.output.read_text())
    assert report["skipped_failure_points"] == ["early", "middle", "late"]
    assert len(report["skipped_cases"]) == 6
    assert not report["pairs"]


def test_entire_suite_keeps_consistent_arms_and_checks_controls(modules, monkeypatch, tmp_path, matched_pair):
    calls = []
    monkeypatch.setattr(modules.runner, "input_identity", lambda path: {})

    def observation(options, pair, directory, provenance):
        calls.append(options)
        sample = copy.deepcopy(matched_pair[options["mode"] == "on"])
        sample.update(scenario=options["scenario"], training_epochs=8,
                      placement_strategy=options["placement_strategy"],
                      reports=[{"time_ns": i * 10**9} for i in range(1, 9)])
        if options["scenario"] != "none":
            sample["recoveries"] = [{"failure_to_next_report_s": 2}]
            sample["node_fault"] = {"completed": True, "scenario": options["scenario"],
                                    "report_number": options["fault_after_epoch"]}
        return sample

    monkeypatch.setattr(modules.runner, "run_observation", observation)
    args = NS(epochs=8, failure_point=["early", "middle", "late"], failure_kind=["head-node", "worker-node"], repeats=1,
              data_directory=tmp_path, timeout_s=420, output=tmp_path / "report.json")
    assert modules.runner.run_comparison(args, tmp_path, {}) == 0
    assert len(calls) == 14
    assert {(o["mode"], o["restart_scope"]) for o in calls} == {("off", "full"), ("on", "selective")}
    assert all(o["placement_strategy"] == "STRICT_SPREAD" for o in calls)
    assert [o["fault_after_epoch"] for o in calls] == [0, 0, 1, 1, 4, 4, 7, 7, 1, 1, 4, 4, 7, 7]
    report = json.loads(args.output.read_text())
    assert len(report["pairs"]) == 7
    assert {p["scenario"] for p in report["pairs"]} == {"none", "head-node", "worker-node"}
    for sample in report["samples"][2:]:
        assert sample["matches_no_failure_predictions"]
        assert sample["recoveries"][0]["next_report_excess_vs_control_s"] == 1
        assert sample["recoveries"][0]["lost_uncommitted_optimizer_steps"] is None


def test_plot_panels_do_not_mix_head_and_worker_or_duplicate_controls(modules):
    plot = importlib.import_module("plot_fashion_training")
    cases = modules.runner.comparison_cases(8, ["early", "middle", "late"], ["head-node", "worker-node"])
    report = {"profile": "fashion-mnist-failure-matrix", "failure_kinds": ["head-node", "worker-node"],
              "failure_epochs": {"none": 0, "early": 1, "middle": 4, "late": 7},
              "samples": [{**c, "arm": a} for c in cases for a in ("ordinary", "integrated")]}
    rows, columns, panels = plot.panel_layout(report)
    assert (rows, columns) == (2, 4)
    for kind, point in panels:
        for arm in ("ordinary", "integrated"):
            samples = plot.panel_samples(report, kind, point, arm)
            assert len(samples) == 1
            assert samples[0]["scenario"] == ("none" if point == "none" else kind)
    assert plot.panel_samples(report, "head-node", "none", "ordinary")[0] is (
        plot.panel_samples(report, "worker-node", "none", "ordinary")[0])
    report["samples"] = report["samples"][:2]
    assert plot.panel_layout(report) == (rows, columns, panels)
    assert plot.panel_samples(report, "head-node", "late", "ordinary") == []


@pytest.fixture
def node_checkpoint(later_checkpoint):
    path = later_checkpoint / "timeline.json"
    timeline = json.loads(path.read_text())
    for group, nodes in zip(timeline["groups"], (("a", "b"), ("c", "b"))):
        for worker, node in zip(group, nodes):
            worker["node_id"] = node
    fault = {**timeline["fault"], "scenario": "worker-node", "completed": True,
             "groups": [timeline["groups"][0]], "checkpoint": timeline["reports"][2]["checkpoint"],
             "checkpoint_committed_ns": 3000000000, "operation_finished_ns": 3400000000,
             "original_head_node_id": "head", "executor_node_ids": ["a", "b", "c", "d"],
             "worker_node_failure": {
                 "failure_scope": "logical_worker_node_processes_with_surviving_shared_storage",
                 "all_node_processes_exited": True, "gcs_marked_dead": True,
                 "node_id": "a", "training_worker_pid": 10, "node_process_pids": [10, 11, 12],
                 "surviving_node_ids": ["head", "coordinator", "b", "c", "d"]}}
    path.write_text(json.dumps(timeline))
    (later_checkpoint / "node-fault.json").write_text(json.dumps({"node_fault": fault}))
    return later_checkpoint, timeline, fault


def test_worker_node_validation_requires_loss_and_checkpoint_retry(modules, node_checkpoint):
    directory, _, _ = node_checkpoint
    result = modules.workload.validate(directory, selective=True, inject=False, node_scenario="worker-node")
    assert result["recoveries"][0]["worker_retry_occurred"]
    assert result["recoveries"][0]["retained_ranks"] == [1]
    assert result["recoveries"][0]["node_operation_s"] == pytest.approx(0.3)


@pytest.mark.parametrize("corruption", ["process_only", "wrong_victim", "same_node", "missing_checkpoint", "late_injection", "dead_destination", "head_destination", "same_destination", "gcs_alive"])
def test_worker_node_evidence_rejects_wrong_scope(modules, node_checkpoint, corruption):
    directory, timeline, fault = node_checkpoint
    if corruption == "process_only":
        fault["worker_node_failure"]["all_node_processes_exited"] = False
    elif corruption == "wrong_victim":
        fault["worker_node_failure"]["training_worker_pid"] = 99
    elif corruption == "same_node":
        timeline["groups"][0][1]["node_id"] = "a"
    elif corruption == "missing_checkpoint":
        fault["checkpoint"] = {}
    elif corruption == "late_injection":
        fault["operation_finished_ns"] = 4100000000
    elif corruption == "head_destination":
        timeline["groups"][1][0]["node_id"] = "head"
    elif corruption == "same_destination":
        timeline["groups"][1][0]["node_id"] = "b"
    elif corruption == "gcs_alive":
        fault["worker_node_failure"]["gcs_marked_dead"] = False
    else:
        timeline["groups"][1][0]["node_id"] = "a"
    (directory / "timeline.json").write_text(json.dumps(timeline))
    (directory / "node-fault.json").write_text(json.dumps({"node_fault": fault}))
    with pytest.raises(ValueError):
        modules.workload.validate(directory, selective=True, inject=False, node_scenario="worker-node")


def test_head_replacement_can_preserve_training_workers(modules, node_checkpoint):
    directory, timeline, fault = node_checkpoint
    timeline["groups"] = timeline["groups"][:1]
    for filename in ("2.json", "3.json"):
        (directory / "starts" / filename).unlink()
    fault.update(scenario="head-node", head_replacement={
        "failure_scope": "all_head_processes_with_surviving_gcs_storage",
        "gcs_storage_backend": "rocksdb", "original_head_processes_exited": True,
        "original_head_node_id": "head", "replacement_head_node_id": "new-head",
        "original_gcs_pid": 100, "replacement_gcs_pid": 200,
        "surviving_node_ids": ["coordinator", "a", "b", "c", "d"],
    })
    fault.pop("worker_node_failure")
    (directory / "timeline.json").write_text(json.dumps(timeline))
    (directory / "node-fault.json").write_text(json.dumps({"node_fault": fault}))
    result = modules.workload.validate(directory, selective=False, inject=False, node_scenario="head-node")
    recovery = result["recoveries"][0]
    assert recovery["worker_retry_occurred"] is False
    assert recovery["retained_ranks"] == [0, 1]
    assert recovery["failure_to_all_workers_invoked_s"] is None
    assert recovery["failure_to_next_report_s"] == pytest.approx(0.9)
    fault["head_replacement"]["original_head_processes_exited"] = False
    (directory / "node-fault.json").write_text(json.dumps({"node_fault": fault}))
    with pytest.raises(ValueError, match="Head replacement"):
        modules.workload.validate(directory, selective=False, inject=False, node_scenario="head-node")


@pytest.mark.parametrize("scenario", ["head-node", "worker-node"])
def test_node_supervisor_runs_on_main_thread_and_releases_gate(modules, tmp_path, scenario):
    import threading
    import time

    calls = []
    def crash(*args):
        assert threading.current_thread() is threading.main_thread()
        calls.append(args)
        return {"observed": True}

    def workload():
        assert threading.current_thread() is not threading.main_thread()
        modules.workload.write_record(tmp_path / "node-fault-request.json", {
            "scenario": scenario, "report_number": 3, "checkpoint": {"model": "sha"},
            "checkpoint_committed_ns": time.monotonic_ns(),
            "groups": [[{"rank": 0, "node_id": "a", "pid": 101},
                        {"rank": 1, "node_id": "b", "pid": 102}]],
        })
        deadline = time.monotonic() + 5
        while not (tmp_path / "release-report-3.json").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Supervisor failed to release the workload")
            time.sleep(.01)
        return "complete"

    assert modules.workload.training_node_fault(
        tmp_path, scenario, 3, ("a", "b", "c", "d"), "head", crash, crash, workload, 5) == "complete"
    assert calls == ([()] if scenario == "head-node" else [("a", 101)])
    fault = json.loads((tmp_path / "node-fault.json").read_text())["node_fault"]
    assert fault["completed"]
    assert fault["checkpoint_committed_ns"] <= fault["request_ns"] <= fault["operation_finished_ns"]


def test_node_supervisor_preserves_failure_before_injection(modules, tmp_path):
    def workload():
        raise LookupError("original workload failure")
    def crash(*args):
        pytest.fail("Must not inject before the requested epoch")
    with pytest.raises(LookupError, match="original workload failure"):
        modules.workload.training_node_fault(
            tmp_path, "head-node", 3, ("a", "b"), "head", crash, crash, workload, 5)
    fault = json.loads((tmp_path / "node-fault.json").read_text())["node_fault"]
    assert fault["completed"] is False
    assert (tmp_path / "release-report-3.json").exists()


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


def test_optimizer_gate_counts_real_updates_and_restores_method(modules, monkeypatch, tmp_path):
    import torch

    parameter = torch.nn.Parameter(torch.tensor([1.0]))
    optimizer = torch.optim.Adam([parameter], lr=.001)
    original = torch.optim.Adam.step
    identity = {"rank": 0, "actor_id": "actor", "pid": 10, "checkpoint": None}
    writer = modules.workload.write_record

    def release_gate(path, value):
        writer(path, value)
        if Path(path).name == "active-ready-0.json":
            assert int(optimizer.state[parameter]["step"]) == 121
            writer(tmp_path / "release-active.json", {})

    monkeypatch.setattr(modules.workload, "write_record", release_gate)
    with modules.workload.active_optimizer_steps(tmp_path, {"checkpoint_epoch": 1, "step": 3}, identity, None):
        for _ in range(122):
            parameter.grad = torch.ones_like(parameter)
            optimizer.step()
    assert torch.optim.Adam.step is original
    ready = json.loads((tmp_path / "active-ready-0.json").read_text())
    continued = json.loads((tmp_path / "active-continued-0.json").read_text())
    assert ready["epoch"] == 2 and ready["step"] == 3
    assert ready["absolute_update"] == 121 and continued["absolute_update"] == 122
    assert not (tmp_path / "active-recomputed-0.json").exists()


def test_optimizer_gate_records_recomputation_from_restored_epoch(modules, tmp_path):
    import torch
    from ray.train import Checkpoint

    checkpoint_path = tmp_path / "checkpoint"
    checkpoint_path.mkdir()
    torch.save({"epoch": 1}, checkpoint_path / "training.pt")
    checkpoint = Checkpoint.from_directory(str(checkpoint_path))
    parameter = torch.nn.Parameter(torch.tensor([1.0]))
    optimizer = torch.optim.Adam([parameter])
    identity = {"rank": 1, "actor_id": "retained", "pid": 11, "checkpoint": {"training.pt": "sha"}}
    with modules.workload.active_optimizer_steps(tmp_path, {"checkpoint_epoch": 1, "step": 3}, identity, checkpoint):
        for _ in range(3):
            parameter.grad = torch.ones_like(parameter)
            optimizer.step()
    event = json.loads((tmp_path / "active-recomputed-1.json").read_text())
    assert event["restored_epoch"] == 1
    assert event["optimizer_updates_this_invocation"] == 3
    assert event["absolute_update"] == 121
    assert not (tmp_path / "active-ready-1.json").exists()


@pytest.mark.parametrize("retry", [True, False])
def test_active_recovery_distinguishes_rollback_from_continuation(modules, tmp_path, retry):
    old = [{"rank": r, "actor_id": f"old-{r}", "pid": 10 + r} for r in (0, 1)]
    new = [{**w, "actor_id": f"new-{w['rank']}"} for w in old] if retry else old
    plan = {"checkpoint_epoch": 1, "step": 59}
    gates = [{**w, "checkpoint": None, "restored_epoch": 0, "checkpoint_epoch": 1,
              "epoch": 2, "step": 59, "absolute_update": 177,
              "optimizer_updates_this_invocation": 177, "time_ns": 200} for w in old]
    checkpoint = {"training.pt": "sha"}
    fault = {"failure_timing": "active", "groups": [old], "gates": gates,
             "checkpoint": checkpoint, "checkpoint_committed_ns": 100,
             "request_ns": 300, "operation_finished_ns": 400}
    diagnostics = {"node_fault": fault, "groups": [old, new] if retry else [old],
                   "starts": [{**w, "checkpoint": checkpoint, "time_ns": 450} for w in new] if retry else [],
                   "reports": [{"time_ns": 100}, {"time_ns": 700}], "recoveries": [{}]}
    kind = "recomputed" if retry else "continued"
    for w in new:
        modules.workload.write_record(tmp_path / f"active-{kind}-{w['rank']}.json", {
            **w, "time_ns": 600, "restored_epoch": 1 if retry else 0,
            "absolute_update": 177 if retry else 178,
            "optimizer_updates_this_invocation": 59 if retry else 178,
            "checkpoint": checkpoint if retry else None,
        })
    modules.workload.validate_active_training(tmp_path, diagnostics, plan)
    recovery = diagnostics["recoveries"][0]
    assert recovery["model_checkpoint_restored"] is retry
    assert recovery["recomputed_optimizer_steps_per_rank"] == (59 if retry else 0)
    fault["gates"][1]["optimizer_updates_this_invocation"] -= 1
    with pytest.raises(ValueError, match="uncheckpointed optimizer work"):
        modules.workload.validate_active_training(tmp_path, diagnostics, plan)


@pytest.mark.parametrize("fail_supervisor", [False, True])
def test_active_supervisor_waits_for_both_ranks_and_releases_on_error(modules, tmp_path, fail_supervisor):
    import threading
    import time

    group = [{"rank": r, "node_id": node, "pid": 10 + r, "actor_id": f"actor-{r}"}
             for r, node in enumerate(("a", "b"))]

    def workload():
        modules.workload.write_record(tmp_path / "active-checkpoint.json", {
            "groups": [group], "report_number": 1, "checkpoint": {"training.pt": "sha"},
            "checkpoint_committed_ns": time.monotonic_ns()})
        for w in group:
            modules.workload.write_record(tmp_path / f"active-ready-{w['rank']}.json", {
                **w, "checkpoint": None, "restored_epoch": 0, "checkpoint_epoch": 1,
                "epoch": 2, "step": 59, "absolute_update": 177,
                "optimizer_updates_this_invocation": 177, "time_ns": time.monotonic_ns()})
        deadline = time.monotonic() + 5
        while not (tmp_path / "release-active.json").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("Gate was not released")
            time.sleep(.01)
        return "finished"

    def crash():
        assert threading.current_thread() is threading.main_thread()
        assert all((tmp_path / f"active-ready-{r}.json").exists() for r in (0, 1))
        if fail_supervisor:
            raise LookupError("head replacement failed")
        return {"replaced": True}

    args = (tmp_path, "head-node", {"checkpoint_epoch": 1, "step": 59},
            ("a", "b"), "head", crash, None, workload, 5)
    if fail_supervisor:
        with pytest.raises(LookupError, match="head replacement failed"):
            modules.workload.active_training_fault(*args)
    else:
        assert modules.workload.active_training_fault(*args) == "finished"
    fault = json.loads((tmp_path / "node-fault.json").read_text())["node_fault"]
    assert fault["completed"] is not fail_supervisor
    assert (tmp_path / "release-active.json").exists()


@pytest.mark.parametrize("corruption", [None, "enabled_native", "enabled_mode", "full_fallback"])
def test_retry_only_pair_rejects_fixed_r_and_fallback(modules, matched_pair, corruption):
    ordinary, selective = matched_pair
    selective["mode"] = "off"
    selective["native_settings"]["enable_streaming_recovery"] = False
    if corruption == "enabled_native":
        selective["native_settings"]["enable_streaming_recovery"] = True
    elif corruption == "enabled_mode":
        selective["mode"] = "on"
    elif corruption == "full_fallback":
        selective["restart_scope"] = "full"
    if corruption:
        with pytest.raises(ValueError):
            modules.checks.compare_pair(ordinary, selective, comparison="retry")
    else:
        assert modules.checks.compare_pair(ordinary, selective, comparison="retry")["workload_s_change_pct"] == 0


def test_worker_retry_cli_defaults_to_small_node_experiment(modules, monkeypatch, tmp_path):
    captured = {}
    monkeypatch.setattr(modules.runner, "source_provenance", lambda _: {})
    def capture(args, directory, provenance):
        captured.update(vars(args))
        return 0
    monkeypatch.setattr(modules.runner, "run_comparison", capture)
    monkeypatch.setattr("sys.argv", ["run_fashion_training_comparison.py",
        "--data-directory", str(tmp_path), "--result-directory", str(tmp_path),
        "--output", str(tmp_path / "report.json")])
    assert modules.runner.main() == 0
    assert captured["comparison"] == "retry"
    assert captured["failure_kind"] == ["worker-node"]
    assert captured["failure_point"] == ["middle"]
    assert captured["failure_timing"] == "active"
    assert captured["epochs"] == 4 and captured["repeats"] == 1
