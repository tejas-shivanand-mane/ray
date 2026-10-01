"""Owner-loss evidence and controlled progress checks without a Ray cluster."""

import copy
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
    return NS(runner=importlib.import_module("run_fashion_owner_comparison"),
              workload=importlib.import_module("train_workload"),
              plot=importlib.import_module("plot_fashion_owner_recovery"))


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("inject", [False, True])
def test_ordered_gate_precedes_computation_and_preserves_outputs(modules, monkeypatch, tmp_path, streaming, inject):
    module = modules.workload
    module.write_record(tmp_path / "owner-progress-plan.json", {"map_count": 4, "target_index": 2, "inject": inject})
    module.write_record(tmp_path / "map-computed-1.json", {"index": 1})
    calls, outputs = [], [object(), object()]
    monkeypatch.setattr(module, "wait_for_owner_fault", lambda path, index: calls.append(("gate", index)))
    monkeypatch.setattr(module, "record_map_computed", lambda path, index: calls.append(("computed", index)))

    def original(*args):
        calls.append(("compute", 2))
        return (value for value in outputs) if streaming else outputs

    wrapped = module.owner_gated_map(original, str(tmp_path), streaming)
    result = (list(wrapped(NS(_map_args=[None, None, True]), 2, object(), 4)) if streaming
              else wrapped(2, object(), 4, None, None, True, 0))
    assert result == outputs
    assert calls == ([("gate", 2)] if inject else []) + [("compute", 2), ("computed", 2)]


def test_later_maps_cannot_compute_before_selected_failure(modules, monkeypatch, tmp_path):
    module = modules.workload
    module.write_record(tmp_path / "owner-progress-plan.json", {"map_count": 4, "target_index": 0, "inject": True})
    module.write_record(tmp_path / "map-computed-0.json", {"index": 0})
    times = iter([0, 61])
    monkeypatch.setattr(module.time, "monotonic", lambda: next(times, 61))
    with pytest.raises(TimeoutError, match="Ordered shuffle"):
        module.before_ordered_map(tmp_path, 1)


@pytest.fixture
def pair(modules, tmp_path):
    result = []
    for mode in ("off", "on"):
        directory = tmp_path / mode
        directory.mkdir()
        np.save(directory / "predictions.npy", np.ones((10000, 10), dtype=np.float32))
        plan = {"map_count": 4, "target_index": 2, "inject": True}
        progress = [{"index": i, "time_ns": (i + 2 if i < 2 else i + 6) * 10**9,
                     "task_id": f"task-{i}", "node_id": "executor"} for i in range(4)]
        owner = {"task_id": "task-2", "owner_node_id": "head", "owner_worker_id": "owner", "recorded_ns": 2 * 10**9}
        if mode == "off":
            owner["object_ref_hex"] = "metadata-2"
        result.append({
            "mode": mode, "scenario": "data-owner", "failure_point": "middle", "pair": 1,
            "status": "passed" if mode == "on" else "failed", "validation_status": "passed" if mode == "on" else "failed",
            "workload_completed": mode == "on", "directory": str(directory), "workload_s": 12,
            "workload_started_ns": 10**9, "workload_finished_ns": 13 * 10**9,
            "training_epochs": 8, "workload_sha256": "workload", "torch_version": "torch",
            "input_identity": {"dataset": "Fashion-MNIST"}, "numerical_probe": True,
            "restart_scope": "full", "owner_placement": "head", "placement_strategy": "STRICT_SPREAD",
            "selected_owner_node_id": "head", "shuffle_owner": owner,
            "owner_progress_plan": plan, "map_progress": progress,
            "no_failure_control_passed": True, "matches_no_failure_predictions": mode == "on",
            "native_settings": {"enable_recovery_streaming_fixed_r": mode == "on", "holders": 2},
            "provenance": {key: "same" for key in modules.runner.PROVENANCE_KEYS},
            "data_owner_fault": {
                "completed": True, "stage": "RandomShuffle.map", "request_ns": 5 * 10**9,
                "replacement_ready_ns": 6 * 10**9, "submission_batch_settled": True,
                "fixed_r_submission_batch_settled": mode == "on", "ownership": owner.copy(),
                "progress_plan": plan, "completed_maps_before_failure": copy.deepcopy(progress[:2]),
                "target": {"task_id": "task-2", "map_index": 2, "node_id": "executor", "blocked_ns": 4 * 10**9},
                "head_replacement": {"original_head_processes_exited": True,
                                     "failure_scope": "all_head_processes_with_surviving_gcs_storage",
                                     "gcs_storage_backend": "rocksdb", "original_head_node_id": "head",
                                     "replacement_head_node_id": "new-head", "original_gcs_pid": 100,
                                     "replacement_gcs_pid": 200, "surviving_node_ids": ["executor"]},
            },
            "ordinary_owner_loss": {"error_type": "OwnerDiedError", "source": "shuffle_metadata_fetch",
                                    "object_ref_hex": "metadata-2", "owner_node_id": "head",
                                    "owner_worker_id": "owner", "observed_ns": 7 * 10**9},
            "data_exchanges": [{"operator": "RandomShuffle", "fixed_r_recovered_tasks": 1 if mode == "on" else 0,
                                "fixed_r_recovered_task_details": [{"task_id": "task-2"}] if mode == "on" else []}],
        })
    return result


def test_owner_pair_reports_recovery_without_fabricated_speedup(modules, pair):
    values = modules.runner.compare_owner_pair(*pair)
    assert values["owner_loss_demonstrated"]
    assert values["on_vs_off_pct"] is None
    assert values["off_s"] is None
    assert pair[0]["status"] == "failed"


@pytest.mark.parametrize("corruption", ["prefix", "point", "replay", "timeout", "input", "late_compute", "wrong_head"])
def test_owner_pair_rejects_mismatched_evidence(modules, pair, corruption):
    off, on = pair
    if corruption == "prefix":
        on["data_owner_fault"]["completed_maps_before_failure"].pop()
    elif corruption == "point":
        on["data_owner_fault"]["target"]["map_index"] = 3
    elif corruption == "replay":
        on["data_exchanges"][0]["fixed_r_recovered_task_details"] = []
    elif corruption == "timeout":
        off["timeout"] = True
    elif corruption == "input":
        off["input_identity"] = {}
    elif corruption == "late_compute":
        on["data_owner_fault"]["completed_maps_before_failure"][1]["time_ns"] = 7 * 10**9
    else:
        on["data_owner_fault"]["head_replacement"]["replacement_head_node_id"] = "head"
    with pytest.raises(ValueError):
        modules.runner.compare_owner_pair(off, on)


def test_plot_keeps_owner_loss_failed_and_ignores_cleanup_computation(modules, pair):
    off = pair[0]
    off["expected_owner_loss"] = True
    trace = modules.plot.map_trace(off)
    assert trace["outcome"] == "OwnerDiedError"
    assert trace["epochs"][-1] == 2
    assert trace["seconds"][-1] == 6
    assert trace["fault_s"] == 4


def test_matrix_runs_eight_observations_with_standard_retries(modules, monkeypatch, tmp_path, pair):
    calls = []
    monkeypatch.setattr(modules.runner, "input_identity", lambda _: {})
    monkeypatch.setattr(modules.runner, "match_control", lambda *args: None)
    monkeypatch.setattr(modules.runner, "compare_owner_pair", lambda off, on: {
        "off_completed": off["scenario"] == "none", "on_completed": True,
        "on_vs_off_pct": 0 if off["scenario"] == "none" else None,
        "owner_loss_demonstrated": off["scenario"] != "none",
    })

    def observe(options, number, directory, provenance):
        calls.append(options)
        sample = copy.deepcopy(pair[options["mode"] == "on"])
        sample.update(scenario=options["scenario"])
        if options["scenario"] == "none":
            sample.update(status="passed", workload_completed=True)
        return sample

    monkeypatch.setattr(modules.runner, "run_observation", observe)
    args = NS(data_directory=tmp_path, epochs=8, repeats=1, timeout_s=180,
              failure_point=["early", "middle", "late"], output=tmp_path / "report.json")
    assert modules.runner.run_comparison(args, tmp_path, {}) == 0
    assert len(calls) == 8
    assert all(o["restart_scope"] == "full" and o["owner_placement"] == "head" for o in calls)
    assert [o["owner_progress_plan"]["target_index"] for o in calls] == [0, 0, 0, 0, 2, 2, 3, 3]
    report = json.loads(args.output.read_text())
    assert all(s["status"] == "failed" and s["expected_owner_loss"] for s in report["samples"][2::2])
    assert len(report["pairs"]) == 4
