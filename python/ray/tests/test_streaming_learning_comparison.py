"""Reject misleading streaming/recovery evidence without launching Ray clusters."""

import copy
import importlib
from pathlib import Path

import pytest


@pytest.fixture
def checks(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    return importlib.import_module("streaming_learning")


def epoch_reports():
    result = []
    for epoch in (1, 2):
        metrics = [{"rank": rank, "epoch": epoch, "resumed_from_epoch": 1 if epoch == 2 else 0,
                    "sample_ids": list(range(rank * 4, rank * 4 + 4)), "train_rows": 4,
                    "input_sha256": [str(i) for i in range(rank * 4, rank * 4 + 4)],
                    "optimizer_steps": 2, "training_loss": 1.0} for rank in (0, 1)]
        metrics[0].update(validation_rows=4, accuracy=.5, validation_loss=1.0)
        result.append({"metrics": metrics})
    return result


def test_exact_image_accounting_after_checkpoint_retry(checks):
    manifest = {"selected_ids": {"train": list(range(8))}, "validation_rows": 4,
                "input_sha256": {"train": {str(i): str(i) for i in range(8)}}}
    checks.validate_epoch_samples(epoch_reports(), manifest, 2, 2, retry_epoch=1)


@pytest.mark.parametrize("corruption", ["duplicate", "wrong_tensor", "wrong_epoch", "wrong_checkpoint", "lost_update", "nan_loss"])
def test_bad_learning_evidence_is_rejected(checks, corruption):
    reports = epoch_reports()
    last = reports[1]["metrics"][1]
    if corruption == "duplicate":
        last["sample_ids"][-1] = 0
    elif corruption == "wrong_tensor":
        last["input_sha256"][0] = "changed tensor or label"
    elif corruption == "wrong_epoch":
        last["epoch"] = 3
    elif corruption == "wrong_checkpoint":
        last["resumed_from_epoch"] = 0
    elif corruption == "lost_update":
        last["optimizer_steps"] = 1
    else:
        last["training_loss"] = float("nan")
    manifest = {"selected_ids": {"train": list(range(8))}, "validation_rows": 4,
                "input_sha256": {"train": {str(i): str(i) for i in range(8)}}}
    with pytest.raises(ValueError):
        checks.validate_epoch_samples(reports, manifest, 2, 2, retry_epoch=1)


def test_lazy_input_alone_does_not_prove_overlap(checks):
    events = [{"kind": "update", "rank": rank, "epoch": 1, "time_ns": timestamp}
              for rank in (0, 1) for timestamp in (20, 40)]
    events.append({"kind": "decode", "split": "train", "started_ns": 5, "finished_ns": 10})
    assert not checks.summarize_overlap(events, 1)["every_epoch_overlapped"]
    events.append({"kind": "decode", "split": "validation", "started_ns": 22, "finished_ns": 30})
    assert not checks.summarize_overlap(events, 1)["every_epoch_overlapped"]
    events.append({"kind": "decode", "split": "train", "started_ns": 22, "finished_ns": 30})
    assert checks.summarize_overlap(events, 1)["every_epoch_overlapped"]


def test_later_epoch_decode_is_not_interrupted_epoch_progress(checks):
    events = [{"kind": "update", "rank": rank, "epoch": epoch, "time_ns": epoch * 100 + offset}
              for epoch in (1, 2, 3) for rank in (0, 1) for offset in (20, 40)]
    events.append({"kind": "decode", "split": "train", "started_ns": 310, "finished_ns": 330})
    overlap = checks.summarize_overlap(events, 3, {"report_number": 1, "request_ns": 225})
    assert overlap["decode_calls_started_after_fault_in_interrupted_epoch"] == 0


def test_active_gate_uses_declared_epoch_length(checks):
    harness = importlib.import_module("train_workload")
    plan = {"checkpoint_epoch": 2, "step": 4, "steps_per_epoch": 8}
    group = [{"rank": rank, "actor_id": str(rank), "pid": rank + 10} for rank in (0, 1)]
    fault = {"groups": [group], "checkpoint_committed_ns": 10, "request_ns": 30,
             "gates": [{**w, "restored_epoch": 0, "checkpoint": None, "checkpoint_epoch": 2,
                        "epoch": 3, "step": 4, "absolute_update": 20,
                        "optimizer_updates_this_invocation": 20, "time_ns": 20} for w in group]}
    harness.validate_active_gates(fault, plan)
    wrong = copy.deepcopy(fault)
    wrong["gates"][0]["absolute_update"] = 240  # The old Fashion-specific 118-step assumption.
    with pytest.raises(ValueError):
        harness.validate_active_gates(wrong, plan)


def test_plot_counts_actual_repeated_work_without_result_directories(checks):
    plot = importlib.import_module("plot_streaming_learning")
    sample = {"workload_started_ns": 1, "stream_events": [
        {"kind": "decode", "split": "train", "finished_ns": 5, "sample_ids": [0, 1]},
        {"kind": "decode", "split": "train", "finished_ns": 10, "sample_ids": [0, 1]},
        {"kind": "decode", "split": "validation", "finished_ns": 15, "sample_ids": [0, 1]},
    ]}
    assert plot.event_trace(sample, "decode")[1] == [0, 2, 4]


def test_timeout_plot_keeps_observed_stop_without_claiming_completion(checks):
    plot = importlib.import_module("plot_streaming_learning")
    trace = plot.learning_trace({"workload_started_ns": 10**9, "observation_finished_ns": 6 * 10**9,
                                 "timeout": True, "status": "failed", "reports": []})
    assert trace["seconds"][-1] == 5
    assert trace["epochs"][-1] == 0
    assert trace["outcome"] == "timeout (censored)"


@pytest.mark.parametrize("corruption", [None, "selective", "native", "input", "failed"])
def test_comparison_keeps_retry_policy_and_inputs_matched(checks, corruption):
    from fashion_comparison import PROVENANCE_KEYS

    left = {key: "same" for key in (
        "training_epochs", "input_identity", "workload_sha256", "torch_version", "torchvision_version",
        "model_parameters", "batch_size", "checkpoint_policy", "failure_point", "fault_after_epoch", "fault_after_step")}
    left.update(status="passed", workload_completed=True, workload_s=10, final_accuracy=.5,
                scenario="none", mode="off", restart_scope="full", owner_placement="default",
                placement_strategy="STRICT_SPREAD", provenance={key: "same" for key in PROVENANCE_KEYS},
                native_settings={"enable_streaming_recovery": False, "holders": 2})
    right = copy.deepcopy(left)
    right.update(mode="on", workload_s=12, final_accuracy=.6)
    right["native_settings"]["enable_streaming_recovery"] = True
    if corruption == "selective":
        right["restart_scope"] = "selective"
    elif corruption == "native":
        right["native_settings"]["holders"] = 3
    elif corruption == "input":
        right["input_identity"] = "different"
    elif corruption == "failed":
        right["status"] = "failed"
    if corruption:
        with pytest.raises(ValueError):
            checks.compare_samples(left, right)
    else:
        result = checks.compare_samples(left, right)
        assert result["accuracy_difference_pp"] == pytest.approx(10)
        assert result["workload_s_change_pct"] == pytest.approx(20)
        assert result["prediction_equivalence_claimed"] is False
