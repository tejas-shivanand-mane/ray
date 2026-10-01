"""Evidence regressions; these tests do not launch Ray or train a model."""

import copy
import importlib
from pathlib import Path

import pytest


@pytest.fixture
def checks(monkeypatch):
    root = Path(__file__).resolve().parents[3]
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks"))
    monkeypatch.syspath_prepend(str(root / "gossip_benchmarks/_support"))
    return importlib.import_module("coordinator_comparison")


def evidence(mode="resume", retry=False, fault=True):
    options = {"scenario": "coordinator-process" if fault else "none", "mode": mode,
               "training_epochs": 2, "steps_per_epoch": 4, "fault_after_epoch": 1, "fault_after_step": 2}
    group = [{"rank": r, "actor_id": f"old-{r}", "pid": 10 + r, "node_id": f"node-{r}"} for r in (0, 1)]
    groups = [group]
    starts = [{**w, "checkpoint": None, "time_ns": 10} for w in group]
    reports = [{"checkpoint": {"training.pt": f"hash-{epoch}"}, "time_ns": 50 if epoch == 1 else 300,
                "metrics": [{"rank": r, "sample_ids": [r]} for r in (0, 1)]} for epoch in (1, 2)]
    events = []
    for r in (0, 1):
        for index in range(1, 7 if retry else 9):
            events.append({"kind": "update", "rank": r, "epoch": (index - 1) // 4 + 1,
                           "step": (index - 1) % 4 + 1, "resumed_from_epoch": 0,
                           "invocation": f"old-{r}", "time_ns": 10 + index * 8 if index < 6 else 100 + index * 8})
    if retry:
        new = [{**w, "actor_id": f"new-{w['rank']}", "pid": 20 + w["rank"]} for w in group]
        groups.append(new)
        starts += [{**w, "checkpoint": reports[0]["checkpoint"], "time_ns": 180} for w in new]
        for r in (0, 1):
            for step in range(1, 5):
                events.append({"kind": "update", "rank": r, "epoch": 2, "step": step,
                               "resumed_from_epoch": 1, "invocation": f"new-{r}", "time_ns": 180 + step * 10})
    sample = {"groups": groups, "starts": starts, "reports": reports, "stream_events": events,
              "data_executions": [{"worker_id": "replacement", "state": "completed", "time_ns": 290}],
              "workload_started_ns": 1, "workload_finished_ns": 310, "workload_completed": True,
              "observation_finished_ns": 320, "status": "passed", **options}
    if fault:
        sample["coordinator_fault"] = {
            "completed": True, "scope": "coordinator_process_only", "groups": [copy.deepcopy(group)],
            "report_number": 1, "checkpoint": reports[0]["checkpoint"], "checkpoint_committed_ns": 50,
            "request_ns": 100, "operation_finished_ns": 120,
            "alive_nodes_before": ["owner", "node-0", "node-1"], "alive_nodes_after": ["owner", "node-0", "node-1"],
            "old": {"worker_id": "original", "node_id": "owner"},
            "new": {"worker_id": "replacement", "node_id": "owner"} if mode == "resume" else None,
            "gates": [{"rank": r, "epoch": 2, "step": 2, "time_ns": 90} for r in (0, 1)],
        }
    return sample, options


@pytest.mark.parametrize("mode,retry", [("ordinary", True), ("ordinary", False), ("resume", False), ("deterministic", True)])
def test_real_outcome_is_recorded_without_forcing_baseline_retry(checks, mode, retry):
    sample, options = evidence(mode, retry)
    checks.validate_progress(sample, options)
    assert sample["recovery"]["checkpoint_restored"] == retry
    assert sample["recovery"]["repeated_optimizer_updates_per_rank"] == {"0": 2 if retry else 0, "1": 2 if retry else 0}


@pytest.mark.parametrize("corruption", ["fallback", "old_coordinator", "no_execution", "node_loss", "checkpoint", "missing_update", "duplicate", "reentry"])
def test_resume_rejects_misleading_success(checks, corruption):
    sample, options = evidence(retry=corruption == "fallback")
    if corruption == "old_coordinator":
        sample["coordinator_fault"]["new"]["worker_id"] = "original"
    elif corruption == "no_execution":
        sample["data_executions"] = []
    elif corruption == "node_loss":
        sample["coordinator_fault"]["alive_nodes_after"].remove("node-0")
    elif corruption == "checkpoint":
        sample["coordinator_fault"]["checkpoint"] = {"training.pt": "wrong"}
    elif corruption == "missing_update":
        sample["stream_events"].pop(0)
    elif corruption == "duplicate":
        sample["stream_events"].append(copy.deepcopy(sample["stream_events"][0]))
    elif corruption == "reentry":
        sample["stream_events"][0]["invocation"] = "unexpected"
    with pytest.raises(ValueError):
        checks.validate_progress(sample, options)


def test_gate_supplies_missing_post_step_telemetry_once(checks):
    sample, options = evidence("ordinary", True)
    sample["stream_events"] = [e for e in sample["stream_events"]
                               if not (e["resumed_from_epoch"] == 0 and e["epoch"] == 2 and e["step"] == 2)]
    checks.validate_progress(sample, options)
    assert sample["recovery"]["repeated_optimizer_updates_per_rank"] == {"0": 2, "1": 2}


def test_plot_rollback_requires_observed_checkpoint_restore(checks):
    baseline, _ = evidence("ordinary", True)
    trace = checks.progress_trace(baseline, 4)
    assert any(a == 6 and b == 4 for a, b in zip(trace["updates"], trace["updates"][1:]))
    assert trace["updates"][-1] == 8
    resumed, _ = evidence()
    trace = checks.progress_trace(resumed, 4)
    assert trace["updates"] == sorted(trace["updates"])
    assert trace["updates"][-1] == 8


def test_timeout_is_not_promoted_to_completion(checks):
    sample, _ = evidence()
    sample.update(timeout=True, status="failed")
    trace = checks.progress_trace(sample, 4)
    assert trace["outcome"] == "timeout (censored)"
    assert trace["seconds"][-1] == (320 - 1) / 1e9


def paired_samples():
    from fashion_comparison import PROVENANCE_KEYS
    left, _ = evidence("ordinary", fault=False)
    left.update({k: "same" for k in ("input_identity", "workload_sha256", "torch_version", "torchvision_version",
                                    "model_parameters", "batch_size", "checkpoint_policy")})
    left.update(fixed_r_enabled=False, selective_retry=False, restart_scope="full", train_max_failures=1,
                owner_placement="default", placement_strategy="STRICT_SPREAD", sharding="ordinary",
                coordinator_restart_budget=0, native_settings={"enable_streaming_recovery": False},
                provenance={key: "same" for key in PROVENANCE_KEYS}, workload_s=10, final_accuracy=.4,
                pair=1, decoded_training_rows=16)
    right = copy.deepcopy(left)
    right.update(mode="resume", sharding="deterministic_chunks", coordinator_restart_budget=1, workload_s=12)
    return left, right


@pytest.mark.parametrize("corruption", [None, "retry", "fixed_r", "sharding", "native", "failed"])
def test_comparison_contract(checks, corruption):
    left, right = paired_samples()
    if corruption == "retry":
        right["train_max_failures"] = 0
    elif corruption == "fixed_r":
        right["fixed_r_enabled"] = True
    elif corruption == "sharding":
        right["sharding"] = "ordinary"
    elif corruption == "native":
        right["native_settings"]["enable_streaming_recovery"] = True
    elif corruption == "failed":
        right["status"] = "failed"
    if corruption:
        with pytest.raises(ValueError):
            checks.compare_samples(left, right)
    else:
        assert checks.compare_samples(left, right)["workload_s_change_pct"] == pytest.approx(20)


def test_deterministic_fault_must_match_own_control(checks):
    _, control = paired_samples()
    fault = copy.deepcopy(control)
    fault["scenario"] = "coordinator-process"
    assert checks.compare_samples(control, fault, control=True)["exact_checkpoint_and_sample_order_match"]
    fault["reports"][-1]["checkpoint"]["training.pt"] = "changed model or optimizer"
    with pytest.raises(ValueError):
        checks.compare_samples(control, fault, control=True)


@pytest.mark.parametrize("retry", [False, True])
def test_failed_checkpoint_completion_stays_failed_in_plot(checks, retry):
    sample, _ = evidence("ordinary", retry)
    sample.update(status="failed", error="Final evidence check failed")
    assert checks.progress_trace(sample, 4)["outcome"] == "failed"


def test_wrong_checkpoint_delivery_is_rejected(checks):
    sample, options = evidence("ordinary", True)
    sample["starts"][-1]["checkpoint"] = {"training.pt": "older checkpoint"}
    with pytest.raises(ValueError):
        checks.validate_progress(sample, options)


@pytest.mark.parametrize("resume", [False, True])
def test_async_kill_is_observed_not_assumed(checks, monkeypatch, resume):
    harness = importlib.import_module("coordinator_training")
    old = {"worker_id": "old", "node_id": "node"}
    new = {"worker_id": "new", "node_id": "node"}
    calls = []
    def probe(actor, timeout):
        calls.append(actor)
        if len(calls) == 1:
            return old
        if resume:
            return new
        raise harness.ray.exceptions.RayActorError()
    monkeypatch.setattr(harness, "probe", probe)
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)
    assert harness.await_fault("actor", old, resume) == (new if resume else None)
    assert len(calls) == 2


def test_baseline_may_finish_buffered_epoch_before_retry(checks):
    sample, options = evidence("ordinary", True)
    for rank in (0, 1):
        for step in (3, 4):
            sample["stream_events"].append({"kind": "update", "rank": rank, "epoch": 2, "step": step,
                                            "resumed_from_epoch": 0, "invocation": f"old-{rank}",
                                            "time_ns": 150 + step})
    checks.validate_progress(sample, options)
    assert sample["recovery"]["repeated_optimizer_updates_per_rank"] == {"0": 4, "1": 4}


def test_plot_uses_checkpoint_actually_delivered_not_fault_epoch(checks):
    sample, _ = evidence("ordinary", True)
    sample["reports"][1]["time_ns"] = 160
    for start in sample["starts"]:
        if start["checkpoint"]:
            start["checkpoint"] = sample["reports"][1]["checkpoint"]
    assert checks.restored_epoch(sample) == 2


@pytest.mark.parametrize("changed", [None, "checkpoint", "sample_order"])
def test_same_sharding_arms_must_match_each_other(checks, changed):
    _, right = paired_samples()
    left = copy.deepcopy(right)
    left.update(mode="deterministic", coordinator_restart_budget=0)
    if changed == "checkpoint":
        right["reports"][-1]["checkpoint"]["training.pt"] = "different weights"
    elif changed == "sample_order":
        right["reports"][-1]["metrics"][0]["sample_ids"] = [99]
    if changed:
        with pytest.raises(ValueError):
            checks.compare_samples(left, right)
    else:
        assert checks.compare_samples(left, right)["exact_checkpoint_and_sample_order_match"]


def test_summary_averages_paired_changes_and_exposes_missing_pairs(checks):
    report = {"scenarios": ["none", "coordinator-process"], "repeats": 3,
              "comparison_modes": [("deterministic", "resume")], "comparisons": [
                  {"scenario": "none", "left": "deterministic", "right": "resume", "pair": 1,
                   "left_workload_s": 10, "right_workload_s": 8, "workload_s_change_pct": -20},
                  {"scenario": "none", "left": "deterministic", "right": "resume", "pair": 3,
                   "left_workload_s": 20, "right_workload_s": 18, "workload_s_change_pct": -10},
              ]}
    control, fault = checks.summarize_comparisons(report)
    assert control["completed_pairs"] == 2
    assert control["included_pairs"] == [1, 3]
    assert control["missing_pairs"] == [2]
    assert control["paired_change_pct"]["mean"] == -15
    assert control["paired_change_pct"]["stdev"] == pytest.approx(50 ** .5)
    assert control["paired_difference_s"]["values"] == [-2, -2]
    assert fault["completed_pairs"] == 0
    assert fault["paired_change_pct"]["mean"] is None
    assert fault["paired_change_pct"]["stdev"] is None
    report["comparisons"] = report["comparisons"][:1]
    assert checks.summarize_comparisons(report)[0]["paired_change_pct"]["stdev"] is None
    report["comparisons"].append(copy.deepcopy(report["comparisons"][0]))
    with pytest.raises(ValueError):
        checks.summarize_comparisons(report)


@pytest.mark.parametrize("failed_control", [False, True])
def test_short_runner_pairs_only_requested_modes_and_valid_observations(checks, monkeypatch, tmp_path, failed_control):
    import json
    runner = importlib.import_module("run_coordinator_training_comparison")
    monkeypatch.setattr(runner, "input_identity", lambda _: {"training_rows": 16})
    monkeypatch.setattr(runner, "source_provenance", lambda _: {})
    calls = []

    def observe(options, pair, directory, provenance):
        calls.append((options["scenario"], pair, options["mode"]))
        _, sample = paired_samples()
        sample.update(mode=options["mode"], scenario=options["scenario"], pair=pair,
                      coordinator_restart_budget=1 if options["mode"] == "resume" else 0)
        if failed_control and calls[-1] == ("none", 2, "resume"):
            sample.update(status="failed", workload_completed=False, timeout=True, error="timed out")
        return sample

    monkeypatch.setattr(runner, "run_observation", observe)
    monkeypatch.setattr(runner.sys, "platform", "linux")
    monkeypatch.setattr(runner.sys, "argv", ["comparison", "--same-sharding-only", "--repeats", "2",
                                            "--batch-size", "2", "--data-directory", str(tmp_path),
                                            "--result-directory", str(tmp_path),
                                            "--output", str(tmp_path / "report.json")])
    assert runner.main() == (1 if failed_control else 0)
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["modes"] == ["deterministic", "resume"]
    assert calls[:4] == [("none", 1, "deterministic"), ("none", 1, "resume"),
                         ("none", 2, "resume"), ("none", 2, "deterministic")]
    assert report["observations_per_repetition"] == 4
    assert len(calls) == (7 if failed_control else 8)
    assert all(row["completed_pairs"] == (1 if failed_control else 2) for row in report["summary"])
    assert bool(report["skipped"]) == failed_control
    assert all(row["exact_checkpoint_and_sample_order_match"] for row in report["comparisons"])


def test_short_mode_and_three_arm_mode_are_mutually_exclusive(checks, monkeypatch, tmp_path):
    runner = importlib.import_module("run_coordinator_training_comparison")
    monkeypatch.setattr(runner.sys, "argv", ["comparison", "--same-sharding-only",
                                            "--include-deterministic-baseline", "--data-directory", str(tmp_path)])
    with pytest.raises(SystemExit) as raised:
        runner.main()
    assert raised.value.code == 2


def test_plot_caption_describes_actual_sharding(checks):
    plot = importlib.import_module("plot_coordinator_training")
    assert "Identical deterministic sharding" in plot.comparison_caption({"modes": ["deterministic", "resume"]})
    assert "sharding differ" in plot.comparison_caption({"modes": ["ordinary", "deterministic", "resume"]})
