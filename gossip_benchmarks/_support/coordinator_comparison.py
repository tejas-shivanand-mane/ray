"""JSON evidence checks and progress traces for coordinator input recovery."""

import math
import statistics


def failure_cases(steps, points=None, custom_step=None, controls_only=False):
    """Named points are within the epoch following a committed checkpoint."""
    if steps < 2:
        raise ValueError("Use at least two optimizer updates per epoch")
    cases = [{"scenario": "none", "failure_point": "none", "fault_after_step": steps // 2}]
    if controls_only:
        return cases
    if custom_step is not None:
        if points:
            raise ValueError("Choose named failure points or a custom step")
        selected = [("custom", custom_step)]
    else:
        requested = set(points or ["middle"])
        if not requested <= {"early", "middle", "late", "all"}:
            raise ValueError("Unknown failure point")
        if "all" in requested:
            requested = {"early", "middle", "late"}
        positions = {"early": max(1, steps // 8), "middle": steps // 2,
                     "late": min(steps - 1, 7 * steps // 8)}
        selected = [(name, step) for name, step in positions.items() if name in requested]
    if any(not 0 < step < steps for _, step in selected):
        raise ValueError("Leave optimizer work before and after the fault within its epoch")
    if len({step for _, step in selected}) != len(selected):
        raise ValueError("Too few updates per epoch for distinct requested failure points")
    return cases + [{"scenario": "coordinator-process", "failure_point": name,
                     "fault_after_step": step} for name, step in selected]


def report_cases(report):
    # Reports written before the stage matrix have only one fault case.
    return report.get("cases") or [{"scenario": s, "failure_point": None,
                                    "fault_after_step": report.get("fault_after_step")}
                                   for s in report["scenarios"]]


def case_samples(report, case):
    return [s for s in report["samples"] if s["scenario"] == case["scenario"]
            and s.get("failure_point") == case["failure_point"]]


def updates(sample):
    """Use the hook's timestamp for the real update completed before injection.

    The application's update event follows Adam.step returning, so its timestamp
    includes the fault gate. The hook is also evidence if Train kills that worker
    before the application can emit its event. Never count the hook twice.
    """
    events = [dict(e) for e in sample.get("stream_events", []) if e["kind"] == "update"]
    for gate in sample.get("coordinator_fault", {}).get("gates", []):
        matches = [e for e in events if e["rank"] == gate["rank"]
                   and e["epoch"] == gate["epoch"] and e["step"] == gate["step"]
                   and e["resumed_from_epoch"] == 0]
        if len(matches) > 1:
            raise ValueError("Duplicate pre-fault optimizer update")
        if matches:
            matches[0]["time_ns"] = gate["time_ns"]
        else:
            invocations = {e["invocation"] for e in events
                           if e["rank"] == gate["rank"] and e["resumed_from_epoch"] == 0}
            if len(invocations) != 1:
                raise ValueError("Cannot associate optimizer gate with initial invocation")
            events.append({"kind": "update", "rank": gate["rank"], "epoch": gate["epoch"],
                           "step": gate["step"], "time_ns": gate["time_ns"],
                           "resumed_from_epoch": 0, "invocation": next(iter(invocations))})
    return sorted(events, key=lambda e: e["time_ns"])


def restored_epoch(sample):
    restored = [s for s in sample.get("starts", []) if s.get("checkpoint") is not None]
    if not restored:
        return 0
    matches = [i for i, report in enumerate(sample.get("reports", []), 1)
               if report.get("checkpoint") and all(s["checkpoint"] == report["checkpoint"]
                                                  and s["time_ns"] > report["time_ns"] for s in restored)]
    if len(matches) != 1:
        raise ValueError("Cannot identify the exact committed checkpoint restored by both ranks")
    return matches[0]


def validate_progress(sample, options):
    groups, starts = sample["groups"], sample["starts"]
    inject = options["scenario"] in ("coordinator-process", "coordinator-node")
    node_loss = options["scenario"] == "coordinator-node"
    resume = options["mode"] == "resume"
    if len(groups) not in (1, 2) or (len(groups) == 2 and (not inject or resume)):
        raise ValueError("Input resume must retain its training invocation; fallback is not resume success")
    if any(sorted(w["rank"] for w in g) != [0, 1] for g in groups):
        raise ValueError("Expected exactly two worker ranks per group")
    worker_key = lambda w: (w["rank"], w["actor_id"], w["pid"])
    if sorted(map(worker_key, starts)) != sorted(worker_key(w) for g in groups for w in g):
        raise ValueError("Training invocation identities do not match worker groups")
    restored = [s for s in starts if s["checkpoint"] is not None]
    retried = len(groups) == 2
    retry_epoch = restored_epoch(sample)
    if not retried and restored:
        raise ValueError("Unexpected checkpoint restoration")
    epoch, steps = options["fault_after_epoch"], options["steps_per_epoch"]
    target = epoch * steps + options["fault_after_step"]
    fault = sample.get("coordinator_fault")
    if inject:
        if (not fault or not fault.get("completed") or fault.get("error")
                or fault["scope"] != ("coordinator_node" if node_loss else "coordinator_process_only")
                or fault["groups"] != [groups[0]]
                or fault["operation_finished_ns"] < fault["request_ns"]):
            raise ValueError("Missing coordinator-only fault evidence")
        before, after = set(fault["alive_nodes_before"]), set(fault["alive_nodes_after"])
        target_node = fault["old"]["node_id"]
        if node_loss:
            loss = fault.get("node_failure", {})
            protected = {fault.get("driver_node_id"), fault["old"].get("owner_node_id"),
                         *(w["node_id"] for w in groups[0])}
            if (target_node not in before or after != before - {target_node}
                    or None in protected or not protected <= after
                    or loss.get("node_id") != target_node
                    or loss.get("all_node_processes_exited") is not True
                    or loss.get("gcs_marked_dead") is not True
                    or fault["old"].get("pid") not in loss.get("node_process_pids", [])
                    or set(loss.get("surviving_node_ids", [])) != after):
                raise ValueError("Missing isolated coordinator-node loss evidence")
        elif before != after or target_node not in after:
            raise ValueError("Process-only failure unexpectedly lost a node")
        committed = sample["reports"][epoch - 1]
        if (fault["report_number"] != epoch or not committed["checkpoint"]
                or fault["checkpoint"] != committed["checkpoint"]
                or fault["checkpoint_committed_ns"] != committed["time_ns"]
                or committed["time_ns"] >= fault["request_ns"]):
            raise ValueError("Fault checkpoint differs from committed report")
        if resume:
            new = fault.get("new")
            if (not new or new["worker_id"] == fault["old"]["worker_id"]
                    or new["node_id"] not in after
                    or ((new["node_id"] == target_node) == node_loss)):
                raise ValueError("No confirmed coordinator process replacement")
            if not any(e["worker_id"] == new["worker_id"] and e["state"] == "completed"
                       and e["time_ns"] > fault["request_ns"] for e in sample["data_executions"]):
                raise ValueError("No completed data execution in the replacement coordinator")
        elif fault.get("new") is not None:
            raise ValueError("Unexpected coordinator restart in checkpoint baseline")
        if retried:
            if {w["actor_id"] for w in groups[0]} & {w["actor_id"] for w in groups[1]}:
                raise ValueError("Expected full-group Train retry")
            if (not epoch <= retry_epoch < options["training_epochs"]
                    or sorted(map(worker_key, restored)) != sorted(map(worker_key, groups[1]))
                    or any(s["time_ns"] <= fault["request_ns"] for s in restored)):
                raise ValueError("Retry must restore the exact committed checkpoint on both ranks")
    elif fault:
        raise ValueError("Unexpected control fault")

    events = updates(sample)
    total = options["training_epochs"] * steps
    repeated, first, caught_up = {}, [], []
    for rank in (0, 1):
        rank_events = [e for e in events if e["rank"] == rank]
        origins = {e["resumed_from_epoch"] for e in rank_events}
        if origins != ({0, retry_epoch} if retried else {0}):
            raise ValueError("Optimizer telemetry has unexpected checkpoint origins")
        for origin in origins:
            attempt = [e for e in rank_events if e["resumed_from_epoch"] == origin]
            if len({e["invocation"] for e in attempt}) != 1:
                raise ValueError("Unexpected train-loop reentry")
            indices = [(e["epoch"] - 1) * steps + e["step"] for e in attempt]
            if any(not 1 <= e["step"] <= steps for e in attempt):
                raise ValueError("Invalid optimizer step")
            end = max(indices)
            if indices != list(range(origin * steps + 1, end + 1)):
                raise ValueError("Missing, duplicated or reordered optimizer updates")
            if (not retried or origin == retry_epoch) and end != total:
                raise ValueError("Final invocation did not complete the training work")
            if retried and origin == 0 and not max(target, retry_epoch * steps) <= end <= total:
                raise ValueError("Retry was not within the selected unfinished epoch")
        repeated[str(rank)] = len(rank_events) - total
        if repeated[str(rank)] < 0 or (not retried and repeated[str(rank)] != 0):
            raise ValueError("Unexpected repeated optimizer work")
        if inject:
            continuing = [e for e in rank_events if e["time_ns"] > fault["request_ns"]
                          and e["resumed_from_epoch"] == (retry_epoch if retried else 0)
                          and (retried or (e["epoch"] - 1) * steps + e["step"] > target)]
            beyond = [e for e in continuing if (e["epoch"] - 1) * steps + e["step"] > target]
            if not continuing or not beyond:
                raise ValueError("No optimizer progress beyond pre-fault weights")
            first.append(min(e["time_ns"] for e in continuing))
            caught_up.append(min(e["time_ns"] for e in beyond))
    sample["restored_checkpoint_epoch"] = retry_epoch
    sample["recovery"] = {
        "kind": ("checkpoint_retry" if retried else "input_resume" if inject and resume
                 else "continued" if inject else "control"),
        "checkpoint_restored": retried, "retained_training_workers": not retried,
        "repeated_optimizer_updates_per_rank": repeated,
        "optimizer_accounting": "observed completed updates; a killed worker may lose its final telemetry write",
        "failure_to_both_ranks_next_update_s": (max(first) - fault["request_ns"]) / 1e9 if inject else None,
        "failure_to_both_ranks_beyond_prefault_progress_s": (
            (max(caught_up) - fault["request_ns"]) / 1e9 if inject else None
        ),
    }
    # Count completed decode work, including replay; no inference from time alone.
    sample["decoded_training_rows"] = sum(len(e["sample_ids"]) for e in sample["stream_events"]
                                           if e["kind"] == "decode" and e["split"] == "train")


def compare_samples(left, right, *, control=False):
    """Return right relative to left; fault comparison uses control on the left."""
    from fashion_comparison import PROVENANCE_KEYS

    for sample in (left, right):
        if (sample["status"] != "passed" or not sample.get("workload_completed") or sample.get("timeout")
                or not math.isfinite(sample["workload_s"]) or sample["workload_s"] <= 0):
            raise ValueError("Only validated completed observations can be paired")
        if (sample["fixed_r_enabled"] is not False or sample["selective_retry"] is not False
                or sample["restart_scope"] != "full" or sample["train_max_failures"] != 1
                or sample["owner_placement"] != "default"
                or sample["placement_strategy"] != "STRICT_SPREAD"):
            raise ValueError("Recovery, ownership or retry settings differ from the comparison contract")
        mode = sample["mode"]
        if (mode not in ("ordinary", "deterministic", "resume")
                or sample["sharding"] != ("ordinary" if mode == "ordinary" else "deterministic_chunks")
                or sample["coordinator_restart_budget"] != (1 if mode == "resume" else 0)):
            raise ValueError("Unexpected splitter configuration")
    for key in ("training_epochs", "input_identity", "workload_sha256", "torch_version", "torchvision_version",
                "model_parameters", "batch_size", "checkpoint_policy", "native_settings"):
        if left[key] != right[key]:
            raise ValueError(f"Mismatched {key}")
    placement = lambda s: s.get("coordinator_placement", "owner_node_hard_affinity")
    if placement(left) != placement(right):
        raise ValueError("Mismatched coordinator placement")
    for sample in (left, right):
        if sample["scenario"] == "coordinator-node" and (
                placement(sample) != "separate_node_soft_affinity" or sample["mode"] == "ordinary"):
            raise ValueError("Node loss requires the matched separate-coordinator topology")
    if not left["native_settings"] or any(v is not False for k, v in left["native_settings"].items()
                                               if k.startswith("enable_")):
        raise ValueError("Fixed-R native protection must be disabled")
    for key in PROVENANCE_KEYS:
        if left["provenance"][key] != right["provenance"][key]:
            raise ValueError(f"Mismatched provenance: {key}")
    equivalent = False
    if control:
        if left["scenario"] != "none" or left["mode"] != right["mode"] or left["pair"] != right["pair"]:
            raise ValueError("Expected a matching no-failure control")
    elif (left["scenario"] != right["scenario"] or left["pair"] != right["pair"]
          or left.get("failure_point") != right.get("failure_point")
          or left["fault_after_epoch"] != right["fault_after_epoch"]
          or left["fault_after_step"] != right["fault_after_step"]):
        raise ValueError("Mismatched fault configuration")
    if left["sharding"] == right["sharding"] == "deterministic_chunks":
        for a, b in zip(left["reports"], right["reports"]):
            if a["checkpoint"] != b["checkpoint"]:
                raise ValueError("Deterministic recovery changed model/optimizer/RNG checkpoint")
            ids = lambda r: [m["sample_ids"] for m in sorted(r["metrics"], key=lambda m: m["rank"])]
            if ids(a) != ids(b):
                raise ValueError("Deterministic recovery changed per-rank sample order")
        if len(left["reports"]) != len(right["reports"]):
            raise ValueError("Missing recovered epochs")
        equivalent = True
    return {"left_workload_s": left["workload_s"], "right_workload_s": right["workload_s"],
            "workload_s_change_pct": 100 * (right["workload_s"] / left["workload_s"] - 1),
            "accuracy_difference_pp": 100 * (right["final_accuracy"] - left["final_accuracy"]),
            "exact_checkpoint_and_sample_order_match": equivalent,
            "decoded_training_rows_change": right["decoded_training_rows"] - left["decoded_training_rows"]}


def summarize_comparisons(report):
    """Summarize only evidence-validated pairs, exposing missing repetitions.

    A standard deviation is descriptive spread across pairs, not a confidence
    interval. Average the per-pair percentages instead of taking a ratio of means.
    """
    def describe(values):
        return {"mean": statistics.mean(values) if values else None,
                "stdev": statistics.stdev(values) if len(values) > 1 else None,
                "values": values}

    result = []
    for case in report_cases(report):
        scenario, point = case["scenario"], case["failure_point"]
        for left, right in report["comparison_modes"]:
            rows = sorted((r for r in report["comparisons"] if r["scenario"] == scenario
                           and r.get("failure_point") == point
                           and (r["left"], r["right"]) == (left, right)), key=lambda r: r["pair"])
            pairs = [r["pair"] for r in rows]
            if len(pairs) != len(set(pairs)) or any(not 1 <= p <= report["repeats"] for p in pairs):
                raise ValueError("Duplicate or unexpected comparison repetition")
            result.append({
                "scenario": scenario, "failure_point": point, "left": left, "right": right,
                "requested_pairs": report["repeats"], "completed_pairs": len(rows),
                "included_pairs": pairs,
                "missing_pairs": [p for p in range(1, report["repeats"] + 1) if p not in pairs],
                "left_workload_s": describe([r["left_workload_s"] for r in rows]),
                "right_workload_s": describe([r["right_workload_s"] for r in rows]),
                "paired_change_pct": describe([r["workload_s_change_pct"] for r in rows]),
                "paired_difference_s": describe([r["right_workload_s"] - r["left_workload_s"] for r in rows]),
            })
    return result


def progress_trace(sample, steps):
    """Minimum current optimizer position across ranks, resetting only on restore.

    A failure/timeout endpoint remains censored. Startup and checkpoint validation
    are outside the workload clock. The plateau after the last update includes
    application validation/checkpoint work, not extra optimizer work.
    """
    start = sample.get("workload_started_ns")
    if start is None:
        return {"seconds": [], "updates": [], "outcome": "failed before workload"}
    events = [(e["time_ns"], "update", e) for e in updates(sample)]
    # Reset once, at the first observed replacement invocation. That is when the
    # rollback is visible, not when an asynchronously requested kill is issued.
    restored = [s for s in sample.get("starts", []) if s.get("checkpoint") is not None]
    if restored:
        origin = restored_epoch(sample)
        events.append((min(s["time_ns"] for s in restored), "restore", origin))
    events.sort(key=lambda item: item[0])
    position = [0, 0]
    x, y = [0.0], [0]
    for timestamp, kind, event in events:
        if kind == "restore":
            position = [event * steps] * 2
        else:
            position[event["rank"]] = max(position[event["rank"]], (event["epoch"] - 1) * steps + event["step"])
        x.append(max(0, (timestamp - start) / 1e9))
        y.append(min(position))
    passed = sample.get("status") == "passed" and sample.get("workload_completed") and not sample.get("timeout")
    end = (sample.get("workload_finished_ns") if sample.get("workload_completed") and not sample.get("timeout")
           else sample.get("observation_finished_ns"))
    if end is not None:
        x.append(max(x[-1], (end - start) / 1e9))
        y.append(y[-1])
    return {"seconds": x, "updates": y,
            "outcome": "completed" if passed else "timeout (censored)" if sample.get("timeout") else "failed"}
