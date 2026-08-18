#!/usr/bin/env python3
"""Patch 4A diagnostic: holder-formation cost vs actual downstream borrowers.

Fixed target:
    recovery_succession_target_holder_count = 4

Cases:
    borrowers = 0, 1, 2, 3, 4

For each borrower count:
    Disabled
    Succession-R4

The Disabled case is paired with Succession-R4 because increasing the borrower
count also adds application actor hops. Comparing the two at the same borrower
count isolates the recovery-succession overhead from the base pipeline cost.

Outputs:
    patch4a_runs.csv
    patch4a_summary.csv
    plots/throughput_vs_borrowers_*.png
    plots/p50_latency_vs_borrowers_*.png
    plots/p95_latency_vs_borrowers_*.png
    plots/holders_vs_borrowers_*.png
    plots/formation_components_*.png
"""
from __future__ import annotations

import os

# Set before importing ray.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import argparse
import math
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    add_method_columns,
    disabled,
    mean_ci95,
    percentile,
    read_csv,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

TARGET_HOLDERS = 4
BORROWER_COUNTS = [0, 1, 2, 3, 4]

PROFILE_KEYS = [
    "profiling_enabled",
    "candidate_reports_received",
    "candidate_reports_accepted",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "owner_task_spec_copy_count",
    "owner_task_spec_copy_time_ns",
    "first_holder_piggyback_copies_sent",
    "first_holder_piggyback_bytes_sent",
    "first_holder_piggyback_serialize_time_ns",
    "holder_install_rpc_time_ns",
    "holder_commit_rpc_time_ns",
    "witness_update_rpc_time_ns",
    "witness_publish_count",
    "witness_publish_time_ns",
    "witness_publish_max_time_ns",
    "holder_admissions_committed",
    "holder_admission_time_ns",
    "holder_admission_max_time_ns",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
]

ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
]


@dataclass(frozen=True)
class Payload:
    name: str
    size_bytes: int


@dataclass
class Pending:
    submitted_ns: int
    next_consumer: int
    tagged: bool


def methods() -> list[Method]:
    return [disabled(), succession(TARGET_HOLDERS)]


def parse_payload(text: str) -> Payload:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("payload must be NAME:BYTES") from exc
    if not name or size <= 0:
        raise argparse.ArgumentTypeError(
            "payload must have non-empty NAME and positive BYTES"
        )
    return Payload(name, size)


def start_cluster(
    method: Method,
    cpus_per_node: int,
    witness_count: int,
) -> tuple[Cluster, list[str]]:
    """Always create the same six logical Ray nodes."""
    cluster = Cluster()

    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=witness_count,
            profiling_enabled=method.recovery_enabled,
        ),
        include_dashboard=False,
    )

    workers = [
        cluster.add_node(
            num_cpus=cpus_per_node,
            resources={"producer_node": 1},
        )
    ]

    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(
                num_cpus=cpus_per_node,
                resources={f"consumer_{i}": 1},
            )
        )

    return cluster, [node.node_id for node in workers]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int) -> bytes:
        prefix = request_id.to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch_and_export(self, wrapped_ref):
            # Receive the original ObjectRef rather than a dereferenced value.
            ref = wrapped_ref[0]
            value = ray.get(ref)
            if not value:
                raise RuntimeError("empty payload")
            return [ref]

        def ping(self) -> int:
            import os
            return os.getpid()

    return produce, Consumer


def run_workload(
    *,
    produce: Any,
    consumers: list[Any],
    borrower_count: int,
    producer_strategy: Any,
    payload_bytes: int,
    warmup_s: float,
    duration_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, Pending] = {}

    request_id = 0
    tagged_pending = 0
    tagged_submitted = 0
    completed_in_window = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    warmup_end_ns = start_ns + int(warmup_s * 1e9)
    measure_end_ns = warmup_end_ns + int(duration_s * 1e9)

    def submit_one() -> None:
        nonlocal request_id, tagged_pending, tagged_submitted

        submitted_ns = time.perf_counter_ns()
        tagged = warmup_end_ns <= submitted_ns < measure_end_ns

        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes)

        if borrower_count == 0:
            stage_ref = payload_ref
            next_consumer = 0
        else:
            stage_ref = consumers[0].touch_and_export.remote([payload_ref])
            next_consumer = 1

        pending[stage_ref] = Pending(
            submitted_ns=submitted_ns,
            next_consumer=next_consumer,
            tagged=tagged,
        )

        request_id += 1
        if tagged:
            tagged_pending += 1
            tagged_submitted += 1

    def process_one(allow_resubmit: bool) -> bool:
        nonlocal tagged_pending, completed_in_window

        if not pending:
            return False

        ready, _ = ray.wait(
            list(pending),
            num_returns=1,
            timeout=wait_timeout_s,
        )
        if not ready:
            return False

        stage_ref = ready[0]
        result = ray.get(stage_ref)
        state = pending.pop(stage_ref)

        if borrower_count > 0:
            forwarded_ref = result[0]

            if state.next_consumer < borrower_count:
                nxt = consumers[state.next_consumer].touch_and_export.remote(
                    [forwarded_ref]
                )
                pending[nxt] = Pending(
                    submitted_ns=state.submitted_ns,
                    next_consumer=state.next_consumer + 1,
                    tagged=state.tagged,
                )
                return True

        completed_ns = time.perf_counter_ns()

        if warmup_end_ns <= completed_ns < measure_end_ns:
            completed_in_window += 1

        if state.tagged:
            latencies_ms.append(
                (completed_ns - state.submitted_ns) / 1e6
            )
            tagged_pending -= 1

        if allow_resubmit:
            now_ns = time.perf_counter_ns()
            if now_ns < measure_end_ns or tagged_pending > 0:
                submit_one()

        return True

    for _ in range(inflight):
        submit_one()

    while True:
        if time.perf_counter_ns() >= measure_end_ns and tagged_pending == 0:
            break
        process_one(True)

    deadline = time.monotonic() + drain_timeout_s
    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"drain timeout with {len(pending)} pending stage calls"
            )
        process_one(False)

    return {
        "completed_in_window": completed_in_window,
        "latency_sample_count": len(latencies_ms),
        "latency_tagged_submitted": tagged_submitted,
        "throughput_rps": completed_in_window / duration_s,
        "logical_payload_throughput_mib_s": (
            completed_in_window
            * payload_bytes
            / duration_s
            / (1024.0 * 1024.0)
        ),
        "latency_mean_ms": (
            statistics.fmean(latencies_ms) if latencies_ms else math.nan
        ),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
        "total_pipeline_submitted": request_id,
    }


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    profile: dict[str, Any] = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }
    if raw is not None:
        for key in PROFILE_KEYS:
            if key in raw:
                profile[key] = raw[key]
    return profile


def outstanding_async(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[completed]))
        for sent, completed in ASYNC_PAIRS
    )


def wait_for_profile_quiescence(
    timeout_s: float,
    stable_s: float,
) -> tuple[dict[str, Any], bool]:
    """Wait after measurement until asynchronous callbacks stop changing."""
    deadline = time.monotonic() + timeout_s
    last_signature: tuple[Any, ...] | None = None
    stable_since: float | None = None
    last_profile = profile_defaults()

    while time.monotonic() < deadline:
        last_profile = profile_defaults(
            global_worker.core_worker.get_recovery_succession_profile()
        )

        signature = tuple(last_profile[key] for key in PROFILE_KEYS)
        now = time.monotonic()

        if outstanding_async(last_profile) == 0:
            if signature == last_signature:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= stable_s:
                    return last_profile, True
            else:
                stable_since = now
        else:
            stable_since = None

        last_signature = signature
        time.sleep(0.05)

    return last_profile, False


def avg_us(total_ns: Any, count: Any) -> float:
    count = int(count)
    if count <= 0:
        return math.nan
    return float(total_ns) / count / 1e3


def add_profile_metrics(
    summary: dict[str, Any],
    method: Method,
    borrower_count: int,
    profile: dict[str, Any],
    quiescent: bool,
) -> None:
    for key in PROFILE_KEYS:
        summary[f"profile_{key}"] = profile[key]

    summary["profile_quiescent"] = int(quiescent)
    summary["profile_async_outstanding"] = outstanding_async(profile)
    summary["profile_async_balanced"] = int(
        all(
            int(profile[sent]) == int(profile[completed])
            for sent, completed in ASYNC_PAIRS
        )
    )

    summary["profile_owner_task_spec_copy_avg_us"] = avg_us(
        profile["owner_task_spec_copy_time_ns"],
        profile["owner_task_spec_copy_count"],
    )
    summary["profile_first_holder_piggyback_serialize_avg_us"] = avg_us(
        profile["first_holder_piggyback_serialize_time_ns"],
        profile["first_holder_piggyback_copies_sent"],
    )
    summary["profile_holder_install_rpc_avg_us"] = avg_us(
        profile["holder_install_rpc_time_ns"],
        profile["holder_install_rpcs_completed"],
    )
    summary["profile_witness_update_rpc_avg_us"] = avg_us(
        profile["witness_update_rpc_time_ns"],
        profile["witness_update_rpcs_completed"],
    )
    summary["profile_witness_publish_avg_us"] = avg_us(
        profile["witness_publish_time_ns"],
        profile["witness_publish_count"],
    )
    summary["profile_holder_commit_rpc_avg_us"] = avg_us(
        profile["holder_commit_rpc_time_ns"],
        profile["holder_commit_rpcs_completed"],
    )
    summary["profile_holder_admission_avg_us"] = avg_us(
        profile["holder_admission_time_ns"],
        profile["holder_admissions_committed"],
    )

    if method.recovery_enabled:
        expected_holders = min(borrower_count, TARGET_HOLDERS)
        expected_frozen = borrower_count >= TARGET_HOLDERS

        summary["profile_validation_applicable"] = 1
        summary["profile_expected_max_non_owner_holders"] = expected_holders
        summary["profile_holder_count_ok"] = int(
            int(profile["max_non_owner_holders"]) == expected_holders
        )
        summary["profile_expected_frozen"] = int(expected_frozen)
        summary["profile_frozen_ok"] = int(
            (int(profile["frozen_commits"]) > 0) == expected_frozen
        )
    else:
        summary["profile_validation_applicable"] = 0
        summary["profile_expected_max_non_owner_holders"] = ""
        summary["profile_holder_count_ok"] = ""
        summary["profile_expected_frozen"] = ""
        summary["profile_frozen_ok"] = ""


    total_tasks = int(summary["total_pipeline_submitted"])

    # Patch 4F changes H1 transport, not lineage replication semantics.
    logical_task_spec_copies = (
        int(profile["holder_install_rpcs_sent"])
        + int(profile["first_holder_piggyback_copies_sent"])
    )
    summary["profile_logical_task_spec_copies_sent"] = logical_task_spec_copies
    summary["profile_logical_task_spec_copies_per_task"] = (
        logical_task_spec_copies / total_tasks
        if total_tasks > 0
        else math.nan
    )

    control_bytes = (
        int(profile["task_spec_bytes_sent"])
        + int(profile["manifest_bytes_sent"])
    )

    summary["profile_control_bytes_total"] = control_bytes

    summary["profile_control_bytes_per_task"] = (
        control_bytes / total_tasks
        if total_tasks > 0
        else math.nan
    )

    summary["profile_holder_admissions_per_task"] = (
        int(profile["holder_admissions_committed"]) / total_tasks
        if total_tasks > 0
        else math.nan
    )

    summary["profile_generations_committed_per_task"] = (
        int(profile["manifest_generations_committed"]) / total_tasks
        if total_tasks > 0
        else math.nan
    )


def run_one(
    args: argparse.Namespace,
    method: Method,
    payload: Payload,
    borrower_count: int,
    repetition: int,
) -> dict[str, Any]:
    cluster = None

    try:
        cluster, node_ids = start_cluster(
            method,
            args.cpus_per_node,
            args.witness_count,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(
            ray,
            TARGET_HOLDERS + 2,
            args.cluster_timeout_seconds,
        )

        produce, Consumer = make_remote_types()

        # Always create all four actors, even when fewer are used.
        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([consumer.ping.remote() for consumer in consumers])

        # Exclude cluster/actor setup from the recovery profile.
        if method.recovery_enabled:
            global_worker.core_worker.reset_recovery_succession_profile()

        summary = run_workload(
            produce=produce,
            consumers=consumers,
            borrower_count=borrower_count,
            producer_strategy=NodeAffinitySchedulingStrategy(
                node_id=node_ids[0],
                soft=False,
            ),
            payload_bytes=payload.size_bytes,
            warmup_s=args.warmup_seconds,
            duration_s=args.duration_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
        )

        if method.recovery_enabled:
            profile, quiescent = wait_for_profile_quiescence(
                args.profile_quiescence_timeout_seconds,
                args.profile_stable_seconds,
            )
        else:
            profile = profile_defaults(
                global_worker.core_worker.get_recovery_succession_profile()
            )
            quiescent = True

        add_profile_metrics(
            summary,
            method,
            borrower_count,
            profile,
            quiescent,
        )

        base = {
            "repetition": repetition,
            "payload_name": payload.name,
            "payload_bytes": payload.size_bytes,
            "target_holders": TARGET_HOLDERS,
            "borrower_count": borrower_count,
        }

        row = add_method_columns({**base, **summary}, method)

        if method.recovery_enabled:
            print(
                "  "
                f"max_holders={profile['max_non_owner_holders']} "
                f"expected={borrower_count} "
                f"frozen_commits={profile['frozen_commits']} "
                f"admissions={profile['holder_admissions_committed']} "
                f"install={profile['holder_install_rpcs_sent']}/"
                f"{profile['holder_install_rpcs_completed']} "
                f"commit={profile['holder_commit_rpcs_sent']}/"
                f"{profile['holder_commit_rpcs_completed']} "
                f"witness={profile['witness_update_rpcs_sent']}/"
                f"{profile['witness_update_rpcs_completed']} "
                f"quiescent={int(quiescent)}"
            )

        return row

    finally:
        safe_shutdown(ray, cluster)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    groups = sorted(
        {
            (
                row["method"],
                row["method_label"],
                int(row["holders"]),
                int(row["borrower_count"]),
                row["payload_name"],
                int(row["payload_bytes"]),
            )
            for row in rows
        }
    )

    for (
        method_key,
        method_label,
        holders,
        borrower_count,
        payload_name,
        payload_bytes,
    ) in groups:
        group = [
            row
            for row in rows
            if (
                row["method"] == method_key
                and int(row["borrower_count"]) == borrower_count
                and row["payload_name"] == payload_name
            )
        ]

        throughput_mean, throughput_ci = mean_ci95(
            float(row["throughput_rps"]) for row in group
        )
        p50_mean, p50_ci = mean_ci95(
            float(row["latency_p50_ms"]) for row in group
        )
        p95_mean, p95_ci = mean_ci95(
            float(row["latency_p95_ms"]) for row in group
        )

        def mean_metric(name: str) -> float:
            vals = [
                float(row[name])
                for row in group
                if not math.isnan(float(row[name]))
            ]
            return statistics.fmean(vals) if vals else math.nan

        applicable = [
            row
            for row in group
            if int(row["profile_validation_applicable"]) == 1
        ]

        out.append(
            {
                "method": method_key,
                "method_label": method_label,
                "holders": holders,
                "target_holders": TARGET_HOLDERS,
                "borrower_count": borrower_count,
                "payload_name": payload_name,
                "payload_bytes": payload_bytes,
                "repetitions": len(group),
                "throughput_mean_rps": throughput_mean,
                "throughput_ci95_rps": throughput_ci,
                "p50_latency_mean_ms": p50_mean,
                "p50_latency_ci95_ms": p50_ci,
                "p95_latency_mean_ms": p95_mean,
                "p95_latency_ci95_ms": p95_ci,
                "profile_max_non_owner_holders_max": max(
                    int(row["profile_max_non_owner_holders"])
                    for row in group
                ),
                "profile_frozen_commits_mean": statistics.fmean(
                    float(row["profile_frozen_commits"])
                    for row in group
                ),
                "profile_owner_task_spec_copy_avg_us_mean": mean_metric(
                    "profile_owner_task_spec_copy_avg_us"
                ),
                "profile_holder_install_rpc_avg_us_mean": mean_metric(
                    "profile_holder_install_rpc_avg_us"
                ),
                "profile_witness_update_rpc_avg_us_mean": mean_metric(
                    "profile_witness_update_rpc_avg_us"
                ),
                "profile_witness_publish_avg_us_mean": mean_metric(
                    "profile_witness_publish_avg_us"
                ),
                "profile_holder_commit_rpc_avg_us_mean": mean_metric(
                    "profile_holder_commit_rpc_avg_us"
                ),
                "profile_holder_admission_avg_us_mean": mean_metric(
                    "profile_holder_admission_avg_us"
                ),
                "profile_task_spec_bytes_sent_mean": statistics.fmean(
                    float(row["profile_task_spec_bytes_sent"])
                    for row in group
                ),
                "profile_manifest_bytes_sent_mean": statistics.fmean(
                    float(row["profile_manifest_bytes_sent"])
                    for row in group
                ),
                "profile_async_balanced_all": min(
                    int(row["profile_async_balanced"])
                    for row in group
                ),
                "profile_quiescent_all": min(
                    int(row["profile_quiescent"])
                    for row in group
                ),
                "profile_holder_count_ok_all": (
                    min(int(row["profile_holder_count_ok"]) for row in applicable)
                    if applicable
                    else ""
                ),
                "profile_frozen_ok_all": (
                    min(int(row["profile_frozen_ok"]) for row in applicable)
                    if applicable
                    else ""
                ),

                "profile_control_bytes_per_task_mean": mean_metric(
                    "profile_control_bytes_per_task"
                ),
                "profile_holder_admissions_per_task_mean": mean_metric(
                    "profile_holder_admissions_per_task"
                ),
                "profile_generations_committed_per_task_mean": mean_metric(
                    "profile_generations_committed_per_task"
                ),
                "profile_max_generation_max": max(
                    int(row["profile_max_generation"])
                    for row in group
                ),
                "profile_manifest_generations_committed_mean": statistics.fmean(
                    float(row["profile_manifest_generations_committed"])
                    for row in group
                ),
            }
        )

    # Add paired overhead columns to Succession-R4 rows.
    for row in out:
        row["paired_throughput_loss_pct"] = ""
        row["paired_p95_latency_increase_pct"] = ""

        if row["method"] != "succession":
            continue

        matches = [
            baseline
            for baseline in out
            if (
                baseline["method"] == "disabled"
                and baseline["borrower_count"] == row["borrower_count"]
                and baseline["payload_name"] == row["payload_name"]
            )
        ]
        if not matches:
            continue

        baseline = matches[0]
        base_t = float(baseline["throughput_mean_rps"])
        rec_t = float(row["throughput_mean_rps"])
        base_l = float(baseline["p95_latency_mean_ms"])
        rec_l = float(row["p95_latency_mean_ms"])

        if base_t > 0:
            row["paired_throughput_loss_pct"] = (
                100.0 * (base_t - rec_t) / base_t
            )
        if base_l > 0:
            row["paired_p95_latency_increase_pct"] = (
                100.0 * (rec_l - base_l) / base_l
            )

    return out


def run(args: argparse.Namespace) -> None:
    rows: list[dict[str, Any]] = []

    cases = [
        (method, payload, borrower_count)
        for payload in args.payloads
        for borrower_count in BORROWER_COUNTS
        for method in methods()
    ]

    rng = random.Random(args.seed)
    total = args.repetitions * len(cases)
    index = 0

    for repetition in range(1, args.repetitions + 1):
        order = cases[:]
        if not args.fixed_order:
            rng.shuffle(order)

        for method, payload, borrower_count in order:
            index += 1
            print(
                f"[{index}/{total}] "
                f"rep={repetition} "
                f"payload={payload.name} "
                f"borrowers={borrower_count} "
                f"method={method.label}"
            )

            rows.append(
                run_one(
                    args,
                    method,
                    payload,
                    borrower_count,
                    repetition,
                )
            )

    root = Path(args.output_dir)
    write_csv(root / "patch4a_runs.csv", rows)
    write_csv(root / "patch4a_summary.csv", summarize(rows))
    print(f"Wrote results to {root}")


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    root = Path(args.output_dir)
    rows = read_csv(root / "patch4a_summary.csv")
    plot_dir = root / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    payloads = sorted(
        {
            (int(row["payload_bytes"]), row["payload_name"])
            for row in rows
        }
    )

    for payload_bytes, payload_name in payloads:
        subset = [
            row for row in rows if row["payload_name"] == payload_name
        ]
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in payload_name
        )

        for metric, ci_metric, ylabel, filename in [
            (
                "throughput_mean_rps",
                "throughput_ci95_rps",
                "Completed pipelines / s",
                "throughput_vs_borrowers",
            ),
            (
                "p50_latency_mean_ms",
                "p50_latency_ci95_ms",
                "P50 end-to-end latency (ms)",
                "p50_latency_vs_borrowers",
            ),
            (
                "p95_latency_mean_ms",
                "p95_latency_ci95_ms",
                "P95 end-to-end latency (ms)",
                "p95_latency_vs_borrowers",
            ),
        ]:
            plt.figure(figsize=(7.5, 4.8))
            for method_label in ["Disabled", f"Succession-R{TARGET_HOLDERS}"]:
                method_rows = sorted(
                    [
                        row
                        for row in subset
                        if row["method_label"] == method_label
                    ],
                    key=lambda row: int(row["borrower_count"]),
                )
                plt.errorbar(
                    [int(row["borrower_count"]) for row in method_rows],
                    [float(row[metric]) for row in method_rows],
                    yerr=[float(row[ci_metric]) for row in method_rows],
                    marker="o",
                    capsize=3,
                    label=method_label,
                )
            plt.xlabel("Actual downstream borrowers")
            plt.ylabel(ylabel)
            plt.title(f"{payload_name} ({payload_bytes} B)")
            plt.xticks(BORROWER_COUNTS)
            plt.legend()
            plt.tight_layout()
            plt.savefig(
                plot_dir / f"{filename}_{safe_name}.png",
                dpi=200,
            )
            plt.close()

        recovery_rows = sorted(
            [row for row in subset if row["method"] == "succession"],
            key=lambda row: int(row["borrower_count"]),
        )

        # Actual admitted holders.
        plt.figure(figsize=(7.5, 4.8))
        xs = [int(row["borrower_count"]) for row in recovery_rows]
        plt.plot(
            xs,
            [
                int(row["profile_max_non_owner_holders_max"])
                for row in recovery_rows
            ],
            marker="o",
            label="Observed",
        )
        plt.plot(
            BORROWER_COUNTS,
            BORROWER_COUNTS,
            linestyle="--",
            label="Expected",
        )
        plt.xlabel("Actual downstream borrowers")
        plt.ylabel("Max admitted non-owner holders")
        plt.xticks(BORROWER_COUNTS)
        plt.yticks(BORROWER_COUNTS)
        plt.legend()
        plt.tight_layout()
        plt.savefig(
            plot_dir / f"holders_vs_borrowers_{safe_name}.png",
            dpi=200,
        )
        plt.close()

        # Formation timing components.
        plt.figure(figsize=(8.2, 5.0))
        for key, label in [
            (
                "profile_owner_task_spec_copy_avg_us_mean",
                "Owner TaskSpec copy",
            ),
            (
                "profile_holder_install_rpc_avg_us_mean",
                "Install RPC RTT",
            ),
            (
                "profile_witness_publish_avg_us_mean",
                "Witness publish",
            ),
            (
                "profile_holder_commit_rpc_avg_us_mean",
                "Commit RPC RTT",
            ),
            (
                "profile_holder_admission_avg_us_mean",
                "Total holder admission",
            ),
        ]:
            plt.plot(
                xs,
                [float(row[key]) for row in recovery_rows],
                marker="o",
                label=label,
            )
        plt.xlabel("Actual downstream borrowers")
        plt.ylabel("Average time (µs)")
        plt.xticks(BORROWER_COUNTS)
        plt.legend()
        plt.tight_layout()
        plt.savefig(
            plot_dir / f"formation_components_{safe_name}.png",
            dpi=200,
        )
        plt.close()

    print(f"Wrote plots to {plot_dir}")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot"],
        nargs="?",
        default="run-and-plot",
    )
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/09_patch4a_holder_formation",
    )
    p.add_argument("--warmup-seconds", type=float, default=5)
    p.add_argument("--duration-seconds", type=float, default=30)
    p.add_argument("--inflight", type=int, default=64)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument(
        "--payloads",
        type=parse_payload,
        nargs="+",
        default=[Payload("1KiB", 1024)],
    )
    p.add_argument("--cpus-per-node", type=int, default=3)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--wait-timeout-seconds", type=float, default=1)
    p.add_argument("--drain-timeout-seconds", type=float, default=120)

    # Wait happens only after measured throughput/latency has finished.
    p.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=10,
    )
    p.add_argument(
        "--profile-stable-seconds",
        type=float,
        default=0.5,
    )

    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")

    return p


def main() -> None:
    args = parser().parse_args()

    if args.command in {"run", "run-and-plot"}:
        run(args)

    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
