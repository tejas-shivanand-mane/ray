#!/usr/bin/env python3
"""
Publication-oriented no-failure steady-state benchmark for Ray recovery succession.

Compares:
    Disabled
    Enabled-1-holder
    Enabled-2-holders
    Enabled-3-holders
    Enabled-4-holders

for every configured payload size.

IMPORTANT TOPOLOGY
------------------
The current recovery-succession implementation does NOT count the original task
executor as an independent recovery holder. Therefore every request traverses
FOUR independent downstream consumers so R=4 can actually form:

    driver/owner -> producer -> C1 -> C2 -> C3 -> C4
                               H1    H2    H3    H4

All configurations traverse the same producer + four-consumer application path.
Only the recovery configuration changes.

LOCAL CLUSTER TOPOLOGY
----------------------
    node 0: head + driver/owner, 0 application CPUs
    node 1: producer worker node
    node 2: consumer/holder candidate 1
    node 3: consumer/holder candidate 2
    node 4: consumer/holder candidate 3
    node 5: consumer/holder candidate 4

This uses ray.cluster_utils.Cluster, so these are logical Ray nodes on one
physical host. Use this for local controlled comparison. Repeat final paper
numbers on physically separate Ray nodes.

MEASUREMENT DESIGN
------------------
1. Actors/workers are pre-started before the timed workload.
2. A fixed-concurrency closed-loop workload is used.
3. Warmup and measurement are continuous; there is no drain/GC/restart between them.
4. Throughput counts final pipeline completions in the exact measurement window.
5. Latency runs from producer submission until final consumer completion.
6. Requests submitted during measurement are tagged for latency. After the
   measurement window closes, background load continues until all tagged requests
   finish, avoiding right-censoring and low-load tail bias.
7. ray.wait(..., num_returns=1) avoids benchmark-induced completion batching.
8. The cluster is recreated for every case/payload run.

OUTPUTS
-------
<output-dir>/benchmark_runs.csv
<output-dir>/benchmark_timeseries.csv
<output-dir>/benchmark_summary.csv
<output-dir>/plots/<payload>_throughput.png
<output-dir>/plots/<payload>_p95_latency.png

Example debug run:
    python gossip_benchmarks/recovery_steady_state_benchmark.py \
        --output-dir gossip_benchmarks/recovery_benchmark_results \
        --warmup-seconds 5 \
        --duration-seconds 30 \
        --bucket-seconds 5 \
        --inflight 64 \
        --repetitions 3 \
        --payloads small:1024 medium:65536 large:262144 big:2097152

Suggested stronger run:
    python gossip_benchmarks/recovery_steady_state_benchmark.py \
        --output-dir gossip_benchmarks/recovery_benchmark_results_final \
        --warmup-seconds 15 \
        --duration-seconds 60 \
        --bucket-seconds 5 \
        --inflight 64 \
        --repetitions 7 \
        --payloads small:1024 medium:65536 large:262144 big:2097152
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Force performance-mode logging. Do not use setdefault(): recovery debugging
# often leaves RAY_BACKEND_LOG_LEVEL=info in the shell environment.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

import matplotlib.pyplot as plt
import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@dataclass(frozen=True)
class Case:
    label: str
    enabled: bool
    holders: int


@dataclass(frozen=True)
class Payload:
    name: str
    size_bytes: int


@dataclass
class PendingStage:
    request_id: int
    next_consumer_index: int
    submitted_ns: int
    payload_ref: ray.ObjectRef
    latency_tagged: bool


CASES = [
    Case("Disabled", False, 0),
    Case("Enabled-1-holder", True, 1),
    Case("Enabled-2-holders", True, 2),
    Case("Enabled-3-holders", True, 3),
    Case("Enabled-4-holders", True, 4),
]

CONFIG_ORDER = [case.label for case in CASES]


def percentile(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def latency_summary(values_ms: list[float]) -> dict[str, float]:
    return {
        "latency_mean_ms": statistics.fmean(values_ms) if values_ms else math.nan,
        "latency_p50_ms": percentile(values_ms, 0.50),
        "latency_p95_ms": percentile(values_ms, 0.95),
        "latency_p99_ms": percentile(values_ms, 0.99),
    }


# Two-sided Student-t critical values for 95% confidence intervals.
_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
    26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def mean_ci95(values: list[float]) -> tuple[float, float]:
    clean = [float(v) for v in values if not math.isnan(float(v))]
    if not clean:
        return math.nan, math.nan
    mean = statistics.fmean(clean)
    if len(clean) == 1:
        return mean, 0.0
    stdev = statistics.stdev(clean)
    df = len(clean) - 1
    tcrit = _T95.get(df, 1.96)
    return mean, tcrit * stdev / math.sqrt(len(clean))


def parse_payload(value: str) -> Payload:
    try:
        name, size_text = value.split(":", 1)
        size_bytes = int(size_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid payload {value!r}; expected NAME:BYTES, e.g. small:1024"
        ) from exc
    name = name.strip()
    if not name:
        raise argparse.ArgumentTypeError("Payload name cannot be empty")
    if size_bytes <= 0:
        raise argparse.ArgumentTypeError("Payload size must be positive")
    return Payload(name=name, size_bytes=size_bytes)


def human_size(size_bytes: int) -> str:
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):g} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:g} KiB"
    return f"{size_bytes} B"


def safe_filename(text: str) -> str:
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in text)


def start_cluster(
    *,
    case: Case,
    cpus_per_node: int,
    witness_count: int,
) -> tuple[Cluster, list[str]]:
    """Create head/owner + producer + four consumer logical nodes."""
    cluster = Cluster()

    system_config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": witness_count,
    }
    if case.enabled:
        system_config["recovery_succession_target_holder_count"] = case.holders

    # Driver/owner node. No application tasks execute here.
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config,
        include_dashboard=False,
    )

    # 1 producer + 4 independent consumer/holder-candidate nodes.
    workers = [
        cluster.add_node(
            num_cpus=cpus_per_node,
            include_dashboard=False,
        )
        for _ in range(5)
    ]
    node_ids = [node.node_id for node in workers]
    if len(set(node_ids)) != 5:
        raise RuntimeError("Expected five distinct worker node IDs")
    return cluster, node_ids


def run_continuous_closed_loop(
    *,
    produce: Any,
    consumers: list[Any],
    producer_strategy: NodeAffinitySchedulingStrategy,
    warmup_s: float,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    payload_bytes: int,
    producer_task_cpus: float,
    drain_timeout_s: float,
    wait_timeout_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run continuous warmup + measurement at fixed closed-loop concurrency."""
    if len(consumers) != 4:
        raise ValueError("Exactly four consumers are required")

    pending: dict[ray.ObjectRef, PendingStage] = {}
    request_id = 0
    total_submitted = 0
    tagged_submitted = 0
    tagged_pending = 0
    completed_in_window = 0
    all_tagged_latencies_ms: list[float] = []

    workload_start_ns = time.perf_counter_ns()
    warmup_end_ns = workload_start_ns + int(warmup_s * 1e9)
    measurement_end_ns = warmup_end_ns + int(duration_s * 1e9)

    bucket_count = max(1, math.ceil(duration_s / bucket_s))
    bucket_counts = [0 for _ in range(bucket_count)]
    bucket_latencies_ms: list[list[float]] = [[] for _ in range(bucket_count)]

    def submit_one() -> None:
        nonlocal request_id, total_submitted, tagged_submitted, tagged_pending

        submitted_ns = time.perf_counter_ns()
        latency_tagged = warmup_end_ns <= submitted_ns < measurement_end_ns

        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=producer_task_cpus,
        ).remote(request_id, payload_bytes)

        first_stage_ref = consumers[0].touch_and_export.remote([payload_ref])
        pending[first_stage_ref] = PendingStage(
            request_id=request_id,
            next_consumer_index=1,
            submitted_ns=submitted_ns,
            payload_ref=payload_ref,
            latency_tagged=latency_tagged,
        )

        request_id += 1
        total_submitted += 1
        if latency_tagged:
            tagged_submitted += 1
            tagged_pending += 1

    def process_one(*, allow_resubmit: bool) -> bool:
        nonlocal completed_in_window, tagged_pending

        if not pending:
            return False

        ready, _ = ray.wait(
            list(pending.keys()),
            num_returns=1,
            timeout=wait_timeout_s,
        )
        if not ready:
            return False

        completed_stage_ref = ready[0]
        exported = ray.get(completed_stage_ref)
        state = pending.pop(completed_stage_ref)
        del state.payload_ref

        if not exported:
            raise RuntimeError("Consumer returned an empty ObjectRef wrapper")
        fresh_payload_ref = exported[0]

        # Continue through all four consumers.
        if state.next_consumer_index < len(consumers):
            next_stage_ref = consumers[
                state.next_consumer_index
            ].touch_and_export.remote([fresh_payload_ref])
            pending[next_stage_ref] = PendingStage(
                request_id=state.request_id,
                next_consumer_index=state.next_consumer_index + 1,
                submitted_ns=state.submitted_ns,
                payload_ref=fresh_payload_ref,
                latency_tagged=state.latency_tagged,
            )
            return True

        # Final consumer completed: one end-to-end pipeline completed.
        completed_ns = time.perf_counter_ns()

        if warmup_end_ns <= completed_ns < measurement_end_ns:
            completed_in_window += 1
            elapsed_s = (completed_ns - warmup_end_ns) / 1e9
            bucket_index = min(int(elapsed_s // bucket_s), bucket_count - 1)
            bucket_counts[bucket_index] += 1

        if state.latency_tagged:
            latency_ms = (completed_ns - state.submitted_ns) / 1e6
            all_tagged_latencies_ms.append(latency_ms)
            tagged_pending -= 1

            # Diagnostic time-series latency: bucket by completion time when the
            # tagged request completes inside the measurement window.
            if warmup_end_ns <= completed_ns < measurement_end_ns:
                elapsed_s = (completed_ns - warmup_end_ns) / 1e9
                bucket_index = min(int(elapsed_s // bucket_s), bucket_count - 1)
                bucket_latencies_ms[bucket_index].append(latency_ms)

        # Maintain closed-loop offered load. After the measurement window, keep
        # background traffic only until all tagged latency requests finish.
        if allow_resubmit:
            now_ns = time.perf_counter_ns()
            if now_ns < measurement_end_ns or tagged_pending > 0:
                submit_one()

        return True

    # Fill the closed loop before processing warmup traffic.
    for _ in range(inflight):
        submit_one()

    # Continuous warmup + measurement + loaded tagged-tail completion.
    while True:
        now_ns = time.perf_counter_ns()
        if now_ns >= measurement_end_ns and tagged_pending == 0:
            break
        process_one(allow_resubmit=True)

    tagged_tail_end_ns = time.perf_counter_ns()

    # Final unmeasured drain for clean shutdown only.
    drain_start_ns = time.perf_counter_ns()
    drain_deadline_ns = drain_start_ns + int(drain_timeout_s * 1e9)
    while pending:
        if time.perf_counter_ns() > drain_deadline_ns:
            raise TimeoutError(
                f"Final drain exceeded {drain_timeout_s:.1f}s with "
                f"{len(pending)} stage calls pending"
            )
        process_one(allow_resubmit=False)

    final_drain_seconds = (time.perf_counter_ns() - drain_start_ns) / 1e9
    measurement_tail_seconds = max(
        0.0,
        (tagged_tail_end_ns - measurement_end_ns) / 1e9,
    )

    summary: dict[str, Any] = {
        "total_submitted_requests": total_submitted,
        "latency_tagged_submitted": tagged_submitted,
        "latency_sample_count": len(all_tagged_latencies_ms),
        "completed_in_window": completed_in_window,
        "throughput_rps": completed_in_window / duration_s,
        "logical_payload_throughput_mib_s": (
            completed_in_window * payload_bytes / duration_s / (1024.0 * 1024.0)
        ),
        "measurement_tail_seconds": measurement_tail_seconds,
        "final_drain_seconds": final_drain_seconds,
    }
    summary.update(latency_summary(all_tagged_latencies_ms))

    timeseries: list[dict[str, Any]] = []
    for bucket_index in range(bucket_count):
        bucket_start_s = bucket_index * bucket_s
        bucket_end_s = min((bucket_index + 1) * bucket_s, duration_s)
        actual_bucket_s = bucket_end_s - bucket_start_s
        row: dict[str, Any] = {
            "bucket_index": bucket_index,
            "elapsed_start_s": bucket_start_s,
            "elapsed_end_s": bucket_end_s,
            "completed_requests": bucket_counts[bucket_index],
            "throughput_rps": (
                bucket_counts[bucket_index] / actual_bucket_s
                if actual_bucket_s > 0
                else math.nan
            ),
        }
        row.update(latency_summary(bucket_latencies_ms[bucket_index]))
        timeseries.append(row)

    return summary, timeseries


def run_one(
    *,
    case: Case,
    payload: Payload,
    repetition: int,
    run_order: int,
    warmup_s: float,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    cpus_per_node: int,
    producer_task_cpus: float,
    consumer_concurrency: int,
    witness_count: int,
    drain_timeout_s: float,
    wait_timeout_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cluster = None
    run_started = time.perf_counter()

    try:
        cluster, node_ids = start_cluster(
            case=case,
            cpus_per_node=cpus_per_node,
            witness_count=witness_count,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        producer_node_id = node_ids[0]
        consumer_node_ids = node_ids[1:]
        if len(consumer_node_ids) != 4:
            raise RuntimeError("Expected exactly four consumer nodes")

        @ray.remote(max_retries=2)
        def produce(request_id: int, size: int) -> bytes:
            marker = request_id.to_bytes(8, byteorder="little", signed=False)
            if size <= len(marker):
                return marker[:size]
            return marker + (b"x" * (size - len(marker)))

        @ray.remote(max_restarts=0)
        class Consumer:
            def __init__(self, expected_size: int):
                self.expected_size = expected_size

            def ping(self) -> int:
                return os.getpid()

            def touch_and_export(
                self,
                wrapped_ref: list[ray.ObjectRef],
            ) -> list[ray.ObjectRef]:
                if len(wrapped_ref) != 1:
                    raise RuntimeError("Expected exactly one wrapped ObjectRef")
                payload_ref = wrapped_ref[0]
                value = ray.get(payload_ref)
                if len(value) != self.expected_size:
                    raise RuntimeError(
                        f"Expected {self.expected_size} bytes, got {len(value)}"
                    )
                # Re-export the original ObjectRef so the latest recovery
                # metadata/manifest is propagated to the next borrower.
                return [payload_ref]

        producer_strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node_id,
            soft=False,
        )

        consumers = [
            Consumer.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,
                ),
                num_cpus=1,
                max_concurrency=consumer_concurrency,
            ).remote(payload.size_bytes)
            for node_id in consumer_node_ids
        ]

        # Force actor startup before preflight/warmup.
        ray.get([consumer.ping.remote() for consumer in consumers])

        # Unmeasured end-to-end preflight request: loads producer code, verifies
        # the payload path, and exercises metadata propagation before warmup.
        preflight_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=producer_task_cpus,
        ).remote((1 << 31) + run_order, payload.size_bytes)

        fresh_ref = preflight_ref
        for consumer in consumers:
            wrapped = ray.get(consumer.touch_and_export.remote([fresh_ref]))
            fresh_ref = wrapped[0]
        ray.get(fresh_ref)

        summary, timeseries = run_continuous_closed_loop(
            produce=produce,
            consumers=consumers,
            producer_strategy=producer_strategy,
            warmup_s=warmup_s,
            duration_s=duration_s,
            bucket_s=bucket_s,
            inflight=inflight,
            payload_bytes=payload.size_bytes,
            producer_task_cpus=producer_task_cpus,
            drain_timeout_s=drain_timeout_s,
            wait_timeout_s=wait_timeout_s,
        )

        common = {
            "repetition": repetition,
            "run_order": run_order,
            "config": case.label,
            "recovery_enabled": int(case.enabled),
            "target_non_owner_holders": case.holders,
            "holder_candidate_workers": 4,
            "payload_name": payload.name,
            "payload_bytes": payload.size_bytes,
            "inflight": inflight,
            "warmup_seconds": warmup_s,
            "duration_seconds": duration_s,
            "bucket_seconds": bucket_s,
            "cluster_nodes": 6,
            "worker_nodes": 5,
            "consumer_stages": 4,
            "cpus_per_node": cpus_per_node,
            "producer_task_cpus": producer_task_cpus,
            "consumer_concurrency": consumer_concurrency,
            "witness_count": witness_count,
            "physical_host_mode": "single_host_logical_nodes",
        }

        summary.update(common)
        summary["total_wall_seconds"] = time.perf_counter() - run_started
        for row in timeseries:
            row.update(common)
        return summary, timeseries

    finally:
        ray.shutdown()
        if cluster is not None:
            cluster.shutdown()


RUN_FIELDS = [
    "repetition", "run_order", "config", "recovery_enabled",
    "target_non_owner_holders", "holder_candidate_workers",
    "payload_name", "payload_bytes", "inflight", "warmup_seconds",
    "duration_seconds", "bucket_seconds", "cluster_nodes", "worker_nodes",
    "consumer_stages", "cpus_per_node", "producer_task_cpus",
    "consumer_concurrency", "witness_count", "physical_host_mode",
    "total_submitted_requests", "latency_tagged_submitted",
    "latency_sample_count", "completed_in_window", "throughput_rps",
    "logical_payload_throughput_mib_s", "latency_mean_ms",
    "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
    "measurement_tail_seconds", "final_drain_seconds", "total_wall_seconds",
]

TIMESERIES_FIELDS = [
    "repetition", "run_order", "config", "recovery_enabled",
    "target_non_owner_holders", "holder_candidate_workers",
    "payload_name", "payload_bytes", "inflight", "warmup_seconds",
    "duration_seconds", "bucket_seconds", "cluster_nodes", "worker_nodes",
    "consumer_stages", "cpus_per_node", "producer_task_cpus",
    "consumer_concurrency", "witness_count", "physical_host_mode",
    "bucket_index", "elapsed_start_s", "elapsed_end_s", "completed_requests",
    "throughput_rps", "latency_mean_ms", "latency_p50_ms",
    "latency_p95_ms", "latency_p99_ms",
]

SUMMARY_FIELDS = [
    "payload_name", "payload_bytes", "config", "target_non_owner_holders",
    "repetitions", "throughput_rps_mean", "throughput_rps_ci95",
    "latency_p95_ms_mean", "latency_p95_ms_ci95", "latency_mean_ms_mean",
    "latency_mean_ms_ci95", "normalized_throughput_mean",
    "normalized_throughput_ci95", "normalized_p95_latency_mean",
    "normalized_p95_latency_ci95",
]


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_summary(
    run_rows: list[dict[str, Any]],
    payloads: list[Payload],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_rep: dict[tuple[str, int, str], dict[str, Any]] = {}

    for row in run_rows:
        grouped.setdefault((row["payload_name"], row["config"]), []).append(row)
        by_rep[(row["payload_name"], int(row["repetition"]), row["config"])] = row

    output: list[dict[str, Any]] = []

    for payload in payloads:
        for case in CASES:
            rows = grouped.get((payload.name, case.label), [])
            if not rows:
                continue

            throughput_mean, throughput_ci = mean_ci95(
                [float(row["throughput_rps"]) for row in rows]
            )
            p95_mean, p95_ci = mean_ci95(
                [float(row["latency_p95_ms"]) for row in rows]
            )
            latency_mean, latency_mean_ci = mean_ci95(
                [float(row["latency_mean_ms"]) for row in rows]
            )

            normalized_throughput: list[float] = []
            normalized_p95: list[float] = []

            for repetition in sorted({int(row["repetition"]) for row in rows}):
                current = by_rep.get((payload.name, repetition, case.label))
                disabled = by_rep.get((payload.name, repetition, "Disabled"))
                if current is None or disabled is None:
                    continue

                disabled_tp = float(disabled["throughput_rps"])
                disabled_p95 = float(disabled["latency_p95_ms"])
                current_tp = float(current["throughput_rps"])
                current_p95 = float(current["latency_p95_ms"])

                if disabled_tp > 0:
                    normalized_throughput.append(current_tp / disabled_tp)
                if disabled_p95 > 0:
                    normalized_p95.append(current_p95 / disabled_p95)

            norm_tp_mean, norm_tp_ci = mean_ci95(normalized_throughput)
            norm_p95_mean, norm_p95_ci = mean_ci95(normalized_p95)

            output.append({
                "payload_name": payload.name,
                "payload_bytes": payload.size_bytes,
                "config": case.label,
                "target_non_owner_holders": case.holders,
                "repetitions": len(rows),
                "throughput_rps_mean": throughput_mean,
                "throughput_rps_ci95": throughput_ci,
                "latency_p95_ms_mean": p95_mean,
                "latency_p95_ms_ci95": p95_ci,
                "latency_mean_ms_mean": latency_mean,
                "latency_mean_ms_ci95": latency_mean_ci,
                "normalized_throughput_mean": norm_tp_mean,
                "normalized_throughput_ci95": norm_tp_ci,
                "normalized_p95_latency_mean": norm_p95_mean,
                "normalized_p95_latency_ci95": norm_p95_ci,
            })

    return output


def plot_payload_comparison(
    *,
    summary_rows: list[dict[str, Any]],
    payload: Payload,
    plots_dir: Path,
) -> tuple[Path, Path]:
    rows_by_config = {
        row["config"]: row
        for row in summary_rows
        if row["payload_name"] == payload.name
    }

    ordered_rows = [rows_by_config[case.label] for case in CASES]
    x = [case.holders for case in CASES]
    xticklabels = ["Disabled", "1", "2", "3", "4"]

    plots_dir.mkdir(parents=True, exist_ok=True)
    stem = safe_filename(payload.name)

    throughput_path = plots_dir / f"{stem}_throughput.png"
    throughput_mean = [float(row["throughput_rps_mean"]) for row in ordered_rows]
    throughput_ci = [float(row["throughput_rps_ci95"]) for row in ordered_rows]

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.errorbar(
        x,
        throughput_mean,
        yerr=throughput_ci,
        marker="o",
        linewidth=1.8,
        capsize=4,
    )
    ax.set_xticks(x, xticklabels)
    ax.set_xlabel("Recovery configuration (non-owner holders)")
    ax.set_ylabel("Completed pipelines/s")
    ax.set_title(
        f"No-failure throughput — {payload.name} ({human_size(payload.size_bytes)})"
    )
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(throughput_path, dpi=250, bbox_inches="tight")
    plt.close(fig)

    latency_path = plots_dir / f"{stem}_p95_latency.png"
    latency_mean = [float(row["latency_p95_ms_mean"]) for row in ordered_rows]
    latency_ci = [float(row["latency_p95_ms_ci95"]) for row in ordered_rows]

    fig, ax = plt.subplots(figsize=(8.2, 5.2))
    ax.errorbar(
        x,
        latency_mean,
        yerr=latency_ci,
        marker="o",
        linewidth=1.8,
        capsize=4,
    )
    ax.set_xticks(x, xticklabels)
    ax.set_xlabel("Recovery configuration (non-owner holders)")
    ax.set_ylabel("P95 end-to-end latency (ms)")
    ax.set_title(
        f"No-failure P95 latency — {payload.name} ({human_size(payload.size_bytes)})"
    )
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(latency_path, dpi=250, bbox_inches="tight")
    plt.close(fig)

    return throughput_path, latency_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("recovery_benchmark_results"),
    )
    parser.add_argument("--warmup-seconds", type=float, default=10.0)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--bucket-seconds", type=float, default=5.0)
    parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    parser.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--wait-timeout-seconds", type=float, default=0.05)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument("--cpus-per-node", type=int, default=4)
    parser.add_argument("--producer-task-cpus", type=float, default=1.0)
    parser.add_argument("--consumer-concurrency", type=int, default=64)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=20260810)
    parser.add_argument(
        "--payloads",
        type=parse_payload,
        nargs="+",
        default=[
            Payload("small", 1024),
            Payload("medium", 64 * 1024),
            Payload("large", 256 * 1024),
            Payload("big", 2 * 1024 * 1024),
        ],
        metavar="NAME:BYTES",
    )
    parser.add_argument(
        "--fixed-order",
        action="store_true",
        help="Do not randomize case/payload order within each repetition.",
    )

    args = parser.parse_args()

    positive_values = {
        "--warmup-seconds": args.warmup_seconds,
        "--duration-seconds": args.duration_seconds,
        "--bucket-seconds": args.bucket_seconds,
        "--drain-timeout-seconds": args.drain_timeout_seconds,
        "--wait-timeout-seconds": args.wait_timeout_seconds,
        "--inflight": args.inflight,
        "--cpus-per-node": args.cpus_per_node,
        "--producer-task-cpus": args.producer_task_cpus,
        "--consumer-concurrency": args.consumer_concurrency,
        "--witness-count": args.witness_count,
        "--repetitions": args.repetitions,
    }
    for name, value in positive_values.items():
        if value <= 0:
            parser.error(f"{name} must be positive")

    if args.cooldown_seconds < 0:
        parser.error("--cooldown-seconds cannot be negative")

    if args.producer_task_cpus > args.cpus_per_node:
        parser.error("--producer-task-cpus cannot exceed --cpus-per-node")

    payload_names = [payload.name for payload in args.payloads]
    if len(payload_names) != len(set(payload_names)):
        parser.error("Payload names must be unique")

    return args


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    run_csv = args.output_dir / "benchmark_runs.csv"
    timeseries_csv = args.output_dir / "benchmark_timeseries.csv"
    summary_csv = args.output_dir / "benchmark_summary.csv"
    plots_dir = args.output_dir / "plots"

    run_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []

    combinations = [
        (case, payload)
        for payload in args.payloads
        for case in CASES
    ]

    total_runs = args.repetitions * len(combinations)
    run_order = 0

    for repetition in range(args.repetitions):
        ordered = list(combinations)
        if not args.fixed_order:
            random.Random(args.seed + repetition).shuffle(ordered)

        for case, payload in ordered:
            run_order += 1
            print(
                "\n"
                f"[run {run_order}/{total_runs}] "
                f"rep={repetition + 1}/{args.repetitions} "
                f"payload={payload.name}:{payload.size_bytes} "
                f"case={case.label}",
                flush=True,
            )

            summary, timeseries = run_one(
                case=case,
                payload=payload,
                repetition=repetition,
                run_order=run_order,
                warmup_s=args.warmup_seconds,
                duration_s=args.duration_seconds,
                bucket_s=args.bucket_seconds,
                inflight=args.inflight,
                cpus_per_node=args.cpus_per_node,
                producer_task_cpus=args.producer_task_cpus,
                consumer_concurrency=args.consumer_concurrency,
                witness_count=args.witness_count,
                drain_timeout_s=args.drain_timeout_seconds,
                wait_timeout_s=args.wait_timeout_seconds,
            )

            run_rows.append(summary)
            timeseries_rows.extend(timeseries)

            # Persist after every independent run.
            write_csv(run_csv, run_rows, RUN_FIELDS)
            write_csv(timeseries_csv, timeseries_rows, TIMESERIES_FIELDS)

            print(
                "  "
                f"throughput={summary['throughput_rps']:.2f} pipelines/s, "
                f"p95={summary['latency_p95_ms']:.2f} ms, "
                f"mean={summary['latency_mean_ms']:.2f} ms, "
                f"latency_samples={summary['latency_sample_count']}",
                flush=True,
            )

            if args.cooldown_seconds > 0:
                time.sleep(args.cooldown_seconds)

    summary_rows = build_summary(run_rows, args.payloads)
    write_csv(summary_csv, summary_rows, SUMMARY_FIELDS)

    print("\nGenerating per-payload plots...", flush=True)
    for payload in args.payloads:
        throughput_path, latency_path = plot_payload_comparison(
            summary_rows=summary_rows,
            payload=payload,
            plots_dir=plots_dir,
        )
        print(f"  {throughput_path.resolve()}", flush=True)
        print(f"  {latency_path.resolve()}", flush=True)

    print("\nWrote:", flush=True)
    print(f"  {run_csv.resolve()}", flush=True)
    print(f"  {timeseries_csv.resolve()}", flush=True)
    print(f"  {summary_csv.resolve()}", flush=True)


if __name__ == "__main__":
    main()
