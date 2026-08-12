#!/usr/bin/env python3
"""Patch 4A control: measure overhead added by profiling itself.

Fixed recovery configuration:
    Succession-R4

Cases:
    borrowers = 0, 4
    profiling = OFF, ON

The application topology, payload, inflight concurrency, warmup, and measurement
window are identical between profiling OFF and ON. This isolates the overhead of
Patch 4A instrumentation from the underlying recovery-succession overhead.

Outputs:
    profiling_control_runs.csv
    profiling_control_summary.csv
    plots/throughput_profiling_control_*.png
    plots/p95_latency_profiling_control_*.png
"""
from __future__ import annotations

import os

# Must be set before importing ray.
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
    add_method_columns,
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
BORROWER_COUNTS = [0, 4]

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
    cpus_per_node: int,
    witness_count: int,
    profiling_enabled: bool,
) -> tuple[Cluster, list[str]]:
    """Create the same six logical Ray nodes for every case."""
    method = succession(TARGET_HOLDERS)
    cluster = Cluster()

    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=witness_count,
            profiling_enabled=profiling_enabled,
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
                next_ref = consumers[
                    state.next_consumer
                ].touch_and_export.remote([forwarded_ref])

                pending[next_ref] = Pending(
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
            statistics.fmean(latencies_ms)
            if latencies_ms
            else math.nan
        ),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }

    if raw:
        for key in PROFILE_KEYS:
            if key in raw:
                result[key] = raw[key]

    return result


def outstanding_async(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[completed]))
        for sent, completed in ASYNC_PAIRS
    )


def wait_for_profile_quiescence(
    timeout_s: float,
    stable_s: float,
) -> tuple[dict[str, Any], bool]:
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


def add_profile_snapshot(
    summary: dict[str, Any],
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


def run_one(
    args: argparse.Namespace,
    payload: Payload,
    borrower_count: int,
    profiling_enabled: bool,
    repetition: int,
) -> dict[str, Any]:
    cluster = None

    try:
        cluster, node_ids = start_cluster(
            args.cpus_per_node,
            args.witness_count,
            profiling_enabled,
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

        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]

        ray.get([consumer.ping.remote() for consumer in consumers])

        # Exclude cluster/actor setup from counters when profiling is enabled.
        if profiling_enabled:
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

        if profiling_enabled:
            profile, quiescent = wait_for_profile_quiescence(
                args.profile_quiescence_timeout_seconds,
                args.profile_stable_seconds,
            )
        else:
            # This should return {"profiling_enabled": false}; fill the rest
            # with zeroes so the CSV schema is identical.
            profile = profile_defaults(
                global_worker.core_worker.get_recovery_succession_profile()
            )
            quiescent = True

        add_profile_snapshot(summary, profile, quiescent)

        method = succession(TARGET_HOLDERS)

        row = add_method_columns(
            {
                "repetition": repetition,
                "payload_name": payload.name,
                "payload_bytes": payload.size_bytes,
                "target_holders": TARGET_HOLDERS,
                "borrower_count": borrower_count,
                "profiling_case": "ON" if profiling_enabled else "OFF",
                "profiling_requested": int(profiling_enabled),
                **summary,
            },
            method,
        )

        if profiling_enabled:
            print(
                "  "
                f"holders={profile['max_non_owner_holders']} "
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
                int(row["borrower_count"]),
                row["profiling_case"],
                row["payload_name"],
                int(row["payload_bytes"]),
            )
            for row in rows
        }
    )

    for borrower_count, profiling_case, payload_name, payload_bytes in groups:
        group = [
            row
            for row in rows
            if (
                int(row["borrower_count"]) == borrower_count
                and row["profiling_case"] == profiling_case
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

        out.append(
            {
                "borrower_count": borrower_count,
                "profiling_case": profiling_case,
                "profiling_requested": int(profiling_case == "ON"),
                "payload_name": payload_name,
                "payload_bytes": payload_bytes,
                "repetitions": len(group),
                "throughput_mean_rps": throughput_mean,
                "throughput_ci95_rps": throughput_ci,
                "p50_latency_mean_ms": p50_mean,
                "p50_latency_ci95_ms": p50_ci,
                "p95_latency_mean_ms": p95_mean,
                "p95_latency_ci95_ms": p95_ci,
                "profile_quiescent_all": min(
                    int(row["profile_quiescent"]) for row in group
                ),
                "profile_async_balanced_all": min(
                    int(row["profile_async_balanced"]) for row in group
                ),
            }
        )

    # Add direct ON-vs-OFF overhead columns to the ON rows.
    for row in out:
        row["profiling_throughput_loss_pct"] = ""
        row["profiling_p50_latency_increase_pct"] = ""
        row["profiling_p95_latency_increase_pct"] = ""

        if row["profiling_case"] != "ON":
            continue

        matches = [
            off
            for off in out
            if (
                off["profiling_case"] == "OFF"
                and off["borrower_count"] == row["borrower_count"]
                and off["payload_name"] == row["payload_name"]
            )
        ]

        if not matches:
            continue

        off = matches[0]

        off_t = float(off["throughput_mean_rps"])
        on_t = float(row["throughput_mean_rps"])

        off_p50 = float(off["p50_latency_mean_ms"])
        on_p50 = float(row["p50_latency_mean_ms"])

        off_p95 = float(off["p95_latency_mean_ms"])
        on_p95 = float(row["p95_latency_mean_ms"])

        if off_t > 0:
            row["profiling_throughput_loss_pct"] = (
                100.0 * (off_t - on_t) / off_t
            )

        if off_p50 > 0:
            row["profiling_p50_latency_increase_pct"] = (
                100.0 * (on_p50 - off_p50) / off_p50
            )

        if off_p95 > 0:
            row["profiling_p95_latency_increase_pct"] = (
                100.0 * (on_p95 - off_p95) / off_p95
            )

    return out


def run(args: argparse.Namespace) -> None:
    rows: list[dict[str, Any]] = []

    cases = [
        (payload, borrower_count, profiling_enabled)
        for payload in args.payloads
        for borrower_count in BORROWER_COUNTS
        for profiling_enabled in [False, True]
    ]

    rng = random.Random(args.seed)

    total = args.repetitions * len(cases)
    index = 0

    for repetition in range(1, args.repetitions + 1):
        order = cases[:]

        if not args.fixed_order:
            rng.shuffle(order)

        for payload, borrower_count, profiling_enabled in order:
            index += 1
            print(
                f"[{index}/{total}] "
                f"rep={repetition} "
                f"payload={payload.name} "
                f"borrowers={borrower_count} "
                f"profiling={'ON' if profiling_enabled else 'OFF'}"
            )

            rows.append(
                run_one(
                    args,
                    payload,
                    borrower_count,
                    profiling_enabled,
                    repetition,
                )
            )

    root = Path(args.output_dir)
    write_csv(root / "profiling_control_runs.csv", rows)
    write_csv(
        root / "profiling_control_summary.csv",
        summarize(rows),
    )

    print(f"Wrote results to {root}")


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    root = Path(args.output_dir)
    rows = read_csv(root / "profiling_control_summary.csv")

    plot_dir = root / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    payloads = sorted(
        {
            (int(row["payload_bytes"]), row["payload_name"])
            for row in rows
        }
    )

    for payload_bytes, payload_name in payloads:
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in payload_name
        )

        subset = [
            row
            for row in rows
            if row["payload_name"] == payload_name
        ]

        for metric, ci_metric, ylabel, filename in [
            (
                "throughput_mean_rps",
                "throughput_ci95_rps",
                "Completed pipelines / s",
                "throughput_profiling_control",
            ),
            (
                "p95_latency_mean_ms",
                "p95_latency_ci95_ms",
                "P95 end-to-end latency (ms)",
                "p95_latency_profiling_control",
            ),
        ]:
            plt.figure(figsize=(7.4, 4.8))

            for profiling_case in ["OFF", "ON"]:
                case_rows = sorted(
                    [
                        row
                        for row in subset
                        if row["profiling_case"] == profiling_case
                    ],
                    key=lambda row: int(row["borrower_count"]),
                )

                plt.errorbar(
                    [
                        int(row["borrower_count"])
                        for row in case_rows
                    ],
                    [float(row[metric]) for row in case_rows],
                    yerr=[
                        float(row[ci_metric])
                        for row in case_rows
                    ],
                    marker="o",
                    capsize=3,
                    label=f"Profiling {profiling_case}",
                )

            plt.xlabel("Actual downstream borrowers")
            plt.ylabel(ylabel)
            plt.title(f"Succession-R4 — {payload_name} ({payload_bytes} B)")
            plt.xticks(BORROWER_COUNTS)
            plt.legend()
            plt.tight_layout()
            plt.savefig(
                plot_dir / f"{filename}_{safe_name}.png",
                dpi=200,
            )
            plt.close()


def parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot"],
        nargs="?",
        default="run-and-plot",
    )

    parser.add_argument(
        "--output-dir",
        default=(
            "gossip_benchmarks/results/"
            "10_patch4a_profiling_control"
        ),
    )

    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=5.0,
    )

    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--inflight",
        type=int,
        default=64,
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--payloads",
        type=parse_payload,
        nargs="+",
        default=[Payload("1KiB", 1024)],
    )

    parser.add_argument(
        "--cpus-per-node",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--witness-count",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--cluster-timeout-seconds",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--wait-timeout-seconds",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--drain-timeout-seconds",
        type=float,
        default=120.0,
    )

    parser.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=5.0,
    )

    parser.add_argument(
        "--profile-stable-seconds",
        type=float,
        default=0.25,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    parser.add_argument(
        "--fixed-order",
        action="store_true",
    )

    return parser


def main() -> None:
    args = parser().parse_args()

    if args.command in {"run", "run-and-plot"}:
        run(args)

    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
