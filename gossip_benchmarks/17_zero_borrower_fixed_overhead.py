#!/usr/bin/env python3
"""Benchmark 17: isolate fixed Recovery Succession overhead at zero borrowers.

This intentionally mirrors Benchmark 09's b0 environment:
- one head node with 0 CPUs
- one producer node
- four consumer nodes and four idle Consumer actors
- no produced ObjectRef is forwarded to a consumer
- same inflight producer workload

Cases:
1. Disabled
2. Succession-R4, profiling OFF
3. Succession-R4, profiling ON

The profiling-on case also reports eager zero-borrower work:
- task argument metadata
- initial manifest construction
- witness selection
- TaskSpec manifest attachment
- owner task registration

No Ray rebuild is required.

Example:
    python gossip_benchmarks/17_zero_borrower_fixed_overhead.py \
        --output-dir gossip_benchmarks/results/17_zero_borrower_fixed_overhead \
        --repetitions 2 \
        --warmup-seconds 5 \
        --duration-seconds 30 \
        --inflight 64 \
        --payload-bytes 1024 \
        --cpus-per-node 3 \
        --witness-count 2
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

# Keep benchmark logging comparable to Benchmark 09.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


TARGET_HOLDERS = 4

PROFILE_STAGES = [
    ("task_argument_metadata", "task_argument_metadata_calls", "task_argument_metadata_time_ns"),
    ("initial_manifest_build", "initial_manifest_build_count", "initial_manifest_build_time_ns"),
    ("witness_selection", "witness_selection_count", "witness_selection_time_ns"),
    (
        "task_spec_manifest_attach",
        "task_spec_manifest_attach_count",
        "task_spec_manifest_attach_time_ns",
    ),
    ("register_owned_task", "register_owned_task_count", "register_owned_task_time_ns"),
]


@dataclass(frozen=True)
class Case:
    key: str
    label: str
    recovery_enabled: bool
    profiling_enabled: bool


CASES = [
    Case("disabled", "Disabled", False, False),
    Case("succession_no_profile", "Succession-R4 / profiling OFF", True, False),
    Case("succession_profile", "Succession-R4 / profiling ON", True, True),
]


def percentile(values: list[float], q: float) -> float:
    vals = sorted(values)
    if not vals:
        return math.nan
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def mean_ci95(values: list[float]) -> tuple[float, float]:
    vals = [float(v) for v in values if not math.isnan(float(v))]
    if not vals:
        return math.nan, math.nan
    mean = statistics.fmean(vals)
    if len(vals) == 1:
        return mean, 0.0
    # Student-t 95% for df=1..10; enough for this diagnostic.
    t95 = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
    }
    df = len(vals) - 1
    tcrit = t95.get(df, 1.96)
    return mean, tcrit * statistics.stdev(vals) / math.sqrt(len(vals))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def safe_shutdown(cluster: Cluster | None) -> None:
    try:
        ray.shutdown()
    except Exception:
        pass
    if cluster is not None:
        try:
            cluster.shutdown()
        except Exception:
            pass


def wait_for_cluster(expected_nodes: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive = 0
    while time.monotonic() < deadline:
        alive = sum(1 for node in ray.nodes() if node.get("Alive"))
        if alive >= expected_nodes:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected_nodes} logical Ray nodes became alive")


def system_config(case: Case, witness_count: int) -> dict[str, Any]:
    config: dict[str, Any] = {
        "enable_recovery_succession": case.recovery_enabled,
        "enable_recovery_witness_holder_baseline": False,
        "recovery_succession_witness_count": max(1, int(witness_count)),
        "enable_recovery_succession_profiling": case.profiling_enabled,
    }
    if case.recovery_enabled:
        config["recovery_succession_target_holder_count"] = TARGET_HOLDERS
    return config


def start_cluster(
    case: Case, cpus_per_node: int, witness_count: int
) -> tuple[Cluster, list[str]]:
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(case, witness_count),
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
        def ping(self) -> int:
            import os

            return os.getpid()

    return produce, Consumer


def run_b0_workload(
    *,
    produce: Any,
    producer_strategy: Any,
    payload_bytes: int,
    warmup_s: float,
    duration_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, tuple[int, bool]] = {}

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

        ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes)

        pending[ref] = (submitted_ns, tagged)
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

        ref = ready[0]
        ray.get(ref)
        submitted_ns, tagged = pending.pop(ref)

        completed_ns = time.perf_counter_ns()
        if warmup_end_ns <= completed_ns < measure_end_ns:
            completed_in_window += 1

        if tagged:
            latencies_ms.append((completed_ns - submitted_ns) / 1e6)
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
            raise TimeoutError(f"drain timeout with {len(pending)} pending tasks")
        process_one(False)

    return {
        "throughput_rps": completed_in_window / duration_s,
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
        "latency_sample_count": len(latencies_ms),
        "tagged_submitted": tagged_submitted,
        "total_pipeline_submitted": request_id,
    }


def add_profile_breakdown(row: dict[str, Any], profile: dict[str, Any]) -> None:
    total_tasks = max(1, int(row["total_pipeline_submitted"]))
    row["profile_profiling_enabled"] = int(bool(profile.get("profiling_enabled", False)))

    for prefix, count_key, time_key in PROFILE_STAGES:
        count = int(profile.get(count_key, 0) or 0)
        total_ns = int(profile.get(time_key, 0) or 0)
        row[f"profile_{count_key}"] = count
        row[f"profile_{time_key}"] = total_ns
        row[f"profile_{prefix}_calls_per_task"] = count / total_tasks
        row[f"profile_{prefix}_avg_us"] = (
            total_ns / count / 1e3 if count > 0 else math.nan
        )
        row[f"profile_{prefix}_total_us_per_task"] = total_ns / total_tasks / 1e3

    # Useful raw byte/count fields if present in this build.
    for key in [
        "initial_manifest_bytes",
        "manifest_generations_committed",
        "holder_admissions_committed",
        "max_non_owner_holders",
        "task_spec_bytes_sent",
        "manifest_bytes_sent",
        "witness_update_rpcs_sent",
        "holder_install_rpcs_sent",
    ]:
        row[f"profile_{key}"] = int(profile.get(key, 0) or 0)


def run_one(args: argparse.Namespace, case: Case, repetition: int) -> dict[str, Any]:
    cluster: Cluster | None = None
    try:
        cluster, node_ids = start_cluster(
            case, args.cpus_per_node, args.witness_count
        )
        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        wait_for_cluster(TARGET_HOLDERS + 2, args.cluster_timeout_seconds)

        produce, Consumer = make_remote_types()

        # Match Benchmark 09 b0: construct and start all four otherwise-idle consumers.
        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([consumer.ping.remote() for consumer in consumers])

        if case.recovery_enabled:
            global_worker.core_worker.reset_recovery_succession_profile()

        row: dict[str, Any] = {
            "repetition": repetition,
            "case": case.key,
            "label": case.label,
            "recovery_enabled": int(case.recovery_enabled),
            "profiling_enabled": int(case.profiling_enabled),
            "target_holders": TARGET_HOLDERS,
            "borrower_count": 0,
            "payload_bytes": args.payload_bytes,
        }

        row.update(
            run_b0_workload(
                produce=produce,
                producer_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_ids[0],
                    soft=False,
                ),
                payload_bytes=args.payload_bytes,
                warmup_s=args.warmup_seconds,
                duration_s=args.duration_seconds,
                inflight=args.inflight,
                wait_timeout_s=args.wait_timeout_seconds,
                drain_timeout_s=args.drain_timeout_seconds,
            )
        )

        profile = dict(global_worker.core_worker.get_recovery_succession_profile())
        add_profile_breakdown(row, profile)

        print(
            f"  throughput={row['throughput_rps']:.2f} rps "
            f"p95={row['latency_p95_ms']:.2f} ms "
            f"initial_manifest_calls={row['profile_initial_manifest_build_count']} "
            f"register_owned_calls={row['profile_register_owned_task_count']}"
        )
        return row
    finally:
        safe_shutdown(cluster)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []

    numeric_profile_fields = [
        f"profile_{prefix}_avg_us" for prefix, _, _ in PROFILE_STAGES
    ] + [
        f"profile_{prefix}_total_us_per_task" for prefix, _, _ in PROFILE_STAGES
    ]

    for case in CASES:
        group = [row for row in rows if row["case"] == case.key]
        throughput, throughput_ci = mean_ci95(
            [float(row["throughput_rps"]) for row in group]
        )
        p95, p95_ci = mean_ci95(
            [float(row["latency_p95_ms"]) for row in group]
        )

        summary: dict[str, Any] = {
            "case": case.key,
            "label": case.label,
            "recovery_enabled": int(case.recovery_enabled),
            "profiling_enabled": int(case.profiling_enabled),
            "repetitions": len(group),
            "throughput_mean_rps": throughput,
            "throughput_ci95_rps": throughput_ci,
            "p95_latency_mean_ms": p95,
            "p95_latency_ci95_ms": p95_ci,
        }

        for field in numeric_profile_fields:
            vals = [
                float(row[field])
                for row in group
                if field in row and not math.isnan(float(row[field]))
            ]
            summary[f"{field}_mean"] = statistics.fmean(vals) if vals else math.nan

        # Counts are useful for confirming that b0 still eagerly initializes
        # recovery state despite having no borrowers.
        for _, count_key, _ in PROFILE_STAGES:
            field = f"profile_{count_key}"
            vals = [float(row.get(field, 0)) for row in group]
            summary[f"{field}_mean"] = statistics.fmean(vals) if vals else 0.0

        result.append(summary)

    disabled = next(row for row in result if row["case"] == "disabled")
    no_profile = next(
        row for row in result if row["case"] == "succession_no_profile"
    )
    profiled = next(row for row in result if row["case"] == "succession_profile")

    base = float(disabled["throughput_mean_rps"])
    no_prof = float(no_profile["throughput_mean_rps"])
    prof = float(profiled["throughput_mean_rps"])

    for row in result:
        t = float(row["throughput_mean_rps"])
        row["loss_vs_disabled_pct"] = (
            100.0 * (base - t) / base if base > 0 else math.nan
        )

    # This isolates the incremental cost of turning profiling on while the
    # protocol itself remains enabled.
    profiled["profiling_incremental_loss_vs_no_profile_pct"] = (
        100.0 * (no_prof - prof) / no_prof if no_prof > 0 else math.nan
    )
    disabled["profiling_incremental_loss_vs_no_profile_pct"] = ""
    no_profile["profiling_incremental_loss_vs_no_profile_pct"] = ""

    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/17_zero_borrower_fixed_overhead",
    )
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--warmup-seconds", type=float, default=5)
    parser.add_argument("--duration-seconds", type=float, default=30)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--cpus-per-node", type=int, default=3)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--cluster-timeout-seconds", type=float, default=30)
    parser.add_argument("--wait-timeout-seconds", type=float, default=1)
    parser.add_argument("--drain-timeout-seconds", type=float, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--fixed-order", action="store_true")
    args = parser.parse_args()

    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive")
    if args.inflight <= 0:
        raise ValueError("--inflight must be positive")
    if args.payload_bytes <= 0:
        raise ValueError("--payload-bytes must be positive")

    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []
    total = args.repetitions * len(CASES)
    index = 0

    for repetition in range(1, args.repetitions + 1):
        order = CASES[:]
        if not args.fixed_order:
            rng.shuffle(order)

        for case in order:
            index += 1
            print(
                f"[{index}/{total}] rep={repetition} "
                f"case={case.label} borrowers=0"
            )
            rows.append(run_one(args, case, repetition))

    out_dir = Path(args.output_dir)
    runs_path = out_dir / "zero_borrower_runs.csv"
    summary_path = out_dir / "zero_borrower_summary.csv"
    write_csv(runs_path, rows)
    summary = summarize(rows)
    write_csv(summary_path, summary)

    print("\nSummary:")
    for row in summary:
        extra = ""
        if row["case"] == "succession_profile":
            extra = (
                " profiling_incremental_loss="
                f"{float(row['profiling_incremental_loss_vs_no_profile_pct']):.2f}%"
            )
        print(
            f"  {row['label']}: "
            f"{float(row['throughput_mean_rps']):.2f} rps, "
            f"loss_vs_disabled={float(row['loss_vs_disabled_pct']):.2f}%"
            f"{extra}"
        )

    print(f"\nWrote: {runs_path}")
    print(f"Wrote: {summary_path}")


if __name__ == "__main__":
    main()
