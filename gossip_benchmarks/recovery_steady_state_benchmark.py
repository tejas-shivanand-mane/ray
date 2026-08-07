#!/usr/bin/env python3
"""
Steady-state, no-failure benchmark for Ray recovery succession.

Measures:
  * completed end-to-end pipelines per second
  * payload throughput in MiB/s
  * end-to-end latency: mean, p50, p95, p99

Every request follows the same application path in every configuration:

  driver/owner -> producer task -> consumer 1 -> consumer 2 -> consumer 3

The cluster always has five logical Ray nodes:
  node 0: head + driver; owns producer ObjectRefs
  node 1: producer executor; can become non-owner holder 1
  nodes 2-4: persistent consumer actors; can become holders 2-4

There are no failures in this benchmark.

The warmup phase is fully drained before measurement. During measurement, the
benchmark keeps a fixed number of pipelines in flight. Throughput counts only
requests completed inside the exact measurement window. Latency includes every
request submitted during the measurement window, including requests that finish
during the final drain, avoiding right-censoring of slow requests.
"""

from __future__ import annotations

import argparse
import csv
import gc
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")

import ray
from ray._private.internal_api import global_gc
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


CASES = [
    Case("Disabled", False, 0),
    Case("Enabled-1-holder", True, 1),
    Case("Enabled-2-holders", True, 2),
    Case("Enabled-3-holders", True, 3),
    Case("Enabled-4-holders", True, 4),
]


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


def start_cluster(case: Case, cpus_per_node: int) -> tuple[Cluster, list[str]]:
    cluster = Cluster()
    system_config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": 2,
    }
    if case.enabled:
        system_config["recovery_succession_target_holder_count"] = case.holders

    cluster.add_node(num_cpus=0, _system_config=system_config)
    workers = [cluster.add_node(num_cpus=cpus_per_node) for _ in range(4)]
    return cluster, [node.node_id for node in workers]


def latency_summary(values_ms: list[float]) -> dict[str, float]:
    return {
        "latency_mean_ms": statistics.fmean(values_ms) if values_ms else math.nan,
        "latency_p50_ms": percentile(values_ms, 0.50),
        "latency_p95_ms": percentile(values_ms, 0.95),
        "latency_p99_ms": percentile(values_ms, 0.99),
    }


def run_closed_loop(
    *,
    produce: Any,
    consumers: list[Any],
    producer_strategy: NodeAffinitySchedulingStrategy,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    payload_bytes: int,
    record: bool,
    drain_timeout_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Run a closed-loop workload with fixed concurrency."""
    pending: dict[ray.ObjectRef, tuple[int, int, int, ray.ObjectRef]] = {}
    request_id = 0
    submitted_requests = 0
    completed_in_window = 0
    all_latencies_ms: list[float] = []

    phase_start_ns = time.perf_counter_ns()
    phase_deadline_ns = phase_start_ns + int(duration_s * 1e9)

    bucket_count = max(1, math.ceil(duration_s / bucket_s))
    bucket_counts = [0 for _ in range(bucket_count)]
    bucket_latencies_ms: list[list[float]] = [[] for _ in range(bucket_count)]

    def submit_one() -> None:
        nonlocal request_id, submitted_requests
        started_ns = time.perf_counter_ns()
        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes)
        first_stage_ref = consumers[0].touch_and_export.remote([payload_ref])
        pending[first_stage_ref] = (request_id, 1, started_ns, payload_ref)
        request_id += 1
        submitted_requests += 1

    for _ in range(inflight):
        submit_one()

    while time.perf_counter_ns() < phase_deadline_ns:
        ready, _ = ray.wait(
            list(pending),
            num_returns=min(len(pending), 64),
            timeout=0.1,
        )
        if not ready:
            continue

        exported_refs = ray.get(ready)
        for completed_stage_ref, exported in zip(ready, exported_refs):
            req_id, next_consumer, started_ns, old_payload_ref = pending.pop(
                completed_stage_ref
            )
            del old_payload_ref
            fresh_payload_ref = exported[0]

            if next_consumer < len(consumers):
                next_stage_ref = consumers[next_consumer].touch_and_export.remote(
                    [fresh_payload_ref]
                )
                pending[next_stage_ref] = (
                    req_id,
                    next_consumer + 1,
                    started_ns,
                    fresh_payload_ref,
                )
                continue

            completed_ns = time.perf_counter_ns()
            latency_ms = (completed_ns - started_ns) / 1e6

            if record:
                all_latencies_ms.append(latency_ms)
                if completed_ns <= phase_deadline_ns:
                    completed_in_window += 1
                    elapsed_s = (completed_ns - phase_start_ns) / 1e9
                    bucket_index = min(int(elapsed_s // bucket_s), bucket_count - 1)
                    bucket_counts[bucket_index] += 1
                    bucket_latencies_ms[bucket_index].append(latency_ms)

            if time.perf_counter_ns() < phase_deadline_ns:
                submit_one()

    drain_start_ns = time.perf_counter_ns()
    drain_deadline_ns = drain_start_ns + int(drain_timeout_s * 1e9)

    while pending:
        if time.perf_counter_ns() > drain_deadline_ns:
            raise TimeoutError(
                f"Drain exceeded {drain_timeout_s:.1f}s with "
                f"{len(pending)} stage calls still pending"
            )

        ready, _ = ray.wait(
            list(pending),
            num_returns=min(len(pending), 64),
            timeout=0.5,
        )
        if not ready:
            continue

        exported_refs = ray.get(ready)
        for completed_stage_ref, exported in zip(ready, exported_refs):
            req_id, next_consumer, started_ns, old_payload_ref = pending.pop(
                completed_stage_ref
            )
            del old_payload_ref
            fresh_payload_ref = exported[0]

            if next_consumer < len(consumers):
                next_stage_ref = consumers[next_consumer].touch_and_export.remote(
                    [fresh_payload_ref]
                )
                pending[next_stage_ref] = (
                    req_id,
                    next_consumer + 1,
                    started_ns,
                    fresh_payload_ref,
                )
                continue

            if record:
                completed_ns = time.perf_counter_ns()
                all_latencies_ms.append((completed_ns - started_ns) / 1e6)

    drain_seconds = (time.perf_counter_ns() - drain_start_ns) / 1e9

    if not record:
        return {}, []

    summary: dict[str, Any] = {
        "submitted_requests": submitted_requests,
        "completed_in_window": completed_in_window,
        "throughput_rps": completed_in_window / duration_s,
        "data_throughput_mib_s": (
            completed_in_window * payload_bytes / duration_s / (1024.0 * 1024.0)
        ),
        "latency_sample_count": len(all_latencies_ms),
        "drain_seconds": drain_seconds,
    }
    summary.update(latency_summary(all_latencies_ms))

    timeseries: list[dict[str, Any]] = []
    for index, bucket_values in enumerate(bucket_latencies_ms):
        bucket_start_s = index * bucket_s
        bucket_end_s = min((index + 1) * bucket_s, duration_s)
        actual_bucket_s = bucket_end_s - bucket_start_s
        row: dict[str, Any] = {
            "bucket_index": index,
            "elapsed_start_s": bucket_start_s,
            "elapsed_end_s": bucket_end_s,
            "completed_requests": bucket_counts[index],
            "throughput_rps": (
                bucket_counts[index] / actual_bucket_s
                if actual_bucket_s > 0
                else math.nan
            ),
        }
        row.update(latency_summary(bucket_values))
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
    cooldown_s: float,
    drain_timeout_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cluster, node_ids = start_cluster(case, cpus_per_node)
    run_started = time.perf_counter()

    try:
        ray.init(address=cluster.address, log_to_driver=False)

        producer_node_id = node_ids[0]
        consumer_node_ids = node_ids[1:]

        @ray.remote(max_retries=2)
        def produce(request_id: int, size: int) -> bytes:
            marker = request_id.to_bytes(8, byteorder="little", signed=False)
            if size <= len(marker):
                return marker[:size]
            return marker + (b"x" * (size - len(marker)))

        @ray.remote(max_restarts=0, max_concurrency=1)
        class Consumer:
            def __init__(self, expected_size: int):
                self.expected_size = expected_size
                self.current_ref = None

            def touch_and_export(
                self,
                wrapped_ref: list[ray.ObjectRef],
            ) -> list[ray.ObjectRef]:
                self.current_ref = wrapped_ref[0]
                value = ray.get(self.current_ref)
                if len(value) != self.expected_size:
                    raise RuntimeError(
                        f"Expected {self.expected_size} bytes, got {len(value)}"
                    )
                return [self.current_ref]

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
            ).remote(payload.size_bytes)
            for node_id in consumer_node_ids
        ]

        run_closed_loop(
            produce=produce,
            consumers=consumers,
            producer_strategy=producer_strategy,
            duration_s=warmup_s,
            bucket_s=bucket_s,
            inflight=inflight,
            payload_bytes=payload.size_bytes,
            record=False,
            drain_timeout_s=drain_timeout_s,
        )

        gc.collect()
        global_gc()
        if cooldown_s > 0:
            time.sleep(cooldown_s)

        summary, timeseries = run_closed_loop(
            produce=produce,
            consumers=consumers,
            producer_strategy=producer_strategy,
            duration_s=duration_s,
            bucket_s=bucket_s,
            inflight=inflight,
            payload_bytes=payload.size_bytes,
            record=True,
            drain_timeout_s=drain_timeout_s,
        )

        common = {
            "repetition": repetition,
            "run_order": run_order,
            "config": case.label,
            "recovery_enabled": int(case.enabled),
            "target_non_owner_holders": case.holders,
            "payload_name": payload.name,
            "payload_bytes": payload.size_bytes,
            "inflight": inflight,
            "warmup_seconds": warmup_s,
            "duration_seconds": duration_s,
            "bucket_seconds": bucket_s,
            "cluster_nodes": 5,
            "physical_host_mode": "single_host_logical_nodes",
        }

        summary.update(common)
        summary["total_wall_seconds"] = time.perf_counter() - run_started
        for row in timeseries:
            row.update(common)
        return summary, timeseries
    finally:
        ray.shutdown()
        cluster.shutdown()


RUN_FIELDS = [
    "repetition", "run_order", "config", "recovery_enabled",
    "target_non_owner_holders", "payload_name", "payload_bytes", "inflight",
    "warmup_seconds", "duration_seconds", "bucket_seconds", "cluster_nodes",
    "physical_host_mode", "submitted_requests", "completed_in_window",
    "throughput_rps", "data_throughput_mib_s", "latency_sample_count",
    "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
    "drain_seconds", "total_wall_seconds",
]

TIMESERIES_FIELDS = [
    "repetition", "run_order", "config", "recovery_enabled",
    "target_non_owner_holders", "payload_name", "payload_bytes", "inflight",
    "warmup_seconds", "duration_seconds", "bucket_seconds", "cluster_nodes",
    "physical_host_mode", "bucket_index", "elapsed_start_s", "elapsed_end_s",
    "completed_requests", "throughput_rps", "latency_mean_ms",
    "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
]


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("recovery_benchmark_results"),
    )
    parser.add_argument("--warmup-seconds", type=float, default=5.0)
    parser.add_argument("--duration-seconds", type=float, default=30.0)
    parser.add_argument("--bucket-seconds", type=float, default=1.0)
    parser.add_argument("--cooldown-seconds", type=float, default=1.0)
    parser.add_argument("--drain-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument("--cpus-per-node", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--seed", type=int, default=20260806)
    parser.add_argument(
        "--payloads",
        type=parse_payload,
        nargs="+",
        default=[Payload("small", 1024), Payload("big", 2 * 1024 * 1024)],
        metavar="NAME:BYTES",
        help=(
            "Payload definitions. Default: small:1024 big:2097152. "
            "Example: --payloads small:1024 medium:65536 big:8388608"
        ),
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
        "--inflight": args.inflight,
        "--cpus-per-node": args.cpus_per_node,
        "--repetitions": args.repetitions,
    }
    for name, value in positive_values.items():
        if value <= 0:
            parser.error(f"{name} must be positive")

    if args.cooldown_seconds < 0:
        parser.error("--cooldown-seconds cannot be negative")

    payload_names = [payload.name for payload in args.payloads]
    if len(payload_names) != len(set(payload_names)):
        parser.error("Payload names must be unique")

    return args


def main() -> None:
    args = parse_args()
    run_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []

    run_csv = args.output_dir / "benchmark_runs.csv"
    timeseries_csv = args.output_dir / "benchmark_timeseries.csv"

    combinations = [(case, payload) for payload in args.payloads for case in CASES]

    run_order = 0
    for repetition in range(args.repetitions):
        ordered = list(combinations)
        if not args.fixed_order:
            random.Random(args.seed + repetition).shuffle(ordered)

        for case, payload in ordered:
            run_order += 1
            print(
                f"[run {run_order}/{args.repetitions * len(combinations)}] "
                f"rep={repetition + 1} payload={payload.name}:{payload.size_bytes} "
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
                cooldown_s=args.cooldown_seconds,
                drain_timeout_s=args.drain_timeout_seconds,
            )

            run_rows.append(summary)
            timeseries_rows.extend(timeseries)
            write_csv(run_csv, run_rows, RUN_FIELDS)
            write_csv(timeseries_csv, timeseries_rows, TIMESERIES_FIELDS)

            print(
                f"  throughput={summary['throughput_rps']:.2f} pipelines/s, "
                f"data={summary['data_throughput_mib_s']:.2f} MiB/s, "
                f"mean={summary['latency_mean_ms']:.2f} ms, "
                f"p95={summary['latency_p95_ms']:.2f} ms",
                flush=True,
            )

    print(f"Wrote {run_csv.resolve()}")
    print(f"Wrote {timeseries_csv.resolve()}")


if __name__ == "__main__":
    main()
