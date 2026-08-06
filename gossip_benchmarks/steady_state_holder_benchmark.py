#!/usr/bin/env python3
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
    x = sorted(values)
    if len(x) == 1:
        return x[0]
    p = (len(x) - 1) * q
    lo, hi = math.floor(p), math.ceil(p)
    return x[lo] if lo == hi else x[lo] + (x[hi] - x[lo]) * (p - lo)


def start_cluster(case: Case, cpus_per_node: int) -> tuple[Cluster, list[str]]:
    cluster = Cluster()
    config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": 2,
    }
    if case.enabled:
        config["recovery_succession_target_holder_count"] = case.holders

    # Head/driver is the owner. It executes no application tasks.
    cluster.add_node(num_cpus=0, _system_config=config)
    workers = [cluster.add_node(num_cpus=cpus_per_node) for _ in range(4)]
    return cluster, [node.node_id for node in workers]


def run_phase(
    produce: Any,
    consumers: list[Any],
    producer_strategy: NodeAffinitySchedulingStrategy,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    payload_bytes: int,
    record: bool,
) -> list[dict[str, float]]:
    # pending value:
    # (request_id, next_consumer_index, start_time, current_payload_ref)
    pending: dict[ray.ObjectRef, tuple[int, int, float, ray.ObjectRef]] = {}
    request_id = 0
    started = time.perf_counter()
    deadline = started + duration_s
    n_buckets = max(1, math.ceil(duration_s / bucket_s))
    counts = [0] * n_buckets
    latencies: list[list[float]] = [[] for _ in range(n_buckets)]

    def submit() -> None:
        nonlocal request_id
        t0 = time.perf_counter()
        payload_ref = produce.options(
            scheduling_strategy=producer_strategy, num_cpus=1
        ).remote(request_id, payload_bytes)
        call_ref = consumers[0].touch_and_export.remote([payload_ref])
        pending[call_ref] = (request_id, 1, t0, payload_ref)
        request_id += 1

    for _ in range(inflight):
        submit()

    while time.perf_counter() < deadline:
        ready, _ = ray.wait(
            list(pending), num_returns=min(64, len(pending)), timeout=0.1
        )
        if not ready:
            continue
        results = ray.get(ready)

        for call_ref, exported in zip(ready, results):
            req_id, next_index, t0, old_ref = pending.pop(call_ref)
            del old_ref
            fresh_ref = exported[0]

            if next_index < len(consumers):
                next_call = consumers[next_index].touch_and_export.remote([fresh_ref])
                pending[next_call] = (req_id, next_index + 1, t0, fresh_ref)
                continue

            now = time.perf_counter()
            elapsed = now - started
            if record and elapsed < duration_s:
                bucket = min(int(elapsed // bucket_s), n_buckets - 1)
                counts[bucket] += 1
                latencies[bucket].append((now - t0) * 1000.0)

            if time.perf_counter() < deadline:
                submit()

    # Drain without recording so each case shuts down cleanly.
    while pending:
        ready, _ = ray.wait(
            list(pending), num_returns=min(64, len(pending)), timeout=1.0
        )
        if not ready:
            continue
        results = ray.get(ready)
        for call_ref, exported in zip(ready, results):
            req_id, next_index, t0, old_ref = pending.pop(call_ref)
            del old_ref
            fresh_ref = exported[0]
            if next_index < len(consumers):
                next_call = consumers[next_index].touch_and_export.remote([fresh_ref])
                pending[next_call] = (req_id, next_index + 1, t0, fresh_ref)

    if not record:
        return []

    rows = []
    for i, bucket_values in enumerate(latencies):
        rows.append(
            {
                "elapsed_seconds": i * bucket_s,
                "completed_requests": counts[i],
                "throughput_rps": counts[i] / bucket_s,
                "latency_mean_ms": (
                    statistics.fmean(bucket_values) if bucket_values else math.nan
                ),
                "latency_p50_ms": percentile(bucket_values, 0.50),
                "latency_p95_ms": percentile(bucket_values, 0.95),
                "latency_p99_ms": percentile(bucket_values, 0.99),
            }
        )
    return rows


def run_case(
    case: Case,
    repetition: int,
    warmup_s: float,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    payload_bytes: int,
    cpus_per_node: int,
) -> list[dict[str, Any]]:
    cluster, node_ids = start_cluster(case, cpus_per_node)
    try:
        ray.init(address=cluster.address, log_to_driver=False)

        @ray.remote(max_retries=2)
        def produce(request_id: int, size: int) -> bytes:
            marker = request_id.to_bytes(8, "little", signed=False)
            return (marker[:size] if size <= 8 else marker + b"x" * (size - 8))

        @ray.remote(max_restarts=0, max_concurrency=1)
        class Consumer:
            def __init__(self, expected_size: int):
                self.expected_size = expected_size
                self.ref = None

            def touch_and_export(self, wrapped_ref):
                self.ref = wrapped_ref[0]
                value = ray.get(self.ref)
                if len(value) != self.expected_size:
                    raise RuntimeError(
                        f"Expected {self.expected_size} bytes, got {len(value)}"
                    )
                # Re-exporting propagates the newest manifest to the next node.
                return [self.ref]

        producer_strategy = NodeAffinitySchedulingStrategy(
            node_id=node_ids[0], soft=False
        )
        consumers = [
            Consumer.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_id, soft=False
                ),
                num_cpus=1,
            ).remote(payload_bytes)
            for node_id in node_ids[1:]
        ]

        run_phase(
            produce, consumers, producer_strategy, warmup_s, bucket_s,
            inflight, payload_bytes, False
        )
        gc.collect()
        global_gc()
        time.sleep(1.0)

        rows = run_phase(
            produce, consumers, producer_strategy, duration_s, bucket_s,
            inflight, payload_bytes, True
        )
        for row in rows:
            row.update(
                repetition=repetition,
                config=case.label,
                recovery_enabled=int(case.enabled),
                target_non_owner_holders=case.holders,
                cluster_nodes=5,
                payload_bytes=payload_bytes,
                inflight=inflight,
                bucket_seconds=bucket_s,
            )
        return rows
    finally:
        ray.shutdown()
        cluster.shutdown()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "repetition", "config", "recovery_enabled",
        "target_non_owner_holders", "cluster_nodes", "payload_bytes",
        "inflight", "bucket_seconds", "elapsed_seconds",
        "completed_requests", "throughput_rps", "latency_mean_ms",
        "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, default=Path("holder_benchmark.csv"))
    p.add_argument("--warmup-seconds", type=float, default=10)
    p.add_argument("--duration-seconds", type=float, default=60)
    p.add_argument("--bucket-seconds", type=float, default=1)
    p.add_argument("--inflight", type=int, default=64)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--cpus-per-node", type=int, default=1)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--fixed-order", action="store_true")
    args = p.parse_args()

    for name in (
        "warmup_seconds", "duration_seconds", "bucket_seconds",
        "inflight", "payload_bytes", "cpus_per_node", "repetitions",
    ):
        if getattr(args, name) <= 0:
            p.error(f"--{name.replace('_', '-')} must be positive")

    all_rows: list[dict[str, Any]] = []
    for repetition in range(args.repetitions):
        cases = list(CASES)
        if not args.fixed_order:
            random.Random(20260806 + repetition).shuffle(cases)

        for case in cases:
            print(
                f"[repetition {repetition + 1}/{args.repetitions}] {case.label}",
                flush=True,
            )
            all_rows.extend(
                run_case(
                    case, repetition, args.warmup_seconds,
                    args.duration_seconds, args.bucket_seconds,
                    args.inflight, args.payload_bytes, args.cpus_per_node,
                )
            )
            write_csv(args.output, all_rows)

    print(f"Wrote {args.output.resolve()}")


if __name__ == "__main__":
    main()
