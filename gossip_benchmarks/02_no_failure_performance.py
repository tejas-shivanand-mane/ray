#!/usr/bin/env python3
"""Paper experiment: No-failure steady-state recovery overhead.

Same six-logical-node application pipeline for every case:
    driver -> producer -> consumer1 -> consumer2 -> consumer3 -> consumer4

Compares:
    Disabled
    Succession-R1..R4
    WitnessBaseline-R1..R4

Outputs benchmark_runs.csv, benchmark_timeseries.csv, benchmark_summary.csv and
plots/throughput_all_payloads.png + plots/p95_latency_all_payloads.png.
"""
from __future__ import annotations

import argparse
import math
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
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
    witness_baseline,
    write_csv,
)


@dataclass(frozen=True)
class Payload:
    name: str
    size_bytes: int


@dataclass
class Pending:
    request_id: int
    next_consumer: int
    submitted_ns: int
    payload_ref: ray.ObjectRef
    tagged: bool


def methods() -> list[Method]:
    return [disabled()] + [succession(r) for r in range(1, 5)] + [witness_baseline(r) for r in range(1, 5)]


def parse_payload(text: str) -> Payload:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("payload must be NAME:BYTES") from exc
    if not name or size <= 0:
        raise argparse.ArgumentTypeError("payload must have non-empty NAME and positive BYTES")
    return Payload(name, size)


def start_cluster(method: Method, cpus_per_node: int, witness_count: int) -> tuple[Cluster, list[str]]:
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(method, witness_count=witness_count),
        include_dashboard=False,
    )
    workers = []
    workers.append(cluster.add_node(num_cpus=cpus_per_node, resources={"producer_node": 1}))
    for i in range(1, 5):
        workers.append(cluster.add_node(num_cpus=cpus_per_node, resources={f"consumer_{i}": 1}))
    return cluster, [n.node_id for n in workers]


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
    producer_strategy: Any,
    payload_bytes: int,
    warmup_s: float,
    duration_s: float,
    bucket_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pending: dict[ray.ObjectRef, Pending] = {}
    request_id = 0
    tagged_pending = 0
    tagged_submitted = 0
    completed_in_window = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    warmup_end_ns = start_ns + int(warmup_s * 1e9)
    measure_end_ns = warmup_end_ns + int(duration_s * 1e9)
    bucket_count = max(1, math.ceil(duration_s / bucket_s))
    bucket_counts = [0] * bucket_count
    bucket_lats: list[list[float]] = [[] for _ in range(bucket_count)]

    def submit_one() -> None:
        nonlocal request_id, tagged_pending, tagged_submitted
        submitted = time.perf_counter_ns()
        tagged = warmup_end_ns <= submitted < measure_end_ns
        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes)
        first = consumers[0].touch_and_export.remote([payload_ref])
        pending[first] = Pending(request_id, 1, submitted, payload_ref, tagged)
        request_id += 1
        if tagged:
            tagged_pending += 1
            tagged_submitted += 1

    def process_one(allow_resubmit: bool) -> bool:
        nonlocal tagged_pending, completed_in_window
        if not pending:
            return False
        ready, _ = ray.wait(list(pending), num_returns=1, timeout=wait_timeout_s)
        if not ready:
            return False
        stage_ref = ready[0]
        exported = ray.get(stage_ref)
        state = pending.pop(stage_ref)
        fresh_ref = exported[0]
        if state.next_consumer < 4:
            nxt = consumers[state.next_consumer].touch_and_export.remote([fresh_ref])
            pending[nxt] = Pending(
                state.request_id,
                state.next_consumer + 1,
                state.submitted_ns,
                fresh_ref,
                state.tagged,
            )
            return True

        completed = time.perf_counter_ns()
        if warmup_end_ns <= completed < measure_end_ns:
            completed_in_window += 1
            idx = min(int(((completed - warmup_end_ns) / 1e9) // bucket_s), bucket_count - 1)
            bucket_counts[idx] += 1
        if state.tagged:
            latency = (completed - state.submitted_ns) / 1e6
            latencies_ms.append(latency)
            tagged_pending -= 1
            if warmup_end_ns <= completed < measure_end_ns:
                idx = min(int(((completed - warmup_end_ns) / 1e9) // bucket_s), bucket_count - 1)
                bucket_lats[idx].append(latency)

        if allow_resubmit:
            now = time.perf_counter_ns()
            if now < measure_end_ns or tagged_pending > 0:
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
            raise TimeoutError(f"drain timeout with {len(pending)} stage calls pending")
        process_one(False)

    summary = {
        "completed_in_window": completed_in_window,
        "latency_sample_count": len(latencies_ms),
        "latency_tagged_submitted": tagged_submitted,
        "throughput_rps": completed_in_window / duration_s,
        "logical_payload_throughput_mib_s": completed_in_window * payload_bytes / duration_s / (1024.0 * 1024.0),
        "latency_mean_ms": statistics.fmean(latencies_ms) if latencies_ms else math.nan,
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }
    timeseries = []
    for i in range(bucket_count):
        lo = i * bucket_s
        hi = min((i + 1) * bucket_s, duration_s)
        timeseries.append(
            {
                "bucket_index": i,
                "elapsed_start_s": lo,
                "elapsed_end_s": hi,
                "throughput_rps": bucket_counts[i] / max(hi - lo, 1e-9),
                "latency_p95_ms": percentile(bucket_lats[i], 0.95),
            }
        )
    return summary, timeseries


def run_one(args: argparse.Namespace, method: Method, payload: Payload, repetition: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cluster = None
    try:
        cluster, node_ids = start_cluster(method, args.cpus_per_node, args.witness_count)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 6, args.cluster_timeout_seconds)
        produce, Consumer = make_remote_types()
        consumers = [
            Consumer.options(resources={f"consumer_{i}": 0.01}, num_cpus=0).remote()
            for i in range(1, 5)
        ]
        ray.get([c.ping.remote() for c in consumers])
        summary, ts = run_workload(
            produce=produce,
            consumers=consumers,
            producer_strategy=NodeAffinitySchedulingStrategy(node_id=node_ids[0], soft=False),
            payload_bytes=payload.size_bytes,
            warmup_s=args.warmup_seconds,
            duration_s=args.duration_seconds,
            bucket_s=args.bucket_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
        )
        base = {
            "repetition": repetition,
            "payload_name": payload.name,
            "payload_bytes": payload.size_bytes,
        }
        run_row = add_method_columns({**base, **summary}, method)
        ts_rows = [add_method_columns({**base, **row}, method) for row in ts]
        return run_row, ts_rows
    finally:
        safe_shutdown(ray, cluster)


def summarize(run_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    keys = sorted({(r["method"], r["holders"], r["payload_name"], r["payload_bytes"], r["method_label"]) for r in run_rows})
    for method, holders, payload_name, payload_bytes, label in keys:
        rows = [r for r in run_rows if (r["method"], r["holders"], r["payload_name"]) == (method, holders, payload_name)]
        t_mean, t_ci = mean_ci95(float(r["throughput_rps"]) for r in rows)
        l_mean, l_ci = mean_ci95(float(r["latency_p95_ms"]) for r in rows)
        out.append({
            "method": method,
            "method_label": label,
            "holders": holders,
            "payload_name": payload_name,
            "payload_bytes": payload_bytes,
            "repetitions": len(rows),
            "throughput_mean_rps": t_mean,
            "throughput_ci95_rps": t_ci,
            "p95_latency_mean_ms": l_mean,
            "p95_latency_ci95_ms": l_ci,
        })
    return out


def run(args: argparse.Namespace) -> None:
    run_rows: list[dict[str, Any]] = []
    ts_rows: list[dict[str, Any]] = []
    cases = [(m, p) for p in args.payloads for m in methods()]
    rng = random.Random(args.seed)
    for rep in range(1, args.repetitions + 1):
        order = cases[:]
        if not args.fixed_order:
            rng.shuffle(order)
        for method, payload in order:
            print(f"rep={rep} payload={payload.name} method={method.label}")
            row, ts = run_one(args, method, payload, rep)
            run_rows.append(row)
            ts_rows.extend(ts)
    root = Path(args.output_dir)
    write_csv(root / "benchmark_runs.csv", run_rows)
    write_csv(root / "benchmark_timeseries.csv", ts_rows)
    write_csv(root / "benchmark_summary.csv", summarize(run_rows))


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    root = Path(args.output_dir)
    rows = read_csv(root / "benchmark_summary.csv")
    plot_dir = root / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    payloads = sorted({(int(r["payload_bytes"]), r["payload_name"]) for r in rows})
    method_order = methods()

    for metric, ci_col, ylabel, filename in [
        ("throughput_mean_rps", "throughput_ci95_rps", "Completed pipelines / s", "throughput_all_payloads.png"),
        ("p95_latency_mean_ms", "p95_latency_ci95_ms", "P95 end-to-end latency (ms)", "p95_latency_all_payloads.png"),
    ]:
        plt.figure(figsize=(10.5, 5.5))
        for payload_bytes, payload_name in payloads:
            xs, ys, es = [], [], []
            for idx, method in enumerate(method_order):
                found = [r for r in rows if r["payload_name"] == payload_name and r["method_label"] == method.label]
                if not found:
                    continue
                xs.append(idx)
                ys.append(float(found[0][metric]))
                es.append(float(found[0][ci_col]))
            plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=f"{payload_name} ({payload_bytes} B)")
        plt.xticks(range(len(method_order)), [m.label for m in method_order], rotation=35, ha="right")
        plt.ylabel(ylabel)
        plt.xlabel("Recovery method / redundancy")
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / filename, dpi=200)
        plt.close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/02_no_failure_performance")
    p.add_argument("--warmup-seconds", type=float, default=5)
    p.add_argument("--duration-seconds", type=float, default=30)
    p.add_argument("--bucket-seconds", type=float, default=5)
    p.add_argument("--inflight", type=int, default=64)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--payloads", type=parse_payload, nargs="+", default=[Payload("1KiB", 1024), Payload("64KiB", 65536), Payload("256KiB", 262144), Payload("2MiB", 2097152)])
    p.add_argument("--cpus-per-node", type=int, default=3)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--wait-timeout-seconds", type=float, default=1)
    p.add_argument("--drain-timeout-seconds", type=float, default=120)
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
