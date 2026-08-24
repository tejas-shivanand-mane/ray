#!/usr/bin/env python3
"""Paper experiment: Experimental Results -> Simple Recovery.

Compares three methods under one owner/producer node failure:
  * Disabled
  * Succession-R1 (proposed recovery succession)
  * WitnessBaseline-R1 (witness-as-holder baseline)

Outputs:
  results.csv
  plots/avail_thput.png
  plots/avail_lat.png
"""
from __future__ import annotations

import argparse
import math
import os
import random
import time
from pathlib import Path
from typing import Any

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    add_method_columns,
    disabled,
    percentile,
    read_csv,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_protection,
    witness_baseline,
    write_csv,
)

METHODS = [disabled(), succession(1), witness_baseline(1)]


def selected_methods(args: argparse.Namespace) -> list[Method]:
    selected: list[Method] = []
    for key in args.methods:
        if key == "disabled":
            selected.append(disabled())
        elif key == "succession":
            selected.append(succession(1))
        elif key == "witness_baseline":
            selected.append(witness_baseline(1))
        else:
            raise ValueError(f"unknown method: {key}")
    return selected


def start_cluster(method: Method, object_timeout_ms: int) -> tuple[Cluster, Any]:
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=2,
            object_timeout_ms=object_timeout_ms,
        ),
        include_dashboard=False,
    )
    failure_node = cluster.add_node(num_cpus=1, resources={"owner_node": 1})
    cluster.add_node(num_cpus=1, resources={"holder_1": 1})
    cluster.add_node(num_cpus=1, resources={"witness_1": 1})
    cluster.add_node(num_cpus=1, resources={"witness_2": 1})
    cluster.add_node(num_cpus=1, resources={"borrower_node": 1})
    return cluster, failure_node


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(payload_bytes: int) -> bytes:
        return b"x" * payload_bytes

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, failure_node_id: str):
            self.failure_node_id = failure_node_id

        def create(self, payload_bytes: int):
            ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.failure_node_id, soft=True
                ),
                num_cpus=1,
            ).remote(payload_bytes)
            return [ref]

        def ping(self) -> int:
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            return [self.ref]

        def ping(self) -> int:
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def read(self):
            return len(ray.get(self.ref))

        def ping(self) -> int:
            return os.getpid()

    return Owner, Holder, Borrower


def bucketize(
    events: list[tuple[float, bool, float]],
    *,
    trial: int,
    method: Method,
    duration_s: float,
    failure_at_s: float,
    bucket_s: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    bucket_count = math.ceil(duration_s / bucket_s)
    for idx in range(bucket_count):
        start = idx * bucket_s
        end = min(duration_s, start + bucket_s)
        selected = [e for e in events if start <= e[0] < end]
        successful_ms = [e[2] * 1000.0 for e in selected if e[1]]
        row: dict[str, Any] = {
            "trial": trial,
            "elapsed_seconds": start,
            "bucket_seconds": end - start,
            "failure_at_seconds": failure_at_s,
            "successful_requests": len(successful_ms),
            "failed_requests": sum(1 for e in selected if not e[1]),
            "throughput_rps": len(successful_ms) / max(end - start, 1e-9),
            "latency_p50_ms": percentile(successful_ms, 0.50),
            "latency_p95_ms": percentile(successful_ms, 0.95),
        }
        rows.append(add_method_columns(row, method))
    return rows


def run_one(args: argparse.Namespace, method: Method, trial: int) -> list[dict[str, Any]]:
    cluster = None
    try:
        cluster, failure_node = start_cluster(method, args.object_timeout_ms)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 6, args.cluster_timeout_seconds)
        logs = session_dirs(cluster)
        Owner, Holder, Borrower = make_remote_types()
        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote(
            failure_node.node_id
        )
        holder = Holder.options(resources={"holder_1": 0.01}, num_cpus=0).remote()
        borrower = Borrower.options(resources={"borrower_node": 0.01}, num_cpus=0).remote()
        ray.get([owner.ping.remote(), holder.ping.remote(), borrower.ping.remote()])

        base_ref = ray.get(owner.create.remote(args.payload_bytes))[0]
        ready, _ = ray.wait([base_ref], num_returns=1, timeout=30, fetch_local=False)
        if not ready:
            raise TimeoutError("Producer did not complete before the benchmark")

        ref_for_borrower = base_ref
        if method.key == "succession":
            ray.get(holder.hold.remote([base_ref]))
            wait_for_protection(
                method=method,
                session_paths=logs,
                timeout_s=args.formation_timeout_seconds,
                rank=1,
            )
            ref_for_borrower = ray.get(holder.export.remote())[0]
        elif method.key == "witness_baseline":
            wait_for_protection(
                method=method,
                session_paths=logs,
                timeout_s=args.formation_timeout_seconds,
            )

        ray.get(borrower.hold.remote([ref_for_borrower]))
        if args.borrower_settle_seconds:
            time.sleep(args.borrower_settle_seconds)
        if ray.get(borrower.read.remote(), timeout=10) != args.payload_bytes:
            raise RuntimeError("Warm-up read returned an unexpected object size")

        events: list[tuple[float, bool, float]] = []
        start = time.perf_counter()
        failed = False
        printed_errors = 0
        while True:
            elapsed = time.perf_counter() - start
            if elapsed >= args.duration_seconds:
                break
            if not failed and elapsed >= args.failure_at_seconds:
                cluster.remove_node(failure_node, allow_graceful=False)
                failed = True
                continue
            t0 = time.perf_counter()
            ok = False
            try:
                value = ray.get(
                    borrower.read.remote(), timeout=args.request_timeout_seconds
                )
                ok = value == args.payload_bytes
            except Exception as exc:
                if elapsed >= args.failure_at_seconds and printed_errors < 3:
                    print(f"[{method.label}] post-failure read: {type(exc).__name__}: {exc}")
                    printed_errors += 1
            t1 = time.perf_counter()
            events.append((t1 - start, ok, t1 - t0))
            if not ok and args.failed_request_backoff_seconds:
                time.sleep(args.failed_request_backoff_seconds)

        return bucketize(
            events,
            trial=trial,
            method=method,
            duration_s=args.duration_seconds,
            failure_at_s=args.failure_at_seconds,
            bucket_s=args.bucket_seconds,
        )
    finally:
        safe_shutdown(ray, cluster)


def run(args: argparse.Namespace) -> Path:
    rows: list[dict[str, Any]] = []
    order = selected_methods(args)
    rng = random.Random(args.seed)
    for trial in range(1, args.trials + 1):
        trial_order = order[:]
        if not args.fixed_order:
            rng.shuffle(trial_order)
        for method in trial_order:
            print(f"trial={trial} method={method.label}")
            rows.extend(run_one(args, method, trial))
    out = Path(args.output_dir) / "results.csv"
    write_csv(out, rows)
    return out


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    path = Path(args.output_dir) / "results.csv"
    rows = read_csv(path)
    plot_dir = Path(args.output_dir) / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    for metric, ylabel, filename in [
        ("throughput_rps", "Successful requests / s", "avail_thput.png"),
        ("latency_p95_ms", "P95 successful-request latency (ms)", "avail_lat.png"),
    ]:
        plt.figure(figsize=(8.5, 4.8))
        present_method_keys = {r["method"] for r in rows}
        for method in METHODS:
            if method.key not in present_method_keys:
                continue
            xs = sorted({float(r["elapsed_seconds"]) for r in rows if r["method"] == method.key})
            ys = []
            for x in xs:
                vals = [
                    float(r[metric])
                    for r in rows
                    if r["method"] == method.key
                    and float(r["elapsed_seconds"]) == x
                    and r[metric] not in {"", "nan", "NaN"}
                ]
                ys.append(sum(vals) / len(vals) if vals else math.nan)
            plt.plot(xs, ys, marker="o", label=method.label)
        failure_at = float(rows[0]["failure_at_seconds"])
        plt.axvline(failure_at, linestyle="--", label="Failure injection")
        plt.xlabel("Elapsed time (s)")
        plt.ylabel(ylabel)
        plt.legend()
        plt.tight_layout()
        plt.savefig(plot_dir / filename, dpi=200)
        plt.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/01_simple_recovery")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument(
        "--methods",
        nargs="+",
        choices=["disabled", "succession", "witness_baseline"],
        default=["disabled", "succession", "witness_baseline"],
        help="Methods to run; default preserves the original benchmark matrix.",
    )
    p.add_argument("--duration-seconds", type=float, default=45.0)
    p.add_argument("--failure-at-seconds", type=float, default=15.0)
    p.add_argument("--bucket-seconds", type=float, default=1.0)
    p.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    p.add_argument("--object-timeout-ms", type=int, default=1000)
    p.add_argument("--cluster-timeout-seconds", type=float, default=20.0)
    p.add_argument("--formation-timeout-seconds", type=float, default=15.0)
    p.add_argument("--borrower-settle-seconds", type=float, default=1.0)
    p.add_argument("--request-timeout-seconds", type=float, default=1.0)
    p.add_argument("--failed-request-backoff-seconds", type=float, default=0.01)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if args.command in {"run", "run-and-plot"}:
        run(args)
    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
