#!/usr/bin/env python3
"""Paper experiment: Recovery latency.

Compares proposed succession and witness-as-holder baseline for R=1..4 while an
in-flight stateless task is lost with its owner/executor node.
"""
from __future__ import annotations

import argparse
import os
import random
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    add_method_columns,
    mean_ci95,
    read_csv,
    read_marker,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
    wait_for_protection,
    witness_baseline,
    write_csv,
)


def methods_for_r(r: int) -> list[Method]:
    return [succession(r), witness_baseline(r)]


def start_cluster(method: Method, cpus_per_node: int, witness_count: int) -> tuple[Cluster, Any]:
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=system_config(method, witness_count=witness_count), include_dashboard=False)
    failure_node = cluster.add_node(num_cpus=max(1, cpus_per_node), resources={"failure_node": 1})
    for i in range(1, 5):
        cluster.add_node(num_cpus=max(1, cpus_per_node), resources={f"holder_{i}": 1})
    cluster.add_node(num_cpus=max(1, cpus_per_node), resources={"borrower_node": 1})
    return cluster, failure_node


def make_types():
    @ray.remote(max_retries=2)
    def work(seed: int, duration_s: float, payload_bytes: int, marker: str, token: str) -> bytes:
        with open(marker, "a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")
        time.sleep(duration_s)
        with open(marker, "a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        prefix = seed.to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - 8)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, node_id: str):
            self.node_id = node_id
        def dispatch(self, seed: int, duration_s: float, payload_bytes: int, marker: str, token: str):
            ref = work.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=True),
                num_cpus=1,
            ).remote(seed, duration_s, payload_bytes, marker, token)
            return [ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return True
        def export(self):
            return [self.ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return True
        def read(self):
            return ray.get(self.ref)

    return Owner, Holder, Borrower


def run_one(args: argparse.Namespace, method: Method, duration_s: float, trial: int) -> dict[str, Any]:
    cluster = None
    marker = Path(tempfile.gettempdir()) / f"ray_recovery_latency_{uuid.uuid4().hex}.csv"
    try:
        cluster, failure_node = start_cluster(method, args.cpus_per_node, args.witness_count)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 7, args.cluster_timeout_seconds)
        logs = session_dirs(cluster)
        Owner, Holder, Borrower = make_types()
        owner = Owner.options(resources={"failure_node": 0.01}, num_cpus=0).remote(failure_node.node_id)
        holders = [Holder.options(resources={f"holder_{i}": 0.01}, num_cpus=0).remote() for i in range(1, 5)]
        borrower = Borrower.options(resources={"borrower_node": 0.01}, num_cpus=0).remote()
        token = uuid.uuid4().hex
        ref = ray.get(owner.dispatch.remote(trial, duration_s, args.payload_bytes, str(marker), token))[0]
        if not wait_for_marker(marker, "START", args.start_timeout_seconds):
            raise TimeoutError("original execution did not start")

        fresh = ref
        if method.key == "succession":
            for rank in range(1, method.holders + 1):
                ray.get(holders[rank - 1].hold.remote([fresh]))
                wait_for_protection(method=method, session_paths=logs, timeout_s=args.formation_timeout_seconds, rank=rank)
                fresh = ray.get(holders[rank - 1].export.remote())[0]
        else:
            wait_for_protection(method=method, session_paths=logs, timeout_s=args.formation_timeout_seconds)

        ray.get(borrower.hold.remote([fresh]))
        failure_wall_ns = time.time_ns()
        failure_perf = time.perf_counter()
        cluster.remove_node(failure_node, allow_graceful=False)
        success = False
        error = ""
        try:
            value = ray.get(borrower.read.remote(), timeout=args.get_timeout_seconds)
            success = len(value) == args.payload_bytes
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        result_perf = time.perf_counter()

        starts = [r for r in read_marker(marker) if r[0] == "START" and r[1] >= failure_wall_ns]
        replay_start_latency = ((starts[0][1] - failure_wall_ns) / 1e9) if starts else float("nan")
        row = {
            "trial": trial,
            "task_duration_s": duration_s,
            "payload_bytes": args.payload_bytes,
            "success": int(success),
            "failure_to_replay_start_s": replay_start_latency,
            "failure_to_result_s": result_perf - failure_perf,
            "post_failure_start_count": len(starts),
            "error": error,
        }
        return add_method_columns(row, method)
    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass


def run(args: argparse.Namespace) -> None:
    cases = [(m, d) for d in args.task_durations for r in range(1, 5) for m in methods_for_r(r)]
    rows = []
    rng = random.Random(args.seed)
    for trial in range(1, args.trials + 1):
        order = cases[:]
        if not args.fixed_order:
            rng.shuffle(order)
        for method, duration in order:
            print(f"trial={trial} duration={duration}s method={method.label}")
            rows.append(run_one(args, method, duration, trial))
    write_csv(Path(args.output_dir) / "recovery_latency.csv", rows)


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "recovery_latency.csv")
    plot_dir = Path(args.output_dir) / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    for metric, ylabel, name in [
        ("failure_to_result_s", "Failure-to-result latency (s)", "recovery_total_time.png"),
        ("failure_to_replay_start_s", "Failure-to-replay-start latency (s)", "recovery_detection_time.png"),
    ]:
        plt.figure(figsize=(9.5, 5.2))
        for r in range(1, 5):
            for method_key, prefix in [("succession", "Succession"), ("witness_baseline", "Witness baseline")]:
                xs, ys, es = [], [], []
                for d in sorted({float(x["task_duration_s"]) for x in rows}):
                    vals = [float(x[metric]) for x in rows if x["method"] == method_key and int(x["holders"]) == r and float(x["task_duration_s"]) == d and x["success"] == "1"]
                    if vals:
                        mean, ci = mean_ci95(vals)
                        xs.append(d); ys.append(mean); es.append(ci)
                if xs:
                    plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=f"{prefix}, R={r}")
        plt.xlabel("Original task duration (s)")
        plt.ylabel(ylabel)
        plt.legend(ncol=2)
        plt.tight_layout()
        plt.savefig(plot_dir / name, dpi=200)
        plt.close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/03_recovery_latency")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--task-durations", type=float, nargs="+", default=[5, 10, 20, 30])
    p.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    p.add_argument("--cpus-per-node", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--formation-timeout-seconds", type=float, default=15)
    p.add_argument("--start-timeout-seconds", type=float, default=10)
    p.add_argument("--get-timeout-seconds", type=float, default=90)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")
    return p


def main() -> None:
    args = parser().parse_args()
    if args.command in {"run", "run-and-plot"}: run(args)
    if args.command in {"plot", "run-and-plot"}: plot(args)


if __name__ == "__main__": main()
