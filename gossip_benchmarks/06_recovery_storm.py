#!/usr/bin/env python3
"""Paper experiment: Recovery under correlated recovery storms.

A single owner loses N independent in-flight task outputs at once.  Compares
Disabled, proposed Succession-R2, and WitnessBaseline-R2 by default.
"""
from __future__ import annotations

import argparse
import os
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
    disabled,
    mean_ci95,
    read_csv,
    read_marker,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
    wait_for_log,
    wait_for_protection,
    witness_baseline,
    write_csv,
)


def cases(r: int) -> list[Method]:
    return [disabled(), succession(r), witness_baseline(r)]


def start_cluster(method: Method, args) -> tuple[Cluster, Any]:
    c = Cluster()
    c.add_node(num_cpus=0, _system_config=system_config(method, witness_count=args.witness_count, object_timeout_ms=args.object_timeout_ms), include_dashboard=False)
    failure_node = c.add_node(num_cpus=max(2, args.failure_node_cpus), resources={"failure_node": 1})
    for i in range(1, args.holders + 1): c.add_node(num_cpus=args.survivor_cpus, resources={f"holder_{i}": 1})
    c.add_node(num_cpus=args.survivor_cpus, resources={"borrower_node": 1})
    for i in range(args.witness_count): c.add_node(num_cpus=0, resources={f"extra_witness_{i+1}": 1})
    return c, failure_node


def types():
    @ray.remote(max_retries=2)
    def work(index, duration_s, payload_bytes, marker, token):
        with open(marker, "a", buffering=1) as f: f.write(f"START,{time.time_ns()},{os.getpid()},{token}:{index}\n")
        time.sleep(duration_s)
        with open(marker, "a", buffering=1) as f: f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}:{index}\n")
        return index.to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, node_id): self.node_id = node_id
        def dispatch(self, n, duration_s, payload_bytes, marker, token):
            return [[work.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=True), num_cpus=1).remote(i, duration_s, payload_bytes, marker, token) for i in range(n)]]

    @ray.remote(max_restarts=0, max_concurrency=4)
    class Holder:
        def __init__(self): self.refs = []
        def hold_one(self, wrapped):
            self.refs.append(wrapped[0])
            return len(self.refs)
        def export(self): return [self.refs[-1]]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold_all(self, refs): self.refs = list(refs); return len(self.refs)
        def read_all(self): return ray.get(self.refs)

    return Owner, Holder, Borrower


def run_one(args, method: Method, n: int, trial: int) -> dict[str, Any]:
    c = None
    marker = Path(tempfile.gettempdir()) / f"ray_storm_{uuid.uuid4().hex}.csv"
    try:
        c, failure_node = start_cluster(method, args)
        ray.init(address=c.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + args.holders + 1 + args.witness_count, args.cluster_timeout_seconds)
        logs = session_dirs(c)
        Owner, Holder, Borrower = types()
        owner = Owner.options(resources={"failure_node": .01}, num_cpus=0).remote(failure_node.node_id)
        holders = [Holder.options(resources={f"holder_{i}": .01}, num_cpus=0).remote() for i in range(1, args.holders + 1)]
        borrower = Borrower.options(resources={"borrower_node": .01}, num_cpus=0).remote()
        token = uuid.uuid4().hex
        refs = ray.get(owner.dispatch.remote(n, args.task_duration_seconds, args.payload_bytes, str(marker), token))[0]
        if len(wait_for_marker(marker, "START", args.start_timeout_seconds, min_count=n)) < n:
            raise TimeoutError(f"only some of {n} original tasks started")

        protected = []
        if method.key == "witness_baseline":
            baseline_needle = "Installed full TaskSpec on all witness-holder baseline nodes"
            if len(wait_for_log(logs, baseline_needle, args.formation_timeout_seconds, min_count=n)) < n:
                raise RuntimeError(f"baseline protected fewer than {n} storm tasks")

        for task_index, ref in enumerate(refs, start=1):
            fresh = ref
            if method.key == "succession":
                for rank, holder in enumerate(holders, start=1):
                    ray.get(holder.hold_one.remote([fresh]))
                    needle = (
                        "Committed recovery succession manifest after witness publication "
                        f"with {rank + 1} total members"
                    )
                    if len(wait_for_log(logs, needle, args.formation_timeout_seconds, min_count=task_index)) < task_index:
                        raise RuntimeError(
                            f"only part of the storm reached succession rank {rank}"
                        )
                    fresh = ray.get(holder.export.remote())[0]
            protected.append(fresh)
        ray.get(borrower.hold_all.remote(protected))

        failure_wall = time.time_ns(); failure_perf = time.perf_counter()
        c.remove_node(failure_node, allow_graceful=False)
        values, error = [], ""
        try:
            values = ray.get(borrower.read_all.remote(), timeout=args.get_timeout_seconds)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        result_latency = time.perf_counter() - failure_perf
        post_starts = [r for r in read_marker(marker) if r[0] == "START" and r[1] >= failure_wall]
        first_replay = min(((r[1] - failure_wall) / 1e9 for r in post_starts), default=float("nan"))
        success_count = len(values) if values else 0
        row = {
            "trial": trial,
            "storm_size": n,
            "success_count": success_count,
            "success_rate": success_count / n,
            "failure_to_first_replay_s": first_replay,
            "failure_to_all_results_s": result_latency,
            "post_failure_execution_count": len(post_starts),
            "duplicate_replays": max(0, len(post_starts) - n),
            "error": error,
        }
        return add_method_columns(row, method)
    finally:
        safe_shutdown(ray, c)
        try: marker.unlink()
        except OSError: pass


def run(args):
    rows = []
    for trial in range(1, args.trials + 1):
        for n in args.storm_sizes:
            for method in cases(args.holders):
                print(f"trial={trial} N={n} method={method.label}")
                rows.append(run_one(args, method, n, trial))
    write_csv(Path(args.output_dir) / "recovery_storm.csv", rows)


def plot(args):
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "recovery_storm.csv")
    d = Path(args.output_dir) / "plots"; d.mkdir(parents=True, exist_ok=True)
    method_list = cases(args.holders)

    plt.figure(figsize=(8, 4.8))
    for m in method_list:
        xs, ys = [], []
        for n in args.storm_sizes:
            vals = [float(r["success_rate"]) for r in rows if r["method"] == m.key and int(r["storm_size"]) == n]
            if vals: xs.append(n); ys.append(sum(vals)/len(vals))
        plt.plot(xs, ys, marker="o", label=m.label)
    plt.xlabel("Simultaneously lost outputs"); plt.ylabel("Recovery success rate"); plt.ylim(-0.05, 1.05); plt.legend(); plt.tight_layout()
    plt.savefig(d / "recovery_storm_success_rate.png", dpi=200); plt.close()

    plt.figure(figsize=(8, 4.8))
    for m in method_list:
        if m.key == "disabled": continue
        for metric, suffix in [("failure_to_first_replay_s", "first replay"), ("failure_to_all_results_s", "all results")]:
            xs, ys = [], []
            for n in args.storm_sizes:
                vals = [float(r[metric]) for r in rows if r["method"] == m.key and int(r["storm_size"]) == n and float(r["success_rate"]) > 0]
                if vals:
                    mean, _ = mean_ci95(vals); xs.append(n); ys.append(mean)
            plt.plot(xs, ys, marker="o", label=f"{m.label}: {suffix}")
    plt.xlabel("Simultaneously lost outputs"); plt.ylabel("Latency from failure (s)"); plt.legend(); plt.tight_layout()
    plt.savefig(d / "recovery_storm_latency_scaling.png", dpi=200); plt.close()


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/06_recovery_storm")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--storm-sizes", type=int, nargs="+", default=[1,4,8,16,32])
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--task-duration-seconds", type=float, default=20)
    p.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    p.add_argument("--failure-node-cpus", type=int, default=32)
    p.add_argument("--survivor-cpus", type=int, default=3)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--object-timeout-ms", type=int, default=1000)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--formation-timeout-seconds", type=float, default=20)
    p.add_argument("--start-timeout-seconds", type=float, default=15)
    p.add_argument("--get-timeout-seconds", type=float, default=240)
    return p


def main():
    args = parser().parse_args()
    if args.command in {"run", "run-and-plot"}: run(args)
    if args.command in {"plot", "run-and-plot"}: plot(args)
if __name__ == "__main__": main()
