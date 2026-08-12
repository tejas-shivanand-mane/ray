#!/usr/bin/env python3
"""Paper experiment: Succession fallback under holder failures.

This is intentionally proposed-method-only.  It validates the ordered dynamic
succession list H1..H4.  The witness-as-holder baseline does not form that same
ordered dynamic holder structure, so mixing it into this benchmark would change
the question being tested.
"""
from __future__ import annotations

import argparse
import os
import re
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    add_method_columns,
    find_log_lines,
    read_csv,
    read_marker,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
    wait_for_protection,
    write_csv,
)

METHOD = succession(4)


def accepted_rank(logs: set[Path]) -> int:
    pat = re.compile(r"Recovery succession accepted by holder rank\s+(\d+)")
    found = []
    for line in find_log_lines(logs, "Recovery succession accepted by holder rank"):
        m = pat.search(line)
        if m:
            found.append(int(m.group(1)))
    return found[-1] if found else -1


def start_cluster(args: argparse.Namespace) -> tuple[Cluster, Any, list[Any]]:
    c = Cluster()
    c.add_node(num_cpus=0, _system_config=system_config(METHOD, witness_count=args.witness_count, object_timeout_ms=args.object_timeout_ms), include_dashboard=False)
    owner_node = c.add_node(num_cpus=2, resources={"owner_node": 1})
    holder_nodes = [c.add_node(num_cpus=2, resources={f"holder_{i}": 1}) for i in range(1, 5)]
    c.add_node(num_cpus=2, resources={"borrower_node": 1})
    for i in range(args.witness_count):
        c.add_node(num_cpus=0, resources={f"extra_witness_{i+1}": 1})
    return c, owner_node, holder_nodes


def types():
    @ray.remote(max_retries=2)
    def work(duration_s: float, payload_bytes: int, marker: str, token: str) -> bytes:
        with open(marker, "a", buffering=1) as f: f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")
        time.sleep(duration_s)
        with open(marker, "a", buffering=1) as f: f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        return b"x" * payload_bytes

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, node_id): self.node_id = node_id
        def dispatch(self, duration_s, payload_bytes, marker, token):
            return [work.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=True), num_cpus=1).remote(duration_s, payload_bytes, marker, token)]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped): self.ref = wrapped[0]; return True
        def export(self): return [self.ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped): self.ref = wrapped[0]; return True
        def read(self): return ray.get(self.ref)

    return Owner, Holder, Borrower


def run_one(args: argparse.Namespace, predead: int, trial: int) -> dict[str, Any]:
    cluster = None
    marker = Path(tempfile.gettempdir()) / f"ray_fallback_{uuid.uuid4().hex}.csv"
    try:
        cluster, owner_node, holder_nodes = start_cluster(args)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + 4 + 1 + args.witness_count, args.cluster_timeout_seconds)
        logs = session_dirs(cluster)
        Owner, Holder, Borrower = types()
        owner = Owner.options(resources={"owner_node": .01}, num_cpus=0).remote(owner_node.node_id)
        holders = [Holder.options(resources={f"holder_{i}": .01}, num_cpus=0).remote() for i in range(1, 5)]
        borrower = Borrower.options(resources={"borrower_node": .01}, num_cpus=0).remote()
        token = uuid.uuid4().hex
        ref = ray.get(owner.dispatch.remote(args.task_duration_seconds, args.payload_bytes, str(marker), token))[0]
        wait_for_marker(marker, "START", args.start_timeout_seconds)
        fresh = ref
        formation_start = time.perf_counter()
        for rank, holder in enumerate(holders, start=1):
            ray.get(holder.hold.remote([fresh]))
            wait_for_protection(method=METHOD, session_paths=logs, timeout_s=args.formation_timeout_seconds, rank=rank)
            fresh = ray.get(holder.export.remote())[0]
        formation_time = time.perf_counter() - formation_start
        ray.get(borrower.hold.remote([fresh]))

        for i in range(predead):
            cluster.remove_node(holder_nodes[i], allow_graceful=False)
        failure_wall = time.time_ns()
        failure_perf = time.perf_counter()
        cluster.remove_node(owner_node, allow_graceful=False)
        success, error = False, ""
        try:
            value = ray.get(borrower.read.remote(), timeout=args.get_timeout_seconds)
            success = len(value) == args.payload_bytes
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        result_latency = time.perf_counter() - failure_perf
        post_starts = [r for r in read_marker(marker) if r[0] == "START" and r[1] >= failure_wall]
        observed = accepted_rank(logs)
        expected = predead + 1 if predead < 4 else -1
        row = {
            "trial": trial,
            "predead_holders": predead,
            "expected_rank": expected,
            "accepted_rank": observed,
            "success": int(success),
            "rank_correct": int(observed == expected),
            "failure_to_result_s": result_latency,
            "post_failure_execution_count": len(post_starts),
            "formation_time_s": formation_time,
            "error": error,
        }
        return add_method_columns(row, METHOD)
    finally:
        safe_shutdown(ray, cluster)
        try: marker.unlink()
        except OSError: pass


def run(args):
    rows = []
    for trial in range(1, args.trials + 1):
        for k in range(5):
            print(f"trial={trial} predead={k}")
            rows.append(run_one(args, k, trial))
    write_csv(Path(args.output_dir) / "holder_fallback.csv", rows)


def plot(args):
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "holder_fallback.csv")
    xs = list(range(5))
    observed, expected = [], []
    for k in xs:
        vals = [int(r["accepted_rank"]) for r in rows if int(r["predead_holders"]) == k]
        observed.append(sum(vals) / len(vals) if vals else float("nan"))
        expected.append(k + 1 if k < 4 else float("nan"))
    d = Path(args.output_dir) / "plots"; d.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(7.5, 4.8))
    plt.plot(xs, expected, marker="o", label="Expected next surviving rank")
    plt.plot(xs, observed, marker="s", label="Observed accepted rank")
    plt.xlabel("Earlier holders failed before owner failure")
    plt.ylabel("Recovery holder rank")
    plt.xticks(xs)
    plt.legend(); plt.tight_layout(); plt.savefig(d / "accepted_rank_vs_predead_holders.png", dpi=200); plt.close()


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/04_holder_witness_fallback")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--task-duration-seconds", type=float, default=20)
    p.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--object-timeout-ms", type=int, default=1000)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--formation-timeout-seconds", type=float, default=15)
    p.add_argument("--start-timeout-seconds", type=float, default=10)
    p.add_argument("--get-timeout-seconds", type=float, default=90)
    return p


def main():
    args = parser().parse_args()
    if args.command in {"run", "run-and-plot"}: run(args)
    if args.command in {"plot", "run-and-plot"}: plot(args)

if __name__ == "__main__": main()
