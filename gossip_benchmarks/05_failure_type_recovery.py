#!/usr/bin/env python3
"""Paper experiment: Recovery across failure types.

Failure modes:
  owner_worker              - kill only the owner worker/actor
  owner_node                - kill owner's node; original executor is separate
  owner_and_executor_node   - kill node containing owner and original executor

Compares Succession-R and WitnessBaseline-R at the same R (default R=2).
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

FAILURE_MODES = ["owner_worker", "owner_node", "owner_and_executor_node"]


def start_cluster(method: Method, args) -> tuple[Cluster, Any, Any]:
    c = Cluster()
    c.add_node(num_cpus=0, _system_config=system_config(method, witness_count=args.witness_count, object_timeout_ms=args.object_timeout_ms), include_dashboard=False)
    owner_node = c.add_node(num_cpus=2, resources={"owner_node": 1})
    executor_node = c.add_node(num_cpus=2, resources={"executor_node": 1})
    for i in range(1, 5): c.add_node(num_cpus=2, resources={f"holder_{i}": 1})
    c.add_node(num_cpus=2, resources={"borrower_node": 1})
    return c, owner_node, executor_node


def types():
    @ray.remote(max_retries=2)
    def work(duration_s, payload_bytes, marker, token):
        with open(marker, "a", buffering=1) as f: f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")
        time.sleep(duration_s)
        with open(marker, "a", buffering=1) as f: f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        return b"x" * payload_bytes

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def dispatch(self, node_id, duration_s, payload_bytes, marker, token):
            return [work.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=True), num_cpus=1).remote(duration_s, payload_bytes, marker, token)]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped): self.ref = wrapped[0]; return True
        def export(self): return [self.ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped): self.ref = wrapped[0]; return True
        def read(self): return ray.get(self.ref)

    return Owner, Holder, Borrower


def run_one(args, method: Method, failure_mode: str, trial: int) -> dict[str, Any]:
    c = None
    marker = Path(tempfile.gettempdir()) / f"ray_failure_type_{uuid.uuid4().hex}.csv"
    try:
        c, owner_node, executor_node = start_cluster(method, args)
        ray.init(address=c.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 8, args.cluster_timeout_seconds)
        logs = session_dirs(c)
        Owner, Holder, Borrower = types()
        owner = Owner.options(resources={"owner_node": .01}, num_cpus=0).remote()
        holders = [Holder.options(resources={f"holder_{i}": .01}, num_cpus=0).remote() for i in range(1, 5)]
        borrower = Borrower.options(resources={"borrower_node": .01}, num_cpus=0).remote()

        execution_node_id = owner_node.node_id if failure_mode == "owner_and_executor_node" else executor_node.node_id
        token = uuid.uuid4().hex
        ref = ray.get(owner.dispatch.remote(execution_node_id, args.task_duration_seconds, args.payload_bytes, str(marker), token))[0]
        wait_for_marker(marker, "START", args.start_timeout_seconds)

        fresh = ref
        if method.key == "succession":
            for rank in range(1, method.holders + 1):
                ray.get(holders[rank - 1].hold.remote([fresh]))
                wait_for_protection(method=method, session_paths=logs, timeout_s=args.formation_timeout_seconds, rank=rank)
                fresh = ray.get(holders[rank - 1].export.remote())[0]
        else:
            wait_for_protection(method=method, session_paths=logs, timeout_s=args.formation_timeout_seconds)
        ray.get(borrower.hold.remote([fresh]))

        failure_wall = time.time_ns(); failure_perf = time.perf_counter()
        if failure_mode == "owner_worker":
            ray.kill(owner, no_restart=True)
        else:
            c.remove_node(owner_node, allow_graceful=False)
        success, error = False, ""
        try:
            value = ray.get(borrower.read.remote(), timeout=args.get_timeout_seconds)
            success = len(value) == args.payload_bytes
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        result_latency = time.perf_counter() - failure_perf
        starts = [r for r in read_marker(marker) if r[0] == "START" and r[1] >= failure_wall]
        replay_start = (starts[0][1] - failure_wall) / 1e9 if starts else float("nan")
        row = {
            "trial": trial,
            "failure_mode": failure_mode,
            "success": int(success),
            "failure_to_replay_start_s": replay_start,
            "replay_to_result_s": result_latency - replay_start if starts else float("nan"),
            "failure_to_result_s": result_latency,
            "post_failure_execution_count": len(starts),
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
        for mode in FAILURE_MODES:
            for method in [succession(args.holders), witness_baseline(args.holders)]:
                print(f"trial={trial} mode={mode} method={method.label}")
                rows.append(run_one(args, method, mode, trial))
    write_csv(Path(args.output_dir) / "failure_type.csv", rows)


def plot(args):
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "failure_type.csv")
    d = Path(args.output_dir) / "plots"; d.mkdir(parents=True, exist_ok=True)
    labels, detection, replay = [], [], []
    for mode in FAILURE_MODES:
        for method_key, short in [("succession", "Succession"), ("witness_baseline", "Witness baseline")]:
            subset = [r for r in rows if r["failure_mode"] == mode and r["method"] == method_key and r["success"] == "1"]
            det, _ = mean_ci95(float(r["failure_to_replay_start_s"]) for r in subset)
            rep, _ = mean_ci95(float(r["replay_to_result_s"]) for r in subset)
            labels.append(f"{mode}\n{short}"); detection.append(det); replay.append(rep)
    xs = list(range(len(labels)))
    plt.figure(figsize=(10.5, 5.5))
    plt.bar(xs, detection, label="Failure detection + replay initiation")
    plt.bar(xs, replay, bottom=detection, label="Replay execution to result")
    plt.xticks(xs, labels, rotation=25, ha="right")
    plt.ylabel("Recovery latency (s)"); plt.legend(); plt.tight_layout()
    plt.savefig(d / "failure_type_recovery_latency.png", dpi=200); plt.close()


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/05_failure_type_recovery")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--holders", type=int, default=2, choices=[1,2,3,4])
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
