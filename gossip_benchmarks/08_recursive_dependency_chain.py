#!/usr/bin/env python3
"""Paper experiment: Recursive dependency-chain recovery.

Builds a serial chain (not an arbitrary DAG) and compares Disabled,
Succession-R2, and WitnessBaseline-R2 by default.  Every stage is submitted by
an owner on the failure node so owner loss removes lineage for the entire chain.
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
    for i in range(1, args.holders + 1): c.add_node(num_cpus=2, resources={f"holder_{i}": 1})
    c.add_node(num_cpus=2, resources={"borrower_node": 1})
    for i in range(args.witness_count): c.add_node(num_cpus=0, resources={f"extra_witness_{i+1}": 1})
    return c, failure_node


def types():
    @ray.remote(max_retries=2)
    def stage(prev_value, stage_index, duration_s, payload_bytes, marker, token):
        with open(marker, "a", buffering=1) as f: f.write(f"START,{time.time_ns()},{os.getpid()},{token}:{stage_index}\n")
        time.sleep(duration_s)
        if stage_index == 0:
            value = 0
        else:
            value = int.from_bytes(prev_value[:8], "little") + 1
        out = value.to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)
        with open(marker, "a", buffering=1) as f: f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}:{stage_index}\n")
        return out

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, node_id): self.node_id = node_id
        def first(self, duration_s, payload_bytes, marker, token):
            return [stage.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=True), num_cpus=1).remote(None, 0, duration_s, payload_bytes, marker, token)]
        def next(self, wrapped_prev, stage_index, duration_s, payload_bytes, marker, token):
            prev = wrapped_prev[0]
            return [stage.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=True), num_cpus=1).remote(prev, stage_index, duration_s, payload_bytes, marker, token)]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def __init__(self): self.refs = []
        def hold(self, wrapped):
            self.refs.append(wrapped[0])
            return True
        def export(self): return [self.refs[-1]]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped): self.ref = wrapped[0]; return True
        def read(self): return ray.get(self.ref)

    return Owner, Holder, Borrower


def protect_ref(ref, method: Method, holders, logs, timeout_s, protected_index: int):
    fresh = ref
    if method.key == "succession":
        for rank in range(1, method.holders + 1):
            ray.get(holders[rank - 1].hold.remote([fresh]))
            needle = (
                "Committed recovery succession manifest after witness publication "
                f"with {rank + 1} total members"
            )
            if len(wait_for_log(logs, needle, timeout_s, min_count=protected_index)) < protected_index:
                raise RuntimeError(
                    f"chain stage {protected_index} did not reach succession rank {rank}"
                )
            fresh = ray.get(holders[rank - 1].export.remote())[0]
    elif method.key == "witness_baseline":
        needle = "Installed full TaskSpec on all witness-holder baseline nodes"
        if len(wait_for_log(logs, needle, timeout_s, min_count=protected_index)) < protected_index:
            raise RuntimeError(
                f"baseline protection missing for chain stage {protected_index}"
            )
    return fresh


def run_one(args, method: Method, chain_length: int, trial: int) -> dict[str, Any]:
    c = None
    marker = Path(tempfile.gettempdir()) / f"ray_chain_{uuid.uuid4().hex}.csv"
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

        ref = ray.get(owner.first.remote(args.stage_duration_seconds, args.payload_bytes, str(marker), token))[0]
        wait_for_marker(marker, "START", args.start_timeout_seconds)
        fresh = protect_ref(ref, method, holders, logs, args.formation_timeout_seconds, 1)
        for stage_index in range(1, chain_length):
            ref = ray.get(owner.next.remote([fresh], stage_index, args.stage_duration_seconds, args.payload_bytes, str(marker), token))[0]
            fresh = protect_ref(ref, method, holders, logs, args.formation_timeout_seconds, stage_index + 1)
        ray.get(borrower.hold.remote([fresh]))

        # The intended experiment fails the node while stage 0 is still in flight.
        pre_failure = read_marker(marker)
        if any(r[0] == "FINISH" and r[3].endswith(":0") for r in pre_failure):
            raise RuntimeError("stage 0 finished before failure injection; increase --stage-duration-seconds")

        failure_wall = time.time_ns(); failure_perf = time.perf_counter()
        c.remove_node(failure_node, allow_graceful=False)
        success, correct, error = False, False, ""
        try:
            value = ray.get(borrower.read.remote(), timeout=args.get_timeout_seconds)
            success = True
            correct = int.from_bytes(value[:8], "little") == chain_length - 1
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        total = time.perf_counter() - failure_perf
        starts = [r for r in read_marker(marker) if r[0] == "START" and r[1] >= failure_wall]
        rel = sorted((r[1] - failure_wall) / 1e9 for r in starts)
        return add_method_columns({
            "trial": trial,
            "chain_length": chain_length,
            "success": int(success),
            "correct_result": int(correct),
            "failure_to_first_replay_s": rel[0] if rel else float("nan"),
            "failure_to_last_replay_s": rel[-1] if rel else float("nan"),
            "failure_to_final_result_s": total,
            "post_failure_stage_executions": len(starts),
            "duplicate_replays": max(0, len(starts) - chain_length),
            "error": error,
        }, method)
    finally:
        safe_shutdown(ray, c)
        try: marker.unlink()
        except OSError: pass


def run(args):
    rows = []
    for trial in range(1, args.trials + 1):
        for n in args.chain_lengths:
            for method in cases(args.holders):
                print(f"trial={trial} chain={n} method={method.label}")
                rows.append(run_one(args, method, n, trial))
    write_csv(Path(args.output_dir) / "recursive_chain.csv", rows)


def plot(args):
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "recursive_chain.csv")
    d = Path(args.output_dir) / "plots"; d.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 5.2))
    for method_key, label in [("succession", "Succession"), ("witness_baseline", "Witness baseline")]:
        for metric, suffix in [("failure_to_first_replay_s", "first replay"), ("failure_to_last_replay_s", "last replay"), ("failure_to_final_result_s", "final result")]:
            xs, ys, es = [], [], []
            for n in args.chain_lengths:
                vals = [float(r[metric]) for r in rows if r["method"] == method_key and int(r["chain_length"]) == n and r["success"] == "1"]
                if vals:
                    mean, ci = mean_ci95(vals); xs.append(n); ys.append(mean); es.append(ci)
            plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=f"{label}: {suffix}")
    plt.xlabel("Protected dependency-chain length")
    plt.ylabel("Latency from failure (s)")
    plt.legend(ncol=2); plt.tight_layout()
    # Preserve the paper's current figure filename even though this is a serial chain, not a DAG.
    plt.savefig(d / "recovery_chain_dag_latency.png", dpi=200); plt.close()


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/08_recursive_dependency_chain")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--chain-lengths", type=int, nargs="+", default=[2,4,8,16])
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--stage-duration-seconds", type=float, default=2)
    p.add_argument("--payload-bytes", type=int, default=1024 * 1024)
    p.add_argument("--failure-node-cpus", type=int, default=4)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--object-timeout-ms", type=int, default=1000)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--formation-timeout-seconds", type=float, default=20)
    p.add_argument("--start-timeout-seconds", type=float, default=10)
    p.add_argument("--get-timeout-seconds", type=float, default=180)
    return p


def main():
    args = parser().parse_args()
    if args.command in {"run", "run-and-plot"}: run(args)
    if args.command in {"plot", "run-and-plot"}: plot(args)
if __name__ == "__main__": main()
