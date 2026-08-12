#!/usr/bin/env python3
"""Paper experiment: Recovery-succession formation scaling.

Compares proposed succession and witness-as-holder baseline for 1..4 redundancy
members and batches of protected outputs.

Two timings are recorded:
  native_formation_time_s
    Succession: beginning of holder admission -> all manifests committed.
    Baseline: beginning of submission -> all full TaskSpecs installed on witnesses.
  protection_ready_time_s
    Method-neutral: beginning of task submission -> requested protection is ready.

Use protection_ready_time_s for proposed-vs-baseline comparison.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import ray
from ray.cluster_utils import Cluster

from _benchmark_common import (
    Method,
    add_method_columns,
    mean_ci95,
    read_csv,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_log,
    witness_baseline,
    write_csv,
)


def start_cluster(method: Method, args) -> Cluster:
    c = Cluster()
    c.add_node(num_cpus=0, _system_config=system_config(method, witness_count=args.witness_count), include_dashboard=False)
    c.add_node(num_cpus=args.producer_cpus, resources={"producer_node": 1})
    for i in range(1, 5): c.add_node(num_cpus=1, resources={f"holder_{i}": 1})
    return c


def types():
    @ray.remote(max_retries=2)
    def produce(index: int, payload_bytes: int) -> bytes:
        return index.to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold_many(self, wrapped_refs):
            self.refs = list(wrapped_refs)
            return len(self.refs)
        def export_many(self):
            return list(self.refs)

    return produce, Holder


def run_one(args, method: Method, n: int, trial: int) -> dict[str, Any]:
    c = None
    try:
        c = start_cluster(method, args)
        ray.init(address=c.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 6, args.cluster_timeout_seconds)
        logs = session_dirs(c)
        produce, Holder = types()
        holders = [Holder.options(resources={f"holder_{i}": .01}, num_cpus=0).remote() for i in range(1, 5)]

        submit_start = time.perf_counter()
        refs = [produce.options(resources={"producer_node": .01}, num_cpus=1).remote(i, args.payload_bytes) for i in range(n)]

        if method.key == "witness_baseline":
            needle = "Installed full TaskSpec on all witness-holder baseline nodes"
            if len(wait_for_log(logs, needle, args.formation_timeout_seconds, min_count=n)) < n:
                raise RuntimeError(f"baseline protection did not become ready for all {n} tasks")
            protection_ready = time.perf_counter() - submit_start
            native = protection_ready
        else:
            # Keep submission outside the native holder-admission timing, matching
            # the original paper definition, but include it in protection_ready.
            native_start = time.perf_counter()
            fresh = refs
            for rank in range(1, method.holders + 1):
                if ray.get(holders[rank - 1].hold_many.remote(fresh)) != n:
                    raise RuntimeError("holder did not retain all refs")
                needle = (
                    "Committed recovery succession manifest after witness publication "
                    f"with {rank + 1} total members"
                )
                if len(wait_for_log(logs, needle, args.formation_timeout_seconds, min_count=n)) < n:
                    raise RuntimeError(f"rank {rank} did not commit all {n} manifests")
                fresh = ray.get(holders[rank - 1].export_many.remote())
            native = time.perf_counter() - native_start
            protection_ready = time.perf_counter() - submit_start

        return add_method_columns({
            "trial": trial,
            "protected_outputs": n,
            "payload_bytes": args.payload_bytes,
            "native_formation_time_s": native,
            "protection_ready_time_s": protection_ready,
        }, method)
    finally:
        safe_shutdown(ray, c)


def run(args):
    rows = []
    for trial in range(1, args.trials + 1):
        for n in args.protected_outputs:
            for r in range(1, 5):
                for method in [succession(r), witness_baseline(r)]:
                    print(f"trial={trial} N={n} method={method.label}")
                    rows.append(run_one(args, method, n, trial))
    write_csv(Path(args.output_dir) / "formation_scaling.csv", rows)


def plot(args):
    import matplotlib.pyplot as plt
    rows = read_csv(Path(args.output_dir) / "formation_scaling.csv")
    d = Path(args.output_dir) / "plots"; d.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9.5, 5.4))
    for r in range(1, 5):
        for method_key, prefix in [("succession", "Succession"), ("witness_baseline", "Witness baseline")]:
            xs, ys, es = [], [], []
            for n in args.protected_outputs:
                vals = [1000 * float(x["protection_ready_time_s"]) for x in rows if x["method"] == method_key and int(x["holders"]) == r and int(x["protected_outputs"]) == n]
                if vals:
                    mean, ci = mean_ci95(vals); xs.append(n); ys.append(mean); es.append(ci)
            plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=f"{prefix}, R={r}")
    plt.xlabel("Protected task outputs")
    plt.ylabel("Submission-to-protection-ready time (ms)")
    plt.legend(ncol=2)
    plt.tight_layout(); plt.savefig(d / "recovery_formation_scaling.png", dpi=200); plt.close()


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "run-and-plot"], nargs="?", default="run-and-plot")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/07_formation_scaling")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--protected-outputs", type=int, nargs="+", default=[1,4,8,16,32,64])
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--producer-cpus", type=int, default=8)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30)
    p.add_argument("--formation-timeout-seconds", type=float, default=30)
    return p


def main():
    args = parser().parse_args()
    if args.command in {"run", "run-and-plot"}: run(args)
    if args.command in {"plot", "run-and-plot"}: plot(args)
if __name__ == "__main__": main()
