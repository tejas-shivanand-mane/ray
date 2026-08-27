#!/usr/bin/env python3
"""Diagnostic 38: isolate steady-state owner retirement/cleanup overhead.

Runs the same producer -> consumer pipeline used by benchmark 30, but compares
normal ObjectRef retirement with a mode that intentionally retains every
producer ObjectRef until the timed window is over.

If K=32 approaches Disabled only in retained-ref mode, the remaining throughput
loss comes from continuous owner-side completion/ref-deletion/cleanup churn,
not from the Frontier activation or consumer receive data path.
"""
from __future__ import annotations

import argparse
import gc
import os
import statistics
import time
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"
os.environ["RAY_RECOVERY_PROFILING"] = "0"
os.environ["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
os.environ["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    disabled,
    percentile,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
)

R = 2
K = 32
BURST = 32
INFLIGHT = 128
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
VARIANTS = ["disabled", "fixed_r", "frontier_k32"]
MODES = ["normal", "retain"]


def config_for(variant: str) -> dict[str, Any]:
    if variant == "disabled":
        cfg = system_config(disabled(), witness_count=R, profiling_enabled=False)
        cfg.update({
            "enable_recovery_frontier": False,
            "recovery_frontier_group_size": 1,
            "recovery_baseline_perf_protect_every_n": 1,
        })
        return cfg

    cfg = system_config(witness_baseline(R), witness_count=R, profiling_enabled=False)
    cfg.update({
        "enable_recovery_frontier": variant == "frontier_k32",
        "recovery_frontier_group_size": K if variant == "frontier_k32" else 1,
        "recovery_baseline_perf_protect_every_n": 1,
    })
    return cfg


def run_case(variant: str, mode: str, duration_s: float, warmup_s: float) -> dict[str, float]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(num_cpus=0, _system_config=config_for(variant), include_dashboard=False)
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        cluster.add_node(num_cpus=4, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            return int(i).to_bytes(8, "little", signed=False) + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_restarts=0, max_concurrency=256)
        class Consumer:
            def touch(self, wrapped):
                value = ray.get(wrapped[0])
                return int.from_bytes(value[:8], "little", signed=False)

            def ping(self):
                return True

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        def window(seconds: float, base: int) -> tuple[float, float, int]:
            pending: dict[ray.ObjectRef, int] = {}
            submitted_ns: dict[int, int] = {}
            retained: list[ray.ObjectRef] = []
            next_id = base
            completed = 0
            latencies: list[float] = []
            start_ns = time.perf_counter_ns()
            end_ns = start_ns + int(seconds * 1e9)

            def submit_burst() -> None:
                nonlocal next_id
                burst: list[tuple[int, ray.ObjectRef]] = []
                for _ in range(BURST):
                    rid = next_id
                    next_id += 1
                    submitted_ns[rid] = time.perf_counter_ns()
                    payload_ref = produce.options(
                        scheduling_strategy=strategy,
                        num_cpus=1,
                    ).remote(rid, PAYLOAD_BYTES, padding)
                    burst.append((rid, payload_ref))
                    if mode == "retain":
                        retained.append(payload_ref)
                for rid, payload_ref in burst:
                    pending[consumer.touch.remote([payload_ref])] = rid

            def drain_ready(timeout: float = 0.01) -> None:
                nonlocal completed
                if not pending:
                    return
                ready, _ = ray.wait(
                    list(pending),
                    num_returns=min(64, len(pending)),
                    timeout=timeout,
                )
                now_ns = time.perf_counter_ns()
                for ref in ready:
                    rid = pending.pop(ref)
                    observed = int(ray.get(ref))
                    if observed != rid:
                        raise RuntimeError((rid, observed))
                    if now_ns <= end_ns:
                        completed += 1
                    latencies.append((now_ns - submitted_ns.pop(rid)) / 1e6)

            while len(pending) + BURST <= INFLIGHT:
                submit_burst()

            while time.perf_counter_ns() < end_ns:
                drain_ready()
                while time.perf_counter_ns() < end_ns and len(pending) + BURST <= INFLIGHT:
                    submit_burst()

            deadline = time.monotonic() + 30.0
            while pending:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"drain timeout: {len(pending)}")
                drain_ready()

            throughput = completed / seconds
            p95 = percentile(latencies, 0.95)
            retained_count = len(retained)
            # Keep refs alive through the full measurement and drain. Release only now.
            retained.clear()
            gc.collect()
            return throughput, p95, retained_count

        if warmup_s > 0:
            window(warmup_s, 1_000_000)
            time.sleep(0.5)

        throughput, p95, retained_count = window(duration_s, 10_000_000)
        return {
            "throughput": throughput,
            "p95": p95,
            "retained": float(retained_count),
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repetitions", type=int, default=3)
    ap.add_argument("--warmup-seconds", type=float, default=2.0)
    ap.add_argument("--duration-seconds", type=float, default=8.0)
    args = ap.parse_args()

    rows: dict[tuple[str, str], list[dict[str, float]]] = {
        (variant, mode): [] for variant in VARIANTS for mode in MODES
    }

    for rep in range(args.repetitions):
        print(f"repetition {rep + 1}/{args.repetitions}", flush=True)
        for variant in VARIANTS:
            for mode in MODES:
                print(f"  running {variant:12s} {mode}...", flush=True)
                row = run_case(variant, mode, args.duration_seconds, args.warmup_seconds)
                rows[(variant, mode)].append(row)
                print(
                    f"    throughput={row['throughput']:.1f} rps  p95={row['p95']:.2f} ms  retained={int(row['retained'])}",
                    flush=True,
                )

    print("\nRetirement isolation profile:")
    print("  variant        mode      throughput_rps   vs_same_normal%   p95_ms")
    for variant in VARIANTS:
        normal_mean = statistics.fmean(r["throughput"] for r in rows[(variant, "normal")])
        for mode in MODES:
            thr = statistics.fmean(r["throughput"] for r in rows[(variant, mode)])
            p95 = statistics.fmean(r["p95"] for r in rows[(variant, mode)])
            delta = 100.0 * (thr - normal_mean) / normal_mean if normal_mean else 0.0
            print(f"  {variant:<14} {mode:<8} {thr:>14.1f} {delta:>17.2f} {p95:>9.2f}")

    disabled_retain = statistics.fmean(r["throughput"] for r in rows[("disabled", "retain")])
    k32_retain = statistics.fmean(r["throughput"] for r in rows[("frontier_k32", "retain")])
    retain_overhead = 100.0 * (disabled_retain - k32_retain) / disabled_retain
    print(f"\nK32 overhead vs Disabled with retirement suppressed: {retain_overhead:.2f}%")


if __name__ == "__main__":
    main()
