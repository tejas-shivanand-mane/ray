#!/usr/bin/env python3
"""Profile submission-only cost of Recovery Frontier registration.

This diagnostic deliberately does NOT export producer ObjectRefs to another
worker. It measures only the owner-side task submission/retention path where the
frontier planner currently registers each eligible task and copies its TaskSpec.

Each case submits repeated 32-task bursts and drains the tasks outside the timed
submission interval. Recovery is never activated, so witness RPC counts must
remain zero.

If frontier K values all have similar submission cost and exceed frozen Fixed-R,
that isolates the remaining overhead to K-independent per-task frontier work
rather than grouped publication.
"""
from __future__ import annotations

import os
import statistics
import time
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import percentile, safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
BURST = 32
ROUNDS = 80
PADDING_BYTES = 1024
VARIANTS = [
    ("fixed_r", None),
    ("frontier_k1", 1),
    ("frontier_k4", 4),
    ("frontier_k8", 8),
    ("frontier_k16", 16),
    ("frontier_k32", 32),
]


def config_for(k: int | None) -> dict[str, Any]:
    cfg = system_config(
        witness_baseline(R),
        witness_count=R,
        profiling_enabled=True,
    )
    cfg.update(
        {
            "enable_recovery_frontier": k is not None,
            "recovery_frontier_group_size": 1 if k is None else int(k),
            "recovery_baseline_perf_protect_every_n": 1,
        }
    )
    return cfg


def run_case(label: str, k: int | None) -> dict[str, Any]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(num_cpus=0, _system_config=config_for(k), include_dashboard=False)
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        # Spare node keeps the topology comparable to the activation benchmark.
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 3, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, padding: bytes) -> int:
            if padding:
                _ = padding[0]
            return i

        strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node.node_id,
            soft=False,
        )
        padding = b"p" * PADDING_BYTES

        # Warm one burst so worker startup and first-task setup are excluded.
        warm = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(i, padding)
            for i in range(BURST)
        ]
        assert ray.get(warm) == list(range(BURST))
        del warm

        global_worker.core_worker.reset_recovery_succession_profile()

        burst_ms: list[float] = []
        next_id = 1_000_000
        for _round in range(ROUNDS):
            ids = list(range(next_id, next_id + BURST))
            next_id += BURST

            t0 = time.perf_counter_ns()
            refs = [
                produce.options(scheduling_strategy=strategy, num_cpus=1).remote(i, padding)
                for i in ids
            ]
            elapsed_ms = (time.perf_counter_ns() - t0) / 1e6
            burst_ms.append(elapsed_ms)

            # Drain outside the timed region. These are owner-local gets and do
            # not activate recovery or publish witness metadata.
            assert ray.get(refs) == ids
            del refs

        p = global_worker.core_worker.get_recovery_succession_profile()
        witness_sent = int(p.get("witness_update_rpcs_sent", 0))
        assert witness_sent == 0, (label, p)

        retained_created = int(p.get("owner_retained_task_specs_created", 0))
        retained_copy_ns = int(p.get("owner_retained_task_spec_copy_time_ns", 0))
        expected_tasks = BURST * ROUNDS
        assert retained_created == expected_tasks, (label, expected_tasks, p)

        return {
            "label": label,
            "k": 0 if k is None else k,
            "mean_burst_ms": statistics.fmean(burst_ms),
            "p50_burst_ms": percentile(burst_ms, 0.50),
            "p95_burst_ms": percentile(burst_ms, 0.95),
            "mean_us_per_submit": statistics.fmean(burst_ms) * 1000.0 / BURST,
            "owner_retained_created": retained_created,
            "owner_retained_copy_ms": retained_copy_ns / 1e6,
            "owner_retained_copy_us_per_task": (
                retained_copy_ns / 1e3 / retained_created if retained_created else 0.0
            ),
            "witness_updates_sent": witness_sent,
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    rows = []
    for label, k in VARIANTS:
        print(f"running {label}...", flush=True)
        row = run_case(label, k)
        rows.append(row)
        print(
            f"  submit={row['mean_us_per_submit']:.2f} us/task  "
            f"p95-burst={row['p95_burst_ms']:.3f} ms  "
            f"retained-copy={row['owner_retained_copy_us_per_task']:.2f} us/task",
            flush=True,
        )

    fixed = rows[0]["mean_us_per_submit"]
    print("\nSubmission-only profile:")
    print(
        "  variant        submit_us/task  delta_vs_fixed%  p95_burst_ms  "
        "retained_copy_us/task  witness_updates"
    )
    for row in rows:
        delta = 100.0 * (row["mean_us_per_submit"] - fixed) / fixed if fixed else 0.0
        print(
            f"  {row['label']:<14} {row['mean_us_per_submit']:>14.2f}  "
            f"{delta:>14.2f}  {row['p95_burst_ms']:>12.3f}  "
            f"{row['owner_retained_copy_us_per_task']:>21.2f}  "
            f"{row['witness_updates_sent']:>15}"
        )


if __name__ == "__main__":
    main()
