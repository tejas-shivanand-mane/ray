#!/usr/bin/env python3
"""Profile real Recovery Frontier activation cost for one 32-task burst.

This is a diagnostic, not a paper throughput benchmark. Producer tasks are
allowed to finish before any ObjectRef is exported. We then time only the
owner->consumer export/protection phase and inspect the recovery profile.

Expected witness-update RPC counts with R=2 and 32 registered tasks:
  fixed_r      64
  frontier_k1  64
  frontier_k4  16
  frontier_k8   8
  frontier_k16  4
  frontier_k32  2

If these counts hold while activation wall time remains high for small K, the
remaining overhead is dominated by one synchronous all-R ACK barrier per
frontier group rather than by failure to reduce RPC count.
"""
from __future__ import annotations

import os
import time
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
N = 32
PAYLOAD_BYTES = 1024
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


def profile() -> dict[str, Any]:
    return global_worker.core_worker.get_recovery_succession_profile()


def run_case(label: str, k: int | None) -> dict[str, Any]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(num_cpus=0, _system_config=config_for(k), include_dashboard=False)
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        cluster.add_node(num_cpus=2, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            return int(i).to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_restarts=0, max_concurrency=128)
        class Consumer:
            def touch(self, wrapped):
                value = ray.get(wrapped[0])
                return int.from_bytes(value[:8], "little")

            def ping(self):
                return True

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())

        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        # Register the whole burst, then finish execution before activation.
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                i, PAYLOAD_BYTES, padding
            )
            for i in range(N)
        ]
        values = ray.get(refs)
        assert len(values) == N

        # Measure only downstream export/protection, not producer execution.
        global_worker.core_worker.reset_recovery_succession_profile()
        t0 = time.perf_counter()
        observed = ray.get([consumer.touch.remote([ref]) for ref in refs])
        activation_s = time.perf_counter() - t0
        assert observed == list(range(N)), observed

        p = profile()
        sent = int(p.get("witness_update_rpcs_sent", 0))
        done = int(p.get("witness_update_rpcs_completed", 0))
        manifests = int(p.get("initial_manifest_build_count", 0))
        spec_bytes = int(p.get("task_spec_bytes_sent", 0))

        expected_groups = N if k is None or k == 1 else (N + k - 1) // k
        expected_updates = expected_groups * R
        assert sent == expected_updates, (label, expected_updates, p)
        assert done == expected_updates, (label, expected_updates, p)
        assert manifests == expected_groups, (label, expected_groups, p)

        return {
            "label": label,
            "k": 0 if k is None else k,
            "groups": expected_groups,
            "activation_s": activation_s,
            "activation_ms_per_task": activation_s * 1000.0 / N,
            "witness_updates_sent": sent,
            "witness_updates_done": done,
            "initial_manifests": manifests,
            "task_spec_bytes_sent": spec_bytes,
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
            f"  groups={row['groups']:2d}  updates={row['witness_updates_sent']:2d}  "
            f"activation={row['activation_s']:.4f}s  "
            f"per-task={row['activation_ms_per_task']:.3f}ms  "
            f"TaskSpecBytes={row['task_spec_bytes_sent']}",
            flush=True,
        )

    print("\nActivation profile:")
    print(
        "  variant        groups  updates  activation_s  ms/task   TaskSpecBytes"
    )
    for row in rows:
        print(
            f"  {row['label']:<14} {row['groups']:>6}  "
            f"{row['witness_updates_sent']:>7}  {row['activation_s']:>12.4f}  "
            f"{row['activation_ms_per_task']:>7.3f}  {row['task_spec_bytes_sent']:>13}"
        )


if __name__ == "__main__":
    main()
