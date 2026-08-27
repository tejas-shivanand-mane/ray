#!/usr/bin/env python3
"""Diagnostic 33: activation cost breakdown with a Disabled control.

The previous activation profile proved that Recovery Frontiers reduce physical
witness updates correctly, but all recovery-enabled variants still took about
0.29 ms/task to export a finished producer ObjectRef downstream. That profile
omitted the most important control: recovery disabled.

This diagnostic uses one 128-task burst. All producer tasks are allowed to
finish before any ObjectRef is exported. We then reset recovery profiling and
time only downstream ObjectRef export / recovery activation.

Cases:
  disabled
  fixed_r
  frontier_k1
  frontier_k4
  frontier_k8
  frontier_k16
  frontier_k32

Besides wall time it prints existing CoreWorker recovery hot-path counters so we
can identify which per-task recovery stage remains dominant after RPC grouping.
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

from _benchmark_common import (
    disabled,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
)

R = 2
N = 128
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k1", "recovery", 1),
    ("frontier_k4", "recovery", 4),
    ("frontier_k8", "recovery", 8),
    ("frontier_k16", "recovery", 16),
    ("frontier_k32", "recovery", 32),
]


def config_for(mode: str, k: int | None) -> dict[str, Any]:
    if mode == "disabled":
        cfg = system_config(disabled(), witness_count=R, profiling_enabled=False)
        cfg.update(
            {
                "enable_recovery_frontier": False,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
            }
        )
        return cfg

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
    try:
        return global_worker.core_worker.get_recovery_succession_profile()
    except Exception:
        return {}


def ns_per_task(p: dict[str, Any], key: str) -> float:
    return float(p.get(key, 0)) / N / 1e3


def run_case(label: str, mode: str, k: int | None) -> dict[str, Any]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=config_for(mode, k),
            include_dashboard=False,
        )
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

        @ray.remote(max_restarts=0, max_concurrency=256)
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

        # Finish execution before activation so the timed interval contains
        # downstream export/protection only.
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                i, PAYLOAD_BYTES, padding
            )
            for i in range(N)
        ]
        values = ray.get(refs)
        assert len(values) == N

        try:
            global_worker.core_worker.reset_recovery_succession_profile()
        except Exception:
            pass

        t0 = time.perf_counter_ns()
        observed = ray.get([consumer.touch.remote([ref]) for ref in refs])
        elapsed_ns = time.perf_counter_ns() - t0
        assert observed == list(range(N)), observed

        # Fixed-R publication is asynchronous. Give completion callbacks a
        # short opportunity to settle before reading the diagnostic snapshot.
        deadline = time.monotonic() + 5.0
        p = profile()
        while time.monotonic() < deadline:
            sent = int(p.get("witness_update_rpcs_sent", 0))
            done = int(p.get("witness_update_rpcs_completed", 0))
            if sent == done:
                break
            time.sleep(0.01)
            p = profile()

        sent = int(p.get("witness_update_rpcs_sent", 0))
        done = int(p.get("witness_update_rpcs_completed", 0))
        manifests = int(p.get("initial_manifest_build_count", 0))

        if mode == "disabled":
            expected_groups = 0
            expected_updates = 0
        else:
            expected_groups = N if k is None or k == 1 else (N + k - 1) // k
            expected_updates = expected_groups * R
            assert sent == expected_updates, (label, expected_updates, p)
            assert done == expected_updates, (label, expected_updates, p)
            assert manifests == expected_groups, (label, expected_groups, p)

        return {
            "label": label,
            "k": 0 if k is None else k,
            "groups": expected_groups,
            "updates": sent,
            "activation_ms": elapsed_ns / 1e6,
            "activation_us_per_task": elapsed_ns / N / 1e3,
            "metadata_lookup_us_per_task": ns_per_task(p, "recovery_metadata_lookup_time_ns"),
            "task_arg_metadata_us_per_task": ns_per_task(p, "task_argument_metadata_time_ns"),
            "ensure_args_us_per_task": ns_per_task(p, "ensure_task_arguments_time_ns"),
            "manifest_build_us_per_task": ns_per_task(p, "initial_manifest_build_time_ns"),
            "witness_select_us_per_task": ns_per_task(p, "witness_selection_time_ns"),
            "register_owned_us_per_task": ns_per_task(p, "register_owned_task_time_ns"),
            "witness_publish_us_per_task": ns_per_task(p, "witness_publish_time_ns"),
            "metadata_lookup_calls": int(p.get("recovery_metadata_lookup_calls", 0)),
            "task_arg_metadata_calls": int(p.get("task_argument_metadata_calls", 0)),
            "register_owned_calls": int(p.get("register_owned_task_count", 0)),
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    rows = []
    for label, mode, k in VARIANTS:
        print(f"running {label}...", flush=True)
        row = run_case(label, mode, k)
        rows.append(row)
        print(
            f"  activation={row['activation_us_per_task']:.2f} us/task  "
            f"groups={row['groups']} updates={row['updates']}",
            flush=True,
        )

    disabled_us = rows[0]["activation_us_per_task"]
    print("\nActivation breakdown (128 finished producer tasks):")
    print(
        "  variant        activation_us/task  delta_vs_disabled_us  groups updates  "
        "lookup_us  argmeta_us  ensure_us  manifest_us  select_us  register_us  publish_us"
    )
    for row in rows:
        print(
            f"  {row['label']:<14} {row['activation_us_per_task']:>18.2f}  "
            f"{row['activation_us_per_task'] - disabled_us:>20.2f}  "
            f"{row['groups']:>6} {row['updates']:>7}  "
            f"{row['metadata_lookup_us_per_task']:>9.3f}  "
            f"{row['task_arg_metadata_us_per_task']:>10.3f}  "
            f"{row['ensure_args_us_per_task']:>9.3f}  "
            f"{row['manifest_build_us_per_task']:>11.3f}  "
            f"{row['witness_select_us_per_task']:>9.3f}  "
            f"{row['register_owned_us_per_task']:>11.3f}  "
            f"{row['witness_publish_us_per_task']:>10.3f}"
        )

    print("\nCall counts (sanity):")
    for row in rows:
        print(
            f"  {row['label']:<14} lookup_calls={row['metadata_lookup_calls']}  "
            f"argmeta_calls={row['task_arg_metadata_calls']}  "
            f"register_owned={row['register_owned_calls']}"
        )


if __name__ == "__main__":
    main()
