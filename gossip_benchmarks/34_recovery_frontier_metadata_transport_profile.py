#!/usr/bin/env python3
"""Diagnostic 34: quantify per-ObjectRef recovery metadata that Frontier does not amortize.

Recovery Frontier reduces fixed-R holder publication operations, but every downstream
ObjectRef still carries task-centric recovery metadata. This diagnostic measures the
existing C++ counters for that transport sidecar and repeats activation measurements
to reduce the one-shot noise seen in diagnostic 33.

The key quantities are:
  * metadata transport bytes per attached dependency
  * full-metadata equivalent bytes per dependency
  * compact-transport hit rate
  * activation microseconds per task
  * witness updates per task

No source/C++ changes are required to run this script.
"""
from __future__ import annotations

import argparse
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

from _benchmark_common import disabled, safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
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
        cfg.update({
            "enable_recovery_frontier": False,
            "recovery_frontier_group_size": 1,
            "recovery_baseline_perf_protect_every_n": 1,
        })
        return cfg

    cfg = system_config(witness_baseline(R), witness_count=R, profiling_enabled=True)
    cfg.update({
        "enable_recovery_frontier": k is not None,
        "recovery_frontier_group_size": 1 if k is None else int(k),
        "recovery_baseline_perf_protect_every_n": 1,
    })
    return cfg


def profile() -> dict[str, Any]:
    try:
        return global_worker.core_worker.get_recovery_succession_profile()
    except Exception:
        return {}


def run_case(label: str, mode: str, k: int | None, n: int) -> dict[str, Any]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(num_cpus=0, _system_config=config_for(mode, k), include_dashboard=False)
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

        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                i, PAYLOAD_BYTES, padding
            )
            for i in range(n)
        ]
        assert len(ray.get(refs)) == n

        try:
            global_worker.core_worker.reset_recovery_succession_profile()
        except Exception:
            pass

        t0 = time.perf_counter_ns()
        observed = ray.get([consumer.touch.remote([ref]) for ref in refs])
        elapsed_ns = time.perf_counter_ns() - t0
        assert observed == list(range(n)), observed

        deadline = time.monotonic() + 5.0
        p = profile()
        while time.monotonic() < deadline:
            sent = int(p.get("witness_update_rpcs_sent", 0))
            done = int(p.get("witness_update_rpcs_completed", 0))
            if sent == done:
                break
            time.sleep(0.01)
            p = profile()

        attached = int(p.get("task_argument_metadata_refs_attached", 0))
        compact = int(p.get("task_argument_metadata_compact_refs", 0))
        fallbacks = int(p.get("task_argument_metadata_compact_fallbacks", 0))
        transport_bytes = int(p.get("task_argument_metadata_transport_bytes", 0))
        full_bytes = int(p.get("task_argument_metadata_full_bytes_equivalent", 0))
        argmeta_ns = int(p.get("task_argument_metadata_time_ns", 0))
        sent = int(p.get("witness_update_rpcs_sent", 0))
        done = int(p.get("witness_update_rpcs_completed", 0))
        manifests = int(p.get("initial_manifest_build_count", 0))
        metadata_builds = int(p.get("task_centric_metadata_builds", 0))
        lookup_calls = int(p.get("recovery_metadata_lookup_calls", 0))

        if mode == "disabled":
            assert sent == 0 and done == 0
        else:
            expected_groups = n if k is None or k == 1 else (n + k - 1) // k
            expected_updates = expected_groups * R
            assert sent == expected_updates, (label, expected_updates, p)
            assert done == expected_updates, (label, expected_updates, p)
            assert manifests == expected_groups, (label, expected_groups, p)
            assert attached == n, (label, n, attached, p)
            assert compact + fallbacks == attached, (label, attached, compact, fallbacks)

        return {
            "label": label,
            "activation_us_per_task": elapsed_ns / n / 1e3,
            "attached": attached,
            "compact": compact,
            "fallbacks": fallbacks,
            "transport_bytes_per_ref": transport_bytes / attached if attached else 0.0,
            "full_bytes_per_ref": full_bytes / attached if attached else 0.0,
            "compact_ratio": compact / attached if attached else 0.0,
            "argmeta_us_per_ref": argmeta_ns / attached / 1e3 if attached else 0.0,
            "witness_updates": sent,
            "updates_per_task": sent / n,
            "metadata_builds_per_task": metadata_builds / n,
            "lookup_calls_per_task": lookup_calls / n,
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=256)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()

    if args.tasks <= 0 or args.repetitions <= 0:
        raise SystemExit("--tasks and --repetitions must be positive")

    all_rows: dict[str, list[dict[str, Any]]] = {label: [] for label, _, _ in VARIANTS}

    for rep in range(args.repetitions):
        print(f"repetition {rep + 1}/{args.repetitions}", flush=True)
        for label, mode, k in VARIANTS:
            print(f"  running {label}...", flush=True)
            row = run_case(label, mode, k, args.tasks)
            all_rows[label].append(row)
            print(
                f"    activation={row['activation_us_per_task']:.2f} us/task  "
                f"meta={row['transport_bytes_per_ref']:.1f} B/ref  "
                f"compact={100.0 * row['compact_ratio']:.1f}%  "
                f"updates/task={row['updates_per_task']:.4f}",
                flush=True,
            )

    print("\nRepeated metadata transport profile:")
    print(
        "  variant        activation_us/task  meta_B/ref  full_B/ref  compact%  "
        "argmeta_us/ref  updates/task  builds/task  lookup_calls/task"
    )
    for label, _, _ in VARIANTS:
        rows = all_rows[label]
        mean = lambda key: statistics.fmean(float(row[key]) for row in rows)
        print(
            f"  {label:<14} {mean('activation_us_per_task'):>18.2f}  "
            f"{mean('transport_bytes_per_ref'):>10.1f}  "
            f"{mean('full_bytes_per_ref'):>10.1f}  "
            f"{100.0 * mean('compact_ratio'):>8.1f}  "
            f"{mean('argmeta_us_per_ref'):>14.3f}  "
            f"{mean('updates_per_task'):>12.4f}  "
            f"{mean('metadata_builds_per_task'):>11.2f}  "
            f"{mean('lookup_calls_per_task'):>17.2f}"
        )


if __name__ == "__main__":
    main()
