#!/usr/bin/env python3
"""Diagnostic 39: isolate exporting Recovery Frontier refs before producer completion.

Benchmark 30 exports each producer ObjectRef immediately after submitting a burst,
while diagnostics 31-34 first wait for every producer task to finish.  This test
compares those two states directly with the same 32-task burst:

  pending  - export nested ObjectRefs immediately while producers are sleeping/queued
  finished - wait for all producers to finish, then export the same refs

The consumer deliberately does NOT dereference the nested ObjectRef.  It only stores
it and returns its ObjectID, so the pending case measures owner export/protection and
consumer receipt rather than producer execution latency.
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

from _benchmark_common import disabled, safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
N = 32
PRODUCER_DELAY_S = 0.50
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
REPETITIONS = 3

VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k32", "recovery", 32),
]
MODES = ["pending", "finished"]


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


def reset_profile() -> None:
    try:
        global_worker.core_worker.reset_recovery_succession_profile()
    except Exception:
        pass


def run_case(label: str, recovery_mode: str, k: int | None, export_mode: str) -> dict[str, float]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=config_for(recovery_mode, k),
            include_dashboard=False,
        )
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        cluster.add_node(num_cpus=2, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, delay_s: float, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            time.sleep(delay_s)
            return int(i).to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
        class Consumer:
            def hold(self, wrapped):
                ref = wrapped[0]
                return ref.hex()

            def ping(self):
                return True

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())

        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                i, PRODUCER_DELAY_S, PAYLOAD_BYTES, padding
            )
            for i in range(N)
        ]

        if export_mode == "finished":
            values = ray.get(refs)
            assert len(values) == N
        elif export_mode != "pending":
            raise ValueError(export_mode)

        reset_profile()

        submit_start = time.perf_counter_ns()
        calls = [consumer.hold.remote([ref]) for ref in refs]
        submit_elapsed_ns = time.perf_counter_ns() - submit_start

        wall_start = time.perf_counter_ns()
        observed_ids = ray.get(calls)
        wait_elapsed_ns = time.perf_counter_ns() - wall_start
        assert observed_ids == [ref.hex() for ref in refs]

        # Let asynchronous Fixed-R publications settle before reading counters.
        deadline = time.monotonic() + 5.0
        p = profile()
        while time.monotonic() < deadline:
            sent = int(p.get("witness_update_rpcs_sent", 0))
            done = int(p.get("witness_update_rpcs_completed", 0))
            if sent == done:
                break
            time.sleep(0.01)
            p = profile()

        if recovery_mode == "disabled":
            expected_groups = 0
            expected_updates = 0
        else:
            expected_groups = N if k is None or k == 1 else (N + int(k) - 1) // int(k)
            expected_updates = expected_groups * R
            assert int(p.get("witness_update_rpcs_sent", 0)) == expected_updates, p
            assert int(p.get("witness_update_rpcs_completed", 0)) == expected_updates, p
            assert int(p.get("initial_manifest_build_count", 0)) == expected_groups, p

        # Drain pending producers only after the export measurement.
        if export_mode == "pending":
            values = ray.get(refs)
            assert len(values) == N

        publish_count = int(p.get("witness_publish_count", 0))
        publish_ns = int(p.get("witness_publish_time_ns", 0))

        return {
            "submit_us_task": submit_elapsed_ns / N / 1e3,
            "consumer_wait_us_task": wait_elapsed_ns / N / 1e3,
            "publish_us_group": publish_ns / publish_count / 1e3 if publish_count else 0.0,
            "groups": float(expected_groups),
            "updates": float(expected_updates),
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    rows: dict[tuple[str, str], list[dict[str, float]]] = {
        (label, mode): [] for label, _, _ in VARIANTS for mode in MODES
    }

    for rep in range(REPETITIONS):
        print(f"repetition {rep + 1}/{REPETITIONS}", flush=True)
        for label, recovery_mode, k in VARIANTS:
            for mode in MODES:
                print(f"  {label:<14} {mode}...", flush=True)
                row = run_case(label, recovery_mode, k, mode)
                rows[(label, mode)].append(row)
                print(
                    f"    submit={row['submit_us_task']:.2f} us/task  "
                    f"consumer_wait={row['consumer_wait_us_task']:.2f} us/task  "
                    f"publish={row['publish_us_group']:.1f} us/group",
                    flush=True,
                )

    print("\nPending-vs-finished export profile:")
    print(
        "  variant        mode      submit_us/task  delta_pending_us  consumer_wait_us/task  publish_us/group  groups  updates"
    )
    for label, _, _ in VARIANTS:
        finished_submit = statistics.fmean(
            r["submit_us_task"] for r in rows[(label, "finished")]
        )
        for mode in MODES:
            group = rows[(label, mode)]
            mean = lambda key: statistics.fmean(r[key] for r in group)
            delta = mean("submit_us_task") - finished_submit
            print(
                f"  {label:<14} {mode:<8} "
                f"{mean('submit_us_task'):>14.2f}  "
                f"{delta:>16.2f}  "
                f"{mean('consumer_wait_us_task'):>21.2f}  "
                f"{mean('publish_us_group'):>16.1f}  "
                f"{mean('groups'):>6.0f}  "
                f"{mean('updates'):>7.0f}"
            )


if __name__ == "__main__":
    main()
