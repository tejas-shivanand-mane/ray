#!/usr/bin/env python3
"""Benchmark 45: profile the once-per-K32 first-activation tax.

Benchmark 44 showed that calls 2..32 are effectively at the precommitted
baseline and that the RPC-in-flight window is usually gone before Python regains
control.  This benchmark therefore isolates the first cold downstream export of
one K32 group and reports the already-existing Recovery Succession profile
counters that execute synchronously in that path.

No C++ instrumentation is required.
"""
from __future__ import annotations

import os
import statistics
import time
from dataclasses import dataclass
from typing import Any, Callable

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
K = 32
N = 32
REPETITIONS = 7
PENDING_DELAY_S = 0.05
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
POLL_TIMEOUT_S = 5.0


@dataclass
class SerializationStats:
    calls: int = 0
    time_ns: int = 0


class TimedSerializer:
    def __init__(self):
        self.ctx = global_worker.get_serialization_context()
        self.original: Callable[[Any], Any] | None = None
        self.stats = SerializationStats()

    def __enter__(self) -> SerializationStats:
        self.original = self.ctx.serialize
        original = self.original
        stats = self.stats

        def timed(value):
            start = time.perf_counter_ns()
            try:
                return original(value)
            finally:
                stats.calls += 1
                stats.time_ns += time.perf_counter_ns() - start

        self.ctx.serialize = timed
        return stats

    def __exit__(self, exc_type, exc, tb):
        assert self.original is not None
        self.ctx.serialize = self.original


def config() -> dict[str, Any]:
    cfg = system_config(witness_baseline(R), witness_count=R, profiling_enabled=True)
    cfg.update(
        {
            "enable_recovery_frontier": True,
            "recovery_frontier_group_size": K,
            "recovery_baseline_perf_protect_every_n": 1,
        }
    )
    return cfg


def profile() -> dict[str, Any]:
    try:
        return global_worker.core_worker.get_recovery_succession_profile()
    except Exception:
        return {}


def reset_profile() -> None:
    global_worker.core_worker.reset_recovery_succession_profile()


def wait_for_ack(timeout_s: float = POLL_TIMEOUT_S) -> None:
    deadline = time.monotonic() + timeout_s
    last = profile()
    while time.monotonic() < deadline:
        sent = int(last.get("witness_update_rpcs_sent", 0))
        done = int(last.get("witness_update_rpcs_completed", 0))
        if sent >= R and done >= R:
            return
        time.sleep(0.0001)
        last = profile()
    raise AssertionError(f"timed out waiting for K32 ACKs: {last}")


def total_us(p: dict[str, Any], key: str) -> float:
    return int(p.get(key, 0)) / 1e3


def measure_one(consumer, ref):
    with TimedSerializer() as ser:
        start = time.perf_counter_ns()
        call = consumer.hold.remote([ref])
        elapsed_ns = time.perf_counter_ns() - start
    return call, elapsed_ns / 1e3, ser.time_ns / 1e3


def mean(rows: list[dict[str, float]], key: str) -> float:
    return statistics.fmean(row[key] for row in rows)


def main() -> None:
    cluster = None
    keepalive = []
    rows: list[dict[str, float]] = []
    try:
        cluster = Cluster()
        cluster.add_node(num_cpus=0, _system_config=config(), include_dashboard=False)
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        cluster.add_node(num_cpus=2, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, delay_s: float, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            if delay_s:
                time.sleep(delay_s)
            return int(i).to_bytes(8, "little", signed=True) + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
        class Consumer:
            def hold(self, wrapped):
                return wrapped[0].hex()

            def ping(self):
                return True

        @ray.remote(max_retries=0)
        def warm_produce():
            return b"w"

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        # Recovery-ineligible warmup preserves exact K32 group boundaries.
        warm_ref = warm_produce.options(scheduling_strategy=strategy, num_cpus=1).remote()
        ray.get(consumer.hold.remote([warm_ref]))
        ray.get(warm_ref)
        time.sleep(0.02)

        for rep in range(REPETITIONS):
            refs = [
                produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                    i, PENDING_DELAY_S, PAYLOAD_BYTES, padding
                )
                for i in range(N)
            ]
            keepalive.extend(refs)

            reset_profile()
            cold_call, cold_us, cold_ser_us = measure_one(consumer, refs[0])

            # Snapshot immediately after .remote() returns.  Synchronous manager
            # counters are complete at this point; publication may still be finishing.
            cold_profile = profile()
            wait_for_ack()
            ray.get(cold_call)

            # Same producer/group after commitment gives the first-call baseline
            # without any first-activation work.
            reset_profile()
            pre_call, pre_us, pre_ser_us = measure_one(consumer, refs[0])
            ray.get(pre_call)
            pre_profile = profile()
            assert int(pre_profile.get("witness_update_rpcs_sent", 0)) == 0, pre_profile

            ensure_us = total_us(cold_profile, "ensure_task_arguments_time_ns")
            initial_manifest_us = total_us(cold_profile, "initial_manifest_build_time_ns")
            witness_selection_us = total_us(cold_profile, "witness_selection_time_ns")
            witness_gcs_us = total_us(cold_profile, "witness_gcs_query_time_ns")
            register_owned_us = total_us(cold_profile, "register_owned_task_time_ns")
            arg_metadata_us = total_us(cold_profile, "task_argument_metadata_time_ns")
            recovery_lookup_us = total_us(cold_profile, "recovery_metadata_lookup_time_ns")

            # These three are subcomponents of EnsureRecoverySuccessionForTaskArguments.
            known_inside_ensure = initial_manifest_us + witness_selection_us + register_owned_us

            rows.append(
                {
                    "cold_us": cold_us,
                    "cold_ser_us": cold_ser_us,
                    "pre_us": pre_us,
                    "pre_ser_us": pre_ser_us,
                    "cold_delta_us": cold_us - pre_us,
                    "ensure_us": ensure_us,
                    "initial_manifest_us": initial_manifest_us,
                    "witness_selection_us": witness_selection_us,
                    "witness_gcs_us": witness_gcs_us,
                    "register_owned_us": register_owned_us,
                    "arg_metadata_us": arg_metadata_us,
                    "recovery_lookup_us": recovery_lookup_us,
                    "other_inside_ensure_us": ensure_us - known_inside_ensure,
                }
            )

            ray.get(refs)
            time.sleep(0.02)

        print("\nRecovery Frontier K32 first-activation profile")
        print(f"  R={R}, K={K}, repetitions={REPETITIONS}, producer_state=pending")
        print(f"  cold first .remote()              {mean(rows,'cold_us'):9.2f} us")
        print(f"    Python serialization            {mean(rows,'cold_ser_us'):9.2f} us")
        print(f"  committed first .remote()         {mean(rows,'pre_us'):9.2f} us")
        print(f"    Python serialization            {mean(rows,'pre_ser_us'):9.2f} us")
        print(f"  cold first-activation delta       {mean(rows,'cold_delta_us'):9.2f} us/group")
        print(f"  amortized delta at K32            {mean(rows,'cold_delta_us')/K:9.2f} us/task")

        print("\nExisting C++ profile counters on the cold first call:")
        print(f"  EnsureRecoverySuccessionForArgs   {mean(rows,'ensure_us'):9.2f} us")
        print("    nested subcomponents (do not add Ensure again):")
        print(f"      initial manifest build        {mean(rows,'initial_manifest_us'):9.2f} us")
        print(f"      witness selection             {mean(rows,'witness_selection_us'):9.2f} us")
        print(f"      witness GCS query             {mean(rows,'witness_gcs_us'):9.2f} us")
        print(f"      RegisterOwnedTaskLazy         {mean(rows,'register_owned_us'):9.2f} us")
        print(f"      other inside Ensure           {mean(rows,'other_inside_ensure_us'):9.2f} us")
        print(f"  task argument metadata            {mean(rows,'arg_metadata_us'):9.2f} us")
        print(f"  recovery metadata lookup          {mean(rows,'recovery_lookup_us'):9.2f} us")

        delta = mean(rows, "cold_delta_us")
        ensure = mean(rows, "ensure_us")
        argmeta = mean(rows, "arg_metadata_us")
        print("\nDecision:")
        if delta > 0:
            print(f"  Ensure accounts for ~{100.0*ensure/delta:5.1f}% of cold delta")
            print(f"  task-argument metadata accounts for ~{100.0*argmeta/delta:5.1f}% of cold delta")
        print("  If witness/manifest dominates -> prewarm group topology.")
        print("  If RegisterOwnedTaskLazy dominates -> preinitialize task-centric state at producer registration.")
        print("  If other-inside-Ensure dominates -> profile GetTaskSpec/membership/cache/metadata lookups next.")
        print("  If argument metadata dominates -> optimize deferred sidecar construction.")

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
