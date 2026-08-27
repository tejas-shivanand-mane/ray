#!/usr/bin/env python3
"""Benchmark 47: decisively locate the K32 first-activation actor-submit tax.

Benchmark 46 used cProfile, but python/ray/_raylet.pyx is compiled with
``# cython: profile=False``.  cProfile therefore charges time spent in the
Cython/native ``submit_actor_task`` call to ActorHandle._actor_method_call and
cannot identify the native boundary.

This benchmark uses two independent techniques that require no Ray rebuild:

1. Temporarily replace ``global_worker.core_worker`` with a transparent Python
   proxy for the duration of one actor .remote() call.  The proxy wall-times the
   real Cython ``submit_actor_task`` method explicitly.
2. Compare a nested ``[ObjectRef]`` argument (the Benchmark-30 workload) with a
   direct ``ObjectRef`` argument.  Direct ObjectRefs are passed by reference and
   avoid the by-value list serialization path.  If the cold delta remains for
   the direct case, Python/by-value serialization cannot explain it.

The benchmark measures only the first cold export of a full K32 group and then
repeats the same call after that group is committed.
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


@dataclass
class NativeStats:
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


class TimedCoreWorkerProxy:
    """Transparent proxy that times only the real Cython submit_actor_task call."""

    def __init__(self, target):
        self._target = target
        self.stats = NativeStats()

    def __getattr__(self, name):
        return getattr(self._target, name)

    def submit_actor_task(self, *args, **kwargs):
        start = time.perf_counter_ns()
        try:
            return self._target.submit_actor_task(*args, **kwargs)
        finally:
            self.stats.calls += 1
            self.stats.time_ns += time.perf_counter_ns() - start


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


def measure_call(fn: Callable[[], Any]):
    # Obtain the serialization context before replacing core_worker so this
    # diagnostic never changes how the context itself is looked up.
    serializer = TimedSerializer()
    original_core_worker = global_worker.core_worker
    proxy = TimedCoreWorkerProxy(original_core_worker)

    with serializer as ser:
        global_worker.core_worker = proxy
        try:
            start = time.perf_counter_ns()
            call = fn()
            wall_ns = time.perf_counter_ns() - start
        finally:
            global_worker.core_worker = original_core_worker

    assert proxy.stats.calls == 1, proxy.stats
    return call, wall_ns / 1e3, proxy.stats.time_ns / 1e3, ser.time_ns / 1e3


def mean(rows: list[dict[str, float]], key: str) -> float:
    return statistics.fmean(row[key] for row in rows)


def run_mode(
    *,
    mode: str,
    produce,
    consumer,
    strategy,
    padding: bytes,
    keepalive: list[Any],
) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []

    for rep in range(REPETITIONS):
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                rep * 1000 + i,
                PENDING_DELAY_S,
                PAYLOAD_BYTES,
                padding,
            )
            for i in range(N)
        ]
        keepalive.extend(refs)

        if mode == "nested":
            make_call = lambda: consumer.hold_nested.remote([refs[0]])
        elif mode == "direct":
            make_call = lambda: consumer.hold_direct.remote(refs[0])
        else:
            raise ValueError(mode)

        reset_profile()
        cold_call, cold_wall_us, cold_native_us, cold_ser_us = measure_call(make_call)
        cold_profile = profile()
        wait_for_ack()
        ray.get(cold_call)

        reset_profile()
        pre_call, pre_wall_us, pre_native_us, pre_ser_us = measure_call(make_call)
        ray.get(pre_call)
        pre_profile = profile()
        assert int(pre_profile.get("witness_update_rpcs_sent", 0)) == 0, pre_profile

        rows.append(
            {
                "cold_wall_us": cold_wall_us,
                "pre_wall_us": pre_wall_us,
                "wall_delta_us": cold_wall_us - pre_wall_us,
                "cold_native_us": cold_native_us,
                "pre_native_us": pre_native_us,
                "native_delta_us": cold_native_us - pre_native_us,
                "cold_ser_us": cold_ser_us,
                "pre_ser_us": pre_ser_us,
                "ser_delta_us": cold_ser_us - pre_ser_us,
                "cold_rpcs_sent": float(cold_profile.get("witness_update_rpcs_sent", 0)),
                "cold_rpcs_done_at_return": float(
                    cold_profile.get("witness_update_rpcs_completed", 0)
                ),
            }
        )

        ray.get(refs)
        time.sleep(0.02)

    return rows


def print_mode(mode: str, rows: list[dict[str, float]]) -> None:
    wall_delta = mean(rows, "wall_delta_us")
    native_delta = mean(rows, "native_delta_us")
    ser_delta = mean(rows, "ser_delta_us")

    print(f"\n  mode={mode}")
    print(f"    cold .remote() wall              {mean(rows,'cold_wall_us'):9.2f} us")
    print(f"    committed .remote() wall         {mean(rows,'pre_wall_us'):9.2f} us")
    print(f"    cold wall delta                  {wall_delta:9.2f} us/group")
    print(f"    amortized wall delta @K32        {wall_delta/K:9.2f} us/task")
    print()
    print(f"    Cython submit_actor_task cold    {mean(rows,'cold_native_us'):9.2f} us")
    print(f"    Cython submit_actor_task pre     {mean(rows,'pre_native_us'):9.2f} us")
    print(f"    Cython/native boundary delta     {native_delta:9.2f} us/group")
    if wall_delta > 0:
        print(
            f"    fraction wall delta in boundary  {100.0*native_delta/wall_delta:9.1f}%"
        )
    print()
    print(f"    Python serialization cold        {mean(rows,'cold_ser_us'):9.2f} us")
    print(f"    Python serialization pre         {mean(rows,'pre_ser_us'):9.2f} us")
    print(f"    serialization delta              {ser_delta:9.2f} us/group")
    print(
        f"    boundary delta minus serialization {native_delta-ser_delta:7.2f} us/group"
    )
    print(
        f"    witness ACKs complete at return  {mean(rows,'cold_rpcs_done_at_return'):5.2f}/{R} mean"
    )


def main() -> None:
    cluster = None
    keepalive: list[Any] = []
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
            return int(i).to_bytes(8, "little", signed=True) + b"x" * max(
                0, payload_bytes - 8
            )

        @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
        class Consumer:
            def hold_nested(self, wrapped):
                # Nested refs intentionally remain refs inside the list.
                return wrapped[0].hex()

            def hold_direct(self, value):
                # Direct ObjectRef arguments are dependency-resolved by Ray and
                # arrive here as the produced bytes value.
                return len(value)

            def ping(self):
                return True

        @ray.remote(max_retries=0)
        def warm_produce():
            return b"w"

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        # Recovery-ineligible warmup does not consume a K32 membership slot.
        warm_ref = warm_produce.options(scheduling_strategy=strategy, num_cpus=1).remote()
        ray.get(consumer.hold_nested.remote([warm_ref]))
        ray.get(warm_ref)
        time.sleep(0.02)

        nested_rows = run_mode(
            mode="nested",
            produce=produce,
            consumer=consumer,
            strategy=strategy,
            padding=padding,
            keepalive=keepalive,
        )
        direct_rows = run_mode(
            mode="direct",
            produce=produce,
            consumer=consumer,
            strategy=strategy,
            padding=padding,
            keepalive=keepalive,
        )

        print("\nRecovery Frontier K32 explicit actor-submit boundary profile")
        print(f"  R={R}, K={K}, repetitions={REPETITIONS}, producer_state=pending")
        print_mode("nested [ObjectRef] (Benchmark-30 shape)", nested_rows)
        print_mode("direct ObjectRef (no by-value list serialization)", direct_rows)

        nested_native = mean(nested_rows, "native_delta_us")
        direct_native = mean(direct_rows, "native_delta_us")
        nested_wall = mean(nested_rows, "wall_delta_us")
        direct_wall = mean(direct_rows, "wall_delta_us")

        print("\nDecision:")
        print(
            "  If Cython/native boundary delta ~= wall delta in both modes, the missing tax is"
        )
        print(
            "  below ActorHandle._actor_method_call.  If the direct case remains large, by-value"
        )
        print(
            "  Python serialization is ruled out and CoreWorker::SubmitActorTask is the next target."
        )
        print(
            "  If nested is large but direct collapses, profile _raylet.pyx argument preparation."
        )
        if nested_wall > 0 and direct_wall > 0:
            print(
                f"  observed boundary fractions: nested={100*nested_native/nested_wall:.1f}% "
                f"direct={100*direct_native/direct_wall:.1f}%"
            )

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
