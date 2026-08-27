#!/usr/bin/env python3
"""Benchmark 44: isolate the remaining K32 publication-overlap boundary.

Benchmark 43 showed that moving StageAppend/envelope/RPC kickoff off the caller
thread removes most synchronous kickoff tax, but a large cold-path penalty
remains while the one K32 publication is active.

This benchmark uses the existing witness RPC counters to separate calls 2..32
into three timing windows:

  immediate   submit immediately after call #1 returns
  rpc_inflight wait until all R witness RPCs are SENT but not all completed,
               then submit calls 2..32 while the RPCs are in flight
  after_ack   wait until all R witness RPCs complete, then submit calls 2..32

A fourth precommitted case measures the steady committed-prefix baseline.

Interpretation:
  immediate >> rpc_inflight  => StageAppend/envelope/kickoff interference
  rpc_inflight >> after_ack  => true RPC/callback in-flight interference
  after_ack ~ precommitted   => no meaningful residual boundary after commit
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

from _benchmark_common import (
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
)

R = 2
K = 32
N = 32
REPETITIONS = 5
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
    try:
        global_worker.core_worker.reset_recovery_succession_profile()
    except Exception:
        pass


def rpc_counts() -> tuple[int, int]:
    p = profile()
    return (
        int(p.get("witness_update_rpcs_sent", 0)),
        int(p.get("witness_update_rpcs_completed", 0)),
    )


def wait_for_rpc_sent_before_ack(timeout_s: float = POLL_TIMEOUT_S) -> tuple[float, bool]:
    """Return (wait_us, caught_inflight_window)."""
    start = time.perf_counter_ns()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        sent, done = rpc_counts()
        if sent >= R:
            return (time.perf_counter_ns() - start) / 1e3, done < R
    raise AssertionError(f"timed out waiting for RPC send: profile={profile()}")


def wait_for_ack(timeout_s: float = POLL_TIMEOUT_S) -> float:
    start = time.perf_counter_ns()
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        sent, done = rpc_counts()
        if sent >= R and done >= R:
            return (time.perf_counter_ns() - start) / 1e3
    raise AssertionError(f"timed out waiting for ACK: profile={profile()}")


def make_refs(produce, strategy, padding: bytes):
    refs = [
        produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
            i, PENDING_DELAY_S, PAYLOAD_BYTES, padding
        )
        for i in range(N)
    ]
    return refs


def submit_one(consumer, ref):
    with TimedSerializer() as ser:
        start = time.perf_counter_ns()
        call = consumer.hold.remote([ref])
        elapsed = time.perf_counter_ns() - start
    return call, elapsed / 1e3, ser.time_ns / 1e3


def submit_rest(consumer, refs):
    with TimedSerializer() as ser:
        start = time.perf_counter_ns()
        calls = [consumer.hold.remote([ref]) for ref in refs]
        elapsed = time.perf_counter_ns() - start
    count = len(refs)
    return (
        calls,
        elapsed / count / 1e3,
        ser.time_ns / count / 1e3,
    )


def run_cold_case(consumer, refs, mode: str) -> dict[str, float]:
    reset_profile()
    first_call, first_us, first_ser_us = submit_one(consumer, refs[0])

    wait_sent_us = 0.0
    wait_ack_us = 0.0
    caught_inflight = 0.0

    if mode == "rpc_inflight":
        wait_sent_us, caught = wait_for_rpc_sent_before_ack()
        caught_inflight = 1.0 if caught else 0.0
    elif mode == "after_ack":
        wait_ack_us = wait_for_ack()
    elif mode != "immediate":
        raise ValueError(mode)

    rest_calls, rest_us_task, rest_ser_us_task = submit_rest(consumer, refs[1:])
    ray.get([first_call] + rest_calls)
    wait_for_ack()

    p = profile()
    return {
        "first_us": first_us,
        "first_serialize_us": first_ser_us,
        "rest_us_task": rest_us_task,
        "rest_serialize_us_task": rest_ser_us_task,
        "wait_sent_us": wait_sent_us,
        "wait_ack_us": wait_ack_us,
        "caught_inflight": caught_inflight,
        "publish_us_group": (
            int(p.get("witness_publish_time_ns", 0))
            / max(1, int(p.get("witness_publish_count", 0)))
            / 1e3
        ),
        "rpc_us_update": (
            int(p.get("witness_update_rpc_time_ns", 0))
            / max(1, int(p.get("witness_update_rpcs_completed", 0)))
            / 1e3
        ),
    }


def run_precommitted_case(consumer, refs) -> dict[str, float]:
    reset_profile()
    warm = [consumer.hold.remote([ref]) for ref in refs]
    ray.get(warm)
    wait_for_ack()

    reset_profile()
    first_call, first_us, first_ser_us = submit_one(consumer, refs[0])
    rest_calls, rest_us_task, rest_ser_us_task = submit_rest(consumer, refs[1:])
    ray.get([first_call] + rest_calls)

    p = profile()
    assert int(p.get("witness_update_rpcs_sent", 0)) == 0, p
    return {
        "first_us": first_us,
        "first_serialize_us": first_ser_us,
        "rest_us_task": rest_us_task,
        "rest_serialize_us_task": rest_ser_us_task,
        "wait_sent_us": 0.0,
        "wait_ack_us": 0.0,
        "caught_inflight": 0.0,
        "publish_us_group": 0.0,
        "rpc_us_update": 0.0,
    }


def mean(rows, mode: str, key: str) -> float:
    vals = [r[key] for m, r in rows if m == mode]
    return statistics.fmean(vals)


def main() -> None:
    cluster = None
    keepalive = []
    rows: list[tuple[str, dict[str, float]]] = []
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

        consumer = Consumer.options(resources={"consumer_node": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        # Recovery-ineligible warmup, so measured K32 group boundaries stay exact.
        @ray.remote(max_retries=0)
        def warm_produce():
            return b"w"

        warm_ref = warm_produce.options(scheduling_strategy=strategy, num_cpus=1).remote()
        ray.get(consumer.hold.remote([warm_ref]))
        ray.get(warm_ref)
        time.sleep(0.02)

        modes = ("immediate", "rpc_inflight", "after_ack", "precommitted")
        for rep in range(REPETITIONS):
            for mode in modes:
                refs = make_refs(produce, strategy, padding)
                keepalive.extend(refs)
                if mode == "precommitted":
                    row = run_precommitted_case(consumer, refs)
                else:
                    row = run_cold_case(consumer, refs, mode)
                rows.append((mode, row))
                ray.get(refs)
                time.sleep(0.02)

        print("\nRecovery Frontier K32 RPC-overlap boundary")
        print(f"  R={R}, K={K}, N={N}, repetitions={REPETITIONS}, state=pending")
        print(
            "  mode           first_us  rest_us/task  rest_ser_us/task  wait_to_boundary_us  publish_us/group  rpc_us/update"
        )
        for mode in modes:
            wait_key = "wait_sent_us" if mode == "rpc_inflight" else "wait_ack_us"
            print(
                f"  {mode:<14}"
                f" {mean(rows,mode,'first_us'):>8.2f}"
                f" {mean(rows,mode,'rest_us_task'):>13.2f}"
                f" {mean(rows,mode,'rest_serialize_us_task'):>17.2f}"
                f" {mean(rows,mode,wait_key):>20.2f}"
                f" {mean(rows,mode,'publish_us_group'):>17.1f}"
                f" {mean(rows,mode,'rpc_us_update'):>14.1f}"
            )

        caught = sum(r["caught_inflight"] for m, r in rows if m == "rpc_inflight")
        total = sum(1 for m, _ in rows if m == "rpc_inflight")

        immediate = mean(rows, "immediate", "rest_us_task")
        inflight = mean(rows, "rpc_inflight", "rest_us_task")
        after_ack = mean(rows, "after_ack", "rest_us_task")
        pre = mean(rows, "precommitted", "rest_us_task")

        print("\nBoundary decomposition (calls 2..32 only):")
        print(f"  caught RPC-in-flight window       {int(caught)}/{total} repetitions")
        print(f"  immediate rest                    {immediate:9.2f} us/task")
        print(f"  after RPC sent, before ACK        {inflight:9.2f} us/task")
        print(f"  after ACK                         {after_ack:9.2f} us/task")
        print(f"  precommitted                      {pre:9.2f} us/task")
        print(f"  pre-RPC kickoff/staging component {immediate - inflight:9.2f} us/task")
        print(f"  true in-flight RPC component      {inflight - after_ack:9.2f} us/task")
        print(f"  post-ACK residual                 {after_ack - pre:9.2f} us/task")

        print("\nDecision rule:")
        print("  large pre-RPC component  -> optimize StageAppend/envelope/kickoff")
        print("  large in-flight component -> move publication earlier or isolate RPC/callback execution")
        print("  large post-ACK residual   -> inspect deferred dispatch / metadata boundary")
        if caught < total:
            print("  NOTE: some rpc_inflight repetitions missed the short sent-before-ACK window;")
            print("        rerun once before drawing a strong conclusion from that row.")

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
