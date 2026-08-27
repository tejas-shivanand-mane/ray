#!/usr/bin/env python3
"""Diagnostic 43: decisive Recovery Frontier cold-path decomposition.

This benchmark is intentionally narrow. It explains the remaining K=32 cold
submission penalty by separating, in the same Ray process/cluster:

  1. precommitted/base submission cost,
  2. synchronous cost paid by the first cold borrower that kicks off one K32
     Frontier publication, and
  3. interference paid by later submissions while that publication is in flight.

It additionally wraps the driver's real SerializationContext.serialize() method,
so nested-[ObjectRef] Python serialization is measured rather than inferred.

For K32 the split case does:
  - time borrower call #1,
  - wait until exactly R witness updates for the one full group have ACKed,
  - time calls #2..#32.

Therefore, ignoring the deliberately excluded wait:

  split_weighted = (first_call + 31 * clean_rest_mean) / 32
  kickoff_tax    = split_weighted - precommitted
  overlap_tax    = cold_burst - split_weighted

The two taxes add back to the measured cold-vs-precommitted gap (up to noise).
That tells us whether the final optimization belongs in synchronous append
construction/serialization or in concurrent publication/RPC callback isolation.
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
    disabled,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
)

R = 2
K = 32
N = 32
REPETITIONS = 3
PENDING_DELAY_S = 0.05
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024

VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k32", "recovery", K),
]
STATES = ["pending", "finished"]


@dataclass
class SerializationStats:
    calls: int = 0
    time_ns: int = 0


class TimedSerializer:
    """Temporarily wrap the driver's actual SerializationContext.serialize."""

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

        # SerializationContext is a normal Python class, so an instance-level
        # wrapper safely shadows the bound method for this diagnostic interval.
        self.ctx.serialize = timed
        return stats

    def __exit__(self, exc_type, exc, tb):
        assert self.original is not None
        self.ctx.serialize = self.original


def config_for(mode: str, k: int | None) -> dict[str, Any]:
    if mode == "disabled":
        cfg = system_config(disabled(), witness_count=R, profiling_enabled=True)
        cfg.update(
            {
                "enable_recovery_frontier": False,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
            }
        )
        return cfg

    cfg = system_config(witness_baseline(R), witness_count=R, profiling_enabled=True)
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


def reset_profile() -> None:
    try:
        global_worker.core_worker.reset_recovery_succession_profile()
    except Exception:
        pass


def wait_updates(expected: int, timeout_s: float = 5.0) -> dict[str, Any]:
    if expected == 0:
        return profile()
    deadline = time.monotonic() + timeout_s
    last = profile()
    while time.monotonic() < deadline:
        sent = int(last.get("witness_update_rpcs_sent", 0))
        done = int(last.get("witness_update_rpcs_completed", 0))
        if sent >= expected and done >= expected:
            return last
        time.sleep(0.001)
        last = profile()
    raise AssertionError(
        f"timed out waiting for witness updates: expected={expected}, profile={last}"
    )


def expected_updates(recovery_mode: str, k: int | None) -> int:
    if recovery_mode == "disabled":
        return 0
    groups = N if k is None or k == 1 else (N + int(k) - 1) // int(k)
    return groups * R


def publish_us_per_group(p: dict[str, Any]) -> float:
    count = int(p.get("witness_publish_count", 0))
    ns = int(p.get("witness_publish_time_ns", 0))
    return ns / count / 1e3 if count else 0.0


def rpc_us_per_update(p: dict[str, Any]) -> float:
    count = int(p.get("witness_update_rpcs_completed", 0))
    ns = int(p.get("witness_update_rpc_time_ns", 0))
    return ns / count / 1e3 if count else 0.0


def make_refs(produce, strategy, state: str, padding: bytes):
    delay = PENDING_DELAY_S if state == "pending" else 0.0
    refs = [
        produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
            i, delay, PAYLOAD_BYTES, padding
        )
        for i in range(N)
    ]
    if state == "finished":
        values = ray.get(refs)
        assert len(values) == N
    return refs


def drain_refs(refs):
    values = ray.get(refs)
    assert len(values) == N


def timed_submit_all(consumer, refs):
    with TimedSerializer() as ser:
        start = time.perf_counter_ns()
        calls = [consumer.hold.remote([ref]) for ref in refs]
        elapsed = time.perf_counter_ns() - start
    return calls, elapsed, ser


def precommit_group(consumer, refs, expected: int):
    """Activate protection before the timed interval, then fully settle it."""
    reset_profile()
    warm = [consumer.hold.remote([ref]) for ref in refs]
    ray.get(warm)
    p = wait_updates(expected)
    # Ensure the async callback/manager side has quiesced before resetting.
    time.sleep(0.01)
    return p


def run_burst_case(consumer, refs, expected: int) -> dict[str, float]:
    reset_profile()
    calls, elapsed_ns, ser = timed_submit_all(consumer, refs)
    ray.get(calls)
    p = wait_updates(expected)
    return {
        "submit_us_task": elapsed_ns / N / 1e3,
        "serialize_us_task": ser.time_ns / N / 1e3,
        "serialize_calls_task": ser.calls / N,
        "publish_us_group": publish_us_per_group(p),
        "rpc_us_update": rpc_us_per_update(p),
    }


def run_precommitted_case(consumer, refs, expected: int) -> dict[str, float]:
    precommit_group(consumer, refs, expected)
    reset_profile()
    calls, elapsed_ns, ser = timed_submit_all(consumer, refs)
    ray.get(calls)
    p = profile()
    assert int(p.get("witness_update_rpcs_sent", 0)) == 0, p
    return {
        "submit_us_task": elapsed_ns / N / 1e3,
        "serialize_us_task": ser.time_ns / N / 1e3,
        "serialize_calls_task": ser.calls / N,
        "publish_us_group": 0.0,
        "rpc_us_update": 0.0,
    }


def run_split_case(consumer, refs, expected_first: int) -> dict[str, float]:
    """For K32, isolate first-call kickoff from in-flight interference."""
    reset_profile()

    with TimedSerializer() as first_ser:
        first_start = time.perf_counter_ns()
        first_call = consumer.hold.remote([refs[0]])
        first_ns = time.perf_counter_ns() - first_start

    # For one full K32 group, the first borrower kicks off exactly R physical
    # witness writes for the entire group. Exclude the wait itself from the
    # submission timings; it deliberately removes publication overlap before
    # measuring the remaining 31 calls.
    p_after_publish = wait_updates(expected_first)

    with TimedSerializer() as rest_ser:
        rest_start = time.perf_counter_ns()
        rest_calls = [consumer.hold.remote([ref]) for ref in refs[1:]]
        rest_ns = time.perf_counter_ns() - rest_start

    ray.get([first_call] + rest_calls)
    return {
        "first_us": first_ns / 1e3,
        "first_serialize_us": first_ser.time_ns / 1e3,
        "first_serialize_calls": float(first_ser.calls),
        "rest_us_task": rest_ns / (N - 1) / 1e3,
        "rest_serialize_us_task": rest_ser.time_ns / (N - 1) / 1e3,
        "rest_serialize_calls_task": rest_ser.calls / (N - 1),
        "publish_us_group": publish_us_per_group(p_after_publish),
        "rpc_us_update": rpc_us_per_update(p_after_publish),
    }


def run_variant(label: str, recovery_mode: str, k: int | None):
    cluster = None
    rows = []
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
            if delay_s:
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
        expected = expected_updates(recovery_mode, k)

        # Warm generic task/actor machinery before measurements.
        warm_ref = produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
            -1, 0.0, PAYLOAD_BYTES, padding
        )
        ray.get(consumer.hold.remote([warm_ref]))
        time.sleep(0.02)

        for rep in range(REPETITIONS):
            for state in STATES:
                # Fresh full groups for each mode; never reuse a cold group across
                # burst/split/precommitted measurements.
                refs = make_refs(produce, strategy, state, padding)
                burst = run_burst_case(consumer, refs, expected)
                drain_refs(refs)
                rows.append((rep, state, "burst", burst))

                refs = make_refs(produce, strategy, state, padding)
                pre = run_precommitted_case(consumer, refs, expected)
                drain_refs(refs)
                rows.append((rep, state, "precommitted", pre))

                if label in ("disabled", "frontier_k32"):
                    refs = make_refs(produce, strategy, state, padding)
                    first_expected = 0 if label == "disabled" else R
                    split = run_split_case(consumer, refs, first_expected)
                    drain_refs(refs)
                    rows.append((rep, state, "split", split))

                time.sleep(0.02)

        return rows
    finally:
        safe_shutdown(ray, cluster)


def mean_rows(rows, state: str, mode: str, key: str) -> float:
    vals = [r[3][key] for r in rows if r[1] == state and r[2] == mode]
    return statistics.fmean(vals)


def main() -> None:
    all_rows = {}
    for label, recovery_mode, k in VARIANTS:
        print(f"\n=== {label} ===", flush=True)
        all_rows[label] = run_variant(label, recovery_mode, k)

    print("\nDecisive cold-path profile (3 paired repetitions):")
    print(
        "  variant        state     mode          submit_us/task  serialize_us/task  publish_us/group  rpc_us/update"
    )
    for label, _, _ in VARIANTS:
        rows = all_rows[label]
        for state in STATES:
            for mode in ("burst", "precommitted"):
                print(
                    f"  {label:<14} {state:<9} {mode:<13} "
                    f"{mean_rows(rows,state,mode,'submit_us_task'):>14.2f}  "
                    f"{mean_rows(rows,state,mode,'serialize_us_task'):>17.2f}  "
                    f"{mean_rows(rows,state,mode,'publish_us_group'):>16.1f}  "
                    f"{mean_rows(rows,state,mode,'rpc_us_update'):>13.1f}"
                )

    print("\nK32 split decomposition (wait for group ACK excluded from split timing):")
    krows = all_rows["frontier_k32"]
    drows = all_rows["disabled"]
    for state in STATES:
        cold = mean_rows(krows, state, "burst", "submit_us_task")
        pre = mean_rows(krows, state, "precommitted", "submit_us_task")
        first = mean_rows(krows, state, "split", "first_us")
        first_ser = mean_rows(krows, state, "split", "first_serialize_us")
        rest = mean_rows(krows, state, "split", "rest_us_task")
        rest_ser = mean_rows(krows, state, "split", "rest_serialize_us_task")
        split_weighted = (first + (N - 1) * rest) / N
        kickoff_tax = split_weighted - pre
        overlap_tax = cold - split_weighted
        cold_gap = cold - pre

        d_cold = mean_rows(drows, state, "burst", "submit_us_task")
        d_pre = mean_rows(drows, state, "precommitted", "submit_us_task")
        control_shift = d_cold - d_pre
        net_k32_gap = cold_gap - control_shift

        print(f"\n  state={state}")
        print(f"    cold burst                    {cold:9.2f} us/task")
        print(f"    precommitted                  {pre:9.2f} us/task")
        print(f"    first cold call               {first:9.2f} us")
        print(f"      Python serialization        {first_ser:9.2f} us")
        print(f"    calls 2..32 after ACK         {rest:9.2f} us/task")
        print(f"      Python serialization        {rest_ser:9.2f} us/task")
        print(f"    split weighted (no ACK wait)  {split_weighted:9.2f} us/task")
        print(f"    synchronous kickoff tax       {kickoff_tax:9.2f} us/task")
        print(f"    in-flight overlap tax         {overlap_tax:9.2f} us/task")
        print(f"    total cold-precommit gap      {cold_gap:9.2f} us/task")
        print(f"    disabled cold-precommit shift {control_shift:9.2f} us/task")
        print(f"    net K32 cold-path gap         {net_k32_gap:9.2f} us/task")

    print("\nDecision rule:")
    print("  - large synchronous kickoff tax -> move StageAppend/envelope construction off caller thread")
    print("  - large in-flight overlap tax   -> isolate publication/RPC callback work from submission path")
    print("  - large Python serialization    -> fix nested-ObjectRef serializer path")
    print("  - small taxes but large gap     -> profile only the remaining residual boundary")


if __name__ == "__main__":
    main()
