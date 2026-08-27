#!/usr/bin/env python3
"""Benchmark 46: locate the K32 first-cold actor .remote() stall.

Benchmark 45 showed ~1-2 ms of cold-vs-committed latency that is not explained
by existing Recovery Succession C++ counters. Benchmark 30 uses the same actor
consumer path, so this diagnostic profiles only the Python actor .remote() call
and separates time attributed by cProfile to the native CoreWorker
submit_actor_task boundary from Python-side wrapper work.

No C++ rebuild is required.
"""
from __future__ import annotations

import cProfile
import os
import pstats
import statistics
import time
from collections import defaultdict
from typing import Any

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


def recovery_profile() -> dict[str, Any]:
    try:
        return global_worker.core_worker.get_recovery_succession_profile()
    except Exception:
        return {}


def reset_recovery_profile() -> None:
    global_worker.core_worker.reset_recovery_succession_profile()


def wait_for_ack(timeout_s: float = POLL_TIMEOUT_S) -> None:
    deadline = time.monotonic() + timeout_s
    last = recovery_profile()
    while time.monotonic() < deadline:
        sent = int(last.get("witness_update_rpcs_sent", 0))
        done = int(last.get("witness_update_rpcs_completed", 0))
        if sent >= R and done >= R:
            return
        time.sleep(0.0001)
        last = recovery_profile()
    raise AssertionError(f"timed out waiting for K32 ACKs: {last}")


def profile_remote(consumer, ref):
    profiler = cProfile.Profile()
    start_ns = time.perf_counter_ns()
    profiler.enable()
    call = consumer.hold.remote([ref])
    profiler.disable()
    wall_us = (time.perf_counter_ns() - start_ns) / 1e3

    stats = pstats.Stats(profiler).stats
    by_name: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0])
    # [call_count, self_seconds, cumulative_seconds]
    for key, value in stats.items():
        cc, nc, tt, ct, callers = value
        del cc, callers
        name = str(key[2])
        bucket = by_name[name]
        bucket[0] += float(nc)
        bucket[1] += float(tt)
        bucket[2] += float(ct)

    native_self_us = sum(
        v[1] * 1e6 for name, v in by_name.items() if "submit_actor_task" in name
    )
    native_cum_us = sum(
        v[2] * 1e6 for name, v in by_name.items() if "submit_actor_task" in name
    )

    return call, wall_us, native_self_us, native_cum_us, by_name


def mean(rows: list[dict[str, Any]], key: str) -> float:
    return statistics.fmean(float(row[key]) for row in rows)


def aggregate_function_deltas(rows: list[dict[str, Any]]) -> list[tuple[float, str, float, float]]:
    names = set()
    for row in rows:
        names.update(row["cold_functions"].keys())
        names.update(row["pre_functions"].keys())

    ranked = []
    for name in names:
        cold_vals = []
        pre_vals = []
        for row in rows:
            cold_vals.append(row["cold_functions"].get(name, [0.0, 0.0, 0.0])[1] * 1e6)
            pre_vals.append(row["pre_functions"].get(name, [0.0, 0.0, 0.0])[1] * 1e6)
        cold = statistics.fmean(cold_vals)
        pre = statistics.fmean(pre_vals)
        ranked.append((cold - pre, name, cold, pre))
    ranked.sort(reverse=True)
    return ranked


def main() -> None:
    cluster = None
    keepalive = []
    rows: list[dict[str, Any]] = []
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

        # Recovery-ineligible warmup preserves exact K32 boundaries.
        warm_ref = warm_produce.options(scheduling_strategy=strategy, num_cpus=1).remote()
        ray.get(consumer.hold.remote([warm_ref]))
        ray.get(warm_ref)
        time.sleep(0.02)

        for rep in range(REPETITIONS):
            refs = [
                produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                    rep * N + i, PENDING_DELAY_S, PAYLOAD_BYTES, padding
                )
                for i in range(N)
            ]
            keepalive.extend(refs)

            reset_recovery_profile()
            cold_call, cold_wall, cold_native_self, cold_native_cum, cold_funcs = profile_remote(
                consumer, refs[0]
            )
            wait_for_ack()
            ray.get(cold_call)

            reset_recovery_profile()
            pre_call, pre_wall, pre_native_self, pre_native_cum, pre_funcs = profile_remote(
                consumer, refs[0]
            )
            ray.get(pre_call)
            p = recovery_profile()
            assert int(p.get("witness_update_rpcs_sent", 0)) == 0, p

            rows.append(
                {
                    "cold_wall_us": cold_wall,
                    "pre_wall_us": pre_wall,
                    "wall_delta_us": cold_wall - pre_wall,
                    "cold_native_self_us": cold_native_self,
                    "pre_native_self_us": pre_native_self,
                    "native_self_delta_us": cold_native_self - pre_native_self,
                    "cold_native_cum_us": cold_native_cum,
                    "pre_native_cum_us": pre_native_cum,
                    "cold_functions": cold_funcs,
                    "pre_functions": pre_funcs,
                }
            )

            ray.get(refs)
            time.sleep(0.02)

        wall_delta = mean(rows, "wall_delta_us")
        native_delta = mean(rows, "native_self_delta_us")

        print("\nRecovery Frontier K32 actor native-boundary profile")
        print(f"  R={R}, K={K}, repetitions={REPETITIONS}, producer_state=pending")
        print(f"  cold .remote() wall                {mean(rows,'cold_wall_us'):9.2f} us")
        print(f"  committed .remote() wall           {mean(rows,'pre_wall_us'):9.2f} us")
        print(f"  cold wall delta                    {wall_delta:9.2f} us/group")
        print(f"  amortized wall delta @K32          {wall_delta / K:9.2f} us/task")
        print()
        print(f"  native submit_actor_task self cold {mean(rows,'cold_native_self_us'):9.2f} us")
        print(f"  native submit_actor_task self pre  {mean(rows,'pre_native_self_us'):9.2f} us")
        print(f"  native submit_actor_task delta     {native_delta:9.2f} us/group")
        print(f"  native cumulative cold             {mean(rows,'cold_native_cum_us'):9.2f} us")
        print(f"  native cumulative pre              {mean(rows,'pre_native_cum_us'):9.2f} us")
        if wall_delta > 0:
            print(f"  fraction of cold wall delta at native boundary {100.0 * native_delta / wall_delta:7.1f}%")

        print("\nLargest cold-minus-committed cProfile SELF-time deltas:")
        for delta, name, cold, pre in aggregate_function_deltas(rows)[:12]:
            print(f"  {delta:9.2f} us  cold={cold:9.2f} pre={pre:9.2f}  {name}")

        print("\nDecision:")
        print("  native delta ~ wall delta -> instrument CoreWorker::SubmitActorTask stages next")
        print("  native delta small         -> optimize/profile Python actor wrapper instead")
        print("  Note: cProfile perturbs absolute latency; use the location of the differential,")
        print("        not these absolute times, as the decision signal.")

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
