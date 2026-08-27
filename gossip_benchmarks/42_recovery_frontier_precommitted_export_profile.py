#!/usr/bin/env python3
"""Diagnostic 42: isolate publication interference from steady export cost.

Diagnostic 41 showed that Frontier K32 adds ~75 us/task externally but only a few
microseconds inside CoreWorker::SubmitTask.  This test distinguishes two causes:

  * cold: the timed downstream submissions perform the first export and therefore
    overlap with Frontier/Fixed-R witness publication.
  * precommitted: every producer ref is exported once before timing and protection
    is allowed to finish.  The profile is then reset and the same refs are exported
    again during the timed section.  No witness update should occur while timed.

If the large external K32 penalty disappears in precommitted mode, the "unaccounted"
time in diagnostic 41 is publication/event-loop/lock contention between successive
.remote() calls, not Python/Cython recovery serialization.
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
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
REPETITIONS = 3

VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k32", "recovery", 32),
]
MODES = ["cold", "precommitted"]


def config_for(mode: str, k: int | None) -> dict[str, Any]:
    if mode == "disabled":
        cfg = system_config(disabled(), witness_count=R, profiling_enabled=True)
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


def wait_for_updates_complete(timeout_s: float = 5.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    p = profile()
    while time.monotonic() < deadline:
        sent = int(p.get("witness_update_rpcs_sent", 0))
        done = int(p.get("witness_update_rpcs_completed", 0))
        if sent == done:
            return p
        time.sleep(0.01)
        p = profile()
    return p


def run_case(label: str, recovery_mode: str, k: int | None, mode: str) -> dict[str, float]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=config_for(recovery_mode, k),
            include_dashboard=False,
        )
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        consumer_node = cluster.add_node(num_cpus=4, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            return int(i).to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_retries=0)
        def hold(wrapped):
            ref = wrapped[0]
            return ref.hex()

        producer_strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node.node_id, soft=False
        )
        consumer_strategy = NodeAffinitySchedulingStrategy(
            node_id=consumer_node.node_id, soft=False
        )
        padding = b"p" * PADDING_BYTES

        refs = [
            produce.options(scheduling_strategy=producer_strategy, num_cpus=1).remote(
                i, PAYLOAD_BYTES, padding
            )
            for i in range(N)
        ]
        assert len(ray.get(refs)) == N

        if mode == "precommitted":
            # Export every producer ref once before the timed section.  For K32 this
            # commits one shared group; for Fixed-R it commits all 32 individual tasks.
            reset_profile()
            prime_calls = [
                hold.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote([ref])
                for ref in refs
            ]
            assert ray.get(prime_calls) == [ref.hex() for ref in refs]
            p_prime = wait_for_updates_complete()
            if recovery_mode == "recovery":
                expected_prime_updates = (N * R) if k is None else R
                assert int(p_prime.get("witness_update_rpcs_sent", 0)) == expected_prime_updates, p_prime
                assert int(p_prime.get("witness_update_rpcs_completed", 0)) == expected_prime_updates, p_prime
        elif mode != "cold":
            raise ValueError(mode)

        reset_profile()

        start_ns = time.perf_counter_ns()
        calls = [
            hold.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote([ref])
            for ref in refs
        ]
        external_ns = time.perf_counter_ns() - start_ns

        assert ray.get(calls) == [ref.hex() for ref in refs]
        p = wait_for_updates_complete()

        prof_calls = int(p.get("normal_submit_profile_calls", 0))
        assert prof_calls == N, (label, mode, prof_calls, p)

        updates = int(p.get("witness_update_rpcs_sent", 0))
        if recovery_mode == "disabled" or mode == "precommitted":
            assert updates == 0, p
        elif k is None:
            assert updates == N * R, p
        else:
            assert updates == R, p

        total_cpp_us = int(p.get("normal_submit_total_time_ns", 0)) / N / 1e3
        build_us = int(p.get("normal_submit_build_common_time_ns", 0)) / N / 1e3
        argmeta_us = int(p.get("task_argument_metadata_time_ns", 0)) / N / 1e3
        ensure_us = int(p.get("ensure_task_arguments_time_ns", 0)) / N / 1e3
        external_us = external_ns / N / 1e3

        return {
            "external_us": external_us,
            "total_cpp_us": total_cpp_us,
            "build_us": build_us,
            "ensure_us": ensure_us,
            "argmeta_us": argmeta_us,
            "unaccounted_us": external_us - total_cpp_us,
            "updates": float(updates),
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
                    f"    external={row['external_us']:.2f}  cpp={row['total_cpp_us']:.2f}  "
                    f"build={row['build_us']:.2f}  updates={int(row['updates'])}",
                    flush=True,
                )

    print("\nCold-vs-precommitted normal-task export profile (us/task):")
    print(
        "  variant        mode           external  total_cpp  build_common  ensure  argmeta  unaccounted  updates"
    )
    for label, _, _ in VARIANTS:
        for mode in MODES:
            group = rows[(label, mode)]
            mean = lambda key: statistics.fmean(r[key] for r in group)
            print(
                f"  {label:<14} {mode:<14} "
                f"{mean('external_us'):>9.2f}  {mean('total_cpp_us'):>9.2f}  "
                f"{mean('build_us'):>12.2f}  {mean('ensure_us'):>6.2f}  "
                f"{mean('argmeta_us'):>7.2f}  {mean('unaccounted_us'):>11.2f}  "
                f"{mean('updates'):>7.0f}"
            )

    print("\nEffect of precommit (precommitted - cold, us/task):")
    for label, _, _ in VARIANTS:
        cold = rows[(label, "cold")]
        pre = rows[(label, "precommitted")]
        c = lambda key: statistics.fmean(r[key] for r in cold)
        p = lambda key: statistics.fmean(r[key] for r in pre)
        print(
            f"  {label:<14} external={p('external_us') - c('external_us'):>8.2f}  "
            f"cpp={p('total_cpp_us') - c('total_cpp_us'):>8.2f}  "
            f"unaccounted={p('unaccounted_us') - c('unaccounted_us'):>8.2f}"
        )

    disabled_pre = statistics.fmean(
        r["external_us"] for r in rows[("disabled", "precommitted")]
    )
    frontier_pre = statistics.fmean(
        r["external_us"] for r in rows[("frontier_k32", "precommitted")]
    )
    print(
        "\nPrecommitted K32 external overhead vs Disabled: "
        f"{100.0 * (frontier_pre / disabled_pre - 1.0):.2f}%"
    )


if __name__ == "__main__":
    main()
