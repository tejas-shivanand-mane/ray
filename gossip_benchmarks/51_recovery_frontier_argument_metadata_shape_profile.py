#!/usr/bin/env python3
"""Benchmark 51: isolate steady committed argument-metadata construction cost.

Benchmark 42 showed that, once a K32 Frontier group is already committed, almost
all of the remaining extra C++ BuildCommonTaskSpec time is in task-argument
metadata population rather than EnsureRecoverySuccessionForTaskArguments.

This benchmark keeps the producer recovery state precommitted and compares two
normal-task argument shapes over the same producer ObjectRefs:

  direct: hold_direct.remote(ref)
          The ObjectRef is a normal by-reference task argument and Ray
          dereferences it before task execution.

  nested: hold_nested.remote([ref])
          The ObjectRef is serialized inside a by-value Python container and
          remains an ObjectRef at task execution.

Why this distinction matters:
  Nested serialization can arrive at BuildCommonTaskSpec with recovery metadata
  already attached to the ObjectReference. PopulateTaskArgumentMetadataInternal
  currently saves that metadata as a compatibility fallback before rebuilding
  the authoritative task-level sidecar from manager state. If nested argmeta is
  materially more expensive than direct argmeta, that fallback handling is the
  next narrow optimization target. If the two are similar, manager lookup/full
  metadata reconstruction + compact-sidecar encoding dominate instead.

No witness publication belongs in the timed section: all producer refs are
exported once before timing and protection is allowed to finish. The timed
profile therefore measures the steady committed metadata/export path only.
"""
from __future__ import annotations

import statistics
import time
from typing import Any

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import disabled, safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
K = 32
N = 32
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
REPETITIONS = 5

VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k32", "recovery", K),
]
MODES = ("direct", "nested")


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


def run_case(label: str, recovery_mode: str, k: int | None, arg_mode: str) -> dict[str, float]:
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

        # Recovery-ineligible downstream tasks. Direct ObjectRefs are
        # dereferenced by Ray before execution; nested refs remain ObjectRefs.
        @ray.remote(max_retries=0)
        def hold_direct(value: bytes):
            return len(value)

        @ray.remote(max_retries=0)
        def hold_nested(wrapped):
            return wrapped[0].hex()

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
        values = ray.get(refs)
        assert len(values) == N

        # Prime through the nested export path so every producer ref has crossed
        # the borrower/export boundary before the timed section. For K32 this
        # commits one group; Fixed-R commits all N independent tasks.
        reset_profile()
        prime_calls = [
            hold_nested.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote([ref])
            for ref in refs
        ]
        assert ray.get(prime_calls) == [ref.hex() for ref in refs]
        p_prime = wait_for_updates_complete()

        if recovery_mode == "recovery":
            expected_updates = (N * R) if k is None else R
            assert int(p_prime.get("witness_update_rpcs_sent", 0)) == expected_updates, p_prime
            assert int(p_prime.get("witness_update_rpcs_completed", 0)) == expected_updates, p_prime

        reset_profile()

        start_ns = time.perf_counter_ns()
        if arg_mode == "direct":
            calls = [
                hold_direct.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote(ref)
                for ref in refs
            ]
        elif arg_mode == "nested":
            calls = [
                hold_nested.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote([ref])
                for ref in refs
            ]
        else:
            raise ValueError(arg_mode)
        external_ns = time.perf_counter_ns() - start_ns

        results = ray.get(calls)
        if arg_mode == "direct":
            assert results == [PAYLOAD_BYTES for _ in refs]
        else:
            assert results == [ref.hex() for ref in refs]

        p = wait_for_updates_complete()
        assert int(p.get("witness_update_rpcs_sent", 0)) == 0, p

        prof_calls = int(p.get("normal_submit_profile_calls", 0))
        assert prof_calls == N, (label, arg_mode, prof_calls, p)

        total_cpp_us = int(p.get("normal_submit_total_time_ns", 0)) / N / 1e3
        build_us = int(p.get("normal_submit_build_common_time_ns", 0)) / N / 1e3
        ensure_us = int(p.get("ensure_task_arguments_time_ns", 0)) / N / 1e3
        argmeta_us = int(p.get("task_argument_metadata_time_ns", 0)) / N / 1e3
        external_us = external_ns / N / 1e3

        return {
            "external_us": external_us,
            "total_cpp_us": total_cpp_us,
            "build_us": build_us,
            "ensure_us": ensure_us,
            "argmeta_us": argmeta_us,
            "unaccounted_us": external_us - total_cpp_us,
            "metadata_refs": float(p.get("task_argument_metadata_refs_attached", 0)),
            "compact_refs": float(p.get("task_argument_metadata_compact_refs", 0)),
        }
    finally:
        safe_shutdown(ray, cluster)


def mean(rows: list[dict[str, float]], key: str) -> float:
    return statistics.fmean(r[key] for r in rows)


def ci95(rows: list[dict[str, float]], key: str) -> float:
    vals = [r[key] for r in rows]
    if len(vals) < 2:
        return 0.0
    return 1.96 * statistics.stdev(vals) / (len(vals) ** 0.5)


def main() -> None:
    rows: dict[tuple[str, str], list[dict[str, float]]] = {
        (label, mode): [] for label, _, _ in VARIANTS for mode in MODES
    }

    for rep in range(REPETITIONS):
        print(f"repetition {rep + 1}/{REPETITIONS}", flush=True)
        # Alternate mode order each repetition to reduce order bias.
        mode_order = MODES if rep % 2 == 0 else tuple(reversed(MODES))
        for label, recovery_mode, k in VARIANTS:
            for mode in mode_order:
                print(f"  {label:<14} {mode}...", flush=True)
                row = run_case(label, recovery_mode, k, mode)
                rows[(label, mode)].append(row)
                print(
                    f"    external={row['external_us']:.2f}  cpp={row['total_cpp_us']:.2f}  "
                    f"build={row['build_us']:.2f}  ensure={row['ensure_us']:.2f}  "
                    f"argmeta={row['argmeta_us']:.2f}",
                    flush=True,
                )

    print("\nPrecommitted argument-shape metadata profile (us/task):")
    print(
        "  variant        mode       external          total_cpp         build_common      ensure            argmeta           unaccounted"
    )
    for label, _, _ in VARIANTS:
        for mode in MODES:
            group = rows[(label, mode)]
            print(
                f"  {label:<14} {mode:<8} "
                f"{mean(group,'external_us'):>8.2f} +/- {ci95(group,'external_us'):>6.2f}  "
                f"{mean(group,'total_cpp_us'):>8.2f} +/- {ci95(group,'total_cpp_us'):>6.2f}  "
                f"{mean(group,'build_us'):>8.2f} +/- {ci95(group,'build_us'):>6.2f}  "
                f"{mean(group,'ensure_us'):>7.2f} +/- {ci95(group,'ensure_us'):>6.2f}  "
                f"{mean(group,'argmeta_us'):>7.2f} +/- {ci95(group,'argmeta_us'):>6.2f}  "
                f"{mean(group,'unaccounted_us'):>9.2f}"
            )

    print("\nNested - direct paired-shape deltas (difference of repetition means, us/task):")
    print("  variant        external    total_cpp   build_common   ensure   argmeta")
    for label, _, _ in VARIANTS:
        direct = rows[(label, "direct")]
        nested = rows[(label, "nested")]
        print(
            f"  {label:<14} "
            f"{mean(nested,'external_us') - mean(direct,'external_us'):>8.2f}  "
            f"{mean(nested,'total_cpp_us') - mean(direct,'total_cpp_us'):>9.2f}  "
            f"{mean(nested,'build_us') - mean(direct,'build_us'):>12.2f}  "
            f"{mean(nested,'ensure_us') - mean(direct,'ensure_us'):>7.2f}  "
            f"{mean(nested,'argmeta_us') - mean(direct,'argmeta_us'):>7.2f}"
        )

    f_direct = rows[("frontier_k32", "direct")]
    f_nested = rows[("frontier_k32", "nested")]
    print("\nFrontier K32 decision guide:")
    print(
        f"  direct argmeta = {mean(f_direct,'argmeta_us'):.2f} us/task; "
        f"nested argmeta = {mean(f_nested,'argmeta_us'):.2f} us/task; "
        f"nested-direct = {mean(f_nested,'argmeta_us') - mean(f_direct,'argmeta_us'):.2f} us/task"
    )
    print("  materially larger nested argmeta -> optimize legacy ObjectRef metadata fallback handling")
    print("  similar direct/nested argmeta     -> optimize manager metadata reconstruction / compact encoding")
    print("  Benchmark 48 remains the final authority after any source optimization")


if __name__ == "__main__":
    main()
