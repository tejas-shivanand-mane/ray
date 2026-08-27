#!/usr/bin/env python3
"""Benchmark 51: isolate steady committed argument-metadata construction cost.

Benchmark 42 showed that, once a K32 Frontier group is already committed, almost
all of the remaining extra C++ BuildCommonTaskSpec time is in task-argument
metadata population rather than EnsureRecoverySuccessionForTaskArguments.

This benchmark keeps producer recovery state precommitted and compares two
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

Unlike the first version of this benchmark, each recovery variant now starts
ONE Ray cluster and runs every repetition/argument shape inside it. This keeps
the measured work identical while reducing cluster startups from 30 to 3.
"""
from __future__ import annotations

import argparse
import statistics
import time
from typing import Any

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
DEFAULT_N = 32
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
DEFAULT_REPETITIONS = 5

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


def collect_timed_case(
    label: str,
    arg_mode: str,
    refs,
    hold_direct,
    hold_nested,
    consumer_strategy,
    n: int,
) -> dict[str, float]:
    reset_profile()

    start_ns = time.perf_counter_ns()
    if arg_mode == "direct":
        calls = [
            hold_direct.options(
                scheduling_strategy=consumer_strategy, num_cpus=0
            ).remote(ref)
            for ref in refs
        ]
    elif arg_mode == "nested":
        calls = [
            hold_nested.options(
                scheduling_strategy=consumer_strategy, num_cpus=0
            ).remote([ref])
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
    assert prof_calls == n, (label, arg_mode, prof_calls, p)

    total_cpp_us = int(p.get("normal_submit_total_time_ns", 0)) / n / 1e3
    build_us = int(p.get("normal_submit_build_common_time_ns", 0)) / n / 1e3
    ensure_us = int(p.get("ensure_task_arguments_time_ns", 0)) / n / 1e3
    argmeta_us = int(p.get("task_argument_metadata_time_ns", 0)) / n / 1e3
    external_us = external_ns / n / 1e3

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


def run_variant(
    label: str,
    recovery_mode: str,
    k: int | None,
    repetitions: int,
    n: int,
) -> dict[str, list[dict[str, float]]]:
    cluster = None
    rows = {mode: [] for mode in MODES}
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
            produce.options(
                scheduling_strategy=producer_strategy, num_cpus=1
            ).remote(i, PAYLOAD_BYTES, padding)
            for i in range(n)
        ]
        values = ray.get(refs)
        assert len(values) == n

        # Prime ONCE through the nested export path. Protection is now durable
        # for every later direct/nested repetition in this same cluster.
        reset_profile()
        prime_calls = [
            hold_nested.options(
                scheduling_strategy=consumer_strategy, num_cpus=0
            ).remote([ref])
            for ref in refs
        ]
        assert ray.get(prime_calls) == [ref.hex() for ref in refs]
        p_prime = wait_for_updates_complete()

        if recovery_mode == "recovery":
            expected_updates = (n * R) if k is None else ((n + int(k) - 1) // int(k)) * R
            assert int(p_prime.get("witness_update_rpcs_sent", 0)) == expected_updates, p_prime
            assert int(p_prime.get("witness_update_rpcs_completed", 0)) == expected_updates, p_prime

        print(
            f"  {label}: precommit complete; running {repetitions} paired repetitions",
            flush=True,
        )

        for rep in range(repetitions):
            mode_order = MODES if rep % 2 == 0 else tuple(reversed(MODES))
            print(f"    repetition {rep + 1}/{repetitions}", flush=True)
            for mode in mode_order:
                row = collect_timed_case(
                    label,
                    mode,
                    refs,
                    hold_direct,
                    hold_nested,
                    consumer_strategy,
                    n,
                )
                rows[mode].append(row)
                print(
                    f"      {mode:<6} external={row['external_us']:.2f}  "
                    f"cpp={row['total_cpp_us']:.2f}  build={row['build_us']:.2f}  "
                    f"ensure={row['ensure_us']:.2f}  argmeta={row['argmeta_us']:.2f}",
                    flush=True,
                )

        return rows
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--n", type=int, default=DEFAULT_N)
    args = parser.parse_args()
    if args.repetitions < 1:
        raise ValueError("--repetitions must be >= 1")
    if args.n < 1:
        raise ValueError("--n must be >= 1")

    all_rows: dict[tuple[str, str], list[dict[str, float]]] = {}

    print(
        f"Benchmark 51: {len(VARIANTS)} clusters total, "
        f"N={args.n}, repetitions={args.repetitions}",
        flush=True,
    )
    for label, recovery_mode, k in VARIANTS:
        print(f"\nStarting variant {label}...", flush=True)
        variant_rows = run_variant(
            label, recovery_mode, k, args.repetitions, args.n
        )
        for mode in MODES:
            all_rows[(label, mode)] = variant_rows[mode]

    print("\nPrecommitted argument-shape metadata profile (us/task):")
    print(
        "  variant        mode       external          total_cpp         "
        "build_common      ensure            argmeta           unaccounted"
    )
    for label, _, _ in VARIANTS:
        for mode in MODES:
            group = all_rows[(label, mode)]
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
        direct = all_rows[(label, "direct")]
        nested = all_rows[(label, "nested")]
        print(
            f"  {label:<14} "
            f"{mean(nested,'external_us') - mean(direct,'external_us'):>8.2f}  "
            f"{mean(nested,'total_cpp_us') - mean(direct,'total_cpp_us'):>9.2f}  "
            f"{mean(nested,'build_us') - mean(direct,'build_us'):>12.2f}  "
            f"{mean(nested,'ensure_us') - mean(direct,'ensure_us'):>7.2f}  "
            f"{mean(nested,'argmeta_us') - mean(direct,'argmeta_us'):>7.2f}"
        )

    f_direct = all_rows[("frontier_k32", "direct")]
    f_nested = all_rows[("frontier_k32", "nested")]
    print("\nFrontier K32 decision guide:")
    print(
        f"  direct argmeta = {mean(f_direct,'argmeta_us'):.2f} us/task; "
        f"nested argmeta = {mean(f_nested,'argmeta_us'):.2f} us/task; "
        f"nested-direct = "
        f"{mean(f_nested,'argmeta_us') - mean(f_direct,'argmeta_us'):.2f} us/task"
    )
    print(
        "  materially larger nested argmeta -> optimize legacy ObjectRef metadata fallback handling"
    )
    print(
        "  similar direct/nested argmeta     -> optimize manager metadata reconstruction / compact encoding"
    )
    print("  Benchmark 48 remains the final authority after any source optimization")


if __name__ == "__main__":
    main()
