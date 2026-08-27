#!/usr/bin/env python3
"""Diagnostic 41: stage-by-stage C++ profile of normal downstream submission.

This is the instrumented counterpart to diagnostic 40.  It uses a normal stateless
borrower task and resets the CoreWorker submit-stage counters after producer creation,
so exactly N downstream submissions are measured.

The purpose is to localize the persistent K32 per-task tax among:
  * prebuild: CoreWorker SubmitTask preamble before BuildCommonTaskSpec
  * build_common: BuildCommonTaskSpec, including recovery dependency activation/metadata
  * finalize: SetNormalTaskSpec + ConsumeAndBuild
  * add_pending: TaskManager::AddPendingTask
  * owner_setup: downstream owner retention/pin/callback setup (normally tiny here because
    the borrower task uses max_retries=0)
  * dispatch: ordinary io_service post or deferred-frontier bookkeeping/publication start
  * total_cpp: complete synchronous CoreWorker::SubmitTask time

Existing recovery timers are also printed for EnsureRecoverySuccessionForTaskArguments
and task-argument metadata construction.  Three repetitions are intentionally enough;
we are looking for tens-of-microseconds stage deltas, not sub-microsecond effects.
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

STAGE_KEYS = [
    ("prebuild_us", "normal_submit_prebuild_time_ns"),
    ("build_common_us", "normal_submit_build_common_time_ns"),
    ("finalize_us", "normal_submit_finalize_spec_time_ns"),
    ("add_pending_us", "normal_submit_add_pending_time_ns"),
    ("owner_setup_us", "normal_submit_owner_setup_time_ns"),
    ("dispatch_us", "normal_submit_dispatch_setup_time_ns"),
    ("total_cpp_us", "normal_submit_total_time_ns"),
]


def config_for(mode: str, k: int | None) -> dict[str, Any]:
    if mode == "disabled":
        # Profiling must be enabled for the CoreWorker-local submit counters even
        # though recovery itself is disabled.
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
        consumer_node = cluster.add_node(num_cpus=4, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, delay_s: float, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            time.sleep(delay_s)
            return int(i).to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

        # Keep the downstream task itself outside recovery scope. Only its argument
        # export/borrow path should activate producer recovery.
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
                i, PRODUCER_DELAY_S, PAYLOAD_BYTES, padding
            )
            for i in range(N)
        ]

        if export_mode == "finished":
            assert len(ray.get(refs)) == N
        elif export_mode != "pending":
            raise ValueError(export_mode)

        reset_profile()

        submit_start = time.perf_counter_ns()
        calls = [
            hold.options(scheduling_strategy=consumer_strategy, num_cpus=0).remote([ref])
            for ref in refs
        ]
        external_submit_ns = time.perf_counter_ns() - submit_start

        observed_ids = ray.get(calls)
        assert observed_ids == [ref.hex() for ref in refs]

        deadline = time.monotonic() + 5.0
        p = profile()
        while time.monotonic() < deadline:
            sent = int(p.get("witness_update_rpcs_sent", 0))
            done = int(p.get("witness_update_rpcs_completed", 0))
            if sent == done:
                break
            time.sleep(0.01)
            p = profile()

        prof_calls = int(p.get("normal_submit_profile_calls", 0))
        assert prof_calls == N, (label, export_mode, prof_calls, p)

        if recovery_mode == "disabled":
            expected_updates = 0
            assert int(p.get("witness_update_rpcs_sent", 0)) == 0
        else:
            expected_groups = N if k is None or k == 1 else (N + int(k) - 1) // int(k)
            expected_updates = expected_groups * R
            assert int(p.get("witness_update_rpcs_sent", 0)) == expected_updates, p
            assert int(p.get("witness_update_rpcs_completed", 0)) == expected_updates, p

        row: dict[str, float] = {
            "external_submit_us": external_submit_ns / N / 1e3,
            "profile_calls": float(prof_calls),
        }
        for out_key, profile_key in STAGE_KEYS:
            row[out_key] = int(p.get(profile_key, 0)) / prof_calls / 1e3

        row["unaccounted_us"] = row["external_submit_us"] - row["total_cpp_us"]
        row["ensure_us"] = int(p.get("ensure_task_arguments_time_ns", 0)) / prof_calls / 1e3
        row["argmeta_us"] = int(p.get("task_argument_metadata_time_ns", 0)) / prof_calls / 1e3
        row["updates_per_task"] = expected_updates / N

        if export_mode == "pending":
            assert len(ray.get(refs)) == N

        return row
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
                    f"    external={row['external_submit_us']:.2f}  "
                    f"cpp={row['total_cpp_us']:.2f}  "
                    f"build={row['build_common_us']:.2f}  "
                    f"pending={row['add_pending_us']:.2f}  "
                    f"dispatch={row['dispatch_us']:.2f} us/task",
                    flush=True,
                )

    print("\nNormal-task C++ submit-stage profile (us/task):")
    print(
        "  variant        mode      external   total_cpp  prebuild  build_common  finalize  add_pending  owner_setup  dispatch  ensure  argmeta  unaccounted"
    )
    for label, _, _ in VARIANTS:
        for mode in MODES:
            group = rows[(label, mode)]
            mean = lambda key: statistics.fmean(r[key] for r in group)
            print(
                f"  {label:<14} {mode:<8} "
                f"{mean('external_submit_us'):>9.2f}  "
                f"{mean('total_cpp_us'):>9.2f}  "
                f"{mean('prebuild_us'):>8.2f}  "
                f"{mean('build_common_us'):>12.2f}  "
                f"{mean('finalize_us'):>8.2f}  "
                f"{mean('add_pending_us'):>11.2f}  "
                f"{mean('owner_setup_us'):>11.2f}  "
                f"{mean('dispatch_us'):>8.2f}  "
                f"{mean('ensure_us'):>6.2f}  "
                f"{mean('argmeta_us'):>7.2f}  "
                f"{mean('unaccounted_us'):>11.2f}"
            )

    print("\nFinished-mode deltas versus Disabled (us/task):")
    disabled = {
        key: statistics.fmean(r[key] for r in rows[("disabled", "finished")])
        for key in [
            "external_submit_us", "total_cpp_us", "prebuild_us", "build_common_us",
            "finalize_us", "add_pending_us", "owner_setup_us", "dispatch_us",
            "ensure_us", "argmeta_us", "unaccounted_us",
        ]
    }
    for label in ["fixed_r", "frontier_k32"]:
        group = rows[(label, "finished")]
        print(f"  {label}:")
        for key in [
            "external_submit_us", "total_cpp_us", "prebuild_us", "build_common_us",
            "finalize_us", "add_pending_us", "owner_setup_us", "dispatch_us",
            "ensure_us", "argmeta_us", "unaccounted_us",
        ]:
            value = statistics.fmean(r[key] for r in group)
            print(f"    {key:<20} {value - disabled[key]:>8.2f}")


if __name__ == "__main__":
    main()
