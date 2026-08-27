#!/usr/bin/env python3
"""Diagnostic 37: isolate Recovery Frontier consumer/executor receive cost.

The owner-side activation diagnostics show K>=4 can approach Disabled even while
benchmark 30 still has substantial steady-state throughput overhead. Benchmark 30
also continuously sends every protected ObjectRef into a downstream actor. On the
receiving worker, recovery-enabled TaskSpecs execute RegisterExecutorTask(), which
expands dependency recovery metadata and installs borrower-local recovery state.

This diagnostic completes all producer tasks before timing any downstream work,
then resets the recovery profile inside the Consumer worker itself. It reports the
existing receive-side C++ counters per consumed dependency:
  * RegisterExecutorTask calls and wall time
  * metadata refs seen
  * candidate-report build calls/time
  * candidate reports actually built
  * candidate queue calls/time

Fixed-R witness-holder mode should build zero candidate reports; any non-trivial
RegisterExecutorTask cost is therefore pure per-task borrower/metadata bookkeeping
that Recovery Frontier currently does not amortize with K.
"""
from __future__ import annotations

import argparse
import os
import statistics
import time
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import disabled, safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 2
PAYLOAD_BYTES = 1024
PADDING_BYTES = 1024
VARIANTS = [
    ("disabled", "disabled", None),
    ("fixed_r", "recovery", None),
    ("frontier_k1", "recovery", 1),
    ("frontier_k4", "recovery", 4),
    ("frontier_k8", "recovery", 8),
    ("frontier_k16", "recovery", 16),
    ("frontier_k32", "recovery", 32),
]


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


def run_case(label: str, mode: str, k: int | None, n: int) -> dict[str, Any]:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=config_for(mode, k),
            include_dashboard=False,
        )
        producer_node = cluster.add_node(num_cpus=4, resources={"producer_node": 1})
        cluster.add_node(num_cpus=2, resources={"consumer_node": 1})
        cluster.add_node(num_cpus=0, resources={"spare_holder": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, 30.0)

        @ray.remote(max_retries=2)
        def produce(i: int, payload_bytes: int, padding: bytes):
            if padding:
                _ = padding[0]
            return int(i).to_bytes(8, "little") + b"x" * max(0, payload_bytes - 8)

        @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
        class Consumer:
            def touch(self, wrapped):
                value = ray.get(wrapped[0])
                return int.from_bytes(value[:8], "little")

            def reset_profile(self):
                from ray._private.worker import global_worker
                try:
                    global_worker.core_worker.reset_recovery_succession_profile()
                except Exception:
                    pass
                return True

            def profile(self):
                from ray._private.worker import global_worker
                try:
                    return global_worker.core_worker.get_recovery_succession_profile()
                except Exception:
                    return {}

        consumer = Consumer.options(
            resources={"consumer_node": 0.01},
            num_cpus=0,
        ).remote()
        ray.get(consumer.reset_profile.remote())

        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node.node_id, soft=False)
        padding = b"p" * PADDING_BYTES

        # Register the whole producer set before exporting any ObjectRef so K>1
        # sees full groups, then force producer completion before timing receive.
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                i, PAYLOAD_BYTES, padding
            )
            for i in range(n)
        ]
        produced = ray.get(refs)
        assert len(produced) == n

        # Reset after Consumer startup and producer completion. The following
        # timed region is only downstream TaskSpec delivery + consume/read.
        ray.get(consumer.reset_profile.remote())

        t0 = time.perf_counter_ns()
        observed = ray.get([consumer.touch.remote([ref]) for ref in refs])
        elapsed_ns = time.perf_counter_ns() - t0
        assert observed == list(range(n)), observed

        p = ray.get(consumer.profile.remote())

        register_calls = int(p.get("register_executor_task_calls", 0))
        register_ns = int(p.get("register_executor_task_time_ns", 0))
        refs_seen = int(p.get("register_executor_metadata_refs_seen", 0))
        reports_built = int(p.get("register_executor_candidate_reports_built", 0))
        report_build_calls = int(p.get("candidate_report_build_calls", 0))
        report_build_ns = int(p.get("candidate_report_build_time_ns", 0))
        candidate_reports_built = int(p.get("candidate_reports_built", 0))
        queue_calls = int(p.get("candidate_queue_calls", 0))
        queue_ns = int(p.get("candidate_queue_time_ns", 0))

        if mode == "disabled":
            # No RecoverySuccessionManager is active on the consumer.
            register_calls = register_ns = refs_seen = reports_built = 0
            report_build_calls = report_build_ns = candidate_reports_built = 0
            queue_calls = queue_ns = 0
        else:
            # One downstream task with one protected dependency per input ref.
            assert register_calls >= n, (label, n, p)
            assert refs_seen >= n, (label, n, p)
            # Fixed-R mode must never enter candidate admission.
            assert reports_built == 0, (label, p)
            assert candidate_reports_built == 0, (label, p)

        return {
            "label": label,
            "wall_us_per_task": elapsed_ns / n / 1e3,
            "register_calls_per_task": register_calls / n,
            "register_us_per_task": register_ns / n / 1e3,
            "metadata_refs_per_task": refs_seen / n,
            "report_build_calls_per_task": report_build_calls / n,
            "report_build_us_per_task": report_build_ns / n / 1e3,
            "candidate_reports_per_task": candidate_reports_built / n,
            "queue_calls_per_task": queue_calls / n,
            "queue_us_per_task": queue_ns / n / 1e3,
        }
    finally:
        safe_shutdown(ray, cluster)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=512)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()

    if args.tasks <= 0 or args.repetitions <= 0:
        raise SystemExit("--tasks and --repetitions must be positive")

    rows_by_label: dict[str, list[dict[str, Any]]] = {
        label: [] for label, _, _ in VARIANTS
    }

    for rep in range(args.repetitions):
        print(f"repetition {rep + 1}/{args.repetitions}", flush=True)
        for label, mode, k in VARIANTS:
            print(f"  running {label}...", flush=True)
            row = run_case(label, mode, k, args.tasks)
            rows_by_label[label].append(row)
            print(
                f"    wall={row['wall_us_per_task']:.2f} us/task  "
                f"register={row['register_us_per_task']:.3f} us/task  "
                f"refs={row['metadata_refs_per_task']:.2f}/task",
                flush=True,
            )

    print("\nConsumer receive profile:")
    print(
        "  variant        wall_us/task  register_us/task  register_calls/task  "
        "meta_refs/task  report_build_us/task  reports/task  queue_us/task"
    )

    for label, _, _ in VARIANTS:
        rows = rows_by_label[label]
        mean = lambda key: statistics.fmean(float(row[key]) for row in rows)
        print(
            f"  {label:<14} {mean('wall_us_per_task'):>12.2f}  "
            f"{mean('register_us_per_task'):>16.3f}  "
            f"{mean('register_calls_per_task'):>19.2f}  "
            f"{mean('metadata_refs_per_task'):>14.2f}  "
            f"{mean('report_build_us_per_task'):>20.3f}  "
            f"{mean('candidate_reports_per_task'):>12.2f}  "
            f"{mean('queue_us_per_task'):>13.3f}"
        )


if __name__ == "__main__":
    main()
