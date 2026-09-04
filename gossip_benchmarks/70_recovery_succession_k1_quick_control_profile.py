#!/usr/bin/env python3
"""Benchmark 70: very short ordinary Succession K=1 control-path profile.

This is a diagnostic, not a throughput benchmark. It launches one fresh cluster,
creates 32 eligible producer tasks, exports every ObjectRef to two node-distinct
borrowers, and waits for the expected R=2 holder admissions to quiesce.

The goal is to decide which next optimization is worth implementing:
  * candidate-report transport/building,
  * holder installation,
  * witness publication, or
  * common admission/owner bookkeeping.

Profiling is intentionally ON here, so do not compare its throughput with
Benchmark 69. There is no repeated sweep and no timed 20 s window.

Run:
  python gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py
"""
from __future__ import annotations

import argparse
import importlib.util
import math
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")
os.environ["RAY_RECOVERY_PROFILING"] = "1"
os.environ["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
os.environ["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
os.environ["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"

HERE = Path(__file__).resolve().parent
BENCH58_PATH = HERE / "58_recovery_frontier_succession_performance.py"


def _load_b58():
    spec = importlib.util.spec_from_file_location("recovery_k1_quick_profile_b58", BENCH58_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH58_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b58 = _load_b58()

import ray  # noqa: E402
from ray._private.worker import global_worker  # noqa: E402
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: E402
from _benchmark_common import safe_shutdown, wait_for_cluster  # noqa: E402


OWNER_ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
]
BORROWER_ASYNC_PAIRS = [
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
    ("candidate_rpc_physical_rpcs_sent", "candidate_rpc_physical_rpcs_completed"),
]


def _sum_profiles(profiles: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for profile in profiles:
        for key, value in profile.items():
            if isinstance(value, (int, float)) and math.isfinite(float(value)):
                out[key] = out.get(key, 0) + int(value)
    return out


def _outstanding(profile: dict[str, int], pairs: list[tuple[str, str]]) -> int:
    return sum(max(0, profile.get(sent, 0) - profile.get(done, 0)) for sent, done in pairs)


def _avg_us(profile: dict[str, int], time_key: str, count_key: str) -> float:
    count = int(profile.get(count_key, 0))
    return float(profile.get(time_key, 0)) / count / 1e3 if count else 0.0


def _per_task(profile: dict[str, int], key: str, tasks: int) -> float:
    return float(profile.get(key, 0)) / tasks if tasks else 0.0


def run(args: argparse.Namespace) -> None:
    if args.tasks <= 0 or args.tasks % 32:
        raise ValueError("--tasks must be a positive multiple of 32")

    cluster = None
    try:
        cluster_args = SimpleNamespace(
            holders=2,
            witness_count=2,
            cpus_per_node=args.cpus_per_node,
        )
        cluster, producer_node = b58.start_cluster(
            cluster_args, "succession_k1", profiling=True
        )
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 6, args.cluster_timeout_seconds)

        produce, Borrower = b58.remote_types()
        borrowers = [
            Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote()
            for i in range(2)
        ]
        ray.get([b.ping.remote() for b in borrowers])

        global_worker.core_worker.reset_recovery_succession_profile()
        ray.get([b.reset_profile.remote() for b in borrowers])

        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = b58.build_padding(args.task_spec_padding_bytes, args.inline_chunk_bytes)

        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                70_000_000 + i, args.payload_bytes, *padding
            )
            for i in range(args.tasks)
        ]
        expected_ids = [ref.hex() for ref in refs]

        start_ns = time.perf_counter_ns()
        held = ray.get([b.hold.remote(refs) for b in borrowers])
        borrower_delivery_ms = (time.perf_counter_ns() - start_ns) / 1e6
        if any(ids != expected_ids for ids in held):
            raise RuntimeError("borrower did not receive exact live reference batch")

        expected_admissions = args.tasks * 2
        deadline = time.monotonic() + args.timeout_seconds
        stable_since: float | None = None
        last_signature = None
        owner: dict[str, int] = {}
        borrower_profile: dict[str, int] = {}

        while time.monotonic() < deadline:
            owner = {
                k: int(v)
                for k, v in dict(
                    global_worker.core_worker.get_recovery_succession_profile()
                ).items()
                if isinstance(v, (int, float))
            }
            borrower_profile = _sum_profiles(ray.get([b.profile.remote() for b in borrowers]))

            ready = (
                owner.get("candidate_reports_received", 0) >= expected_admissions
                and owner.get("holder_admissions_committed", 0) >= expected_admissions
                and _outstanding(owner, OWNER_ASYNC_PAIRS) == 0
                and _outstanding(borrower_profile, BORROWER_ASYNC_PAIRS) == 0
            )
            signature = (
                owner.get("candidate_reports_received", 0),
                owner.get("holder_install_rpcs_completed", 0),
                owner.get("witness_update_rpcs_completed", 0),
                owner.get("holder_admissions_committed", 0),
                borrower_profile.get("candidate_rpc_physical_rpcs_completed", 0),
            )
            now = time.monotonic()
            if ready:
                if signature == last_signature:
                    if stable_since is None:
                        stable_since = now
                    elif now - stable_since >= args.stable_seconds:
                        break
                else:
                    stable_since = now
            else:
                stable_since = None
            last_signature = signature
            time.sleep(0.02)
        else:
            raise TimeoutError(
                "control profile did not quiesce: "
                f"owner={owner} borrowers={borrower_profile}"
            )

        if owner.get("holder_admissions_committed", 0) != expected_admissions:
            raise AssertionError(
                "expected exactly "
                f"{expected_admissions} admissions, got "
                f"{owner.get('holder_admissions_committed', 0)}"
            )

        print("\nQuick Succession K=1 control profile:")
        print(f"  producer tasks                         = {args.tasks}")
        print(f"  expected/committed holder admissions   = {expected_admissions}")
        print(f"  borrower batch delivery                = {borrower_delivery_ms:.2f} ms")
        print()

        print("Logical/physical fan-out per producer task:")
        print(
            "  candidate reports                     = "
            f"{_per_task(owner, 'candidate_reports_received', args.tasks):.2f} / task"
        )
        print(
            "  candidate physical RPCs               = "
            f"{_per_task(borrower_profile, 'candidate_rpc_physical_rpcs_sent', args.tasks):.3f} / task"
        )
        print(
            "  holder install RPCs                   = "
            f"{_per_task(owner, 'holder_install_rpcs_sent', args.tasks):.2f} / task"
        )
        print(
            "  witness update RPCs                   = "
            f"{_per_task(owner, 'witness_update_rpcs_sent', args.tasks):.2f} / task"
        )
        print(
            "  committed manifest generations        = "
            f"{_per_task(owner, 'manifest_generations_committed', args.tasks):.2f} / task"
        )
        print()

        print("Average measured asynchronous/control latency:")
        print(
            "  candidate physical RPC RTT            = "
            f"{_avg_us(borrower_profile, 'candidate_rpc_time_ns', 'candidate_rpc_physical_rpcs_completed'):.1f} us"
        )
        print(
            "  holder install RPC RTT                = "
            f"{_avg_us(owner, 'holder_install_rpc_time_ns', 'holder_install_rpcs_completed'):.1f} us"
        )
        print(
            "  witness update RPC RTT                = "
            f"{_avg_us(owner, 'witness_update_rpc_time_ns', 'witness_update_rpcs_completed'):.1f} us"
        )
        print(
            "  witness publication stage             = "
            f"{_avg_us(owner, 'witness_publish_time_ns', 'witness_publish_count'):.1f} us"
        )
        print(
            "  whole holder admission                = "
            f"{_avg_us(owner, 'holder_admission_time_ns', 'holder_admissions_committed'):.1f} us"
        )
        print()

        witness_completed = owner.get("witness_update_rpcs_completed", 0)
        client_queue_ns = owner.get("witness_update_client_queue_time_ns", 0)
        submit_to_cq_ns = owner.get("witness_update_client_submit_to_cq_time_ns", 0)
        cq_to_main_ns = owner.get("witness_update_client_cq_to_main_loop_time_ns", 0)
        main_to_batch_ns = owner.get(
            "witness_update_client_main_loop_to_batch_callback_time_ns", 0
        )
        client_phase_samples = owner.get("witness_update_client_phase_samples", 0)
        server_batch_queue_ns = owner.get("witness_update_server_batch_queue_time_ns", 0)
        handler_ns = owner.get("witness_update_handler_time_ns", 0)
        handler_samples = owner.get("witness_update_handler_samples", 0)
        mutex_wait_ns = owner.get("witness_update_mutex_wait_time_ns", 0)
        mutex_hold_ns = owner.get("witness_update_mutex_hold_time_ns", 0)
        rtt_ns = owner.get("witness_update_rpc_time_ns", 0)
        handler_outside_mutex_ns = max(0, handler_ns - mutex_wait_ns - mutex_hold_ns)
        residual_ns = max(
            0,
            rtt_ns
            - client_queue_ns
            - submit_to_cq_ns
            - cq_to_main_ns
            - main_to_batch_ns,
        )
        physical_batches = owner.get("witness_update_physical_batches_completed", 0)
        physical_batch_items = owner.get("witness_update_physical_batch_items", 0)
        h1_samples = owner.get("h1_publish_readiness_samples", 0)
        h2_reserved = owner.get("h2_reserved_at_h1_publish", 0)
        h2_installed = owner.get("h2_installed_at_h1_publish", 0)
        h1_ack_samples = owner.get("h1_ack_readiness_samples", 0)
        h2_reserved_at_ack = owner.get("h2_reserved_at_h1_ack", 0)
        h2_installed_at_ack = owner.get("h2_installed_at_h1_ack", 0)

        def per_completed_us(total_ns: int) -> float:
            return total_ns / witness_completed / 1e3 if witness_completed else 0.0

        print("Witness publication barrier decomposition:")
        print(
            "  client witness-batch queue            = "
            f"{per_completed_us(client_queue_ns):.1f} us / logical update"
        )
        print(
            "  client submit -> gRPC CQ              = "
            f"{per_completed_us(submit_to_cq_ns):.1f} us / logical update"
        )
        print(
            "  gRPC CQ -> main event loop            = "
            f"{per_completed_us(cq_to_main_ns):.1f} us / logical update"
        )
        print(
            "  main loop -> Raylet batch callback    = "
            f"{per_completed_us(main_to_batch_ns):.1f} us / logical update"
        )
        print(
            "  client phase timing coverage          = "
            f"{client_phase_samples}/{witness_completed} logical updates"
        )
        print(
            "  witness batch serial-position queue   = "
            f"{per_completed_us(server_batch_queue_ns):.1f} us / logical update"
        )
        print(
            "  witness handler total (amortized)     = "
            f"{per_completed_us(handler_ns):.1f} us / logical update "
            f"({handler_samples}/{witness_completed} nonzero samples)"
        )
        print(
            "    recovery_witness_mutex wait         = "
            f"{per_completed_us(mutex_wait_ns):.1f} us"
        )
        print(
            "    recovery_witness_mutex hold         = "
            f"{per_completed_us(mutex_hold_ns):.1f} us"
        )
        print(
            "    handler outside mutex               = "
            f"{per_completed_us(handler_outside_mutex_ns):.1f} us"
        )
        print(
            "  unaccounted logical callback tail     = "
            f"{per_completed_us(residual_ns):.1f} us / logical update"
        )
        print(
            "  physical witness batches              = "
            f"{physical_batches} "
            f"({_per_task(owner, 'witness_update_physical_batches_completed', args.tasks):.3f} / task)"
        )
        print(
            "  logical updates / physical batch      = "
            f"{physical_batch_items / physical_batches if physical_batches else 0.0:.2f}"
        )
        print(
            "  H2 reserved when H1 publish starts    = "
            f"{h2_reserved}/{h1_samples} "
            f"({100.0 * h2_reserved / h1_samples if h1_samples else 0.0:.1f}%)"
        )
        print(
            "  H2 installed when H1 publish starts   = "
            f"{h2_installed}/{h1_samples} "
            f"({100.0 * h2_installed / h1_samples if h1_samples else 0.0:.1f}%)"
        )
        print(
            "  H2 reserved when H1 witness ACKs      = "
            f"{h2_reserved_at_ack}/{h1_ack_samples} "
            f"({100.0 * h2_reserved_at_ack / h1_ack_samples if h1_ack_samples else 0.0:.1f}%)"
        )
        print(
            "  H2 installed when H1 witness ACKs     = "
            f"{h2_installed_at_ack}/{h1_ack_samples} "
            f"({100.0 * h2_installed_at_ack / h1_ack_samples if h1_ack_samples else 0.0:.1f}%)"
        )
        print()

        print("Synchronous CPU/copy work:")
        print(
            "  owner TaskSpec copies                 = "
            f"{owner.get('owner_task_spec_copy_count', 0)} "
            f"({_avg_us(owner, 'owner_task_spec_copy_time_ns', 'owner_task_spec_copy_count'):.1f} us/copy)"
        )
        print(
            "  owner TaskSpec bytes sent             = "
            f"{owner.get('task_spec_bytes_sent', 0) / max(1, args.tasks):.1f} B/task"
        )
        print(
            "  owner manifest bytes sent             = "
            f"{owner.get('manifest_bytes_sent', 0) / max(1, args.tasks):.1f} B/task"
        )
        print(
            "  borrower candidate-build CPU          = "
            f"{_avg_us(borrower_profile, 'candidate_report_build_time_ns', 'candidate_report_build_calls'):.1f} us/call"
        )
        print(
            "  borrower candidate-queue CPU          = "
            f"{_avg_us(borrower_profile, 'candidate_queue_time_ns', 'candidate_queue_calls'):.1f} us/call"
        )
        print()

        publish_us = _avg_us(owner, "witness_publish_time_ns", "witness_publish_count")
        install_us = _avg_us(owner, "holder_install_rpc_time_ns", "holder_install_rpcs_completed")
        admission_us = _avg_us(owner, "holder_admission_time_ns", "holder_admissions_committed")
        print("Decision signal:")
        print(
            "  admission vs (install + publish)       = "
            f"{admission_us:.1f} us vs {install_us + publish_us:.1f} us"
        )
        print(
            "  use the barrier decomposition + H2 readiness above to choose the next optimization"
        )
        print("  R=2 and W=2 remain unchanged; this is diagnosis only.")

        # Keep refs alive until all measurements have been printed, then release.
        ray.get([b.clear.remote() for b in borrowers])
    finally:
        safe_shutdown(ray, cluster)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--tasks", type=int, default=32)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--timeout-seconds", type=float, default=10.0)
    p.add_argument("--stable-seconds", type=float, default=0.15)
    return p


if __name__ == "__main__":
    run(parser().parse_args())
