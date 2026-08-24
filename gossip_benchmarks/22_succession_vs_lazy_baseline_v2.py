#!/usr/bin/env python3
"""
Benchmark 22 v2: Recovery Succession 4L vs lazy fixed-R witness-holder baseline.

Research question
-----------------
Does Recovery Succession's borrower-adaptive lineage replication save enough
full TaskSpec traffic/state to justify its runtime overhead relative to a strong
lazy fixed-R baseline?

Current semantics used by this benchmark
----------------------------------------
R = 4.

Both methods are lazy:
  * B=0:
      no recovery activation and no remote full TaskSpec replication.
      Patch 4L may retain one dormant owner-side TaskSpec copy while the
      producer ObjectRef remains live; this is measured separately.
  * B>=1, Succession:
      full TaskSpec copies/task = min(B, R)
      achieved holders/task    = min(B, R)
  * B>=1, WitnessBaseline:
      first real downstream borrow activates recovery and installs the complete
      TaskSpec on all R witness-holder nodes.
      full TaskSpec copies/task = R
      achieved holders/task     = R

The workload is shallow-wide fan-out: B distinct consumer actors on B distinct
nodes receive the same producer ObjectRef directly from the driver/owner.
This isolates actual downstream fan-out without adding chain depth.

TaskSpec-size sweep
-------------------
The producer TaskSpec is enlarged by many small by-value byte arguments while
the returned payload remains small. The CSV records measured serialized
TaskSpec bytes/copy; requested padding is only the sweep control.

Measurement discipline
----------------------
The benchmark deliberately separates two questions:

1. LIVE-REFERENCE STATE PHASE
   A fixed batch of producer ObjectRefs is kept strongly referenced by the
   driver until the expected protection state is reached. This is the phase
   used for exact lineage-copy/byte and achieved-holder comparisons.

2. STEADY-STATE PERFORMANCE PHASE
   Pipelines are allowed to finish and their ObjectRefs die naturally. This is
   the phase used for throughput and latency. We record the recovery work that
   actually occurred, but we do NOT require every already-dead pipeline to have
   reached R/min(B,R) protection.

This avoids incorrectly classifying normal cleanup of dead ObjectRefs as a
Recovery Succession correctness failure.

Metrics
-------
Steady-state:
  * throughput
  * p50/p95/p99 end-to-end fan-out pipeline latency

Recovery state/traffic:
  * remote complete TaskSpec copies and bytes per pipeline
  * Patch-4L owner-retained TaskSpec count/bytes (current and peak)
  * combined live TaskSpec state = owner-retained + remote holder copies
  * measured TaskSpec bytes/copy
  * recovery-manifest bytes
  * candidate-report request bytes
  * normal-path recovery metadata bytes
  * approximate total measured recovery wire payload bytes
  * achieved full-lineage holder count
  * candidate reports / candidate physical RPCs

Protection:
  * protection-ready latency on a warmed single canary pipeline

Robustness
----------
Each (method, B, TaskSpec size, repetition) is executed in a fresh subprocess.
This prevents process-global RayConfig leakage between cases. Completed rows are
persisted immediately. Re-running the same command resumes incomplete cases
unless --overwrite is supplied.

Outputs
-------
  succession_vs_baseline_runs.csv
  succession_vs_baseline_summary.csv
  succession_vs_baseline_paired.csv
  plots/
"""

from __future__ import annotations

import os

# Set before importing ray in every subprocess.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import argparse
import gc
import json
import math
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    disabled,
    mean_ci95,
    percentile,
    read_csv,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    witness_baseline,
    write_csv,
)

TARGET_HOLDERS = 4
DEFAULT_BORROWERS = [0, 1, 2, 3, 4]


@dataclass(frozen=True)
class SpecPadding:
    name: str
    size_bytes: int


PROFILE_KEYS = [
    "profiling_enabled",
    "candidate_reports_received",
    "candidate_reports_accepted",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "owner_task_spec_copy_count",
    "owner_task_spec_copy_time_ns",
    "owner_lazy_task_spec_copies_avoided",
    "owner_retained_task_specs_current",
    "owner_retained_task_specs_peak",
    "owner_retained_task_spec_bytes_current",
    "owner_retained_task_spec_bytes_peak",
    "owner_retained_task_specs_created",
    "owner_retained_task_specs_released",
    "owner_retained_task_spec_copy_time_ns",
    "task_centric_metadata_builds",
    "first_holder_piggyback_copies_sent",
    "first_holder_piggyback_bytes_sent",
    "first_holder_piggyback_serialize_time_ns",
    "holder_install_rpc_time_ns",
    "holder_commit_rpc_time_ns",
    "witness_update_rpc_time_ns",
    "witness_publish_count",
    "witness_publish_time_ns",
    "witness_publish_max_time_ns",
    "holder_admissions_committed",
    "holder_admission_time_ns",
    "holder_admission_max_time_ns",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
    "task_argument_metadata_calls",
    "task_argument_metadata_time_ns",
    "task_argument_metadata_refs_attached",
    "task_argument_metadata_compact_refs",
    "task_argument_metadata_compact_fallbacks",
    "task_argument_metadata_full_bytes_equivalent",
    "task_argument_metadata_transport_bytes",
    "initial_manifest_build_count",
    "initial_manifest_build_time_ns",
    "initial_manifest_bytes",
    "witness_selection_count",
    "witness_selection_time_ns",
    "witness_gcs_query_count",
    "witness_gcs_query_time_ns",
    "task_spec_manifest_attach_count",
    "task_spec_manifest_attach_time_ns",
    "register_owned_task_count",
    "register_owned_task_time_ns",
    "recovery_metadata_lookup_calls",
    "recovery_metadata_lookup_hits",
    "recovery_metadata_lookup_time_ns",
    "ensure_task_arguments_calls",
    "ensure_task_arguments_time_ns",
    "register_executor_task_calls",
    "register_executor_task_time_ns",
    "register_executor_metadata_refs_seen",
    "register_executor_candidate_reports_built",
    "candidate_report_build_calls",
    "candidate_reports_built",
    "candidate_report_build_time_ns",
    "candidate_queue_calls",
    "candidate_queue_time_ns",
    "candidate_rpc_logical_reports_sent",
    "candidate_rpc_logical_reports_completed",
    "candidate_rpc_physical_rpcs_sent",
    "candidate_rpc_physical_rpcs_completed",
    "candidate_rpc_request_bytes_sent",
    "candidate_rpc_time_ns",
]

SUM_KEYS = set(PROFILE_KEYS) - {
    "profiling_enabled",
    "max_generation",
    "max_non_owner_holders",
}
MAX_KEYS = {"max_generation", "max_non_owner_holders"}

ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
]


def parse_spec_padding(text: str) -> SpecPadding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("TaskSpec padding must be NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError(
            "TaskSpec padding needs a non-empty NAME and BYTES >= 0"
        )
    return SpecPadding(name=name, size_bytes=size)


def method_from_key(key: str) -> Method:
    if key == "succession":
        return succession(TARGET_HOLDERS)
    if key == "witness_baseline":
        return witness_baseline(TARGET_HOLDERS)
    if key == "disabled":
        return disabled()
    raise ValueError(f"unknown method {key!r}")


def method_keys(args: argparse.Namespace) -> list[str]:
    keys = ["succession", "witness_baseline"]
    if args.include_disabled:
        keys.insert(0, "disabled")
    return keys


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }
    if raw:
        for key in PROFILE_KEYS:
            if key in raw:
                out[key] = raw[key]
    return out


def aggregate_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [profile_defaults(p) for p in profiles]
    out = profile_defaults()
    out["profiling_enabled"] = any(bool(p["profiling_enabled"]) for p in vals)
    for key in SUM_KEYS:
        out[key] = sum(int(p[key]) for p in vals)
    for key in MAX_KEYS:
        out[key] = max((int(p[key]) for p in vals), default=0)
    return out


def outstanding(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[completed]))
        for sent, completed in ASYNC_PAIRS
    )


def safe_div(numer: float, denom: float) -> float:
    return numer / denom if denom else math.nan


def build_padding(total_bytes: int, chunk_bytes: int) -> tuple[bytes, ...]:
    """Use many small by-value args so the bytes remain in the producer TaskSpec."""
    if total_bytes <= 0:
        return ()

    chunks: list[bytes] = []
    remaining = total_bytes
    token = 1

    while remaining > 0:
        n = min(remaining, chunk_bytes)
        chunks.append(bytes([token % 251]) * n)
        token += 1
        remaining -= n

    return tuple(chunks)


def start_cluster(
    method: Method,
    *,
    cpus_per_node: int,
    witness_count: int,
) -> tuple[Cluster, list[str]]:
    cluster = Cluster()

    # Driver/head is the logical owner. Keep it CPU-less.
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=witness_count,
            profiling_enabled=(
                method.recovery_enabled
                and os.environ.get("RAY_RECOVERY_PROFILING", "1") == "1"
            ),
        ),
        include_dashboard=False,
    )

    workers = [
        cluster.add_node(
            num_cpus=cpus_per_node,
            resources={"producer_node": 1},
        )
    ]

    # Four distinct borrower nodes also provide enough independent nodes for
    # WitnessBaseline-R4's fixed four full-lineage holders.
    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(
                num_cpus=cpus_per_node,
                resources={f"consumer_{i}": 1},
            )
        )

    return cluster, [node.node_id for node in workers]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        request_id: int,
        payload_bytes: int,
        *lineage_padding: bytes,
    ) -> bytes:
        # Touch the padding so it is semantically used. Serialization already
        # happened before execution; the return stays intentionally small.
        if lineage_padding and lineage_padding[0]:
            _ = lineage_padding[0][0]
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch(self, wrapped_ref):
            ref = wrapped_ref[0]
            value = ray.get(ref)
            if len(value) < 8:
                raise RuntimeError("producer payload too small")
            return int.from_bytes(value[:8], "little", signed=False)

        def ping(self) -> int:
            import os
            return os.getpid()

        def reset_recovery_profile(self) -> None:
            from ray._private.worker import global_worker as gw
            gw.core_worker.reset_recovery_succession_profile()

        def recovery_profile(self) -> dict[str, Any]:
            from ray._private.worker import global_worker as gw
            return gw.core_worker.get_recovery_succession_profile()

    return produce, Consumer


def reset_profiles(consumers: list[Any], recovery_enabled: bool) -> None:
    if not recovery_enabled:
        return
    global_worker.core_worker.reset_recovery_succession_profile()
    ray.get([consumer.reset_recovery_profile.remote() for consumer in consumers])


def profile_snapshot(
    consumers: list[Any],
    recovery_enabled: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not recovery_enabled:
        return profile_defaults(), profile_defaults()

    owner = profile_defaults(
        global_worker.core_worker.get_recovery_succession_profile()
    )
    borrower_raw = ray.get(
        [consumer.recovery_profile.remote() for consumer in consumers]
    )
    borrowers = aggregate_profiles(borrower_raw)
    return owner, borrowers


def wait_for_profile_quiescence(
    consumers: list[Any],
    recovery_enabled: bool,
    *,
    timeout_s: float,
    stable_s: float,
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    if not recovery_enabled:
        return profile_defaults(), profile_defaults(), True

    deadline = time.monotonic() + timeout_s
    last_sig: tuple[Any, ...] | None = None
    stable_since: float | None = None
    owner, borrowers = profile_snapshot(consumers, recovery_enabled)

    while time.monotonic() < deadline:
        owner, borrowers = profile_snapshot(consumers, recovery_enabled)
        sig = (
            tuple(owner[key] for key in PROFILE_KEYS)
            + tuple(borrowers[key] for key in PROFILE_KEYS)
        )
        now = time.monotonic()

        if outstanding(owner) == 0 and outstanding(borrowers) == 0:
            if sig == last_sig:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= stable_s:
                    return owner, borrowers, True
            else:
                stable_since = now
        else:
            stable_since = None

        last_sig = sig
        time.sleep(0.05)

    return owner, borrowers, False


def expected_full_lineage_copies(method: Method, borrower_count: int) -> int:
    if not method.recovery_enabled or borrower_count == 0:
        return 0
    if method.baseline_enabled:
        return TARGET_HOLDERS
    return min(borrower_count, TARGET_HOLDERS)


def protection_target_reached(
    method: Method,
    borrower_count: int,
    owner_profile: dict[str, Any],
) -> bool:
    if not method.recovery_enabled or borrower_count == 0:
        return True

    if method.baseline_enabled:
        # Lazy fixed-R baseline publishes the full TaskSpec to every one of its
        # R witness-holder nodes on first activation.
        return (
            int(owner_profile["witness_update_rpcs_completed"]) >= TARGET_HOLDERS
            and int(owner_profile["task_spec_bytes_sent"]) > 0
        )

    return int(owner_profile["holder_admissions_committed"]) >= min(
        borrower_count, TARGET_HOLDERS
    )


def measure_protection_ready(
    *,
    method: Method,
    borrower_count: int,
    produce: Any,
    consumers: list[Any],
    producer_strategy: Any,
    padding: tuple[bytes, ...],
    payload_bytes: int,
    timeout_s: float,
) -> tuple[float, bool]:
    if not method.recovery_enabled or borrower_count == 0:
        return math.nan, True

    request_id = (1 << 31) + borrower_count
    payload_ref = produce.options(
        scheduling_strategy=producer_strategy,
        num_cpus=1,
    ).remote(request_id, payload_bytes, *padding)

    # Wait for producer completion before forwarding the ObjectRef.
    #
    # IMPORTANT: payload_ref itself stays strongly referenced throughout
    # protection formation. This separates pending-ref metadata propagation
    # from actual Recovery Succession holder formation.
    payload = ray.get(payload_ref)
    if len(payload) < 8:
        raise RuntimeError("protection canary producer payload too small")

    observed = int.from_bytes(payload[:8], "little", signed=False)
    if observed != request_id:
        raise RuntimeError(
            "protection canary producer validation failed: "
            f"expected {request_id}, got {observed}"
        )

    # Measure protection formation only after the producer object is ready.
    start_ns = time.perf_counter_ns()

    # Direct fan-out: all B borrowers receive the owner's original, still-live ref.
    stage_refs = [
        consumers[i].touch.remote([payload_ref])
        for i in range(borrower_count)
    ]
    values = ray.get(stage_refs)
    if any(value != request_id for value in values):
        raise RuntimeError("protection canary returned wrong value")

    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        owner = profile_defaults(
            global_worker.core_worker.get_recovery_succession_profile()
        )
        if protection_target_reached(method, borrower_count, owner):
            return (time.perf_counter_ns() - start_ns) / 1e6, True
        time.sleep(0.01)

    return (time.perf_counter_ns() - start_ns) / 1e6, False


def run_fanout_window(
    *,
    produce: Any,
    consumers: list[Any],
    borrower_count: int,
    producer_strategy: Any,
    padding: tuple[bytes, ...],
    payload_bytes: int,
    duration_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
    request_id_base: int,
) -> dict[str, Any]:
    """
    Keep `inflight` logical producer->fanout pipelines active.

    A logical pipeline completes when:
      B=0: the producer result is ready.
      B>0: all B direct consumer calls have consumed the same ObjectRef.
    """
    pending: dict[ray.ObjectRef, tuple[int, bool]] = {}
    remaining: dict[int, int] = {}
    submitted_ns: dict[int, int] = {}

    next_request_id = request_id_base
    total_pipeline_submitted = 0
    completed_in_window = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    end_ns = start_ns + int(duration_s * 1e9)

    def submit_one() -> None:
        nonlocal next_request_id, total_pipeline_submitted

        request_id = next_request_id
        next_request_id += 1
        now_ns = time.perf_counter_ns()

        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes, *padding)

        submitted_ns[request_id] = now_ns
        total_pipeline_submitted += 1

        if borrower_count == 0:
            remaining[request_id] = 1
            pending[payload_ref] = (request_id, True)
            return

        remaining[request_id] = borrower_count
        for i in range(borrower_count):
            stage_ref = consumers[i].touch.remote([payload_ref])
            pending[stage_ref] = (request_id, False)

    def process_ready(allow_resubmit: bool) -> int:
        nonlocal completed_in_window
        if not pending:
            return 0

        ready, _ = ray.wait(
            list(pending),
            num_returns=min(32, len(pending)),
            timeout=wait_timeout_s,
        )
        if not ready:
            return 0

        completed_pipelines = 0

        for ready_ref in ready:
            request_id, producer_direct = pending.pop(ready_ref)
            result = ray.get(ready_ref)

            if producer_direct:
                if len(result) < 8:
                    raise RuntimeError("producer result too small")
                observed = int.from_bytes(result[:8], "little", signed=False)
            else:
                observed = int(result)

            if observed != request_id:
                raise RuntimeError(
                    f"pipeline validation failed: expected {request_id}, got {observed}"
                )

            remaining[request_id] -= 1
            if remaining[request_id] != 0:
                continue

            completion_ns = time.perf_counter_ns()
            if completion_ns <= end_ns:
                completed_in_window += 1

            latencies_ms.append(
                (completion_ns - submitted_ns.pop(request_id)) / 1e6
            )
            del remaining[request_id]
            completed_pipelines += 1

            if allow_resubmit and time.perf_counter_ns() < end_ns:
                submit_one()

        return completed_pipelines

    for _ in range(inflight):
        submit_one()

    while time.perf_counter_ns() < end_ns:
        process_ready(True)

    deadline = time.monotonic() + drain_timeout_s
    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"drain timeout with {len(remaining)} logical pipelines "
                f"and {len(pending)} stage refs pending"
            )
        process_ready(False)

    return {
        "throughput_rps": completed_in_window / duration_s,
        "completed_in_window": completed_in_window,
        "total_pipeline_submitted": total_pipeline_submitted,
        "latency_sample_count": len(latencies_ms),
        "latency_mean_ms": (
            statistics.fmean(latencies_ms) if latencies_ms else math.nan
        ),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def derive_recovery_metrics(
    *,
    method: Method,
    borrower_count: int,
    owner: dict[str, Any],
    borrowers: dict[str, Any],
    pipeline_count: int,
) -> dict[str, Any]:
    tasks = max(1, pipeline_count)

    if not method.recovery_enabled:
        full_lineage_transfers = 0
        achieved_holders = 0.0
    elif method.baseline_enabled:
        # Each baseline witness update includes the complete TaskSpec.
        full_lineage_transfers = int(owner["witness_update_rpcs_sent"])
        achieved_holders = safe_div(full_lineage_transfers, pipeline_count)
    else:
        # 4K full mode has no H1 piggyback. Each logical holder install carries
        # one full TaskSpec; physical install batching does not change this count.
        full_lineage_transfers = int(owner["holder_install_rpcs_sent"])
        achieved_holders = safe_div(
            int(owner["holder_admissions_committed"]),
            pipeline_count,
        )

    full_lineage_bytes = int(owner["task_spec_bytes_sent"])

    owner_retained_task_specs_current = int(
        owner["owner_retained_task_specs_current"]
    )
    owner_retained_task_specs_peak = int(
        owner["owner_retained_task_specs_peak"]
    )
    owner_retained_task_spec_bytes_current = int(
        owner["owner_retained_task_spec_bytes_current"]
    )
    owner_retained_task_spec_bytes_peak = int(
        owner["owner_retained_task_spec_bytes_peak"]
    )
    owner_retained_task_specs_created = int(
        owner["owner_retained_task_specs_created"]
    )
    owner_retained_task_specs_released = int(
        owner["owner_retained_task_specs_released"]
    )
    owner_retained_task_spec_copy_time_ns = int(
        owner["owner_retained_task_spec_copy_time_ns"]
    )

    manifest_bytes = int(owner["manifest_bytes_sent"])
    candidate_request_bytes = int(borrowers["candidate_rpc_request_bytes_sent"])
    normal_path_metadata_bytes = int(owner["task_argument_metadata_transport_bytes"])

    # Under this no-failure benchmark, Succession's manifest bytes are generated
    # by holder installs plus witness updates. For each admission stage the
    # proposed manifest is identical in the install and its witness publishes,
    # so the message-count ratio exactly separates those two components.
    witness_updates = int(owner["witness_update_rpcs_sent"])
    holder_installs = int(owner["holder_install_rpcs_sent"])
    manifest_message_count = witness_updates + holder_installs
    derived_witness_manifest_bytes = (
        manifest_bytes * witness_updates / manifest_message_count
        if manifest_message_count > 0
        else 0.0
    )

    explicit_control_bytes = (
        full_lineage_bytes
        + manifest_bytes
        + candidate_request_bytes
    )
    measured_recovery_wire_payload_bytes = (
        explicit_control_bytes
        + normal_path_metadata_bytes
    )

    expected_copies_per_task = expected_full_lineage_copies(
        method, borrower_count
    )
    expected_total_copies = expected_copies_per_task * pipeline_count

    return {
        "expected_full_lineage_copies_per_pipeline": expected_copies_per_task,
        "expected_full_lineage_transfers_total": expected_total_copies,
        "full_lineage_transfers_total": full_lineage_transfers,
        "full_lineage_transfer_count_ok": int(
            full_lineage_transfers == expected_total_copies
        ),
        "full_lineage_copies_per_pipeline": safe_div(
            full_lineage_transfers, pipeline_count
        ),
        "full_lineage_bytes_total": full_lineage_bytes,
        "full_lineage_bytes_per_pipeline": safe_div(
            full_lineage_bytes, pipeline_count
        ),
        "measured_task_spec_bytes_per_copy": safe_div(
            full_lineage_bytes, full_lineage_transfers
        ),

        # Patch 4L owner-side retained lineage is memory/state, not network
        # replication, so keep it separate from task_spec_bytes_sent.
        "owner_retained_task_specs_current": owner_retained_task_specs_current,
        "owner_retained_task_specs_peak": owner_retained_task_specs_peak,
        "owner_retained_task_specs_current_per_pipeline": safe_div(
            owner_retained_task_specs_current, pipeline_count
        ),
        "owner_retained_task_specs_peak_per_pipeline": safe_div(
            owner_retained_task_specs_peak, pipeline_count
        ),
        "owner_retained_task_spec_bytes_current": (
            owner_retained_task_spec_bytes_current
        ),
        "owner_retained_task_spec_bytes_peak": owner_retained_task_spec_bytes_peak,
        "owner_retained_task_spec_bytes_current_per_pipeline": safe_div(
            owner_retained_task_spec_bytes_current, pipeline_count
        ),
        "owner_retained_task_spec_bytes_peak_per_pipeline": safe_div(
            owner_retained_task_spec_bytes_peak, pipeline_count
        ),
        "measured_owner_retained_task_spec_bytes_per_copy": safe_div(
            owner_retained_task_spec_bytes_current,
            owner_retained_task_specs_current,
        ),
        "owner_retained_task_specs_created": owner_retained_task_specs_created,
        "owner_retained_task_specs_released": owner_retained_task_specs_released,
        "owner_retained_task_specs_created_per_pipeline": safe_div(
            owner_retained_task_specs_created, pipeline_count
        ),
        "owner_retained_task_specs_released_per_pipeline": safe_div(
            owner_retained_task_specs_released, pipeline_count
        ),
        "owner_retained_task_spec_copy_time_ns": (
            owner_retained_task_spec_copy_time_ns
        ),
        "owner_retained_task_spec_copy_time_us_per_created": safe_div(
            owner_retained_task_spec_copy_time_ns / 1e3,
            owner_retained_task_specs_created,
        ),

        "manifest_bytes_total": manifest_bytes,
        "manifest_bytes_per_pipeline": safe_div(
            manifest_bytes, pipeline_count
        ),
        "derived_witness_manifest_bytes_total": derived_witness_manifest_bytes,
        "derived_witness_manifest_bytes_per_pipeline": safe_div(
            derived_witness_manifest_bytes, pipeline_count
        ),
        "candidate_request_bytes_total": candidate_request_bytes,
        "candidate_request_bytes_per_pipeline": safe_div(
            candidate_request_bytes, pipeline_count
        ),
        "normal_path_recovery_metadata_bytes_total": normal_path_metadata_bytes,
        "normal_path_recovery_metadata_bytes_per_pipeline": safe_div(
            normal_path_metadata_bytes, pipeline_count
        ),
        "explicit_recovery_control_bytes_total": explicit_control_bytes,
        "explicit_recovery_control_bytes_per_pipeline": safe_div(
            explicit_control_bytes, pipeline_count
        ),
        "measured_recovery_wire_payload_bytes_total": (
            measured_recovery_wire_payload_bytes
        ),
        "measured_recovery_wire_payload_bytes_per_pipeline": safe_div(
            measured_recovery_wire_payload_bytes, pipeline_count
        ),
        "achieved_full_lineage_holders_per_pipeline": achieved_holders,
        "candidate_reports_received_per_pipeline": safe_div(
            int(owner["candidate_reports_received"]), pipeline_count
        ),
        "candidate_reports_accepted_per_pipeline": safe_div(
            int(owner["candidate_reports_accepted"]), pipeline_count
        ),
        "candidate_logical_reports_sent_per_pipeline": safe_div(
            int(borrowers["candidate_rpc_logical_reports_sent"]),
            pipeline_count,
        ),
        "candidate_physical_rpcs_sent_per_pipeline": safe_div(
            int(borrowers["candidate_rpc_physical_rpcs_sent"]),
            pipeline_count,
        ),
        "candidate_mean_batch_width": safe_div(
            int(borrowers["candidate_rpc_logical_reports_sent"]),
            int(borrowers["candidate_rpc_physical_rpcs_sent"]),
        ),
        "holder_install_logical_rpcs_per_pipeline": safe_div(
            int(owner["holder_install_rpcs_sent"]), pipeline_count
        ),
        "witness_update_rpcs_per_pipeline": safe_div(
            int(owner["witness_update_rpcs_sent"]), pipeline_count
        ),
        "initial_manifest_builds_per_pipeline": safe_div(
            int(owner["initial_manifest_build_count"]), pipeline_count
        ),
        "owner_lazy_task_spec_copies_avoided_per_pipeline": safe_div(
            int(owner["owner_lazy_task_spec_copies_avoided"]), pipeline_count
        ),
        "first_holder_piggyback_copies_per_pipeline": safe_div(
            int(owner["first_holder_piggyback_copies_sent"]), pipeline_count
        ),
    }



def wait_for_live_state_target(
    *,
    method: Method,
    borrower_count: int,
    task_count: int,
    consumers: list[Any],
    timeout_s: float,
) -> tuple[dict[str, Any], dict[str, Any], bool]:
    """Wait while producer refs are still live until the intended protection is reached."""
    if not method.recovery_enabled or borrower_count == 0:
        owner, borrowers = profile_snapshot(consumers, method.recovery_enabled)
        return owner, borrowers, True

    expected_per_task = expected_full_lineage_copies(method, borrower_count)
    expected_total = expected_per_task * task_count

    deadline = time.monotonic() + timeout_s
    owner, borrowers = profile_snapshot(consumers, method.recovery_enabled)

    while time.monotonic() < deadline:
        owner, borrowers = profile_snapshot(consumers, method.recovery_enabled)

        if method.baseline_enabled:
            ready = (
                int(owner["witness_update_rpcs_completed"]) >= expected_total
                and int(owner["task_spec_bytes_sent"]) > 0
            )
        else:
            ready = (
                int(owner["holder_admissions_committed"]) >= expected_total
            )

        if ready:
            owner, borrowers, quiescent = wait_for_profile_quiescence(
                consumers,
                method.recovery_enabled,
                timeout_s=timeout_s,
                stable_s=0.25,
            )
            return owner, borrowers, quiescent

        time.sleep(0.02)

    owner, borrowers = profile_snapshot(consumers, method.recovery_enabled)
    return owner, borrowers, False


def run_live_state_batch(
    *,
    args: argparse.Namespace,
    method: Method,
    borrower_count: int,
    produce: Any,
    consumers: list[Any],
    producer_strategy: Any,
    padding: tuple[bytes, ...],
) -> dict[str, Any]:
    """
    Measure architectural recovery state while the application still owns refs.

    The returned ObjectRefs stay in `producer_refs` until after the protection
    target and profile snapshot. Therefore an admission rejected in this phase
    cannot be explained by the application having already released the object.
    """
    reset_profiles(consumers, method.recovery_enabled)

    request_id_base = 5_000_000
    producer_refs: list[ray.ObjectRef] = []
    request_ids: list[int] = []

    for i in range(args.state_task_count):
        request_id = request_id_base + i
        request_ids.append(request_id)
        producer_refs.append(
            produce.options(
                scheduling_strategy=producer_strategy,
                num_cpus=1,
            ).remote(
                request_id,
                args.payload_bytes,
                *padding,
            )
        )

    # Producer-completion barrier.
    #
    # Keep all original ObjectRefs strongly referenced in producer_refs, but
    # wait until the producer tasks have completed before handing those refs
    # to borrowers. This makes the live-state phase test:
    #
    #   completed producer + live ObjectRef -> holder formation
    #
    # rather than also testing pending-ref recovery-metadata propagation.
    payloads = ray.get(producer_refs)

    for request_id, payload in zip(request_ids, payloads):
        if len(payload) < 8:
            raise RuntimeError("live-state producer payload too small")

        observed = int.from_bytes(payload[:8], "little", signed=False)
        if observed != request_id:
            raise RuntimeError(
                "live-state producer validation failed: "
                f"expected {request_id}, got {observed}"
            )

    # Formation time starts only after all producer objects are ready.
    start_ns = time.perf_counter_ns()

    if borrower_count > 0:
        stage_refs: list[ray.ObjectRef] = []
        expected_ids: list[int] = []

        for request_id, producer_ref in zip(request_ids, producer_refs):
            for i in range(borrower_count):
                stage_refs.append(consumers[i].touch.remote([producer_ref]))
                expected_ids.append(request_id)

        values = ray.get(stage_refs)
        for expected, observed in zip(expected_ids, values):
            if int(observed) != expected:
                raise RuntimeError(
                    f"live-state validation failed: expected {expected}, got {observed}"
                )

    owner, borrower_profile, target_ok = wait_for_live_state_target(
        method=method,
        borrower_count=borrower_count,
        task_count=args.state_task_count,
        consumers=consumers,
        timeout_s=args.protection_timeout_seconds,
    )

    formation_ms = (time.perf_counter_ns() - start_ns) / 1e6

    derived = derive_recovery_metrics(
        method=method,
        borrower_count=borrower_count,
        owner=owner,
        borrowers=borrower_profile,
        pipeline_count=args.state_task_count,
    )

    b0_lazy_ok = True
    if method.recovery_enabled and borrower_count == 0:
        b0_lazy_ok = (
            int(owner["initial_manifest_build_count"]) == 0
            and int(owner["task_spec_bytes_sent"]) == 0
            and int(owner["witness_update_rpcs_sent"]) == 0
            and int(owner["holder_install_rpcs_sent"]) == 0
        )

    succession_4k_ok = True
    if (
        method.recovery_enabled
        and not method.baseline_enabled
        and borrower_count > 0
    ):
        succession_4k_ok = (
            float(derived["first_holder_piggyback_copies_per_pipeline"]) == 0.0
        )

    live_valid = (
        bool(target_ok)
        and bool(b0_lazy_ok)
        and bool(succession_4k_ok)
        and (
            not method.recovery_enabled
            or int(derived["full_lineage_transfer_count_ok"]) == 1
        )
    )

    live_total_lineage_state_bytes = (
        int(derived["full_lineage_bytes_total"])
        + int(derived["owner_retained_task_spec_bytes_current"])
    )
    live_total_taskspec_copies = (
        float(derived["full_lineage_copies_per_pipeline"])
        + float(derived["owner_retained_task_specs_current_per_pipeline"])
    )

    result: dict[str, Any] = {
        **derived,
        "live_total_full_taskspec_copies_per_pipeline": live_total_taskspec_copies,
        "live_total_lineage_state_bytes_current": live_total_lineage_state_bytes,
        "live_total_lineage_state_bytes_per_pipeline": safe_div(
            live_total_lineage_state_bytes, args.state_task_count
        ),
        "live_state_task_count": args.state_task_count,
        "live_state_formation_ms": formation_ms,
        "live_state_target_ok": int(target_ok),
        "live_state_b0_lazy_ok": int(b0_lazy_ok),
        # Keep the old 4K field for CSV compatibility; 4L preserves the same
        # no-piggyback transport condition.
        "live_state_succession_4k_no_piggyback_ok": int(succession_4k_ok),
        "live_state_succession_4l_no_piggyback_ok": int(succession_4k_ok),
        "live_state_valid": int(live_valid),
    }

    for key in PROFILE_KEYS:
        result[f"live_owner_{key}"] = owner[key]
        result[f"live_borrower_{key}"] = borrower_profile[key]

    # Only now let the application refs die. Wait for resulting tombstone /
    # lineage cleanup before the later throughput profile is reset.
    producer_refs.clear()
    gc.collect()

    wait_for_profile_quiescence(
        consumers,
        method.recovery_enabled,
        timeout_s=args.profile_quiescence_timeout_seconds,
        stable_s=args.profile_stable_seconds,
    )

    return result



def run_one(
    args: argparse.Namespace,
    *,
    method: Method,
    borrower_count: int,
    padding_case: SpecPadding,
    repetition: int,
) -> dict[str, Any]:
    cluster = None

    try:
        cluster, node_ids = start_cluster(
            method,
            cpus_per_node=args.cpus_per_node,
            witness_count=args.witness_count,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(
            ray,
            TARGET_HOLDERS + 2,
            args.cluster_timeout_seconds,
        )

        produce, Consumer = make_remote_types()

        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([consumer.ping.remote() for consumer in consumers])

        producer_strategy = NodeAffinitySchedulingStrategy(
            node_id=node_ids[0],
            soft=False,
        )

        padding = build_padding(
            padding_case.size_bytes,
            args.inline_chunk_bytes,
        )

        # Warm the exact same path.
        if args.warmup_seconds > 0:
            run_fanout_window(
                produce=produce,
                consumers=consumers,
                borrower_count=borrower_count,
                producer_strategy=producer_strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight=args.inflight,
                wait_timeout_s=args.wait_timeout_seconds,
                drain_timeout_s=args.drain_timeout_seconds,
                request_id_base=1_000_000,
            )
            wait_for_profile_quiescence(
                consumers,
                method.recovery_enabled,
                timeout_s=args.profile_quiescence_timeout_seconds,
                stable_s=args.profile_stable_seconds,
            )

        # Single warmed canary: payload_ref remains alive until target is reached.
        reset_profiles(consumers, method.recovery_enabled)
        protection_ready_ms, protection_ready_ok = measure_protection_ready(
            method=method,
            borrower_count=borrower_count,
            produce=produce,
            consumers=consumers,
            producer_strategy=producer_strategy,
            padding=padding,
            payload_bytes=args.payload_bytes,
            timeout_s=args.protection_timeout_seconds,
        )
        wait_for_profile_quiescence(
            consumers,
            method.recovery_enabled,
            timeout_s=args.profile_quiescence_timeout_seconds,
            stable_s=args.profile_stable_seconds,
        )

        # Architectural measurement: refs are held live until protection forms.
        live_state = run_live_state_batch(
            args=args,
            method=method,
            borrower_count=borrower_count,
            produce=produce,
            consumers=consumers,
            producer_strategy=producer_strategy,
            padding=padding,
        )

        # Independent steady-state performance measurement. Here refs are
        # intentionally allowed to die naturally after pipeline completion.
        reset_profiles(consumers, method.recovery_enabled)

        perf = run_fanout_window(
            produce=produce,
            consumers=consumers,
            borrower_count=borrower_count,
            producer_strategy=producer_strategy,
            padding=padding,
            payload_bytes=args.payload_bytes,
            duration_s=args.duration_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
            request_id_base=10_000_000,
        )

        perf_owner, perf_borrower, perf_quiescent = wait_for_profile_quiescence(
            consumers,
            method.recovery_enabled,
            timeout_s=args.profile_quiescence_timeout_seconds,
            stable_s=args.profile_stable_seconds,
        )

        perf_recovery = derive_recovery_metrics(
            method=method,
            borrower_count=borrower_count,
            owner=perf_owner,
            borrowers=perf_borrower,
            pipeline_count=int(perf["total_pipeline_submitted"]),
        )

        # Prefix throughput-phase recovery activity so it cannot be confused
        # with the exact live-reference state metrics above.
        perf_recovery_prefixed = {
            f"perf_observed_{key}": value
            for key, value in perf_recovery.items()
        }

        row: dict[str, Any] = {
            "repetition": repetition,
            "method": method.key,
            "method_label": method.label,
            "recovery_enabled": int(method.recovery_enabled),
            "baseline_enabled": int(method.baseline_enabled),
            "target_holders": TARGET_HOLDERS,
            "borrower_count": borrower_count,
            "task_spec_padding_name": padding_case.name,
            "task_spec_padding_bytes": padding_case.size_bytes,
            "inline_chunk_bytes": args.inline_chunk_bytes,
            "payload_bytes": args.payload_bytes,
            "warmup_seconds": args.warmup_seconds,
            "duration_seconds": args.duration_seconds,
            "inflight": args.inflight,
            "protection_ready_ms": protection_ready_ms,
            "protection_ready_ok": int(protection_ready_ok),
            "perf_profile_quiescent": int(perf_quiescent),
            "perf_profile_owner_async_outstanding": outstanding(perf_owner),
            "perf_profile_borrower_async_outstanding": outstanding(perf_borrower),
            **perf,
            **live_state,
            **perf_recovery_prefixed,
        }

        for key in PROFILE_KEYS:
            row[f"perf_owner_{key}"] = perf_owner[key]
            row[f"perf_borrower_{key}"] = perf_borrower[key]

        row["run_valid"] = int(
            bool(protection_ready_ok)
            and bool(live_state["live_state_valid"])
            and bool(perf_quiescent)
        )

        print(
            "  "
            f"throughput={row['throughput_rps']:.1f} rps "
            f"p95={row['latency_p95_ms']:.2f} ms "
            f"protection={row['protection_ready_ms']:.2f} ms "
            f"LIVE copies/pipeline={row['full_lineage_copies_per_pipeline']:.2f} "
            f"LIVE lineage_KiB/pipeline="
            f"{row['full_lineage_bytes_per_pipeline'] / 1024.0:.1f} "
            f"PERF observed copies/pipeline="
            f"{row['perf_observed_full_lineage_copies_per_pipeline']:.2f} "
            f"valid={row['run_valid']}"
        )

        return row

    finally:
        safe_shutdown(ray, cluster)


SUMMARY_METRICS = [
    "throughput_rps",
    "latency_mean_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "protection_ready_ms",
    "live_state_formation_ms",
    "full_lineage_copies_per_pipeline",
    "full_lineage_bytes_per_pipeline",
    "measured_task_spec_bytes_per_copy",
    "owner_retained_task_specs_current_per_pipeline",
    "owner_retained_task_specs_peak_per_pipeline",
    "owner_retained_task_spec_bytes_current_per_pipeline",
    "owner_retained_task_spec_bytes_peak_per_pipeline",
    "measured_owner_retained_task_spec_bytes_per_copy",
    "owner_retained_task_specs_created_per_pipeline",
    "owner_retained_task_specs_released_per_pipeline",
    "owner_retained_task_spec_copy_time_us_per_created",
    "live_total_full_taskspec_copies_per_pipeline",
    "live_total_lineage_state_bytes_per_pipeline",
    "manifest_bytes_per_pipeline",
    "derived_witness_manifest_bytes_per_pipeline",
    "candidate_request_bytes_per_pipeline",
    "normal_path_recovery_metadata_bytes_per_pipeline",
    "explicit_recovery_control_bytes_per_pipeline",
    "measured_recovery_wire_payload_bytes_per_pipeline",
    "achieved_full_lineage_holders_per_pipeline",
    "candidate_reports_received_per_pipeline",
    "candidate_reports_accepted_per_pipeline",
    "candidate_logical_reports_sent_per_pipeline",
    "candidate_physical_rpcs_sent_per_pipeline",
    "candidate_mean_batch_width",
    "holder_install_logical_rpcs_per_pipeline",
    "witness_update_rpcs_per_pipeline",
    "initial_manifest_builds_per_pipeline",
    "owner_lazy_task_spec_copies_avoided_per_pipeline",
    "first_holder_piggyback_copies_per_pipeline",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = sorted(
        {
            (
                row["method"],
                row["method_label"],
                int(row["borrower_count"]),
                row["task_spec_padding_name"],
                int(row["task_spec_padding_bytes"]),
            )
            for row in rows
        },
        key=lambda item: (item[4], item[2], item[0]),
    )

    out: list[dict[str, Any]] = []

    for method, label, borrowers, size_name, size_bytes in groups:
        matched = [
            row
            for row in rows
            if row["method"] == method
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == size_name
        ]

        summary: dict[str, Any] = {
            "method": method,
            "method_label": label,
            "borrower_count": borrowers,
            "target_holders": TARGET_HOLDERS,
            "task_spec_padding_name": size_name,
            "task_spec_padding_bytes": size_bytes,
            "repetitions": len(matched),
            "all_runs_valid": int(
                all(int(row["run_valid"]) == 1 for row in matched)
            ),
        }

        for metric in SUMMARY_METRICS:
            vals: list[float] = []
            for row in matched:
                value = float(row[metric])
                if not math.isnan(value):
                    vals.append(value)
            mean, ci95 = mean_ci95(vals)
            summary[f"{metric}_mean"] = mean
            summary[f"{metric}_ci95"] = ci95

        out.append(summary)

    return out


def paired_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cases = sorted(
        {
            (
                int(row["borrower_count"]),
                row["task_spec_padding_name"],
                int(row["task_spec_padding_bytes"]),
            )
            for row in summary
            if row["method"] in {"succession", "witness_baseline"}
        },
        key=lambda item: (item[2], item[0]),
    )

    for borrowers, size_name, size_bytes in cases:
        s_matches = [
            row
            for row in summary
            if row["method"] == "succession"
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == size_name
        ]
        b_matches = [
            row
            for row in summary
            if row["method"] == "witness_baseline"
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == size_name
        ]
        if not s_matches or not b_matches:
            continue

        s = s_matches[0]
        b = b_matches[0]

        s_lineage = float(s["full_lineage_bytes_per_pipeline_mean"])
        b_lineage = float(b["full_lineage_bytes_per_pipeline_mean"])
        s_retained = float(
            s["owner_retained_task_spec_bytes_current_per_pipeline_mean"]
        )
        b_retained = float(
            b["owner_retained_task_spec_bytes_current_per_pipeline_mean"]
        )
        s_total_state = float(
            s["live_total_lineage_state_bytes_per_pipeline_mean"]
        )
        b_total_state = float(
            b["live_total_lineage_state_bytes_per_pipeline_mean"]
        )
        s_wire = float(s["measured_recovery_wire_payload_bytes_per_pipeline_mean"])
        b_wire = float(b["measured_recovery_wire_payload_bytes_per_pipeline_mean"])
        s_thr = float(s["throughput_rps_mean"])
        b_thr = float(b["throughput_rps_mean"])
        s_p95 = float(s["latency_p95_ms_mean"])
        b_p95 = float(b["latency_p95_ms_mean"])
        s_protect = float(s["protection_ready_ms_mean"])
        b_protect = float(b["protection_ready_ms_mean"])

        expected_amp = (
            math.nan
            if borrowers == 0
            else TARGET_HOLDERS / min(borrowers, TARGET_HOLDERS)
        )

        out.append(
            {
                "borrower_count": borrowers,
                "target_holders": TARGET_HOLDERS,
                "task_spec_padding_name": size_name,
                "task_spec_padding_bytes": size_bytes,
                "succession_all_runs_valid": s["all_runs_valid"],
                "baseline_all_runs_valid": b["all_runs_valid"],
                "expected_lineage_amplification_baseline_over_succession": expected_amp,
                "measured_lineage_bytes_amplification_baseline_over_succession": safe_div(
                    b_lineage, s_lineage
                ),
                "succession_full_lineage_bytes_per_pipeline": s_lineage,
                "baseline_full_lineage_bytes_per_pipeline": b_lineage,
                "lineage_bytes_saved_by_succession_per_pipeline": b_lineage - s_lineage,
                "lineage_bytes_reduction_pct_succession_vs_baseline": (
                    100.0 * (b_lineage - s_lineage) / b_lineage
                    if b_lineage > 0
                    else math.nan
                ),
                "succession_owner_retained_task_spec_bytes_per_pipeline": s_retained,
                "baseline_owner_retained_task_spec_bytes_per_pipeline": b_retained,
                "succession_live_total_lineage_state_bytes_per_pipeline": (
                    s_total_state
                ),
                "baseline_live_total_lineage_state_bytes_per_pipeline": (
                    b_total_state
                ),
                "live_total_lineage_state_bytes_saved_by_succession_per_pipeline": (
                    b_total_state - s_total_state
                ),
                "live_total_lineage_state_reduction_pct_succession_vs_baseline": (
                    100.0 * (b_total_state - s_total_state) / b_total_state
                    if b_total_state > 0
                    else math.nan
                ),
                "succession_full_lineage_copies_per_pipeline": float(
                    s["full_lineage_copies_per_pipeline_mean"]
                ),
                "baseline_full_lineage_copies_per_pipeline": float(
                    b["full_lineage_copies_per_pipeline_mean"]
                ),
                "succession_total_recovery_wire_payload_bytes_per_pipeline": s_wire,
                "baseline_total_recovery_wire_payload_bytes_per_pipeline": b_wire,
                "total_recovery_wire_ratio_baseline_over_succession": safe_div(
                    b_wire, s_wire
                ),
                "succession_throughput_rps": s_thr,
                "baseline_throughput_rps": b_thr,
                "succession_throughput_advantage_pct_vs_baseline": (
                    100.0 * (s_thr - b_thr) / b_thr
                    if b_thr > 0
                    else math.nan
                ),
                "succession_p95_latency_ms": s_p95,
                "baseline_p95_latency_ms": b_p95,
                "succession_p95_change_pct_vs_baseline": (
                    100.0 * (s_p95 - b_p95) / b_p95
                    if b_p95 > 0
                    else math.nan
                ),
                "succession_protection_ready_ms": s_protect,
                "baseline_protection_ready_ms": b_protect,
                "succession_protection_ready_change_pct_vs_baseline": (
                    100.0 * (s_protect - b_protect) / b_protect
                    if b_protect > 0
                    and not math.isnan(s_protect)
                    and not math.isnan(b_protect)
                    else math.nan
                ),
                "succession_achieved_holders": float(
                    s["achieved_full_lineage_holders_per_pipeline_mean"]
                ),
                "baseline_achieved_holders": float(
                    b["achieved_full_lineage_holders_per_pipeline_mean"]
                ),
                "succession_candidate_physical_rpcs_per_pipeline": float(
                    s["candidate_physical_rpcs_sent_per_pipeline_mean"]
                ),
                "succession_candidate_mean_batch_width": float(
                    s["candidate_mean_batch_width_mean"]
                ),
            }
        )

    return out


def case_key_from_row(row: dict[str, Any]) -> tuple[str, int, int, int]:
    return (
        str(row["method"]),
        int(row["borrower_count"]),
        int(row["task_spec_padding_bytes"]),
        int(row["repetition"]),
    )


def save_outputs(
    output_dir: Path,
    rows: list[dict[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "succession_vs_baseline_runs.csv", rows)
    summary = summarize(rows)
    write_csv(output_dir / "succession_vs_baseline_summary.csv", summary)
    write_csv(output_dir / "succession_vs_baseline_paired.csv", paired_rows(summary))


def child_command(
    args: argparse.Namespace,
    *,
    method_key: str,
    borrower_count: int,
    padding: SpecPadding,
    repetition: int,
    output_json: Path,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-run",
        "--single-method",
        method_key,
        "--single-borrower-count",
        str(borrower_count),
        "--single-padding-name",
        padding.name,
        "--single-padding-bytes",
        str(padding.size_bytes),
        "--single-repetition",
        str(repetition),
        "--single-output-json",
        str(output_json),
        "--payload-bytes",
        str(args.payload_bytes),
        "--inline-chunk-bytes",
        str(args.inline_chunk_bytes),
        "--warmup-seconds",
        str(args.warmup_seconds),
        "--duration-seconds",
        str(args.duration_seconds),
        "--inflight",
        str(args.inflight),
        "--state-task-count",
        str(args.state_task_count),
        "--cpus-per-node",
        str(args.cpus_per_node),
        "--witness-count",
        str(args.witness_count),
        "--cluster-timeout-seconds",
        str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds",
        str(args.wait_timeout_seconds),
        "--drain-timeout-seconds",
        str(args.drain_timeout_seconds),
        "--profile-quiescence-timeout-seconds",
        str(args.profile_quiescence_timeout_seconds),
        "--profile-stable-seconds",
        str(args.profile_stable_seconds),
        "--protection-timeout-seconds",
        str(args.protection_timeout_seconds),
    ]


def run_parent(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    runs_path = output_dir / "succession_vs_baseline_runs.csv"

    if args.overwrite:
        for path in [
            runs_path,
            output_dir / "succession_vs_baseline_summary.csv",
            output_dir / "succession_vs_baseline_paired.csv",
        ]:
            if path.exists():
                path.unlink()

    rows: list[dict[str, Any]] = []
    if runs_path.exists():
        rows = [dict(row) for row in read_csv(runs_path)]

    completed = {case_key_from_row(row) for row in rows}

    cases = [
        (method_key, borrower_count, padding, repetition)
        for repetition in range(1, args.repetitions + 1)
        for padding in args.task_spec_padding
        for borrower_count in args.borrowers
        for method_key in method_keys(args)
    ]

    if not args.fixed_order:
        rng = random.Random(args.seed)
        rng.shuffle(cases)

    pending_cases = [
        case
        for case in cases
        if (
            case[0],
            case[1],
            case[2].size_bytes,
            case[3],
        )
        not in completed
    ]

    print(
        f"Total matrix cases={len(cases)}, already complete={len(cases)-len(pending_cases)}, "
        f"remaining={len(pending_cases)}"
    )

    failures: list[str] = []

    for index, (method_key, borrower_count, padding, repetition) in enumerate(
        pending_cases, start=1
    ):
        label = (
            f"rep={repetition} method={method_key} B={borrower_count} "
            f"TaskSpec={padding.name}"
        )
        print(f"[{index}/{len(pending_cases)}] {label}", flush=True)

        output_json = (
            output_dir
            / f".single_{method_key}_B{borrower_count}_{padding.size_bytes}_rep{repetition}.json"
        )
        if output_json.exists():
            output_json.unlink()

        proc = subprocess.run(
            child_command(
                args,
                method_key=method_key,
                borrower_count=borrower_count,
                padding=padding,
                repetition=repetition,
                output_json=output_json,
            )
        )

        if proc.returncode != 0 or not output_json.exists():
            msg = f"FAILED: {label} (exit={proc.returncode})"
            failures.append(msg)
            print(msg, file=sys.stderr, flush=True)
            if not args.keep_going:
                save_outputs(output_dir, rows)
                raise SystemExit(proc.returncode or 1)
            continue

        row = json.loads(output_json.read_text())
        output_json.unlink(missing_ok=True)
        rows.append(row)

        # Persist after every successful case so a later crash loses nothing.
        save_outputs(output_dir, rows)

    save_outputs(output_dir, rows)

    if failures:
        print("\nCompleted with failed cases:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        print("Re-run the same command to resume only missing cases.", file=sys.stderr)


def run_single(args: argparse.Namespace) -> None:
    if args.single_method is None:
        raise ValueError("_single-run requires --single-method")
    if args.single_borrower_count is None:
        raise ValueError("_single-run requires --single-borrower-count")
    if args.single_padding_name is None or args.single_padding_bytes is None:
        raise ValueError("_single-run requires padding fields")
    if args.single_repetition is None or args.single_output_json is None:
        raise ValueError("_single-run requires repetition/output fields")

    method = method_from_key(args.single_method)
    padding = SpecPadding(
        args.single_padding_name,
        args.single_padding_bytes,
    )

    row = run_one(
        args,
        method=method,
        borrower_count=args.single_borrower_count,
        padding_case=padding,
        repetition=args.single_repetition,
    )

    Path(args.single_output_json).write_text(
        json.dumps(row, allow_nan=True)
    )


def plot_results(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    output_dir = Path(args.output_dir)
    summary_path = output_dir / "succession_vs_baseline_summary.csv"
    paired_path = output_dir / "succession_vs_baseline_paired.csv"

    if not summary_path.exists() or not paired_path.exists():
        raise FileNotFoundError(
            "Run the benchmark first; summary/paired CSVs are missing"
        )

    summary = read_csv(summary_path)
    paired = read_csv(paired_path)
    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    size_cases = sorted(
        {
            (
                int(row["task_spec_padding_bytes"]),
                row["task_spec_padding_name"],
            )
            for row in summary
        }
    )

    method_order = [
        ("succession", "Succession-R4"),
        ("witness_baseline", "LazyWitnessBaseline-R4"),
    ]
    if any(row["method"] == "disabled" for row in summary):
        method_order.insert(0, ("disabled", "Disabled"))

    for size_bytes, size_name in size_cases:
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in size_name
        )
        subset = [
            row
            for row in summary
            if int(row["task_spec_padding_bytes"]) == size_bytes
        ]

        for metric, ci_metric, ylabel, filename in [
            (
                "throughput_rps_mean",
                "throughput_rps_ci95",
                "Completed fan-out pipelines / s",
                "throughput_vs_borrowers",
            ),
            (
                "latency_p95_ms_mean",
                "latency_p95_ms_ci95",
                "P95 end-to-end latency (ms)",
                "p95_latency_vs_borrowers",
            ),
            (
                "full_lineage_bytes_per_pipeline_mean",
                "full_lineage_bytes_per_pipeline_ci95",
                "Remote full TaskSpec bytes / pipeline",
                "full_lineage_bytes_vs_borrowers",
            ),
            (
                "live_total_lineage_state_bytes_per_pipeline_mean",
                "live_total_lineage_state_bytes_per_pipeline_ci95",
                "Live total TaskSpec state bytes / pipeline",
                "live_total_lineage_state_bytes_vs_borrowers",
            ),
            (
                "measured_recovery_wire_payload_bytes_per_pipeline_mean",
                "measured_recovery_wire_payload_bytes_per_pipeline_ci95",
                "Measured recovery wire payload bytes / pipeline",
                "total_recovery_bytes_vs_borrowers",
            ),
            (
                "achieved_full_lineage_holders_per_pipeline_mean",
                "achieved_full_lineage_holders_per_pipeline_ci95",
                "Achieved full-lineage holders / pipeline",
                "achieved_holders_vs_borrowers",
            ),
        ]:
            plt.figure(figsize=(7.8, 4.9))
            for method_key, label in method_order:
                rows = sorted(
                    [row for row in subset if row["method"] == method_key],
                    key=lambda row: int(row["borrower_count"]),
                )
                if not rows:
                    continue
                plt.errorbar(
                    [int(row["borrower_count"]) for row in rows],
                    [float(row[metric]) for row in rows],
                    yerr=[float(row[ci_metric]) for row in rows],
                    marker="o",
                    capsize=3,
                    label=label,
                )
            plt.xlabel("Distinct direct downstream borrowers B")
            plt.ylabel(ylabel)
            plt.title(f"TaskSpec padding {size_name} ({size_bytes} B)")
            plt.xticks(DEFAULT_BORROWERS)
            plt.legend()
            plt.tight_layout()
            plt.savefig(plot_dir / f"{filename}_{safe_name}.png", dpi=200)
            plt.close()

        # Protection-ready is undefined at B=0.
        plt.figure(figsize=(7.8, 4.9))
        for method_key, label in method_order:
            if method_key == "disabled":
                continue
            rows = sorted(
                [
                    row
                    for row in subset
                    if row["method"] == method_key
                    and int(row["borrower_count"]) > 0
                ],
                key=lambda row: int(row["borrower_count"]),
            )
            if not rows:
                continue
            plt.errorbar(
                [int(row["borrower_count"]) for row in rows],
                [float(row["protection_ready_ms_mean"]) for row in rows],
                yerr=[float(row["protection_ready_ms_ci95"]) for row in rows],
                marker="o",
                capsize=3,
                label=label,
            )
        plt.xlabel("Distinct direct downstream borrowers B")
        plt.ylabel("Protection-ready latency (ms)")
        plt.title(f"TaskSpec padding {size_name} ({size_bytes} B)")
        plt.xticks([1, 2, 3, 4])
        plt.legend()
        plt.tight_layout()
        plt.savefig(
            plot_dir / f"protection_ready_vs_borrowers_{safe_name}.png",
            dpi=200,
        )
        plt.close()

    # Architectural amplification plot across TaskSpec sizes.
    plt.figure(figsize=(8.0, 5.0))
    for size_bytes, size_name in size_cases:
        rows = sorted(
            [
                row
                for row in paired
                if int(row["task_spec_padding_bytes"]) == size_bytes
                and int(row["borrower_count"]) > 0
            ],
            key=lambda row: int(row["borrower_count"]),
        )
        if not rows:
            continue
        plt.plot(
            [int(row["borrower_count"]) for row in rows],
            [
                float(
                    row[
                        "measured_lineage_bytes_amplification_baseline_over_succession"
                    ]
                )
                for row in rows
            ],
            marker="o",
            label=size_name,
        )

    plt.plot(
        [1, 2, 3, 4],
        [4.0, 2.0, 4.0 / 3.0, 1.0],
        marker="x",
        linestyle="--",
        label="Expected R/min(B,R)",
    )
    plt.xlabel("Distinct direct downstream borrowers B")
    plt.ylabel("Baseline / Succession full-lineage bytes")
    plt.xticks([1, 2, 3, 4])
    plt.legend()
    plt.tight_layout()
    plt.savefig(plot_dir / "lineage_byte_amplification.png", dpi=200)
    plt.close()

    print(f"Wrote plots to {plot_dir}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot", "_single-run"],
        nargs="?",
        default="run-and-plot",
    )

    parser.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/22_succession_vs_lazy_baseline_v2",
    )
    parser.add_argument("--borrowers", type=int, nargs="+", default=DEFAULT_BORROWERS)
    parser.add_argument(
        "--task-spec-padding",
        type=parse_spec_padding,
        nargs="+",
        default=[
            SpecPadding("1KiB", 1024),
            SpecPadding("16KiB", 16 * 1024),
            SpecPadding("256KiB", 256 * 1024),
            SpecPadding("1MiB", 1024 * 1024),
        ],
    )
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--inline-chunk-bytes", type=int, default=4096)

    # Two repetitions is the project default for long matrix benchmarks.
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--warmup-seconds", type=float, default=3.0)
    parser.add_argument("--duration-seconds", type=float, default=15.0)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument(
        "--state-task-count",
        type=int,
        default=32,
        help="Number of strongly-live producer refs used for exact state/byte validation.",
    )

    parser.add_argument("--cpus-per-node", type=int, default=3)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    parser.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    parser.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=30.0,
    )
    parser.add_argument("--profile-stable-seconds", type=float, default=0.5)
    parser.add_argument("--protection-timeout-seconds", type=float, default=60.0)

    parser.add_argument("--include-disabled", action="store_true")
    parser.add_argument("--fixed-order", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--keep-going", action="store_true")

    # Internal subprocess fields.
    parser.add_argument("--single-method", choices=["disabled", "succession", "witness_baseline"])
    parser.add_argument("--single-borrower-count", type=int)
    parser.add_argument("--single-padding-name")
    parser.add_argument("--single-padding-bytes", type=int)
    parser.add_argument("--single-repetition", type=int)
    parser.add_argument("--single-output-json")

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.payload_bytes < 8:
        raise ValueError("--payload-bytes must be >= 8")
    if args.inline_chunk_bytes <= 0:
        raise ValueError("--inline-chunk-bytes must be positive")
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive")
    if args.inflight <= 0:
        raise ValueError("--inflight must be positive")
    if args.state_task_count <= 0:
        raise ValueError("--state-task-count must be positive")
    if any(b < 0 or b > TARGET_HOLDERS for b in args.borrowers):
        raise ValueError("borrower counts must be between 0 and 4")


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)

    if args.command == "_single-run":
        run_single(args)
        return

    if args.command in {"run", "run-and-plot"}:
        run_parent(args)

    if args.command in {"plot", "run-and-plot"}:
        plot_results(args)


if __name__ == "__main__":
    main()
