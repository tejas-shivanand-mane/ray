#!/usr/bin/env python3
"""Adaptive protection under heterogeneous fanout and real failures.

Paper question
--------------
Does Recovery Succession turn application dataflow into useful recovery
redundancy more efficiently than a lazy fixed-R witness-holder baseline?

Unlike the synthetic state-amplification sweep, this benchmark runs one mixed
workload containing objects with different downstream fanout, then actually
destroys the owner and the original result node and measures recovery.

Default fanout distribution:
    60%  B=0   never exported: neither method pays recovery cost
    20%  B=1
    10%  B=2
     7%  B=3
     3%  B=4

For R=4 this gives, per 100 produced objects:
    Succession full-lineage copies = 20*1 + 10*2 + 7*3 + 3*4 = 73
    Lazy baseline copies           = (20+10+7+3)*4            = 160

So Succession should reduce full-lineage copies by 54.375% for this workload
while still recovering all activated objects after a pure owner+result-node
failure, provided at least one succession holder survives.

Failure frontier
----------------
For each run the benchmark optionally pre-kills K borrower nodes (default
K=0,1,2,3), then kills:
    1. the dedicated owner node, and
    2. the dedicated producer/result node.

After pre-killing K borrower nodes, recovery is demanded only by application
borrowers that are still alive and already hold the corresponding ObjectRef.
This is important: it exercises the worker-side recovery path used by the
system, adds no artificial observer holder, and measures availability to live
application demand.

Objects whose every application borrower died are not counted as recovery
failures because no surviving application component still requests them.

This produces a cost/useful-availability comparison:
    * Succession: lower lineage cost by placing state on actual borrowers.
    * Fixed baseline: higher lineage cost through R proactive full copies.
    * Both should satisfy surviving application demand after owner/result loss
      when their recovery mechanisms are functioning correctly.

Important result-object choice
------------------------------
Producer outputs are deliberately larger than Ray's normal direct-call inline
threshold (default here: 512 KiB). The owner only waits with fetch_local=False,
and the benchmark kills both owner and producer/result nodes before recovery.
This is intended to force actual replay rather than accidentally serving a
surviving original value. Post-failure producer START markers verify replay.

No C++ changes are required. The benchmark relies on the existing profiling
counters and on the lazy baseline implementation.

Outputs
-------
  adaptive_runs.csv
  adaptive_objects.csv
  adaptive_summary.csv
  adaptive_paired.csv

  plots/lineage_cost.png
  plots/recovery_success_vs_failures.png
  plots/cost_useful_availability_frontier.png
  plots/recovery_latency_p95.png
  plots/recovery_latency_cdf.png
  plots/success_by_fanout.png
"""
from __future__ import annotations

import os

# Recovery correctness is easier to diagnose with backend info logs.
os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import argparse
import concurrent.futures
import math
import random
import statistics
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    add_method_columns,
    mean_ci95,
    percentile,
    read_csv,
    read_marker,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    witness_baseline,
    write_csv,
)

R = 4

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
]

ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
]


@dataclass(frozen=True)
class WorkloadPlan:
    fanouts: tuple[int, ...]
    borrower_order: tuple[tuple[int, ...], ...]
    fanout_counts: tuple[tuple[int, int], ...]


def methods() -> list[Method]:
    return [succession(R), witness_baseline(R)]


def safe_div(n: float, d: float) -> float:
    return n / d if d else math.nan


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }
    if raw:
        for key in PROFILE_KEYS:
            if key in raw:
                out[key] = raw[key]
    return out


def outstanding_async(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[done]))
        for sent, done in ASYNC_PAIRS
    )


def parse_fanout_distribution(text: str) -> dict[int, float]:
    """Parse e.g. '0:60,1:20,2:10,3:7,4:3' into positive weights."""
    out: dict[int, float] = {}
    for item in text.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            raw_b, raw_w = item.split(":", 1)
            b = int(raw_b)
            w = float(raw_w)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                "fanout distribution must look like 0:60,1:20,2:10,3:7,4:3"
            ) from exc
        if b < 0 or w < 0:
            raise argparse.ArgumentTypeError(
                "fanout values and weights must be non-negative"
            )
        out[b] = out.get(b, 0.0) + w

    if not out or sum(out.values()) <= 0:
        raise argparse.ArgumentTypeError("fanout distribution has no positive weight")

    return out


def exact_counts(total: int, weights: dict[int, float]) -> dict[int, int]:
    """Largest-remainder allocation so counts sum exactly to total."""
    weight_sum = sum(weights.values())
    raw = {b: total * w / weight_sum for b, w in weights.items()}
    counts = {b: int(math.floor(v)) for b, v in raw.items()}
    remaining = total - sum(counts.values())

    order = sorted(
        weights,
        key=lambda b: (raw[b] - counts[b], weights[b], -b),
        reverse=True,
    )
    for b in order[:remaining]:
        counts[b] += 1

    return counts


def build_plan(
    *,
    task_count: int,
    distribution: dict[int, float],
    borrower_nodes: int,
    seed: int,
) -> WorkloadPlan:
    if max(distribution) > borrower_nodes:
        raise ValueError(
            f"fanout distribution requests B={max(distribution)}, but only "
            f"{borrower_nodes} borrower nodes exist"
        )

    rng = random.Random(seed)
    counts = exact_counts(task_count, distribution)

    fanouts: list[int] = []
    for b in sorted(counts):
        fanouts.extend([b] * counts[b])
    rng.shuffle(fanouts)

    orders: list[tuple[int, ...]] = []
    borrower_ids = list(range(borrower_nodes))
    for b in fanouts:
        if b == 0:
            orders.append(())
        else:
            orders.append(tuple(rng.sample(borrower_ids, b)))

    return WorkloadPlan(
        fanouts=tuple(fanouts),
        borrower_order=tuple(orders),
        fanout_counts=tuple(sorted(counts.items())),
    )


def theoretical_counts(plan: WorkloadPlan) -> dict[str, int]:
    activated = sum(1 for b in plan.fanouts if b > 0)
    succession_copies = sum(min(b, R) for b in plan.fanouts)
    baseline_copies = activated * R
    return {
        "activated": activated,
        "succession_copies": succession_copies,
        "baseline_copies": baseline_copies,
    }


def expected_succession_survival(
    plan: WorkloadPlan,
    killed_borrowers: set[int],
) -> tuple[int, int]:
    protected = 0
    survivors = 0
    for b, order in zip(plan.fanouts, plan.borrower_order):
        if b <= 0:
            continue
        protected += 1
        holders = set(order[: min(b, R)])
        if holders - killed_borrowers:
            survivors += 1
    return survivors, protected


def build_padding(total_bytes: int, chunk_bytes: int) -> tuple[bytes, ...]:
    if total_bytes <= 0:
        return ()
    out: list[bytes] = []
    remaining = total_bytes
    token = 1
    while remaining > 0:
        n = min(remaining, chunk_bytes)
        out.append(bytes([token % 251]) * n)
        token += 1
        remaining -= n
    return tuple(out)


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        request_id: int,
        result_bytes: int,
        marker: str,
        token: str,
        work_ms: float,
        *padding: bytes,
    ) -> bytes:
        if padding and padding[0]:
            _ = padding[0][0]

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"START,{time.time_ns()},{os.getpid()},"
                f"{token}:{request_id}\n"
            )

        if work_ms > 0:
            time.sleep(work_ms / 1000.0)

        prefix = request_id.to_bytes(8, "little", signed=False)
        value = prefix + b"x" * max(0, result_bytes - len(prefix))

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"FINISH,{time.time_ns()},{os.getpid()},"
                f"{token}:{request_id}\n"
            )

        return value

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def __init__(self, producer_node_id: str):
            self.producer_node_id = producer_node_id
            self.refs: list[ray.ObjectRef] = []

        def reset_profile(self) -> bool:
            from ray._private.worker import global_worker
            global_worker.core_worker.reset_recovery_succession_profile()
            return True

        def get_profile(self) -> dict[str, Any]:
            from ray._private.worker import global_worker
            return global_worker.core_worker.get_recovery_succession_profile()

        def create_tasks(
            self,
            task_count: int,
            result_bytes: int,
            task_spec_padding_bytes: int,
            inline_chunk_bytes: int,
            marker: str,
            token: str,
            work_ms: float,
            producer_cpus_per_task: float,
        ) -> int:
            padding = build_padding(
                task_spec_padding_bytes,
                inline_chunk_bytes,
            )
            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.producer_node_id,
                soft=False,
            )
            self.refs = [
                produce.options(
                    scheduling_strategy=strategy,
                    num_cpus=producer_cpus_per_task,
                ).remote(
                    i,
                    result_bytes,
                    marker,
                    token,
                    work_ms,
                    *padding,
                )
                for i in range(task_count)
            ]
            return len(self.refs)

        def export_stage(
            self,
            assignments: dict[int, list[int]],
            borrowers: list[Any],
        ) -> int:
            calls = []
            total = 0
            for borrower_idx, indices in assignments.items():
                if not indices:
                    continue
                wrapped = [[self.refs[i]] for i in indices]
                calls.append(
                    borrowers[borrower_idx].hold_many.remote(
                        indices,
                        wrapped,
                    )
                )
                total += len(indices)
            if calls:
                accepted = ray.get(calls)
                if sum(int(x) for x in accepted) != total:
                    raise RuntimeError("borrower hold count mismatch")
            return total

        def wait_all_ready(self, timeout_s: float) -> int:
            if not self.refs:
                return 0
            ready, _ = ray.wait(
                self.refs,
                num_returns=len(self.refs),
                timeout=timeout_s,
                fetch_local=False,
            )
            return len(ready)

        def export_observer_refs(self, indices: list[int]):
            # Nested refs preserve ObjectRefs instead of dereferencing values.
            return [[self.refs[i]] for i in indices]

        def ping(self) -> int:
            return os.getpid()

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=128)
    class Borrower:
        def __init__(self):
            self.refs: dict[int, ray.ObjectRef] = {}

        def hold_many(
            self,
            indices: list[int],
            wrapped_refs,
        ) -> int:
            if len(indices) != len(wrapped_refs):
                raise RuntimeError("indices/ref length mismatch")
            for i, wrapped in zip(indices, wrapped_refs):
                self.refs[int(i)] = wrapped[0]
            return len(indices)

        def held_count(self) -> int:
            return len(self.refs)

        def read_one(self, object_index: int) -> tuple[int, int]:
            """Demand one already-borrowed object from this surviving worker."""
            value = ray.get(self.refs[int(object_index)])
            if not isinstance(value, (bytes, bytearray)) or len(value) < 8:
                raise RuntimeError("unexpected recovered value")
            decoded = int.from_bytes(
                value[:8], "little", signed=False
            )
            return decoded, len(value)

        def ping(self) -> int:
            return os.getpid()

    return Owner, Borrower


def start_cluster(
    method: Method,
    args: argparse.Namespace,
) -> tuple[Cluster, Any, Any, list[Any]]:
    c = Cluster()
    object_store_bytes = args.object_store_mib * 1024 * 1024

    c.add_node(
        num_cpus=0,
        object_store_memory=object_store_bytes,
        _system_config=system_config(
            method,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
            profiling_enabled=True,
        ),
        include_dashboard=False,
    )

    owner_node = c.add_node(
        num_cpus=1,
        object_store_memory=object_store_bytes,
        resources={"owner_node": 1},
    )

    producer_node = c.add_node(
        num_cpus=args.producer_cpus,
        object_store_memory=object_store_bytes,
        resources={"producer_node": 1},
    )

    borrower_nodes = [
        c.add_node(
            num_cpus=1,
            object_store_memory=object_store_bytes,
            resources={f"borrower_{i}": 1},
        )
        for i in range(args.borrower_nodes)
    ]

    return c, owner_node, producer_node, borrower_nodes


def get_owner_profile(owner) -> dict[str, Any]:
    return profile_defaults(ray.get(owner.get_profile.remote()))


def wait_for_profile_target(
    owner,
    *,
    key: str,
    target: int,
    timeout_s: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last = get_owner_profile(owner)
    while time.monotonic() < deadline:
        last = get_owner_profile(owner)
        if int(last.get(key, 0)) >= target:
            return last
        time.sleep(0.03)
    raise TimeoutError(
        f"profile counter {key} did not reach {target}; "
        f"observed {last.get(key, 0)}"
    )


def wait_for_profile_quiescence(
    owner,
    *,
    timeout_s: float,
    stable_s: float,
) -> tuple[dict[str, Any], bool]:
    deadline = time.monotonic() + timeout_s
    last_sig = None
    stable_since = None
    last = get_owner_profile(owner)

    while time.monotonic() < deadline:
        last = get_owner_profile(owner)
        sig = tuple(last.get(k, 0) for k in PROFILE_KEYS)
        now = time.monotonic()

        if outstanding_async(last) == 0:
            if sig == last_sig:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= stable_s:
                    return last, True
            else:
                stable_since = now
        else:
            stable_since = None

        last_sig = sig
        time.sleep(0.05)

    return last, False


def stage_assignments(
    plan: WorkloadPlan,
    stage_rank: int,
    borrower_nodes: int,
) -> dict[int, list[int]]:
    """stage_rank is zero-based: 0 means first actual borrower."""
    out = {i: [] for i in range(borrower_nodes)}
    for object_index, order in enumerate(plan.borrower_order):
        if len(order) > stage_rank:
            out[order[stage_rank]].append(object_index)
    return out


def full_lineage_transfers(
    method: Method,
    profile: dict[str, Any],
) -> int:
    if method.baseline_enabled:
        return int(profile["witness_update_rpcs_sent"])
    return int(profile["holder_install_rpcs_sent"])


def recovery_control_requests(profile: dict[str, Any]) -> int:
    return (
        int(profile["candidate_reports_received"])
        + int(profile["holder_install_rpcs_sent"])
        + int(profile["holder_commit_rpcs_sent"])
        + int(profile["witness_update_rpcs_sent"])
    )


def wait_node_dead(node_id: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        alive = False
        found = False
        for node in ray.nodes():
            if node.get("NodeID") == node_id:
                found = True
                alive = bool(node.get("Alive"))
                break
        if found and not alive:
            return True
        time.sleep(0.05)
    return False


def collect_borrower_read(
    *,
    object_index: int,
    result_ref: ray.ObjectRef,
    expected_result_bytes: int,
    failure_perf: float,
    timeout_s: float,
) -> dict[str, Any]:
    try:
        decoded, result_len = ray.get(
            result_ref, timeout=timeout_s
        )
        latency = time.perf_counter() - failure_perf
        correct = (
            int(decoded) == object_index
            and int(result_len) == expected_result_bytes
        )
        return {
            "object_index": object_index,
            "success": 1,
            "correct": int(correct),
            "latency_s": latency,
            "error": "",
        }
    except Exception as exc:
        latency = time.perf_counter() - failure_perf
        return {
            "object_index": object_index,
            "success": 0,
            "correct": 0,
            "latency_s": latency,
            "error": f"{type(exc).__name__}: {exc}",
        }


def recover_live_demand(
    *,
    requesters: dict[int, int],
    borrowers: list[Any],
    expected_result_bytes: int,
    failure_perf: float,
    timeout_s: float,
    concurrency: int,
) -> list[dict[str, Any]]:
    """Demand each object from one surviving borrower that already holds it."""
    if not requesters:
        return []

    # Submit all worker-side ray.get() calls first so recovery can overlap.
    result_refs = {
        object_index: borrowers[borrower_index].read_one.remote(
            object_index
        )
        for object_index, borrower_index in requesters.items()
    }

    workers = min(max(1, concurrency), len(result_refs))
    out: list[dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                collect_borrower_read,
                object_index=i,
                result_ref=result_ref,
                expected_result_bytes=expected_result_bytes,
                failure_perf=failure_perf,
                timeout_s=timeout_s,
            )
            for i, result_ref in result_refs.items()
        ]

        for future in concurrent.futures.as_completed(futures):
            out.append(future.result())

    return out


def marker_replay_stats(
    marker: Path,
    failure_wall_ns: int,
) -> dict[str, Any]:
    starts = [
        row for row in read_marker(marker)
        if row[0] == "START" and row[1] >= failure_wall_ns
    ]
    rel = sorted((row[1] - failure_wall_ns) / 1e9 for row in starts)

    return {
        "post_failure_replay_count": len(starts),
        "failure_to_first_replay_s": rel[0] if rel else math.nan,
        "failure_to_last_replay_s": rel[-1] if rel else math.nan,
    }


def run_one(
    args: argparse.Namespace,
    *,
    method: Method,
    plan: WorkloadPlan,
    repetition: int,
    prekill_count: int,
    killed_borrower_ids: tuple[int, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    c = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_adaptive_{uuid.uuid4().hex}.csv"
    )

    try:
        c, owner_node, producer_node, borrower_nodes = start_cluster(
            method,
            args,
        )

        ray.init(
            address=c.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        expected_nodes = 3 + args.borrower_nodes
        wait_for_cluster(
            ray,
            expected_nodes,
            args.cluster_timeout_seconds,
        )

        Owner, Borrower = make_remote_types()

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(producer_node.node_id)

        borrowers = [
            Borrower.options(
                resources={f"borrower_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(args.borrower_nodes)
        ]

        ray.get(
            [owner.ping.remote()]
            + [borrower.ping.remote() for borrower in borrowers]
        )

        ray.get(owner.reset_profile.remote())

        token = uuid.uuid4().hex
        counts = theoretical_counts(plan)
        activated_indices = [
            i for i, b in enumerate(plan.fanouts) if b > 0
        ]

        workload_start = time.perf_counter()

        created = ray.get(
            owner.create_tasks.remote(
                args.task_count,
                args.result_bytes,
                args.task_spec_padding_bytes,
                args.inline_chunk_bytes,
                str(marker),
                token,
                args.task_work_ms,
                args.producer_cpus_per_task,
            )
        )
        if created != args.task_count:
            raise RuntimeError(
                f"owner created {created}/{args.task_count} tasks"
            )

        creation_time_s = time.perf_counter() - workload_start

        formation_start = time.perf_counter()
        max_fanout = max(plan.fanouts) if plan.fanouts else 0
        succession_expected = 0

        for stage in range(max_fanout):
            assignments = stage_assignments(
                plan,
                stage,
                args.borrower_nodes,
            )
            stage_count = sum(len(v) for v in assignments.values())
            if stage_count == 0:
                continue

            accepted = ray.get(
                owner.export_stage.remote(assignments, borrowers)
            )
            if accepted != stage_count:
                raise RuntimeError(
                    f"stage {stage + 1}: exported {accepted}/{stage_count}"
                )

            if method.baseline_enabled:
                if stage == 0:
                    expected_updates = counts["activated"] * R
                    wait_for_profile_target(
                        owner,
                        key="witness_update_rpcs_completed",
                        target=expected_updates,
                        timeout_s=args.formation_timeout_seconds,
                    )
            elif stage < R:
                succession_expected += stage_count
                wait_for_profile_target(
                    owner,
                    key="holder_admissions_committed",
                    target=succession_expected,
                    timeout_s=args.formation_timeout_seconds,
                )

        profile, quiescent = wait_for_profile_quiescence(
            owner,
            timeout_s=args.profile_quiescence_timeout_seconds,
            stable_s=args.profile_stable_seconds,
        )
        formation_time_s = time.perf_counter() - formation_start

        expected_copies = (
            counts["baseline_copies"]
            if method.baseline_enabled
            else counts["succession_copies"]
        )
        observed_copies = full_lineage_transfers(method, profile)

        if observed_copies != expected_copies:
            raise RuntimeError(
                f"{method.label}: expected {expected_copies} full-lineage "
                f"transfers, observed {observed_copies}"
            )

        # Ensure all originals completed, without fetching them onto the owner.
        ready_count = ray.get(
            owner.wait_all_ready.remote(args.task_ready_timeout_seconds)
        )
        if ready_count != args.task_count:
            raise TimeoutError(
                f"only {ready_count}/{args.task_count} producer results "
                "became ready before failure"
            )

        # No artificial observer is introduced. Recovery will be demanded by
        # actual surviving application borrowers that already hold each ref.
        profile_after_observer = get_owner_profile(owner)
        observer_neutral = 1

        workload_ready_time_s = time.perf_counter() - workload_start

        # Kill selected application borrower/holder nodes first.
        killed_set = set(killed_borrower_ids)
        for borrower_idx in killed_borrower_ids:
            c.remove_node(
                borrower_nodes[borrower_idx],
                allow_graceful=False,
            )

        if killed_borrower_ids:
            time.sleep(args.prekill_settle_seconds)

        expected_survivors, expected_protected = expected_succession_survival(
            plan,
            killed_set,
        )

        # Destroy both ownership state and all original result data.
        failure_wall_ns = time.time_ns()
        failure_perf = time.perf_counter()

        c.remove_node(owner_node, allow_graceful=False)
        c.remove_node(producer_node, allow_graceful=False)

        owner_dead = wait_node_dead(
            owner_node.node_id,
            args.node_death_timeout_seconds,
        )
        producer_dead = wait_node_dead(
            producer_node.node_id,
            args.node_death_timeout_seconds,
        )

        # Select one surviving application borrower for every object that
        # still has live demand. For B<=R, each such borrower is itself one of
        # Succession's natural holders, so no extra recovery state is introduced.
        requesters: dict[int, int] = {}
        for object_index in activated_indices:
            survivors = [
                borrower_index
                for borrower_index in plan.borrower_order[object_index]
                if borrower_index not in killed_set
            ]
            if survivors:
                requesters[object_index] = survivors[0]

        object_results = recover_live_demand(
            requesters=requesters,
            borrowers=borrowers,
            expected_result_bytes=args.result_bytes,
            failure_perf=failure_perf,
            timeout_s=args.recovery_timeout_seconds,
            concurrency=args.recovery_concurrency,
        )

        success_latencies = [
            float(row["latency_s"])
            for row in object_results
            if int(row["success"]) == 1
        ]

        success_count = sum(int(row["success"]) for row in object_results)
        correct_count = sum(int(row["correct"]) for row in object_results)

        replay = marker_replay_stats(marker, failure_wall_ns)

        full_bytes = int(profile_after_observer["task_spec_bytes_sent"])
        metadata_bytes = int(profile_after_observer["manifest_bytes_sent"])
        control_requests = recovery_control_requests(profile_after_observer)

        full_mib = full_bytes / (1024.0 * 1024.0)
        metadata_mib = metadata_bytes / (1024.0 * 1024.0)

        row: dict[str, Any] = {
            "repetition": repetition,
            "prekill_borrower_nodes": prekill_count,
            "killed_borrower_ids": ";".join(
                str(x) for x in killed_borrower_ids
            ),
            "target_holders": R,
            "task_count": args.task_count,
            "activated_objects": counts["activated"],
            "unactivated_objects": args.task_count - counts["activated"],
            "activated_fraction": safe_div(
                counts["activated"],
                args.task_count,
            ),
            "task_spec_padding_bytes": args.task_spec_padding_bytes,
            "result_bytes": args.result_bytes,
            "expected_full_lineage_transfers": expected_copies,
            "observed_full_lineage_transfers": observed_copies,
            "copy_count_valid": int(observed_copies == expected_copies),
            "full_lineage_copies_per_total_object": safe_div(
                observed_copies,
                args.task_count,
            ),
            "full_lineage_copies_per_activated_object": safe_div(
                observed_copies,
                counts["activated"],
            ),
            "full_lineage_bytes_total": full_bytes,
            "full_lineage_mib_total": full_mib,
            "full_lineage_mib_per_1000_objects": safe_div(
                full_mib * 1000.0,
                args.task_count,
            ),
            "recovery_metadata_bytes_total": metadata_bytes,
            "recovery_metadata_mib_total": metadata_mib,
            "control_requests_total": control_requests,
            "control_requests_per_total_object": safe_div(
                control_requests,
                args.task_count,
            ),
            "control_requests_per_activated_object": safe_div(
                control_requests,
                counts["activated"],
            ),
            "profile_quiescent": int(quiescent),
            "observer_neutral": observer_neutral,
            "creation_time_s": creation_time_s,
            "protection_formation_time_s": formation_time_s,
            "workload_ready_time_s": workload_ready_time_s,
            "formation_activated_objects_per_s": safe_div(
                counts["activated"],
                formation_time_s,
            ),
            "owner_node_dead_confirmed": int(owner_dead),
            "producer_node_dead_confirmed": int(producer_dead),
            "live_demand_objects": len(requesters),
            "live_demand_fraction_of_activated": safe_div(
                len(requesters),
                counts["activated"],
            ),
            "recovery_requested_objects": len(requesters),
            "recovery_success_count": success_count,
            "recovery_success_rate": safe_div(
                success_count,
                len(requesters),
            ),
            "recovery_correct_count": correct_count,
            "recovery_correct_rate": safe_div(
                correct_count,
                len(requesters),
            ),
            "recovery_latency_mean_s": (
                statistics.fmean(success_latencies)
                if success_latencies else math.nan
            ),
            "recovery_latency_p50_s": percentile(
                success_latencies, 0.50
            ),
            "recovery_latency_p95_s": percentile(
                success_latencies, 0.95
            ),
            "recovery_latency_p99_s": percentile(
                success_latencies, 0.99
            ),
            "recovery_latency_max_s": (
                max(success_latencies)
                if success_latencies else math.nan
            ),
            "expected_succession_survivable_objects": expected_survivors,
            "expected_succession_survival_rate": safe_div(
                expected_survivors,
                expected_protected,
            ),
            "lineage_mib_per_successful_recovery": safe_div(
                full_mib,
                success_count,
            ),
            "successful_recoveries_per_lineage_mib": safe_div(
                success_count,
                full_mib,
            ),
            **replay,
        }

        row["duplicate_replays"] = max(
            0,
            int(row["post_failure_replay_count"]) - success_count,
        )
        row["replay_per_successful_recovery"] = safe_div(
            int(row["post_failure_replay_count"]),
            success_count,
        )

        for b, count in plan.fanout_counts:
            row[f"fanout_{b}_count"] = count

        for key in PROFILE_KEYS:
            row[f"profile_{key}"] = profile_after_observer.get(key, 0)

        row = add_method_columns(row, method)

        object_rows: list[dict[str, Any]] = []
        result_by_index = {
            int(r["object_index"]): r for r in object_results
        }

        for i in activated_indices:
            assigned = plan.borrower_order[i]
            succession_holders = assigned[: min(plan.fanouts[i], R)]
            expected_survive = bool(
                set(succession_holders) - killed_set
            )
            has_live_demand = i in requesters
            object_result = result_by_index.get(i, {
                "success": 0,
                "correct": 0,
                "latency_s": math.nan,
                "error": "NO_LIVE_APPLICATION_BORROWER",
            })

            object_row = {
                "repetition": repetition,
                "prekill_borrower_nodes": prekill_count,
                "killed_borrower_ids": ";".join(
                    str(x) for x in killed_borrower_ids
                ),
                "object_index": i,
                "fanout": plan.fanouts[i],
                "assigned_borrowers": ";".join(
                    str(x) for x in assigned
                ),
                "succession_holder_borrowers": ";".join(
                    str(x) for x in succession_holders
                ),
                "expected_succession_survives": int(expected_survive),
                "has_live_demand": int(has_live_demand),
                "requester_borrower": (
                    requesters[i] if has_live_demand else -1
                ),
                "success": int(object_result["success"]) if has_live_demand else 0,
                "correct": int(object_result["correct"]),
                "latency_s": float(object_result["latency_s"]),
                "error": object_result["error"],
            }
            object_rows.append(add_method_columns(object_row, method))

        print(
            f"  copies={observed_copies}/{expected_copies} "
            f"lineage={full_mib:.1f} MiB "
            f"messages={control_requests} "
            f"success={success_count}/{len(requesters)} "
            f"({100.0 * row['recovery_success_rate']:.1f}%) "
            f"p95={row['recovery_latency_p95_s']:.3f}s "
            f"replays={row['post_failure_replay_count']}"
        )

        return row, object_rows

    finally:
        safe_shutdown(ray, c)
        try:
            marker.unlink()
        except OSError:
            pass


RUN_METRICS = [
    "full_lineage_copies_per_total_object",
    "full_lineage_copies_per_activated_object",
    "full_lineage_mib_per_1000_objects",
    "recovery_metadata_mib_total",
    "control_requests_per_total_object",
    "control_requests_per_activated_object",
    "protection_formation_time_s",
    "workload_ready_time_s",
    "formation_activated_objects_per_s",
    "live_demand_fraction_of_activated",
    "recovery_success_rate",
    "recovery_correct_rate",
    "expected_succession_survival_rate",
    "recovery_latency_mean_s",
    "recovery_latency_p50_s",
    "recovery_latency_p95_s",
    "recovery_latency_p99_s",
    "recovery_latency_max_s",
    "lineage_mib_per_successful_recovery",
    "successful_recoveries_per_lineage_mib",
    "post_failure_replay_count",
    "replay_per_successful_recovery",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    groups = sorted(
        {
            (
                row["method"],
                row["method_label"],
                int(row["prekill_borrower_nodes"]),
            )
            for row in rows
        },
        key=lambda x: (x[2], x[0]),
    )

    for method, label, k in groups:
        subset = [
            row for row in rows
            if row["method"] == method
            and int(row["prekill_borrower_nodes"]) == k
        ]

        summary: dict[str, Any] = {
            "method": method,
            "method_label": label,
            "prekill_borrower_nodes": k,
            "repetitions": len(subset),
            "all_copy_counts_valid": int(
                all(int(r["copy_count_valid"]) == 1 for r in subset)
            ),
            "all_harness_neutral": int(
                all(int(r["observer_neutral"]) == 1 for r in subset)
            ),
            "all_profiles_quiescent": int(
                all(int(r["profile_quiescent"]) == 1 for r in subset)
            ),
        }

        for metric in RUN_METRICS:
            mean, ci = mean_ci95(
                float(row[metric]) for row in subset
            )
            summary[f"{metric}_mean"] = mean
            summary[f"{metric}_ci95"] = ci

        out.append(summary)

    return out


def paired_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    ks = sorted({int(r["prekill_borrower_nodes"]) for r in summary})

    for k in ks:
        s = next(
            r for r in summary
            if r["method"] == "succession"
            and int(r["prekill_borrower_nodes"]) == k
        )
        b = next(
            r for r in summary
            if r["method"] == "witness_baseline"
            and int(r["prekill_borrower_nodes"]) == k
        )

        s_bytes = float(s["full_lineage_mib_per_1000_objects_mean"])
        b_bytes = float(b["full_lineage_mib_per_1000_objects_mean"])
        s_success = float(s["recovery_success_rate_mean"])
        b_success = float(b["recovery_success_rate_mean"])

        out.append({
            "prekill_borrower_nodes": k,
            "baseline_over_succession_lineage_cost": safe_div(
                b_bytes, s_bytes
            ),
            "succession_lineage_saving_percent": (
                100.0 * safe_div(b_bytes - s_bytes, b_bytes)
                if b_bytes else math.nan
            ),
            "succession_recovery_success_rate": s_success,
            "baseline_recovery_success_rate": b_success,
            "success_rate_delta_succession_minus_baseline": (
                s_success - b_success
            ),
            "succession_p95_recovery_latency_s": float(
                s["recovery_latency_p95_s_mean"]
            ),
            "baseline_p95_recovery_latency_s": float(
                b["recovery_latency_p95_s_mean"]
            ),
            "succession_protection_formation_time_s": float(
                s["protection_formation_time_s_mean"]
            ),
            "baseline_protection_formation_time_s": float(
                b["protection_formation_time_s_mean"]
            ),
        })

    return out


def run_experiment(args: argparse.Namespace) -> None:
    distribution = parse_fanout_distribution(args.fanout_distribution)

    if max(distribution) > args.borrower_nodes:
        raise ValueError(
            f"distribution needs {max(distribution)} borrower nodes; "
            f"--borrower-nodes={args.borrower_nodes}"
        )

    if any(k < 0 or k >= args.borrower_nodes for k in args.prekill_counts):
        raise ValueError(
            "--prekill-counts must be between 0 and borrower_nodes-1"
        )

    all_rows: list[dict[str, Any]] = []
    all_object_rows: list[dict[str, Any]] = []

    for repetition in range(1, args.repetitions + 1):
        plan_seed = args.seed + repetition - 1
        plan = build_plan(
            task_count=args.task_count,
            distribution=distribution,
            borrower_nodes=args.borrower_nodes,
            seed=plan_seed,
        )

        kill_rng = random.Random(args.seed * 1000 + repetition)
        kill_order = list(range(args.borrower_nodes))
        kill_rng.shuffle(kill_order)

        cases: list[tuple[int, tuple[int, ...], Method]] = []
        for k in args.prekill_counts:
            killed = tuple(sorted(kill_order[:k]))
            for method in methods():
                cases.append((k, killed, method))

        if not args.fixed_order:
            random.Random(args.seed + 10000 + repetition).shuffle(cases)

        print(
            f"\nrepetition={repetition} "
            f"fanout_counts={dict(plan.fanout_counts)}"
        )

        theoretical = theoretical_counts(plan)
        saving = 100.0 * (
            1.0
            - theoretical["succession_copies"]
            / theoretical["baseline_copies"]
        ) if theoretical["baseline_copies"] else 0.0

        print(
            "theory: "
            f"activated={theoretical['activated']}/{args.task_count}, "
            f"Succession copies={theoretical['succession_copies']}, "
            f"Baseline copies={theoretical['baseline_copies']}, "
            f"saving={saving:.2f}%"
        )

        for k, killed, method in cases:
            print(
                f"run method={method.label} "
                f"prekill={k} killed={list(killed)}"
            )
            row, object_rows = run_one(
                args,
                method=method,
                plan=plan,
                repetition=repetition,
                prekill_count=k,
                killed_borrower_ids=killed,
            )
            all_rows.append(row)
            all_object_rows.extend(object_rows)

    outdir = Path(args.output_dir)
    write_csv(outdir / "adaptive_runs.csv", all_rows)
    write_csv(outdir / "adaptive_objects.csv", all_object_rows)

    summary = summarize(all_rows)
    write_csv(outdir / "adaptive_summary.csv", summary)
    write_csv(outdir / "adaptive_paired.csv", paired_rows(summary))


def plot_results(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    outdir = Path(args.output_dir)
    summary = read_csv(outdir / "adaptive_summary.csv")
    objects = read_csv(outdir / "adaptive_objects.csv")
    plotdir = outdir / "plots"
    plotdir.mkdir(parents=True, exist_ok=True)

    ks = sorted({int(r["prekill_borrower_nodes"]) for r in summary})

    method_specs = [
        ("succession", "Recovery Succession"),
        ("witness_baseline", "Lazy fixed-R baseline"),
    ]

    # 1) Workload-level lineage cost. It should be essentially invariant to K.
    plt.figure(figsize=(7.5, 4.8))
    labels = []
    values = []
    errors = []
    for method, label in method_specs:
        row = next(
            r for r in summary
            if r["method"] == method
            and int(r["prekill_borrower_nodes"]) == min(ks)
        )
        labels.append(label)
        values.append(float(row["full_lineage_mib_per_1000_objects_mean"]))
        errors.append(float(row["full_lineage_mib_per_1000_objects_ci95"]))

    plt.bar(labels, values, yerr=errors, capsize=4)
    plt.ylabel("Full-lineage MiB / 1000 produced objects")
    plt.xticks(rotation=10, ha="right")
    plt.tight_layout()
    plt.savefig(plotdir / "lineage_cost.png", dpi=200)
    plt.close()

    # 2) Useful availability to surviving application demand.
    plt.figure(figsize=(7.5, 4.8))
    for method, label in method_specs:
        xs, ys, es = [], [], []
        for k in ks:
            row = next(
                r for r in summary
                if r["method"] == method
                and int(r["prekill_borrower_nodes"]) == k
            )
            xs.append(k)
            ys.append(100.0 * float(row["recovery_success_rate_mean"]))
            es.append(100.0 * float(row["recovery_success_rate_ci95"]))
        plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=label)

    plt.xlabel("Borrower/holder nodes failed before owner failure")
    plt.ylabel("Successful recoveries for live demand (%)")
    plt.xticks(ks)
    plt.ylim(0, 105)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "recovery_success_vs_failures.png", dpi=200)
    plt.close()

    # 3) Cost/useful-availability frontier: left/up is better.
    plt.figure(figsize=(7.5, 5.2))
    for method, label in method_specs:
        xs, ys = [], []
        for k in ks:
            row = next(
                r for r in summary
                if r["method"] == method
                and int(r["prekill_borrower_nodes"]) == k
            )
            xs.append(float(row["full_lineage_mib_per_1000_objects_mean"]))
            ys.append(100.0 * float(row["recovery_success_rate_mean"]))
        plt.plot(xs, ys, marker="o", label=label)
        for x, y, k in zip(xs, ys, ks):
            plt.annotate(f"K={k}", (x, y), textcoords="offset points", xytext=(5, 4))

    plt.xlabel("Full-lineage MiB / 1000 produced objects")
    plt.ylabel("Successful recoveries for live demand (%)")
    plt.ylim(0, 105)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "cost_reliability_frontier.png", dpi=200)
    plt.close()

    # 4) p95 recovery latency for successful objects.
    plt.figure(figsize=(7.5, 4.8))
    for method, label in method_specs:
        xs, ys, es = [], [], []
        for k in ks:
            row = next(
                r for r in summary
                if r["method"] == method
                and int(r["prekill_borrower_nodes"]) == k
            )
            xs.append(k)
            ys.append(float(row["recovery_latency_p95_s_mean"]))
            es.append(float(row["recovery_latency_p95_s_ci95"]))
        plt.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=label)

    plt.xlabel("Borrower/holder nodes failed before owner failure")
    plt.ylabel("p95 failure-to-result latency (s)")
    plt.xticks(ks)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "recovery_latency_p95.png", dpi=200)
    plt.close()

    # 5) CDF for successful recovery latency, aggregated over repetitions/K=0.
    plt.figure(figsize=(7.5, 4.8))
    k0 = min(ks)
    for method, label in method_specs:
        vals = sorted(
            float(r["latency_s"])
            for r in objects
            if r["method"] == method
            and int(r["prekill_borrower_nodes"]) == k0
            and r["success"] == "1"
        )
        if not vals:
            continue
        ys = [(i + 1) / len(vals) for i in range(len(vals))]
        plt.plot(vals, ys, label=label)

    plt.xlabel("Failure-to-result latency (s)")
    plt.ylabel("CDF")
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "recovery_latency_cdf.png", dpi=200)
    plt.close()

    # 6) Fanout-specific success after the most severe configured pre-kill.
    severe_k = max(ks)
    fanouts = sorted(
        {
            int(r["fanout"])
            for r in objects
            if int(r["prekill_borrower_nodes"]) == severe_k
        }
    )

    plt.figure(figsize=(7.5, 4.8))
    for method, label in method_specs:
        xs, ys = [], []
        for b in fanouts:
            subset = [
                r for r in objects
                if r["method"] == method
                and int(r["prekill_borrower_nodes"]) == severe_k
                and int(r["fanout"]) == b
                and r.get("has_live_demand", "1") == "1"
            ]
            if not subset:
                continue
            xs.append(b)
            ys.append(
                100.0
                * sum(int(r["success"]) for r in subset)
                / len(subset)
            )
        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Object downstream fanout B")
    plt.ylabel(f"Recovery success with K={severe_k} pre-failed holders (%)")
    plt.xticks(fanouts)
    plt.ylim(0, 105)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "success_by_fanout.png", dpi=200)
    plt.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot"],
        nargs="?",
        default="run-and-plot",
    )

    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/20_adaptive_protection_failure",
    )

    p.add_argument("--task-count", type=int, default=100)
    p.add_argument("--repetitions", type=int, default=1)

    p.add_argument(
        "--fanout-distribution",
        default="0:60,1:20,2:10,3:7,4:3",
        help="Weighted fanout distribution, e.g. 0:60,1:20,2:10,3:7,4:3",
    )

    p.add_argument("--borrower-nodes", type=int, default=4)

    p.add_argument(
        "--prekill-counts",
        type=int,
        nargs="+",
        default=[0, 1, 2, 3],
        help="How many borrower/holder nodes to kill before owner failure.",
    )

    p.add_argument(
        "--task-spec-padding-bytes",
        type=int,
        default=256 * 1024,
    )
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)

    p.add_argument(
        "--result-bytes",
        type=int,
        default=512 * 1024,
        help=(
            "Keep this safely above Ray's direct-call inline threshold so "
            "killing owner+producer forces actual replay."
        ),
    )

    p.add_argument("--task-work-ms", type=float, default=1.0)
    p.add_argument("--producer-cpus", type=int, default=4)
    p.add_argument("--producer-cpus-per-task", type=float, default=1.0)

    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--object-timeout-ms", type=int, default=1000)

    p.add_argument(
        "--object-store-mib",
        type=int,
        default=512,
        help="Object store allocation for each logical node.",
    )

    p.add_argument("--cluster-timeout-seconds", type=float, default=45.0)
    p.add_argument("--formation-timeout-seconds", type=float, default=60.0)
    p.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=30.0,
    )
    p.add_argument("--profile-stable-seconds", type=float, default=0.5)
    p.add_argument("--task-ready-timeout-seconds", type=float, default=120.0)
    p.add_argument("--observer-settle-seconds", type=float, default=0.25)
    p.add_argument("--prekill-settle-seconds", type=float, default=0.5)
    p.add_argument("--node-death-timeout-seconds", type=float, default=15.0)
    p.add_argument("--recovery-timeout-seconds", type=float, default=90.0)
    p.add_argument("--recovery-concurrency", type=int, default=128)

    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")

    return p


def validate_args(args: argparse.Namespace) -> None:
    if args.task_count <= 0:
        raise ValueError("--task-count must be positive")
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive")
    if args.borrower_nodes < R:
        raise ValueError(
            f"--borrower-nodes must be at least R={R} so B=R is possible"
        )
    if args.inline_chunk_bytes <= 0:
        raise ValueError("--inline-chunk-bytes must be positive")
    if args.result_bytes < 128 * 1024:
        print(
            "WARNING: --result-bytes is small. If Ray serves the original "
            "result inline, post-failure replay may not be forced."
        )
    if args.object_store_mib < 128:
        raise ValueError("--object-store-mib should be at least 128")


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)

    if args.command in {"run", "run-and-plot"}:
        run_experiment(args)

    if args.command in {"plot", "run-and-plot"}:
        plot_results(args)


if __name__ == "__main__":
    main()
