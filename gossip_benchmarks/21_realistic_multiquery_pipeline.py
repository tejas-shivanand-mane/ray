#!/usr/bin/env python3
"""Benchmark 21: realistic multi-query decision-support pipeline.

Research question
-----------------
When a Ray application naturally creates heterogeneous object sharing through a
multi-query analytical pipeline, does Recovery Succession reduce complete
TaskSpec / replay-lineage amplification relative to a lazy fixed-R witness-holder
baseline, and what does it cost in control traffic, protection formation,
no-failure performance, and recovery behavior?

Why this workload is different from Benchmarks 18 and 20
--------------------------------------------------------
This benchmark NEVER assigns an object a requested fanout B.  Instead, it builds
partitioned fact objects and a small set of shared dimension objects, then applies
six fixed decision-support query templates.  A fact object's fanout is simply the
number of query workers whose real query predicate selects that partition.  The
dimension objects are broadcast because every query needs them.

Thus one application naturally produces:
  * sparse/moderate sharing for selectively accessed fact partitions;
  * dense sharing for broadcast dimension objects;
  * objects with B >= R, where Succession should lose its lineage-copy advantage.

The workload is TPC-H-*inspired* in that it is a partitioned, concurrent,
decision-support query mix with broad rollups plus selective time/region/product/
shipping slices.  It is NOT a TPC-H implementation and must not be reported as a
TPC-H result.

Recovery semantics
------------------
The owner actor owns every recoverable producer task.  Producer tasks are placed
on a dedicated result node with SOFT node affinity.  This is essential: replay in
the custom Ray implementation clears only soft node affinity, so soft=False would
make replay incorrectly remain pinned to a dead node.

Query-worker actors are application borrowers only; actors themselves are never
recovered by Recovery Succession.  They retain nested ObjectRefs without reading
values before the recovery failure.  After protection has formed and originals
are ready, the benchmark can kill selected query-worker nodes, then kill BOTH the
owner node and the producer/result node.  Recovery is demanded by one actual
surviving query worker that already held each requested ObjectRef.

The recovery claim is deliberately limited to surviving application demand.  An
object for which every query consumer died has no live application demand and is
not counted as a failed recovery.  This experiment MUST NOT be described as
showing that Succession has the same absolute R-failure durability as R fixed
independent replicas.

Protection-readiness semantics
------------------------------
For a naturally shared object with fanout B:
    Succession native target = min(B, R)
    Lazy fixed-R baseline    = R after first activation

Therefore ``native_protection_ready_time_s`` is a *native-policy* readiness
metric, not equal-durability readiness when B < R.  The CSV always reports:
  * achieved full-lineage holders per activated object;
  * fraction of activated objects naturally eligible for full R (B >= R);
  * native target holder count / object;
so the latency result cannot be separated from achieved protection.

No-failure behavior
-------------------
The same objects and the same query assignments are created for both methods.
After protection formation, all six query workers concurrently read and process
exactly the objects selected by their query predicates.  The benchmark reports
query throughput, logical partition-consumption throughput, makespan, and p50/p95
query latency.

TaskSpec-size sweep
-------------------
TaskSpec size is varied orthogonally by adding inert by-value padding arguments to
producer tasks.  Application results and query predicates do not change.  The
CSV reports measured serialized TaskSpec bytes, not only requested padding.

Outputs
-------
  realistic_plan.csv
  realistic_runs.csv
  realistic_objects.csv
  realistic_query_runs.csv
  realistic_summary.csv
  realistic_paired.csv

  plots/natural_fanout_histogram.png
  plots/lineage_cost_vs_taskspec.png
  plots/role_lineage_amplification.png
  plots/control_requests_vs_taskspec.png
  plots/nofailure_throughput.png
  plots/nofailure_p95_latency.png
  plots/native_protection_ready.png
  plots/achieved_holders.png
  plots/recovery_latency_p95.png
  plots/recovery_success_vs_query_failures.png

Typical debug run
-----------------
python gossip_benchmarks/21_realistic_multiquery_pipeline.py run-and-plot \
  --repetitions 1 \
  --periods 4 \
  --task-spec-padding 16KiB:16384 \
  --prekill-query-counts 0

Suggested main paper run (state + no-failure + clean owner/result failure)
-------------------------------------------------------------------------
python gossip_benchmarks/21_realistic_multiquery_pipeline.py run-and-plot \
  --repetitions 5 \
  --periods 6 \
  --task-spec-padding \
      1KiB:1024 16KiB:16384 256KiB:262144 1MiB:1048576 \
  --prekill-query-counts 0

Suggested resilience characterization (do not claim equal absolute durability)
-------------------------------------------------------------------------------
python gossip_benchmarks/21_realistic_multiquery_pipeline.py run-and-plot \
  --phases recovery \
  --repetitions 5 \
  --periods 6 \
  --task-spec-padding 256KiB:262144 \
  --prekill-query-counts 0 2 4
"""
from __future__ import annotations

import os

# Performance runs must not be distorted by high-volume recovery INFO logging.
# Correctness/recovery timing uses structured counters plus the producer marker.
os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

import argparse
import concurrent.futures
import math
import random
import statistics
import tempfile
import time
import uuid
from collections import Counter
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

REGIONS = ("AMERICA", "EUROPE", "ASIA", "MIDDLE_EAST")
CATEGORIES = ("STANDARD", "PROMO", "BULK")
SHIP_MODES = ("AIR", "SHIP")


# Superset of the counters used by Benchmarks 18 and 20.  Missing counters are
# normalized to zero so this script remains compatible with nearby instrumented
# revisions of the custom Ray tree.
PROFILE_KEYS = [
    "profiling_enabled",
    "candidate_reports_received",
    "candidate_reports_accepted",
    "candidate_report_bytes_received",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "full_lineage_transfer_count",
    "holder_install_task_spec_bytes_sent",
    "holder_install_manifest_bytes_sent",
    "witness_update_task_spec_bytes_sent",
    "witness_update_manifest_bytes_sent",
    "holder_commit_manifest_bytes_sent",
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
class SpecSize:
    name: str
    padding_bytes: int


@dataclass(frozen=True)
class ObjectSpec:
    object_index: int
    role: str  # fact or dimension
    period: int = -1
    region: str = ""
    category: str = ""
    ship_mode: str = ""
    dimension_name: str = ""


@dataclass(frozen=True)
class QuerySpec:
    query_id: int
    name: str
    min_period: int | None = None
    max_period: int | None = None
    regions: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()
    ship_modes: tuple[str, ...] = ()
    all_facts: bool = False
    needs_dimensions: bool = True


@dataclass(frozen=True)
class WorkloadPlan:
    objects: tuple[ObjectSpec, ...]
    queries: tuple[QuerySpec, ...]
    assignments: tuple[tuple[int, ...], ...]  # query_id -> object indices
    consumers_by_object: tuple[tuple[int, ...], ...]
    fact_object_ids: tuple[int, ...]
    dimension_object_ids: tuple[int, ...]
    fanout_histogram: tuple[tuple[int, int], ...]
    fact_fanout_histogram: tuple[tuple[int, int], ...]
    dimension_fanout_histogram: tuple[tuple[int, int], ...]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def methods() -> list[Method]:
    return [succession(R), witness_baseline(R)]


def safe_div(n: float, d: float) -> float:
    return n / d if d else math.nan


def parse_size(text: str) -> SpecSize:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError("NAME must be non-empty and BYTES >= 0")
    return SpecSize(name=name, padding_bytes=size)


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


def profile_delta(after: dict[str, Any], before: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in PROFILE_KEYS:
        if key == "profiling_enabled":
            out[key] = bool(after.get(key, False))
        else:
            out[key] = int(after.get(key, 0)) - int(before.get(key, 0))
    return out


def outstanding_async(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[done]))
        for sent, done in ASYNC_PAIRS
    )


def full_lineage_transfers(method: Method, profile: dict[str, Any]) -> int:
    # Keep the method-specific definition used by Benchmark 20.  Some newer
    # builds also expose full_lineage_transfer_count; record it separately but do
    # not rely on it because older instrumented builds may leave it absent/zero.
    if method.baseline_enabled:
        return int(profile["witness_update_rpcs_sent"])
    return int(profile["holder_install_rpcs_sent"])


def full_lineage_transfers_completed(method: Method, profile: dict[str, Any]) -> int:
    if method.baseline_enabled:
        return int(profile["witness_update_rpcs_completed"])
    return int(profile["holder_install_rpcs_completed"])


def recovery_control_requests(profile: dict[str, Any]) -> int:
    return (
        int(profile["candidate_reports_received"])
        + int(profile["holder_install_rpcs_sent"])
        + int(profile["holder_commit_rpcs_sent"])
        + int(profile["witness_update_rpcs_sent"])
    )


def build_padding(total_bytes: int, chunk_bytes: int) -> tuple[bytes, ...]:
    if total_bytes <= 0:
        return ()
    chunks: list[bytes] = []
    left = total_bytes
    token = 1
    while left > 0:
        n = min(left, chunk_bytes)
        chunks.append(bytes([token % 251]) * n)
        token += 1
        left -= n
    return tuple(chunks)


def finite_values(values: Iterable[float]) -> list[float]:
    return [float(v) for v in values if math.isfinite(float(v))]


# ---------------------------------------------------------------------------
# Workload construction: fanout comes from predicates, never from B assignment.
# ---------------------------------------------------------------------------


def build_queries(periods: int) -> tuple[QuerySpec, ...]:
    # These are fixed semantic query templates.  Their overlaps, combined with
    # partition metadata, determine object sharing.  The windows scale only with
    # the number of configured time buckets; no target fanout is consulted.
    recent_start = max(0, periods - 2)
    promo_start = max(0, periods - 4)
    air_start = 1 if periods > 1 else 0

    return (
        QuerySpec(
            query_id=0,
            name="global_rollup",
            all_facts=True,
        ),
        QuerySpec(
            query_id=1,
            name="recent_window",
            min_period=recent_start,
        ),
        QuerySpec(
            query_id=2,
            name="regional_slice",
            regions=("AMERICA", "EUROPE"),
        ),
        QuerySpec(
            query_id=3,
            name="promotion_slice",
            min_period=promo_start,
            categories=("PROMO",),
        ),
        QuerySpec(
            query_id=4,
            name="air_freight_slice",
            min_period=air_start,
            ship_modes=("AIR",),
        ),
        QuerySpec(
            query_id=5,
            name="asia_bulk_slice",
            regions=("ASIA",),
            categories=("BULK",),
        ),
    )


def query_matches_fact(query: QuerySpec, obj: ObjectSpec) -> bool:
    if obj.role != "fact":
        return False
    if query.all_facts:
        return True
    if query.min_period is not None and obj.period < query.min_period:
        return False
    if query.max_period is not None and obj.period > query.max_period:
        return False
    if query.regions and obj.region not in query.regions:
        return False
    if query.categories and obj.category not in query.categories:
        return False
    if query.ship_modes and obj.ship_mode not in query.ship_modes:
        return False
    return True


def build_plan(periods: int, dimension_objects: int) -> WorkloadPlan:
    queries = build_queries(periods)
    objects: list[ObjectSpec] = []

    for period in range(periods):
        for region in REGIONS:
            for category in CATEGORIES:
                for ship_mode in SHIP_MODES:
                    objects.append(
                        ObjectSpec(
                            object_index=len(objects),
                            role="fact",
                            period=period,
                            region=region,
                            category=category,
                            ship_mode=ship_mode,
                        )
                    )

    fact_ids = tuple(obj.object_index for obj in objects)

    for i in range(dimension_objects):
        objects.append(
            ObjectSpec(
                object_index=len(objects),
                role="dimension",
                dimension_name=f"dimension_{i}",
            )
        )

    dim_ids = tuple(
        obj.object_index for obj in objects if obj.role == "dimension"
    )

    assignments: list[list[int]] = [[] for _ in queries]
    consumers: list[list[int]] = [[] for _ in objects]

    for query in queries:
        selected: list[int] = []
        for obj in objects:
            if obj.role == "fact" and query_matches_fact(query, obj):
                selected.append(obj.object_index)
            elif obj.role == "dimension" and query.needs_dimensions:
                selected.append(obj.object_index)

        assignments[query.query_id] = selected
        for object_index in selected:
            consumers[object_index].append(query.query_id)

    fanouts = [len(x) for x in consumers]
    fact_fanouts = [len(consumers[i]) for i in fact_ids]
    dim_fanouts = [len(consumers[i]) for i in dim_ids]

    return WorkloadPlan(
        objects=tuple(objects),
        queries=queries,
        assignments=tuple(tuple(x) for x in assignments),
        consumers_by_object=tuple(tuple(x) for x in consumers),
        fact_object_ids=fact_ids,
        dimension_object_ids=dim_ids,
        fanout_histogram=tuple(sorted(Counter(fanouts).items())),
        fact_fanout_histogram=tuple(sorted(Counter(fact_fanouts).items())),
        dimension_fanout_histogram=tuple(sorted(Counter(dim_fanouts).items())),
    )


def role_expected_transfers(
    plan: WorkloadPlan,
    method: Method,
    role_ids: Iterable[int],
) -> int:
    ids = list(role_ids)
    if method.baseline_enabled:
        return sum(
            R for i in ids if len(plan.consumers_by_object[i]) > 0
        )
    return sum(
        min(len(plan.consumers_by_object[i]), R)
        for i in ids
        if len(plan.consumers_by_object[i]) > 0
    )


def workload_counts(plan: WorkloadPlan, method: Method) -> dict[str, int | float]:
    activated = [
        i for i, consumers in enumerate(plan.consumers_by_object) if consumers
    ]
    r_eligible = [
        i for i in activated if len(plan.consumers_by_object[i]) >= R
    ]
    expected_fact = role_expected_transfers(
        plan, method, plan.fact_object_ids
    )
    expected_dim = role_expected_transfers(
        plan, method, plan.dimension_object_ids
    )
    expected_total = expected_fact + expected_dim
    logical_edges = sum(len(x) for x in plan.assignments)

    return {
        "produced_objects": len(plan.objects),
        "fact_objects": len(plan.fact_object_ids),
        "dimension_objects": len(plan.dimension_object_ids),
        "activated_objects": len(activated),
        "r_eligible_objects": len(r_eligible),
        "logical_query_object_edges": logical_edges,
        "expected_fact_transfers": expected_fact,
        "expected_dimension_transfers": expected_dim,
        "expected_total_transfers": expected_total,
        "native_target_holders_per_activated_object": safe_div(
            expected_total, len(activated)
        ),
        "r_eligible_fraction": safe_div(len(r_eligible), len(activated)),
    }


def plan_rows(plan: WorkloadPlan) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for obj in plan.objects:
        consumers = plan.consumers_by_object[obj.object_index]
        rows.append({
            "object_index": obj.object_index,
            "role": obj.role,
            "period": obj.period,
            "region": obj.region,
            "category": obj.category,
            "ship_mode": obj.ship_mode,
            "dimension_name": obj.dimension_name,
            "natural_fanout": len(consumers),
            "consumer_queries": ";".join(str(x) for x in consumers),
            "consumer_query_names": ";".join(
                plan.queries[x].name for x in consumers
            ),
            "r_eligible": int(len(consumers) >= R),
        })
    return rows


# ---------------------------------------------------------------------------
# Ray application
# ---------------------------------------------------------------------------


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        object_index: int,
        role_tag: int,
        result_bytes: int,
        marker: str,
        token: str,
        work_ms: float,
        *padding: bytes,
    ) -> bytes:
        # Touch padding so it remains semantically part of the producer TaskSpec.
        if padding and padding[0]:
            _ = padding[0][0]

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"START,{time.time_ns()},{os.getpid()},"
                f"{token}:{object_index}\n"
            )

        if work_ms > 0:
            time.sleep(work_ms / 1000.0)

        prefix = object_index.to_bytes(8, "little", signed=False)
        role = bytes([role_tag & 0xFF])
        header = prefix + role
        value = header + b"x" * max(0, result_bytes - len(header))
        value = value[:result_bytes]

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"FINISH,{time.time_ns()},{os.getpid()},"
                f"{token}:{object_index}\n"
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

        def create_objects(
            self,
            object_specs: tuple[ObjectSpec, ...],
            result_bytes: int,
            task_spec_padding_bytes: int,
            inline_chunk_bytes: int,
            marker: str,
            token: str,
            producer_work_ms: float,
            producer_cpus_per_task: float,
        ) -> int:
            padding = build_padding(
                task_spec_padding_bytes,
                inline_chunk_bytes,
            )
            # CRITICAL: replay clears SOFT node affinity only.
            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.producer_node_id,
                soft=True,
            )
            self.refs = []
            for obj in object_specs:
                role_tag = 1 if obj.role == "fact" else 2
                self.refs.append(
                    produce.options(
                        scheduling_strategy=strategy,
                        num_cpus=producer_cpus_per_task,
                    ).remote(
                        obj.object_index,
                        role_tag,
                        result_bytes,
                        marker,
                        token,
                        producer_work_ms,
                        *padding,
                    )
                )
            return len(self.refs)

        def export_role(
            self,
            role_object_ids: tuple[int, ...],
            assignments: tuple[tuple[int, ...], ...],
            query_workers: list[Any],
            query_order: tuple[int, ...],
        ) -> int:
            allowed = set(int(x) for x in role_object_ids)
            calls = []
            exported_edges = 0
            for query_id in query_order:
                ids = [
                    i for i in assignments[query_id] if i in allowed
                ]
                if not ids:
                    continue
                # Nested refs are intentionally retained as ObjectRefs on the
                # borrower rather than being auto-dereferenced as task args.
                wrapped = [[self.refs[i]] for i in ids]
                calls.append(
                    query_workers[query_id].hold_many.remote(ids, wrapped)
                )
                exported_edges += len(ids)
            if calls:
                accepted = ray.get(calls)
                if sum(int(x) for x in accepted) != exported_edges:
                    raise RuntimeError("query-worker hold count mismatch")
            return exported_edges

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

        def ping(self) -> int:
            return os.getpid()

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=128)
    class QueryWorker:
        def __init__(self, query_id: int):
            self.query_id = int(query_id)
            self.refs: dict[int, ray.ObjectRef] = {}

        def hold_many(self, ids: list[int], wrapped_refs) -> int:
            if len(ids) != len(wrapped_refs):
                raise RuntimeError("ids/ref length mismatch")
            for object_index, wrapped in zip(ids, wrapped_refs):
                self.refs[int(object_index)] = wrapped[0]
            return len(ids)

        def held_ids(self) -> list[int]:
            return sorted(self.refs)

        def read_one(self, object_index: int) -> tuple[int, int, int]:
            value = ray.get(self.refs[int(object_index)])
            if not isinstance(value, (bytes, bytearray)) or len(value) < 9:
                raise RuntimeError("unexpected object payload")
            decoded = int.from_bytes(value[:8], "little", signed=False)
            role_tag = int(value[8])
            return decoded, len(value), role_tag

        def run_query(
            self,
            object_ids: tuple[int, ...],
            query_work_ms: float,
        ) -> dict[str, Any]:
            start = time.perf_counter()
            refs = [self.refs[int(i)] for i in object_ids]
            values = ray.get(refs) if refs else []

            decoded_sum = 0
            total_bytes = 0
            role_sum = 0
            for expected_id, value in zip(object_ids, values):
                if not isinstance(value, (bytes, bytearray)) or len(value) < 9:
                    raise RuntimeError("unexpected query input")
                decoded = int.from_bytes(value[:8], "little", signed=False)
                if decoded != int(expected_id):
                    raise RuntimeError(
                        f"query {self.query_id}: object id mismatch "
                        f"{decoded} != {expected_id}"
                    )
                decoded_sum += decoded
                total_bytes += len(value)
                role_sum += int(value[8])

            if query_work_ms > 0:
                time.sleep(query_work_ms / 1000.0)

            return {
                "query_id": self.query_id,
                "object_count": len(object_ids),
                "decoded_sum": decoded_sum,
                "total_bytes": total_bytes,
                "role_checksum": role_sum,
                "worker_elapsed_s": time.perf_counter() - start,
            }

        def ping(self) -> int:
            return os.getpid()

    return Owner, QueryWorker


def start_cluster(
    method: Method,
    args: argparse.Namespace,
    query_count: int,
) -> tuple[Cluster, Any, Any, list[Any]]:
    cluster = Cluster()
    object_store_bytes = args.object_store_mib * 1024 * 1024

    cluster.add_node(
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

    owner_node = cluster.add_node(
        num_cpus=1,
        object_store_memory=object_store_bytes,
        resources={"owner_node": 1},
    )

    producer_node = cluster.add_node(
        num_cpus=args.producer_cpus,
        object_store_memory=object_store_bytes,
        resources={"producer_node": 1},
    )

    query_nodes = [
        cluster.add_node(
            num_cpus=1,
            object_store_memory=object_store_bytes,
            resources={f"query_node_{i}": 1},
        )
        for i in range(query_count)
    ]

    return cluster, owner_node, producer_node, query_nodes


# ---------------------------------------------------------------------------
# Profiling / readiness helpers
# ---------------------------------------------------------------------------


def get_owner_profile(owner) -> dict[str, Any]:
    return profile_defaults(ray.get(owner.get_profile.remote()))


def wait_for_profile_target(
    owner,
    *,
    method: Method,
    target_transfers: int,
    timeout_s: float,
) -> tuple[dict[str, Any], bool, float]:
    key = (
        "witness_update_rpcs_completed"
        if method.baseline_enabled
        else "holder_admissions_committed"
    )
    start = time.perf_counter()
    deadline = time.monotonic() + timeout_s
    last = get_owner_profile(owner)
    while time.monotonic() < deadline:
        last = get_owner_profile(owner)
        if int(last.get(key, 0)) >= target_transfers:
            return last, True, time.perf_counter() - start
        time.sleep(0.03)
    return last, False, time.perf_counter() - start


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


def form_role_protection(
    *,
    owner,
    method: Method,
    role_ids: tuple[int, ...],
    assignments: tuple[tuple[int, ...], ...],
    query_workers: list[Any],
    query_order: tuple[int, ...],
    expected_cumulative_transfers: int,
    args: argparse.Namespace,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Activate one application role and wait only for its native protection target.

    Deliberately do NOT perform the stable/quiescent profiling wait here.  That
    observer wait is harness overhead and must not be charged to application
    protection-ready latency or no-failure throughput.  A single quiescence wait
    is performed later, outside the measured application critical path.
    """
    before = get_owner_profile(owner)
    start = time.perf_counter()
    exported_edges = ray.get(
        owner.export_role.remote(
            role_ids,
            assignments,
            query_workers,
            query_order,
        )
    )
    export_done_s = time.perf_counter() - start

    target_profile, target_reached, target_wait_s = wait_for_profile_target(
        owner,
        method=method,
        target_transfers=expected_cumulative_transfers,
        timeout_s=args.formation_timeout_seconds,
    )
    total_s = time.perf_counter() - start

    return target_profile, {
        "exported_edges": exported_edges,
        "export_done_s": export_done_s,
        "target_wait_s": target_wait_s,
        "formation_observation_s": total_s,
        "native_ready_s": total_s if target_reached else math.nan,
        "native_target_reached": int(target_reached),
        "profile_delta": profile_delta(target_profile, before),
    }


# ---------------------------------------------------------------------------
# No-failure query execution
# ---------------------------------------------------------------------------


def run_queries_no_failure(
    *,
    query_workers: list[Any],
    plan: WorkloadPlan,
    query_work_ms: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start = time.perf_counter()
    pending: dict[ray.ObjectRef, int] = {}
    for query in plan.queries:
        ref = query_workers[query.query_id].run_query.remote(
            plan.assignments[query.query_id],
            query_work_ms,
        )
        pending[ref] = query.query_id

    query_rows: list[dict[str, Any]] = []
    while pending:
        ready, _ = ray.wait(list(pending), num_returns=1)
        ref = ready[0]
        query_id = pending.pop(ref)
        result = ray.get(ref)
        completion_s = time.perf_counter() - start
        expected_ids = plan.assignments[query_id]
        expected_sum = sum(expected_ids)
        correct = (
            int(result["query_id"]) == query_id
            and int(result["object_count"]) == len(expected_ids)
            and int(result["decoded_sum"]) == expected_sum
        )
        query_rows.append({
            "query_id": query_id,
            "query_name": plan.queries[query_id].name,
            "object_count": len(expected_ids),
            "expected_id_sum": expected_sum,
            "observed_id_sum": int(result["decoded_sum"]),
            "total_bytes": int(result["total_bytes"]),
            "worker_elapsed_s": float(result["worker_elapsed_s"]),
            "completion_from_batch_start_s": completion_s,
            "correct": int(correct),
        })

    makespan = time.perf_counter() - start
    completion_latencies = [
        float(r["completion_from_batch_start_s"]) for r in query_rows
    ]
    logical_edges = sum(len(x) for x in plan.assignments)

    summary = {
        "query_count": len(plan.queries),
        "logical_query_object_edges": logical_edges,
        "query_makespan_s": makespan,
        "queries_per_s": safe_div(len(plan.queries), makespan),
        "logical_consumptions_per_s": safe_div(logical_edges, makespan),
        "query_latency_p50_s": percentile(completion_latencies, 0.50),
        "query_latency_p95_s": percentile(completion_latencies, 0.95),
        "all_queries_correct": int(all(int(r["correct"]) for r in query_rows)),
    }
    return summary, query_rows


# ---------------------------------------------------------------------------
# Failure/recovery helpers
# ---------------------------------------------------------------------------


def wait_node_dead(node_id: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        found = False
        alive = False
        for node in ray.nodes():
            if node.get("NodeID") == node_id:
                found = True
                alive = bool(node.get("Alive"))
                break
        if found and not alive:
            return True
        time.sleep(0.05)
    return False


def collect_read(
    *,
    object_index: int,
    role: str,
    result_ref: ray.ObjectRef,
    expected_result_bytes: int,
    failure_perf: float,
    timeout_s: float,
) -> dict[str, Any]:
    try:
        decoded, result_len, role_tag = ray.get(result_ref, timeout=timeout_s)
        latency = time.perf_counter() - failure_perf
        expected_role_tag = 1 if role == "fact" else 2
        correct = (
            int(decoded) == object_index
            and int(result_len) == expected_result_bytes
            and int(role_tag) == expected_role_tag
        )
        return {
            "object_index": object_index,
            "success": 1,
            "correct": int(correct),
            "latency_s": latency,
            "error": "",
        }
    except Exception as exc:
        return {
            "object_index": object_index,
            "success": 0,
            "correct": 0,
            "latency_s": time.perf_counter() - failure_perf,
            "error": f"{type(exc).__name__}: {exc}",
        }


def recover_live_demand(
    *,
    requesters: dict[int, int],
    query_workers: list[Any],
    objects: tuple[ObjectSpec, ...],
    expected_result_bytes: int,
    failure_perf: float,
    timeout_s: float,
    concurrency: int,
) -> list[dict[str, Any]]:
    if not requesters:
        return []

    result_refs = {
        object_index: query_workers[query_id].read_one.remote(object_index)
        for object_index, query_id in requesters.items()
    }

    workers = min(max(1, concurrency), len(result_refs))
    out: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                collect_read,
                object_index=object_index,
                role=objects[object_index].role,
                result_ref=result_ref,
                expected_result_bytes=expected_result_bytes,
                failure_perf=failure_perf,
                timeout_s=timeout_s,
            )
            for object_index, result_ref in result_refs.items()
        ]
        for future in concurrent.futures.as_completed(futures):
            out.append(future.result())
    return out


def marker_replay_stats(marker: Path, failure_wall_ns: int) -> dict[str, Any]:
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


def choose_requesters(
    plan: WorkloadPlan,
    killed_query_ids: set[int],
) -> dict[int, int]:
    # Choose one genuine surviving application consumer for every object that
    # still has live demand.  This introduces no observer/extra borrower.
    requesters: dict[int, int] = {}
    for object_index, consumers in enumerate(plan.consumers_by_object):
        live = [q for q in consumers if q not in killed_query_ids]
        if live:
            # Deterministic and balanced enough because query predicates differ.
            requesters[object_index] = live[object_index % len(live)]
    return requesters


# ---------------------------------------------------------------------------
# One complete run
# ---------------------------------------------------------------------------


def run_one(
    args: argparse.Namespace,
    *,
    method: Method,
    spec_size: SpecSize,
    plan: WorkloadPlan,
    repetition: int,
    phase: str,
    prekill_query_count: int,
    killed_query_ids: tuple[int, ...],
    query_order: tuple[int, ...],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_realistic_multiquery_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster, owner_node, producer_node, query_nodes = start_cluster(
            method,
            args,
            len(plan.queries),
        )
        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        wait_for_cluster(
            ray,
            3 + len(plan.queries),
            args.cluster_timeout_seconds,
        )

        Owner, QueryWorker = make_remote_types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(producer_node.node_id)
        query_workers = [
            QueryWorker.options(
                resources={f"query_node_{i}": 0.01},
                num_cpus=0,
            ).remote(i)
            for i in range(len(plan.queries))
        ]

        ray.get(
            [owner.ping.remote()]
            + [worker.ping.remote() for worker in query_workers]
        )
        ray.get(owner.reset_profile.remote())

        token = uuid.uuid4().hex
        counts = workload_counts(plan, method)
        workload_start = time.perf_counter()

        created = ray.get(
            owner.create_objects.remote(
                plan.objects,
                args.result_bytes,
                spec_size.padding_bytes,
                args.inline_chunk_bytes,
                str(marker),
                token,
                args.producer_work_ms,
                args.producer_cpus_per_task,
            )
        )
        if created != len(plan.objects):
            raise RuntimeError(
                f"owner created {created}/{len(plan.objects)} objects"
            )
        creation_submit_s = time.perf_counter() - workload_start

        # Phase A: selective fact-block sharing.
        fact_expected_cumulative = int(counts["expected_fact_transfers"])
        fact_profile, fact_formation = form_role_protection(
            owner=owner,
            method=method,
            role_ids=plan.fact_object_ids,
            assignments=plan.assignments,
            query_workers=query_workers,
            query_order=query_order,
            expected_cumulative_transfers=fact_expected_cumulative,
            args=args,
        )

        # Phase B: dense dimension broadcast.  This is an internal control regime
        # where every dimension object has B = number of query workers >= R.
        total_expected = int(counts["expected_total_transfers"])
        final_profile, dim_formation = form_role_protection(
            owner=owner,
            method=method,
            role_ids=plan.dimension_object_ids,
            assignments=plan.assignments,
            query_workers=query_workers,
            query_order=query_order,
            expected_cumulative_transfers=total_expected,
            args=args,
        )

        formation_start_to_ready_s = (
            float(fact_formation["formation_observation_s"])
            + float(dim_formation["formation_observation_s"])
        )
        all_native_targets_reached = (
            int(fact_formation["native_target_reached"]) == 1
            and int(dim_formation["native_target_reached"]) == 1
        )

        # Ensure every original exists before measuring no-failure query service
        # or killing the result node.  fetch_local=False avoids pulling originals
        # to the owner node.
        ready_count = ray.get(
            owner.wait_all_ready.remote(args.task_ready_timeout_seconds)
        )
        if ready_count != len(plan.objects):
            raise TimeoutError(
                f"only {ready_count}/{len(plan.objects)} originals became ready"
            )
        workload_ready_s = time.perf_counter() - workload_start

        # Settle profiling counters once.  This observer delay is recorded and
        # subtracted from the no-failure end-to-end application time below.
        settle_start = time.perf_counter()
        final_profile, all_quiescent = wait_for_profile_quiescence(
            owner,
            timeout_s=args.profile_quiescence_timeout_seconds,
            stable_s=args.profile_stable_seconds,
        )
        profile_settle_s = time.perf_counter() - settle_start

        observed_transfers = full_lineage_transfers(method, final_profile)
        observed_completed = full_lineage_transfers_completed(
            method, final_profile
        )
        expected_transfers = int(counts["expected_total_transfers"])
        activated = int(counts["activated_objects"])

        full_bytes = int(final_profile["task_spec_bytes_sent"])
        manifest_bytes = int(final_profile["manifest_bytes_sent"])
        candidate_report_bytes = int(
            final_profile.get("candidate_report_bytes_received", 0)
        )
        # Protocol metadata excludes complete TaskSpecs.  Candidate-report bytes
        # and manifest bytes are counted separately below as well so the paper
        # can state exactly what is included in this aggregate.
        metadata_bytes = manifest_bytes + candidate_report_bytes
        control_requests = recovery_control_requests(final_profile)
        full_mib = full_bytes / (1024.0 * 1024.0)
        metadata_mib = metadata_bytes / (1024.0 * 1024.0)

        # Role deltas let the same realistic workload expose both selective and
        # dense/broadcast regimes without a synthetic B sweep.
        fact_delta = fact_formation["profile_delta"]
        # Attribute everything after the fact target snapshot to the dimension
        # stage. Because fact protection reached its commit target before the
        # dimension exports begin, this also captures any late asynchronous bytes
        # without losing them from the role accounting.
        dim_delta = profile_delta(final_profile, fact_profile)
        fact_lineage_bytes = int(fact_delta["task_spec_bytes_sent"])
        dim_lineage_bytes = int(dim_delta["task_spec_bytes_sent"])

        common_row: dict[str, Any] = {
            "phase": phase,
            "repetition": repetition,
            "prekill_query_nodes": prekill_query_count,
            "killed_query_ids": ";".join(str(x) for x in killed_query_ids),
            "query_launch_order": ";".join(str(x) for x in query_order),
            "target_holders": R,
            "task_spec_padding_name": spec_size.name,
            "task_spec_padding_bytes": spec_size.padding_bytes,
            "result_bytes": args.result_bytes,
            "periods": args.periods,
            "query_count": len(plan.queries),
            "produced_objects": int(counts["produced_objects"]),
            "fact_objects": int(counts["fact_objects"]),
            "dimension_objects": int(counts["dimension_objects"]),
            "activated_objects": activated,
            "r_eligible_objects": int(counts["r_eligible_objects"]),
            "r_eligible_fraction": float(counts["r_eligible_fraction"]),
            "logical_query_object_edges": int(counts["logical_query_object_edges"]),
            "native_target_holders_per_activated_object": float(
                counts["native_target_holders_per_activated_object"]
            ),
            "expected_fact_full_lineage_transfers": int(
                counts["expected_fact_transfers"]
            ),
            "expected_dimension_full_lineage_transfers": int(
                counts["expected_dimension_transfers"]
            ),
            "expected_full_lineage_transfers": expected_transfers,
            "observed_full_lineage_transfers": observed_transfers,
            "observed_full_lineage_transfers_completed": observed_completed,
            "native_target_reached": int(all_native_targets_reached),
            "copy_count_valid": int(observed_transfers == expected_transfers),
            "full_lineage_bytes_total": full_bytes,
            "full_lineage_mib_total": full_mib,
            "full_lineage_mib_per_1000_objects": safe_div(
                full_mib * 1000.0, len(plan.objects)
            ),
            "full_lineage_bytes_per_activated_object": safe_div(
                full_bytes, activated
            ),
            "measured_task_spec_bytes_per_full_transfer": safe_div(
                full_bytes, observed_transfers
            ),
            "fact_full_lineage_bytes": fact_lineage_bytes,
            "dimension_full_lineage_bytes": dim_lineage_bytes,
            "fact_full_lineage_transfers": full_lineage_transfers(
                method, fact_delta
            ),
            "dimension_full_lineage_transfers": full_lineage_transfers(
                method, dim_delta
            ),
            "manifest_bytes_sent_total": manifest_bytes,
            "candidate_report_bytes_received_total": candidate_report_bytes,
            "recovery_metadata_bytes_total": metadata_bytes,
            "recovery_metadata_mib_total": metadata_mib,
            "control_requests_total": control_requests,
            "control_requests_per_activated_object": safe_div(
                control_requests, activated
            ),
            "achieved_full_lineage_holders_per_activated_object": safe_div(
                observed_completed, activated
            ),
            "profile_max_non_owner_holders": int(
                final_profile.get("max_non_owner_holders", 0)
            ),
            "creation_submit_s": creation_submit_s,
            "fact_native_protection_observation_s": float(
                fact_formation["formation_observation_s"]
            ),
            "dimension_native_protection_observation_s": float(
                dim_formation["formation_observation_s"]
            ),
            "native_protection_ready_time_s": (
                formation_start_to_ready_s
                if all_native_targets_reached else math.nan
            ),
            "profile_quiescent": int(all_quiescent),
            "profile_settle_time_s": profile_settle_s,
            "workload_ready_time_s": workload_ready_s,
            "end_to_end_workload_time_s": math.nan,
            "end_to_end_queries_per_s": math.nan,
            "end_to_end_logical_consumptions_per_s": math.nan,
            "end_to_end_query_latency_p50_s": math.nan,
            "end_to_end_query_latency_p95_s": math.nan,
            "owner_node_dead_confirmed": 0,
            "producer_node_dead_confirmed": 0,
            "live_demand_objects": 0,
            "recovery_requested_objects": 0,
            "recovery_success_count": 0,
            "recovery_correct_count": 0,
            "recovery_success_rate": math.nan,
            "recovery_correct_rate": math.nan,
            "recovery_latency_mean_s": math.nan,
            "recovery_latency_p50_s": math.nan,
            "recovery_latency_p95_s": math.nan,
            "recovery_latency_p99_s": math.nan,
            "recovery_latency_max_s": math.nan,
            "post_failure_replay_count": 0,
            "failure_to_first_replay_s": math.nan,
            "failure_to_last_replay_s": math.nan,
            "duplicate_replays": 0,
            "replay_per_successful_recovery": math.nan,
            "queries_per_s": math.nan,
            "logical_consumptions_per_s": math.nan,
            "query_makespan_s": math.nan,
            "query_latency_p50_s": math.nan,
            "query_latency_p95_s": math.nan,
            "all_queries_correct": 0,
        }

        object_rows: list[dict[str, Any]] = []
        query_rows: list[dict[str, Any]] = []

        if phase == "nofailure":
            query_batch_start_from_workload_s = time.perf_counter() - workload_start
            perf, query_rows = run_queries_no_failure(
                query_workers=query_workers,
                plan=plan,
                query_work_ms=args.query_work_ms,
            )
            raw_end_to_end_s = time.perf_counter() - workload_start
            # The stable-profile observer wait is not application work. It occurs
            # in a non-overlapping gap immediately before query execution, so
            # subtracting it gives a clean creation+protection+query wall time.
            end_to_end_s = max(0.0, raw_end_to_end_s - profile_settle_s)
            perf.update({
                "end_to_end_workload_time_s": end_to_end_s,
                "end_to_end_queries_per_s": safe_div(len(plan.queries), end_to_end_s),
                "end_to_end_logical_consumptions_per_s": safe_div(
                    int(counts["logical_query_object_edges"]), end_to_end_s
                ),
                "end_to_end_query_latency_p50_s": max(
                    0.0,
                    query_batch_start_from_workload_s
                    - profile_settle_s
                    + float(perf["query_latency_p50_s"]),
                ),
                "end_to_end_query_latency_p95_s": max(
                    0.0,
                    query_batch_start_from_workload_s
                    - profile_settle_s
                    + float(perf["query_latency_p95_s"]),
                ),
            })
            common_row.update(perf)

        elif phase == "recovery":
            killed_set = set(killed_query_ids)
            for query_id in killed_query_ids:
                cluster.remove_node(
                    query_nodes[query_id],
                    allow_graceful=False,
                )
            if killed_query_ids:
                time.sleep(args.prekill_settle_seconds)

            requesters = choose_requesters(plan, killed_set)

            failure_wall_ns = time.time_ns()
            failure_perf = time.perf_counter()
            cluster.remove_node(owner_node, allow_graceful=False)
            cluster.remove_node(producer_node, allow_graceful=False)

            owner_dead = wait_node_dead(
                owner_node.node_id,
                args.node_death_timeout_seconds,
            )
            producer_dead = wait_node_dead(
                producer_node.node_id,
                args.node_death_timeout_seconds,
            )

            results = recover_live_demand(
                requesters=requesters,
                query_workers=query_workers,
                objects=plan.objects,
                expected_result_bytes=args.result_bytes,
                failure_perf=failure_perf,
                timeout_s=args.recovery_timeout_seconds,
                concurrency=args.recovery_concurrency,
            )

            by_index = {int(r["object_index"]): r for r in results}
            latencies = [
                float(r["latency_s"])
                for r in results
                if int(r["success"]) == 1
            ]
            success_count = sum(int(r["success"]) for r in results)
            correct_count = sum(int(r["correct"]) for r in results)
            replay = marker_replay_stats(marker, failure_wall_ns)

            common_row.update({
                "owner_node_dead_confirmed": int(owner_dead),
                "producer_node_dead_confirmed": int(producer_dead),
                "live_demand_objects": len(requesters),
                "recovery_requested_objects": len(requesters),
                "recovery_success_count": success_count,
                "recovery_correct_count": correct_count,
                "recovery_success_rate": safe_div(success_count, len(requesters)),
                "recovery_correct_rate": safe_div(correct_count, len(requesters)),
                "recovery_latency_mean_s": (
                    statistics.fmean(latencies) if latencies else math.nan
                ),
                "recovery_latency_p50_s": percentile(latencies, 0.50),
                "recovery_latency_p95_s": percentile(latencies, 0.95),
                "recovery_latency_p99_s": percentile(latencies, 0.99),
                "recovery_latency_max_s": max(latencies) if latencies else math.nan,
                **replay,
            })
            common_row["duplicate_replays"] = max(
                0,
                int(common_row["post_failure_replay_count"]) - success_count,
            )
            common_row["replay_per_successful_recovery"] = safe_div(
                int(common_row["post_failure_replay_count"]),
                success_count,
            )

            for obj in plan.objects:
                consumers = plan.consumers_by_object[obj.object_index]
                live_consumers = [q for q in consumers if q not in killed_set]
                has_live_demand = obj.object_index in requesters
                result = by_index.get(obj.object_index, {
                    "success": 0,
                    "correct": 0,
                    "latency_s": math.nan,
                    "error": "NO_LIVE_APPLICATION_CONSUMER",
                })
                object_rows.append({
                    "phase": phase,
                    "repetition": repetition,
                    "prekill_query_nodes": prekill_query_count,
                    "killed_query_ids": ";".join(
                        str(x) for x in killed_query_ids
                    ),
                    "task_spec_padding_name": spec_size.name,
                    "task_spec_padding_bytes": spec_size.padding_bytes,
                    "object_index": obj.object_index,
                    "role": obj.role,
                    "natural_fanout": len(consumers),
                    "consumer_queries": ";".join(str(x) for x in consumers),
                    "live_consumer_queries": ";".join(
                        str(x) for x in live_consumers
                    ),
                    "has_live_demand": int(has_live_demand),
                    "requester_query": (
                        requesters[obj.object_index]
                        if has_live_demand else -1
                    ),
                    "success": int(result["success"]) if has_live_demand else 0,
                    "correct": int(result["correct"]),
                    "latency_s": float(result["latency_s"]),
                    "error": str(result["error"]),
                })

        else:
            raise ValueError(f"unknown phase {phase}")

        # Attach structured profile fields after the phase.  For recovery runs we
        # intentionally use the pre-failure profile snapshot because the owner is
        # gone after failure.
        for key in PROFILE_KEYS:
            common_row[f"profile_{key}"] = final_profile.get(key, 0)

        run_row = add_method_columns(common_row, method)
        object_rows = [add_method_columns(r, method) for r in object_rows]
        query_rows = [
            add_method_columns({
                **r,
                "phase": phase,
                "repetition": repetition,
                "prekill_query_nodes": prekill_query_count,
                "task_spec_padding_name": spec_size.name,
                "task_spec_padding_bytes": spec_size.padding_bytes,
            }, method)
            for r in query_rows
        ]

        if phase == "nofailure":
            print(
                f"  copies={observed_transfers}/{expected_transfers} "
                f"lineage={full_mib:.2f} MiB "
                f"ctrl={control_requests} "
                f"throughput={run_row['logical_consumptions_per_s']:.2f} edges/s "
                f"p95={run_row['query_latency_p95_s']:.3f}s"
            )
        else:
            print(
                f"  copies={observed_transfers}/{expected_transfers} "
                f"lineage={full_mib:.2f} MiB "
                f"ctrl={control_requests} "
                f"recovery={run_row['recovery_success_count']}/"
                f"{run_row['recovery_requested_objects']} "
                f"p95={run_row['recovery_latency_p95_s']:.3f}s "
                f"replays={run_row['post_failure_replay_count']}"
            )

        return run_row, object_rows, query_rows

    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


SUMMARY_METRICS = [
    "full_lineage_mib_per_1000_objects",
    "full_lineage_bytes_per_activated_object",
    "measured_task_spec_bytes_per_full_transfer",
    "fact_full_lineage_bytes",
    "dimension_full_lineage_bytes",
    "fact_full_lineage_transfers",
    "dimension_full_lineage_transfers",
    "recovery_metadata_mib_total",
    "control_requests_per_activated_object",
    "achieved_full_lineage_holders_per_activated_object",
    "native_target_holders_per_activated_object",
    "r_eligible_fraction",
    "native_protection_ready_time_s",
    "fact_native_protection_observation_s",
    "dimension_native_protection_observation_s",
    "workload_ready_time_s",
    "end_to_end_workload_time_s",
    "end_to_end_queries_per_s",
    "end_to_end_logical_consumptions_per_s",
    "end_to_end_query_latency_p50_s",
    "end_to_end_query_latency_p95_s",
    "queries_per_s",
    "logical_consumptions_per_s",
    "query_makespan_s",
    "query_latency_p50_s",
    "query_latency_p95_s",
    "recovery_success_rate",
    "recovery_correct_rate",
    "recovery_latency_mean_s",
    "recovery_latency_p50_s",
    "recovery_latency_p95_s",
    "recovery_latency_p99_s",
    "recovery_latency_max_s",
    "post_failure_replay_count",
    "replay_per_successful_recovery",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    groups = sorted({
        (
            row["phase"],
            row["method"],
            row["method_label"],
            row["task_spec_padding_name"],
            int(row["task_spec_padding_bytes"]),
            int(row["prekill_query_nodes"]),
        )
        for row in rows
    }, key=lambda x: (x[0], x[4], x[5], x[1]))

    for phase, method, label, pad_name, pad_bytes, prekill in groups:
        subset = [
            r for r in rows
            if r["phase"] == phase
            and r["method"] == method
            and r["task_spec_padding_name"] == pad_name
            and int(r["task_spec_padding_bytes"]) == pad_bytes
            and int(r["prekill_query_nodes"]) == prekill
        ]
        summary: dict[str, Any] = {
            "phase": phase,
            "method": method,
            "method_label": label,
            "task_spec_padding_name": pad_name,
            "task_spec_padding_bytes": pad_bytes,
            "prekill_query_nodes": prekill,
            "repetitions": len(subset),
            "all_copy_counts_valid": int(
                all(int(r["copy_count_valid"]) == 1 for r in subset)
            ),
            "all_native_targets_reached": int(
                all(int(r["native_target_reached"]) == 1 for r in subset)
            ),
            "all_profiles_quiescent": int(
                all(int(r["profile_quiescent"]) == 1 for r in subset)
            ),
            "all_queries_correct": int(
                all(
                    int(r["all_queries_correct"]) == 1
                    for r in subset if phase == "nofailure"
                )
            ) if phase == "nofailure" else "",
        }

        for metric in SUMMARY_METRICS:
            values = finite_values(float(r[metric]) for r in subset)
            if values:
                mean, ci = mean_ci95(values)
            else:
                mean, ci = math.nan, math.nan
            summary[f"{metric}_mean"] = mean
            summary[f"{metric}_ci95"] = ci

        out.append(summary)
    return out


def paired_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    keys = sorted({
        (
            r["phase"],
            r["task_spec_padding_name"],
            int(r["task_spec_padding_bytes"]),
            int(r["prekill_query_nodes"]),
        )
        for r in summary
    }, key=lambda x: (x[0], x[2], x[3]))

    for phase, pad_name, pad_bytes, prekill in keys:
        s = next(
            r for r in summary
            if r["phase"] == phase
            and r["method"] == "succession"
            and r["task_spec_padding_name"] == pad_name
            and int(r["prekill_query_nodes"]) == prekill
        )
        b = next(
            r for r in summary
            if r["phase"] == phase
            and r["method"] == "witness_baseline"
            and r["task_spec_padding_name"] == pad_name
            and int(r["prekill_query_nodes"]) == prekill
        )

        s_bytes = float(s["full_lineage_mib_per_1000_objects_mean"])
        b_bytes = float(b["full_lineage_mib_per_1000_objects_mean"])
        s_ctrl = float(s["control_requests_per_activated_object_mean"])
        b_ctrl = float(b["control_requests_per_activated_object_mean"])

        row = {
            "phase": phase,
            "task_spec_padding_name": pad_name,
            "task_spec_padding_bytes": pad_bytes,
            "prekill_query_nodes": prekill,
            "baseline_over_succession_lineage_cost": safe_div(b_bytes, s_bytes),
            "succession_lineage_saving_percent": (
                100.0 * safe_div(b_bytes - s_bytes, b_bytes)
                if b_bytes else math.nan
            ),
            "succession_over_baseline_control_requests": safe_div(s_ctrl, b_ctrl),
            "succession_achieved_holders_per_object": float(
                s["achieved_full_lineage_holders_per_activated_object_mean"]
            ),
            "baseline_achieved_holders_per_object": float(
                b["achieved_full_lineage_holders_per_activated_object_mean"]
            ),
            "succession_native_target_holders_per_object": float(
                s["native_target_holders_per_activated_object_mean"]
            ),
            "baseline_native_target_holders_per_object": float(
                b["native_target_holders_per_activated_object_mean"]
            ),
            "r_eligible_fraction": float(s["r_eligible_fraction_mean"]),
            "succession_native_protection_ready_s": float(
                s["native_protection_ready_time_s_mean"]
            ),
            "baseline_native_protection_ready_s": float(
                b["native_protection_ready_time_s_mean"]
            ),
        }

        if phase == "nofailure":
            s_thr = float(s["logical_consumptions_per_s_mean"])
            b_thr = float(b["logical_consumptions_per_s_mean"])
            row.update({
                "succession_logical_consumptions_per_s": s_thr,
                "baseline_logical_consumptions_per_s": b_thr,
                "succession_over_baseline_query_throughput": safe_div(s_thr, b_thr),
                "succession_query_p95_s": float(s["query_latency_p95_s_mean"]),
                "baseline_query_p95_s": float(b["query_latency_p95_s_mean"]),
                # Supplemental application-wall metrics. These include the
                # benchmark's protection-target observation barrier and should
                # not replace the separately reported formation metric.
                "succession_end_to_end_logical_consumptions_per_s": float(
                    s["end_to_end_logical_consumptions_per_s_mean"]
                ),
                "baseline_end_to_end_logical_consumptions_per_s": float(
                    b["end_to_end_logical_consumptions_per_s_mean"]
                ),
                "succession_end_to_end_query_p95_s": float(
                    s["end_to_end_query_latency_p95_s_mean"]
                ),
                "baseline_end_to_end_query_p95_s": float(
                    b["end_to_end_query_latency_p95_s_mean"]
                ),
            })
        else:
            row.update({
                "succession_recovery_success_rate": float(
                    s["recovery_success_rate_mean"]
                ),
                "baseline_recovery_success_rate": float(
                    b["recovery_success_rate_mean"]
                ),
                "success_rate_delta_succession_minus_baseline": (
                    float(s["recovery_success_rate_mean"])
                    - float(b["recovery_success_rate_mean"])
                ),
                "succession_recovery_p95_s": float(
                    s["recovery_latency_p95_s_mean"]
                ),
                "baseline_recovery_p95_s": float(
                    b["recovery_latency_p95_s_mean"]
                ),
            })
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Experiment driver
# ---------------------------------------------------------------------------


def run_experiment(args: argparse.Namespace) -> None:
    plan = build_plan(args.periods, args.dimension_objects)
    outdir = Path(args.output_dir)
    write_csv(outdir / "realistic_plan.csv", plan_rows(plan))

    print("\nNatural workload fanout (all objects):", dict(plan.fanout_histogram))
    print("Natural fact-block fanout:", dict(plan.fact_fanout_histogram))
    print("Dense dimension fanout:", dict(plan.dimension_fanout_histogram))

    # The graph is exactly the same across methods and TaskSpec sizes.
    all_rows: list[dict[str, Any]] = []
    all_object_rows: list[dict[str, Any]] = []
    all_query_rows: list[dict[str, Any]] = []

    query_ids = list(range(len(plan.queries)))

    for repetition in range(1, args.repetitions + 1):
        launch_rng = random.Random(args.seed + 1000 * repetition)
        query_order_list = query_ids[:]
        launch_rng.shuffle(query_order_list)
        query_order = tuple(query_order_list)

        kill_rng = random.Random(args.seed + 100000 * repetition)
        kill_order = query_ids[:]
        kill_rng.shuffle(kill_order)

        cases: list[tuple[str, int, tuple[int, ...], Method, SpecSize]] = []
        for spec in args.task_spec_padding:
            if "nofailure" in args.phases:
                for method in methods():
                    cases.append(("nofailure", 0, (), method, spec))

            if "recovery" in args.phases:
                for k in args.prekill_query_counts:
                    killed = tuple(sorted(kill_order[:k]))
                    for method in methods():
                        cases.append(("recovery", k, killed, method, spec))

        if not args.fixed_order:
            random.Random(args.seed + 1000000 * repetition).shuffle(cases)

        for phase, prekill, killed, method, spec in cases:
            print(
                f"\nrep={repetition} phase={phase} method={method.label} "
                f"padding={spec.name} prekill_queries={prekill} "
                f"killed={list(killed)} launch_order={list(query_order)}"
            )
            row, object_rows, query_rows = run_one(
                args,
                method=method,
                spec_size=spec,
                plan=plan,
                repetition=repetition,
                phase=phase,
                prekill_query_count=prekill,
                killed_query_ids=killed,
                query_order=query_order,
            )
            all_rows.append(row)
            all_object_rows.extend(object_rows)
            all_query_rows.extend(query_rows)

    write_csv(outdir / "realistic_runs.csv", all_rows)
    if all_object_rows:
        write_csv(outdir / "realistic_objects.csv", all_object_rows)
    if all_query_rows:
        write_csv(outdir / "realistic_query_runs.csv", all_query_rows)

    summary = summarize(all_rows)
    write_csv(outdir / "realistic_summary.csv", summary)
    write_csv(outdir / "realistic_paired.csv", paired_rows(summary))


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def plot_results(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    outdir = Path(args.output_dir)
    plan_csv = read_csv(outdir / "realistic_plan.csv")
    summary = read_csv(outdir / "realistic_summary.csv")
    plotdir = outdir / "plots"
    plotdir.mkdir(parents=True, exist_ok=True)

    method_specs = [
        ("succession", "Recovery Succession"),
        ("witness_baseline", "Lazy fixed-R baseline"),
    ]

    # 1. Natural fanout distribution.  This is the key workload-characterization
    # figure proving that B was generated by the application, not configured.
    fanout_counts = Counter(int(r["natural_fanout"]) for r in plan_csv)
    fact_counts = Counter(
        int(r["natural_fanout"]) for r in plan_csv if r["role"] == "fact"
    )
    dim_counts = Counter(
        int(r["natural_fanout"]) for r in plan_csv if r["role"] == "dimension"
    )
    xs = sorted(fanout_counts)
    width = 0.35
    plt.figure(figsize=(7.6, 4.8))
    plt.bar([x - width / 2 for x in xs], [fact_counts[x] for x in xs], width=width, label="Fact blocks")
    plt.bar([x + width / 2 for x in xs], [dim_counts[x] for x in xs], width=width, label="Dimension blocks")
    plt.xlabel("Distinct downstream query workers B (naturally observed)")
    plt.ylabel("Objects")
    plt.xticks(xs)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "natural_fanout_histogram.png", dpi=200)
    plt.close()

    # Helper: for state cost use nofailure if present, else the least severe
    # recovery case. State is established before failures and should agree.
    phase_for_state = "nofailure" if any(r["phase"] == "nofailure" for r in summary) else "recovery"
    state_rows = [r for r in summary if r["phase"] == phase_for_state]
    if phase_for_state == "recovery":
        min_k = min(int(r["prekill_query_nodes"]) for r in state_rows)
        state_rows = [r for r in state_rows if int(r["prekill_query_nodes"]) == min_k]

    padding_cases = sorted({
        (int(r["task_spec_padding_bytes"]), r["task_spec_padding_name"])
        for r in state_rows
    })
    x_positions = list(range(len(padding_cases)))
    x_labels = [name for _, name in padding_cases]

    # 2. Full-lineage traffic vs TaskSpec size.
    plt.figure(figsize=(7.6, 4.8))
    for method, label in method_specs:
        ys, es = [], []
        for pad_bytes, pad_name in padding_cases:
            row = next(
                r for r in state_rows
                if r["method"] == method
                and r["task_spec_padding_name"] == pad_name
                and int(r["task_spec_padding_bytes"]) == pad_bytes
            )
            ys.append(float(row["full_lineage_mib_per_1000_objects_mean"]))
            es.append(float(row["full_lineage_mib_per_1000_objects_ci95"]))
        plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
    plt.xlabel("Producer TaskSpec padding")
    plt.ylabel("Full-lineage MiB / 1000 produced objects")
    plt.xticks(x_positions, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "lineage_cost_vs_taskspec.png", dpi=200)
    plt.close()

    # 3. Internal falsification/control: selective fact blocks should show an
    # amplification gap, while dense dimension broadcasts (B >= R) should not.
    # Use the largest TaskSpec case so tiny fixed protocol overheads do not
    # dominate the ratio.
    largest_state_pad = max(int(r["task_spec_padding_bytes"]) for r in state_rows)
    role_state = [
        r for r in state_rows
        if int(r["task_spec_padding_bytes"]) == largest_state_pad
    ]
    role_labels = ["Selective fact blocks", "Broadcast dimensions"]
    role_metric_names = ["fact_full_lineage_bytes_mean", "dimension_full_lineage_bytes_mean"]
    role_ratios = []
    for metric_name in role_metric_names:
        s_row = next(r for r in role_state if r["method"] == "succession")
        b_row = next(r for r in role_state if r["method"] == "witness_baseline")
        role_ratios.append(
            safe_div(float(b_row[metric_name]), float(s_row[metric_name]))
        )
    plt.figure(figsize=(7.3, 4.8))
    plt.bar(role_labels, role_ratios)
    plt.axhline(1.0, linestyle="--", linewidth=1)
    plt.ylabel("Baseline / Succession full-lineage bytes")
    plt.xticks(rotation=8, ha="right")
    plt.tight_layout()
    plt.savefig(plotdir / "role_lineage_amplification.png", dpi=200)
    plt.close()

    # 4. Control request cost.
    plt.figure(figsize=(7.6, 4.8))
    for method, label in method_specs:
        ys, es = [], []
        for pad_bytes, pad_name in padding_cases:
            row = next(
                r for r in state_rows
                if r["method"] == method
                and r["task_spec_padding_name"] == pad_name
                and int(r["task_spec_padding_bytes"]) == pad_bytes
            )
            ys.append(float(row["control_requests_per_activated_object_mean"]))
            es.append(float(row["control_requests_per_activated_object_ci95"]))
        plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
    plt.xlabel("Producer TaskSpec padding")
    plt.ylabel("Recovery-control request messages / activated object")
    plt.xticks(x_positions, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "control_requests_vs_taskspec.png", dpi=200)
    plt.close()

    # 5-6. No-failure performance.
    nofail = [r for r in summary if r["phase"] == "nofailure"]
    if nofail:
        plt.figure(figsize=(7.6, 4.8))
        for method, label in method_specs:
            ys, es = [], []
            for pad_bytes, pad_name in padding_cases:
                row = next(
                    r for r in nofail
                    if r["method"] == method
                    and r["task_spec_padding_name"] == pad_name
                    and int(r["task_spec_padding_bytes"]) == pad_bytes
                )
                ys.append(float(row["logical_consumptions_per_s_mean"]))
                es.append(float(row["logical_consumptions_per_s_ci95"]))
            plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
        plt.xlabel("Producer TaskSpec padding")
        plt.ylabel("Query-service logical object consumptions / s")
        plt.xticks(x_positions, x_labels)
        plt.legend()
        plt.tight_layout()
        plt.savefig(plotdir / "nofailure_throughput.png", dpi=200)
        plt.close()

        plt.figure(figsize=(7.6, 4.8))
        for method, label in method_specs:
            ys, es = [], []
            for pad_bytes, pad_name in padding_cases:
                row = next(
                    r for r in nofail
                    if r["method"] == method
                    and r["task_spec_padding_name"] == pad_name
                    and int(r["task_spec_padding_bytes"]) == pad_bytes
                )
                ys.append(float(row["query_latency_p95_s_mean"]))
                es.append(float(row["query_latency_p95_s_ci95"]))
            plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
        plt.xlabel("Producer TaskSpec padding")
        plt.ylabel("p95 query-service latency (s)")
        plt.xticks(x_positions, x_labels)
        plt.legend()
        plt.tight_layout()
        plt.savefig(plotdir / "nofailure_p95_latency.png", dpi=200)
        plt.close()

    # 7. Native-policy protection readiness.  The label deliberately says
    # native: holder targets differ for B<R and must be paired with plot 7.
    plt.figure(figsize=(7.6, 4.8))
    for method, label in method_specs:
        ys, es = [], []
        for pad_bytes, pad_name in padding_cases:
            row = next(
                r for r in state_rows
                if r["method"] == method
                and r["task_spec_padding_name"] == pad_name
                and int(r["task_spec_padding_bytes"]) == pad_bytes
            )
            ys.append(float(row["native_protection_ready_time_s_mean"]))
            es.append(float(row["native_protection_ready_time_s_ci95"]))
        plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
    plt.xlabel("Producer TaskSpec padding")
    plt.ylabel("Native-policy protection-ready time (s)")
    plt.xticks(x_positions, x_labels)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "native_protection_ready.png", dpi=200)
    plt.close()

    # 8. Achieved holders, mandatory companion to native readiness.
    plt.figure(figsize=(7.6, 4.8))
    for method, label in method_specs:
        ys, es = [], []
        for pad_bytes, pad_name in padding_cases:
            row = next(
                r for r in state_rows
                if r["method"] == method
                and r["task_spec_padding_name"] == pad_name
                and int(r["task_spec_padding_bytes"]) == pad_bytes
            )
            ys.append(float(row["achieved_full_lineage_holders_per_activated_object_mean"]))
            es.append(float(row["achieved_full_lineage_holders_per_activated_object_ci95"]))
        plt.errorbar(x_positions, ys, yerr=es, marker="o", capsize=3, label=label)
    plt.axhline(R, linestyle="--", linewidth=1, label=f"Fixed target R={R}")
    plt.xlabel("Producer TaskSpec padding")
    plt.ylabel("Achieved full-lineage holders / activated object")
    plt.xticks(x_positions, x_labels)
    plt.ylim(bottom=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig(plotdir / "achieved_holders.png", dpi=200)
    plt.close()

    recovery = [r for r in summary if r["phase"] == "recovery"]
    if recovery:
        # 9. p95 recovery latency for K=0 vs TaskSpec size. This isolates the
        # owner/result-node failure without conflating query-worker losses.
        if any(int(r["prekill_query_nodes"]) == 0 for r in recovery):
            k0 = [r for r in recovery if int(r["prekill_query_nodes"]) == 0]
            k0_cases = sorted({
                (int(r["task_spec_padding_bytes"]), r["task_spec_padding_name"])
                for r in k0
            })
            k0_x = list(range(len(k0_cases)))
            k0_labels = [name for _, name in k0_cases]
            plt.figure(figsize=(7.6, 4.8))
            for method, label in method_specs:
                ys, es = [], []
                for pad_bytes, pad_name in k0_cases:
                    row = next(
                        r for r in k0
                        if r["method"] == method
                        and r["task_spec_padding_name"] == pad_name
                    )
                    ys.append(float(row["recovery_latency_p95_s_mean"]))
                    es.append(float(row["recovery_latency_p95_s_ci95"]))
                plt.errorbar(k0_x, ys, yerr=es, marker="o", capsize=3, label=label)
            plt.xlabel("Producer TaskSpec padding")
            plt.ylabel("p95 failure-to-result latency (s), K=0")
            plt.xticks(k0_x, k0_labels)
            plt.legend()
            plt.tight_layout()
            plt.savefig(plotdir / "recovery_latency_p95.png", dpi=200)
            plt.close()

        # 10. Resilience characterization at the largest tested TaskSpec size.
        largest_pad = max(int(r["task_spec_padding_bytes"]) for r in recovery)
        resilience = [
            r for r in recovery
            if int(r["task_spec_padding_bytes"]) == largest_pad
        ]
        ks = sorted({int(r["prekill_query_nodes"]) for r in resilience})
        plt.figure(figsize=(7.6, 4.8))
        for method, label in method_specs:
            ys, es = [], []
            for k in ks:
                row = next(
                    r for r in resilience
                    if r["method"] == method
                    and int(r["prekill_query_nodes"]) == k
                )
                ys.append(100.0 * float(row["recovery_success_rate_mean"]))
                es.append(100.0 * float(row["recovery_success_rate_ci95"]))
            plt.errorbar(ks, ys, yerr=es, marker="o", capsize=3, label=label)
        plt.xlabel("Query-worker nodes failed before owner/result-node failure")
        plt.ylabel("Successful recoveries for surviving application demand (%)")
        plt.xticks(ks)
        plt.ylim(0, 105)
        plt.legend()
        plt.tight_layout()
        plt.savefig(plotdir / "recovery_success_vs_query_failures.png", dpi=200)
        plt.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


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
        default="gossip_benchmarks/results/21_realistic_multiquery_pipeline",
    )
    p.add_argument(
        "--phases",
        nargs="+",
        choices=["nofailure", "recovery"],
        default=["nofailure", "recovery"],
    )
    p.add_argument("--repetitions", type=int, default=1)
    p.add_argument(
        "--periods",
        type=int,
        default=6,
        help=(
            "Number of time partitions. Each period has 4 regions x 3 product "
            "categories x 2 ship modes = 24 fact blocks."
        ),
    )
    p.add_argument(
        "--dimension-objects",
        type=int,
        default=2,
        help="Small broadcast object count; every query naturally consumes them.",
    )
    p.add_argument(
        "--task-spec-padding",
        type=parse_size,
        nargs="+",
        default=[
            SpecSize("16KiB", 16 * 1024),
            SpecSize("256KiB", 256 * 1024),
        ],
        help="One or more NAME:BYTES producer TaskSpec padding cases.",
    )
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument(
        "--result-bytes",
        type=int,
        default=512 * 1024,
        help=(
            "Keep safely above Ray direct-call inline thresholds so killing the "
            "producer/result node forces replay instead of serving an inline copy."
        ),
    )
    p.add_argument("--producer-work-ms", type=float, default=1.0)
    p.add_argument("--query-work-ms", type=float, default=1.0)
    p.add_argument("--producer-cpus", type=int, default=4)
    p.add_argument("--producer-cpus-per-task", type=float, default=1.0)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--object-timeout-ms", type=int, default=1000)
    p.add_argument(
        "--object-store-mib",
        type=int,
        default=512,
        help="Object store allocation for each logical Ray node.",
    )
    p.add_argument(
        "--prekill-query-counts",
        type=int,
        nargs="+",
        default=[0],
        help=(
            "Recovery-only resilience characterization. Query-worker nodes are "
            "pre-failed after protection formation; objects with no surviving "
            "query consumer are not counted as failed recovery."
        ),
    )
    p.add_argument("--cluster-timeout-seconds", type=float, default=60.0)
    p.add_argument("--formation-timeout-seconds", type=float, default=90.0)
    p.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=30.0,
    )
    p.add_argument("--profile-stable-seconds", type=float, default=0.5)
    p.add_argument("--task-ready-timeout-seconds", type=float, default=180.0)
    p.add_argument("--prekill-settle-seconds", type=float, default=0.5)
    p.add_argument("--node-death-timeout-seconds", type=float, default=20.0)
    p.add_argument("--recovery-timeout-seconds", type=float, default=120.0)
    p.add_argument("--recovery-concurrency", type=int, default=128)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")
    return p


def validate_args(args: argparse.Namespace) -> None:
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive")
    if args.periods <= 0:
        raise ValueError("--periods must be positive")
    if args.dimension_objects < 0:
        raise ValueError("--dimension-objects must be non-negative")
    if args.inline_chunk_bytes <= 0:
        raise ValueError("--inline-chunk-bytes must be positive")
    if args.producer_cpus <= 0:
        raise ValueError("--producer-cpus must be positive")
    if args.producer_cpus_per_task <= 0:
        raise ValueError("--producer-cpus-per-task must be positive")
    query_count = len(build_queries(args.periods))
    if any(k < 0 or k >= query_count for k in args.prekill_query_counts):
        raise ValueError(
            f"--prekill-query-counts values must be in [0, {query_count - 1}]"
        )
    if args.result_bytes < 9:
        raise ValueError("--result-bytes must be at least 9 bytes")
    if args.result_bytes < 128 * 1024:
        print(
            "WARNING: --result-bytes is small. If Ray serves originals inline, "
            "the recovery phase may not force replay."
        )
    if args.object_store_mib < 256:
        raise ValueError("--object-store-mib should be at least 256")


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)

    if args.command in {"run", "run-and-plot"}:
        run_experiment(args)
    if args.command in {"plot", "run-and-plot"}:
        plot_results(args)


if __name__ == "__main__":
    main()
