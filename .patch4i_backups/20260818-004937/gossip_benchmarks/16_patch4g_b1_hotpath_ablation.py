#!/usr/bin/env python3
"""Patch 4G: focused B1 hot-path profiling and ablation benchmark.

This benchmark intentionally runs only one real downstream borrower (B1), since
B1 is the unresolved ~20% steady-state overhead case.

Cases:
  Disabled
  MetadataOnly              compact recovery metadata, no TaskSpec/candidate
  PiggybackNoCandidate      metadata + 4F TaskSpec sidecar, no candidate
  CandidateRpcNoAdmit       metadata + candidate RPC, owner immediately NO_SLOT
  NoPiggyback               full recovery; H1 uses InstallRecoveryHolder
  Full4F                    ordinary Patch-4F recovery

The three middle ablations intentionally weaken durability and are BENCHMARK ONLY.
Default repetitions = 2 to keep iteration time reasonable.
"""
from __future__ import annotations

import os
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import argparse
import json
import math
import random
import statistics
import subprocess
import sys
import tempfile
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
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

TARGET_HOLDERS = 4
BORROWERS = 1

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

SUM_KEYS = set(PROFILE_KEYS) - {"profiling_enabled", "max_generation", "max_non_owner_holders"}
MAX_KEYS = {"max_generation", "max_non_owner_holders"}
ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
]


@dataclass(frozen=True)
class Case:
    key: str
    label: str
    recovery: bool
    mode: str


def cases() -> list[Case]:
    return [
        Case("disabled", "Disabled", False, "full"),
        Case("metadata_only", "MetadataOnly", True, "metadata_only"),
        Case("piggyback_no_candidate", "PiggybackNoCandidate", True, "piggyback_no_candidate"),
        Case("candidate_rpc_no_admit", "CandidateRpcNoAdmit", True, "candidate_rpc_no_admit"),
        Case("no_piggyback", "NoPiggyback", True, "no_piggyback"),
        Case("full", "Full4F", True, "full"),
    ]


def method_for(case: Case) -> Method:
    return succession(TARGET_HOLDERS) if case.recovery else disabled()


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    out = {k: (False if k == "profiling_enabled" else 0) for k in PROFILE_KEYS}
    if raw:
        for k in PROFILE_KEYS:
            if k in raw:
                out[k] = raw[k]
    return out


def aggregate_profiles(profiles: list[dict[str, Any]]) -> dict[str, Any]:
    vals = [profile_defaults(p) for p in profiles]
    out = profile_defaults()
    out["profiling_enabled"] = any(bool(p["profiling_enabled"]) for p in vals)
    for k in SUM_KEYS:
        out[k] = sum(int(p[k]) for p in vals)
    for k in MAX_KEYS:
        out[k] = max((int(p[k]) for p in vals), default=0)
    return out


def outstanding(profile: dict[str, Any]) -> int:
    return sum(max(0, int(profile[a]) - int(profile[b])) for a, b in ASYNC_PAIRS)


def avg_us(total_ns: Any, count: Any) -> float:
    c = int(count)
    return math.nan if c <= 0 else float(total_ns) / c / 1e3


def start_cluster(case: Case, args: argparse.Namespace) -> tuple[Cluster, list[str]]:
    method = method_for(case)
    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=args.witness_count,
            profiling_enabled=case.recovery,
            ablation_mode=case.mode,
        ),
        include_dashboard=False,
    )
    workers = [cluster.add_node(num_cpus=args.cpus_per_node, resources={"producer_node": 1})]
    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(num_cpus=args.cpus_per_node, resources={f"consumer_{i}": 1})
        )
    return cluster, [n.node_id for n in workers]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int) -> bytes:
        prefix = request_id.to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch_and_export(self, wrapped_ref):
            ref = wrapped_ref[0]
            value = ray.get(ref)
            if not value:
                raise RuntimeError("empty payload")
            return [ref]

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


def run_workload(*, produce, consumer, producer_strategy, args) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, tuple[int, bool]] = {}
    request_id = 0
    tagged_pending = 0
    completed = 0
    tagged_submitted = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    warmup_end = start_ns + int(args.warmup_seconds * 1e9)
    measure_end = warmup_end + int(args.duration_seconds * 1e9)

    def submit_one() -> None:
        nonlocal request_id, tagged_pending, tagged_submitted
        now = time.perf_counter_ns()
        tagged = warmup_end <= now < measure_end
        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, args.payload_bytes)
        stage = consumer.touch_and_export.remote([payload_ref])
        pending[stage] = (now, tagged)
        request_id += 1
        if tagged:
            tagged_pending += 1
            tagged_submitted += 1

    def process_one(resubmit: bool) -> bool:
        nonlocal tagged_pending, completed
        if not pending:
            return False
        ready, _ = ray.wait(list(pending), num_returns=1, timeout=args.wait_timeout_seconds)
        if not ready:
            return False
        ref = ready[0]
        ray.get(ref)
        submitted_ns, tagged = pending.pop(ref)
        done = time.perf_counter_ns()
        if warmup_end <= done < measure_end:
            completed += 1
        if tagged:
            latencies_ms.append((done - submitted_ns) / 1e6)
            tagged_pending -= 1
        if resubmit and (time.perf_counter_ns() < measure_end or tagged_pending > 0):
            submit_one()
        return True

    for _ in range(args.inflight):
        submit_one()
    while time.perf_counter_ns() < measure_end or tagged_pending > 0:
        process_one(True)
    deadline = time.monotonic() + args.drain_timeout_seconds
    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(f"drain timeout with {len(pending)} pending")
        process_one(False)

    return {
        "completed_in_window": completed,
        "total_pipeline_submitted": request_id,
        "latency_sample_count": len(latencies_ms),
        "latency_tagged_submitted": tagged_submitted,
        "throughput_rps": completed / args.duration_seconds,
        "latency_mean_ms": statistics.fmean(latencies_ms) if latencies_ms else math.nan,
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def profile_snapshot(consumers) -> tuple[dict[str, Any], dict[str, Any]]:
    owner = profile_defaults(global_worker.core_worker.get_recovery_succession_profile())
    borrower_raw = ray.get([c.recovery_profile.remote() for c in consumers])
    borrower = aggregate_profiles(borrower_raw)
    return owner, borrower


def wait_for_profile_quiescence(consumers, args) -> tuple[dict[str, Any], dict[str, Any], bool]:
    deadline = time.monotonic() + args.profile_quiescence_timeout_seconds
    last_sig = None
    stable_since = None
    owner, borrower = profile_snapshot(consumers)
    while time.monotonic() < deadline:
        owner, borrower = profile_snapshot(consumers)
        sig = tuple(owner[k] for k in PROFILE_KEYS) + tuple(borrower[k] for k in PROFILE_KEYS)
        now = time.monotonic()
        if outstanding(owner) == 0 and outstanding(borrower) == 0:
            if sig == last_sig:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= args.profile_stable_seconds:
                    return owner, borrower, True
            else:
                stable_since = now
        else:
            stable_since = None
        last_sig = sig
        time.sleep(0.08)
    return owner, borrower, False


def add_scope(row: dict[str, Any], prefix: str, p: dict[str, Any]) -> None:
    for k in PROFILE_KEYS:
        row[f"{prefix}_{k}"] = p[k]


def add_derived(row: dict[str, Any], owner: dict[str, Any], borrower: dict[str, Any]) -> None:
    tasks = max(1, int(row["total_pipeline_submitted"]))
    # Owner-side submission/export hot path.
    row["owner_metadata_lookup_avg_us"] = avg_us(owner["recovery_metadata_lookup_time_ns"], owner["recovery_metadata_lookup_calls"])
    row["owner_ensure_args_avg_us"] = avg_us(owner["ensure_task_arguments_time_ns"], owner["ensure_task_arguments_calls"])
    row["owner_populate_arg_metadata_avg_us"] = avg_us(owner["task_argument_metadata_time_ns"], owner["task_argument_metadata_calls"])
    row["owner_initial_manifest_avg_us"] = avg_us(owner["initial_manifest_build_time_ns"], owner["initial_manifest_build_count"])
    row["owner_witness_selection_avg_us"] = avg_us(owner["witness_selection_time_ns"], owner["witness_selection_count"])
    row["owner_register_owned_avg_us"] = avg_us(owner["register_owned_task_time_ns"], owner["register_owned_task_count"])
    row["owner_piggyback_serialize_avg_us"] = avg_us(owner["first_holder_piggyback_serialize_time_ns"], owner["first_holder_piggyback_copies_sent"])
    row["owner_holder_admission_avg_us"] = avg_us(owner["holder_admission_time_ns"], owner["holder_admissions_committed"])
    row["owner_witness_publish_avg_us"] = avg_us(owner["witness_publish_time_ns"], owner["witness_publish_count"])

    # H1 receive/report hot path.
    row["borrower_register_executor_avg_us"] = avg_us(borrower["register_executor_task_time_ns"], borrower["register_executor_task_calls"])
    row["borrower_candidate_build_avg_us"] = avg_us(borrower["candidate_report_build_time_ns"], borrower["candidate_report_build_calls"])
    row["borrower_candidate_queue_avg_us"] = avg_us(borrower["candidate_queue_time_ns"], borrower["candidate_queue_calls"])
    row["borrower_candidate_rpc_avg_us"] = avg_us(borrower["candidate_rpc_time_ns"], borrower["candidate_rpc_physical_rpcs_completed"])

    # Per-pipeline CPU totals make tiny repeated costs visible.
    row["owner_metadata_lookup_cpu_us_per_pipeline"] = float(owner["recovery_metadata_lookup_time_ns"]) / tasks / 1e3
    row["owner_ensure_args_cpu_us_per_pipeline"] = float(owner["ensure_task_arguments_time_ns"]) / tasks / 1e3
    row["owner_populate_arg_metadata_cpu_us_per_pipeline"] = float(owner["task_argument_metadata_time_ns"]) / tasks / 1e3
    row["borrower_register_executor_cpu_us_per_pipeline"] = float(borrower["register_executor_task_time_ns"]) / tasks / 1e3
    row["borrower_candidate_build_cpu_us_per_pipeline"] = float(borrower["candidate_report_build_time_ns"]) / tasks / 1e3
    row["borrower_candidate_queue_cpu_us_per_pipeline"] = float(borrower["candidate_queue_time_ns"]) / tasks / 1e3
    row["borrower_candidate_reports_per_pipeline"] = float(borrower["candidate_reports_built"]) / tasks
    row["borrower_candidate_rpc_reports_per_pipeline"] = float(borrower["candidate_rpc_logical_reports_sent"]) / tasks
    row["owner_piggyback_copies_per_pipeline"] = float(owner["first_holder_piggyback_copies_sent"]) / tasks
    row["owner_install_rpcs_per_pipeline"] = float(owner["holder_install_rpcs_sent"]) / tasks
    row["owner_control_bytes_per_pipeline"] = (
        float(owner["task_spec_bytes_sent"] + owner["manifest_bytes_sent"] + borrower["candidate_rpc_request_bytes_sent"]) / tasks
    )
    row["owner_metadata_full_equiv_bytes_per_pipeline"] = (
        float(owner["task_argument_metadata_full_bytes_equivalent"]) / tasks
    )
    row["owner_metadata_transport_bytes_per_pipeline"] = (
        float(owner["task_argument_metadata_transport_bytes"]) / tasks
    )
    full_meta_bytes = float(owner["task_argument_metadata_full_bytes_equivalent"])
    row["owner_metadata_transport_ratio"] = (
        float(owner["task_argument_metadata_transport_bytes"]) / full_meta_bytes
        if full_meta_bytes > 0
        else math.nan
    )
    row["owner_metadata_compact_fallbacks_per_pipeline"] = (
        float(owner["task_argument_metadata_compact_fallbacks"]) / tasks
    )


def run_one(case: Case, repetition: int, args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    try:
        cluster, node_ids = start_cluster(case, args)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, TARGET_HOLDERS + 2, args.cluster_timeout_seconds)
        produce, Consumer = make_remote_types()
        consumers = [
            Consumer.options(resources={f"consumer_{i}": 0.01}, num_cpus=0).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([c.ping.remote() for c in consumers])

        if case.recovery:
            global_worker.core_worker.reset_recovery_succession_profile()
            ray.get([c.reset_recovery_profile.remote() for c in consumers])

        result = run_workload(
            produce=produce,
            consumer=consumers[0],
            producer_strategy=NodeAffinitySchedulingStrategy(node_id=node_ids[0], soft=False),
            args=args,
        )

        if case.recovery:
            owner, borrower, quiescent = wait_for_profile_quiescence(consumers, args)
        else:
            owner, borrower = profile_defaults(), profile_defaults()
            quiescent = True

        row: dict[str, Any] = {
            "repetition": repetition,
            "case": case.key,
            "label": case.label,
            "recovery_enabled": int(case.recovery),
            "ablation_mode": case.mode,
            "borrower_count": BORROWERS,
            "target_holders": TARGET_HOLDERS,
            "payload_bytes": args.payload_bytes,
            "profile_quiescent": int(quiescent),
            **result,
        }
        add_scope(row, "owner", owner)
        add_scope(row, "borrower", borrower)
        add_derived(row, owner, borrower)
        return row
    finally:
        safe_shutdown(ray, cluster)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metric_names = [
        "owner_metadata_lookup_avg_us",
        "owner_ensure_args_avg_us",
        "owner_populate_arg_metadata_avg_us",
        "owner_initial_manifest_avg_us",
        "owner_witness_selection_avg_us",
        "owner_register_owned_avg_us",
        "owner_piggyback_serialize_avg_us",
        "owner_holder_admission_avg_us",
        "owner_witness_publish_avg_us",
        "borrower_register_executor_avg_us",
        "borrower_candidate_build_avg_us",
        "borrower_candidate_queue_avg_us",
        "borrower_candidate_rpc_avg_us",
        "owner_metadata_lookup_cpu_us_per_pipeline",
        "owner_ensure_args_cpu_us_per_pipeline",
        "owner_populate_arg_metadata_cpu_us_per_pipeline",
        "borrower_register_executor_cpu_us_per_pipeline",
        "borrower_candidate_build_cpu_us_per_pipeline",
        "borrower_candidate_queue_cpu_us_per_pipeline",
        "borrower_candidate_reports_per_pipeline",
        "borrower_candidate_rpc_reports_per_pipeline",
        "owner_piggyback_copies_per_pipeline",
        "owner_install_rpcs_per_pipeline",
        "owner_control_bytes_per_pipeline",
        "owner_metadata_full_equiv_bytes_per_pipeline",
        "owner_metadata_transport_bytes_per_pipeline",
        "owner_metadata_transport_ratio",
        "owner_metadata_compact_fallbacks_per_pipeline",
    ]

    for case in cases():
        g = [r for r in rows if r["case"] == case.key]
        if not g:
            continue
        t_mean, t_ci = mean_ci95(float(r["throughput_rps"]) for r in g)
        p50_mean, p50_ci = mean_ci95(float(r["latency_p50_ms"]) for r in g)
        p95_mean, p95_ci = mean_ci95(float(r["latency_p95_ms"]) for r in g)
        row: dict[str, Any] = {
            "case": case.key,
            "label": case.label,
            "ablation_mode": case.mode,
            "repetitions": len(g),
            "throughput_mean_rps": t_mean,
            "throughput_ci95_rps": t_ci,
            "p50_latency_mean_ms": p50_mean,
            "p50_latency_ci95_ms": p50_ci,
            "p95_latency_mean_ms": p95_mean,
            "p95_latency_ci95_ms": p95_ci,
            "profile_quiescent_all": min(int(r["profile_quiescent"]) for r in g),
            "owner_max_non_owner_holders_max": max(int(r["owner_max_non_owner_holders"]) for r in g),
        }
        for name in metric_names:
            vals = [float(r[name]) for r in g if not math.isnan(float(r[name]))]
            row[f"{name}_mean"] = statistics.fmean(vals) if vals else math.nan
        out.append(row)

    disabled_rows = [r for r in out if r["case"] == "disabled"]
    base = float(disabled_rows[0]["throughput_mean_rps"]) if disabled_rows else math.nan
    for r in out:
        r["throughput_loss_vs_disabled_pct"] = (
            100.0 * (base - float(r["throughput_mean_rps"])) / base
            if r["case"] != "disabled" and base > 0 else 0.0
        )
    return out


def _common_child_args(args: argparse.Namespace) -> list[str]:
    out = [
        "--warmup-seconds", str(args.warmup_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--inflight", str(args.inflight),
        "--payload-bytes", str(args.payload_bytes),
        "--cpus-per-node", str(args.cpus_per_node),
        "--witness-count", str(args.witness_count),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--profile-quiescence-timeout-seconds",
        str(args.profile_quiescence_timeout_seconds),
        "--profile-stable-seconds", str(args.profile_stable_seconds),
        "--seed", str(args.seed),
        "--output-dir", str(args.output_dir),
    ]
    if args.fixed_order:
        out.append("--fixed-order")
    return out


def run_single_case(args: argparse.Namespace) -> None:
    match = [c for c in cases() if c.key == args.case]
    if len(match) != 1:
        raise ValueError(f"unknown benchmark case {args.case!r}")
    row = run_one(match[0], args.repetition, args)
    Path(args.row_json).write_text(json.dumps(row, allow_nan=True))


def run_benchmark(args: argparse.Namespace) -> None:
    # Patch 4G-1: fresh process per ablation case.
    #
    # RayConfig is process-global and the C++ ablation helper intentionally
    # caches the configured mode. Therefore changing _system_config while
    # repeatedly ray.init()/ray.shutdown() in one Python driver does NOT give
    # a trustworthy per-case owner-side ablation. A fresh process makes every
    # case start with exactly the requested mode.
    order_base = cases()
    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []
    total = args.repetitions * len(order_base)
    idx = 0
    script = str(Path(__file__).resolve())

    with tempfile.TemporaryDirectory(prefix="patch4g1-") as tmp:
        tmp_root = Path(tmp)
        for rep in range(1, args.repetitions + 1):
            order = order_base[:]
            if not args.fixed_order:
                rng.shuffle(order)

            for case in order:
                idx += 1
                print(
                    f"[{idx}/{total}] rep={rep} case={case.label} "
                    f"mode={case.mode} [fresh process]",
                    flush=True,
                )
                row_json = tmp_root / f"rep{rep}_{case.key}.json"
                cmd = [
                    sys.executable,
                    script,
                    "_single-run",
                    "--case", case.key,
                    "--repetition", str(rep),
                    "--row-json", str(row_json),
                    *_common_child_args(args),
                ]
                subprocess.run(cmd, check=True)

                if not row_json.exists():
                    raise RuntimeError(
                        f"child benchmark did not write expected result {row_json}"
                    )
                rows.append(json.loads(row_json.read_text()))

    root = Path(args.output_dir)
    write_csv(root / "patch4g_b1_runs.csv", rows)
    summary = summarize(rows)
    write_csv(root / "patch4g_b1_summary.csv", summary)
    print(f"Wrote {root / 'patch4g_b1_summary.csv'}")
    print("\nB1 throughput loss vs Disabled:")
    for r in summary:
        print(
            f"  {r['label']:24s} "
            f"{float(r['throughput_mean_rps']):9.1f} rps  "
            f"loss={float(r['throughput_loss_vs_disabled_pct']):6.2f}%"
        )


def plot(args: argparse.Namespace) -> None:
    import csv
    import matplotlib.pyplot as plt
    root = Path(args.output_dir)
    with (root / "patch4g_b1_summary.csv").open(newline="") as f:
        rows = list(csv.DictReader(f))
    labels = [r["label"] for r in rows]
    losses = [float(r["throughput_loss_vs_disabled_pct"]) for r in rows]
    plt.figure(figsize=(9, 4.8))
    plt.bar(labels, losses)
    plt.axhline(10.0, linestyle="--")
    plt.ylabel("Throughput loss vs Disabled (%)")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    (root / "plots").mkdir(parents=True, exist_ok=True)
    plt.savefig(root / "plots" / "b1_ablation_throughput_loss.png", dpi=160)
    plt.close()

    hot = [
        ("owner_ensure_args_cpu_us_per_pipeline_mean", "Owner ensure args"),
        ("owner_populate_arg_metadata_cpu_us_per_pipeline_mean", "Owner arg metadata"),
        ("borrower_register_executor_cpu_us_per_pipeline_mean", "H1 register executor"),
        ("borrower_candidate_build_cpu_us_per_pipeline_mean", "H1 candidate build"),
        ("borrower_candidate_queue_cpu_us_per_pipeline_mean", "H1 candidate queue"),
    ]
    x = range(len(labels))
    plt.figure(figsize=(9, 5.2))
    for key, name in hot:
        plt.plot(x, [float(r[key]) if r[key] else math.nan for r in rows], marker="o", label=name)
    plt.xticks(list(x), labels, rotation=25, ha="right")
    plt.ylabel("Measured CPU/wall time per pipeline (us)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(root / "plots" / "b1_hotpath_cpu_per_pipeline.png", dpi=160)
    plt.close()
    print(f"Wrote plots to {root / 'plots'}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(s):
        s.add_argument("--repetitions", type=int, default=2)
        s.add_argument("--warmup-seconds", type=float, default=3.0)
        s.add_argument("--duration-seconds", type=float, default=15.0)
        s.add_argument("--inflight", type=int, default=64)
        s.add_argument("--payload-bytes", type=int, default=1024)
        s.add_argument("--cpus-per-node", type=int, default=4)
        s.add_argument("--witness-count", type=int, default=2)
        s.add_argument("--wait-timeout-seconds", type=float, default=1.0)
        s.add_argument("--drain-timeout-seconds", type=float, default=30.0)
        s.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
        s.add_argument("--profile-quiescence-timeout-seconds", type=float, default=8.0)
        s.add_argument("--profile-stable-seconds", type=float, default=0.25)
        s.add_argument("--seed", type=int, default=42)
        s.add_argument("--fixed-order", action="store_true")
        s.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")

    r = sub.add_parser("run")
    add_common(r)
    rp = sub.add_parser("run-and-plot")
    add_common(rp)

    # Internal Patch-4G-1 worker command. The parent launches one fresh Python
    # process per (case, repetition) so RayConfig cannot leak across cases.
    one = sub.add_parser("_single-run")
    add_common(one)
    one.add_argument("--case", required=True)
    one.add_argument("--repetition", type=int, required=True)
    one.add_argument("--row-json", required=True)

    pl = sub.add_parser("plot")
    pl.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")
    return p


def main() -> None:
    args = build_parser().parse_args()
    # Patch 4G-1: fresh process per ablation case.
    if args.command == "_single-run":
        run_single_case(args)
        return
    if args.command in {"run", "run-and-plot"}:
        run_benchmark(args)
    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
