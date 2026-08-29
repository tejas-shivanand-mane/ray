#!/usr/bin/env python3
"""Benchmark 58: adaptive Recovery Succession x Recovery Frontier K performance.

Variants:
  disabled
  succession_k1      ordinary per-task Recovery Succession
  succession_k2
  succession_k4
  succession_k8
  succession_k16
  succession_k32

Timed runs use two node-distinct borrowers for every producer ObjectRef so R=2
Succession actually pays/forms H1 and H2. Producer tasks are submitted in bursts
of 32 before any ref is exported, giving every K a full-group workload.
Profiling is forced OFF for timed runs. A separate profiling-ON live-batch sweep
verifies the control-plane 1/K amortization without contaminating throughput.

Outputs:
  succession_frontier_perf_runs.csv
  succession_frontier_perf_summary.csv
  succession_frontier_perf_paired.csv
  succession_frontier_control_plane.csv

Recommended paper run:
  python gossip_benchmarks/58_recovery_frontier_succession_performance.py --overwrite
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import disabled, percentile, safe_shutdown, succession, system_config, wait_for_cluster

R_DEFAULT = 2
VARIANTS = [
    "disabled",
    "succession_k1",
    "succession_k2",
    "succession_k4",
    "succession_k8",
    "succession_k16",
    "succession_k32",
]
K_BY_VARIANT = {
    "succession_k1": 1,
    "succession_k2": 2,
    "succession_k4": 4,
    "succession_k8": 8,
    "succession_k16": 16,
    "succession_k32": 32,
}
ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
    ("candidate_rpc_logical_reports_sent", "candidate_rpc_logical_reports_completed"),
    ("candidate_rpc_physical_rpcs_sent", "candidate_rpc_physical_rpcs_completed"),
]
BORROWER_PROFILE_KEYS = [
    "candidate_rpc_logical_reports_sent",
    "candidate_rpc_logical_reports_completed",
    "candidate_rpc_physical_rpcs_sent",
    "candidate_rpc_physical_rpcs_completed",
    "candidate_rpc_request_bytes_sent",
    "task_argument_metadata_calls",
    "task_argument_metadata_refs_attached",
    "task_argument_metadata_transport_bytes",
    "register_executor_candidate_reports_built",
]
OWNER_PROFILE_KEYS = [
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
    "holder_admissions_committed",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
    "initial_manifest_build_count",
]

@dataclass(frozen=True)
class SpecPadding:
    name: str
    size_bytes: int


def parse_padding(text: str) -> SpecPadding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("TaskSpec padding must be NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError("invalid TaskSpec padding")
    return SpecPadding(name, size)


def build_padding(total: int, chunk: int) -> tuple[bytes, ...]:
    if total <= 0:
        return ()
    out, left, token = [], total, 1
    while left:
        n = min(left, chunk)
        out.append(bytes([token % 251]) * n)
        left -= n
        token += 1
    return tuple(out)


def k_for(variant: str) -> int:
    return K_BY_VARIANT.get(variant, 0)


def child_env(*, profiling: bool) -> dict[str, str]:
    env = dict(os.environ)
    env["RAY_RECOVERY_PROFILING"] = "1" if profiling else "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
    return env


def case_config(variant: str, holders: int, witnesses: int, profiling: bool) -> dict[str, Any]:
    method = disabled() if variant == "disabled" else succession(holders)
    cfg = system_config(method, witness_count=witnesses, profiling_enabled=profiling and method.recovery_enabled)
    k = k_for(variant)
    cfg.update({
        # K=1 is the ordinary Succession baseline. The production adaptive
        # Frontier composition is intentionally enabled only for K>1.
        "enable_recovery_frontier": bool(method.recovery_enabled and k > 1),
        "recovery_frontier_group_size": max(1, k),
        "recovery_baseline_perf_protect_every_n": 1,
        "enable_recovery_succession_certificate_admission": False,
    })
    return cfg


def start_cluster(args: argparse.Namespace, variant: str, profiling: bool) -> tuple[Cluster, str]:
    if args.holders != 2:
        raise ValueError("Benchmark 58 currently requires --holders=2")
    if args.witness_count != args.holders:
        raise ValueError("--witness-count must equal --holders")
    cluster = Cluster()
    cluster.add_node(num_cpus=0, _system_config=case_config(variant, args.holders, args.witness_count, profiling), include_dashboard=False)
    producer = cluster.add_node(num_cpus=args.cpus_per_node, resources={"producer_node": 1})
    for i in range(args.holders):
        cluster.add_node(num_cpus=args.cpus_per_node, resources={f"borrower_node_{i}": 1})
    for i in range(args.witness_count):
        cluster.add_node(num_cpus=0, resources={f"witness_node_{i}": 1})
    return cluster, producer.node_id


def remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int, *padding: bytes) -> bytes:
        if padding and padding[0]:
            _ = padding[0][0]
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=256)
    class Borrower:
        def __init__(self):
            self.refs = []
        def touch(self, wrapped_ref):
            value = ray.get(wrapped_ref[0])
            return int.from_bytes(value[:8], "little", signed=False)
        def hold(self, wrapped_refs):
            refs = list(wrapped_refs)
            self.refs.extend(refs)
            return [ref.hex() for ref in refs]
        def clear(self):
            self.refs.clear()
        def ping(self):
            import os as _os
            return _os.getpid()
        def reset_profile(self):
            from ray._private.worker import global_worker as gw
            gw.core_worker.reset_recovery_succession_profile()
        def profile(self):
            from ray._private.worker import global_worker as gw
            return dict(gw.core_worker.get_recovery_succession_profile())
    return produce, Borrower


def run_window(*, produce: Any, borrowers: list[Any], strategy: Any, padding: tuple[bytes, ...], payload_bytes: int,
               duration_s: float, inflight: int, burst: int, wait_timeout: float, drain_timeout: float,
               request_base: int) -> dict[str, Any]:
    if len(borrowers) != 2:
        raise ValueError("timed workload requires exactly two borrowers")
    if burst <= 0 or inflight < burst or inflight % burst:
        raise ValueError("invalid burst/inflight configuration")

    pending: dict[ray.ObjectRef, int] = {}
    remaining: dict[int, int] = {}
    submitted_ns: dict[int, int] = {}
    next_id = request_base
    completed = 0
    submitted = 0
    latencies: list[float] = []
    end_ns = time.perf_counter_ns() + int(duration_s * 1e9)

    def submit_burst() -> None:
        nonlocal next_id, submitted
        created: list[tuple[int, ray.ObjectRef]] = []
        # Register the full burst before exporting any ref. This gives K>1 full
        # groups while preserving an identical application workload for all K.
        for _ in range(burst):
            rid = next_id
            next_id += 1
            submitted += 1
            submitted_ns[rid] = time.perf_counter_ns()
            ref = produce.options(scheduling_strategy=strategy, num_cpus=1).remote(rid, payload_bytes, *padding)
            created.append((rid, ref))
        for rid, ref in created:
            remaining[rid] = len(borrowers)
            for borrower in borrowers:
                pending[borrower.touch.remote([ref])] = rid

    def process_ready() -> None:
        nonlocal completed
        if not pending:
            return
        ready, _ = ray.wait(list(pending), num_returns=min(64, len(pending)), timeout=wait_timeout)
        for result_ref in ready:
            rid = pending.pop(result_ref)
            if int(ray.get(result_ref)) != rid:
                raise RuntimeError("consumer observed wrong producer result")
            left = remaining[rid] - 1
            if left:
                remaining[rid] = left
                continue
            del remaining[rid]
            now_ns = time.perf_counter_ns()
            if now_ns <= end_ns:
                completed += 1
            latencies.append((now_ns - submitted_ns.pop(rid)) / 1e6)

    while len(remaining) + burst <= inflight:
        submit_burst()
    while time.perf_counter_ns() < end_ns:
        process_ready()
        while time.perf_counter_ns() < end_ns and len(remaining) + burst <= inflight:
            submit_burst()
    deadline = time.monotonic() + drain_timeout
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"drain timed out: {len(pending)} borrower calls remain")
        process_ready()
    if not latencies:
        raise RuntimeError("no completed pipelines")
    return {
        "throughput_rps": completed / duration_s,
        "completed_in_window": completed,
        "total_pipeline_submitted": submitted,
        "latency_sample_count": len(latencies),
        "latency_mean_ms": statistics.fmean(latencies),
        "latency_p50_ms": percentile(latencies, 0.50),
        "latency_p95_ms": percentile(latencies, 0.95),
        "latency_p99_ms": percentile(latencies, 0.99),
    }


def single_perf(args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    try:
        cluster, producer_node = start_cluster(args, args.single_variant, False)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + args.holders + args.witness_count, args.cluster_timeout_seconds)
        produce, Borrower = remote_types()
        borrowers = [Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote() for i in range(args.holders)]
        ray.get([b.ping.remote() for b in borrowers])
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = build_padding(args.single_padding_bytes, args.inline_chunk_bytes)
        if args.warmup_seconds > 0:
            run_window(produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
                       payload_bytes=args.payload_bytes, duration_s=args.warmup_seconds,
                       inflight=args.inflight_tasks, burst=args.burst_size,
                       wait_timeout=args.wait_timeout_seconds, drain_timeout=args.drain_timeout_seconds,
                       request_base=1_000_000)
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)
        perf = run_window(produce=produce, borrowers=borrowers, strategy=strategy, padding=padding,
                          payload_bytes=args.payload_bytes, duration_s=args.duration_seconds,
                          inflight=args.inflight_tasks, burst=args.burst_size,
                          wait_timeout=args.wait_timeout_seconds, drain_timeout=args.drain_timeout_seconds,
                          request_base=10_000_000)
        return {
            "variant": args.single_variant,
            "frontier_k": k_for(args.single_variant),
            "repetition": args.single_repetition,
            "holders": args.holders,
            "borrowers_per_pipeline": len(borrowers),
            "task_spec_padding_name": args.single_padding_name,
            "task_spec_padding_bytes": args.single_padding_bytes,
            "payload_bytes": args.payload_bytes,
            "burst_size": args.burst_size,
            "inflight_tasks": args.inflight_tasks,
            "profiling_enabled": 0,
            **perf,
        }
    finally:
        safe_shutdown(ray, cluster)


def profile_dict(raw: dict[str, Any] | None, keys: list[str]) -> dict[str, int]:
    raw = raw or {}
    return {key: int(raw.get(key, 0)) for key in keys}


def aggregate_borrower_profiles(raws: list[dict[str, Any]]) -> dict[str, int]:
    out = {key: 0 for key in BORROWER_PROFILE_KEYS}
    for raw in raws:
        for key in out:
            out[key] += int(raw.get(key, 0))
    return out


def outstanding(profile: dict[str, int]) -> int:
    return sum(max(0, profile.get(sent, 0) - profile.get(done, 0)) for sent, done in ASYNC_PAIRS)


def single_profile(args: argparse.Namespace) -> dict[str, Any]:
    if args.single_variant == "disabled":
        raise ValueError("profile sweep does not include disabled")
    cluster = None
    try:
        cluster, producer_node = start_cluster(args, args.single_variant, True)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 1 + 1 + args.holders + args.witness_count, args.cluster_timeout_seconds)
        produce, Borrower = remote_types()
        borrowers = [Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote() for i in range(args.holders)]
        ray.get([b.ping.remote() for b in borrowers])
        global_worker.core_worker.reset_recovery_succession_profile()
        ray.get([b.reset_profile.remote() for b in borrowers])
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = build_padding(args.single_padding_bytes, args.inline_chunk_bytes)
        n = args.profile_tasks
        if n <= 0 or n % 32:
            raise ValueError("--profile-tasks must be a positive multiple of 32")
        refs = [produce.options(scheduling_strategy=strategy, num_cpus=1).remote(50_000_000 + i, args.payload_bytes, *padding) for i in range(n)]
        expected_ids = [ref.hex() for ref in refs]
        held = ray.get([b.hold.remote(refs) for b in borrowers])
        if any(ids != expected_ids for ids in held):
            raise RuntimeError("borrower did not receive exact live reference batch")

        k = k_for(args.single_variant)
        groups = n if k == 1 else n // k
        expected = groups * args.holders
        deadline = time.monotonic() + args.profile_timeout_seconds
        stable_since = None
        last_sig = None
        owner: dict[str, int] = {}
        borrower_profile: dict[str, int] = {}
        while time.monotonic() < deadline:
            owner = profile_dict(dict(global_worker.core_worker.get_recovery_succession_profile()), OWNER_PROFILE_KEYS)
            borrower_profile = aggregate_borrower_profiles(ray.get([b.profile.remote() for b in borrowers]))
            sig = (
                owner["candidate_reports_received"], owner["candidate_reports_accepted"],
                owner["holder_admissions_committed"], owner["holder_install_rpcs_sent"],
                owner["holder_install_rpcs_completed"], owner["holder_commit_rpcs_sent"],
                owner["holder_commit_rpcs_completed"], owner["witness_update_rpcs_sent"],
                owner["witness_update_rpcs_completed"],
                borrower_profile["candidate_rpc_logical_reports_sent"],
                borrower_profile["candidate_rpc_logical_reports_completed"],
                borrower_profile["candidate_rpc_physical_rpcs_sent"],
                borrower_profile["candidate_rpc_physical_rpcs_completed"],
            )
            ready = (owner["candidate_reports_received"] >= expected and
                     owner["holder_admissions_committed"] >= expected and
                     outstanding(owner) == 0 and outstanding(borrower_profile) == 0)
            now = time.monotonic()
            if ready:
                if sig == last_sig:
                    if stable_since is None:
                        stable_since = now
                    elif now - stable_since >= args.profile_stable_seconds:
                        break
                else:
                    stable_since = now
            else:
                stable_since = None
            last_sig = sig
            time.sleep(0.05)
        else:
            raise TimeoutError(f"profile did not quiesce: variant={args.single_variant} expected={expected} owner={owner} borrowers={borrower_profile}")

        for key in ["candidate_reports_received", "candidate_reports_accepted", "holder_admissions_committed"]:
            if owner[key] != expected:
                raise AssertionError(f"{args.single_variant}: {key} expected {expected}, got {owner[key]}")

        row: dict[str, Any] = {
            "variant": args.single_variant,
            "frontier_k": k,
            "holders": args.holders,
            "profile_tasks": n,
            "expected_frontier_groups": groups,
            "expected_candidate_reports": expected,
            "candidate_reports_received": owner["candidate_reports_received"],
            "candidate_reports_accepted": owner["candidate_reports_accepted"],
            "holder_admissions_committed": owner["holder_admissions_committed"],
            "candidate_reports_per_task": owner["candidate_reports_received"] / n,
            "holder_admissions_per_task": owner["holder_admissions_committed"] / n,
            "holder_install_rpcs_sent": owner["holder_install_rpcs_sent"],
            "holder_install_rpcs_completed": owner["holder_install_rpcs_completed"],
            "holder_install_rpcs_per_task": owner["holder_install_rpcs_sent"] / n,
            "holder_commit_rpcs_sent": owner["holder_commit_rpcs_sent"],
            "holder_commit_rpcs_completed": owner["holder_commit_rpcs_completed"],
            "witness_update_rpcs_sent": owner["witness_update_rpcs_sent"],
            "witness_update_rpcs_completed": owner["witness_update_rpcs_completed"],
            "witness_update_rpcs_per_task": owner["witness_update_rpcs_sent"] / n,
            "task_spec_bytes_sent": owner["task_spec_bytes_sent"],
            "manifest_bytes_sent": owner["manifest_bytes_sent"],
            "manifest_generations_committed": owner["manifest_generations_committed"],
            "frozen_commits": owner["frozen_commits"],
            "max_non_owner_holders": owner["max_non_owner_holders"],
            "initial_manifest_build_count": owner["initial_manifest_build_count"],
            "task_spec_padding_name": args.single_padding_name,
            "task_spec_padding_bytes": args.single_padding_bytes,
        }
        for key, value in borrower_profile.items():
            row[f"borrowers_{key}"] = value
        row["borrower_candidate_physical_rpcs_per_task"] = borrower_profile["candidate_rpc_physical_rpcs_sent"] / n
        row["borrower_candidate_logical_reports_per_task"] = borrower_profile["candidate_rpc_logical_reports_sent"] / n
        ray.get([b.clear.remote() for b in borrowers])
        return row
    finally:
        safe_shutdown(ray, cluster)


_T95 = {1:12.706,2:4.303,3:3.182,4:2.776,5:2.571,6:2.447,7:2.365,8:2.306,9:2.262,10:2.228,
        11:2.201,12:2.179,13:2.160,14:2.145,15:2.131,16:2.120,17:2.110,18:2.101,19:2.093,20:2.086,
        21:2.080,22:2.074,23:2.069,24:2.064,25:2.060,26:2.056,27:2.052,28:2.048,29:2.045,30:2.042}

def describe(values: Iterable[float]) -> dict[str, float]:
    vals = [float(v) for v in values]
    if not vals:
        return {k: math.nan for k in ["mean","median","stdev","cv_pct","ci95","min","max"]}
    mean = statistics.fmean(vals)
    if len(vals) > 1:
        stdev = statistics.stdev(vals)
        ci = _T95.get(len(vals)-1, 1.96) * stdev / math.sqrt(len(vals))
        cv = 100.0 * stdev / mean if mean else math.nan
    else:
        stdev = cv = ci = math.nan
    return {"mean":mean,"median":statistics.median(vals),"stdev":stdev,"cv_pct":cv,"ci95":ci,"min":min(vals),"max":max(vals)}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.unlink(missing_ok=True)
        return
    fields, seen = [], set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key); fields.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields); writer.writeheader(); writer.writerows(rows)


def block_order(rep: int, seed: int) -> list[str]:
    base = list(VARIANTS); random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def case_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return str(row["variant"]), int(row["task_spec_padding_bytes"]), int(row["repetition"])


def summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    metrics = ["throughput_rps","latency_mean_ms","latency_p50_ms","latency_p95_ms","latency_p99_ms"]
    paddings = sorted({(int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"])) for r in rows})
    for pbytes, pname in paddings:
        for variant in VARIANTS:
            matched = [r for r in rows if str(r["variant"]) == variant and int(r["task_spec_padding_bytes"]) == pbytes]
            if not matched: continue
            item: dict[str, Any] = {"task_spec_padding_name":pname,"task_spec_padding_bytes":pbytes,"variant":variant,"frontier_k":k_for(variant),"repetitions":len(matched)}
            for metric in metrics:
                for stat, value in describe(float(r[metric]) for r in matched).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def paired(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    paddings = sorted({(int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"])) for r in rows})
    for pbytes, pname in paddings:
        reps = sorted({int(r["repetition"]) for r in rows if int(r["task_spec_padding_bytes"]) == pbytes})
        metrics = {v:{"normalized_throughput_pct":[],"throughput_overhead_pct_vs_disabled":[],"throughput_speedup_pct_vs_k1":[],"k1_lost_throughput_recovered_pct":[],"p95_inflation_pct_vs_disabled":[]} for v in VARIANTS}
        counts = {v:0 for v in VARIANTS}
        for rep in reps:
            by = {str(r["variant"]):r for r in rows if int(r["task_spec_padding_bytes"]) == pbytes and int(r["repetition"]) == rep}
            if "disabled" not in by or "succession_k1" not in by: continue
            dthr, dp95 = float(by["disabled"]["throughput_rps"]), float(by["disabled"]["latency_p95_ms"])
            k1thr = float(by["succession_k1"]["throughput_rps"]); loss = dthr - k1thr
            for variant, row in by.items():
                thr, p95 = float(row["throughput_rps"]), float(row["latency_p95_ms"])
                metrics[variant]["normalized_throughput_pct"].append(100*thr/dthr if dthr else math.nan)
                metrics[variant]["throughput_overhead_pct_vs_disabled"].append(100*(dthr-thr)/dthr if dthr else math.nan)
                metrics[variant]["throughput_speedup_pct_vs_k1"].append(100*(thr-k1thr)/k1thr if k1thr else math.nan)
                metrics[variant]["p95_inflation_pct_vs_disabled"].append(100*(p95-dp95)/dp95 if dp95 else math.nan)
                recovered = 100.0 if variant == "disabled" else 0.0 if variant == "succession_k1" else 100*(thr-k1thr)/loss if loss > 0 else math.nan
                if math.isfinite(recovered): metrics[variant]["k1_lost_throughput_recovered_pct"].append(recovered)
                counts[variant] += 1
        for variant in VARIANTS:
            if not counts[variant]: continue
            item: dict[str, Any] = {"task_spec_padding_name":pname,"task_spec_padding_bytes":pbytes,"variant":variant,"frontier_k":k_for(variant),"paired_repetitions":counts[variant]}
            for metric, vals in metrics[variant].items():
                finite = [v for v in vals if math.isfinite(v)]
                for stat, value in describe(finite).items(): item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def write_outputs(out: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(out/"succession_frontier_perf_runs.csv", rows)
    write_csv(out/"succession_frontier_perf_summary.csv", summaries(rows))
    write_csv(out/"succession_frontier_perf_paired.csv", paired(rows))


def perf_cmd(args, variant, padding, rep, temp):
    return [sys.executable, str(Path(__file__).resolve()), "_single-perf", "--single-variant",variant,
            "--single-padding-name",padding.name,"--single-padding-bytes",str(padding.size_bytes),
            "--single-repetition",str(rep),"--single-output-json",str(temp),"--holders",str(args.holders),
            "--witness-count",str(args.witness_count),"--payload-bytes",str(args.payload_bytes),
            "--inline-chunk-bytes",str(args.inline_chunk_bytes),"--burst-size",str(args.burst_size),
            "--inflight-tasks",str(args.inflight_tasks),"--warmup-seconds",str(args.warmup_seconds),
            "--settle-seconds",str(args.settle_seconds),"--duration-seconds",str(args.duration_seconds),
            "--cpus-per-node",str(args.cpus_per_node),"--cluster-timeout-seconds",str(args.cluster_timeout_seconds),
            "--wait-timeout-seconds",str(args.wait_timeout_seconds),"--drain-timeout-seconds",str(args.drain_timeout_seconds)]


def profile_cmd(args, variant, padding, temp):
    return [sys.executable, str(Path(__file__).resolve()), "_single-profile", "--single-variant",variant,
            "--single-padding-name",padding.name,"--single-padding-bytes",str(padding.size_bytes),
            "--single-repetition","0","--single-output-json",str(temp),"--holders",str(args.holders),
            "--witness-count",str(args.witness_count),"--payload-bytes",str(args.payload_bytes),
            "--inline-chunk-bytes",str(args.inline_chunk_bytes),"--cpus-per-node",str(args.cpus_per_node),
            "--cluster-timeout-seconds",str(args.cluster_timeout_seconds),"--profile-tasks",str(args.profile_tasks),
            "--profile-timeout-seconds",str(args.profile_timeout_seconds),"--profile-stable-seconds",str(args.profile_stable_seconds)]


def run_profile_sweep(args, out: Path) -> list[dict[str, Any]]:
    path = out/"succession_frontier_control_plane.csv"
    rows = [dict(r) for r in read_csv(path)]
    completed = {(str(r["variant"]), int(r["task_spec_padding_bytes"])) for r in rows}
    padding = args.task_spec_padding[0]
    variants = [v for v in VARIANTS if v != "disabled"]
    for i, variant in enumerate(variants, 1):
        if (variant, padding.size_bytes) in completed: continue
        print(f"[profile {i}/{len(variants)}] variant={variant} TaskSpec={padding.name}", flush=True)
        temp = out/f".profile_{variant}_{padding.size_bytes}.json"; temp.unlink(missing_ok=True)
        proc = subprocess.run(profile_cmd(args, variant, padding, temp), env=child_env(profiling=True))
        if proc.returncode != 0 or not temp.exists(): write_csv(path, rows); raise SystemExit(proc.returncode or 1)
        row = json.loads(temp.read_text()); temp.unlink(missing_ok=True); rows.append(row); write_csv(path, rows)
        print(f"  groups={int(row['expected_frontier_groups'])} reports/task={float(row['candidate_reports_per_task']):.4f} admissions/task={float(row['holder_admissions_per_task']):.4f} candidate-rpcs/task={float(row['borrower_candidate_physical_rpcs_per_task']):.4f}")
    return rows


def run_parent(args):
    if args.repetitions < 2: raise ValueError("repetitions must be >= 2")
    if args.burst_size % 32: raise ValueError("--burst-size must be divisible by 32")
    if args.inflight_tasks % args.burst_size: raise ValueError("--inflight-tasks must be divisible by --burst-size")
    if args.holders != 2 or args.witness_count != 2: raise ValueError("Benchmark 58 requires R=witness_count=2")
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for name in ["succession_frontier_perf_runs.csv","succession_frontier_perf_summary.csv","succession_frontier_perf_paired.csv","succession_frontier_control_plane.csv"]:
            (out/name).unlink(missing_ok=True)
    runs_path = out/"succession_frontier_perf_runs.csv"
    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    completed = {case_key(r) for r in rows}
    cases = []
    for rep in range(1, args.repetitions+1):
        order = block_order(rep, args.seed)
        for padding in args.task_spec_padding:
            for pos, variant in enumerate(order, 1): cases.append((rep,padding,variant,pos))
    pending = [c for c in cases if (c[2],c[1].size_bytes,c[0]) not in completed]
    print(f"Adaptive Succession x Frontier: R=2 borrowers/pipeline=2 burst={args.burst_size} reps={args.repetitions} warmup={args.warmup_seconds:.1f}s timed={args.duration_seconds:.1f}s cases={len(cases)} remaining={len(pending)}")
    print("  timed profiling=OFF; balanced cyclic blocks; comparisons paired within repetition")
    for i,(rep,padding,variant,pos) in enumerate(pending,1):
        print(f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} position={pos}/7 variant={variant} TaskSpec={padding.name}", flush=True)
        temp = out/f".perf_{variant}_{padding.size_bytes}_{rep}.json"; temp.unlink(missing_ok=True)
        proc = subprocess.run(perf_cmd(args,variant,padding,rep,temp), env=child_env(profiling=False))
        if proc.returncode != 0 or not temp.exists(): write_outputs(out, rows); raise SystemExit(proc.returncode or 1)
        row = json.loads(temp.read_text()); temp.unlink(missing_ok=True); row["block_position"] = pos; row["block_seed"] = args.seed; rows.append(row); write_outputs(out, rows)
        print(f"  throughput={float(row['throughput_rps']):.1f} rps p95={float(row['latency_p95_ms']):.2f} ms")
    print("\nFinal robust comparison:")
    pbytes = args.task_spec_padding[0].size_bytes
    sm = {str(r["variant"]):r for r in summaries(rows) if int(r["task_spec_padding_bytes"]) == pbytes}
    pr = {str(r["variant"]):r for r in paired(rows) if int(r["task_spec_padding_bytes"]) == pbytes}
    for variant in VARIANTS:
        if variant not in sm or variant not in pr: continue
        s,p = sm[variant],pr[variant]
        print(f"  {variant:15s} thr={float(s['throughput_rps_mean']):8.1f} +/- {float(s['throughput_rps_ci95']):5.1f} rps (median={float(s['throughput_rps_median']):8.1f}, CV={float(s['throughput_rps_cv_pct']):4.1f}%) overhead={float(p['throughput_overhead_pct_vs_disabled_mean']):6.2f} +/- {float(p['throughput_overhead_pct_vs_disabled_ci95']):5.2f} pp speedup-vs-k1={float(p['throughput_speedup_pct_vs_k1_mean']):7.2f}% k1-loss-recovered={float(p['k1_lost_throughput_recovered_pct_mean']):7.2f}%")
    if not args.skip_control_plane:
        print("\nControl-plane amortization (profiling ON, separate clusters):")
        prows = run_profile_sweep(args,out)
        for r in sorted(prows,key=lambda x:int(x["frontier_k"])):
            print(f"  K={int(r['frontier_k']):2d} groups={int(r['expected_frontier_groups']):2d} reports/task={float(r['candidate_reports_per_task']):.4f} admissions/task={float(r['holder_admissions_per_task']):.4f} candidate-rpcs/task={float(r['borrower_candidate_physical_rpcs_per_task']):.4f}")


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run","_single-perf","_single-profile"], nargs="?", default="run")
    p.add_argument("--output-dir", default="gossip_benchmarks/results/58_recovery_frontier_succession_performance")
    p.add_argument("--task-spec-padding", type=parse_padding, nargs="+", default=[SpecPadding("1KiB",1024)])
    p.add_argument("--holders", type=int, default=R_DEFAULT); p.add_argument("--witness-count", type=int, default=R_DEFAULT)
    p.add_argument("--payload-bytes", type=int, default=1024); p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32); p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--repetitions", type=int, default=7); p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0); p.add_argument("--duration-seconds", type=float, default=20.0)
    p.add_argument("--cpus-per-node", type=int, default=4); p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0); p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--seed", type=int, default=42); p.add_argument("--overwrite", action="store_true")
    p.add_argument("--profile-tasks", type=int, default=32); p.add_argument("--profile-timeout-seconds", type=float, default=60.0)
    p.add_argument("--profile-stable-seconds", type=float, default=1.0); p.add_argument("--skip-control-plane", action="store_true")
    p.add_argument("--single-variant"); p.add_argument("--single-padding-name"); p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-repetition", type=int); p.add_argument("--single-output-json")
    return p


def main():
    args = parser().parse_args()
    if args.command == "_single-perf":
        if args.single_variant not in VARIANTS or os.environ.get("RAY_RECOVERY_PROFILING") != "0": raise ValueError("invalid timed child invocation")
        Path(args.single_output_json).write_text(json.dumps(single_perf(args), allow_nan=True))
    elif args.command == "_single-profile":
        if args.single_variant not in K_BY_VARIANT or os.environ.get("RAY_RECOVERY_PROFILING") != "1": raise ValueError("invalid profile child invocation")
        Path(args.single_output_json).write_text(json.dumps(single_profile(args), allow_nan=True))
    else:
        run_parent(args)

if __name__ == "__main__":
    main()
