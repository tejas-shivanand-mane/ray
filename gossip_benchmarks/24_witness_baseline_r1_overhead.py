#!/usr/bin/env python3
"""
Benchmark 24: fixed-R witness baseline overhead vs recovery disabled, R=1.

Compares:
  disabled
  original_r1
  optimized_r1

Steady-state only; profiling forced OFF.
Fresh subprocess + fresh Ray cluster per case.

Outputs:
  r1_overhead_runs.csv
  r1_overhead_summary.csv
  r1_overhead_compare.csv
"""

from __future__ import annotations

import argparse
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
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    disabled,
    mean_ci95,
    percentile,
    read_csv,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
    write_csv,
)

TARGET_HOLDERS = 1

OPT_ENV_VARS = [
    "RAY_RECOVERY_BASELINE_COMPACT_METADATA",
    "RAY_RECOVERY_BASELINE_WITNESS_BATCHING",
    "RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY",
    "RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE",
    "RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST",
    "RAY_RECOVERY_BASELINE_FAST_RECEIVER",
    "RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION",
    "RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC",
    "RAY_RECOVERY_BASELINE_BATCH_SWAP",
    "RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION",
    "RAY_RECOVERY_TASKMANAGER_PIN",
]


@dataclass(frozen=True)
class SpecPadding:
    name: str
    size_bytes: int


def parse_spec_padding(text: str) -> SpecPadding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("TaskSpec padding must be NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError("invalid TaskSpec padding")
    return SpecPadding(name, size)


def variant_env(base: dict[str, str], variant: str) -> dict[str, str]:
    env = dict(base)
    env["RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS"] = "0"
    env["RAY_RECOVERY_PROFILING"] = "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"

    if variant == "disabled":
        # These do not matter with recovery disabled, but make the environment
        # deterministic.
        for name in OPT_ENV_VARS:
            env[name] = "0"
        return env

    if variant == "original_r1":
        for name in OPT_ENV_VARS:
            env[name] = "0"
        return env

    if variant == "optimized_r1":
        for name in OPT_ENV_VARS:
            env[name] = "1"
        return env

    raise ValueError(variant)


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


def method_for_variant(variant: str):
    if variant == "disabled":
        return disabled()
    return witness_baseline(TARGET_HOLDERS)


def start_cluster(args: argparse.Namespace, variant: str) -> tuple[Cluster, list[str]]:
    method = method_for_variant(variant)
    cluster = Cluster()

    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=args.witness_count,
            profiling_enabled=False,
        ),
        include_dashboard=False,
    )

    producer = cluster.add_node(
        num_cpus=args.cpus_per_node,
        resources={"producer_node": 1},
    )
    consumer = cluster.add_node(
        num_cpus=args.cpus_per_node,
        resources={"consumer_1": 1},
    )
    return cluster, [producer.node_id, consumer.node_id]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int, *padding: bytes) -> bytes:
        if padding and padding[0]:
            _ = padding[0][0]
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch(self, wrapped_ref):
            value = ray.get(wrapped_ref[0])
            if len(value) < 8:
                raise RuntimeError("payload too small")
            return int.from_bytes(value[:8], "little", signed=False)

        def ping(self):
            import os
            return os.getpid()

    return produce, Consumer


def run_window(
    *,
    produce: Any,
    consumer: Any,
    producer_strategy: Any,
    padding: tuple[bytes, ...],
    payload_bytes: int,
    duration_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
    request_id_base: int,
) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, int] = {}
    submitted_ns: dict[int, int] = {}

    next_id = request_id_base
    completed_in_window = 0
    total_submitted = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    end_ns = start_ns + int(duration_s * 1e9)

    def submit_one():
        nonlocal next_id, total_submitted
        rid = next_id
        next_id += 1
        submitted_ns[rid] = time.perf_counter_ns()
        total_submitted += 1

        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(rid, payload_bytes, *padding)

        stage_ref = consumer.touch.remote([payload_ref])
        pending[stage_ref] = rid

    def process_ready(resubmit: bool):
        nonlocal completed_in_window
        if not pending:
            return
        ready, _ = ray.wait(
            list(pending),
            num_returns=min(32, len(pending)),
            timeout=wait_timeout_s,
        )
        for ref in ready:
            rid = pending.pop(ref)
            observed = int(ray.get(ref))
            if observed != rid:
                raise RuntimeError(f"expected {rid}, got {observed}")

            done_ns = time.perf_counter_ns()
            if done_ns <= end_ns:
                completed_in_window += 1
            latencies_ms.append((done_ns - submitted_ns.pop(rid)) / 1e6)

            if resubmit and time.perf_counter_ns() < end_ns:
                submit_one()

    for _ in range(inflight):
        submit_one()

    while time.perf_counter_ns() < end_ns:
        process_ready(True)

    deadline = time.monotonic() + drain_timeout_s
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"drain timeout with {len(pending)} refs")
        process_ready(False)

    return {
        "throughput_rps": completed_in_window / duration_s,
        "completed_in_window": completed_in_window,
        "total_pipeline_submitted": total_submitted,
        "latency_sample_count": len(latencies_ms),
        "latency_mean_ms": statistics.fmean(latencies_ms),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def run_single(args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    try:
        cluster, node_ids = start_cluster(args, args.single_variant)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 3, args.cluster_timeout_seconds)

        produce, Consumer = make_remote_types()
        consumer = Consumer.options(resources={"consumer_1": 0.01}, num_cpus=0).remote()
        ray.get(consumer.ping.remote())

        strategy = NodeAffinitySchedulingStrategy(node_id=node_ids[0], soft=False)
        padding = build_padding(args.single_padding_bytes, args.inline_chunk_bytes)

        if args.warmup_seconds > 0:
            run_window(
                produce=produce,
                consumer=consumer,
                producer_strategy=strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight=args.inflight,
                wait_timeout_s=args.wait_timeout_seconds,
                drain_timeout_s=args.drain_timeout_seconds,
                request_id_base=1_000_000,
            )

        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        perf = run_window(
            produce=produce,
            consumer=consumer,
            producer_strategy=strategy,
            padding=padding,
            payload_bytes=args.payload_bytes,
            duration_s=args.duration_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
            request_id_base=10_000_000,
        )

        return {
            "variant": args.single_variant,
            "repetition": args.single_repetition,
            "target_holders": TARGET_HOLDERS,
            "borrower_count": 1,
            "task_spec_padding_name": args.single_padding_name,
            "task_spec_padding_bytes": args.single_padding_bytes,
            "profiling_enabled": 0,
            **perf,
        }
    finally:
        safe_shutdown(ray, cluster)


METRICS = [
    "throughput_rps",
    "latency_mean_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
]


def summarize(rows):
    groups = sorted(
        {
            (
                row["variant"],
                int(row["task_spec_padding_bytes"]),
                row["task_spec_padding_name"],
            )
            for row in rows
        },
        key=lambda x: (x[1], x[0]),
    )

    out = []
    for variant, size_bytes, size_name in groups:
        matched = [
            r for r in rows
            if r["variant"] == variant
            and int(r["task_spec_padding_bytes"]) == size_bytes
        ]
        item = {
            "variant": variant,
            "task_spec_padding_name": size_name,
            "task_spec_padding_bytes": size_bytes,
            "target_holders": TARGET_HOLDERS,
            "repetitions": len(matched),
        }
        for metric in METRICS:
            vals = [float(r[metric]) for r in matched]
            mean, ci95 = mean_ci95(vals)
            item[f"{metric}_mean"] = mean
            item[f"{metric}_ci95"] = ci95
        out.append(item)
    return out


def compare_rows(summary):
    out = []
    sizes = sorted({
        (int(r["task_spec_padding_bytes"]), r["task_spec_padding_name"])
        for r in summary
    })
    for size_bytes, size_name in sizes:
        d = next((r for r in summary if r["variant"] == "disabled"
                  and int(r["task_spec_padding_bytes"]) == size_bytes), None)
        o = next((r for r in summary if r["variant"] == "original_r1"
                  and int(r["task_spec_padding_bytes"]) == size_bytes), None)
        a = next((r for r in summary if r["variant"] == "optimized_r1"
                  and int(r["task_spec_padding_bytes"]) == size_bytes), None)
        if d is None:
            continue

        dthr = float(d["throughput_rps_mean"])
        dp95 = float(d["latency_p95_ms_mean"])

        # Keep a fixed schema even while the matrix is only partially complete.
        # write_csv() derives its fieldnames from the first row, so conditionally
        # adding original/optimized fields causes incremental checkpoint writes
        # to fail when later rows contain additional keys.
        row = {
            "task_spec_padding_name": size_name,
            "task_spec_padding_bytes": size_bytes,
            "disabled_throughput_rps": dthr,
            "disabled_p95_ms": dp95,
            "original_r1_throughput_rps": math.nan,
            "original_r1_throughput_overhead_pct_vs_disabled": math.nan,
            "original_r1_p95_ms": math.nan,
            "original_r1_p95_inflation_pct_vs_disabled": math.nan,
            "optimized_r1_throughput_rps": math.nan,
            "optimized_r1_throughput_overhead_pct_vs_disabled": math.nan,
            "optimized_r1_p95_ms": math.nan,
            "optimized_r1_p95_inflation_pct_vs_disabled": math.nan,
        }

        for label, r in [("original_r1", o), ("optimized_r1", a)]:
            if r is None:
                continue
            thr = float(r["throughput_rps_mean"])
            p95 = float(r["latency_p95_ms_mean"])
            row[f"{label}_throughput_rps"] = thr
            row[f"{label}_throughput_overhead_pct_vs_disabled"] = (
                100.0 * (dthr - thr) / dthr if dthr else math.nan
            )
            row[f"{label}_p95_ms"] = p95
            row[f"{label}_p95_inflation_pct_vs_disabled"] = (
                100.0 * (p95 - dp95) / dp95 if dp95 else math.nan
            )

        out.append(row)
    return out


def write_outputs(output_dir: Path, rows):
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "r1_overhead_runs.csv", rows)
    summary = summarize(rows)
    write_csv(output_dir / "r1_overhead_summary.csv", summary)
    write_csv(output_dir / "r1_overhead_compare.csv", compare_rows(summary))


def case_key(row):
    return (
        row["variant"],
        int(row["task_spec_padding_bytes"]),
        int(row["repetition"]),
    )


def child_command(args, variant, padding, repetition, output_json):
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-run",
        "--single-variant", variant,
        "--single-padding-name", padding.name,
        "--single-padding-bytes", str(padding.size_bytes),
        "--single-repetition", str(repetition),
        "--single-output-json", str(output_json),
        "--payload-bytes", str(args.payload_bytes),
        "--inline-chunk-bytes", str(args.inline_chunk_bytes),
        "--warmup-seconds", str(args.warmup_seconds),
        "--settle-seconds", str(args.settle_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--inflight", str(args.inflight),
        "--cpus-per-node", str(args.cpus_per_node),
        "--witness-count", str(args.witness_count),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
    ]


def run_parent(args):
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_path = out / "r1_overhead_runs.csv"

    if args.overwrite:
        for name in ["r1_overhead_runs.csv", "r1_overhead_summary.csv", "r1_overhead_compare.csv"]:
            (out / name).unlink(missing_ok=True)

    rows = [dict(r) for r in read_csv(runs_path)] if runs_path.exists() else []
    completed = {case_key(r) for r in rows}

    variants = ["disabled", "original_r1", "optimized_r1"]
    cases = [
        (variant, padding, rep)
        for rep in range(1, args.repetitions + 1)
        for padding in args.task_spec_padding
        for variant in variants
    ]

    if not args.fixed_order:
        random.Random(args.seed).shuffle(cases)

    pending = [
        c for c in cases
        if (c[0], c[1].size_bytes, c[2]) not in completed
    ]

    print(f"R=1 overhead cases={len(cases)}, remaining={len(pending)}")

    for i, (variant, padding, rep) in enumerate(pending, 1):
        print(f"[{i}/{len(pending)}] rep={rep} variant={variant} TaskSpec={padding.name}", flush=True)
        temp = out / f".single_{variant}_{padding.size_bytes}_{rep}.json"
        temp.unlink(missing_ok=True)

        proc = subprocess.run(
            child_command(args, variant, padding, rep, temp),
            env=variant_env(os.environ, variant),
        )
        if proc.returncode != 0 or not temp.exists():
            write_outputs(out, rows)
            raise SystemExit(proc.returncode or 1)

        row = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        rows.append(row)
        write_outputs(out, rows)

        print(
            f"  throughput={row['throughput_rps']:.1f} rps "
            f"p95={row['latency_p95_ms']:.2f} ms",
            flush=True,
        )


def run_single_child(args):
    if None in (
        args.single_variant,
        args.single_padding_name,
        args.single_padding_bytes,
        args.single_repetition,
        args.single_output_json,
    ):
        raise ValueError("missing internal _single-run args")

    if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
        raise RuntimeError("profiling must be disabled")

    row = run_single(args)
    Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))


def parser():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-run"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/24_witness_baseline_r1_overhead",
    )
    p.add_argument(
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
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warmup-seconds", type=float, default=3.0)
    p.add_argument("--settle-seconds", type=float, default=0.5)
    p.add_argument("--duration-seconds", type=float, default=15.0)
    p.add_argument("--inflight", type=int, default=64)
    p.add_argument("--cpus-per-node", type=int, default=3)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--fixed-order", action="store_true")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--single-variant")
    p.add_argument("--single-padding-name")
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-output-json")
    return p


def main():
    args = parser().parse_args()
    if args.command == "_single-run":
        run_single_child(args)
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
