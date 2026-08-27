#!/usr/bin/env python3
"""Benchmark 30: real Fixed-R Recovery Frontier performance.

Compares the correctness-capable implementation, not the old density proxy:

  disabled
  fixed_r                 (frozen Fixed-R full-lineage replication)
  frontier_k1             (degeneration control; same behavior as fixed_r)
  frontier_k4
  frontier_k8
  frontier_k16
  frontier_k32

All recovery-enabled cases use R=2 by default.

Workload
--------
Independent small tasks are submitted in fixed-size bursts *before* their
ObjectRefs are exported to a downstream consumer. This is important: the real
Recovery Frontier planner can only amortize a protection operation across tasks
that are already registered when the first member of the group is exported.
Every variant sees the exact same burst workload; only the recovery mechanism
changes.

The default burst size is 32, so K in {1,4,8,16,32} is measured on full groups.
Profiling is forced OFF during timed runs.

Outputs
-------
  frontier_perf_runs.csv
  frontier_perf_summary.csv
  frontier_perf_compare.csv

The comparison CSV reports throughput overhead vs Disabled and the fraction of
Fixed-R lost throughput recovered by each frontier K.
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


DEFAULT_R = 2
VARIANTS = [
    "disabled",
    "fixed_r",
    "frontier_k1",
    "frontier_k4",
    "frontier_k8",
    "frontier_k16",
    "frontier_k32",
]
FRONTIER_K = {
    "frontier_k1": 1,
    "frontier_k4": 4,
    "frontier_k8": 8,
    "frontier_k16": 16,
    "frontier_k32": 32,
}


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


def method_for_variant(variant: str, holders: int):
    if variant == "disabled":
        return disabled()
    return witness_baseline(holders)


def variant_env(base: dict[str, str]) -> dict[str, str]:
    env = dict(base)
    # Keep timed runs deterministic and avoid profiling perturbation.
    env["RAY_RECOVERY_PROFILING"] = "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
    return env


def start_cluster(args: argparse.Namespace, variant: str) -> tuple[Cluster, str]:
    method = method_for_variant(variant, args.holders)
    config = system_config(
        method,
        witness_count=args.witness_count,
        profiling_enabled=False,
    )

    if variant in FRONTIER_K:
        config.update(
            {
                "enable_recovery_frontier": True,
                "recovery_frontier_group_size": FRONTIER_K[variant],
                "recovery_baseline_perf_protect_every_n": 1,
            }
        )
    else:
        config.update(
            {
                "enable_recovery_frontier": False,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
            }
        )

    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )
    producer = cluster.add_node(
        num_cpus=args.cpus_per_node,
        resources={"producer_node": 1},
    )
    cluster.add_node(
        num_cpus=args.cpus_per_node,
        resources={"consumer_node": 1},
    )
    # The driver is on the head node and is the ObjectRef owner. Add a spare
    # node so R=2 always has at least two non-owner, node-distinct choices even
    # if the producer/consumer placement changes slightly.
    cluster.add_node(num_cpus=0, resources={"spare_holder": 1})
    return cluster, producer.node_id


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int, *padding: bytes) -> bytes:
        # Touch padding so the argument is not optimized away conceptually. The
        # bytes are part of the replayable TaskSpec and stress the small-task path.
        if padding and padding[0]:
            _ = padding[0][0]
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    @ray.remote(max_restarts=0, max_concurrency=256)
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
    inflight_tasks: int,
    burst_size: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
    request_id_base: int,
) -> dict[str, Any]:
    if burst_size <= 0:
        raise ValueError("burst_size must be positive")
    if inflight_tasks < burst_size:
        raise ValueError("inflight_tasks must be >= burst_size")
    if inflight_tasks % burst_size != 0:
        raise ValueError("inflight_tasks must be divisible by burst_size")

    pending: dict[ray.ObjectRef, int] = {}
    submitted_ns: dict[int, int] = {}
    next_id = request_id_base
    completed_in_window = 0
    total_submitted = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    end_ns = start_ns + int(duration_s * 1e9)

    def submit_burst() -> None:
        nonlocal next_id, total_submitted

        # Critical ordering: register every producer task in the burst first.
        # Only then export refs to Consumer. This gives K>1 a full registered
        # suffix to protect while keeping the workload identical for all cases.
        burst: list[tuple[int, ray.ObjectRef]] = []
        for _ in range(burst_size):
            rid = next_id
            next_id += 1
            submitted_ns[rid] = time.perf_counter_ns()
            total_submitted += 1
            payload_ref = produce.options(
                scheduling_strategy=producer_strategy,
                num_cpus=1,
            ).remote(rid, payload_bytes, *padding)
            burst.append((rid, payload_ref))

        for rid, payload_ref in burst:
            stage_ref = consumer.touch.remote([payload_ref])
            pending[stage_ref] = rid

    def process_ready() -> None:
        nonlocal completed_in_window
        if not pending:
            return
        ready, _ = ray.wait(
            list(pending),
            num_returns=min(64, len(pending)),
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

    while len(pending) + burst_size <= inflight_tasks:
        submit_burst()

    while time.perf_counter_ns() < end_ns:
        process_ready()
        while (
            time.perf_counter_ns() < end_ns
            and len(pending) + burst_size <= inflight_tasks
        ):
            submit_burst()

    deadline = time.monotonic() + drain_timeout_s
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"drain timeout with {len(pending)} refs")
        process_ready()

    if not latencies_ms:
        raise RuntimeError("no completed tasks")

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
        cluster, producer_node_id = start_cluster(args, args.single_variant)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 4, args.cluster_timeout_seconds)

        produce, Consumer = make_remote_types()
        consumer = Consumer.options(
            resources={"consumer_node": 0.01}, num_cpus=0
        ).remote()
        ray.get(consumer.ping.remote())

        strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node_id,
            soft=False,
        )
        padding = build_padding(
            args.single_padding_bytes,
            args.inline_chunk_bytes,
        )

        if args.warmup_seconds > 0:
            run_window(
                produce=produce,
                consumer=consumer,
                producer_strategy=strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight_tasks=args.inflight_tasks,
                burst_size=args.burst_size,
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
            inflight_tasks=args.inflight_tasks,
            burst_size=args.burst_size,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
            request_id_base=10_000_000,
        )

        return {
            "variant": args.single_variant,
            "frontier_k": FRONTIER_K.get(args.single_variant, 0),
            "repetition": args.single_repetition,
            "holders": args.holders,
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


METRICS = [
    "throughput_rps",
    "latency_mean_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = sorted(
        {
            (
                row["variant"],
                int(row["task_spec_padding_bytes"]),
                row["task_spec_padding_name"],
            )
            for row in rows
        },
        key=lambda x: (x[1], VARIANTS.index(x[0])),
    )

    out: list[dict[str, Any]] = []
    for variant, size_bytes, size_name in groups:
        matched = [
            r
            for r in rows
            if r["variant"] == variant
            and int(r["task_spec_padding_bytes"]) == size_bytes
        ]
        item: dict[str, Any] = {
            "variant": variant,
            "frontier_k": FRONTIER_K.get(variant, 0),
            "task_spec_padding_name": size_name,
            "task_spec_padding_bytes": size_bytes,
            "holders": int(matched[0]["holders"]),
            "burst_size": int(matched[0]["burst_size"]),
            "repetitions": len(matched),
        }
        for metric in METRICS:
            vals = [float(r[metric]) for r in matched]
            mean, ci95 = mean_ci95(vals)
            item[f"{metric}_mean"] = mean
            item[f"{metric}_ci95"] = ci95
        out.append(item)
    return out


def compare_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    sizes = sorted(
        {
            (int(r["task_spec_padding_bytes"]), r["task_spec_padding_name"])
            for r in summary
        }
    )

    for size_bytes, size_name in sizes:
        by_variant = {
            r["variant"]: r
            for r in summary
            if int(r["task_spec_padding_bytes"]) == size_bytes
        }
        disabled_row = by_variant.get("disabled")
        fixed_row = by_variant.get("fixed_r")
        if disabled_row is None:
            continue

        dthr = float(disabled_row["throughput_rps_mean"])
        dp95 = float(disabled_row["latency_p95_ms_mean"])
        fixed_thr = (
            float(fixed_row["throughput_rps_mean"])
            if fixed_row is not None
            else math.nan
        )
        baseline_loss = dthr - fixed_thr if fixed_row is not None else math.nan

        for variant in VARIANTS:
            row = by_variant.get(variant)
            if row is None:
                continue
            thr = float(row["throughput_rps_mean"])
            p95 = float(row["latency_p95_ms_mean"])
            overhead = 100.0 * (dthr - thr) / dthr if dthr else math.nan
            p95_inflation = 100.0 * (p95 - dp95) / dp95 if dp95 else math.nan

            if variant.startswith("frontier_") and baseline_loss > 0:
                recovered = 100.0 * (thr - fixed_thr) / baseline_loss
            elif variant == "fixed_r":
                recovered = 0.0
            elif variant == "disabled":
                recovered = 100.0
            else:
                recovered = math.nan

            out.append(
                {
                    "task_spec_padding_name": size_name,
                    "task_spec_padding_bytes": size_bytes,
                    "variant": variant,
                    "frontier_k": FRONTIER_K.get(variant, 0),
                    "throughput_rps": thr,
                    "throughput_overhead_pct_vs_disabled": overhead,
                    "fixed_r_lost_throughput_recovered_pct": recovered,
                    "p95_ms": p95,
                    "p95_inflation_pct_vs_disabled": p95_inflation,
                }
            )
    return out


def write_outputs(output_dir: Path, rows: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "frontier_perf_runs.csv", rows)
    summary = summarize(rows)
    write_csv(output_dir / "frontier_perf_summary.csv", summary)
    write_csv(output_dir / "frontier_perf_compare.csv", compare_rows(summary))


def case_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return (
        row["variant"],
        int(row["task_spec_padding_bytes"]),
        int(row["repetition"]),
    )


def child_command(
    args: argparse.Namespace,
    variant: str,
    padding: SpecPadding,
    repetition: int,
    output_json: Path,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-run",
        "--single-variant",
        variant,
        "--single-padding-name",
        padding.name,
        "--single-padding-bytes",
        str(padding.size_bytes),
        "--single-repetition",
        str(repetition),
        "--single-output-json",
        str(output_json),
        "--holders",
        str(args.holders),
        "--witness-count",
        str(args.witness_count),
        "--payload-bytes",
        str(args.payload_bytes),
        "--inline-chunk-bytes",
        str(args.inline_chunk_bytes),
        "--burst-size",
        str(args.burst_size),
        "--inflight-tasks",
        str(args.inflight_tasks),
        "--warmup-seconds",
        str(args.warmup_seconds),
        "--settle-seconds",
        str(args.settle_seconds),
        "--duration-seconds",
        str(args.duration_seconds),
        "--cpus-per-node",
        str(args.cpus_per_node),
        "--cluster-timeout-seconds",
        str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds",
        str(args.wait_timeout_seconds),
        "--drain-timeout-seconds",
        str(args.drain_timeout_seconds),
    ]


def print_checkpoint(rows: list[dict[str, Any]], padding_bytes: int) -> None:
    summary = summarize(rows)
    available = {
        r["variant"]: r
        for r in summary
        if int(r["task_spec_padding_bytes"]) == padding_bytes
    }
    if "disabled" not in available:
        return
    dthr = float(available["disabled"]["throughput_rps_mean"])
    fixed = available.get("fixed_r")
    fixed_thr = float(fixed["throughput_rps_mean"]) if fixed else math.nan
    print("  current means:")
    for variant in VARIANTS:
        row = available.get(variant)
        if row is None:
            continue
        thr = float(row["throughput_rps_mean"])
        overhead = 100.0 * (dthr - thr) / dthr if dthr else math.nan
        if fixed and dthr > fixed_thr and variant.startswith("frontier_"):
            recovered = 100.0 * (thr - fixed_thr) / (dthr - fixed_thr)
            rec_text = f", recovered={recovered:.1f}%"
        else:
            rec_text = ""
        print(
            f"    {variant:12s} {thr:9.1f} rps  overhead={overhead:6.2f}%{rec_text}"
        )


def run_parent(args: argparse.Namespace) -> None:
    if args.burst_size % max(FRONTIER_K.values()) != 0:
        raise ValueError(
            "For the default full-group comparison, burst_size must be divisible by 32"
        )
    if args.inflight_tasks % args.burst_size != 0:
        raise ValueError("inflight_tasks must be divisible by burst_size")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_path = out / "frontier_perf_runs.csv"

    if args.overwrite:
        for name in [
            "frontier_perf_runs.csv",
            "frontier_perf_summary.csv",
            "frontier_perf_compare.csv",
        ]:
            (out / name).unlink(missing_ok=True)

    rows = [dict(r) for r in read_csv(runs_path)] if runs_path.exists() else []
    completed = {case_key(r) for r in rows}

    cases = [
        (variant, padding, rep)
        for rep in range(1, args.repetitions + 1)
        for padding in args.task_spec_padding
        for variant in VARIANTS
    ]
    if not args.fixed_order:
        random.Random(args.seed).shuffle(cases)

    pending = [
        c
        for c in cases
        if (c[0], c[1].size_bytes, c[2]) not in completed
    ]

    print(
        f"Recovery Frontier performance: R={args.holders}, burst={args.burst_size}, "
        f"cases={len(cases)}, remaining={len(pending)}"
    )

    for i, (variant, padding, rep) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep} variant={variant} "
            f"TaskSpec={padding.name}",
            flush=True,
        )
        temp = out / f".single_{variant}_{padding.size_bytes}_{rep}.json"
        temp.unlink(missing_ok=True)

        proc = subprocess.run(
            child_command(args, variant, padding, rep, temp),
            env=variant_env(os.environ),
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
        print_checkpoint(rows, padding.size_bytes)

    print("\nFinal comparison:")
    comparison = compare_rows(summarize(rows))
    for row in comparison:
        if int(row["task_spec_padding_bytes"]) != args.task_spec_padding[0].size_bytes:
            continue
        print(
            f"  {row['variant']:12s} "
            f"throughput={float(row['throughput_rps']):9.1f} rps  "
            f"overhead={float(row['throughput_overhead_pct_vs_disabled']):6.2f}%  "
            f"recovered={float(row['fixed_r_lost_throughput_recovered_pct']):7.2f}%  "
            f"p95={float(row['p95_ms']):8.2f} ms"
        )


def run_single_child(args: argparse.Namespace) -> None:
    required = [
        args.single_variant,
        args.single_padding_name,
        args.single_padding_bytes,
        args.single_repetition,
        args.single_output_json,
    ]
    if any(v is None for v in required):
        raise ValueError("missing internal _single-run args")
    if args.single_variant not in VARIANTS:
        raise ValueError(f"unknown variant: {args.single_variant}")
    if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
        raise RuntimeError("profiling must be disabled")

    row = run_single(args)
    Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-run"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/30_recovery_frontier_performance",
    )
    # Start with the small-task regime where Fixed-R paid its largest control
    # overhead. Additional sizes can be supplied explicitly after this result.
    p.add_argument(
        "--task-spec-padding",
        type=parse_spec_padding,
        nargs="+",
        default=[SpecPadding("1KiB", 1024)],
    )
    p.add_argument("--holders", type=int, default=DEFAULT_R)
    p.add_argument("--witness-count", type=int, default=DEFAULT_R)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warmup-seconds", type=float, default=3.0)
    p.add_argument("--settle-seconds", type=float, default=0.5)
    p.add_argument("--duration-seconds", type=float, default=15.0)
    p.add_argument("--cpus-per-node", type=int, default=4)
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


def main() -> None:
    args = parser().parse_args()
    if args.command == "_single-run":
        run_single_child(args)
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
