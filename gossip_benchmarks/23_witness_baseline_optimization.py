#!/usr/bin/env python3
"""
Benchmark 23: optimized fixed-R witness-holder baseline performance study.

Purpose
-------
Measure the fixed-R witness-holder baseline itself, with Recovery Succession
completely absent from the benchmark matrix.

Two studies:
  compare   Original fixed-R baseline vs all baseline optimizations.
  ablation  All optimizations vs leave-one-out variants.
  all       Both.

Every case runs in a fresh subprocess/cluster. Recovery profiling is forced OFF
for timed measurements. Correctness/recovery validation should be done
separately (Benchmarks 01/05 and the baseline optimization bisect).

Default workload:
  R = 4 fixed witness holders
  B = 1 direct downstream borrower
  TaskSpec padding = 1KiB, 16KiB, 256KiB, 1MiB
  payload = 1KiB
  3 repetitions
  3s warmup + 15s timed window
  64 in-flight pipelines

Outputs:
  baseline_optimization_runs.csv
  baseline_optimization_summary.csv
  baseline_optimization_compare.csv
  baseline_optimization_ablation.csv

Examples
--------
Compare original vs fully optimized:
  python gossip_benchmarks/23_witness_baseline_optimization.py run \
      --study compare --overwrite

Leave-one-out study:
  python gossip_benchmarks/23_witness_baseline_optimization.py run \
      --study ablation --overwrite

Both:
  python gossip_benchmarks/23_witness_baseline_optimization.py run \
      --study all --overwrite
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

# Set before importing ray in every parent/child process.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    mean_ci95,
    percentile,
    read_csv,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
    write_csv,
)

TARGET_HOLDERS = 4

# Explicit controls are used instead of relying on ALL so ambient shell state
# cannot contaminate original/all/leave-one-out variants.
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

SHORT_NAMES = {
    "RAY_RECOVERY_BASELINE_COMPACT_METADATA": "compact_metadata",
    "RAY_RECOVERY_BASELINE_WITNESS_BATCHING": "witness_batching",
    "RAY_RECOVERY_BASELINE_ELIDE_TASKSPEC_COPY": "elide_taskspec_copy",
    "RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE": "serialize_once",
    "RAY_RECOVERY_BASELINE_SEPARATE_MANIFEST": "separate_manifest",
    "RAY_RECOVERY_BASELINE_FAST_RECEIVER": "fast_receiver",
    "RAY_RECOVERY_BASELINE_FAST_MANIFEST_VALIDATION": "fast_manifest_validation",
    "RAY_RECOVERY_BASELINE_MOVE_WITNESS_TASKSPEC": "move_witness_taskspec",
    "RAY_RECOVERY_BASELINE_BATCH_SWAP": "batch_swap",
    "RAY_RECOVERY_BASELINE_TOPK_WITNESS_SELECTION": "topk_witness_selection",
    "RAY_RECOVERY_TASKMANAGER_PIN": "taskmanager_pin",
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
        raise argparse.ArgumentTypeError("TaskSpec padding requires NAME and BYTES >= 0")
    return SpecPadding(name, size)


def variant_settings(variant: str) -> dict[str, int]:
    if variant == "original":
        return {name: 0 for name in OPT_ENV_VARS}

    settings = {name: 1 for name in OPT_ENV_VARS}
    if variant == "all_optimized":
        return settings

    if not variant.startswith("all_minus_"):
        raise ValueError(f"unknown variant {variant!r}")

    short = variant[len("all_minus_") :]
    matches = [env for env, name in SHORT_NAMES.items() if name == short]
    if len(matches) != 1:
        raise ValueError(f"unknown leave-one-out variant {variant!r}")
    settings[matches[0]] = 0
    return settings


def variants_for_study(study: str) -> list[str]:
    if study == "compare":
        return ["original", "all_optimized"]
    if study == "ablation":
        return ["all_optimized"] + [
            f"all_minus_{SHORT_NAMES[env]}" for env in OPT_ENV_VARS
        ]
    if study == "all":
        return ["original", "all_optimized"] + [
            f"all_minus_{SHORT_NAMES[env]}" for env in OPT_ENV_VARS
        ]
    raise ValueError(study)


def clean_variant_environment(base: dict[str, str], variant: str) -> dict[str, str]:
    env = dict(base)

    # Never allow the convenience ALL switch or old research switches to alter
    # this experiment. The exact variant is encoded explicitly below.
    env["RAY_RECOVERY_BASELINE_ALL_OPTIMIZATIONS"] = "0"
    env["RAY_RECOVERY_PROFILING"] = "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"

    for name, enabled in variant_settings(variant).items():
        env[name] = str(enabled)

    return env


def build_padding(total_bytes: int, chunk_bytes: int) -> tuple[bytes, ...]:
    if total_bytes <= 0:
        return ()
    out: list[bytes] = []
    remaining = total_bytes
    token = 1
    while remaining > 0:
        n = min(remaining, chunk_bytes)
        out.append(bytes([token % 251]) * n)
        remaining -= n
        token += 1
    return tuple(out)


def start_cluster(args: argparse.Namespace) -> tuple[Cluster, list[str]]:
    method = witness_baseline(TARGET_HOLDERS)
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

    nodes = [
        cluster.add_node(
            num_cpus=args.cpus_per_node,
            resources={"producer_node": 1},
        )
    ]

    # Four distinct consumer nodes ensure at least R independent non-owner nodes.
    for i in range(1, TARGET_HOLDERS + 1):
        nodes.append(
            cluster.add_node(
                num_cpus=args.cpus_per_node,
                resources={f"consumer_{i}": 1},
            )
        )

    return cluster, [n.node_id for n in nodes]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        request_id: int,
        payload_bytes: int,
        *lineage_padding: bytes,
    ) -> bytes:
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

    return produce, Consumer


def run_window(
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
    pending: dict[ray.ObjectRef, tuple[int, bool]] = {}
    remaining: dict[int, int] = {}
    submitted_ns: dict[int, int] = {}

    next_request_id = request_id_base
    total_submitted = 0
    completed_in_window = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    end_ns = start_ns + int(duration_s * 1e9)

    def submit_one() -> None:
        nonlocal next_request_id, total_submitted
        request_id = next_request_id
        next_request_id += 1
        submitted_ns[request_id] = time.perf_counter_ns()
        total_submitted += 1

        payload_ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes, *padding)

        remaining[request_id] = borrower_count
        for i in range(borrower_count):
            stage_ref = consumers[i].touch.remote([payload_ref])
            pending[stage_ref] = (request_id, False)

    def process_ready(resubmit: bool) -> None:
        nonlocal completed_in_window
        if not pending:
            return

        ready, _ = ray.wait(
            list(pending),
            num_returns=min(32, len(pending)),
            timeout=wait_timeout_s,
        )
        for ready_ref in ready:
            request_id, _ = pending.pop(ready_ref)
            observed = int(ray.get(ready_ref))
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

            if resubmit and time.perf_counter_ns() < end_ns:
                submit_one()

    for _ in range(inflight):
        submit_one()

    while time.perf_counter_ns() < end_ns:
        process_ready(True)

    deadline = time.monotonic() + drain_timeout_s
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"drain timeout with {len(remaining)} pipelines and "
                f"{len(pending)} stage refs pending"
            )
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


def run_single_case(args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    try:
        cluster, node_ids = start_cluster(args)
        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        wait_for_cluster(ray, TARGET_HOLDERS + 2, args.cluster_timeout_seconds)

        produce, Consumer = make_remote_types()
        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([c.ping.remote() for c in consumers])

        strategy = NodeAffinitySchedulingStrategy(
            node_id=node_ids[0],
            soft=False,
        )
        padding = build_padding(
            args.single_padding_bytes,
            args.inline_chunk_bytes,
        )

        if args.warmup_seconds > 0:
            run_window(
                produce=produce,
                consumers=consumers,
                borrower_count=args.borrowers,
                producer_strategy=strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight=args.inflight,
                wait_timeout_s=args.wait_timeout_seconds,
                drain_timeout_s=args.drain_timeout_seconds,
                request_id_base=1_000_000,
            )

        # Small settle interval keeps cleanup from the warmup from contaminating
        # the first part of the timed window without introducing profiling.
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        perf = run_window(
            produce=produce,
            consumers=consumers,
            borrower_count=args.borrowers,
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
            "borrower_count": args.borrowers,
            "target_holders": TARGET_HOLDERS,
            "task_spec_padding_name": args.single_padding_name,
            "task_spec_padding_bytes": args.single_padding_bytes,
            "payload_bytes": args.payload_bytes,
            "inline_chunk_bytes": args.inline_chunk_bytes,
            "warmup_seconds": args.warmup_seconds,
            "duration_seconds": args.duration_seconds,
            "inflight": args.inflight,
            "profiling_enabled": 0,
            **perf,
        }
    finally:
        safe_shutdown(ray, cluster)


PERF_METRICS = [
    "throughput_rps",
    "latency_mean_ms",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = sorted(
        {
            (
                row["variant"],
                int(row["task_spec_padding_bytes"]),
                row["task_spec_padding_name"],
                int(row["borrower_count"]),
            )
            for row in rows
        },
        key=lambda x: (x[1], x[0]),
    )

    out: list[dict[str, Any]] = []
    for variant, size_bytes, size_name, borrowers in keys:
        matched = [
            row
            for row in rows
            if row["variant"] == variant
            and int(row["task_spec_padding_bytes"]) == size_bytes
            and int(row["borrower_count"]) == borrowers
        ]
        item: dict[str, Any] = {
            "variant": variant,
            "task_spec_padding_name": size_name,
            "task_spec_padding_bytes": size_bytes,
            "borrower_count": borrowers,
            "target_holders": TARGET_HOLDERS,
            "repetitions": len(matched),
        }
        for metric in PERF_METRICS:
            values = [float(row[metric]) for row in matched]
            mean, ci95 = mean_ci95(values)
            item[f"{metric}_mean"] = mean
            item[f"{metric}_ci95"] = ci95
        out.append(item)
    return out


def comparison_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    sizes = sorted(
        {
            (int(r["task_spec_padding_bytes"]), r["task_spec_padding_name"])
            for r in summary
        }
    )
    for size_bytes, size_name in sizes:
        original = next(
            (
                r
                for r in summary
                if r["variant"] == "original"
                and int(r["task_spec_padding_bytes"]) == size_bytes
            ),
            None,
        )
        optimized = next(
            (
                r
                for r in summary
                if r["variant"] == "all_optimized"
                and int(r["task_spec_padding_bytes"]) == size_bytes
            ),
            None,
        )
        if original is None or optimized is None:
            continue

        o_thr = float(original["throughput_rps_mean"])
        a_thr = float(optimized["throughput_rps_mean"])
        o_p95 = float(original["latency_p95_ms_mean"])
        a_p95 = float(optimized["latency_p95_ms_mean"])

        out.append(
            {
                "task_spec_padding_name": size_name,
                "task_spec_padding_bytes": size_bytes,
                "borrower_count": int(original["borrower_count"]),
                "original_throughput_rps": o_thr,
                "optimized_throughput_rps": a_thr,
                "optimized_throughput_change_pct": (
                    100.0 * (a_thr - o_thr) / o_thr if o_thr else math.nan
                ),
                "original_p95_ms": o_p95,
                "optimized_p95_ms": a_p95,
                "optimized_p95_change_pct": (
                    100.0 * (a_p95 - o_p95) / o_p95 if o_p95 else math.nan
                ),
            }
        )
    return out


def ablation_rows(summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    all_rows = {
        int(r["task_spec_padding_bytes"]): r
        for r in summary
        if r["variant"] == "all_optimized"
    }

    for row in summary:
        variant = row["variant"]
        if not variant.startswith("all_minus_"):
            continue
        size = int(row["task_spec_padding_bytes"])
        base = all_rows.get(size)
        if base is None:
            continue

        full_thr = float(base["throughput_rps_mean"])
        ablated_thr = float(row["throughput_rps_mean"])
        full_p95 = float(base["latency_p95_ms_mean"])
        ablated_p95 = float(row["latency_p95_ms_mean"])

        out.append(
            {
                "ablated_optimization": variant[len("all_minus_") :],
                "task_spec_padding_name": row["task_spec_padding_name"],
                "task_spec_padding_bytes": size,
                "borrower_count": int(row["borrower_count"]),
                "all_optimized_throughput_rps": full_thr,
                "ablated_throughput_rps": ablated_thr,
                # Positive means the optimization helps throughput.
                "optimization_throughput_contribution_pct": (
                    100.0 * (full_thr - ablated_thr) / ablated_thr
                    if ablated_thr
                    else math.nan
                ),
                "all_optimized_p95_ms": full_p95,
                "ablated_p95_ms": ablated_p95,
                # Negative means the optimization lowers p95 (good).
                "optimization_p95_effect_pct": (
                    100.0 * (full_p95 - ablated_p95) / ablated_p95
                    if ablated_p95
                    else math.nan
                ),
            }
        )

    out.sort(
        key=lambda r: (
            int(r["task_spec_padding_bytes"]),
            r["ablated_optimization"],
        )
    )
    return out


def write_outputs(output_dir: Path, rows: list[dict[str, Any]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(output_dir / "baseline_optimization_runs.csv", rows)
    summary = summarize(rows)
    write_csv(output_dir / "baseline_optimization_summary.csv", summary)

    compare = comparison_rows(summary)
    if compare:
        write_csv(output_dir / "baseline_optimization_compare.csv", compare)

    ablations = ablation_rows(summary)
    if ablations:
        write_csv(output_dir / "baseline_optimization_ablation.csv", ablations)


def case_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return (
        str(row["variant"]),
        int(row["task_spec_padding_bytes"]),
        int(row["repetition"]),
    )


def child_command(
    args: argparse.Namespace,
    *,
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
        "--borrowers",
        str(args.borrowers),
        "--payload-bytes",
        str(args.payload_bytes),
        "--inline-chunk-bytes",
        str(args.inline_chunk_bytes),
        "--warmup-seconds",
        str(args.warmup_seconds),
        "--settle-seconds",
        str(args.settle_seconds),
        "--duration-seconds",
        str(args.duration_seconds),
        "--inflight",
        str(args.inflight),
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
    ]


def run_parent(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    runs_path = output_dir / "baseline_optimization_runs.csv"

    if args.overwrite:
        for name in [
            "baseline_optimization_runs.csv",
            "baseline_optimization_summary.csv",
            "baseline_optimization_compare.csv",
            "baseline_optimization_ablation.csv",
        ]:
            (output_dir / name).unlink(missing_ok=True)

    rows: list[dict[str, Any]] = []
    if runs_path.exists():
        rows = [dict(row) for row in read_csv(runs_path)]

    completed = {case_key(row) for row in rows}
    variants = (
        list(args.variants)
        if args.variants
        else variants_for_study(args.study)
    )
    known_variants = set(variants_for_study("all"))
    unknown = [variant for variant in variants if variant not in known_variants]
    if unknown:
        raise ValueError(f"unknown --variants entries: {unknown}")

    cases = [
        (variant, padding, repetition)
        for repetition in range(1, args.repetitions + 1)
        for padding in args.task_spec_padding
        for variant in variants
    ]

    if not args.fixed_order:
        rng = random.Random(args.seed)
        rng.shuffle(cases)

    pending = [
        case
        for case in cases
        if (case[0], case[1].size_bytes, case[2]) not in completed
    ]

    print(
        f"Study={args.study} cases={len(cases)}, "
        f"already complete={len(cases)-len(pending)}, remaining={len(pending)}"
    )

    failures: list[str] = []
    for index, (variant, padding, repetition) in enumerate(pending, 1):
        label = f"rep={repetition} variant={variant} TaskSpec={padding.name}"
        print(f"[{index}/{len(pending)}] {label}", flush=True)

        output_json = (
            output_dir
            / f".single_{variant}_{padding.size_bytes}_rep{repetition}.json"
        )
        output_json.unlink(missing_ok=True)

        proc = subprocess.run(
            child_command(
                args,
                variant=variant,
                padding=padding,
                repetition=repetition,
                output_json=output_json,
            ),
            env=clean_variant_environment(os.environ, variant),
        )

        if proc.returncode != 0 or not output_json.exists():
            msg = f"FAILED: {label} (exit={proc.returncode})"
            print(msg, file=sys.stderr)
            failures.append(msg)
            write_outputs(output_dir, rows)
            if not args.keep_going:
                raise SystemExit(proc.returncode or 1)
            continue

        row = json.loads(output_json.read_text())
        output_json.unlink(missing_ok=True)
        rows.append(row)
        write_outputs(output_dir, rows)

        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms",
            flush=True,
        )

    write_outputs(output_dir, rows)

    if failures:
        print("Completed with failed cases:", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)


def run_single(args: argparse.Namespace) -> None:
    required = [
        args.single_variant,
        args.single_padding_name,
        args.single_padding_bytes,
        args.single_repetition,
        args.single_output_json,
    ]
    if any(x is None for x in required):
        raise ValueError("_single-run missing internal case arguments")

    # Defensive check: child environment must encode the exact variant.
    expected = variant_settings(args.single_variant)
    for env_name, enabled in expected.items():
        actual = os.environ.get(env_name)
        if actual != str(enabled):
            raise RuntimeError(
                f"variant env mismatch for {env_name}: expected {enabled}, got {actual!r}"
            )
    if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
        raise RuntimeError("performance child must run with RAY_RECOVERY_PROFILING=0")

    row = run_single_case(args)
    Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "command",
        choices=["run", "_single-run"],
        nargs="?",
        default="run",
    )
    p.add_argument(
        "--study",
        choices=["compare", "ablation", "all"],
        default="compare",
    )
    p.add_argument(
        "--variants",
        nargs="+",
        default=None,
        help=(
            "Optional explicit variant subset. Examples: "
            "all_optimized all_minus_serialize_once. "
            "When supplied, this overrides --study's variant list."
        ),
    )
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/23_witness_baseline_optimization",
    )
    p.add_argument("--borrowers", type=int, default=1, choices=[1, 2, 3, 4])
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
    p.add_argument("--keep-going", action="store_true")

    # Internal subprocess fields.
    p.add_argument("--single-variant")
    p.add_argument("--single-padding-name")
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-output-json")
    return p


def validate_args(args: argparse.Namespace) -> None:
    if args.payload_bytes < 8:
        raise ValueError("--payload-bytes must be >= 8")
    if args.repetitions <= 0:
        raise ValueError("--repetitions must be positive")
    if args.warmup_seconds < 0 or args.duration_seconds <= 0:
        raise ValueError("invalid warmup/duration")
    if args.inflight <= 0:
        raise ValueError("--inflight must be positive")
    if args.inline_chunk_bytes <= 0:
        raise ValueError("--inline-chunk-bytes must be positive")


def main() -> None:
    args = build_parser().parse_args()
    validate_args(args)
    if args.command == "_single-run":
        run_single(args)
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
