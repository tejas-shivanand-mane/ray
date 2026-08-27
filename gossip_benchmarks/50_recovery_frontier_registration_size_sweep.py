#!/usr/bin/env python3
"""Benchmark 50: determine whether Frontier owner-registration cost is TaskSpec-copy bound.

Benchmark 49 showed that K=4..32 owner-registration overhead is essentially flat,
which identifies a per-task Frontier cost. The remaining ambiguity is whether the
extra Frontier cost comes primarily from the canonical replay TaskSpec CopyFrom
or from size-independent planner/mutex/bookkeeping work.

This diagnostic reuses Benchmark 49's exact owner-local submission measurement,
but compares only:

  fixed_r
  frontier_k32

while sweeping TaskSpec padding size. Because both variants submit the same
TaskSpec, normal Python/Cython serialization and TaskManager costs scale in both
arms. A size-dependent increase in the paired

  frontier_k32 - fixed_r

delta therefore isolates extra Frontier work that scales with replay-recipe size.
The current implementation's canonical Frontier TaskSpec CopyFrom is the main
such operation; group/map/lock bookkeeping should be approximately size-flat.

This is diagnostic only. Benchmark 48 remains the end-to-end performance
authority and Benchmark 30's workload semantics are not changed.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

HERE = Path(__file__).resolve().parent
BENCH49_PATH = HERE / "49_recovery_frontier_owner_registration_profile.py"
VARIANTS = ("fixed_r", "frontier_k32")


def _load_benchmark49():
    spec = importlib.util.spec_from_file_location(
        "recovery_frontier_bench49_for_50", BENCH49_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH49_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b49 = _load_benchmark49()


def mean_ci95(values: Iterable[float]) -> tuple[float, float]:
    vals = list(values)
    if not vals:
        return math.nan, math.nan
    mean = statistics.fmean(vals)
    if len(vals) < 2:
        return mean, math.nan
    t975 = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
    }
    critical = t975.get(len(vals) - 1, 1.96)
    return mean, critical * statistics.stdev(vals) / math.sqrt(len(vals))


def parse_sizes(text: str) -> list[int]:
    out: list[int] = []
    for raw in text.split(","):
        raw = raw.strip()
        if not raw:
            continue
        value = int(raw)
        if value < 0:
            raise argparse.ArgumentTypeError("padding sizes must be >= 0")
        out.append(value)
    if len(out) < 2:
        raise argparse.ArgumentTypeError("provide at least two padding sizes")
    if len(set(out)) != len(out):
        raise argparse.ArgumentTypeError("padding sizes must be unique")
    return sorted(out)


def order_for(rep: int, size_index: int) -> tuple[str, str]:
    # Pair variants tightly and alternate which one runs first so slow machine
    # drift does not systematically favor either arm.
    if (rep + size_index) % 2:
        return VARIANTS
    return tuple(reversed(VARIANTS))


def run_one(
    args: argparse.Namespace,
    *,
    variant: str,
    repetition: int,
    padding_bytes: int,
    temp_path: Path,
) -> dict[str, Any]:
    temp_path.unlink(missing_ok=True)
    cmd = [
        sys.executable,
        str(BENCH49_PATH),
        "_single-run",
        "--single-variant",
        variant,
        "--single-repetition",
        str(repetition),
        "--single-output-json",
        str(temp_path),
        "--holders",
        str(args.holders),
        "--witness-count",
        str(args.witness_count),
        "--payload-bytes",
        str(args.payload_bytes),
        "--task-spec-padding-bytes",
        str(padding_bytes),
        "--inline-chunk-bytes",
        str(args.inline_chunk_bytes),
        "--burst-size",
        str(args.burst_size),
        "--samples",
        str(args.samples),
        "--producer-delay-seconds",
        str(args.producer_delay_seconds),
        "--cpus-per-node",
        str(args.cpus_per_node),
        "--settle-seconds",
        str(args.settle_seconds),
        "--cluster-timeout-seconds",
        str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds",
        str(args.wait_timeout_seconds),
    ]
    env = b49.variant_env(dict(os.environ))
    env["RAY_RECOVERY_PROFILING"] = "0"
    proc = subprocess.run(cmd, env=env)
    if proc.returncode != 0 or not temp_path.exists():
        raise SystemExit(proc.returncode or 1)
    row = json.loads(temp_path.read_text())
    temp_path.unlink(missing_ok=True)
    row["task_spec_padding_bytes"] = padding_bytes
    return row


def summarize(rows: list[dict[str, Any]], sizes: list[int]) -> list[dict[str, float]]:
    summary: list[dict[str, float]] = []
    for padding_bytes in sizes:
        paired_deltas: list[float] = []
        fixed_values: list[float] = []
        frontier_values: list[float] = []

        reps = sorted(
            {
                int(row["repetition"])
                for row in rows
                if int(row["task_spec_padding_bytes"]) == padding_bytes
            }
        )
        for rep in reps:
            by_variant = {
                str(row["variant"]): row
                for row in rows
                if int(row["task_spec_padding_bytes"]) == padding_bytes
                and int(row["repetition"]) == rep
            }
            fixed = by_variant.get("fixed_r")
            frontier = by_variant.get("frontier_k32")
            if fixed is None or frontier is None:
                continue
            f = float(fixed["mean_us_per_task"])
            k = float(frontier["mean_us_per_task"])
            fixed_values.append(f)
            frontier_values.append(k)
            paired_deltas.append(k - f)

        if not paired_deltas:
            continue

        fixed_mean, fixed_ci = mean_ci95(fixed_values)
        frontier_mean, frontier_ci = mean_ci95(frontier_values)
        delta_mean, delta_ci = mean_ci95(paired_deltas)
        summary.append(
            {
                "padding_bytes": float(padding_bytes),
                "paired_repetitions": float(len(paired_deltas)),
                "fixed_mean_us": fixed_mean,
                "fixed_ci95_us": fixed_ci,
                "frontier_mean_us": frontier_mean,
                "frontier_ci95_us": frontier_ci,
                "delta_mean_us": delta_mean,
                "delta_ci95_us": delta_ci,
            }
        )
    return summary


def linear_slope_us_per_kib(summary: list[dict[str, float]]) -> float:
    if len(summary) < 2:
        return math.nan
    xs = [row["padding_bytes"] / 1024.0 for row in summary]
    ys = [row["delta_mean_us"] for row in summary]
    xbar = statistics.fmean(xs)
    ybar = statistics.fmean(ys)
    denom = sum((x - xbar) ** 2 for x in xs)
    if denom == 0:
        return math.nan
    return sum((x - xbar) * (y - ybar) for x, y in zip(xs, ys)) / denom


def run(args: argparse.Namespace) -> None:
    sizes = parse_sizes(args.padding_sizes)
    if args.repetitions < 2:
        raise ValueError("repetitions must be >= 2")
    if args.burst_size % 32 != 0:
        raise ValueError("burst-size must be divisible by 32")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    results_path = out / "registration_size_sweep_runs.json"
    if args.overwrite:
        results_path.unlink(missing_ok=True)

    rows: list[dict[str, Any]] = []
    if results_path.exists():
        rows = json.loads(results_path.read_text())

    completed = {
        (
            int(row["task_spec_padding_bytes"]),
            str(row["variant"]),
            int(row["repetition"]),
        )
        for row in rows
    }

    cases: list[tuple[int, int, str, int]] = []
    for size_index, padding_bytes in enumerate(sizes):
        for rep in range(1, args.repetitions + 1):
            for position, variant in enumerate(order_for(rep, size_index), 1):
                cases.append((padding_bytes, rep, variant, position))

    pending = [
        case
        for case in cases
        if (case[0], case[2], case[1]) not in completed
    ]

    print(
        "Recovery Frontier registration TaskSpec-size sweep: "
        f"sizes={sizes}, reps={args.repetitions}, samples={args.samples}, "
        f"remaining={len(pending)}"
    )
    print("  paired comparison is frontier_k32 - fixed_r")
    print("  no downstream ObjectRef export; no Frontier publication is timed")

    for index, (padding_bytes, rep, variant, position) in enumerate(pending, 1):
        print(
            f"[{index}/{len(pending)}] padding={padding_bytes:6d} "
            f"rep={rep} pos={position} variant={variant}",
            flush=True,
        )
        temp = out / f".single_{padding_bytes}_{variant}_{rep}.json"
        row = run_one(
            args,
            variant=variant,
            repetition=rep,
            padding_bytes=padding_bytes,
            temp_path=temp,
        )
        rows.append(row)
        results_path.write_text(json.dumps(rows, indent=2, allow_nan=True))
        print(
            f"  submit={float(row['mean_us_per_task']):8.2f} us/task "
            f"within-run CV={float(row['cv_pct']):5.1f}%",
            flush=True,
        )

    final = summarize(rows, sizes)
    print("\nFinal paired TaskSpec-size comparison:")
    print(
        "  padding_B    fixed_r us/task      frontier_k32 us/task     "
        "extra_frontier us/task"
    )
    for row in final:
        print(
            f"  {int(row['padding_bytes']):9d}  "
            f"{row['fixed_mean_us']:8.2f} +/- {row['fixed_ci95_us']:6.2f}     "
            f"{row['frontier_mean_us']:8.2f} +/- {row['frontier_ci95_us']:6.2f}     "
            f"{row['delta_mean_us']:8.2f} +/- {row['delta_ci95_us']:6.2f}"
        )

    slope = linear_slope_us_per_kib(final)
    if math.isfinite(slope):
        print(f"\nLeast-squares delta slope: {slope:.4f} us per KiB of TaskSpec padding")

    if final:
        base = final[0]["delta_mean_us"]
        last = final[-1]["delta_mean_us"]
        growth = last - base
        print(
            f"Delta growth from {int(final[0]['padding_bytes'])} B to "
            f"{int(final[-1]['padding_bytes'])} B: {growth:.2f} us/task"
        )

    print("\nDecision guide:")
    print("  delta grows materially with padding -> canonical Frontier TaskSpec copy is a major target")
    print("  delta stays approximately flat      -> manager lock / planner maps / bookkeeping dominate")
    print("  nonzero intercept + positive slope  -> mixed fixed bookkeeping + TaskSpec copy cost")
    print("  Benchmark 48 remains the final performance authority after any optimization")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/50_recovery_frontier_registration_size_sweep",
    )
    p.add_argument(
        "--padding-sizes",
        default="0,1024,4096,16384,65536",
        help="comma-separated TaskSpec padding sizes in bytes",
    )
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--samples", type=int, default=10)
    p.add_argument("--repetitions", type=int, default=5)
    p.add_argument("--producer-delay-seconds", type=float, default=0.02)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--settle-seconds", type=float, default=0.05)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=30.0)
    p.add_argument("--overwrite", action="store_true")
    return p


def main() -> None:
    run(parser().parse_args())


if __name__ == "__main__":
    main()
