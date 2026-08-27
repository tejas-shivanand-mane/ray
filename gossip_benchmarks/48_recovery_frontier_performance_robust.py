#!/usr/bin/env python3
"""Benchmark 48: statistically robust wrapper around Benchmark 30.

This benchmark deliberately reuses Benchmark 30's exact single-run workload and
configuration. It changes only the experimental design and statistical analysis:

* seven repetitions by default;
* one complete block contains every Benchmark-30 variant;
* a seeded cyclic Latin-square order balances each variant across run positions;
* every case still starts a fresh Ray cluster through Benchmark 30;
* comparisons are paired within the same repetition/block;
* no automatic outlier removal;
* mean, median, sample standard deviation, CV, and 95% CI are reported;
* overhead vs Disabled and Fixed-R loss recovered are computed per block first,
  then summarized, rather than taking a ratio of independent grand means.

Benchmark 30 remains the workload authority. If its single-run implementation is
changed, this benchmark automatically uses the new implementation.

Outputs
-------
  frontier_perf_robust_runs.csv
  frontier_perf_robust_summary.csv
  frontier_perf_robust_paired.csv

Recommended paper run (also the defaults):
  python gossip_benchmarks/48_recovery_frontier_performance_robust.py --overwrite
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import os
import random
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable

HERE = Path(__file__).resolve().parent
BENCH30_PATH = HERE / "30_recovery_frontier_performance.py"


def _load_benchmark30():
    spec = importlib.util.spec_from_file_location("recovery_frontier_bench30", BENCH30_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH30_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b30 = _load_benchmark30()
VARIANTS = list(b30.VARIANTS)
FRONTIER_K = dict(b30.FRONTIER_K)


def mean_ci95(values: Iterable[float]) -> tuple[float, float]:
    vals = list(values)
    if not vals:
        return math.nan, math.nan
    mean = statistics.fmean(vals)
    if len(vals) < 2:
        return mean, math.nan

    # Student-t 97.5th percentiles for df=1..30. For larger samples use 1.96.
    # The default n=7 therefore uses t_0.975,6=2.447 rather than a normal CI.
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
        11: 2.201,
        12: 2.179,
        13: 2.160,
        14: 2.145,
        15: 2.131,
        16: 2.120,
        17: 2.110,
        18: 2.101,
        19: 2.093,
        20: 2.086,
        21: 2.080,
        22: 2.074,
        23: 2.069,
        24: 2.064,
        25: 2.060,
        26: 2.056,
        27: 2.052,
        28: 2.048,
        29: 2.045,
        30: 2.042,
    }
    df = len(vals) - 1
    critical = t975.get(df, 1.96)
    sem = statistics.stdev(vals) / math.sqrt(len(vals))
    return mean, critical * sem


def describe(values: Iterable[float]) -> dict[str, float]:
    vals = list(values)
    if not vals:
        return {
            "mean": math.nan,
            "median": math.nan,
            "stdev": math.nan,
            "cv_pct": math.nan,
            "ci95": math.nan,
            "min": math.nan,
            "max": math.nan,
        }
    mean, ci95 = mean_ci95(vals)
    stdev = statistics.stdev(vals) if len(vals) >= 2 else math.nan
    cv_pct = 100.0 * stdev / mean if mean and math.isfinite(stdev) else math.nan
    return {
        "mean": mean,
        "median": statistics.median(vals),
        "stdev": stdev,
        "cv_pct": cv_pct,
        "ci95": ci95,
        "min": min(vals),
        "max": max(vals),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.unlink(missing_ok=True)
        return
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def block_order(rep: int, seed: int) -> list[str]:
    """Balanced cyclic order: every variant occupies every position once in 7 reps."""
    base = list(VARIANTS)
    random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def case_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return (
        str(row["variant"]),
        int(row["task_spec_padding_bytes"]),
        int(row["repetition"]),
    )


def child_command(
    args: argparse.Namespace,
    variant: str,
    padding: Any,
    repetition: int,
    output_json: Path,
) -> list[str]:
    # Reuse Benchmark 30's exact single-run child. These argument names mirror
    # Benchmark 30 on purpose so workload semantics cannot drift here.
    return [
        sys.executable,
        str(BENCH30_PATH),
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


def variant_env(base: dict[str, str]) -> dict[str, str]:
    # Keep this identical to Benchmark 30's timed environment.
    return b30.variant_env(base)


def summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    paddings = sorted(
        {
            (int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"]))
            for r in rows
        }
    )
    metrics = ["throughput_rps", "latency_mean_ms", "latency_p95_ms", "latency_p99_ms"]

    for padding_bytes, padding_name in paddings:
        for variant in VARIANTS:
            matched = [
                r
                for r in rows
                if str(r["variant"]) == variant
                and int(r["task_spec_padding_bytes"]) == padding_bytes
            ]
            if not matched:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": padding_name,
                "task_spec_padding_bytes": padding_bytes,
                "variant": variant,
                "frontier_k": FRONTIER_K.get(variant, 0),
                "repetitions": len(matched),
            }
            for metric in metrics:
                desc = describe(float(r[metric]) for r in matched)
                for stat, value in desc.items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def paired_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute normalized comparisons inside each repetition before aggregation."""
    out: list[dict[str, Any]] = []
    paddings = sorted(
        {
            (int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"]))
            for r in rows
        }
    )

    for padding_bytes, padding_name in paddings:
        reps = sorted(
            {
                int(r["repetition"])
                for r in rows
                if int(r["task_spec_padding_bytes"]) == padding_bytes
            }
        )
        per_variant: dict[str, dict[str, list[float]]] = {
            v: {
                "normalized_throughput_pct": [],
                "throughput_overhead_pct_vs_disabled": [],
                "fixed_r_lost_throughput_recovered_pct": [],
                "p95_inflation_pct_vs_disabled": [],
            }
            for v in VARIANTS
        }
        paired_rep_count = {v: 0 for v in VARIANTS}

        for rep in reps:
            by_variant = {
                str(r["variant"]): r
                for r in rows
                if int(r["task_spec_padding_bytes"]) == padding_bytes
                and int(r["repetition"]) == rep
            }
            disabled = by_variant.get("disabled")
            fixed = by_variant.get("fixed_r")
            if disabled is None:
                continue
            dthr = float(disabled["throughput_rps"])
            dp95 = float(disabled["latency_p95_ms"])
            fixed_thr = float(fixed["throughput_rps"]) if fixed is not None else math.nan
            baseline_loss = dthr - fixed_thr if fixed is not None else math.nan

            for variant, row in by_variant.items():
                thr = float(row["throughput_rps"])
                p95 = float(row["latency_p95_ms"])
                per_variant[variant]["normalized_throughput_pct"].append(
                    100.0 * thr / dthr if dthr else math.nan
                )
                per_variant[variant]["throughput_overhead_pct_vs_disabled"].append(
                    100.0 * (dthr - thr) / dthr if dthr else math.nan
                )
                per_variant[variant]["p95_inflation_pct_vs_disabled"].append(
                    100.0 * (p95 - dp95) / dp95 if dp95 else math.nan
                )
                if variant == "disabled":
                    recovered = 100.0
                elif variant == "fixed_r":
                    recovered = 0.0
                elif variant.startswith("frontier_") and baseline_loss > 0:
                    recovered = 100.0 * (thr - fixed_thr) / baseline_loss
                else:
                    recovered = math.nan
                if math.isfinite(recovered):
                    per_variant[variant]["fixed_r_lost_throughput_recovered_pct"].append(
                        recovered
                    )
                paired_rep_count[variant] += 1

        for variant in VARIANTS:
            if paired_rep_count[variant] == 0:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": padding_name,
                "task_spec_padding_bytes": padding_bytes,
                "variant": variant,
                "frontier_k": FRONTIER_K.get(variant, 0),
                "paired_repetitions": paired_rep_count[variant],
            }
            for metric, vals in per_variant[variant].items():
                finite = [v for v in vals if math.isfinite(v)]
                desc = describe(finite)
                for stat, value in desc.items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def write_outputs(out: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(out / "frontier_perf_robust_runs.csv", rows)
    write_csv(out / "frontier_perf_robust_summary.csv", summary_rows(rows))
    write_csv(out / "frontier_perf_robust_paired.csv", paired_rows(rows))


def print_checkpoint(rows: list[dict[str, Any]], padding_bytes: int) -> None:
    paired = {
        r["variant"]: r
        for r in paired_rows(rows)
        if int(r["task_spec_padding_bytes"]) == padding_bytes
    }
    if not paired:
        return
    print("  paired estimates so far:")
    for variant in VARIANTS:
        row = paired.get(variant)
        if row is None:
            continue
        overhead = float(row["throughput_overhead_pct_vs_disabled_mean"])
        ci = float(row["throughput_overhead_pct_vs_disabled_ci95"])
        n = int(row["paired_repetitions"])
        print(f"    {variant:12s} overhead={overhead:6.2f}% +/- {ci:5.2f} pp  n={n}")


def run(args: argparse.Namespace) -> None:
    if args.repetitions < 2:
        raise ValueError("repetitions must be >= 2")
    if args.burst_size % max(FRONTIER_K.values()) != 0:
        raise ValueError("burst_size must be divisible by 32")
    if args.inflight_tasks % args.burst_size != 0:
        raise ValueError("inflight_tasks must be divisible by burst_size")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_path = out / "frontier_perf_robust_runs.csv"
    if args.overwrite:
        for name in [
            "frontier_perf_robust_runs.csv",
            "frontier_perf_robust_summary.csv",
            "frontier_perf_robust_paired.csv",
        ]:
            (out / name).unlink(missing_ok=True)

    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    completed = {case_key(r) for r in rows}

    cases: list[tuple[int, Any, str, int]] = []
    ordinal = 0
    for rep in range(1, args.repetitions + 1):
        order = block_order(rep, args.seed)
        for padding in args.task_spec_padding:
            for position, variant in enumerate(order, 1):
                ordinal += 1
                cases.append((rep, padding, variant, position))

    pending = [
        c
        for c in cases
        if (c[2], c[1].size_bytes, c[0]) not in completed
    ]

    print(
        "Recovery Frontier robust performance: "
        f"R={args.holders}, burst={args.burst_size}, reps={args.repetitions}, "
        f"warmup={args.warmup_seconds:.1f}s, timed={args.duration_seconds:.1f}s, "
        f"cases={len(cases)}, remaining={len(pending)}"
    )
    print("  design=balanced cyclic blocks; comparisons=paired within repetition")
    print("  outliers=reported, never automatically removed")

    for i, (rep, padding, variant, position) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} "
            f"position={position}/{len(VARIANTS)} variant={variant} "
            f"TaskSpec={padding.name}",
            flush=True,
        )
        temp = out / f".robust_{variant}_{padding.size_bytes}_{rep}.json"
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
        row["block_position"] = position
        row["block_seed"] = args.seed
        rows.append(row)
        write_outputs(out, rows)

        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms",
            flush=True,
        )
        # Print a checkpoint only after a complete block for this padding.
        have = {
            str(r["variant"])
            for r in rows
            if int(r["task_spec_padding_bytes"]) == padding.size_bytes
            and int(r["repetition"]) == rep
        }
        if have == set(VARIANTS):
            print_checkpoint(rows, padding.size_bytes)

    print("\nFinal robust comparison (paired across repetitions):")
    summary = {
        r["variant"]: r
        for r in summary_rows(rows)
        if int(r["task_spec_padding_bytes"]) == args.task_spec_padding[0].size_bytes
    }
    paired = {
        r["variant"]: r
        for r in paired_rows(rows)
        if int(r["task_spec_padding_bytes"]) == args.task_spec_padding[0].size_bytes
    }
    for variant in VARIANTS:
        s = summary.get(variant)
        p = paired.get(variant)
        if s is None or p is None:
            continue
        thr_mean = float(s["throughput_rps_mean"])
        thr_ci = float(s["throughput_rps_ci95"])
        thr_median = float(s["throughput_rps_median"])
        cv = float(s["throughput_rps_cv_pct"])
        overhead = float(p["throughput_overhead_pct_vs_disabled_mean"])
        overhead_ci = float(p["throughput_overhead_pct_vs_disabled_ci95"])
        overhead_med = float(p["throughput_overhead_pct_vs_disabled_median"])
        recovered = float(p["fixed_r_lost_throughput_recovered_pct_mean"])
        recovered_ci = float(p["fixed_r_lost_throughput_recovered_pct_ci95"])
        print(
            f"  {variant:12s} "
            f"thr={thr_mean:8.1f} +/- {thr_ci:5.1f} rps "
            f"(median={thr_median:8.1f}, CV={cv:4.1f}%)  "
            f"paired overhead={overhead:6.2f} +/- {overhead_ci:5.2f} pp "
            f"(median={overhead_med:6.2f}%)  "
            f"recovered={recovered:7.2f} +/- {recovered_ci:5.2f}%"
        )


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/48_recovery_frontier_performance_robust",
    )
    p.add_argument(
        "--task-spec-padding",
        type=b30.parse_spec_padding,
        nargs="+",
        default=[b30.SpecPadding("1KiB", 1024)],
    )
    p.add_argument("--holders", type=int, default=b30.DEFAULT_R)
    p.add_argument("--witness-count", type=int, default=b30.DEFAULT_R)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)

    # Paper-quality defaults: long enough to suppress transient noise without
    # making the full 49-case run unnecessarily huge.
    p.add_argument("--repetitions", type=int, default=7)
    p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0)
    p.add_argument("--duration-seconds", type=float, default=20.0)

    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite", action="store_true")
    return p


def main() -> None:
    args = parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
