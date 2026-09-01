#!/usr/bin/env python3
"""Benchmark 69: fast K=1 performance screen for recovery optimization iterations.

This is intentionally a screening benchmark, not the paper-quality final run.
It keeps Benchmark 59's K=1 workload and compares only:

  disabled
  fixed_r
  succession_k1

Defaults are short so a C++ optimization can be accepted/rejected quickly:
  * 2 repetitions (6 fresh clusters total)
  * 1 s warmup
  * 4 s timed window
  * profiling OFF
  * R=2, W=2, two borrowers, K=1

Promising changes should still be validated later with Benchmark 59.

Quick run:
  python gossip_benchmarks/69_recovery_succession_k1_quick_perf.py --overwrite
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
from typing import Any

HERE = Path(__file__).resolve().parent
BENCH59_PATH = HERE / "59_recovery_frontier_fixed_vs_succession_performance.py"


def _load_benchmark59():
    spec = importlib.util.spec_from_file_location("recovery_k1_quick_b59", BENCH59_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH59_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b59 = _load_benchmark59()
b58 = b59.b58

VARIANTS = ["disabled", "fixed_r", "succession_k1"]


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
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def describe(values: list[float]) -> tuple[float, float, float]:
    vals = [float(v) for v in values if math.isfinite(float(v))]
    if not vals:
        return math.nan, math.nan, math.nan
    mean = statistics.fmean(vals)
    if len(vals) == 1:
        return mean, math.nan, math.nan
    stdev = statistics.stdev(vals)
    cv = 100.0 * stdev / mean if mean else math.nan
    # Screening benchmark: report range-scale variability rather than claiming
    # a strong CI from only two repetitions.
    return mean, stdev, cv


def case_key(row: dict[str, Any]) -> tuple[str, int]:
    return str(row["variant"]), int(row["repetition"])


def order_for(rep: int, seed: int) -> list[str]:
    base = list(VARIANTS)
    random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def child_cmd(args: argparse.Namespace, variant: str, rep: int, temp: Path) -> list[str]:
    return [
        sys.executable,
        str(BENCH59_PATH),
        "_single-perf",
        "--single-variant", variant,
        "--single-padding-name", "1KiB",
        "--single-padding-bytes", "1024",
        "--single-repetition", str(rep),
        "--single-output-json", str(temp),
        "--holders", "2",
        "--witness-count", "2",
        "--payload-bytes", str(args.payload_bytes),
        "--inline-chunk-bytes", str(args.inline_chunk_bytes),
        "--burst-size", str(args.burst_size),
        "--inflight-tasks", str(args.inflight_tasks),
        "--warmup-seconds", str(args.warmup_seconds),
        "--settle-seconds", str(args.settle_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--cpus-per-node", str(args.cpus_per_node),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
    ]


def paired_speedups(rows: list[dict[str, Any]]) -> dict[str, list[float]]:
    out = {
        "fixed_overhead": [],
        "succ_overhead": [],
        "succ_vs_fixed": [],
    }
    reps = sorted({int(r["repetition"]) for r in rows})
    for rep in reps:
        by = {
            str(r["variant"]): r
            for r in rows
            if int(r["repetition"]) == rep
        }
        if not all(v in by for v in VARIANTS):
            continue
        d = float(by["disabled"]["throughput_rps"])
        f = float(by["fixed_r"]["throughput_rps"])
        s = float(by["succession_k1"]["throughput_rps"])
        if d:
            out["fixed_overhead"].append(100.0 * (d - f) / d)
            out["succ_overhead"].append(100.0 * (d - s) / d)
        if f:
            out["succ_vs_fixed"].append(100.0 * (s - f) / f)
    return out


def run(args: argparse.Namespace) -> None:
    if args.repetitions < 1:
        raise ValueError("--repetitions must be >= 1")
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_path = out / "k1_quick_runs.csv"
    if args.overwrite:
        runs_path.unlink(missing_ok=True)

    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    completed = {case_key(r) for r in rows}

    cases: list[tuple[int, str, int]] = []
    for rep in range(1, args.repetitions + 1):
        for pos, variant in enumerate(order_for(rep, args.seed), 1):
            cases.append((rep, variant, pos))

    pending = [c for c in cases if (c[1], c[0]) not in completed]
    print(
        "Quick K=1 screen: "
        f"reps={args.repetitions} warmup={args.warmup_seconds:.1f}s "
        f"timed={args.duration_seconds:.1f}s cases={len(cases)} remaining={len(pending)}"
    )
    print("  R=2 W=2 borrowers=2 K=1 profiling=OFF; fresh cluster per case")

    for i, (rep, variant, pos) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} "
            f"position={pos}/3 variant={variant}",
            flush=True,
        )
        temp = out / f".quick_{variant}_{rep}.json"
        temp.unlink(missing_ok=True)
        proc = subprocess.run(
            child_cmd(args, variant, rep, temp),
            env=b58.child_env(profiling=False),
        )
        if proc.returncode != 0 or not temp.exists():
            write_csv(runs_path, rows)
            raise SystemExit(proc.returncode or 1)

        row = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        row["variant"] = variant
        row["repetition"] = rep
        row["block_position"] = pos
        rows.append(row)
        write_csv(runs_path, rows)
        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms"
        )

    print("\nFinal quick K=1 screen:")
    for variant in VARIANTS:
        vals = [
            float(r["throughput_rps"])
            for r in rows
            if str(r["variant"]) == variant
        ]
        mean, stdev, cv = describe(vals)
        if math.isnan(stdev):
            print(f"  {variant:16s} thr={mean:8.1f} rps")
        else:
            print(
                f"  {variant:16s} thr={mean:8.1f} rps "
                f"sd={stdev:6.1f} CV={cv:4.1f}%"
            )

    paired = paired_speedups(rows)
    if paired["succ_vs_fixed"]:
        f_over = statistics.fmean(paired["fixed_overhead"])
        s_over = statistics.fmean(paired["succ_overhead"])
        gap = statistics.fmean(paired["succ_vs_fixed"])
        print("\nPaired decision signal:")
        print(f"  Fixed-R overhead vs disabled = {f_over:7.2f}%")
        print(f"  Succession overhead          = {s_over:7.2f}%")
        print(f"  Succession vs Fixed-R        = {gap:+7.2f}%")
        print("  Use this only to screen patches; validate winners with Benchmark 59.")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/69_recovery_succession_k1_quick_perf",
    )
    p.add_argument("--repetitions", type=int, default=2)
    p.add_argument("--warmup-seconds", type=float, default=1.0)
    p.add_argument("--settle-seconds", type=float, default=0.25)
    p.add_argument("--duration-seconds", type=float, default=4.0)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite", action="store_true")
    return p


if __name__ == "__main__":
    run(parser().parse_args())
