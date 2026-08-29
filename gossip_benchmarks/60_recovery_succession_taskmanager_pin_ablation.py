#!/usr/bin/env python3
"""Benchmark 60: Recovery Succession owner-lineage hot-path ablation.

This benchmark isolates the existing Patch-4N TaskManager-pin optimization under
Benchmark 58's exact two-borrower workload. Benchmark 58 intentionally forces
RAY_RECOVERY_TASKMANAGER_PIN=0, so its published results measure the current
owner-side retained-TaskSpec path.

Cases:
  disabled
  current_k1, current_k4, current_k16, current_k32
  pin_k1,     pin_k4,     pin_k16,     pin_k32

"current" uses the Benchmark-58 production configuration: the Recovery
Succession manager keeps its own dormant TaskSpec copy until lazy activation.
"pin" sets enable_recovery_succession_task_manager_pin=true: TaskManager keeps
the existing TaskEntry instead, while the recovery manager retains only lifetime
bookkeeping. Recovery topology/admission, Frontier K, workload, and R=2 are
otherwise identical.

The default 9 repetitions give exact cyclic position balance across the 9 cases.

Outputs:
  succession_taskmanager_pin_runs.csv
  succession_taskmanager_pin_summary.csv
  succession_taskmanager_pin_paired.csv

Recommended quick validation:
  python gossip_benchmarks/60_recovery_succession_taskmanager_pin_ablation.py \
      --overwrite --repetitions 2 --warmup-seconds 2 --duration-seconds 5

Recommended full run:
  python gossip_benchmarks/60_recovery_succession_taskmanager_pin_ablation.py --overwrite
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

HERE = Path(__file__).resolve().parent
BENCH58_PATH = HERE / "58_recovery_frontier_succession_performance.py"


def _load_benchmark58():
    spec = importlib.util.spec_from_file_location("recovery_frontier_bench58_pin_ablation", BENCH58_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH58_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b58 = _load_benchmark58()

K_VALUES = [1, 4, 16, 32]
CASES = [
    "disabled",
    *[f"current_k{k}" for k in K_VALUES],
    *[f"pin_k{k}" for k in K_VALUES],
]

_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
    7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179,
    13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101,
    19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064,
    25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


@dataclass(frozen=True)
class Padding:
    name: str
    size_bytes: int


def parse_padding(text: str) -> Padding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("TaskSpec padding must be NAME:BYTES") from exc
    if not name or size < 0:
        raise argparse.ArgumentTypeError("invalid TaskSpec padding")
    return Padding(name, size)


def case_k(case: str) -> int:
    if case == "disabled":
        return 0
    return int(case.rsplit("k", 1)[1])


def case_pin(case: str) -> bool:
    return case.startswith("pin_")


def inner_variant(case: str) -> str:
    if case == "disabled":
        return "disabled"
    return f"succession_k{case_k(case)}"


def describe(values: Iterable[float]) -> dict[str, float]:
    vals = [float(v) for v in values if math.isfinite(float(v))]
    if not vals:
        return {"mean": math.nan, "median": math.nan, "stdev": math.nan,
                "cv_pct": math.nan, "ci95": math.nan, "min": math.nan, "max": math.nan}
    mean = statistics.fmean(vals)
    if len(vals) > 1:
        stdev = statistics.stdev(vals)
        ci95 = _T95.get(len(vals) - 1, 1.96) * stdev / math.sqrt(len(vals))
        cv_pct = 100.0 * stdev / mean if mean else math.nan
    else:
        stdev = cv_pct = ci95 = math.nan
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


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def block_order(rep: int, seed: int) -> list[str]:
    base = list(CASES)
    random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def perf_cmd(args: argparse.Namespace, case: str, padding: Padding, rep: int, temp: Path) -> list[str]:
    return [
        sys.executable,
        str(BENCH58_PATH),
        "_single-perf",
        "--single-variant", inner_variant(case),
        "--single-padding-name", padding.name,
        "--single-padding-bytes", str(padding.size_bytes),
        "--single-repetition", str(rep),
        "--single-output-json", str(temp),
        "--holders", str(args.holders),
        "--witness-count", str(args.witness_count),
        "--payload-bytes", str(args.payload_bytes),
        "--inline-chunk-bytes", str(args.inline_chunk_bytes),
        "--burst-size", str(args.burst_size),
        "--inflight-tasks", str(args.inflight_tasks),
        "--warmup-seconds", str(args.warmup_seconds),
        "--settle-seconds", str(args.settle_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
    ]


def child_env(pin: bool) -> dict[str, str]:
    env = dict(os.environ)
    env["RAY_RECOVERY_PROFILING"] = "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_TASKMANAGER_PIN"] = "1" if pin else "0"
    env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
    return env


def case_key(row: dict[str, Any]) -> tuple[str, int, int]:
    return str(row["case"]), int(row["task_spec_padding_bytes"]), int(row["repetition"])


def summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metrics = ["throughput_rps", "latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"]
    paddings = sorted({(int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"])) for r in rows})
    for pbytes, pname in paddings:
        for case in CASES:
            matched = [r for r in rows if str(r["case"]) == case and int(r["task_spec_padding_bytes"]) == pbytes]
            if not matched:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": pname,
                "task_spec_padding_bytes": pbytes,
                "case": case,
                "frontier_k": case_k(case),
                "task_manager_pin": int(case_pin(case)),
                "repetitions": len(matched),
            }
            for metric in metrics:
                for stat, value in describe(float(r[metric]) for r in matched).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def paired_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    paddings = sorted({(int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"])) for r in rows})
    for pbytes, pname in paddings:
        reps = sorted({int(r["repetition"]) for r in rows if int(r["task_spec_padding_bytes"]) == pbytes})
        values: dict[str, dict[str, list[float]]] = {
            case: {
                "normalized_throughput_pct": [],
                "throughput_overhead_pct_vs_disabled": [],
                "p95_inflation_pct_vs_disabled": [],
                "pin_speedup_pct_vs_current_same_k": [],
                "pin_overhead_reduction_pp_vs_current_same_k": [],
                "pin_p95_delta_pct_vs_current_same_k": [],
            }
            for case in CASES
        }
        counts = {case: 0 for case in CASES}
        for rep in reps:
            by = {
                str(r["case"]): r
                for r in rows
                if int(r["task_spec_padding_bytes"]) == pbytes and int(r["repetition"]) == rep
            }
            d = by.get("disabled")
            if d is None:
                continue
            dthr = float(d["throughput_rps"])
            dp95 = float(d["latency_p95_ms"])
            for case, row in by.items():
                thr = float(row["throughput_rps"])
                p95 = float(row["latency_p95_ms"])
                values[case]["normalized_throughput_pct"].append(100.0 * thr / dthr if dthr else math.nan)
                values[case]["throughput_overhead_pct_vs_disabled"].append(100.0 * (dthr - thr) / dthr if dthr else math.nan)
                values[case]["p95_inflation_pct_vs_disabled"].append(100.0 * (p95 - dp95) / dp95 if dp95 else math.nan)
                if case_pin(case):
                    current = by.get(f"current_k{case_k(case)}")
                    if current is not None:
                        cthr = float(current["throughput_rps"])
                        cp95 = float(current["latency_p95_ms"])
                        current_overhead = 100.0 * (dthr - cthr) / dthr if dthr else math.nan
                        pin_overhead = 100.0 * (dthr - thr) / dthr if dthr else math.nan
                        values[case]["pin_speedup_pct_vs_current_same_k"].append(100.0 * (thr - cthr) / cthr if cthr else math.nan)
                        values[case]["pin_overhead_reduction_pp_vs_current_same_k"].append(current_overhead - pin_overhead)
                        values[case]["pin_p95_delta_pct_vs_current_same_k"].append(100.0 * (p95 - cp95) / cp95 if cp95 else math.nan)
                counts[case] += 1
        for case in CASES:
            if counts[case] == 0:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": pname,
                "task_spec_padding_bytes": pbytes,
                "case": case,
                "frontier_k": case_k(case),
                "task_manager_pin": int(case_pin(case)),
                "paired_repetitions": counts[case],
            }
            for metric, vals in values[case].items():
                for stat, value in describe(vals).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def run(args: argparse.Namespace) -> None:
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("Benchmark 60 intentionally matches Benchmark 58 and requires R=2, witnesses=2")
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    runs_path = out / "succession_taskmanager_pin_runs.csv"
    if args.overwrite:
        runs_path.unlink(missing_ok=True)
        (out / "succession_taskmanager_pin_summary.csv").unlink(missing_ok=True)
        (out / "succession_taskmanager_pin_paired.csv").unlink(missing_ok=True)
    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    existing = {case_key(r) for r in rows}

    print("Benchmark 60: Succession TaskManager-pin ablation")
    print(f"  cases={len(CASES)} repetitions={args.repetitions} R={args.holders}")
    if args.repetitions == len(CASES):
        print("  ordering: exact cyclic position balance")
    else:
        print("  ordering: partial cyclic balance (use 9 repetitions for exact balance)")

    for padding in args.task_spec_padding:
        for rep in range(1, args.repetitions + 1):
            order = block_order(rep, args.seed)
            print(f"\nrep {rep}/{args.repetitions}: {' -> '.join(order)}")
            for case in order:
                key = (case, padding.size_bytes, rep)
                if key in existing:
                    print(f"  skip {case}: already present")
                    continue
                temp = out / f".tmp_{case}_{padding.size_bytes}_{rep}.json"
                temp.unlink(missing_ok=True)
                print(f"  run  {case}")
                proc = subprocess.run(
                    perf_cmd(args, case, padding, rep, temp),
                    env=child_env(case_pin(case)),
                    text=True,
                    capture_output=True,
                )
                if proc.returncode != 0:
                    sys.stdout.write(proc.stdout)
                    sys.stderr.write(proc.stderr)
                    raise RuntimeError(f"case failed: {case}, rep={rep}")
                if not temp.exists():
                    raise RuntimeError(f"child produced no result: {case}, rep={rep}")
                row = json.loads(temp.read_text())
                temp.unlink(missing_ok=True)
                row["case"] = case
                row["inner_variant"] = inner_variant(case)
                row["frontier_k"] = case_k(case)
                row["task_manager_pin"] = int(case_pin(case))
                rows.append(row)
                existing.add(key)
                write_csv(runs_path, rows)
                print(f"       thr={float(row['throughput_rps']):8.1f} rps p95={float(row['latency_p95_ms']):7.2f} ms")

    write_csv(runs_path, rows)
    summaries = summary_rows(rows)
    paired = paired_rows(rows)
    write_csv(out / "succession_taskmanager_pin_summary.csv", summaries)
    write_csv(out / "succession_taskmanager_pin_paired.csv", paired)

    print("\nFinal robust comparison:")
    for padding in args.task_spec_padding:
        pbytes = padding.size_bytes
        sby = {str(r["case"]): r for r in summaries if int(r["task_spec_padding_bytes"]) == pbytes}
        pby = {str(r["case"]): r for r in paired if int(r["task_spec_padding_bytes"]) == pbytes}
        d = sby.get("disabled")
        if d is None:
            continue
        print(f"  [{padding.name}] disabled thr={float(d['throughput_rps_mean']):8.1f} +/- {float(d['throughput_rps_ci95']):5.1f} rps")
        print("  K   Current throughput / overhead       TaskManager-pin throughput / overhead    Pin vs Current")
        for k in K_VALUES:
            c = sby.get(f"current_k{k}")
            p = sby.get(f"pin_k{k}")
            cp = pby.get(f"current_k{k}")
            pp = pby.get(f"pin_k{k}")
            if not all([c, p, cp, pp]):
                continue
            print(
                f"  {k:2d}  {float(c['throughput_rps_mean']):8.1f} rps / "
                f"{float(cp['throughput_overhead_pct_vs_disabled_mean']):6.2f}%     "
                f"{float(p['throughput_rps_mean']):8.1f} rps / "
                f"{float(pp['throughput_overhead_pct_vs_disabled_mean']):6.2f}%     "
                f"{float(pp['pin_speedup_pct_vs_current_same_k_mean']):+7.2f}% +/- "
                f"{float(pp['pin_speedup_pct_vs_current_same_k_ci95']):5.2f} pp"
            )


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--output-dir", default="gossip_benchmarks/results/60_recovery_succession_taskmanager_pin_ablation")
    p.add_argument("--task-spec-padding", type=parse_padding, nargs="+", default=[Padding("1KiB", 1024)])
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--repetitions", type=int, default=9)
    p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0)
    p.add_argument("--duration-seconds", type=float, default=20.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=0.05)
    p.add_argument("--drain-timeout-seconds", type=float, default=120.0)
    p.add_argument("--cluster-timeout-seconds", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=602026)
    p.add_argument("--overwrite", action="store_true")
    return p


def main() -> None:
    run(parser().parse_args())


if __name__ == "__main__":
    main()
