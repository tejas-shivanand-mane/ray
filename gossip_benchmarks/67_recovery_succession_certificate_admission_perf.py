#!/usr/bin/env python3
"""Benchmark 67: isolate ordered vs certificate-parallel Succession admission.

Benchmark 59 shows that K=1 adaptive Succession remains substantially slower
than Fixed-R even after metadata-path optimizations. This focused ablation keeps
Benchmark 58/59's exact application workload and changes only the holder
confirmation protocol for ordinary K=1 Succession:

  disabled                 no recovery protection
  fixed_r                  frozen witness-holder Fixed-R, R=2 / W=2
  succession_ordered       ordinary Succession, ordered H1 -> H2 confirmation
  succession_certificate   ordinary Succession, independent witness-backed
                           holder certificates enabled

Both Succession variants keep Recovery Frontier disabled (K=1), R=2, W=2, the
same two node-distinct borrowers, the same burst/inflight settings, and timed
profiling OFF. The certificate variant therefore tests whether serializing the
H1/H2 witness-confirmation stages is the dominant remaining K=1 bottleneck; it
does not reduce the configured holder or witness counts.

The default run uses four repetitions so the four variants occupy every block
position exactly once. For a quick validation run, use --repetitions 3.

Outputs:
  succession_certificate_perf_runs.csv
  succession_certificate_perf_summary.csv
  succession_certificate_perf_paired.csv

Recommended quick run:
  python gossip_benchmarks/67_recovery_succession_certificate_admission_perf.py \
      --repetitions 3 --overwrite
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
BENCH58_PATH = HERE / "58_recovery_frontier_succession_performance.py"


def _load_benchmark58():
    spec = importlib.util.spec_from_file_location(
        "recovery_succession_cert_bench58", BENCH58_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH58_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b58 = _load_benchmark58()

# Import after Benchmark 58 so its timed-run environment defaults remain intact.
from _benchmark_common import disabled, succession, system_config, witness_baseline  # noqa: E402


VARIANTS = [
    "disabled",
    "fixed_r",
    "succession_ordered",
    "succession_certificate",
]


def method_family(variant: str) -> str:
    if variant == "disabled":
        return "disabled"
    if variant == "fixed_r":
        return "fixed_r"
    if variant in ("succession_ordered", "succession_certificate"):
        return "succession"
    raise ValueError(f"unknown variant: {variant}")


def k_for(variant: str) -> int:
    return 0 if variant == "disabled" else 1


def certificate_enabled(variant: str) -> bool:
    return variant == "succession_certificate"


def case_config(
    variant: str,
    holders: int,
    witnesses: int,
    profiling: bool,
) -> dict[str, Any]:
    family = method_family(variant)
    if family == "disabled":
        method = disabled()
    elif family == "fixed_r":
        method = witness_baseline(holders)
    else:
        method = succession(holders)

    cfg = system_config(
        method,
        witness_count=witnesses,
        profiling_enabled=profiling and method.recovery_enabled,
    )
    cfg.update(
        {
            # This benchmark is deliberately K=1 only. The sole Succession
            # difference is ordered-prefix vs independent certificate admission.
            "enable_recovery_frontier": False,
            "recovery_frontier_group_size": 1,
            "recovery_baseline_perf_protect_every_n": 1,
            "enable_recovery_succession_certificate_admission": (
                method.recovery_enabled
                and not method.baseline_enabled
                and certificate_enabled(variant)
            ),
        }
    )
    return cfg


# Reuse Benchmark 58's exact workload and cluster construction. These globals
# are resolved by start_cluster()/single_perf() at runtime.
b58.VARIANTS = VARIANTS
b58.K_BY_VARIANT = {variant: 1 for variant in VARIANTS if variant != "disabled"}
b58.k_for = k_for
b58.case_config = case_config


_T95 = {
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


def describe(values: Iterable[float]) -> dict[str, float]:
    vals = [float(v) for v in values if math.isfinite(float(v))]
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
    mean = statistics.fmean(vals)
    if len(vals) > 1:
        stdev = statistics.stdev(vals)
        ci95 = _T95.get(len(vals) - 1, 1.96) * stdev / math.sqrt(len(vals))
        cv_pct = 100.0 * stdev / mean if mean else math.nan
    else:
        stdev = math.nan
        ci95 = math.nan
        cv_pct = math.nan
    return {
        "mean": mean,
        "median": statistics.median(vals),
        "stdev": stdev,
        "cv_pct": cv_pct,
        "ci95": ci95,
        "min": min(vals),
        "max": max(vals),
    }


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


def block_order(rep: int, seed: int) -> list[str]:
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


def summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metrics = [
        "throughput_rps",
        "latency_mean_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
    ]
    paddings = sorted(
        {
            (int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"]))
            for r in rows
        }
    )
    for pbytes, pname in paddings:
        for variant in VARIANTS:
            matched = [
                r
                for r in rows
                if str(r["variant"]) == variant
                and int(r["task_spec_padding_bytes"]) == pbytes
            ]
            if not matched:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": pname,
                "task_spec_padding_bytes": pbytes,
                "variant": variant,
                "method": method_family(variant),
                "certificate_admission": int(certificate_enabled(variant)),
                "repetitions": len(matched),
            }
            for metric in metrics:
                for stat, value in describe(float(r[metric]) for r in matched).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def paired_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    paddings = sorted(
        {
            (int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"]))
            for r in rows
        }
    )
    for pbytes, pname in paddings:
        reps = sorted(
            {
                int(r["repetition"])
                for r in rows
                if int(r["task_spec_padding_bytes"]) == pbytes
            }
        )
        metrics = {
            variant: {
                "throughput_overhead_pct_vs_disabled": [],
                "throughput_delta_pct_vs_fixed_r": [],
                "throughput_delta_pct_vs_ordered": [],
                "p95_delta_pct_vs_fixed_r": [],
            }
            for variant in VARIANTS
        }
        counts = {variant: 0 for variant in VARIANTS}

        for rep in reps:
            by = {
                str(r["variant"]): r
                for r in rows
                if int(r["task_spec_padding_bytes"]) == pbytes
                and int(r["repetition"]) == rep
            }
            if not all(v in by for v in VARIANTS):
                continue

            disabled_thr = float(by["disabled"]["throughput_rps"])
            fixed_thr = float(by["fixed_r"]["throughput_rps"])
            fixed_p95 = float(by["fixed_r"]["latency_p95_ms"])
            ordered_thr = float(by["succession_ordered"]["throughput_rps"])

            for variant in VARIANTS:
                row = by[variant]
                thr = float(row["throughput_rps"])
                p95 = float(row["latency_p95_ms"])
                metrics[variant]["throughput_overhead_pct_vs_disabled"].append(
                    100.0 * (disabled_thr - thr) / disabled_thr
                    if disabled_thr
                    else math.nan
                )
                metrics[variant]["throughput_delta_pct_vs_fixed_r"].append(
                    100.0 * (thr - fixed_thr) / fixed_thr if fixed_thr else math.nan
                )
                metrics[variant]["throughput_delta_pct_vs_ordered"].append(
                    100.0 * (thr - ordered_thr) / ordered_thr
                    if ordered_thr
                    else math.nan
                )
                metrics[variant]["p95_delta_pct_vs_fixed_r"].append(
                    100.0 * (p95 - fixed_p95) / fixed_p95 if fixed_p95 else math.nan
                )
                counts[variant] += 1

        for variant in VARIANTS:
            if counts[variant] == 0:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": pname,
                "task_spec_padding_bytes": pbytes,
                "variant": variant,
                "paired_repetitions": counts[variant],
            }
            for metric, values in metrics[variant].items():
                for stat, value in describe(values).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def write_outputs(out: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(out / "succession_certificate_perf_runs.csv", rows)
    write_csv(out / "succession_certificate_perf_summary.csv", summary_rows(rows))
    write_csv(out / "succession_certificate_perf_paired.csv", paired_rows(rows))


def perf_cmd(
    args: argparse.Namespace,
    variant: str,
    padding: Any,
    rep: int,
    temp: Path,
) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-perf",
        "--single-variant",
        variant,
        "--single-padding-name",
        padding.name,
        "--single-padding-bytes",
        str(padding.size_bytes),
        "--single-repetition",
        str(rep),
        "--single-output-json",
        str(temp),
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


def run_parent(args: argparse.Namespace) -> None:
    if args.repetitions < 2:
        raise ValueError("--repetitions must be >= 2")
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("Benchmark 67 requires R=witness_count=2")
    if args.burst_size % 32:
        raise ValueError("--burst-size must be divisible by 32")
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    filenames = [
        "succession_certificate_perf_runs.csv",
        "succession_certificate_perf_summary.csv",
        "succession_certificate_perf_paired.csv",
    ]
    if args.overwrite:
        for name in filenames:
            (out / name).unlink(missing_ok=True)

    runs_path = out / filenames[0]
    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    completed = {case_key(r) for r in rows}

    cases: list[tuple[int, Any, str, int]] = []
    for rep in range(1, args.repetitions + 1):
        order = block_order(rep, args.seed)
        for padding in args.task_spec_padding:
            for pos, variant in enumerate(order, 1):
                cases.append((rep, padding, variant, pos))

    pending = [
        case
        for case in cases
        if (case[2], case[1].size_bytes, case[0]) not in completed
    ]

    print(
        "Succession certificate-admission ablation: "
        f"K=1 R=2 W=2 borrowers/pipeline=2 burst={args.burst_size} "
        f"reps={args.repetitions} warmup={args.warmup_seconds:.1f}s "
        f"timed={args.duration_seconds:.1f}s cases={len(cases)} "
        f"remaining={len(pending)}"
    )
    print(
        "  workload=Benchmark58-identical; timed profiling=OFF; "
        "only ordered vs certificate admission changes between Succession cases"
    )

    for i, (rep, padding, variant, pos) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} "
            f"position={pos}/{len(VARIANTS)} variant={variant} "
            f"TaskSpec={padding.name}",
            flush=True,
        )
        temp = out / f".perf_{variant}_{padding.size_bytes}_{rep}.json"
        temp.unlink(missing_ok=True)
        proc = subprocess.run(
            perf_cmd(args, variant, padding, rep, temp),
            env=b58.child_env(profiling=False),
        )
        if proc.returncode != 0 or not temp.exists():
            write_outputs(out, rows)
            raise SystemExit(proc.returncode or 1)

        row = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        row["method"] = method_family(variant)
        row["certificate_admission"] = int(certificate_enabled(variant))
        row["block_position"] = pos
        row["block_seed"] = args.seed
        rows.append(row)
        write_outputs(out, rows)
        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms"
        )

    print("\nFinal certificate-admission comparison:")
    pbytes = args.task_spec_padding[0].size_bytes
    sm = {
        str(r["variant"]): r
        for r in summary_rows(rows)
        if int(r["task_spec_padding_bytes"]) == pbytes
    }
    pr = {
        str(r["variant"]): r
        for r in paired_rows(rows)
        if int(r["task_spec_padding_bytes"]) == pbytes
    }

    for variant in VARIANTS:
        if variant not in sm or variant not in pr:
            continue
        s = sm[variant]
        p = pr[variant]
        print(
            f"  {variant:<23} "
            f"thr={float(s['throughput_rps_mean']):8.1f} "
            f"+/- {float(s['throughput_rps_ci95']):5.1f} rps  "
            f"overhead={float(p['throughput_overhead_pct_vs_disabled_mean']):6.2f}%  "
            f"vs-fixed={float(p['throughput_delta_pct_vs_fixed_r_mean']):+7.2f}%  "
            f"vs-ordered={float(p['throughput_delta_pct_vs_ordered_mean']):+7.2f}%"
        )

    if "succession_certificate" in pr:
        cert = pr["succession_certificate"]
        print("\nDecision signal:")
        print(
            "  certificate speedup vs ordered = "
            f"{float(cert['throughput_delta_pct_vs_ordered_mean']):+.2f}% "
            f"+/- {float(cert['throughput_delta_pct_vs_ordered_ci95']):.2f} pp"
        )
        print(
            "  certificate gap vs Fixed-R     = "
            f"{float(cert['throughput_delta_pct_vs_fixed_r_mean']):+.2f}% "
            f"+/- {float(cert['throughput_delta_pct_vs_fixed_r_ci95']):.2f} pp"
        )
        print(
            "  If certificate mode materially improves throughput, ordered "
            "H1->H2 confirmation is a major bottleneck. If the gap remains "
            "large, physical control-RPC fan-out is the next target."
        )


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument(
        "command", choices=["run", "_single-perf"], nargs="?", default="run"
    )
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/67_recovery_succession_certificate_admission_perf",
    )
    p.add_argument(
        "--task-spec-padding",
        type=b58.parse_padding,
        nargs="+",
        default=[b58.SpecPadding("1KiB", 1024)],
    )
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--repetitions", type=int, default=4)
    p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0)
    p.add_argument("--duration-seconds", type=float, default=20.0)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--single-variant")
    p.add_argument("--single-padding-name")
    p.add_argument("--single-padding-bytes", type=int)
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-output-json")
    return p


def main() -> None:
    args = parser().parse_args()
    if args.command == "_single-perf":
        if args.single_variant not in VARIANTS:
            raise ValueError("invalid timed child variant")
        if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
            raise ValueError("timed child requires RAY_RECOVERY_PROFILING=0")
        row = b58.single_perf(args)
        Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
