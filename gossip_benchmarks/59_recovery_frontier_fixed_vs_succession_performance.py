#!/usr/bin/env python3
"""Benchmark 59: Fixed-R baseline vs adaptive Succession across Recovery Frontier K.

This is the apples-to-apples performance comparison missing from Benchmark 58.

Every timed case reuses Benchmark 58's exact application workload:
  * R=2;
  * two node-distinct borrowers consume every producer ObjectRef;
  * producer tasks are registered in bursts of 32 before refs are exported;
  * profiling is OFF during timed runs;
  * a fresh Ray cluster is used for every case.

Variants:
  disabled

  fixed_r                 frozen witness-holder Fixed-R baseline, Frontier off (K=1)
  fixed_k2
  fixed_k4
  fixed_k8
  fixed_k16
  fixed_k32

  succession_k1           ordinary adaptive Succession, Frontier off (K=1)
  succession_k2
  succession_k4
  succession_k8
  succession_k16
  succession_k32

For K>1, Fixed-R and Succession both enable the production Recovery Frontier with
the same K. Thus the primary comparison is method-at-equal-K, while Disabled is
the common no-protection reference.

The default paper run uses 13 repetitions. There are 13 variants, so the seeded
cyclic ordering places every variant in every run position exactly once. For a
quick validation run, use --repetitions 2 or 3.

Outputs:
  fixed_vs_succession_frontier_perf_runs.csv
  fixed_vs_succession_frontier_perf_summary.csv
  fixed_vs_succession_frontier_perf_paired.csv
  fixed_vs_succession_k_padding_<bytes>.png / .pdf

Plot existing saved runs without executing benchmarks:
  python gossip_benchmarks/59_recovery_frontier_fixed_vs_succession_performance.py plot

This measures application completion throughput, not time to complete holder
admission. Fixed-R gates exports on holder ACKs; Succession can overlap its
witness-backed admission with application execution.

Recommended paper run:
  python gossip_benchmarks/59_recovery_frontier_fixed_vs_succession_performance.py --overwrite
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
    spec = importlib.util.spec_from_file_location("recovery_frontier_bench58", BENCH58_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH58_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b58 = _load_benchmark58()

# Import after Benchmark 58 so environment defaults remain identical to its timed run.
from _benchmark_common import disabled, succession, system_config, witness_baseline  # noqa: E402


K_VALUES = [1, 2, 4, 8, 16, 32]
FIXED_VARIANT_FOR_K = {
    1: "fixed_r",
    2: "fixed_k2",
    4: "fixed_k4",
    8: "fixed_k8",
    16: "fixed_k16",
    32: "fixed_k32",
}
SUCCESSION_VARIANT_FOR_K = {
    1: "succession_k1",
    2: "succession_k2",
    4: "succession_k4",
    8: "succession_k8",
    16: "succession_k16",
    32: "succession_k32",
}
VARIANTS = [
    "disabled",
    *[FIXED_VARIANT_FOR_K[k] for k in K_VALUES],
    *[SUCCESSION_VARIANT_FOR_K[k] for k in K_VALUES],
]
K_BY_VARIANT = {
    **{variant: k for k, variant in FIXED_VARIANT_FOR_K.items()},
    **{variant: k for k, variant in SUCCESSION_VARIANT_FOR_K.items()},
}


def k_for(variant: str) -> int:
    return K_BY_VARIANT.get(variant, 0)


def method_family(variant: str) -> str:
    if variant == "disabled":
        return "disabled"
    if variant == "fixed_r" or variant.startswith("fixed_k"):
        return "fixed_r"
    if variant.startswith("succession_k"):
        return "succession"
    raise ValueError(f"unknown variant: {variant}")


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
    k = k_for(variant)
    cfg.update(
        {
            # K=1 is each method's non-Frontier reference. For K>1, both methods
            # use the same production Recovery Frontier implementation.
            "enable_recovery_frontier": bool(method.recovery_enabled and k > 1),
            "recovery_frontier_group_size": max(1, k),
            "recovery_baseline_perf_protect_every_n": 1,
            "enable_recovery_succession_certificate_admission": False,
        }
    )
    return cfg


# Reuse Benchmark 58's workload and cluster construction, but replace only the
# method-selection globals. start_cluster() resolves these names at runtime.
b58.VARIANTS = VARIANTS
b58.K_BY_VARIANT = K_BY_VARIANT
b58.k_for = k_for
b58.case_config = case_config


_T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447,
    7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179,
    13: 2.160, 14: 2.145, 15: 2.131, 16: 2.120, 17: 2.110, 18: 2.101,
    19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064,
    25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
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
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def block_order(rep: int, seed: int) -> list[str]:
    """Seeded cyclic order; complete positional balance when reps == 13."""
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
                "frontier_k": k_for(variant),
                "repetitions": len(matched),
            }
            for metric in metrics:
                for stat, value in describe(float(r[metric]) for r in matched).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def paired_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """All ratios/differences are computed inside each repetition before aggregation."""
    out: list[dict[str, Any]] = []
    paddings = sorted(
        {
            (int(r["task_spec_padding_bytes"]), str(r["task_spec_padding_name"]))
            for r in rows
        }
    )

    metric_names = [
        "normalized_throughput_pct",
        "throughput_overhead_pct_vs_disabled",
        "throughput_speedup_pct_vs_method_k1",
        "method_k1_lost_throughput_recovered_pct",
        "p95_inflation_pct_vs_disabled",
        "succession_speedup_pct_vs_fixed_same_k",
        "succession_overhead_delta_pp_vs_fixed_same_k",
        "succession_p95_delta_pct_vs_fixed_same_k",
    ]

    for pbytes, pname in paddings:
        reps = sorted(
            {
                int(r["repetition"])
                for r in rows
                if int(r["task_spec_padding_bytes"]) == pbytes
            }
        )
        metrics = {
            variant: {name: [] for name in metric_names}
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
            disabled_row = by.get("disabled")
            if disabled_row is None:
                continue

            dthr = float(disabled_row["throughput_rps"])
            dp95 = float(disabled_row["latency_p95_ms"])

            for variant, row in by.items():
                thr = float(row["throughput_rps"])
                p95 = float(row["latency_p95_ms"])
                family = method_family(variant)

                metrics[variant]["normalized_throughput_pct"].append(
                    100.0 * thr / dthr if dthr else math.nan
                )
                metrics[variant]["throughput_overhead_pct_vs_disabled"].append(
                    100.0 * (dthr - thr) / dthr if dthr else math.nan
                )
                metrics[variant]["p95_inflation_pct_vs_disabled"].append(
                    100.0 * (p95 - dp95) / dp95 if dp95 else math.nan
                )

                if family == "disabled":
                    metrics[variant]["throughput_speedup_pct_vs_method_k1"].append(0.0)
                    metrics[variant]["method_k1_lost_throughput_recovered_pct"].append(100.0)
                else:
                    k1_name = "fixed_r" if family == "fixed_r" else "succession_k1"
                    k1_row = by.get(k1_name)
                    if k1_row is not None:
                        k1thr = float(k1_row["throughput_rps"])
                        loss = dthr - k1thr
                        metrics[variant]["throughput_speedup_pct_vs_method_k1"].append(
                            100.0 * (thr - k1thr) / k1thr if k1thr else math.nan
                        )
                        if variant == k1_name:
                            recovered = 0.0
                        elif loss > 0:
                            recovered = 100.0 * (thr - k1thr) / loss
                        else:
                            recovered = math.nan
                        metrics[variant]["method_k1_lost_throughput_recovered_pct"].append(
                            recovered
                        )

                # Same-K method comparison is attached to the Succession row.
                if family == "succession":
                    k = k_for(variant)
                    fixed_name = FIXED_VARIANT_FOR_K[k]
                    fixed_row = by.get(fixed_name)
                    if fixed_row is not None:
                        fthr = float(fixed_row["throughput_rps"])
                        fp95 = float(fixed_row["latency_p95_ms"])
                        fixed_overhead = 100.0 * (dthr - fthr) / dthr if dthr else math.nan
                        succ_overhead = 100.0 * (dthr - thr) / dthr if dthr else math.nan
                        metrics[variant]["succession_speedup_pct_vs_fixed_same_k"].append(
                            100.0 * (thr - fthr) / fthr if fthr else math.nan
                        )
                        metrics[variant][
                            "succession_overhead_delta_pp_vs_fixed_same_k"
                        ].append(succ_overhead - fixed_overhead)
                        metrics[variant]["succession_p95_delta_pct_vs_fixed_same_k"].append(
                            100.0 * (p95 - fp95) / fp95 if fp95 else math.nan
                        )

                counts[variant] += 1

        for variant in VARIANTS:
            if counts[variant] == 0:
                continue
            item: dict[str, Any] = {
                "task_spec_padding_name": pname,
                "task_spec_padding_bytes": pbytes,
                "variant": variant,
                "method": method_family(variant),
                "frontier_k": k_for(variant),
                "paired_repetitions": counts[variant],
            }
            for metric, vals in metrics[variant].items():
                finite = [v for v in vals if math.isfinite(v)]
                for stat, value in describe(finite).items():
                    item[f"{metric}_{stat}"] = value
            out.append(item)
    return out


def write_outputs(out: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(out / "fixed_vs_succession_frontier_perf_runs.csv", rows)
    write_csv(out / "fixed_vs_succession_frontier_perf_summary.csv", summary_rows(rows))
    write_csv(out / "fixed_vs_succession_frontier_perf_paired.csv", paired_rows(rows))


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
        "--single-variant", variant,
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
        "--cpus-per-node", str(args.cpus_per_node),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
    ]


def run_parent(args: argparse.Namespace) -> None:
    from _recovery_frontier_plots import pyplot
    pyplot()  # Report a missing plotting dependency before running any cases.
    if args.repetitions < 2:
        raise ValueError("--repetitions must be >= 2")
    if args.burst_size % max(K_VALUES):
        raise ValueError("--burst-size must be divisible by 32")
    if args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")
    if args.holders != 2 or args.witness_count != 2:
        raise ValueError("Benchmark 59 requires R=witness_count=2")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    filenames = [
        "fixed_vs_succession_frontier_perf_runs.csv",
        "fixed_vs_succession_frontier_perf_summary.csv",
        "fixed_vs_succession_frontier_perf_paired.csv",
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

    complete_balance = args.repetitions % len(VARIANTS) == 0
    print(
        "Fixed-R vs adaptive Succession x Frontier: "
        f"R=2 borrowers/pipeline=2 burst={args.burst_size} "
        f"reps={args.repetitions} warmup={args.warmup_seconds:.1f}s "
        f"timed={args.duration_seconds:.1f}s cases={len(cases)} "
        f"remaining={len(pending)}"
    )
    print(
        "  workload=Benchmark58-identical; timed profiling=OFF; "
        "comparisons paired within repetition"
    )
    print(
        "  ordering="
        + (
            "complete cyclic positional balance"
            if complete_balance
            else "cyclic partial positional balance (use 13 reps for complete balance)"
        )
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
        row["block_position"] = pos
        row["block_seed"] = args.seed
        rows.append(row)
        write_outputs(out, rows)
        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms"
        )

    print("\nFinal robust comparison:")
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

    disabled_s = sm.get("disabled")
    if disabled_s is not None:
        print(
            f"  disabled        "
            f"thr={float(disabled_s['throughput_rps_mean']):8.1f} "
            f"+/- {float(disabled_s['throughput_rps_ci95']):5.1f} rps "
            f"(CV={float(disabled_s['throughput_rps_cv_pct']):4.1f}%)"
        )

    print("\n  Equal-K comparison:")
    print(
        "  K   Fixed-R throughput / overhead        "
        "Succession throughput / overhead     Succession vs Fixed-R"
    )
    for k in K_VALUES:
        fixed_name = FIXED_VARIANT_FOR_K[k]
        succ_name = SUCCESSION_VARIANT_FOR_K[k]
        if fixed_name not in sm or succ_name not in sm or fixed_name not in pr or succ_name not in pr:
            continue
        fs, fp = sm[fixed_name], pr[fixed_name]
        ss, sp = sm[succ_name], pr[succ_name]
        same_k = float(sp["succession_speedup_pct_vs_fixed_same_k_mean"])
        same_k_ci = float(sp["succession_speedup_pct_vs_fixed_same_k_ci95"])
        print(
            f"  {k:2d}  "
            f"{float(fs['throughput_rps_mean']):8.1f} rps / "
            f"{float(fp['throughput_overhead_pct_vs_disabled_mean']):6.2f}%    "
            f"{float(ss['throughput_rps_mean']):8.1f} rps / "
            f"{float(sp['throughput_overhead_pct_vs_disabled_mean']):6.2f}%    "
            f"{same_k:+7.2f}% +/- {same_k_ci:5.2f} pp"
        )

    print("\n  Within-method Frontier benefit:")
    for family, mapping in [
        ("Fixed-R", FIXED_VARIANT_FOR_K),
        ("Succession", SUCCESSION_VARIANT_FOR_K),
    ]:
        print(f"  {family}:")
        for k in K_VALUES:
            variant = mapping[k]
            if variant not in pr:
                continue
            p = pr[variant]
            print(
                f"    K={k:2d} overhead="
                f"{float(p['throughput_overhead_pct_vs_disabled_mean']):6.2f}% "
                f"+/- {float(p['throughput_overhead_pct_vs_disabled_ci95']):5.2f} pp "
                f"speedup-vs-K1="
                f"{float(p['throughput_speedup_pct_vs_method_k1_mean']):7.2f}% "
                f"K1-loss-recovered="
                f"{float(p['method_k1_lost_throughput_recovered_pct_mean']):7.2f}%"
            )


    plot_results(args)


def plot_results(args: argparse.Namespace) -> None:
    from _recovery_frontier_plots import plot_k

    out = Path(args.output_dir)
    rows = read_csv(out / "fixed_vs_succession_frontier_perf_runs.csv")
    plot_k(rows, summary_rows(rows), paired_rows(rows), out, VARIANTS,
           FIXED_VARIANT_FOR_K, SUCCESSION_VARIANT_FOR_K)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "plot", "_single-perf"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/59_recovery_frontier_fixed_vs_succession_performance",
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
    p.add_argument("--repetitions", type=int, default=13)
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
        row["method"] = method_family(args.single_variant)
        Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))
    elif args.command == "plot":
        plot_results(args)
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
