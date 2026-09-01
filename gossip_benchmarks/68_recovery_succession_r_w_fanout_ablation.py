#!/usr/bin/env python3
"""Benchmark 68: decompose ordinary Succession K=1 control fan-out into R and W.

Benchmark 59/67 show that ordinary adaptive Succession remains ~30% behind the
Fixed-R baseline at K=1, and certificate-parallel admission does not improve it.
This benchmark keeps the Benchmark 58/59 *application workload* fixed at two
node-distinct borrowers per producer ObjectRef, while varying only the adaptive
Succession protection parameters:

  succession_r1_w1
  succession_r1_w2
  succession_r2_w1
  succession_r2_w2

R is the target non-owner holder count. W is the number of witness raylets used
for each witness publication. The R/W=1 cases are diagnostic ablations only;
they are not proposed production durability settings.

References are also included for:
  disabled
  fixed_r_r2_w2

All cases use ordinary K=1 protection (Recovery Frontier disabled), the same
producer burst/inflight workload, profiling OFF, and a fresh cluster per case.
Thus:
  * R2W2 vs R2W1 isolates the marginal W=2 witness fan-out at R=2.
  * R1W2 vs R1W1 isolates the marginal W=2 witness fan-out at R=1.
  * R2W1 vs R1W1 isolates the marginal second-holder cost at W=1.
  * R2W2 vs R1W2 isolates the marginal second-holder cost at W=2.

Default repetitions=6 gives complete cyclic positional balance for six variants.
Quick validation:
  python gossip_benchmarks/68_recovery_succession_r_w_fanout_ablation.py \
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
import time
from pathlib import Path
from typing import Any, Iterable

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

HERE = Path(__file__).resolve().parent
BENCH58_PATH = HERE / "58_recovery_frontier_succession_performance.py"


def _load_benchmark58():
    spec = importlib.util.spec_from_file_location("recovery_succession_rw_b58", BENCH58_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {BENCH58_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b58 = _load_benchmark58()

import ray  # noqa: E402
from ray.cluster_utils import Cluster  # noqa: E402
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: E402
from _benchmark_common import (  # noqa: E402
    disabled,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    witness_baseline,
)


VARIANTS = [
    "disabled",
    "fixed_r_r2_w2",
    "succession_r1_w1",
    "succession_r1_w2",
    "succession_r2_w1",
    "succession_r2_w2",
]

RW = {
    "disabled": (0, 0),
    "fixed_r_r2_w2": (2, 2),
    "succession_r1_w1": (1, 1),
    "succession_r1_w2": (1, 2),
    "succession_r2_w1": (2, 1),
    "succession_r2_w2": (2, 2),
}


def family(variant: str) -> str:
    if variant == "disabled":
        return "disabled"
    if variant.startswith("fixed_r"):
        return "fixed_r"
    if variant.startswith("succession_"):
        return "succession"
    raise ValueError(f"unknown variant {variant}")


def child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["RAY_RECOVERY_PROFILING"] = "0"
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    env["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"
    return env


def case_config(variant: str) -> dict[str, Any]:
    fam = family(variant)
    r, w = RW[variant]
    if fam == "disabled":
        method = disabled()
        # Keep a valid positive config value even though recovery is disabled.
        w_cfg = 1
    elif fam == "fixed_r":
        method = witness_baseline(r)
        w_cfg = w
    else:
        method = succession(r)
        w_cfg = w

    cfg = system_config(method, witness_count=w_cfg, profiling_enabled=False)
    cfg.update(
        {
            "enable_recovery_frontier": False,
            "recovery_frontier_group_size": 1,
            "recovery_baseline_perf_protect_every_n": 1,
            "enable_recovery_succession_certificate_admission": False,
        }
    )
    return cfg


def start_cluster(args: argparse.Namespace, variant: str) -> tuple[Cluster, str]:
    _, witness_count = RW[variant]
    if variant == "disabled":
        # Keep topology close to protected cases without creating unnecessary
        # protection semantics. Two witness-shaped zero-CPU nodes are harmless.
        witness_nodes = 2
    else:
        witness_nodes = witness_count

    cluster = Cluster()
    cluster.add_node(
        num_cpus=0,
        _system_config=case_config(variant),
        include_dashboard=False,
    )
    producer = cluster.add_node(
        num_cpus=args.cpus_per_node,
        resources={"producer_node": 1},
    )

    # IMPORTANT: application workload is always two borrowers, independent of R.
    for i in range(2):
        cluster.add_node(
            num_cpus=args.cpus_per_node,
            resources={f"borrower_node_{i}": 1},
        )
    for i in range(witness_nodes):
        cluster.add_node(num_cpus=0, resources={f"witness_node_{i}": 1})
    return cluster, producer.node_id


def single_perf(args: argparse.Namespace) -> dict[str, Any]:
    cluster = None
    variant = args.single_variant
    try:
        cluster, producer_node = start_cluster(args, variant)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        expected_nodes = 1 + 1 + 2 + (2 if variant == "disabled" else RW[variant][1])
        wait_for_cluster(ray, expected_nodes, args.cluster_timeout_seconds)

        produce, Borrower = b58.remote_types()
        borrowers = [
            Borrower.options(
                resources={f"borrower_node_{i}": 0.01}, num_cpus=0
            ).remote()
            for i in range(2)
        ]
        ray.get([b.ping.remote() for b in borrowers])
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = b58.build_padding(args.single_padding_bytes, args.inline_chunk_bytes)

        if args.warmup_seconds > 0:
            b58.run_window(
                produce=produce,
                borrowers=borrowers,
                strategy=strategy,
                padding=padding,
                payload_bytes=args.payload_bytes,
                duration_s=args.warmup_seconds,
                inflight=args.inflight_tasks,
                burst=args.burst_size,
                wait_timeout=args.wait_timeout_seconds,
                drain_timeout=args.drain_timeout_seconds,
                request_base=1_000_000,
            )
        if args.settle_seconds > 0:
            time.sleep(args.settle_seconds)

        perf = b58.run_window(
            produce=produce,
            borrowers=borrowers,
            strategy=strategy,
            padding=padding,
            payload_bytes=args.payload_bytes,
            duration_s=args.duration_seconds,
            inflight=args.inflight_tasks,
            burst=args.burst_size,
            wait_timeout=args.wait_timeout_seconds,
            drain_timeout=args.drain_timeout_seconds,
            request_base=10_000_000,
        )
        r, w = RW[variant]
        return {
            "variant": variant,
            "method": family(variant),
            "target_holders_r": r,
            "witness_count_w": w,
            "application_borrowers": 2,
            "frontier_k": 1 if variant != "disabled" else 0,
            "repetition": args.single_repetition,
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
        return {k: math.nan for k in ("mean", "median", "stdev", "cv_pct", "ci95")}
    mean = statistics.fmean(vals)
    median = statistics.median(vals)
    if len(vals) == 1:
        return {"mean": mean, "median": median, "stdev": math.nan,
                "cv_pct": math.nan, "ci95": math.nan}
    stdev = statistics.stdev(vals)
    cv = 100.0 * stdev / mean if mean else math.nan
    ci = _T95.get(len(vals) - 1, 1.96) * stdev / math.sqrt(len(vals))
    return {"mean": mean, "median": median, "stdev": stdev, "cv_pct": cv, "ci95": ci}


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


def summary_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    metrics = ["throughput_rps", "latency_mean_ms", "latency_p95_ms", "latency_p99_ms"]
    for variant in VARIANTS:
        matched = [r for r in rows if str(r["variant"]) == variant]
        if not matched:
            continue
        r, w = RW[variant]
        item: dict[str, Any] = {
            "variant": variant,
            "method": family(variant),
            "target_holders_r": r,
            "witness_count_w": w,
            "repetitions": len(matched),
        }
        for metric in metrics:
            for stat, value in describe(float(x[metric]) for x in matched).items():
                item[f"{metric}_{stat}"] = value
        out.append(item)
    return out


def paired_samples(rows: list[dict[str, Any]], lhs: str, rhs: str) -> list[float]:
    reps = sorted({int(r["repetition"]) for r in rows})
    vals: list[float] = []
    for rep in reps:
        by = {
            str(r["variant"]): r
            for r in rows
            if int(r["repetition"]) == rep
        }
        if lhs not in by or rhs not in by:
            continue
        a = float(by[lhs]["throughput_rps"])
        b = float(by[rhs]["throughput_rps"])
        if b:
            vals.append(100.0 * (a - b) / b)
    return vals


def write_outputs(out: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(out / "succession_rw_fanout_runs.csv", rows)
    write_csv(out / "succession_rw_fanout_summary.csv", summary_rows(rows))

    comparisons = [
        ("witness_reduction_at_r2", "succession_r2_w1", "succession_r2_w2"),
        ("witness_reduction_at_r1", "succession_r1_w1", "succession_r1_w2"),
        ("holder_reduction_at_w2", "succession_r1_w2", "succession_r2_w2"),
        ("holder_reduction_at_w1", "succession_r1_w1", "succession_r2_w1"),
        ("fixed_vs_succession_r2_w2", "fixed_r_r2_w2", "succession_r2_w2"),
    ]
    paired_rows: list[dict[str, Any]] = []
    for name, lhs, rhs in comparisons:
        stats = describe(paired_samples(rows, lhs, rhs))
        paired_rows.append(
            {
                "comparison": name,
                "lhs": lhs,
                "rhs": rhs,
                "speedup_pct_mean": stats["mean"],
                "speedup_pct_ci95": stats["ci95"],
                "paired_repetitions": len(paired_samples(rows, lhs, rhs)),
            }
        )
    write_csv(out / "succession_rw_fanout_paired.csv", paired_rows)


def perf_cmd(args: argparse.Namespace, variant: str, rep: int, temp: Path) -> list[str]:
    return [
        sys.executable,
        str(Path(__file__).resolve()),
        "_single-perf",
        "--single-variant", variant,
        "--single-padding-name", args.task_spec_padding.name,
        "--single-padding-bytes", str(args.task_spec_padding.size_bytes),
        "--single-repetition", str(rep),
        "--single-output-json", str(temp),
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
    if args.repetitions < 2:
        raise ValueError("--repetitions must be >=2")
    if args.burst_size <= 0 or args.inflight_tasks % args.burst_size:
        raise ValueError("--inflight-tasks must be divisible by --burst-size")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    if args.overwrite:
        for name in (
            "succession_rw_fanout_runs.csv",
            "succession_rw_fanout_summary.csv",
            "succession_rw_fanout_paired.csv",
        ):
            (out / name).unlink(missing_ok=True)

    runs_path = out / "succession_rw_fanout_runs.csv"
    rows: list[dict[str, Any]] = [dict(r) for r in read_csv(runs_path)]
    completed = {(str(r["variant"]), int(r["repetition"])) for r in rows}

    cases: list[tuple[int, str, int]] = []
    for rep in range(1, args.repetitions + 1):
        for pos, variant in enumerate(block_order(rep, args.seed), 1):
            cases.append((rep, variant, pos))
    pending = [c for c in cases if (c[1], c[0]) not in completed]

    print(
        "Succession K=1 R/W fan-out ablation: "
        f"application borrowers=2 burst={args.burst_size} inflight={args.inflight_tasks} "
        f"reps={args.repetitions} cases={len(cases)} remaining={len(pending)}"
    )
    print("  R/W=1 variants are diagnostics only; application workload is unchanged")

    for i, (rep, variant, pos) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep}/{args.repetitions} "
            f"position={pos}/{len(VARIANTS)} variant={variant}",
            flush=True,
        )
        temp = out / f".rw_{variant}_{rep}.json"
        temp.unlink(missing_ok=True)
        proc = subprocess.run(perf_cmd(args, variant, rep, temp), env=child_env())
        if proc.returncode != 0 or not temp.exists():
            write_outputs(out, rows)
            raise SystemExit(proc.returncode or 1)
        row = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        row["block_position"] = pos
        row["block_seed"] = args.seed
        rows.append(row)
        write_outputs(out, rows)
        print(
            f"  throughput={float(row['throughput_rps']):.1f} rps "
            f"p95={float(row['latency_p95_ms']):.2f} ms"
        )

    summaries = {str(r["variant"]): r for r in summary_rows(rows)}
    print("\nFinal R/W fan-out comparison:")
    disabled_thr = float(summaries["disabled"]["throughput_rps_mean"])
    for variant in VARIANTS:
        s = summaries[variant]
        thr = float(s["throughput_rps_mean"])
        ci = float(s["throughput_rps_ci95"])
        overhead = 100.0 * (disabled_thr - thr) / disabled_thr if disabled_thr else math.nan
        r, w = RW[variant]
        print(
            f"  {variant:<22} R={r} W={w}  "
            f"thr={thr:8.1f} +/- {ci:5.1f} rps  overhead={overhead:6.2f}%"
        )

    comparisons = [
        ("Reduce W 2->1 at R=2", "succession_r2_w1", "succession_r2_w2"),
        ("Reduce W 2->1 at R=1", "succession_r1_w1", "succession_r1_w2"),
        ("Reduce R 2->1 at W=2", "succession_r1_w2", "succession_r2_w2"),
        ("Reduce R 2->1 at W=1", "succession_r1_w1", "succession_r2_w1"),
    ]
    print("\nPaired decomposition signal:")
    for label, lhs, rhs in comparisons:
        stats = describe(paired_samples(rows, lhs, rhs))
        print(
            f"  {label:<24} speedup={stats['mean']:+7.2f}% "
            f"+/- {stats['ci95']:5.2f} pp"
        )

    print("\nInterpretation:")
    print("  large W-reduction speedup -> witness replication/transport is dominant")
    print("  large R-reduction speedup -> second-holder admission/install path is dominant")
    print("  both small -> owner-side/common per-task Succession bookkeeping is dominant")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-perf"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/68_recovery_succession_r_w_fanout_ablation",
    )
    p.add_argument("--task-spec-padding", type=b58.parse_padding, default=b58.SpecPadding("1KiB", 1024))
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--repetitions", type=int, default=6)
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
        row = single_perf(args)
        Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
