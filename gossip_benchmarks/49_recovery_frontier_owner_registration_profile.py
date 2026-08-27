#!/usr/bin/env python3
"""Benchmark 49: isolate Recovery Frontier owner-side registration overhead.

Benchmark 48 shows that K=4..32 now have nearly identical end-to-end overhead,
which points to a per-task cost rather than a per-group publication cost.  This
benchmark measures only the producer submission path:

  * submit the same small recoverable producer tasks used by Benchmark 30;
  * keep their ObjectRefs owner-local and never export them to Consumer;
  * therefore no recovery manifest activation, witness publication, or holder
    RPC belongs in the timed path;
  * compare Disabled, Fixed-R, and Frontier K={1,4,8,16,32};
  * use seven balanced repetitions and paired per-repetition deltas.

Interpretation:

  fixed_r - disabled
      Common recovery owner-retention / TaskManager pin bookkeeping.

  frontier_kN - fixed_r
      Frontier-only owner registration: planner membership, canonical replay
      TaskSpec storage, group bookkeeping, and associated manager locking.

  K1 vs K4..K32
      How much group creation/closure itself matters.  If K4..K32 are flat,
      the remaining Frontier owner cost is per task.

This is a diagnostic benchmark only. Benchmark 30/48 remains the final
end-to-end performance authority.
"""

from __future__ import annotations

import argparse
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

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

HERE = Path(__file__).resolve().parent
BENCH30_PATH = HERE / "30_recovery_frontier_performance.py"


def _load_benchmark30():
    spec = importlib.util.spec_from_file_location("recovery_frontier_bench30_for_49", BENCH30_PATH)
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


def block_order(rep: int, seed: int) -> list[str]:
    base = list(VARIANTS)
    random.Random(seed).shuffle(base)
    shift = (rep - 1) % len(base)
    return base[shift:] + base[:shift]


def variant_env(base: dict[str, str]) -> dict[str, str]:
    return b30.variant_env(base)


def make_producers():
    @ray.remote(max_retries=2)
    def produce(
        request_id: int,
        payload_bytes: int,
        delay_s: float,
        *padding: bytes,
    ) -> bytes:
        if padding and padding[0]:
            _ = padding[0][0]
        if delay_s:
            time.sleep(delay_s)
        prefix = int(request_id).to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    # Recovery-ineligible warmup so warming Python/Cython does not consume a
    # Frontier group member or change the measured K boundaries.
    @ray.remote(max_retries=0)
    def warm_produce(request_id: int) -> int:
        return request_id

    return produce, warm_produce


def summarize_samples(values: list[float]) -> dict[str, float]:
    mean = statistics.fmean(values)
    return {
        "mean_us_per_task": mean,
        "median_us_per_task": statistics.median(values),
        "stdev_us_per_task": statistics.stdev(values) if len(values) >= 2 else math.nan,
        "cv_pct": (
            100.0 * statistics.stdev(values) / mean
            if len(values) >= 2 and mean
            else math.nan
        ),
        "min_us_per_task": min(values),
        "max_us_per_task": max(values),
    }


def run_single(args: argparse.Namespace) -> dict[str, Any]:
    variant = args.single_variant
    if variant not in VARIANTS:
        raise ValueError(f"unknown variant: {variant}")
    if args.burst_size % 32 != 0:
        raise ValueError("burst-size must be divisible by 32")
    if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
        raise RuntimeError("profiling must be disabled for timed registration runs")

    cluster = None
    keepalive: list[ray.ObjectRef] = []
    try:
        cluster, producer_node_id = b30.start_cluster(args, variant)
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        b30.wait_for_cluster(ray, 4, args.cluster_timeout_seconds)

        produce, warm_produce = make_producers()
        strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node_id,
            soft=False,
        )
        padding = b30.build_padding(args.task_spec_padding_bytes, args.inline_chunk_bytes)

        warm_refs = [
            warm_produce.options(
                scheduling_strategy=strategy,
                num_cpus=1,
            ).remote(i)
            for i in range(args.burst_size)
        ]
        ray.get(warm_refs, timeout=args.wait_timeout_seconds)
        del warm_refs
        time.sleep(args.settle_seconds)

        samples: list[float] = []
        request_id = 1_000_000

        for _ in range(args.samples):
            refs: list[ray.ObjectRef] = []
            start_ns = time.perf_counter_ns()
            for _ in range(args.burst_size):
                rid = request_id
                request_id += 1
                ref = produce.options(
                    scheduling_strategy=strategy,
                    num_cpus=1,
                ).remote(
                    rid,
                    args.payload_bytes,
                    args.producer_delay_seconds,
                    *padding,
                )
                refs.append(ref)
            elapsed_ns = time.perf_counter_ns() - start_ns
            samples.append(elapsed_ns / 1e3 / args.burst_size)

            # Keep all owner refs live for the whole measurement so cleanup,
            # tombstoning, or planner-group reclamation cannot enter later
            # samples. Wait only after submission timing has stopped.
            keepalive.extend(refs)
            ray.get(refs, timeout=args.wait_timeout_seconds)

        desc = summarize_samples(samples)
        return {
            "variant": variant,
            "frontier_k": FRONTIER_K.get(variant, 0),
            "repetition": args.single_repetition,
            "burst_size": args.burst_size,
            "samples": args.samples,
            "task_spec_padding_bytes": args.task_spec_padding_bytes,
            "payload_bytes": args.payload_bytes,
            **desc,
        }
    finally:
        try:
            keepalive.clear()
        finally:
            b30.safe_shutdown(ray, cluster)


def paired_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    per_variant: dict[str, dict[str, list[float]]] = {
        v: {
            "submit_us_per_task": [],
            "extra_us_vs_disabled": [],
            "extra_us_vs_fixed_r": [],
        }
        for v in VARIANTS
    }

    reps = sorted({int(r["repetition"]) for r in rows})
    for rep in reps:
        by_variant = {
            str(r["variant"]): r
            for r in rows
            if int(r["repetition"]) == rep
        }
        disabled = by_variant.get("disabled")
        fixed = by_variant.get("fixed_r")
        if disabled is None:
            continue
        d = float(disabled["mean_us_per_task"])
        f = float(fixed["mean_us_per_task"]) if fixed is not None else math.nan
        for variant, row in by_variant.items():
            value = float(row["mean_us_per_task"])
            per_variant[variant]["submit_us_per_task"].append(value)
            per_variant[variant]["extra_us_vs_disabled"].append(value - d)
            if math.isfinite(f):
                per_variant[variant]["extra_us_vs_fixed_r"].append(value - f)

    out: list[dict[str, Any]] = []
    for variant in VARIANTS:
        vals = per_variant[variant]
        if not vals["submit_us_per_task"]:
            continue
        item: dict[str, Any] = {
            "variant": variant,
            "frontier_k": FRONTIER_K.get(variant, 0),
            "paired_repetitions": len(vals["submit_us_per_task"]),
        }
        for key, numbers in vals.items():
            mean, ci = mean_ci95(numbers)
            item[f"{key}_mean"] = mean
            item[f"{key}_ci95"] = ci
            item[f"{key}_median"] = statistics.median(numbers)
        out.append(item)
    return out


def run_parent(args: argparse.Namespace) -> None:
    if args.repetitions < 2:
        raise ValueError("repetitions must be >= 2")
    if args.burst_size % 32 != 0:
        raise ValueError("burst-size must be divisible by 32")

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    results_path = out / "owner_registration_runs.json"
    if args.overwrite:
        results_path.unlink(missing_ok=True)

    rows: list[dict[str, Any]] = []
    if results_path.exists():
        rows = json.loads(results_path.read_text())
    completed = {(str(r["variant"]), int(r["repetition"])) for r in rows}

    cases: list[tuple[int, str, int]] = []
    for rep in range(1, args.repetitions + 1):
        for position, variant in enumerate(block_order(rep, args.seed), 1):
            cases.append((rep, variant, position))

    pending = [c for c in cases if (c[1], c[0]) not in completed]
    print(
        "Recovery Frontier owner-registration profile: "
        f"reps={args.repetitions}, samples={args.samples}, "
        f"burst={args.burst_size}, remaining={len(pending)}"
    )
    print("  no downstream ObjectRef export; no Frontier publication should be timed")

    for i, (rep, variant, position) in enumerate(pending, 1):
        print(
            f"[{i}/{len(pending)}] rep={rep} pos={position} variant={variant}",
            flush=True,
        )
        temp = out / f".single_{variant}_{rep}.json"
        temp.unlink(missing_ok=True)
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "_single-run",
            "--single-variant",
            variant,
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
            "--task-spec-padding-bytes",
            str(args.task_spec_padding_bytes),
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
        proc = subprocess.run(cmd, env=variant_env(os.environ))
        if proc.returncode != 0 or not temp.exists():
            raise SystemExit(proc.returncode or 1)
        row = json.loads(temp.read_text())
        temp.unlink(missing_ok=True)
        rows.append(row)
        results_path.write_text(json.dumps(rows, indent=2, allow_nan=True))
        print(
            f"  submit={float(row['mean_us_per_task']):8.2f} us/task "
            f"within-run CV={float(row['cv_pct']):5.1f}%",
            flush=True,
        )

    print("\nFinal paired owner-registration comparison:")
    print("  variant        submit_us/task       extra_vs_disabled       extra_vs_fixed_r")
    for row in paired_summary(rows):
        print(
            f"  {str(row['variant']):12s} "
            f"{float(row['submit_us_per_task_mean']):8.2f} +/- {float(row['submit_us_per_task_ci95']):6.2f}   "
            f"{float(row['extra_us_vs_disabled_mean']):8.2f} +/- {float(row['extra_us_vs_disabled_ci95']):6.2f}   "
            f"{float(row['extra_us_vs_fixed_r_mean']):8.2f} +/- {float(row['extra_us_vs_fixed_r_ci95']):6.2f}"
        )

    print("\nDecision guide:")
    print("  fixed_r - disabled = common owner-retention/TaskManager bookkeeping")
    print("  frontier - fixed_r = Frontier planner + replay-recipe registration")
    print("  flat K4..K32       = per-task Frontier owner path is the next target")
    print("  falling with K      = group creation/closure still matters")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["run", "_single-run"], nargs="?", default="run")
    p.add_argument(
        "--output-dir",
        default="gossip_benchmarks/results/49_recovery_frontier_owner_registration_profile",
    )
    p.add_argument("--holders", type=int, default=2)
    p.add_argument("--witness-count", type=int, default=2)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--samples", type=int, default=10)
    p.add_argument("--repetitions", type=int, default=7)
    p.add_argument("--producer-delay-seconds", type=float, default=0.02)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--settle-seconds", type=float, default=0.05)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=30.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--single-variant")
    p.add_argument("--single-repetition", type=int)
    p.add_argument("--single-output-json")
    return p


def main() -> None:
    args = parser().parse_args()
    if args.command == "_single-run":
        if args.single_repetition is None or args.single_output_json is None:
            raise ValueError("missing internal _single-run arguments")
        row = run_single(args)
        Path(args.single_output_json).write_text(json.dumps(row, allow_nan=True))
    else:
        run_parent(args)


if __name__ == "__main__":
    main()
