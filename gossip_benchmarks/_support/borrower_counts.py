"""Borrower-count sweep: disabled, Fixed-R K=32 and Succession K=32.

Every borrower must consume each object before its pipeline counts as complete.
R=2/W=2 stays fixed; extra borrowers increase fan-out, not target redundancy.
Each case uses a fresh local cluster with one distinct Ray node per borrower.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import random
import subprocess
import sys

import comparison as b59
from plots import pyplot
from plot_borrowers import plot_borrowers
from suite_runner import run_process

HERE = Path(__file__).resolve().parent
VARIANTS = ["disabled", "fixed_k32", "succession_k32"]
OUTPUTS = (
    "borrower_count_runs.csv", "borrower_count_summary.csv", "borrower_count_paired.csv",
    "borrower_count_comparison.png", "borrower_count_comparison.pdf", "run_config.json",
)


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def configuration(args) -> dict:
    git = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=HERE.parent,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        check=False,
    )
    extension = Path(b59.b58.ray._raylet.__file__)
    extension_stat = extension.stat()
    return {
        "schema_version": 1,
        "variants": VARIANTS,
        "borrower_counts": args.borrower_counts,
        "payload_bytes": args.payload_bytes, "frontier_k": 32,
        "topology": "head + producer + B node-distinct borrowers + 2 witness nodes",
        "case_timeout_seconds": args.case_timeout_seconds,
        "task_spec_padding_name": args.task_spec_padding.name,
        "task_spec_padding_bytes": args.task_spec_padding.size_bytes,
        "holders": 2, "witness_count": 2,
        "inline_chunk_bytes": args.inline_chunk_bytes,
        "burst_size": args.burst_size, "inflight_tasks": args.inflight_tasks,
        "repetitions": args.repetitions, "seed": args.seed,
        "warmup_seconds": args.warmup_seconds,
        "settle_seconds": args.settle_seconds,
        "duration_seconds": args.duration_seconds,
        "cpus_per_node": args.cpus_per_node,
        "wait_timeout_seconds": args.wait_timeout_seconds,
        "cluster_timeout_seconds": args.cluster_timeout_seconds,
        "drain_timeout_seconds": args.drain_timeout_seconds,
        "profiling_enabled": False,
        "measurement": "all B borrowers completed application consumption",
        "durability_measurement": "not measured; B<R may leave Succession below target R",
        "python": sys.executable,
        "source_commit": git.stdout.strip() if git.returncode == 0 else "unknown",
        "ray_version": b59.b58.ray.__version__,
        "ray_build_commit": getattr(b59.b58.ray, "__commit__", "unknown"),
        "ray_extension": str(extension),
        "ray_extension_size": extension_stat.st_size,
        "ray_extension_mtime_ns": extension_stat.st_mtime_ns,
        "ray_extension_sha256": file_sha256(extension),
        "benchmark_source_sha256": {
            name: file_sha256(HERE / name)
            for name in ("borrower_counts.py", "workload.py", "comparison.py",
                         "common.py", "suite_runner.py")
        },
    }


def key(row) -> tuple[int, str, int]:
    return int(row["borrowers_per_pipeline"]), str(row["variant"]), int(row["repetition"])


def validate_rows(rows, config):
    counts = config["borrower_counts"]
    if (config["schema_version"] != 1 or config["variants"] != VARIANTS
            or config["holders"] != 2 or config["witness_count"] != 2
            or config["frontier_k"] != 32 or config["profiling_enabled"]
            or config["repetitions"] < 2 or not counts
            or len(set(counts)) != len(counts) or min(counts) < 1):
        raise ValueError("Invalid borrower-count configuration")
    seen = set()
    for row in rows:
        case = key(row)
        count, variant, rep = case
        if (case in seen or count not in config["borrower_counts"]
                or variant not in VARIANTS or not 1 <= rep <= config["repetitions"]):
            raise ValueError(f"Duplicate or unexpected saved case: {case}")
        seen.add(case)
        expected = {
            "holders": 2, "borrowers_per_pipeline": count, "profiling_enabled": 0,
            "payload_bytes": config["payload_bytes"], "frontier_k": b59.k_for(variant),
            "witness_count": 2,
            "burst_size": config["burst_size"], "inflight_tasks": config["inflight_tasks"],
            "task_spec_padding_bytes": config["task_spec_padding_bytes"],
        }
        for name, value in expected.items():
            if int(row[name]) != value:
                raise ValueError(f"Saved case {case} has incompatible {name}")
        if not math.isfinite(float(row["throughput_rps"])) or float(row["throughput_rps"]) <= 0:
            raise ValueError(f"Saved case {case} has invalid throughput")
        if row["method"] != b59.method_family(variant):
            raise ValueError(f"Saved case {case} has incompatible method")
        if row["task_spec_padding_name"] != config["task_spec_padding_name"]:
            raise ValueError(f"Saved case {case} has incompatible padding name")
        completed = int(row["completed_in_window"])
        submitted = int(row["total_pipeline_submitted"])
        if not 0 < completed <= submitted or int(row["latency_sample_count"]) != submitted:
            raise ValueError(f"Saved case {case} has inconsistent completion counts")
        if not math.isclose(float(row["throughput_rps"]),
                            completed / config["duration_seconds"], rel_tol=1e-10):
            raise ValueError(f"Saved case {case} has inconsistent throughput")
        for metric in ("latency_mean_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms"):
            value = float(row[metric])
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"Saved case {case} has invalid {metric}")
    return seen


def aggregate(rows):
    """Keep borrower counts separate; form ratios only in complete paired blocks."""
    summaries, paired = [], []
    for count in sorted({int(row["borrowers_per_pipeline"]) for row in rows}):
        subset = [row for row in rows if int(row["borrowers_per_pipeline"]) == count]
        for item in b59.summary_rows(subset):
            item["borrowers_per_pipeline"] = count
            item["payload_bytes"] = int(subset[0]["payload_bytes"])
            summaries.append(item)
        samples = {variant: [] for variant in VARIANTS}
        advantages = []
        for rep in sorted({int(row["repetition"]) for row in subset}):
            block = {row["variant"]: row for row in subset if int(row["repetition"]) == rep}
            if set(block) != set(VARIANTS):
                continue
            disabled = float(block["disabled"]["throughput_rps"])
            for variant in VARIANTS:
                samples[variant].append(
                    100 * (1 - float(block[variant]["throughput_rps"]) / disabled))
            advantages.append(100 * (
                float(block["succession_k32"]["throughput_rps"])
                / float(block["fixed_k32"]["throughput_rps"]) - 1))
        for variant, overheads in samples.items():
            if not overheads:
                continue
            item = {
                "borrowers_per_pipeline": count, "variant": variant,
                "method": b59.method_family(variant), "frontier_k": b59.k_for(variant),
                "paired_repetitions": len(overheads),
            }
            metrics = {"throughput_overhead_pct_vs_disabled": overheads}
            if variant == "succession_k32":
                metrics["succession_speedup_pct_vs_fixed_same_k"] = advantages
            for metric, values in metrics.items():
                for stat, value in b59.describe(values).items():
                    item[f"{metric}_{stat}"] = value
            paired.append(item)
    return summaries, paired


def save_csv(path, rows):
    # Keep the completed-case journal intact if writing is interrupted.
    if not rows:
        path.unlink(missing_ok=True)
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    b59.write_csv(temporary, rows)
    temporary.replace(path)


def write_outputs(out, rows):
    summaries, paired = aggregate(rows)
    save_csv(out / "borrower_count_runs.csv", rows)
    save_csv(out / "borrower_count_summary.csv", summaries)
    save_csv(out / "borrower_count_paired.csv", paired)
    return summaries, paired


def plot_saved(out):
    config = json.loads((out / "run_config.json").read_text())
    rows = b59.read_csv(out / "borrower_count_runs.csv")
    seen = validate_rows(rows, config)
    expected = len(config["borrower_counts"]) * len(VARIANTS) * config["repetitions"]
    if len(seen) != expected:
        raise ValueError(f"Only {len(seen)}/{expected} cases complete; resume before plotting")
    summaries, paired = aggregate(rows)
    plot_borrowers(summaries, paired, out, config)
    sm = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in summaries}
    pr = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in paired}
    print("\nFinal borrower-count comparison (all-borrower application completion):")
    for count in sorted(config["borrower_counts"]):
        print(f"\n  Borrowers: {count}")
        for variant in VARIANTS:
            s, p = sm[count, variant], pr[count, variant]
            print(
                f"    {variant:16s} {s['throughput_rps_mean']:8.1f} "
                f"+/- {s['throughput_rps_ci95']:.1f} pipelines/s; "
                f"CV={s['throughput_rps_cv_pct']:.2f}%; "
                f"overhead={p['throughput_overhead_pct_vs_disabled_mean']:.2f}% "
                f"+/- {p['throughput_overhead_pct_vs_disabled_ci95']:.2f} pp"
            )
        p = pr[count, "succession_k32"]
        print(
            "    Succession vs Fixed-R: "
            f"{p['succession_speedup_pct_vs_fixed_same_k_mean']:+.2f}% "
            f"+/- {p['succession_speedup_pct_vs_fixed_same_k_ci95']:.2f} pp"
        )


def run(args):
    pyplot()  # Fail before running cases if matplotlib is unavailable.
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    config = configuration(args)
    config_path = out / "run_config.json"
    if args.overwrite:
        # Delete only this benchmark's named outputs, never an arbitrary directory.
        for name in OUTPUTS:
            (out / name).unlink(missing_ok=True)
    if config_path.exists():
        if json.loads(config_path.read_text()) != config:
            raise ValueError("Saved settings/build differ; choose a new --output-dir or --overwrite")
    else:
        if any((out / name).exists() for name in OUTPUTS if name != "run_config.json"):
            raise ValueError("Outputs exist without run_config.json; use a new directory or --overwrite")
        config_path.write_text(json.dumps(config, indent=2) + "\n")

    rows = [dict(row) for row in b59.read_csv(out / "borrower_count_runs.csv")]
    complete = validate_rows(rows, config)
    variant_order = list(VARIANTS)
    count_order = list(args.borrower_counts)
    random.Random(args.seed).shuffle(variant_order)
    random.Random(args.seed + 1).shuffle(count_order)
    cases = []
    for rep in range(1, args.repetitions + 1):
        count_shift = (rep - 1) % len(count_order)
        counts = count_order[count_shift:] + count_order[:count_shift]
        for count in counts:
            shift = (rep - 1 + count_order.index(count)) % len(VARIANTS)
            order = variant_order[shift:] + variant_order[:shift]
            for position, variant in enumerate(order, 1):
                cases.append((rep, count, variant, position))
    pending = [case for case in cases if (case[1], case[2], case[0]) not in complete]
    print(f"Borrower-count sweep: {len(cases)} cases, {len(pending)} remaining; R=2 W=2",
          flush=True)
    print("  profiling OFF; fresh cluster/case; fixed burst and in-flight task counts", flush=True)
    print(f"  borrower counts: {args.borrower_counts}; payload: {args.payload_bytes} bytes", flush=True)
    print(f"  TaskSpec padding fixed at {args.task_spec_padding.size_bytes} bytes", flush=True)
    print("  all B borrowers must finish; B+4 local Ray nodes; R/W stay 2", flush=True)
    if min(args.borrower_counts) < 2:
        print("  B=1: Succession has fewer borrower candidates than R; "
              "application throughput does not certify two durable holders", flush=True)
    print("  95% CIs are pointwise; complete variant-position balance needs a multiple of 3 reps",
          flush=True)
    print(f"  logs/results: {out}", flush=True)
    for i, (rep, count, variant, position) in enumerate(pending, 1):
        stem = f"case_{count}_{variant}_{rep}"
        temporary = out / (stem + ".json")
        log_path = out / (stem + ".log")
        temporary.unlink(missing_ok=True)
        command = b59.perf_cmd(args, variant, args.task_spec_padding, rep, temporary)
        command[1] = str(Path(__file__).resolve())
        command += ["--single-borrowers", str(count)]
        print(f"[{i}/{len(pending)}] rep={rep} borrowers={count} "
              f"variant={variant}; log: {log_path}", flush=True)
        try:
            run_process(command, log_path=log_path, timeout=args.case_timeout_seconds,
                        env=b59.b58.child_env(profiling=False))
            row = json.loads(temporary.read_text())
            if key(row) != (count, variant, rep):
                raise ValueError("Child result does not match the requested case")
            row["block_position"] = position
            row["block_seed"] = args.seed
            validate_rows([*rows, row], config)
        except Exception:
            print(log_path.read_text(errors="replace")[-12000:] if log_path.exists()
                  else "No child log was created", flush=True)
            print("Stopped; completed cases are saved. Rerun the same command to resume.",
                  flush=True)
            raise
        rows.append(row)
        write_outputs(out, rows)
        temporary.unlink(missing_ok=True)
        print(f"  throughput={float(row['throughput_rps']):.1f} tasks/s", flush=True)
    plot_saved(out)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=("run", "plot"), nargs="?", default="run")
    p.add_argument("--borrower-counts", type=int, nargs="+", default=[1, 2, 4, 8, 16],
                   help="Node-distinct application borrowers; 1 exercises B<R/W with R=W=2")
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding", type=b59.b58.parse_padding,
                   default=b59.b58.SpecPadding("1KiB", 1024))
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0)
    p.add_argument("--duration-seconds", type=float, default=30.0)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=60.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--case-timeout-seconds", type=float, default=600.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-dir", type=Path, default=HERE.parent / "results" / "borrower_counts")
    p.add_argument("--overwrite", action="store_true")
    p.set_defaults(holders=2, witness_count=2)
    return p


def main():
    # Reuse the existing child argument contract; only this benchmark passes
    # borrower_count explicitly. 01/02 and profiling retain their two borrowers.
    if len(sys.argv) > 1 and sys.argv[1] == "_single-perf":
        p = b59.parser()
        p.add_argument("--single-borrowers", type=int, required=True)
        args = p.parse_args()
        if args.single_variant not in VARIANTS or args.single_borrowers < 1:
            p.error("Child requires one of the three sweep variants and at least one borrower")
        if os.environ.get("RAY_RECOVERY_PROFILING") != "0":
            p.error("Timed child requires RAY_RECOVERY_PROFILING=0")
        row = b59.b58.single_perf(args, borrower_count=args.single_borrowers)
        row["method"] = b59.method_family(args.single_variant)
        row["witness_count"] = args.witness_count
        Path(args.single_output_json).write_text(json.dumps(row, allow_nan=False) + "\n")
        return
    p = parser()
    args = p.parse_args()
    if args.command == "plot":
        plot_saved(args.output_dir.resolve())
        return
    if args.repetitions < 2:
        p.error("--repetitions must be at least 2; multiples of 3 balance variant positions")
    if len(set(args.borrower_counts)) != len(args.borrower_counts) or min(args.borrower_counts) < 1:
        p.error("--borrower-counts must be distinct positive integers")
    if args.payload_bytes < 8:
        p.error("--payload-bytes must be at least 8")
    if args.burst_size <= 0 or args.burst_size % 32:
        p.error("--burst-size must be positive and divisible by 32")
    if args.inflight_tasks < args.burst_size or args.inflight_tasks % args.burst_size:
        p.error("--inflight-tasks must be at least a burst and divisible by --burst-size")
    if args.inline_chunk_bytes <= 0 or args.cpus_per_node <= 0:
        p.error("Inline chunk bytes and CPUs per node must be positive")
    for name in ("duration_seconds", "cluster_timeout_seconds", "wait_timeout_seconds",
                 "drain_timeout_seconds", "case_timeout_seconds"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            p.error(f"--{name.replace('_', '-')} must be finite and positive")
    for name in ("warmup_seconds", "settle_seconds"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) < 0:
            p.error(f"--{name.replace('_', '-')} must be finite and nonnegative")
    run(args)


if __name__ == "__main__":
    main()
