#!/usr/bin/env python3
"""Object-size experiment used by 02_object_size_performance.py.

Compare disabled, Fixed-R K=1/32 and Succession K=1/32. Vary returned object
bytes while retaining the same TaskSpec padding, burst size, in-flight count,
R=2/W=2 and fresh-cluster workload. Profiling is OFF. Default: five sizes and
three repetitions, 75 cases. Resume requires identical settings and provenance."""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import re
import signal
import subprocess
import sys

from plots import plot_sizes, pyplot, size_label

HERE = Path(__file__).resolve().parent
BENCH59 = HERE / "comparison.py"
VARIANTS = ["disabled", "fixed_r", "fixed_k32", "succession_k1", "succession_k32"]
OUTPUTS = (
    "object_size_runs.csv", "object_size_summary.csv", "object_size_paired.csv",
    "object_size_comparison.png", "object_size_comparison.pdf", "run_config.json",
)


def load_b59():
    spec = importlib.util.spec_from_file_location("frontier_size_b59", BENCH59)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {BENCH59}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b59 = load_b59()


def parse_size(value: str) -> int:
    match = re.fullmatch(r"(\d+)\s*(B|KiB|MiB|GiB)?", value, re.IGNORECASE)
    if match is None:
        raise argparse.ArgumentTypeError("Use bytes or a binary unit, e.g. 1024, 64KiB, 1MiB")
    multiplier = {"b": 1, "kib": 1 << 10, "mib": 1 << 20, "gib": 1 << 30}
    size = int(match[1]) * multiplier[(match[2] or "B").lower()]
    if size < 8:
        raise argparse.ArgumentTypeError("Object payload must be at least 8 bytes")
    return size


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
        "object_sizes": args.object_sizes,
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
        "measurement": "both borrowers completed application consumption",
        "python": sys.executable,
        "source_commit": git.stdout.strip() if git.returncode == 0 else "unknown",
        "ray_version": b59.b58.ray.__version__,
        "ray_build_commit": getattr(b59.b58.ray, "__commit__", "unknown"),
        "ray_extension": str(extension),
        "ray_extension_size": extension_stat.st_size,
        "ray_extension_mtime_ns": extension_stat.st_mtime_ns,
    }


def key(row) -> tuple[int, str, int]:
    return int(row["payload_bytes"]), str(row["variant"]), int(row["repetition"])


def validate_rows(rows, config):
    seen = set()
    for row in rows:
        case = key(row)
        size, variant, rep = case
        if (case in seen or size not in config["object_sizes"]
                or variant not in VARIANTS or not 1 <= rep <= config["repetitions"]):
            raise ValueError(f"Duplicate or unexpected saved case: {case}")
        seen.add(case)
        expected = {
            "holders": 2, "borrowers_per_pipeline": 2, "profiling_enabled": 0,
            "burst_size": config["burst_size"], "inflight_tasks": config["inflight_tasks"],
            "task_spec_padding_bytes": config["task_spec_padding_bytes"],
        }
        for name, value in expected.items():
            if int(row[name]) != value:
                raise ValueError(f"Saved case {case} has incompatible {name}")
        if not math.isfinite(float(row["throughput_rps"])) or float(row["throughput_rps"]) <= 0:
            raise ValueError(f"Saved case {case} has invalid throughput")
    return seen


def aggregate(rows):
    summaries, paired = [], []
    for size in sorted({int(row["payload_bytes"]) for row in rows}):
        subset = [row for row in rows if int(row["payload_bytes"]) == size]
        for source, destination in (
            (b59.summary_rows(subset), summaries),
            (b59.paired_rows(subset), paired),
        ):
            for row in source:
                row["payload_bytes"] = size
                row["object_size"] = size_label(size)
                destination.append(row)
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
    save_csv(out / "object_size_runs.csv", rows)
    save_csv(out / "object_size_summary.csv", summaries)
    save_csv(out / "object_size_paired.csv", paired)
    return summaries, paired


def plot_saved(out, exclude_sizes=None):
    config = json.loads((out / "run_config.json").read_text())
    excluded = set(exclude_sizes or [])
    unknown = excluded - set(config["object_sizes"])
    if unknown:
        raise ValueError(f"Excluded sizes are not in the saved configuration: {sorted(unknown)}")
    selected = [size for size in config["object_sizes"] if size not in excluded]
    if not selected:
        raise ValueError("At least one object size must remain for plotting")
    # Filtering affects only the in-memory plot view. Preserve the complete
    # raw journal, configuration and summary CSVs for later analysis/resume.
    config = {**config, "object_sizes": selected}
    rows = [row for row in b59.read_csv(out / "object_size_runs.csv")
            if int(row["payload_bytes"]) not in excluded]
    seen = validate_rows(rows, config)
    expected = len(config["object_sizes"]) * len(VARIANTS) * config["repetitions"]
    if len(seen) != expected:
        raise ValueError(f"Only {len(seen)}/{expected} cases complete; resume before plotting")
    summaries, paired = aggregate(rows)
    if excluded:
        print("Plot excludes: " + ", ".join(size_label(size) for size in sorted(excluded)))
    plot_sizes(rows, summaries, paired, out, VARIANTS)
    print("\nFinal object-size comparison (application throughput):")
    sm = {(int(row["payload_bytes"]), row["variant"]): row for row in summaries}
    pr = {(int(row["payload_bytes"]), row["variant"]): row for row in paired}
    for size in sorted(config["object_sizes"]):
        print(f"\n  Object {size_label(size)}")
        for variant in VARIANTS:
            s, p = sm[size, variant], pr[size, variant]
            print(
                f"    {variant:16s} {s['throughput_rps_mean']:8.1f} "
                f"+/- {s['throughput_rps_ci95']:.1f} tasks/s; "
                f"overhead={p['throughput_overhead_pct_vs_disabled_mean']:.2f}% "
                f"+/- {p['throughput_overhead_pct_vs_disabled_ci95']:.2f} pp"
            )


def run_case(command, log_path, timeout):
    with log_path.open("w") as log:
        with subprocess.Popen(
            command, cwd=HERE.parent, env=b59.b58.child_env(profiling=False),
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        ) as process:
            try:
                code = process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                # Allow the child's finally block to stop its Ray cluster.
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            if code:
                raise subprocess.CalledProcessError(code, command)


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

    rows = [dict(row) for row in b59.read_csv(out / "object_size_runs.csv")]
    complete = validate_rows(rows, config)
    variant_order = list(VARIANTS)
    size_order = list(args.object_sizes)
    random.Random(args.seed).shuffle(variant_order)
    random.Random(args.seed + 1).shuffle(size_order)
    cases = []
    for rep in range(1, args.repetitions + 1):
        size_shift = (rep - 1) % len(size_order)
        sizes = size_order[size_shift:] + size_order[:size_shift]
        for size in sizes:
            shift = (rep - 1 + size_order.index(size)) % len(VARIANTS)
            order = variant_order[shift:] + variant_order[:shift]
            for position, variant in enumerate(order, 1):
                cases.append((rep, size, variant, position))
    pending = [case for case in cases if (case[1], case[2], case[0]) not in complete]
    print(f"Object-size sweep: {len(cases)} cases, {len(pending)} remaining; R=2 W=2",
          flush=True)
    print("  profiling OFF; fresh cluster/case; fixed burst and in-flight task counts", flush=True)
    print(f"  payloads: {', '.join(size_label(size) for size in args.object_sizes)}", flush=True)
    print(f"  TaskSpec padding fixed at {size_label(args.task_spec_padding.size_bytes)}", flush=True)
    print("  95% CIs are pointwise; complete variant-position balance needs a multiple of 5 reps",
          flush=True)
    print(f"  logs/results: {out}", flush=True)
    for i, (rep, size, variant, position) in enumerate(pending, 1):
        stem = f"case_{size}_{variant}_{rep}"
        temporary = out / (stem + ".json")
        log_path = out / (stem + ".log")
        temporary.unlink(missing_ok=True)
        case_args = argparse.Namespace(**vars(args))
        case_args.payload_bytes = size
        command = b59.perf_cmd(case_args, variant, args.task_spec_padding, rep, temporary)
        print(f"[{i}/{len(pending)}] rep={rep} object={size_label(size)} "
              f"variant={variant}; log: {log_path}", flush=True)
        try:
            run_case(command, log_path, args.case_timeout_seconds)
            row = json.loads(temporary.read_text())
            if key(row) != (size, variant, rep):
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
    p.add_argument("--object-sizes", type=parse_size, nargs="+",
                   default=[1024, 16384, 262144, 1048576, 4194304],
                   help="Returned object bytes, e.g. 1KiB 16KiB 256KiB 1MiB 4MiB")
    p.add_argument("--exclude-object-sizes", type=parse_size, nargs="+",
                   help="Plot only: omit saved sizes without changing any CSV or configuration")
    p.add_argument("--task-spec-padding", type=b59.b58.parse_padding,
                   default=b59.b58.SpecPadding("1KiB", 1024))
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warmup-seconds", type=float, default=5.0)
    p.add_argument("--settle-seconds", type=float, default=1.0)
    p.add_argument("--duration-seconds", type=float, default=20.0)
    p.add_argument("--burst-size", type=int, default=32)
    p.add_argument("--inflight-tasks", type=int, default=128)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--wait-timeout-seconds", type=float, default=1.0)
    p.add_argument("--drain-timeout-seconds", type=float, default=180.0)
    p.add_argument("--case-timeout-seconds", type=float, default=600.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-dir", type=Path, default=HERE.parent / "results" / "object_sizes")
    p.add_argument("--overwrite", action="store_true")
    p.set_defaults(holders=2, witness_count=2)
    return p


def main():
    p = parser()
    args = p.parse_args()
    if args.command == "plot":
        plot_saved(args.output_dir.resolve(), args.exclude_object_sizes)
        return
    if args.exclude_object_sizes:
        p.error("--exclude-object-sizes is only supported with the plot command")
    if args.repetitions < 2:
        p.error("--repetitions must be at least 2")
    if len(set(args.object_sizes)) != len(args.object_sizes):
        p.error("--object-sizes must not contain duplicates")
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
