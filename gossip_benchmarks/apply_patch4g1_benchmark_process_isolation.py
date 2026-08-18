#!/usr/bin/env python3
"""Patch 4G-1: isolate every Benchmark-16 case in a fresh Python process.

Why:
Patch 4G intentionally changes recovery_succession_benchmark_ablation_mode between
cases. RayConfig is process-global, and Patch 4G also caches the mode in a
function-local static in C++. Reusing the same Python driver across cases freezes
the driver-side mode to whichever recovery case initializes it first.

This patch changes only gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py.
No C++ rebuild is required.

Run from the Ray repository root:
    python gossip_benchmarks/apply_patch4g1_benchmark_process_isolation.py
"""
from __future__ import annotations

import datetime as _dt
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path.cwd()
TARGET = ROOT / "gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py"
MARKER = "Patch 4G-1: fresh process per ablation case."

def die(msg: str) -> None:
    raise SystemExit(msg)

if not TARGET.exists():
    die(f"missing {TARGET}; apply Patch 4G first")

text = TARGET.read_text()

if MARKER in text:
    print("Patch 4G-1 already applied; nothing to do.")
    raise SystemExit(0)

required = [
    'Case("metadata_only", "MetadataOnly", True, "metadata_only")',
    'Case("full", "Full4F", True, "full")',
    'def run_benchmark(args: argparse.Namespace) -> None:',
    'def build_parser() -> argparse.ArgumentParser:',
]
for anchor in required:
    if anchor not in text:
        die(f"expected Patch-4G anchor not found: {anchor!r}")

old = """import argparse
import math
import random
import statistics
import time
"""
new = """import argparse
import json
import math
import random
import statistics
import subprocess
import sys
import tempfile
import time
"""
if old not in text:
    die("import anchor not found")
text = text.replace(old, new, 1)

old_run = """def run_benchmark(args: argparse.Namespace) -> None:
    order_base = cases()
    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []
    total = args.repetitions * len(order_base)
    idx = 0
    for rep in range(1, args.repetitions + 1):
        order = order_base[:]
        if not args.fixed_order:
            rng.shuffle(order)
        for case in order:
            idx += 1
            print(f"[{idx}/{total}] rep={rep} case={case.label} mode={case.mode}")
            rows.append(run_one(case, rep, args))
    root = Path(args.output_dir)
    write_csv(root / "patch4g_b1_runs.csv", rows)
    summary = summarize(rows)
    write_csv(root / "patch4g_b1_summary.csv", summary)
    print(f"Wrote {root / 'patch4g_b1_summary.csv'}")
    print("\\nB1 throughput loss vs Disabled:")
    for r in summary:
        print(f"  {r['label']:24s} {float(r['throughput_mean_rps']):9.1f} rps  loss={float(r['throughput_loss_vs_disabled_pct']):6.2f}%")
"""

new_run = """def _common_child_args(args: argparse.Namespace) -> list[str]:
    out = [
        "--warmup-seconds", str(args.warmup_seconds),
        "--duration-seconds", str(args.duration_seconds),
        "--inflight", str(args.inflight),
        "--payload-bytes", str(args.payload_bytes),
        "--cpus-per-node", str(args.cpus_per_node),
        "--witness-count", str(args.witness_count),
        "--wait-timeout-seconds", str(args.wait_timeout_seconds),
        "--drain-timeout-seconds", str(args.drain_timeout_seconds),
        "--cluster-timeout-seconds", str(args.cluster_timeout_seconds),
        "--profile-quiescence-timeout-seconds",
        str(args.profile_quiescence_timeout_seconds),
        "--profile-stable-seconds", str(args.profile_stable_seconds),
        "--seed", str(args.seed),
        "--output-dir", str(args.output_dir),
    ]
    if args.fixed_order:
        out.append("--fixed-order")
    return out


def run_single_case(args: argparse.Namespace) -> None:
    match = [c for c in cases() if c.key == args.case]
    if len(match) != 1:
        raise ValueError(f"unknown benchmark case {args.case!r}")
    row = run_one(match[0], args.repetition, args)
    Path(args.row_json).write_text(json.dumps(row, allow_nan=True))


def run_benchmark(args: argparse.Namespace) -> None:
    # Patch 4G-1: fresh process per ablation case.
    #
    # RayConfig is process-global and the C++ ablation helper intentionally
    # caches the configured mode. Therefore changing _system_config while
    # repeatedly ray.init()/ray.shutdown() in one Python driver does NOT give
    # a trustworthy per-case owner-side ablation. A fresh process makes every
    # case start with exactly the requested mode.
    order_base = cases()
    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []
    total = args.repetitions * len(order_base)
    idx = 0
    script = str(Path(__file__).resolve())

    with tempfile.TemporaryDirectory(prefix="patch4g1-") as tmp:
        tmp_root = Path(tmp)
        for rep in range(1, args.repetitions + 1):
            order = order_base[:]
            if not args.fixed_order:
                rng.shuffle(order)

            for case in order:
                idx += 1
                print(
                    f"[{idx}/{total}] rep={rep} case={case.label} "
                    f"mode={case.mode} [fresh process]",
                    flush=True,
                )
                row_json = tmp_root / f"rep{rep}_{case.key}.json"
                cmd = [
                    sys.executable,
                    script,
                    "_single-run",
                    "--case", case.key,
                    "--repetition", str(rep),
                    "--row-json", str(row_json),
                    *_common_child_args(args),
                ]
                subprocess.run(cmd, check=True)

                if not row_json.exists():
                    raise RuntimeError(
                        f"child benchmark did not write expected result {row_json}"
                    )
                rows.append(json.loads(row_json.read_text()))

    root = Path(args.output_dir)
    write_csv(root / "patch4g_b1_runs.csv", rows)
    summary = summarize(rows)
    write_csv(root / "patch4g_b1_summary.csv", summary)
    print(f"Wrote {root / 'patch4g_b1_summary.csv'}")
    print("\\nB1 throughput loss vs Disabled:")
    for r in summary:
        print(
            f"  {r['label']:24s} "
            f"{float(r['throughput_mean_rps']):9.1f} rps  "
            f"loss={float(r['throughput_loss_vs_disabled_pct']):6.2f}%"
        )
"""

if old_run not in text:
    die("run_benchmark anchor did not match current Patch-4G benchmark")
text = text.replace(old_run, new_run, 1)

old_parser = """    r = sub.add_parser("run")
    add_common(r)
    rp = sub.add_parser("run-and-plot")
    add_common(rp)
    pl = sub.add_parser("plot")
    pl.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")
    return p
"""
new_parser = """    r = sub.add_parser("run")
    add_common(r)
    rp = sub.add_parser("run-and-plot")
    add_common(rp)

    # Internal Patch-4G-1 worker command. The parent launches one fresh Python
    # process per (case, repetition) so RayConfig cannot leak across cases.
    one = sub.add_parser("_single-run")
    add_common(one)
    one.add_argument("--case", required=True)
    one.add_argument("--repetition", type=int, required=True)
    one.add_argument("--row-json", required=True)

    pl = sub.add_parser("plot")
    pl.add_argument("--output-dir", default="gossip_benchmarks/results/16_patch4g_b1")
    return p
"""
if old_parser not in text:
    die("parser anchor did not match current Patch-4G benchmark")
text = text.replace(old_parser, new_parser, 1)

old_main = """def main() -> None:
    args = build_parser().parse_args()
    if args.command in {"run", "run-and-plot"}:
        run_benchmark(args)
    if args.command in {"plot", "run-and-plot"}:
        plot(args)
"""
new_main = f"""def main() -> None:
    args = build_parser().parse_args()
    # {MARKER}
    if args.command == "_single-run":
        run_single_case(args)
        return
    if args.command in {{"run", "run-and-plot"}}:
        run_benchmark(args)
    if args.command in {{"plot", "run-and-plot"}}:
        plot(args)
"""
if old_main not in text:
    die("main anchor did not match current Patch-4G benchmark")
text = text.replace(old_main, new_main, 1)

stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
backup = ROOT / ".patch4g1_backups" / stamp / TARGET.relative_to(ROOT)
backup.parent.mkdir(parents=True, exist_ok=True)
shutil.copy2(TARGET, backup)
TARGET.write_text(text)

subprocess.run([sys.executable, "-m", "py_compile", str(TARGET)], check=True)
subprocess.run(["git", "diff", "--check"], check=True)

print("Patch 4G-1 applied successfully.")
print(f"Backup: {backup}")
print("No Ray rebuild is required; only Benchmark 16 Python orchestration changed.")
