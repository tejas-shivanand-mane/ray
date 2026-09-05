#!/usr/bin/env python3
"""One bounded correctness pass for the final Frontier optimization.

Run this after rebuilding Ray, then run profiling-OFF Benchmark 59 once.
Each scenario uses its existing benchmark's fresh cluster and assertions.
This runner does not build Ray, run throughput tests, or change system settings.
Logs use a fresh output directory and the suite stops on its first failure.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SUPPORTED_K = (2, 4, 8, 16, 32)
NODE_FAILURE = "53_recovery_frontier_succession_nonleader_node_failure.py"
COMMIT_GAP = "54_recovery_frontier_succession_commit_gap.py"


def cases(ks: list[int]) -> list[tuple[str, list[str]]]:
    result = []
    for k in ks:
        option = ["--initial-piggyback-k", str(k)]
        result.extend([
            (f"k{k}_owner_node_failure", [NODE_FAILURE, *option]),
            (f"k{k}_commit_gap", [COMMIT_GAP, *option]),
            (f"k{k}_confirmation_blocked", [
                COMMIT_GAP, *option, "--fail-holder-witness-confirmation",
            ]),
        ])
    # Preserve partial-group fallback, later appends and terminal cleanup.
    result.extend([
        ("partial_group_owner_failure", [NODE_FAILURE]),
        ("partial_group_commit_gap", [COMMIT_GAP]),
        ("dynamic_append", ["55_recovery_frontier_succession_dynamic_append.py"]),
        ("dynamic_append_atomicity", [
            "56_recovery_frontier_succession_dynamic_append_atomicity.py",
        ]),
        ("fixed_r_full_group_and_rollover", [
            "29_recovery_frontier_k4_rollover.py",
        ]),
        ("fixed_r_terminal_cleanup", [
            "36_recovery_frontier_terminal_group_cleanup.py",
        ]),
    ])
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ks", type=int, nargs="+", choices=SUPPORTED_K,
                        default=list(SUPPORTED_K))
    parser.add_argument("--case-timeout-seconds", type=float, default=600.0)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if (not math.isfinite(args.case_timeout_seconds)
            or args.case_timeout_seconds <= 0):
        parser.error("--case-timeout-seconds must be finite and positive")
    if sys.flags.optimize:
        parser.error("Run without -O: these correctness benchmarks require assertions")
    ks = list(dict.fromkeys(args.ks))
    output_dir = args.output_dir or (
        HERE / "results" / Path(__file__).stem /
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
    env["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
    env.pop("PYTHONOPTIMIZE", None)
    scenarios = cases(ks)
    print(f"Frontier correctness: {len(scenarios)} scenarios; no performance runs",
          flush=True)
    print(f"Logs: {output_dir}", flush=True)
    with (output_dir / "summary.txt").open("w", buffering=1) as summary:
        for i, (name, arguments) in enumerate(scenarios, 1):
            command = [sys.executable, "-u", str(HERE / arguments[0]),
                       *arguments[1:]]
            log_path = output_dir / f"{name}.log"
            print(f"[{i}/{len(scenarios)}] {name}; log: {log_path}", flush=True)
            start = time.monotonic()
            with log_path.open("w") as log:
                try:
                    with subprocess.Popen(
                        command, cwd=HERE.parent, env=env, stdout=log,
                        stderr=subprocess.STDOUT, start_new_session=True,
                    ) as process:
                        try:
                            code = process.wait(timeout=args.case_timeout_seconds)
                        except (subprocess.TimeoutExpired, KeyboardInterrupt):
                            # Let the benchmark's finally block shut down its
                            # own cluster before escalating within this case.
                            process.send_signal(signal.SIGINT)
                            try:
                                process.wait(timeout=30)
                            except subprocess.TimeoutExpired:
                                os.killpg(process.pid, signal.SIGKILL)
                                process.wait()
                            raise
                        if code:
                            raise subprocess.CalledProcessError(code, command)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    summary.write(f"FAIL {name}: {exc}\n")
                    print(log_path.read_text(errors="replace")[-12000:], flush=True)
                    raise SystemExit(f"Stopped at {name}; full log: {log_path}") from exc
            elapsed = time.monotonic() - start
            summary.write(f"PASS {name} {elapsed:.1f}s\n")
            print(f"  PASS ({elapsed:.1f}s)", flush=True)
    print(f"PASS: all {len(scenarios)} correctness scenarios; logs: {output_dir}",
          flush=True)
    print("Next: run Benchmark 59 once with profiling OFF.", flush=True)


if __name__ == "__main__":
    main()
