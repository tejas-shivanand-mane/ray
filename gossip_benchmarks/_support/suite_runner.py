"""Bounded correctness suites. Each scenario runs in an isolated subprocess."""
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
KS = (1, 2, 4, 8, 16, 32)


def scenarios(method, ks, output):
    result = []
    if method == "succession":
        for k in ks:
            options = ["--ordinary-k1"] if k == 1 else ["--initial-piggyback-k", str(k)]
            result.extend([
                (f"k{k}_owner_node_failure", "succession_node_failure.py", options),
                (f"k{k}_witness_ack_commit_gap", "succession_commit_gap.py", options),
                (f"k{k}_confirmation_blocked", "succession_commit_gap.py",
                 [*options, "--fail-holder-witness-confirmation"]),
            ])
        result.extend([
            ("partial_group_owner_failure", "succession_node_failure.py", []),
            ("partial_group_commit_gap", "succession_commit_gap.py", []),
            ("dynamic_append", "succession_append.py", []),
            ("append_atomicity", "succession_append_atomicity.py", []),
            ("sequential_concurrent_retry_late_borrowers", "succession_failover.py",
             ["--cases", "all", "--output-dir", str(output / "failover")]),
        ])
    else:
        result.extend([
            ("k1_regression", "fixed_k1.py", []),
            ("nonleader_owner_node_failure", "fixed_node_failure.py", []),
            ("full_group_and_rollover", "fixed_rollover.py", []),
            ("group_lifecycle", "fixed_lifecycle.py", []),
            ("terminal_group_cleanup", "fixed_cleanup.py", []),
            ("concurrent_recovery_claim", "fixed_concurrent_claim.py", []),
            ("authoritative_witness_failover", "fixed_witness_failover.py", []),
            ("live_witness_stall_no_promotion", "fixed_witness_stall.py", []),
            ("acting_borrower_death", "fixed_acting_borrower.py", []),
            ("inflight_owner_handoff", "fixed_handoff.py", []),
        ])
    return result


def run_process(command, *, log_path, timeout, env):
    with log_path.open("w") as log:
        with subprocess.Popen(
            command, env=env, stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as process:
            try:
                code = process.wait(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt):
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            if code:
                raise subprocess.CalledProcessError(code, command)


def main(method):
    p = argparse.ArgumentParser(description=f"Broad {method} correctness suite; R=2 W=2")
    if method == "succession":
        p.add_argument("--ks", nargs="+", type=int, choices=KS, default=list(KS))
    p.add_argument("--cases", nargs="+", help="Run named scenarios from --list")
    p.add_argument("--list", action="store_true")
    p.add_argument("--case-timeout-seconds", type=float, default=600)
    p.add_argument("--output-dir", type=Path)
    args = p.parse_args()
    if sys.flags.optimize:
        p.error("Run without -O: correctness checks require assertions")
    if not math.isfinite(args.case_timeout_seconds) or args.case_timeout_seconds <= 0:
        p.error("--case-timeout-seconds must be finite and positive")
    output = (args.output_dir or (
        HERE.parent / "results" / f"{method}_correctness" /
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )).resolve()
    cases = scenarios(method, list(dict.fromkeys(getattr(args, "ks", KS))), output)
    if args.cases:
        unknown = set(args.cases) - {name for name, _, _ in cases}
        if unknown:
            p.error(f"Unknown scenarios: {sorted(unknown)}; use --list")
        cases = [case for case in cases if case[0] in args.cases]
    if args.list:
        for name, _, _ in cases:
            print(name)
        return
    output.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env["RAY_BACKEND_LOG_LEVEL"] = "info"
    env["RAY_DEDUP_LOGS"] = "0"
    env["RAY_RECOVERY_PROFILING"] = "1"
    for key in ("RAY_RECOVERY_CERTIFICATE_ADMISSION", "RAY_RECOVERY_TASKMANAGER_PIN",
                "RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"):
        env[key] = "0"
    env.pop("PYTHONOPTIMIZE", None)
    print(f"{method}: {len(cases)} correctness scenarios; R=2 W=2; logs: {output}", flush=True)
    with (output / "summary.txt").open("w", buffering=1) as summary:
        for index, (name, filename, options) in enumerate(cases, 1):
            log = output / f"{name}.log"
            print(f"[{index}/{len(cases)}] {name}; log: {log}", flush=True)
            start = time.monotonic()
            try:
                run_process([sys.executable, "-u", str(HERE / filename), *options],
                            log_path=log, timeout=args.case_timeout_seconds, env=env)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                summary.write(f"FAIL {name}: {exc}\n")
                print(log.read_text(errors="replace")[-12000:], flush=True)
                raise SystemExit(f"Stopped at {name}; full log: {log}") from exc
            summary.write(f"PASS {name} {time.monotonic() - start:.1f}s\n")
            print("  PASS", flush=True)
    print(f"PASS: all {len(cases)} scenarios; logs: {output}", flush=True)
