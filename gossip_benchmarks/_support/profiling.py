"""Service counters, process/thread CPU, and optional native stacks for both methods."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import math
import os
from pathlib import Path
import sys

from suite_runner import run_process

HERE = Path(__file__).resolve().parent


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--modes", nargs="+", choices=("service", "system", "native"),
                   default=["service", "system"])
    p.add_argument("--ks", nargs="+", type=int, choices=(1, 2, 4, 8, 16, 32),
                   default=[1, 2, 4, 8, 16, 32])
    p.add_argument("--repetitions", type=int, default=3, help="System CPU repetitions")
    p.add_argument("--duration-seconds", type=float, default=10)
    p.add_argument("--warmup-seconds", type=float, default=1)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--tasks", type=int, default=32, help="Service-profile producer tasks")
    p.add_argument("--case-timeout-seconds", type=float, default=3600)
    p.add_argument("--output-dir", type=Path)
    args = p.parse_args()
    if args.repetitions < 2 or args.tasks <= 0 or args.tasks % 32 or args.payload_bytes < 8:
        p.error("Require repetitions >=2, tasks a positive multiple of 32, payload >=8")
    for name in ("duration_seconds", "case_timeout_seconds", "warmup_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value < 0 or (name != "warmup_seconds" and value == 0):
            p.error(f"Invalid {name}")
    out = (args.output_dir or (
        HERE.parent / "results" / "profile" /
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )).resolve()
    out.mkdir(parents=True, exist_ok=False)
    jobs = []
    ks = list(dict.fromkeys(args.ks))
    for mode in dict.fromkeys(args.modes):
        if mode == "service":
            jobs.append(("service", "service_profile.py",
                         ["--tasks", str(args.tasks), "--ks", *map(str, ks)], None))
        else:
            for k in ks:
                options = ["--duration-seconds", str(args.duration_seconds),
                           "--warmup-seconds", str(args.warmup_seconds)]
                if mode == "system":
                    options += ["--repetitions", str(args.repetitions)]
                jobs.append((f"{mode}_k{k}", f"{mode}_profile.py", options, k))
    print("R=2 W=2; service timings and CPU samples are diagnostic, not throughput acceptance.",
          flush=True)
    print("Native mode requires user-space perf access; no sudo or sysctl changes are made.",
          flush=True)
    for i, (name, filename, options, k) in enumerate(jobs, 1):
        env = os.environ.copy()
        if k is not None:
            env["RECOVERY_PROFILE_K"] = str(k)
        log = out / f"{name}.log"
        print(f"[{i}/{len(jobs)}] {name}; log: {log}", flush=True)
        run_process(
            [sys.executable, "-u", str(HERE / filename),
             "--output-dir", str(out / name), "--payload-bytes", str(args.payload_bytes),
             *options],
            log_path=log, timeout=args.case_timeout_seconds, env=env,
        )
        print(log.read_text(errors="replace")[-24000:], flush=True)
    print(f"Profiles and logs: {out}", flush=True)
