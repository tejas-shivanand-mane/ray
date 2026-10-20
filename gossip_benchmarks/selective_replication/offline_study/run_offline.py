"""Run offline discovery with a total execution budget and preserved partial output."""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from offline_sweep import dump

ROOT = Path(__file__).resolve().parent


def stop_process(process):
    """Stop a sweep and its worker processes on POSIX systems."""
    if os.name == "posix":
        os.killpg(process.pid, signal.SIGTERM)
    else:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--full-sweep",
        action="store_true",
        help="Run all 1,968 discovery cases and the earlier candidate audit",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--max-minutes", type=float, default=28, help="Total budget, at most 28 minutes"
    )
    parser.add_argument(
        "--profile", type=Path, default=ROOT / "inputs/measured_profile.json"
    )
    args = parser.parse_args()
    if not 0 < args.max_minutes <= 28:
        parser.error("--max-minutes must be greater than zero and at most 28")
    if args.full_sweep and os.name != "posix":
        parser.error("The bounded multiprocess full sweep requires Linux or macOS")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = (
        args.output
        or Path("ray_gcp_results/offline-selective") / f"{stamp}-{uuid4().hex[:8]}"
    ).resolve()
    out.mkdir(parents=True, exist_ok=False)
    jobs = [("sensitivity.py", [])]
    if args.full_sweep:
        jobs.extend(
            [
                (
                    "offline_sweep.py",
                    [
                        "--profile",
                        str(args.profile.resolve()),
                        "--output",
                        str(out / "results"),
                    ],
                ),
                (
                    "heterogeneous_sweep.py",
                    ["--output", str(out / "heterogeneous_results")],
                ),
                (
                    "reliability_sweep.py",
                    ["--output", str(out / "reliability_results")],
                ),
                (
                    "candidate_audit.py",
                    [
                        "--input",
                        str(out / "reliability_results/profile-022.json"),
                        "--output",
                        str(out / "candidate_audit"),
                    ],
                ),
                (
                    "dense_timing_sweep.py",
                    [
                        "--input",
                        str(ROOT / "inputs/candidate.json"),
                        "--output",
                        str(out / "dense_results"),
                    ],
                ),
            ]
        )
    else:
        jobs.append(
            ("reproduce_candidate.py", ["--output", str(out / "dense_results")])
        )
    jobs.extend(
        [
            ("final_candidate.py", ["--results", str(out / "dense_results")]),
            ("verify_results.py", ["--results", str(out)]),
        ]
    )
    started = time.monotonic()
    status = dict(
        state="running", full_sweep=args.full_sweep, completed=[], output=str(out)
    )
    dump(out / "status.json", status)
    print(f"Offline results: {out}", flush=True)
    try:
        for script, options in jobs:
            remaining = args.max_minutes * 60 - (time.monotonic() - started)
            if remaining <= 0:
                raise TimeoutError("Total execution budget exhausted")
            status["active"] = script
            dump(out / "status.json", status)
            print(f"Running {script}", flush=True)
            with (out / f"{Path(script).stem}.log").open("w") as log:
                process = subprocess.Popen(
                    [sys.executable, "-u", str(ROOT / script), *options],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=os.name == "posix",
                )
                try:
                    code = process.wait(timeout=remaining)
                except (subprocess.TimeoutExpired, KeyboardInterrupt):
                    stop_process(process)
                    raise
            print((out / f"{Path(script).stem}.log").read_text(), end="", flush=True)
            if code:
                raise RuntimeError(f"{script} exited with code {code}; see its log")
            status["completed"].append(script)
        status["state"] = "complete"
    except (subprocess.TimeoutExpired, TimeoutError):
        status["state"] = "timed_out"
        print(
            "Execution budget reached; completed results and logs are preserved.",
            flush=True,
        )
    except KeyboardInterrupt:
        status["state"] = "interrupted"
    except Exception as exc:
        status.update(state="failed", error=str(exc))
        print(str(exc), file=sys.stderr)
    finally:
        status["wall_s"] = time.monotonic() - started
        dump(out / "status.json", status)
    return 0 if status["state"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
