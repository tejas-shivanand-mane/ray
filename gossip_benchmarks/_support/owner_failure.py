"""Paced reads of distinct pre-owned objects across a real owner-node failure.

A finite backlog is created and exported directly by its owner to two independent
borrowers. Original executions block on per-object gates. A small prefix is
released and consumed before failure. The owner node is terminated ungracefully; the remaining
gates stay closed, so successful same-ObjectID reads require recovery replay.
This diagnostic is not steady-state producer throughput and is not Benchmark 01.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import sys
import subprocess
import traceback
import tempfile
import time

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

import comparison
from common import safe_shutdown, session_dirs, wait_for_cluster, write_csv
from plots import pyplot
from plotting.plot_04_owner_failure_throughput import draw
from suite_runner import run_process

HERE = Path(__file__).resolve().parent
METHODS = ("disabled", "fixed_r", "succession")


def remote_types():
    @ray.remote(max_retries=2)
    def produce(index, directory, payload_bytes, timeout):
        root = Path(directory)
        marker = root / f"{index}.starts"
        previous = marker.exists()
        with marker.open("a", buffering=1) as out:
            out.write(f"{time.time_ns()},{os.getpid()}\n")
        if not previous:
            deadline = time.monotonic() + timeout
            while not (root / f"{index}.release").exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Original-execution gate was never released")
                time.sleep(0.02)
        return index.to_bytes(8, "little") + b"x" * (payload_bytes - 8)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def create(self, executor, borrowers, tasks, directory, payload_bytes, timeout):
            from ray._private.worker import global_worker
            global_worker.core_worker.reset_recovery_succession_profile()
            self.refs = [
                produce.options(num_cpus=0.1, scheduling_strategy=
                    NodeAffinitySchedulingStrategy(executor, soft=False)).remote(
                        i, directory, payload_bytes, timeout)
                for i in range(tasks)
            ]
            # Full groups are registered before export; consume the leader sidecar last.
            held = ray.get([b.hold.remote(list(reversed(self.refs))) for b in borrowers])
            return [r.hex() for r in self.refs], held

        def profile(self):
            from ray._private.worker import global_worker
            return dict(global_worker.core_worker.get_recovery_succession_profile())

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=4)
    class Borrower:
        def hold(self, refs):
            self.refs = list(reversed(refs))
            return [r.hex() for r in self.refs]

        def read(self, index, timeout):
            ref = self.refs[index]
            try:
                value = ray.get(ref, timeout=timeout)
                return {"id": ref.hex(), "index": int.from_bytes(value[:8], "little"),
                        "bytes": len(value), "ok": True}
            except Exception as exc:
                return {"id": ref.hex(), "ok": False,
                        "error": f"{type(exc).__name__}: {exc}"}
    return Owner, Borrower


def log_tail(path, limit=65536):
    """Read a bounded tail even when native logs are large."""
    with path.open("rb") as stream:
        stream.seek(0, os.SEEK_END)
        stream.seek(max(0, stream.tell() - limit))
        return stream.read(limit)


def save_failure(args, cluster, root, state):
    """Best-effort evidence capture before cluster and marker cleanup."""
    target = Path(args.single_output_json).parent / "diagnostics" / Path(
        args.single_output_json).stem
    target.mkdir(parents=True, exist_ok=True)
    state["settings"] = vars(args)
    state["status"] = "failed"
    state["ray_commit"] = getattr(ray, "__commit__", "unknown")
    # Write the original exception first, even if a subsequent log copy fails.
    (target / "failure.json").write_text(json.dumps(state, indent=2, default=str) + "\n")
    manifest = []
    paths = [(path, target / "markers" / path.name) for path in root.glob("*.starts")]
    if cluster is not None:
        for number, session in enumerate(sorted(session_dirs(cluster))):
            for path in sorted((session / "logs").glob("*")):
                if path.is_file() and (path.suffix == ".err" or
                                      path.name.startswith(("python-core-worker-", "raylet."))):
                    paths.append((path, target / f"session_{number}" / path.name))
    for source, destination in paths:
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(log_tail(source))
            manifest.append({"source": str(source), "saved": str(destination),
                             "tail_limit_bytes": 65536})
        except OSError as exc:
            manifest.append({"source": str(source), "copy_error": str(exc)})
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Failure diagnostics: {target}", file=sys.stderr, flush=True)


def single(args):
    method = args.single_method
    variant = ("disabled" if method == "disabled" else
               comparison.FIXED_VARIANT_FOR_K[args.k] if method == "fixed_r" else
               comparison.SUCCESSION_VARIANT_FOR_K[args.k])
    cluster = None
    events = []
    evidence = {}
    ids = []
    failure_time = None
    owner_node_id = None
    node_removal_finished = None
    phase = "cluster setup"
    with tempfile.TemporaryDirectory(prefix="ray_owner_failure_") as directory:
        root = Path(directory)
        try:
            cluster = Cluster()
            config = comparison.case_config(variant, 2, 2, True)
            config["object_timeout_milliseconds"] = 500
            cluster.add_node(num_cpus=0, _system_config=config, include_dashboard=False)
            owner_node = cluster.add_node(num_cpus=0, resources={"failure_owner": 1})
            owner_node_id = owner_node.node_id
            executor = cluster.add_node(num_cpus=max(4, math.ceil(args.tasks / 10) + 1))
            for i in range(2):
                cluster.add_node(num_cpus=4, resources={f"failure_borrower_{i}": 1})
            for _ in range(2):
                cluster.add_node(num_cpus=0)
            ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
            wait_for_cluster(ray, 7, args.setup_timeout_seconds)
            Owner, Borrower = remote_types()
            owner = Owner.options(num_cpus=0, resources={"failure_owner": 0.01}).remote()
            borrowers = [Borrower.options(
                num_cpus=0, resources={f"failure_borrower_{i}": 0.01}).remote()
                for i in range(2)]
            phase = "object creation and export"
            ids, held = ray.get(owner.create.remote(
                executor.node_id, borrowers, args.tasks, directory, args.payload_bytes,
                args.case_timeout_seconds * 2), timeout=args.setup_timeout_seconds)
            if len(set(ids)) != args.tasks or held != [ids, ids]:
                raise RuntimeError("Owner did not export the same distinct objects to both borrowers")
            deadline = time.monotonic() + args.setup_timeout_seconds
            groups = args.tasks // args.k
            expected = (4 if method == "succession" else 2) * groups
            phase = "protection setup"
            while time.monotonic() < deadline:
                started = all((root / f"{i}.starts").exists() for i in range(args.tasks))
                evidence = ray.get(owner.profile.remote(), timeout=10)
                durable = method == "disabled" or (
                    evidence.get("witness_update_rpcs_completed", 0) >= expected
                    and all(evidence.get(sent, 0) == evidence.get(done, 0)
                            for sent, done in comparison.b58.ASYNC_PAIRS)
                )
                if method == "succession":
                    durable = durable and (
                        evidence.get("holder_admissions_committed", 0) == 2 * groups
                        and evidence.get("max_non_owner_holders", 0) == 2
                    )
                if started and durable:
                    break
                time.sleep(0.05)
            else:
                raise TimeoutError(f"Original starts / R=2 W=2 protection incomplete: {evidence}")

            def consume(index, start, deadline):
                try:
                    calls = [b.read.remote(index, args.request_timeout_seconds) for b in borrowers]
                    left = max(0.01, deadline - time.monotonic())
                    replies = ray.get(calls, timeout=left)
                except ray.exceptions.GetTimeoutError:
                    events.append({"elapsed_seconds": time.monotonic() - start,
                                   "index": index, "ok": False, "error": "observation timeout"})
                    return False
                except ray.exceptions.RayError as exc:
                    events.append({"elapsed_seconds": time.monotonic() - start,
                                   "index": index, "ok": False,
                                   "error": f"{type(exc).__name__}: {exc}"})
                    raise
                ok = all(r["ok"] and r["id"] == ids[index]
                         and r["index"] == index and r["bytes"] == args.payload_bytes
                         for r in replies)
                events.append({"elapsed_seconds": time.monotonic() - start,
                               "index": index, "ok": ok,
                               "error": "" if ok else repr(replies)})
                return ok

            phase = "pre-failure reads"
            start = time.monotonic()
            for i in range(args.before_tasks):
                (root / f"{i}.release").touch()
                if not consume(i, start, time.monotonic() + args.request_timeout_seconds + 5):
                    raise RuntimeError(f"Pre-failure read failed: {events[-1]}")
                time.sleep(args.read_interval_seconds)

            # All unfinished original gates remain closed, including after owner death.
            failure_time = time.monotonic() - start
            failure_wall_ns = time.time_ns()
            phase = "owner-node termination"
            # Start the observation clock at failure injection, not after node
            # removal or failure detection. The head/GCS and executor survive.
            deadline = start + failure_time + args.after_seconds
            cluster.remove_node(owner_node, allow_graceful=False)
            node_removal_finished = time.monotonic() - start
            phase = "post-failure reads"
            for i in range(args.before_tasks, args.tasks):
                if time.monotonic() >= deadline:
                    break
                consume(i, start, deadline)
                time.sleep(args.read_interval_seconds)
            observation_end = time.monotonic() - start
            after = [e for e in events if e["index"] >= args.before_tasks]
            phase = "replay validation"
            replay_counts = {}
            for i in range(args.before_tasks, args.tasks):
                lines = (root / f"{i}.starts").read_text().splitlines()
                replay_counts[str(i)] = sum(int(line.split(",")[0]) >= failure_wall_ns
                                            for line in lines)
            for event in after:
                if event["ok"] and replay_counts[str(event["index"])] < 1:
                    raise RuntimeError("Post-failure success lacked an observed recovery replay")
            # Verify the intended node failure without delaying the start of
            # post-failure reads. This timestamp is a final confirmation, not
            # a measurement of when GCS first detected the failure.
            phase = "owner-node death verification"
            verification_deadline = time.monotonic() + args.setup_timeout_seconds
            while True:
                node_records = [n for n in ray.nodes() if n["NodeID"] == owner_node_id]
                if node_records and all(not n["Alive"] for n in node_records):
                    break
                if time.monotonic() >= verification_deadline:
                    raise TimeoutError("Owner node was not confirmed dead in GCS")
                time.sleep(0.1)
            node_death_confirmed = time.monotonic() - start
            first = next((e["elapsed_seconds"] - failure_time for e in after if e["ok"]), None)
            result = {
                "method": method, "k": args.k, "trial": args.single_trial,
                "failure_type": "owner_node", "owner_node_id": owner_node_id,
                "node_removal_finished_seconds": node_removal_finished,
                "node_death_confirmed_seconds": node_death_confirmed,
                "tasks": args.tasks, "before_tasks": args.before_tasks,
                "payload_bytes": args.payload_bytes, "holders": 2, "witnesses": 2,
                "failure_seconds": failure_time, "observation_end_seconds": observation_end,
                "first_recovered_seconds": first,
                "post_failure_successes": sum(e["ok"] for e in after),
                "post_failure_failures": sum(not e["ok"] for e in after),
                "post_failure_unattempted": args.tasks - args.before_tasks - len(after),
                "replay_counts": replay_counts, "events": events,
                "protection_profile": evidence,
                "measurement": "paced unique-object reads; both borrowers must succeed",
                "ray_commit": getattr(ray, "__commit__", "unknown"),
            }
            Path(args.single_output_json).write_text(json.dumps(result, indent=2) + "\n")
        except Exception:
            state = {"phase": phase, "traceback": traceback.format_exc(),
                     "events": events, "object_ids": ids,
                     "failure_seconds": failure_time, "protection_profile": evidence,
                     "failure_type": "owner_node", "owner_node_id": owner_node_id,
                     "node_removal_finished_seconds": node_removal_finished}
            try:
                save_failure(args, cluster, root, state)
            except Exception as diagnostic_error:
                print(f"Could not finish saving diagnostics: {diagnostic_error}",
                      file=sys.stderr, flush=True)
            raise
        finally:
            safe_shutdown(ray, cluster)


def plot(out, bucket_seconds):
    files = sorted(out.glob("trial_*_*.json"))
    if not files:
        raise FileNotFoundError(f"No saved owner-failure runs in {out}")
    runs = [json.loads(path.read_text()) for path in files]
    rows = []
    summary = []
    for run in runs:
        summary.append({key: value for key, value in run.items()
                        if key not in ("events", "replay_counts", "protection_profile")})
        lo = math.floor(-run["failure_seconds"] / bucket_seconds)
        hi = math.ceil((run["observation_end_seconds"] - run["failure_seconds"]) / bucket_seconds)
        for bucket in range(lo, hi):
            left, right = bucket * bucket_seconds, (bucket + 1) * bucket_seconds
            coverage = min(right, run["observation_end_seconds"] - run["failure_seconds"]) - max(
                left, -run["failure_seconds"])
            count = sum(e["ok"] and left <= e["elapsed_seconds"] - run["failure_seconds"] < right
                        for e in run["events"])
            rows.append({"method": run["method"], "trial": run["trial"],
                         "seconds_from_failure": (left + right) / 2,
                         "throughput_rps": count / coverage,
                         "observed_seconds": coverage})
    draw(rows, runs, out, METHODS)
    write_csv(out / "owner_failure_buckets.csv", rows)
    write_csv(out / "owner_failure_summary.csv", summary)
    for run in runs:
        print(f"{run['method']} trial={run['trial']}: recovered="
              f"{run['post_failure_successes']}/{run['tasks'] - run['before_tasks']}, "
              f"first={run['first_recovered_seconds']} seconds")
    print(f"Plots, raw events, and CSV: {out}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", nargs="?", choices=("run", "plot", "_single"), default="run")
    p.add_argument("--output-dir", type=Path)
    p.add_argument("--trials", type=int, default=1)
    p.add_argument("--k", type=int, choices=(1, 2, 4, 8, 16, 32), default=1)
    p.add_argument("--tasks", type=int, default=32)
    p.add_argument("--before-tasks", type=int, default=8)
    p.add_argument("--payload-bytes", type=int, default=65536)
    p.add_argument("--read-interval-seconds", type=float, default=0.25)
    p.add_argument("--after-seconds", type=float, default=60)
    p.add_argument("--bucket-seconds", type=float, default=1)
    p.add_argument("--request-timeout-seconds", type=float, default=30)
    p.add_argument("--setup-timeout-seconds", type=float, default=180)
    p.add_argument("--case-timeout-seconds", type=float, default=600)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--single-method", choices=METHODS, help=argparse.SUPPRESS)
    p.add_argument("--single-trial", type=int, help=argparse.SUPPRESS)
    p.add_argument("--single-output-json", help=argparse.SUPPRESS)
    args = p.parse_args()
    if args.tasks < 2 or args.tasks > 128 or args.tasks % args.k:
        p.error("--tasks must be 2..128 and divisible by K")
    if not 0 < args.before_tasks < args.tasks or args.payload_bytes < 8 or args.trials < 1:
        p.error("Require 0 < before-tasks < tasks, payload >=8, trials >=1")
    for name in ("read_interval_seconds", "after_seconds", "bucket_seconds",
                 "request_timeout_seconds", "setup_timeout_seconds", "case_timeout_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            p.error(f"Invalid {name}")
    if args.command == "_single":
        if args.single_method is None or args.single_output_json is None:
            p.error("Missing internal child arguments")
        single(args)
        return
    if args.command == "plot":
        if args.output_dir is None:
            p.error("plot requires --output-dir pointing to saved runs")
        plot(args.output_dir.resolve(), args.bucket_seconds)
        return
    pyplot()
    out = (args.output_dir or (
        HERE.parent / "results" / "owner_failure" /
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )).resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "settings.json").write_text(json.dumps(
        {key: str(value) if isinstance(value, Path) else value
         for key, value in vars(args).items()}, indent=2) + "\n")
    rng = random.Random(args.seed)
    for trial in range(1, args.trials + 1):
        order = list(METHODS)
        rng.shuffle(order)
        for method in order:
            name = f"trial_{trial}_{method}"
            print(f"{name}; log: {out / (name + '.log')}", flush=True)
            options = []
            for key in ("k", "tasks", "before_tasks", "payload_bytes", "read_interval_seconds",
                        "after_seconds", "request_timeout_seconds", "setup_timeout_seconds",
                        "case_timeout_seconds"):
                options += ["--" + key.replace("_", "-"), str(getattr(args, key))]
            try:
                run_process([sys.executable, "-u", str(Path(__file__).resolve()), "_single",
                             "--single-method", method, "--single-trial", str(trial),
                             "--single-output-json", str(out / (name + ".json")), *options],
                            log_path=out / (name + ".log"), timeout=args.case_timeout_seconds,
                            env=comparison.b58.child_env(profiling=True))
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                print(f"Failed case: {name}; child log tail:", file=sys.stderr, flush=True)
                try:
                    print(log_tail(out / (name + ".log")).decode(errors="replace"),
                          file=sys.stderr, flush=True)
                except OSError as exc:
                    print(f"Could not read child log: {exc}", file=sys.stderr)
                print(f"Check {out / 'diagnostics' / name} if captured by the child. "
                      "A forced termination may prevent capture.", file=sys.stderr, flush=True)
                raise

    plot(out, args.bucket_seconds)


if __name__ == "__main__":
    main()
