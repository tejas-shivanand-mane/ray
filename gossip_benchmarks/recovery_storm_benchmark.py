#!/usr/bin/env python3
"""
Prototype benchmark: recovery storm after one owner/node failure.

A single owner creates many independent in-flight tasks. The same holder chain is
formed for every output and a persistent borrower retains all ObjectRefs. The owner
node is then removed and the borrower issues concurrent reads for all objects.

The benchmark compares recovery enabled vs disabled and reports:
  * success rate
  * failure-to-first-result / p50 / p95 / p99 result latency
  * total recovery window
  * number of tasks that actually replayed
  * duplicate-execution indicators from per-task START markers
  * per-second successful recovery throughput in a detail CSV
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import statistics
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@dataclass(frozen=True)
class Case:
    label: str
    enabled: bool


CASES = [
    Case("Disabled", False),
    Case("Enabled", True),
]


@dataclass
class ClusterLayout:
    cluster: Cluster
    owner_node: Any


def percentile(values: list[float], q: float) -> float:
    if not values:
        return math.nan
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    pos = (len(vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def find_log_lines(session_dirs: set[Path], text: str) -> list[str]:
    out = []
    for session_dir in session_dirs:
        log_dir = session_dir / "logs"
        if not log_dir.exists():
            continue
        for path in log_dir.glob("*"):
            if not path.is_file():
                continue
            try:
                content = path.read_text(errors="replace")
            except OSError:
                continue
            out.extend(f"{path.name}: {line}" for line in content.splitlines() if text in line)
    return out


def wait_for_log_count(
    session_dirs: set[Path],
    text: str,
    target_count: int,
    timeout_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    last = 0
    while time.monotonic() < deadline:
        last = len(find_log_lines(session_dirs, text))
        if last >= target_count:
            return last
        time.sleep(0.1)
    return last


def read_marker(path: Path) -> dict[int, dict[str, list[int]]]:
    """
    Marker rows:
      START,task_index,time_ns,pid
      FINISH,task_index,time_ns,pid
    """
    data: dict[int, dict[str, list[int]]] = {}
    if not path.exists():
        return data

    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(",")
        if len(parts) != 4 or parts[0] not in {"START", "FINISH"}:
            continue
        try:
            event = parts[0]
            idx = int(parts[1])
            t_ns = int(parts[2])
            pid = int(parts[3])
        except ValueError:
            continue
        entry = data.setdefault(idx, {"START": [], "FINISH": [], "PIDS": []})
        entry[event].append(t_ns)
        entry["PIDS"].append(pid)
    return data


def wait_for_started(path: Path, minimum: int, timeout_s: float) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        marker = read_marker(path)
        started = sum(1 for v in marker.values() if v["START"])
        if started >= minimum:
            return started
        time.sleep(0.05)
    marker = read_marker(path)
    return sum(1 for v in marker.values() if v["START"])


def start_cluster(
    case: Case,
    holders: int,
    cpus_per_node: int,
    witness_count: int,
    object_timeout_ms: int,
) -> ClusterLayout:
    cluster = Cluster()
    config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": witness_count,
        "object_timeout_milliseconds": object_timeout_ms,
    }
    if case.enabled:
        config["recovery_succession_target_holder_count"] = holders

    cluster.add_node(num_cpus=0, _system_config=config, include_dashboard=False)

    owner_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"owner_node": 1},
    )

    for rank in range(1, holders + 1):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )

    cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"borrower_node": 1},
    )

    # Extra nodes make witness placement less constrained in the enabled case.
    for i in range(witness_count):
        cluster.add_node(num_cpus=0, resources={f"witness_extra_{i+1}": 1})

    return ClusterLayout(cluster=cluster, owner_node=owner_node)


def wait_for_cluster(expected: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        alive = sum(1 for n in ray.nodes() if n["Alive"])
        if alive >= expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected} nodes became alive")


def make_remote_types(max_concurrency: int):
    @ray.remote(max_retries=2)
    def produce(
        task_index: int,
        duration_s: float,
        payload_bytes: int,
        marker_path: str,
    ) -> bytes:
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"START,{task_index},{time.time_ns()},{os.getpid()}\n")
        time.sleep(duration_s)
        prefix = task_index.to_bytes(8, "little", signed=False)
        value = prefix[:payload_bytes] if payload_bytes <= 8 else prefix + b"x" * (payload_bytes - 8)
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,{task_index},{time.time_ns()},{os.getpid()}\n")
        return value

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, owner_node_id: str):
            self.owner_node_id = owner_node_id

        def dispatch_many(
            self,
            count: int,
            duration_s: float,
            payload_bytes: int,
            marker_path: str,
        ):
            refs = []
            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.owner_node_id,
                soft=True,
            )
            for i in range(count):
                refs.append(
                    produce.options(
                        scheduling_strategy=strategy,
                        num_cpus=1,
                    ).remote(i, duration_s, payload_bytes, marker_path)
                )
            # Nested return preserves ObjectRefs without driver fetching values.
            return refs

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold_many(self, refs):
            self.refs = list(refs)
            return len(self.refs)

        def export_many(self):
            return list(self.refs)

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=max_concurrency)
    class Borrower:
        def hold_many(self, refs):
            self.refs = list(refs)
            return len(self.refs)

        def read(self, index: int, timeout_s: float):
            value = ray.get(self.refs[index], timeout=timeout_s)
            return index, value

    return Owner, Holder, Borrower


def form_many(
    refs,
    holders: list[Any],
    session_dirs: set[Path],
    count: int,
    timeout_s: float,
):
    fresh_refs = refs
    for rank, holder in enumerate(holders, start=1):
        got = ray.get(holder.hold_many.remote(fresh_refs))
        if got != count:
            raise RuntimeError(f"Holder rank {rank} stored {got}/{count} refs")

        needle = (
            "Committed recovery succession manifest after witness publication "
            f"with {rank + 1} total members"
        )
        observed = wait_for_log_count(session_dirs, needle, count, timeout_s)
        if observed < count:
            raise RuntimeError(
                f"Only {observed}/{count} tasks committed holder rank {rank}"
            )
        fresh_refs = ray.get(holder.export_many.remote())
    return fresh_refs


SUMMARY_FIELDS = [
    "trial",
    "config",
    "recovery_enabled",
    "tasks",
    "holders",
    "task_duration_s",
    "payload_bytes",
    "cpus_per_node",
    "formation_success",
    "pre_failure_started",
    "success_count",
    "failure_count",
    "success_rate",
    "failure_to_first_success_s",
    "failure_to_p50_success_s",
    "failure_to_p95_success_s",
    "failure_to_p99_success_s",
    "failure_to_last_success_s",
    "replayed_task_count",
    "tasks_with_gt2_starts",
    "max_starts_for_one_task",
    "mean_starts_per_task",
]

DETAIL_FIELDS = [
    "trial",
    "config",
    "task_index",
    "success",
    "failure_to_completion_s",
    "error_type",
    "starts_observed",
    "finishes_observed",
]


def run_trial(case: Case, args, trial: int):
    summary = {
        "trial": trial,
        "config": case.label,
        "recovery_enabled": int(case.enabled),
        "tasks": args.tasks,
        "holders": args.holders if case.enabled else 0,
        "task_duration_s": args.task_duration,
        "payload_bytes": args.payload_bytes,
        "cpus_per_node": args.cpus_per_node,
        "formation_success": not case.enabled,
        "pre_failure_started": 0,
        "success_count": 0,
        "failure_count": 0,
        "success_rate": 0.0,
        "failure_to_first_success_s": math.nan,
        "failure_to_p50_success_s": math.nan,
        "failure_to_p95_success_s": math.nan,
        "failure_to_p99_success_s": math.nan,
        "failure_to_last_success_s": math.nan,
        "replayed_task_count": 0,
        "tasks_with_gt2_starts": 0,
        "max_starts_for_one_task": 0,
        "mean_starts_per_task": math.nan,
    }

    marker = Path(tempfile.gettempdir()) / f"ray_recovery_storm_{os.getpid()}_{uuid.uuid4().hex}.csv"
    layout = None
    details: list[dict[str, Any]] = []

    try:
        layout = start_cluster(
            case=case,
            holders=args.holders,
            cpus_per_node=args.cpus_per_node,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
        )
        ray.init(address=layout.cluster.address, log_to_driver=False, include_dashboard=False)

        expected = 1 + 1 + args.holders + 1 + args.witness_count
        wait_for_cluster(expected, args.cluster_timeout)

        Owner, Holder, Borrower = make_remote_types(max(args.tasks, 32))

        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote(
            layout.owner_node.node_id
        )
        ray.get(owner.ping.remote())

        holder_actors = [
            Holder.options(resources={f"holder_{rank}": 0.01}, num_cpus=0).remote()
            for rank in range(1, args.holders + 1)
        ]
        if holder_actors:
            ray.get([h.ping.remote() for h in holder_actors])

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        session_dirs = {
            Path(n.get_session_dir_path())
            for n in layout.cluster.list_all_nodes()
        }

        refs = ray.get(
            owner.dispatch_many.remote(
                args.tasks,
                args.task_duration,
                args.payload_bytes,
                str(marker),
            )
        )

        minimum_started = min(args.minimum_started, args.tasks)
        summary["pre_failure_started"] = wait_for_started(
            marker, minimum_started, args.start_timeout
        )

        if case.enabled:
            refs = form_many(
                refs,
                holder_actors,
                session_dirs,
                args.tasks,
                args.formation_timeout,
            )
            summary["formation_success"] = True

        held = ray.get(borrower.hold_many.remote(refs))
        if held != args.tasks:
            raise RuntimeError(f"Borrower retained only {held}/{args.tasks} refs")

        if args.borrower_settle > 0:
            time.sleep(args.borrower_settle)

        failure_t0 = time.perf_counter()
        layout.cluster.remove_node(layout.owner_node, allow_graceful=False)

        pending: dict[Any, tuple[int, float]] = {}
        for i in range(args.tasks):
            started = time.perf_counter()
            fut = borrower.read.remote(i, args.get_timeout)
            pending[fut] = (i, started)

        completion_latencies: list[float] = []

        while pending:
            ready, _ = ray.wait(
                list(pending),
                num_returns=min(len(pending), 64),
                timeout=0.5,
            )
            if not ready:
                if time.perf_counter() - failure_t0 > args.global_timeout:
                    for fut, (idx, _) in list(pending.items()):
                        details.append(
                            {
                                "trial": trial,
                                "config": case.label,
                                "task_index": idx,
                                "success": False,
                                "failure_to_completion_s": args.global_timeout,
                                "error_type": "GLOBAL_TIMEOUT",
                                "starts_observed": 0,
                                "finishes_observed": 0,
                            }
                        )
                    pending.clear()
                    break
                continue

            for fut in ready:
                idx, _request_start = pending.pop(fut)
                done_s = time.perf_counter() - failure_t0
                success = False
                error_type = ""
                try:
                    returned_idx, value = ray.get(fut)
                    expected_prefix = idx.to_bytes(8, "little", signed=False)[: min(8, args.payload_bytes)]
                    if returned_idx != idx or value[: len(expected_prefix)] != expected_prefix:
                        raise RuntimeError("Wrong recovered value")
                    success = True
                    completion_latencies.append(done_s)
                except Exception as exc:
                    error_type = type(exc).__name__

                details.append(
                    {
                        "trial": trial,
                        "config": case.label,
                        "task_index": idx,
                        "success": success,
                        "failure_to_completion_s": done_s,
                        "error_type": error_type,
                        "starts_observed": 0,
                        "finishes_observed": 0,
                    }
                )

        marker_data = read_marker(marker)
        starts_per_task = []
        for row in details:
            entry = marker_data.get(row["task_index"], {"START": [], "FINISH": []})
            starts = len(entry["START"])
            finishes = len(entry["FINISH"])
            row["starts_observed"] = starts
            row["finishes_observed"] = finishes
            starts_per_task.append(starts)

        successes = [r for r in details if r["success"]]
        summary["success_count"] = len(successes)
        summary["failure_count"] = len(details) - len(successes)
        summary["success_rate"] = len(successes) / max(1, args.tasks)

        if completion_latencies:
            summary["failure_to_first_success_s"] = min(completion_latencies)
            summary["failure_to_p50_success_s"] = percentile(completion_latencies, 0.50)
            summary["failure_to_p95_success_s"] = percentile(completion_latencies, 0.95)
            summary["failure_to_p99_success_s"] = percentile(completion_latencies, 0.99)
            summary["failure_to_last_success_s"] = max(completion_latencies)

        summary["replayed_task_count"] = sum(1 for s in starts_per_task if s >= 2)
        summary["tasks_with_gt2_starts"] = sum(1 for s in starts_per_task if s > 2)
        summary["max_starts_for_one_task"] = max(starts_per_task, default=0)
        summary["mean_starts_per_task"] = (
            statistics.fmean(starts_per_task) if starts_per_task else math.nan
        )

        return summary, details

    finally:
        try:
            ray.shutdown()
        except Exception:
            pass
        if layout is not None:
            try:
                layout.cluster.shutdown()
            except Exception:
                pass
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            pass


def write_rows(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("recovery_storm_results.csv"))
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--tasks", type=int, default=16)
    parser.add_argument("--holders", type=int, default=2)
    parser.add_argument("--task-duration", type=float, default=30.0)
    parser.add_argument("--payload-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--cpus-per-node", type=int, default=2)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--object-timeout-ms", type=int, default=100)
    parser.add_argument("--minimum-started", type=int, default=2)
    parser.add_argument("--cluster-timeout", type=float, default=30.0)
    parser.add_argument("--start-timeout", type=float, default=20.0)
    parser.add_argument("--formation-timeout", type=float, default=60.0)
    parser.add_argument("--borrower-settle", type=float, default=0.5)
    parser.add_argument("--get-timeout", type=float, default=120.0)
    parser.add_argument("--global-timeout", type=float, default=150.0)
    parser.add_argument(
        "--systems",
        nargs="+",
        choices=["disabled", "enabled"],
        default=["disabled", "enabled"],
    )
    args = parser.parse_args()

    selected = [
        c for c in CASES if c.label.lower() in set(args.systems)
    ]

    summaries: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []

    total = len(selected) * args.trials
    run_no = 0

    for case in selected:
        for trial in range(1, args.trials + 1):
            run_no += 1
            print(f"\n[{run_no}/{total}] {case.label} trial={trial}")
            summary, detail = run_trial(case, args, trial)
            summaries.append(summary)
            details.extend(detail)

            print(
                f"  success={summary['success_count']}/{args.tasks} "
                f"p95={summary['failure_to_p95_success_s']:.3f}s "
                f"replayed={summary['replayed_task_count']} "
                f"gt2_starts={summary['tasks_with_gt2_starts']}"
            )

            write_rows(args.output, summaries, SUMMARY_FIELDS)
            detail_path = args.output.with_name(args.output.stem + "_objects.csv")
            write_rows(detail_path, details, DETAIL_FIELDS)

    print(f"\nSummary: {args.output}")
    print(f"Per-object detail: {args.output.with_name(args.output.stem + '_objects.csv')}")


if __name__ == "__main__":
    main()
