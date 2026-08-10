#!/usr/bin/env python3
"""
Recovery-succession prototype benchmark: correlated recovery storm.

N independent retryable tasks execute on one owner/producer node. After all required
original tasks have started, recovery holders are formed, a persistent borrower
retains all ObjectRefs, and the owner/producer node is removed. The borrower then
issues one concurrent read per ObjectRef.

Key methodology:
- By default the original node gets N logical CPU slots, so all N original tasks can
  be in flight before failure.
- Holder/borrower nodes keep fixed --cpus-per-node capacity, so larger storms create
  replay queues naturally.
- Replay is identified by START markers after failure injection, not by total START
  count alone.
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

CASES = [Case("Disabled", False), Case("Enabled", True)]

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
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
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
            out.extend(
                f"{path.name}: {line}"
                for line in content.splitlines()
                if text in line
            )
    return out

def wait_for_log_count(
    session_dirs: set[Path], text: str, target_count: int, timeout_s: float
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
    """Rows: START|FINISH,task_index,time_ns,pid."""
    data: dict[int, dict[str, list[int]]] = {}
    if not path.exists():
        return data

    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(",")
        if len(parts) != 4 or parts[0] not in {"START", "FINISH"}:
            continue
        try:
            event, idx, t_ns, pid = parts[0], int(parts[1]), int(parts[2]), int(parts[3])
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
    original_task_slots: int,
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
        num_cpus=max(1, original_task_slots),
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

    for i in range(witness_count):
        cluster.add_node(
            num_cpus=0,
            resources={f"witness_extra_{i + 1}": 1},
        )

    return ClusterLayout(cluster=cluster, owner_node=owner_node)

def wait_for_cluster(expected: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive = 0
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
        value = (
            prefix[:payload_bytes]
            if payload_bytes <= 8
            else prefix + b"x" * (payload_bytes - 8)
        )

        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,{task_index},{time.time_ns()},{os.getpid()}\n")
        return value

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, owner_node_id: str):
            self.owner_node_id = owner_node_id

        def dispatch_many(
            self, count: int, duration_s: float, payload_bytes: int, marker_path: str
        ):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.owner_node_id, soft=True
            )
            return [
                produce.options(
                    scheduling_strategy=strategy,
                    num_cpus=1,
                ).remote(i, duration_s, payload_bytes, marker_path)
                for i in range(count)
            ]

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

    @ray.remote(
        max_restarts=0,
        max_task_retries=0,
        max_concurrency=max_concurrency,
    )
    class Borrower:
        def hold_many(self, refs):
            self.refs = list(refs)
            return len(self.refs)

        def read(self, index: int, timeout_s: float):
            return index, ray.get(self.refs[index], timeout=timeout_s)

        def ping(self):
            return os.getpid()

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

        # Export again so the next holder receives the newest committed manifest.
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
    "original_task_slots",
    "formation_success",
    "pre_failure_started",
    "pre_failure_finished",
    "success_count",
    "failure_count",
    "success_rate",
    "failure_to_first_success_s",
    "failure_to_p50_success_s",
    "failure_to_p95_success_s",
    "failure_to_p99_success_s",
    "failure_to_last_success_s",
    "replayed_task_count",
    "tasks_with_duplicate_replay",
    "max_post_failure_starts_for_one_task",
    "mean_post_failure_starts_per_task",
    "first_replay_start_s",
    "last_replay_start_s",
    "replay_start_spread_s",
    "recovery_throughput_objects_s",
]

DETAIL_FIELDS = [
    "trial",
    "config",
    "tasks",
    "task_index",
    "success",
    "failure_to_completion_s",
    "error_type",
    "starts_observed",
    "finishes_observed",
    "post_failure_starts",
    "first_post_failure_start_s",
]

def detail_row(
    trial: int,
    config: str,
    tasks: int,
    task_index: int,
    success: bool,
    completion_s: float,
    error_type: str,
) -> dict[str, Any]:
    return {
        "trial": trial,
        "config": config,
        "tasks": tasks,
        "task_index": task_index,
        "success": success,
        "failure_to_completion_s": completion_s,
        "error_type": error_type,
        "starts_observed": 0,
        "finishes_observed": 0,
        "post_failure_starts": 0,
        "first_post_failure_start_s": math.nan,
    }

def run_trial(case: Case, args, trial: int):
    original_task_slots = (
        args.original_task_slots if args.original_task_slots > 0 else args.tasks
    )

    summary = {
        "trial": trial,
        "config": case.label,
        "recovery_enabled": int(case.enabled),
        "tasks": args.tasks,
        "holders": args.holders if case.enabled else 0,
        "task_duration_s": args.task_duration,
        "payload_bytes": args.payload_bytes,
        "cpus_per_node": args.cpus_per_node,
        "original_task_slots": original_task_slots,
        "formation_success": not case.enabled,
        "pre_failure_started": 0,
        "pre_failure_finished": 0,
        "success_count": 0,
        "failure_count": 0,
        "success_rate": 0.0,
        "failure_to_first_success_s": math.nan,
        "failure_to_p50_success_s": math.nan,
        "failure_to_p95_success_s": math.nan,
        "failure_to_p99_success_s": math.nan,
        "failure_to_last_success_s": math.nan,
        "replayed_task_count": 0,
        "tasks_with_duplicate_replay": 0,
        "max_post_failure_starts_for_one_task": 0,
        "mean_post_failure_starts_per_task": math.nan,
        "first_replay_start_s": math.nan,
        "last_replay_start_s": math.nan,
        "replay_start_spread_s": math.nan,
        "recovery_throughput_objects_s": math.nan,
    }

    marker = Path(tempfile.gettempdir()) / (
        f"ray_recovery_storm_{os.getpid()}_{uuid.uuid4().hex}.csv"
    )
    layout = None
    details: list[dict[str, Any]] = []

    try:
        layout = start_cluster(
            case=case,
            holders=args.holders,
            cpus_per_node=args.cpus_per_node,
            original_task_slots=original_task_slots,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
        )
        ray.init(
            address=layout.cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        expected = 1 + 1 + args.holders + 1 + args.witness_count
        wait_for_cluster(expected, args.cluster_timeout)

        Owner, Holder, Borrower = make_remote_types(max(args.tasks, 32))

        owner = Owner.options(
            resources={"owner_node": 0.01}, num_cpus=0
        ).remote(layout.owner_node.node_id)
        ray.get(owner.ping.remote())

        holder_actors = [
            Holder.options(
                resources={f"holder_{rank}": 0.01}, num_cpus=0
            ).remote()
            for rank in range(1, args.holders + 1)
        ]
        if holder_actors:
            ray.get([h.ping.remote() for h in holder_actors])

        borrower = Borrower.options(
            resources={"borrower_node": 0.01}, num_cpus=0
        ).remote()
        ray.get(borrower.ping.remote())

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

        minimum_started = (
            args.tasks
            if args.minimum_started <= 0
            else min(args.minimum_started, args.tasks)
        )
        summary["pre_failure_started"] = wait_for_started(
            marker, minimum_started, args.start_timeout
        )
        if summary["pre_failure_started"] < minimum_started:
            raise RuntimeError(
                f"Only {summary['pre_failure_started']}/{minimum_started} "
                "required original tasks started before failure"
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

        pre_failure_marker = read_marker(marker)
        summary["pre_failure_finished"] = sum(
            1 for entry in pre_failure_marker.values() if entry["FINISH"]
        )
        if summary["pre_failure_finished"] != 0:
            raise RuntimeError(
                f"{summary['pre_failure_finished']} original tasks finished before "
                "failure injection. Increase --task-duration."
            )

        # Marker classification and latency use separate clocks.
        failure_wall_ns = time.time_ns()
        failure_t0 = time.perf_counter()
        layout.cluster.remove_node(layout.owner_node, allow_graceful=False)

        # One concurrent read per independent object. This is not a same-object race.
        pending = {
            borrower.read.remote(i, args.get_timeout): i
            for i in range(args.tasks)
        }
        completion_latencies: list[float] = []

        while pending:
            ready, _ = ray.wait(
                list(pending),
                num_returns=min(len(pending), 64),
                timeout=0.5,
            )

            if not ready:
                if time.perf_counter() - failure_t0 > args.global_timeout:
                    for idx in pending.values():
                        details.append(
                            detail_row(
                                trial,
                                case.label,
                                args.tasks,
                                idx,
                                False,
                                args.global_timeout,
                                "GLOBAL_TIMEOUT",
                            )
                        )
                    pending.clear()
                    break
                continue

            for future in ready:
                idx = pending.pop(future)
                done_s = time.perf_counter() - failure_t0
                success = False
                error_type = ""

                try:
                    returned_idx, value = ray.get(future)
                    expected_prefix = idx.to_bytes(
                        8, "little", signed=False
                    )[: min(8, args.payload_bytes)]
                    if (
                        returned_idx != idx
                        or value[: len(expected_prefix)] != expected_prefix
                    ):
                        raise RuntimeError("Wrong recovered value")
                    success = True
                    completion_latencies.append(done_s)
                except Exception as exc:
                    error_type = type(exc).__name__

                details.append(
                    detail_row(
                        trial,
                        case.label,
                        args.tasks,
                        idx,
                        success,
                        done_s,
                        error_type,
                    )
                )

        marker_data = read_marker(marker)
        post_failure_starts_per_task: list[int] = []
        all_post_failure_start_times_ns: list[int] = []

        for row in details:
            entry = marker_data.get(
                row["task_index"],
                {"START": [], "FINISH": [], "PIDS": []},
            )
            starts = entry["START"]
            finishes = entry["FINISH"]
            post_failure_starts = [t for t in starts if t >= failure_wall_ns]

            row["starts_observed"] = len(starts)
            row["finishes_observed"] = len(finishes)
            row["post_failure_starts"] = len(post_failure_starts)

            if post_failure_starts:
                first_ns = min(post_failure_starts)
                row["first_post_failure_start_s"] = max(
                    0.0, (first_ns - failure_wall_ns) / 1e9
                )
                all_post_failure_start_times_ns.extend(post_failure_starts)

            post_failure_starts_per_task.append(len(post_failure_starts))

        successes = [r for r in details if r["success"]]
        summary["success_count"] = len(successes)
        summary["failure_count"] = len(details) - len(successes)
        summary["success_rate"] = len(successes) / max(1, args.tasks)

        if completion_latencies:
            summary["failure_to_first_success_s"] = min(completion_latencies)
            summary["failure_to_p50_success_s"] = percentile(
                completion_latencies, 0.50
            )
            summary["failure_to_p95_success_s"] = percentile(
                completion_latencies, 0.95
            )
            summary["failure_to_p99_success_s"] = percentile(
                completion_latencies, 0.99
            )
            summary["failure_to_last_success_s"] = max(completion_latencies)

        summary["replayed_task_count"] = sum(
            1 for count in post_failure_starts_per_task if count >= 1
        )
        summary["tasks_with_duplicate_replay"] = sum(
            1 for count in post_failure_starts_per_task if count > 1
        )
        summary["max_post_failure_starts_for_one_task"] = max(
            post_failure_starts_per_task, default=0
        )
        summary["mean_post_failure_starts_per_task"] = (
            statistics.fmean(post_failure_starts_per_task)
            if post_failure_starts_per_task
            else math.nan
        )

        if all_post_failure_start_times_ns:
            first_replay_ns = min(all_post_failure_start_times_ns)
            last_replay_ns = max(all_post_failure_start_times_ns)
            summary["first_replay_start_s"] = max(
                0.0, (first_replay_ns - failure_wall_ns) / 1e9
            )
            summary["last_replay_start_s"] = max(
                0.0, (last_replay_ns - failure_wall_ns) / 1e9
            )
            summary["replay_start_spread_s"] = (
                summary["last_replay_start_s"]
                - summary["first_replay_start_s"]
            )

        # Exclude failure-detection delay from this throughput metric.
        if (
            summary["success_count"] > 0
            and not math.isnan(summary["first_replay_start_s"])
            and not math.isnan(summary["failure_to_last_success_s"])
        ):
            replay_window_s = (
                summary["failure_to_last_success_s"]
                - summary["first_replay_start_s"]
            )
            if replay_window_s > 0:
                summary["recovery_throughput_objects_s"] = (
                    summary["success_count"] / replay_window_s
                )

        details.sort(key=lambda r: r["task_index"])
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

def fmt(value: float) -> str:
    return "nan" if value is None or math.isnan(value) else f"{value:.3f}"

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path, default=Path("recovery_storm_results.csv")
    )
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--tasks", type=int, default=16)
    parser.add_argument(
        "--task-counts",
        nargs="+",
        type=int,
        default=None,
        help="Storm-size sweep; overrides --tasks, e.g. 1 4 8 16 32.",
    )
    parser.add_argument("--holders", type=int, default=2)
    parser.add_argument("--task-duration", type=float, default=20.0)
    parser.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument(
        "--cpus-per-node",
        type=int,
        default=2,
        help="Logical CPUs on each surviving holder/borrower node.",
    )
    parser.add_argument(
        "--original-task-slots",
        type=int,
        default=0,
        help="Logical CPUs on original node; 0 means one slot per storm task.",
    )
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--object-timeout-ms", type=int, default=100)
    parser.add_argument(
        "--minimum-started",
        type=int,
        default=0,
        help="Required original STARTs before failure; 0 means all tasks.",
    )
    parser.add_argument("--cluster-timeout", type=float, default=30.0)
    parser.add_argument("--start-timeout", type=float, default=30.0)
    parser.add_argument("--formation-timeout", type=float, default=60.0)
    parser.add_argument("--borrower-settle", type=float, default=0.5)
    parser.add_argument("--get-timeout", type=float, default=180.0)
    parser.add_argument("--global-timeout", type=float, default=240.0)
    parser.add_argument(
        "--systems",
        nargs="+",
        choices=["disabled", "enabled"],
        default=["disabled", "enabled"],
    )
    args = parser.parse_args()

    if args.trials <= 0:
        parser.error("--trials must be > 0")
    if args.holders <= 0:
        parser.error("--holders must be > 0")
    if args.cpus_per_node <= 0:
        parser.error("--cpus-per-node must be > 0")
    if args.original_task_slots < 0:
        parser.error("--original-task-slots must be >= 0")
    if args.task_duration <= 0:
        parser.error("--task-duration must be > 0")
    if args.payload_bytes <= 0:
        parser.error("--payload-bytes must be > 0")
    if args.witness_count < 0:
        parser.error("--witness-count must be >= 0")

    task_counts = args.task_counts if args.task_counts else [args.tasks]
    if any(n <= 0 for n in task_counts):
        parser.error("All task counts must be > 0")

    selected_names = set(args.systems)
    selected = [c for c in CASES if c.label.lower() in selected_names]

    summaries: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    detail_path = args.output.with_name(args.output.stem + "_objects.csv")

    total = len(task_counts) * len(selected) * args.trials
    run_no = 0

    for task_count in task_counts:
        args.tasks = task_count
        for case in selected:
            for trial in range(1, args.trials + 1):
                run_no += 1
                original_slots = (
                    args.original_task_slots
                    if args.original_task_slots > 0
                    else task_count
                )
                print(
                    f"\n[{run_no}/{total}] tasks={task_count} {case.label} "
                    f"trial={trial} original_slots={original_slots} "
                    f"survivor_cpus_per_node={args.cpus_per_node}"
                )

                summary, detail = run_trial(case, args, trial)
                summaries.append(summary)
                details.extend(detail)

                print(
                    f"  success={summary['success_count']}/{task_count} "
                    f"success_rate={summary['success_rate']:.3f} "
                    f"p95={fmt(summary['failure_to_p95_success_s'])}s "
                    f"last={fmt(summary['failure_to_last_success_s'])}s "
                    f"replayed={summary['replayed_task_count']} "
                    f"duplicates={summary['tasks_with_duplicate_replay']} "
                    f"first_replay={fmt(summary['first_replay_start_s'])}s "
                    f"spread={fmt(summary['replay_start_spread_s'])}s "
                    f"recovery_rate={fmt(summary['recovery_throughput_objects_s'])} obj/s"
                )

                write_rows(args.output, summaries, SUMMARY_FIELDS)
                write_rows(detail_path, details, DETAIL_FIELDS)

    print(f"\nSummary: {args.output}")
    print(f"Per-object detail: {detail_path}")

if __name__ == "__main__":
    main()