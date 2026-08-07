#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@dataclass(frozen=True)
class Case:
    label: str
    enabled: bool
    holders: int


CASES = [
    Case("Disabled", False, 0),
    Case("Enabled-1-holder", True, 1),
    Case("Enabled-2-holders", True, 2),
    Case("Enabled-3-holders", True, 3),
    Case("Enabled-4-holders", True, 4),
]


def find_log_lines(session_dirs: set[Path], text: str) -> list[str]:
    out: list[str] = []
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
            for line in content.splitlines():
                if text in line:
                    out.append(f"{path.name}: {line}")
    return out


def wait_for_log_line(
    session_dirs: set[Path], text: str, timeout_s: float
) -> list[str]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        matches = find_log_lines(session_dirs, text)
        if matches:
            return matches
        time.sleep(0.05)
    return []


def print_recovery_diagnostics(session_dirs: set[Path]) -> None:
    terms = [
        "Committed recovery succession manifest",
        "Applied committed recovery succession manifest",
        "Stored provisional recovery holder",
        "Failed to commit recovery manifest",
        "OWNER_DIED observed but no borrowed recovery succession plan was found",
        "OWNER_DIED intercepted",
        "Preparing recovery succession replay attempt",
        "Promoted borrowed object to owned recovery return",
        "Recovery succession replay accepted",
        "Recovery succession accepted by holder",
        "future resolution restarted against acting holder",
        "Submitting recovery succession replay",
        "Skipping known-dead recovery holder",
        "Confirmed stale local OWNER_DIED",
        "Trying to put an object that already existed in plasma",
        "Failed to handle task return",
        "Completing task",
        "finished from worker",
        "Task dependencies resolved",
        "Requesting lease",
        "Lease granted",
        "Pushing task",
    ]
    seen: set[str] = set()
    for term in terms:
        for line in find_log_lines(session_dirs, term):
            if line not in seen:
                seen.add(line)
                print(f"      {line}")


def start_cluster(
    case: Case,
    cpus_per_node: int,
    object_timeout_ms: int,
) -> tuple[Cluster, Any]:
    cluster = Cluster()

    config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": 2,
        "object_timeout_milliseconds": object_timeout_ms,
    }
    if case.enabled:
        config["recovery_succession_target_holder_count"] = case.holders

    # Head + driver only. This node survives the failure.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    # Owner + original producer. This entire logical node is failed.
    failure_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"owner_node": 1},
    )

    # Four distinct backup nodes. Holder actors consume no CPU so replay can
    # use the node CPU after failure.
    for rank in range(1, 5):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )

    # Dedicated requester/borrower node.
    cluster.add_node(
        num_cpus=0,
        resources={"borrower_node": 1},
    )

    return cluster, failure_node


def wait_for_cluster(expected_nodes: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive_count = 0
    while time.monotonic() < deadline:
        alive_count = len([n for n in ray.nodes() if n["Alive"]])
        if alive_count >= expected_nodes:
            return
        time.sleep(0.1)
    raise TimeoutError(
        f"Only {alive_count}/{expected_nodes} logical Ray nodes became alive"
    )


def read_attempts(path: Path) -> list[tuple[str, int, int, int]]:
    """
    Read producer execution markers.

    Each entry is:
      (event, timestamp_ns, pid, seed)

    event is either START or FINISH.
    """
    if not path.exists():
        return []

    out: list[tuple[str, int, int, int]] = []

    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []

    for line in lines:
        parts = line.split(",")

        if len(parts) != 4:
            continue

        event = parts[0]

        if event not in {"START", "FINISH"}:
            continue

        try:
            timestamp_ns = int(parts[1])
            pid = int(parts[2])
            seed = int(parts[3])

            out.append(
                (
                    event,
                    timestamp_ns,
                    pid,
                    seed,
                )
            )
        except ValueError:
            pass

    return out


def wait_for_first_attempt(path: Path, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        events = read_attempts(path)

        if any(event[0] == "START" for event in events):
            return

        time.sleep(0.01)

    raise TimeoutError("Original producer task did not begin execution")


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        seed: int,
        task_duration_s: float,
        payload_bytes: int,
        marker_path: str,
    ) -> bytes:
        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"START,{time.time_ns()},{os.getpid()},{seed}\n"
            )
            f.flush()

        time.sleep(task_duration_s)

        prefix = seed.to_bytes(8, "little", signed=False)

        if payload_bytes <= 8:
            result = prefix[:payload_bytes]
        else:
            result = prefix + b"x" * (payload_bytes - 8)

        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"FINISH,{time.time_ns()},{os.getpid()},{seed}\n"
            )
            f.flush()

        return result

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, failure_node_id: str):
            self.failure_node_id = failure_node_id

        def ping(self) -> int:
            return os.getpid()

        def dispatch(
            self,
            seed: int,
            task_duration_s: float,
            payload_bytes: int,
            marker_path: str,
        ):
            # Soft affinity keeps the original task in the owner's failure
            # domain, but lets the recovered replay run elsewhere after that
            # node disappears.
            ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.failure_node_id,
                    soft=True,
                ),
                num_cpus=1,
            ).remote(
                seed,
                task_duration_s,
                payload_bytes,
                marker_path,
            )
            return [ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            return [self.ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def read(self, timeout_s: float):
            return ray.get(self.ref, timeout=timeout_s)

    return Owner, Holder, Borrower


def wait_for_holder_commit(
    session_dirs: set[Path],
    non_owner_holders: int,
    timeout_s: float,
) -> None:
    total_members = non_owner_holders + 1
    needle = (
        "Committed recovery succession manifest after witness publication "
        f"with {total_members} total members"
    )
    if not wait_for_log_line(session_dirs, needle, timeout_s):
        raise RuntimeError(
            f"FORMATION_FAILED: holder rank {non_owner_holders} did not commit"
        )


def form_succession(
    case: Case,
    initial_ref,
    holder_actors: dict[int, Any],
    session_dirs: set[Path],
    timeout_s: float,
):
    if not case.enabled:
        return initial_ref, 0.0

    t0 = time.perf_counter()
    fresh_ref = initial_ref

    for rank in range(1, case.holders + 1):
        holder = holder_actors[rank]
        if not ray.get(holder.hold.remote([fresh_ref])):
            raise RuntimeError(f"FORMATION_FAILED: holder rank {rank} hold failed")

        wait_for_holder_commit(session_dirs, rank, timeout_s)

        # Re-export from the newly committed holder so the next hop receives
        # the latest manifest generation.
        fresh_ref = ray.get(holder.export.remote())[0]

    return fresh_ref, time.perf_counter() - t0


FIELDS = [
    "config",
    "recovery_enabled",
    "target_non_owner_holders",
    "task_duration_s",
    "payload_bytes",
    "trial",
    "object_timeout_ms",
    "formation_success",
    "formation_time_s",
    "success",
    "replayed",
    "executions_observed",
    "replay_finished",
    "original_producer_pid",
    "producer_alive_after_failure",
    "failure_injection_s",
    "failure_to_replay_start_s",
    "failure_to_replay_finish_s",
    "failure_to_result_s",
    "dispatch_to_failure_s",
    "dispatch_to_result_s",
    "error_type",
    "error_message",
]


def run_trial(
    *,
    case: Case,
    task_duration_s: float,
    payload_bytes: int,
    trial: int,
    cpus_per_node: int,
    object_timeout_ms: int,
    cluster_timeout_s: float,
    start_timeout_s: float,
    formation_timeout_s: float,
    borrower_settle_s: float,
    get_timeout_s: float,
) -> dict[str, Any]:
    marker = Path(tempfile.gettempdir()) / (
        f"ray_recovery_bench_{os.getpid()}_{uuid.uuid4().hex}.txt"
    )

    seed = (
        trial * 1_000_000
        + int(task_duration_s * 1000)
        + case.holders * 100
    )

    row: dict[str, Any] = {
        "config": case.label,
        "recovery_enabled": int(case.enabled),
        "target_non_owner_holders": case.holders,
        "task_duration_s": task_duration_s,
        "payload_bytes": payload_bytes,
        "trial": trial,
        "object_timeout_ms": object_timeout_ms,
        "formation_success": False,
        "formation_time_s": math.nan,
        "success": False,
        "replayed": False,
        "executions_observed": 0,
        "replay_finished": False,
        "original_producer_pid": -1,
        "producer_alive_after_failure": False,
        "failure_injection_s": math.nan,
        "failure_to_replay_start_s": math.nan,
        "failure_to_replay_finish_s": math.nan,
        "failure_to_result_s": math.nan,
        "dispatch_to_failure_s": math.nan,
        "dispatch_to_result_s": math.nan,
        "error_type": "",
        "error_message": "",
    }

    cluster = None
    failure_node = None
    owner = None
    session_dirs: set[Path] = set()
    dispatch_start_ns = 0
    failure_start_perf_ns = 0
    failure_wall_ns = 0

    try:
        cluster, failure_node = start_cluster(
            case,
            cpus_per_node,
            object_timeout_ms,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        wait_for_cluster(7, cluster_timeout_s)

        Owner, Holder, Borrower = make_remote_types()

        holder_actors = {
            rank: Holder.options(
                resources={f"holder_{rank}": 0.01},
                num_cpus=0,
            ).remote()
            for rank in range(1, 5)
        }

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        failure_node_id = failure_node.node_id

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(failure_node_id)
        ray.get(owner.ping.remote())

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        dispatch_start_ns = time.perf_counter_ns()

        result_ref = ray.get(
            owner.dispatch.remote(
                seed,
                task_duration_s,
                payload_bytes,
                str(marker),
            )
        )[0]

        wait_for_first_attempt(marker, start_timeout_s)

        events = read_attempts(marker)
        start_events = [
            event
            for event in events
            if event[0] == "START"
        ]

        if start_events:
            row["original_producer_pid"] = start_events[0][2]

        result_ref, formation_s = form_succession(
            case,
            result_ref,
            holder_actors,
            session_dirs,
            formation_timeout_s,
        )
        row["formation_success"] = True
        row["formation_time_s"] = formation_s

        ray.get(borrower.hold.remote([result_ref]))

        if borrower_settle_s > 0:
            time.sleep(borrower_settle_s)

        ready, _ = ray.wait([result_ref], timeout=0)
        if ready:
            raise RuntimeError(
                "FINISHED_BEFORE_FAILURE: increase task duration or reduce "
                "formation/settle time"
            )

        failure_start_perf_ns = time.perf_counter_ns()
        failure_wall_ns = time.time_ns()

        cluster.remove_node(
            failure_node,
            allow_graceful=False,
        )

        failure_done_ns = time.perf_counter_ns()
        row["failure_injection_s"] = (
            failure_done_ns - failure_start_perf_ns
        ) / 1e9
        row["dispatch_to_failure_s"] = (
            failure_start_perf_ns - dispatch_start_ns
        ) / 1e9

        producer_pid = row["original_producer_pid"]
        if producer_pid > 0:
            try:
                os.kill(producer_pid, 0)
                row["producer_alive_after_failure"] = True
            except OSError:
                row["producer_alive_after_failure"] = False

        try:
            value = ray.get(
                borrower.read.remote(get_timeout_s),
                timeout=get_timeout_s + 10.0,
            )
            result_ns = time.perf_counter_ns()

            expected = seed.to_bytes(8, "little", signed=False)[
                : min(payload_bytes, 8)
            ]
            if value[: len(expected)] != expected:
                raise RuntimeError("Recovered payload validation failed")

            row["success"] = True
            row["failure_to_result_s"] = (
                result_ns - failure_start_perf_ns
            ) / 1e9
            row["dispatch_to_result_s"] = (
                result_ns - dispatch_start_ns
            ) / 1e9

        except Exception as exc:
            result_ns = time.perf_counter_ns()
            row["failure_to_result_s"] = (
                result_ns - failure_start_perf_ns
            ) / 1e9
            row["dispatch_to_result_s"] = (
                result_ns - dispatch_start_ns
            ) / 1e9
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)

        events = read_attempts(marker)

        start_events = [
            event
            for event in events
            if event[0] == "START"
        ]

        finish_events = [
            event
            for event in events
            if event[0] == "FINISH"
        ]

        row["executions_observed"] = len(start_events)

        post_failure_starts = [
            event
            for event in start_events
            if event[1] > failure_wall_ns
        ]

        post_failure_finishes = [
            event
            for event in finish_events
            if event[1] > failure_wall_ns
        ]

        if post_failure_starts:
            row["replayed"] = True

            replay_wall_ns = min(
                event[1]
                for event in post_failure_starts
            )

            row["failure_to_replay_start_s"] = (
                replay_wall_ns - failure_wall_ns
            ) / 1e9

        if post_failure_finishes:
            row["replay_finished"] = True

            replay_finish_wall_ns = min(
                event[1]
                for event in post_failure_finishes
            )

            row["failure_to_replay_finish_s"] = (
                replay_finish_wall_ns - failure_wall_ns
            ) / 1e9

        if post_failure_starts and not post_failure_finishes:
            print(
                "    Replay started after failure but "
                "no replay FINISH marker was observed."
            )

        if row["success"] and not row["replayed"]:
            row["error_type"] = "NoReplayObserved"
            row["error_message"] = (
                "Result succeeded but no post-failure replay marker was observed"
            )

        if row["success"] and not row["replay_finished"]:
            row["error_type"] = "ReplayDidNotFinish"
            row["error_message"] = (
                "Result succeeded but no post-failure FINISH marker was observed"
            )

        if not row["success"]:
            print("    Recovery diagnostics:")
            print_recovery_diagnostics(session_dirs)

        return row

    except Exception as exc:
        if not row["error_type"]:
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)

        events = read_attempts(marker)

        start_events = [
            event
            for event in events
            if event[0] == "START"
        ]

        finish_events = [
            event
            for event in events
            if event[0] == "FINISH"
        ]

        row["executions_observed"] = len(start_events)

        if failure_wall_ns:
            post_failure_starts = [
                event
                for event in start_events
                if event[1] > failure_wall_ns
            ]

            post_failure_finishes = [
                event
                for event in finish_events
                if event[1] > failure_wall_ns
            ]

            if post_failure_starts:
                row["replayed"] = True

                replay_wall_ns = min(
                    event[1]
                    for event in post_failure_starts
                )

                row["failure_to_replay_start_s"] = (
                    replay_wall_ns - failure_wall_ns
                ) / 1e9

            if post_failure_finishes:
                row["replay_finished"] = True

                replay_finish_wall_ns = min(
                    event[1]
                    for event in post_failure_finishes
                )

                row["failure_to_replay_finish_s"] = (
                    replay_finish_wall_ns - failure_wall_ns
                ) / 1e9

        if session_dirs:
            print("    Trial diagnostics:")
            print_recovery_diagnostics(session_dirs)

        return row

    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

        if cluster is not None:
            try:
                cluster.shutdown()
            except Exception:
                pass

        try:
            marker.unlink(missing_ok=True)
        except Exception:
            pass


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, default=Path("recovery_time_results.csv"))
    p.add_argument("--trials", type=int, default=3)
    p.add_argument(
        "--task-durations",
        type=float,
        nargs="+",
        default=[5, 10, 20, 30],
    )
    p.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    p.add_argument("--cpus-per-node", type=int, default=1)
    p.add_argument("--object-timeout-ms", type=int, default=200)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--start-timeout-seconds", type=float, default=15.0)
    p.add_argument("--formation-timeout-seconds", type=float, default=20.0)
    p.add_argument("--borrower-settle-seconds", type=float, default=0.5)
    p.add_argument("--get-timeout-seconds", type=float, default=120.0)
    p.add_argument("--seed", type=int, default=20260806)
    p.add_argument("--enabled-only", action="store_true")
    p.add_argument("--fixed-order", action="store_true")
    args = p.parse_args()

    cases = [c for c in CASES if not args.enabled_only or c.enabled]

    specs = [
        (case, duration, trial)
        for case in cases
        for duration in args.task_durations
        for trial in range(1, args.trials + 1)
    ]

    if not args.fixed_order:
        random.Random(args.seed).shuffle(specs)

    rows: list[dict[str, Any]] = []

    for i, (case, duration, trial) in enumerate(specs, 1):
        print(
            f"\n{'=' * 76}\n"
            f"Run {i}/{len(specs)} | {case.label} | "
            f"duration={duration:g}s | trial={trial}/{args.trials}\n"
            f"{'=' * 76}",
            flush=True,
        )

        row = run_trial(
            case=case,
            task_duration_s=duration,
            payload_bytes=args.payload_bytes,
            trial=trial,
            cpus_per_node=args.cpus_per_node,
            object_timeout_ms=args.object_timeout_ms,
            cluster_timeout_s=args.cluster_timeout_seconds,
            start_timeout_s=args.start_timeout_seconds,
            formation_timeout_s=args.formation_timeout_seconds,
            borrower_settle_s=args.borrower_settle_seconds,
            get_timeout_s=args.get_timeout_seconds,
        )

        rows.append(row)
        write_csv(args.output, rows)

        print(
            "  "
            f"formation={row['formation_success']} "
            f"success={row['success']} "
            f"replayed={row['replayed']} "
            f"replay_finished={row['replay_finished']} "
            f"executions={row['executions_observed']} "
            f"producer_alive={row['producer_alive_after_failure']} "
            f"failure->replay={row['failure_to_replay_start_s']} "
            f"failure->replay_finish={row['failure_to_replay_finish_s']} "
            f"failure->result={row['failure_to_result_s']} "
            f"error={row['error_type'] or '-'}",
            flush=True,
        )

    print(f"\nWrote {args.output.resolve()}")


if __name__ == "__main__":
    main()