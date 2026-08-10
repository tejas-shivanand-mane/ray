#!/usr/bin/env python3
"""
Diagnostic prototype benchmark: worker failure vs node failure.

Failure modes:
  1. owner_worker:
       kill only the owner actor process; producer runs on a separate node.
  2. owner_node:
       remove the owner's node; producer still runs on a separate node.
  3. owner_plus_producer_node:
       co-locate owner and producer, then remove that node.

This version keeps terminal output compact and adds structured recovery-path
diagnostics to the CSV so a failed run shows where recovery stopped.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Keep backend INFO logs because the recovery implementation currently emits
# the diagnostic events we need at INFO level. They are read from Ray log files,
# not streamed to the driver.
os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


FAILURE_MODES = [
    "owner_worker",
    "owner_node",
    "owner_plus_producer_node",
]

# Compact recovery-path signals already emitted by the current C++ code.
DIAGNOSTIC_TERMS = {
    "owner_died_observed": "OWNER_DIED observed",
    "owner_died_intercepted": "OWNER_DIED intercepted",
    "owner_died_no_plan": (
        "OWNER_DIED observed but no borrowed recovery succession plan was found"
    ),
    "prepare_replay": "Preparing recovery succession replay attempt",
    "holder_accepted": "Recovery succession accepted by holder",
    "replay_accepted": "Recovery succession replay accepted",
    "replay_submitted": "Submitting recovery succession replay",
    "future_resolution_restarted": (
        "future resolution restarted against acting holder"
    ),
    "skipped_dead_holder": "Skipping known-dead recovery holder",
    "stale_owner_died_confirmed": "Confirmed stale local OWNER_DIED",
    "stale_owner_died_ignored": (
    "Ignored stale OWNER_DIED while recovery replay is in progress"
),
"soft_affinity_cleared": (
    "Cleared soft node affinity for recovery succession replay"
),
}


@dataclass
class ClusterLayout:
    cluster: Cluster
    owner_node: Any
    producer_node: Any | None


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
    session_dirs: set[Path],
    text: str,
    timeout_s: float,
) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if find_log_lines(session_dirs, text):
            return True
        time.sleep(0.05)
    return False


def collect_diagnostics(
    session_dirs: set[Path],
) -> tuple[dict[str, bool], dict[str, list[str]]]:
    flags: dict[str, bool] = {}
    lines: dict[str, list[str]] = {}
    for key, text in DIAGNOSTIC_TERMS.items():
        matches = find_log_lines(session_dirs, text)
        flags[key] = bool(matches)
        lines[key] = matches
    return flags, lines


def classify_stage(
    flags: dict[str, bool],
    replay_started: bool,
    success: bool,
) -> str:
    if success:
        return "success"
    if flags["owner_died_no_plan"]:
        return "owner_died_no_borrowed_plan"
    if not flags["owner_died_observed"] and not flags["owner_died_intercepted"]:
        return "owner_died_not_observed"
    if not flags["owner_died_intercepted"]:
        return "owner_died_not_intercepted"
    if not flags["prepare_replay"]:
        return "intercepted_no_replay_preparation"
    if not flags["replay_accepted"]:
        return "replay_prepared_not_accepted"
    if not flags["holder_accepted"]:
        return "replay_accepted_but_borrower_no_holder_accept"
    if not flags["replay_submitted"]:
        return "holder_accepted_but_replay_not_submitted"
    if not replay_started:
        return "replay_submitted_but_execution_not_started"
    return "replay_started_but_result_failed"


def one_line_error(exc: Exception, limit: int = 240) -> str:
    text = " ".join(str(exc).split())
    return text[:limit]


def read_marker(path: Path):
    out = []
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(",")
        if len(parts) != 3 or parts[0] not in {"START", "FINISH"}:
            continue
        try:
            out.append((parts[0], int(parts[1]), int(parts[2])))
        except ValueError:
            pass
    return out


def wait_for_first_start(path: Path, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        starts = [e for e in read_marker(path) if e[0] == "START"]
        if starts:
            return starts[0]
        time.sleep(0.02)
    raise TimeoutError("Original producer did not start")


def wait_for_second_start(path: Path, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        starts = [e for e in read_marker(path) if e[0] == "START"]
        if len(starts) >= 2:
            return starts[1]
        time.sleep(0.02)
    return None


def start_cluster(
    mode: str,
    holders: int,
    cpus_per_node: int,
    witness_count: int,
    object_timeout_ms: int,
) -> ClusterLayout:
    cluster = Cluster()
    config = {
        "enable_recovery_succession": True,
        "recovery_succession_target_holder_count": holders,
        "recovery_succession_witness_count": witness_count,
        "object_timeout_milliseconds": object_timeout_ms,
    }

    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    owner_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"owner_node": 1},
    )

    producer_node = None
    if mode != "owner_plus_producer_node":
        producer_node = cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={"producer_node": 1},
        )

    for rank in range(1, holders + 1):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )

    cluster.add_node(
        num_cpus=1,
        resources={"borrower_node": 1},
    )

    for i in range(witness_count):
        cluster.add_node(
            num_cpus=0,
            resources={f"extra_witness_{i + 1}": 1},
        )

    return ClusterLayout(
        cluster=cluster,
        owner_node=owner_node,
        producer_node=producer_node,
    )


def wait_for_cluster(expected: int, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    alive = 0
    while time.monotonic() < deadline:
        alive = sum(1 for n in ray.nodes() if n["Alive"])
        if alive >= expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected} logical nodes became alive")


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        seed: int,
        duration_s: float,
        payload_bytes: int,
        marker_path: str,
    ):
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()}\n")

        time.sleep(duration_s)

        prefix = seed.to_bytes(8, "little", signed=False)
        value = (
            prefix[:payload_bytes]
            if payload_bytes <= 8
            else prefix + b"x" * (payload_bytes - 8)
        )

        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()}\n")

        return value

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, producer_node_id: str):
            self.producer_node_id = producer_node_id

        def dispatch(
            self,
            seed: int,
            duration_s: float,
            payload_bytes: int,
            marker_path: str,
        ):
            ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.producer_node_id,
                    soft=True,
                ),
                num_cpus=1,
            ).remote(
                seed,
                duration_s,
                payload_bytes,
                marker_path,
            )
            return [ref]

        def crash(self):
            os._exit(66)

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            return [self.ref]

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=2)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def read(self, timeout_s: float):
            return ray.get(self.ref, timeout=timeout_s)

    return Owner, Holder, Borrower


def form_succession(
    ref,
    holders,
    session_dirs: set[Path],
    timeout_s: float,
):
    fresh = ref
    for rank, holder in enumerate(holders, start=1):
        ray.get(holder.hold.remote([fresh]))

        needle = (
            "Committed recovery succession manifest after witness publication "
            f"with {rank + 1} total members"
        )
        if not wait_for_log_line(session_dirs, needle, timeout_s):
            raise RuntimeError(f"Holder rank {rank} did not commit")

        fresh = ray.get(holder.export.remote())[0]

    return fresh


DIAGNOSTIC_FLAG_FIELDS = list(DIAGNOSTIC_TERMS.keys())

FIELDS = [
    "trial",
    "failure_mode",
    "holders",
    "task_duration_s",
    "payload_bytes",
    "formation_success",
    "success",
    "executions_observed",
    "replayed",
    "failure_to_replay_start_s",
    "failure_to_result_s",
    "original_task_finished_after_owner_failure",
    "diagnostic_stage",
    *DIAGNOSTIC_FLAG_FIELDS,
    "error_type",
    "error_message",
]


def run_trial(
    mode: str,
    args,
    trial: int,
) -> tuple[dict[str, Any], dict[str, list[str]]]:
    row: dict[str, Any] = {
        "trial": trial,
        "failure_mode": mode,
        "holders": args.holders,
        "task_duration_s": args.task_duration,
        "payload_bytes": args.payload_bytes,
        "formation_success": False,
        "success": False,
        "executions_observed": 0,
        "replayed": False,
        "failure_to_replay_start_s": math.nan,
        "failure_to_result_s": math.nan,
        "original_task_finished_after_owner_failure": False,
        "diagnostic_stage": "not_collected",
        "error_type": "",
        "error_message": "",
    }
    for key in DIAGNOSTIC_FLAG_FIELDS:
        row[key] = False

    marker = (
        Path(tempfile.gettempdir())
        / f"ray_failure_type_{os.getpid()}_{uuid.uuid4().hex}.csv"
    )

    layout: ClusterLayout | None = None
    session_dirs: set[Path] = set()
    diagnostic_lines: dict[str, list[str]] = {
        key: [] for key in DIAGNOSTIC_FLAG_FIELDS
    }

    try:
        layout = start_cluster(
            mode,
            args.holders,
            args.cpus_per_node,
            args.witness_count,
            args.object_timeout_ms,
        )

        # Keep Python-side Ray output quiet. C++ INFO logs still go to files,
        # which is what collect_diagnostics() reads.
        ray.init(
            address=layout.cluster.address,
            log_to_driver=False,
            include_dashboard=False,
            logging_level=logging.ERROR,
        )

        producer_extra = 0 if mode == "owner_plus_producer_node" else 1
        expected = (
            1  # head
            + 1  # owner
            + producer_extra
            + args.holders
            + 1  # borrower
            + args.witness_count
        )
        wait_for_cluster(expected, args.cluster_timeout)

        Owner, Holder, Borrower = make_remote_types()

        producer_node_id = (
            layout.owner_node.node_id
            if mode == "owner_plus_producer_node"
            else layout.producer_node.node_id
        )

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(producer_node_id)
        ray.get(owner.ping.remote())

        holders = [
            Holder.options(
                resources={f"holder_{rank}": 0.01},
                num_cpus=0,
            ).remote()
            for rank in range(1, args.holders + 1)
        ]
        ray.get([h.ping.remote() for h in holders])

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        session_dirs = {
            Path(n.get_session_dir_path())
            for n in layout.cluster.list_all_nodes()
        }

        seed = trial * 100 + FAILURE_MODES.index(mode)

        ref = ray.get(
            owner.dispatch.remote(
                seed,
                args.task_duration,
                args.payload_bytes,
                str(marker),
            )
        )[0]

        first_start = wait_for_first_start(
            marker,
            args.start_timeout,
        )

        ref = form_succession(
            ref,
            holders,
            session_dirs,
            args.formation_timeout,
        )
        row["formation_success"] = True

        ray.get(borrower.hold.remote([ref]))

        if args.borrower_settle > 0:
            time.sleep(args.borrower_settle)

        failure_wall_ns = time.time_ns()
        failure_perf = time.perf_counter()

        if mode == "owner_worker":
            try:
                ray.get(
                    owner.crash.remote(),
                    timeout=5,
                )
            except Exception:
                pass
        elif mode in {
            "owner_node",
            "owner_plus_producer_node",
        }:
            layout.cluster.remove_node(
                layout.owner_node,
                allow_graceful=False,
            )
        else:
            raise ValueError(mode)

        try:
            value = ray.get(
                borrower.read.remote(args.get_timeout),
                timeout=args.get_timeout + 10,
            )

            row["failure_to_result_s"] = (
                time.perf_counter() - failure_perf
            )

            expected_prefix = seed.to_bytes(
                8,
                "little",
                signed=False,
            )[: min(8, args.payload_bytes)]

            if value[: len(expected_prefix)] != expected_prefix:
                raise RuntimeError("Recovered value is incorrect")

            row["success"] = True

        except Exception as exc:
            row["failure_to_result_s"] = (
                time.perf_counter() - failure_perf
            )
            row["error_type"] = type(exc).__name__
            row["error_message"] = one_line_error(exc)

        # Give replay marker and C++ file logs a short chance to flush.
        second_start = wait_for_second_start(
            marker,
            timeout_s=args.replay_marker_settle,
        )

        if second_start is not None:
            row["failure_to_replay_start_s"] = max(
                0.0,
                (second_start[1] - failure_wall_ns) / 1e9,
            )

        events = read_marker(marker)
        starts = [e for e in events if e[0] == "START"]
        finishes = [e for e in events if e[0] == "FINISH"]

        row["executions_observed"] = len(starts)
        row["replayed"] = len(starts) >= 2

        if mode != "owner_plus_producer_node" and finishes:
            original_pid = first_start[2]
            row["original_task_finished_after_owner_failure"] = any(
                e[2] == original_pid and e[1] > failure_wall_ns
                for e in finishes
            )

        if args.diagnostic_settle > 0:
            time.sleep(args.diagnostic_settle)

        flags, diagnostic_lines = collect_diagnostics(
            session_dirs,
        )

        for key, value in flags.items():
            row[key] = value

        row["diagnostic_stage"] = classify_stage(
            flags,
            replay_started=row["replayed"],
            success=row["success"],
        )

        return row, diagnostic_lines

    except Exception as exc:
        if not row["error_type"]:
            row["error_type"] = type(exc).__name__
            row["error_message"] = one_line_error(exc)

        if session_dirs:
            flags, diagnostic_lines = collect_diagnostics(
                session_dirs,
            )
            for key, value in flags.items():
                row[key] = value

            row["diagnostic_stage"] = classify_stage(
                flags,
                replay_started=row["replayed"],
                success=row["success"],
            )
        else:
            row["diagnostic_stage"] = "setup_failed"

        return row, diagnostic_lines

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


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerows(rows)


def print_compact_result(
    row: dict[str, Any],
    diagnostic_lines: dict[str, list[str]],
    *,
    show_log_lines: bool,
) -> None:
    print(
        "  "
        f"formation={row['formation_success']} "
        f"success={row['success']} "
        f"executions={row['executions_observed']} "
        f"replayed={row['replayed']} "
        f"replay_start={row['failure_to_replay_start_s']} "
        f"result={row['failure_to_result_s']:.3f}s "
        f"original_finished={row['original_task_finished_after_owner_failure']}"
        f"stale_filtered={int(row['stale_owner_died_ignored'])} "
        f"affinity_cleared={int(row['soft_affinity_cleared'])}"
    )

    # Only show the recovery-path detail when a trial fails.
    if not row["success"]:
        print(f"  recovery_path={row['diagnostic_stage']}")
        print(
            "  flags: "
            f"observed={int(row['owner_died_observed'])} "
            f"intercepted={int(row['owner_died_intercepted'])} "
            f"no_plan={int(row['owner_died_no_plan'])} "
            f"prepare={int(row['prepare_replay'])} "
            f"holder={int(row['holder_accepted'])} "
            f"accepted={int(row['replay_accepted'])} "
            f"submitted={int(row['replay_submitted'])} "
            f"future_restart={int(row['future_resolution_restarted'])}"
        )

        if row["error_type"]:
            print(
                f"  error={row['error_type']}: "
                f"{row['error_message']}"
            )

        if show_log_lines:
            print("  relevant recovery log lines:")
            any_line = False
            for key in DIAGNOSTIC_FLAG_FIELDS:
                lines = diagnostic_lines.get(key, [])
                if not lines:
                    continue
                any_line = True
                # One line per signal is enough for this diagnostic pass.
                compact = " ".join(lines[0].split())
                if len(compact) > 260:
                    compact = compact[:260] + "..."
                print(f"    {key}: {compact}")
            if not any_line:
                print("    (none)")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "failure_type_diagnostic_results.csv"
        ),
    )
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=FAILURE_MODES,
        default=FAILURE_MODES,
    )
    parser.add_argument(
        "--trials",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--holders",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--task-duration",
        type=float,
        default=20.0,
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=2 * 1024 * 1024,
    )
    parser.add_argument(
        "--cpus-per-node",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--witness-count",
        type=int,
        default=2,
    )
    parser.add_argument(
        "--object-timeout-ms",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--cluster-timeout",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--start-timeout",
        type=float,
        default=20.0,
    )
    parser.add_argument(
        "--formation-timeout",
        type=float,
        default=30.0,
    )
    parser.add_argument(
        "--borrower-settle",
        type=float,
        default=0.5,
    )
    parser.add_argument(
        "--get-timeout",
        type=float,
        default=120.0,
    )
    parser.add_argument(
        "--replay-marker-settle",
        type=float,
        default=1.0,
        help="Seconds to wait for a second START marker after the read completes.",
    )
    parser.add_argument(
        "--diagnostic-settle",
        type=float,
        default=0.5,
        help="Seconds to allow C++ recovery logs to flush before collecting flags.",
    )
    parser.add_argument(
        "--show-log-lines",
        action="store_true",
        help=(
            "On failed trials, print one matching raw log line per recovery "
            "signal. Default output only prints compact flags."
        ),
    )

    args = parser.parse_args()

    rows: list[dict[str, Any]] = []
    total = len(args.modes) * args.trials
    run_no = 0

    for mode in args.modes:
        for trial in range(1, args.trials + 1):
            run_no += 1
            print(
                f"[{run_no}/{total}] "
                f"mode={mode} trial={trial}"
            )

            row, diagnostic_lines = run_trial(
                mode,
                args,
                trial,
            )
            rows.append(row)

            print_compact_result(
                row,
                diagnostic_lines,
                show_log_lines=args.show_log_lines,
            )

            write_csv(
                args.output,
                rows,
            )

    print(f"Saved to {args.output}")


if __name__ == "__main__":
    main()
