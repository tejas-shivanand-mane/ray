#!/usr/bin/env python3
"""
Recovery-succession chain/DAG dependency-recovery benchmark.

This benchmark tests a stronger case than recovery of independent tasks.

A failure-node owner constructs a serial chain of retryable normal tasks:

    source -> stage1 -> stage2 -> ... -> stageN

Every stage output is explicitly passed through the configured independent
recovery holders BEFORE that ObjectRef is used as the dependency of the next
stage. Thus the complete dependency chain, not only the terminal output, has
recovery-succession metadata retained on failure-independent workers.

After the chain has been constructed and at least a requested number of stages
have begun execution, the owner/original-compute node is forcibly removed. A
persistent borrower then requests only the final ObjectRef.

The benchmark asks:
  * Can the terminal result still be reconstructed after correlated owner /
    original-compute-node loss?
  * Can recovery of a downstream stage recover missing upstream dependencies?
  * Does the deterministic final value remain correct?
  * How many stages replay after failure?
  * Are duplicate post-failure replays observed?

The benchmark compares recovery succession disabled vs enabled.

NOTE:
The current prototype may expose a limitation if normal Ray dependency
resolution does not invoke recovery succession for a replay task's missing
upstream ObjectRefs. Such a failure is useful diagnostic information rather
than something this benchmark hides.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import re
import tempfile
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Holder-formation completion is emitted by the current C++ implementation at
# INFO level. Force INFO so formation checks cannot silently miss commits.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


COMMIT_RE = re.compile(
    r"Committed recovery succession manifest after witness publication "
    r"with\s+(\d+)\s+total members"
)


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
    failure_node: Any


def read_marker(
    path: Path,
) -> dict[int, dict[str, list[tuple[int, int]]]]:
    """
    Marker rows:
      START,stage_index,time_ns,pid
      FINISH,stage_index,time_ns,pid
    """
    data: dict[int, dict[str, list[tuple[int, int]]]] = {}

    if not path.exists():
        return data

    try:
        text = path.read_text(errors="replace")
    except OSError:
        return data

    for line in text.splitlines():
        parts = line.split(",")

        if (
            len(parts) != 4
            or parts[0] not in {"START", "FINISH"}
        ):
            continue

        try:
            event = parts[0]
            stage = int(parts[1])
            t_ns = int(parts[2])
            pid = int(parts[3])
        except ValueError:
            continue

        entry = data.setdefault(
            stage,
            {
                "START": [],
                "FINISH": [],
            },
        )

        entry[event].append((t_ns, pid))

    return data


def wait_for_started_stages(
    path: Path,
    minimum: int,
    timeout_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        data = read_marker(path)

        started = sum(
            1
            for stage_data in data.values()
            if stage_data["START"]
        )

        if started >= minimum:
            return started

        time.sleep(0.02)

    data = read_marker(path)

    return sum(
        1
        for stage_data in data.values()
        if stage_data["START"]
    )


class IncrementalCommitLogCounter:
    """
    Incrementally count recovery-manifest commit messages.

    Only python-core-worker files are inspected. Each file is read from its
    previous byte offset, so polling does not repeatedly rescan entire logs.
    """

    def __init__(self, session_dirs: set[Path]):
        self.session_dirs = set(session_dirs)
        self.offsets: dict[Path, int] = {}
        self.partial: dict[Path, str] = {}
        self.commit_counts: dict[int, int] = defaultdict(int)

    def _candidate_files(self):
        for session_dir in self.session_dirs:
            log_dir = session_dir / "logs"

            if not log_dir.exists():
                continue

            for path in log_dir.glob("python-core-worker-*"):
                if path.is_file():
                    yield path

    def poll(self) -> None:
        for path in self._candidate_files():
            try:
                size = path.stat().st_size
            except OSError:
                continue

            offset = self.offsets.get(path, 0)

            if size < offset:
                offset = 0
                self.partial[path] = ""

            if size == offset:
                continue

            try:
                with path.open("rb") as f:
                    f.seek(offset)
                    chunk = f.read()
                    new_offset = f.tell()
            except OSError:
                continue

            self.offsets[path] = new_offset

            if not chunk:
                continue

            text = (
                self.partial.get(path, "")
                + chunk.decode("utf-8", errors="replace")
            )

            if text.endswith("\n"):
                lines = text.splitlines()
                self.partial[path] = ""
            else:
                pieces = text.split("\n")
                lines = pieces[:-1]
                self.partial[path] = pieces[-1]

            for line in lines:
                match = COMMIT_RE.search(line)

                if match:
                    total_members = int(match.group(1))
                    self.commit_counts[total_members] += 1

    def count(self, total_members: int) -> int:
        self.poll()
        return self.commit_counts.get(total_members, 0)

    def wait_for(
        self,
        total_members: int,
        target: int,
        timeout_s: float,
        poll_interval_s: float,
    ) -> int:
        deadline = time.monotonic() + timeout_s

        while time.monotonic() < deadline:
            observed = self.count(total_members)

            if observed >= target:
                return observed

            time.sleep(poll_interval_s)

        return self.count(total_members)


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

    # Head / driver-control node.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    # Both the owner actor and the original chain tasks live here. This entire
    # node is removed to create correlated owner + original-compute failure.
    failure_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"failure_node": 1},
    )

    # Independent recovery-holder nodes.
    for rank in range(1, holders + 1):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )

    # Persistent final-result borrower.
    cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"borrower_node": 1},
    )

    # Extra witness candidates.
    for i in range(witness_count):
        cluster.add_node(
            num_cpus=0,
            resources={f"extra_witness_{i + 1}": 1},
        )

    return ClusterLayout(
        cluster=cluster,
        failure_node=failure_node,
    )


def wait_for_cluster(
    expected: int,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    alive = 0

    while time.monotonic() < deadline:
        alive = sum(
            1
            for node in ray.nodes()
            if node["Alive"]
        )

        if alive >= expected:
            return

        time.sleep(0.1)

    raise TimeoutError(
        f"Only {alive}/{expected} logical Ray nodes became alive"
    )


def make_remote_types():
    @ray.remote(max_retries=2)
    def source(
        seed: int,
        delay_s: float,
        payload_bytes: int,
        marker_path: str,
    ) -> bytes:
        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"START,0,{time.time_ns()},{os.getpid()}\n"
            )

        time.sleep(delay_s)

        prefix = seed.to_bytes(
            8,
            "little",
            signed=False,
        )

        if payload_bytes <= 8:
            value = prefix[:payload_bytes]
        else:
            value = (
                prefix
                + b"x" * (payload_bytes - 8)
            )

        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"FINISH,0,{time.time_ns()},{os.getpid()}\n"
            )

        return value

    @ray.remote(max_retries=2)
    def stage(
        stage_index: int,
        dep: bytes,
        delay_s: float,
        marker_path: str,
    ) -> bytes:
        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"START,{stage_index},"
                f"{time.time_ns()},{os.getpid()}\n"
            )

        time.sleep(delay_s)

        # Deterministic transformation that preserves object size.
        if dep:
            first = bytes(
                [
                    (
                        dep[0]
                        + stage_index
                    )
                    % 256
                ]
            )
            value = first + dep[1:]
        else:
            value = dep

        with open(marker_path, "a", buffering=1) as f:
            f.write(
                f"FINISH,{stage_index},"
                f"{time.time_ns()},{os.getpid()}\n"
            )

        return value

    @ray.remote(
        max_restarts=0,
        max_task_retries=0,
    )
    class Owner:
        def __init__(
            self,
            failure_node_id: str,
        ):
            self.failure_node_id = failure_node_id

        def _strategy(self):
            return NodeAffinitySchedulingStrategy(
                node_id=self.failure_node_id,
                soft=True,
            )

        def submit_source(
            self,
            seed: int,
            delay_s: float,
            payload_bytes: int,
            marker_path: str,
        ):
            ref = source.options(
                scheduling_strategy=self._strategy(),
                num_cpus=1,
            ).remote(
                seed,
                delay_s,
                payload_bytes,
                marker_path,
            )

            # Nested return preserves the ObjectRef.
            return [ref]

        def submit_stage(
            self,
            stage_index: int,
            wrapped_dep,
            delay_s: float,
            marker_path: str,
        ):
            # The dependency is nested in the actor argument so Ray does not
            # auto-dereference it before this method executes.
            dep_ref = wrapped_dep[0]

            ref = stage.options(
                scheduling_strategy=self._strategy(),
                num_cpus=1,
            ).remote(
                stage_index,
                dep_ref,
                delay_s,
                marker_path,
            )

            return [ref]

        def ping(self):
            return os.getpid()

    @ray.remote(
        max_restarts=0,
        max_concurrency=1,
    )
    class Holder:
        def __init__(self):
            # Keep every protected intermediate ObjectRef alive for the full
            # trial. Overwriting only the most recent ref would weaken the
            # intended dependency-recovery test.
            self.refs = {}

        def hold(
            self,
            stage_index: int,
            wrapped_ref,
        ):
            self.refs[stage_index] = wrapped_ref[0]
            return len(self.refs)

        def export(
            self,
            stage_index: int,
        ):
            return [self.refs[stage_index]]

        def ping(self):
            return os.getpid()

    @ray.remote(
        max_restarts=0,
        max_concurrency=2,
    )
    class Borrower:
        def hold_final(
            self,
            wrapped_ref,
        ):
            self.final_ref = wrapped_ref[0]
            return True

        def read_final(
            self,
            timeout_s: float,
        ):
            return ray.get(
                self.final_ref,
                timeout=timeout_s,
            )

        def ping(self):
            return os.getpid()

    return Owner, Holder, Borrower


def protect_stage_output(
    stage_index: int,
    ref,
    holders: list[Any],
    commit_counter: IncrementalCommitLogCounter,
    cumulative_commit_targets: dict[int, int],
    formation_timeout_s: float,
    log_poll_interval_s: float,
):
    """
    Pass one stage output through every independent holder.

    After each holder receives the ObjectRef, wait for the owner to commit that
    rank for this task before exporting the updated metadata-bearing ref to the
    next holder.
    """
    fresh = ref

    for rank, holder in enumerate(
        holders,
        start=1,
    ):
        retained_count = ray.get(
            holder.hold.remote(
                stage_index,
                [fresh],
            )
        )

        if retained_count < stage_index + 1:
            raise RuntimeError(
                f"Holder rank {rank} retained only "
                f"{retained_count} stage refs after stage "
                f"{stage_index}"
            )

        total_members = rank + 1
        cumulative_commit_targets[total_members] += 1
        target = cumulative_commit_targets[total_members]

        observed = commit_counter.wait_for(
            total_members=total_members,
            target=target,
            timeout_s=formation_timeout_s,
            poll_interval_s=log_poll_interval_s,
        )

        if observed < target:
            raise RuntimeError(
                f"Stage {stage_index} did not commit "
                f"holder rank {rank}: observed "
                f"{observed}/{target} cumulative commits "
                f"with {total_members} total members"
            )

        fresh = ray.get(
            holder.export.remote(stage_index)
        )[0]

    return fresh


def expected_first_byte(
    seed: int,
    chain_length: int,
) -> int:
    value = seed.to_bytes(
        8,
        "little",
        signed=False,
    )[0]

    for stage_index in range(
        1,
        chain_length,
    ):
        value = (
            value
            + stage_index
        ) % 256

    return value


FIELDS = [
    "trial",
    "config",
    "recovery_enabled",
    "chain_length",
    "stage_delay_s",
    "payload_bytes",
    "holders",
    "protected_outputs",
    "formation_success",
    "stages_started_before_failure",
    "stages_finished_before_failure",
    "success",
    "final_value_correct",
    "failure_to_result_s",
    "first_replay_start_s",
    "last_replay_start_s",
    "replay_start_span_s",
    "replay_to_result_s",
    "stages_ever_started",
    "stages_ever_finished",
    "stages_replayed_after_failure",
    "total_post_failure_starts",
    "stages_with_duplicate_replay",
    "max_post_failure_starts_for_one_stage",
    "error_type",
]


def make_row(
    case: Case,
    args,
    chain_length: int,
    trial: int,
):
    return {
        "trial": trial,
        "config": case.label,
        "recovery_enabled": int(case.enabled),
        "chain_length": chain_length,
        "stage_delay_s": args.stage_delay,
        "payload_bytes": args.payload_bytes,
        "holders": (
            args.holders
            if case.enabled
            else 0
        ),
        "protected_outputs": (
            chain_length
            if case.enabled
            else 0
        ),
        "formation_success": (
            not case.enabled
        ),
        "stages_started_before_failure": 0,
        "stages_finished_before_failure": 0,
        "success": False,
        "final_value_correct": False,
        "failure_to_result_s": math.nan,
        "first_replay_start_s": math.nan,
        "last_replay_start_s": math.nan,
        "replay_start_span_s": math.nan,
        "replay_to_result_s": math.nan,
        "stages_ever_started": 0,
        "stages_ever_finished": 0,
        "stages_replayed_after_failure": 0,
        "total_post_failure_starts": 0,
        "stages_with_duplicate_replay": 0,
        "max_post_failure_starts_for_one_stage": 0,
        "error_type": "",
    }


def analyze_marker(
    row: dict[str, Any],
    marker: Path,
    chain_length: int,
    failure_wall_ns: int,
) -> None:
    data = read_marker(marker)

    pre_failure_started = 0
    pre_failure_finished = 0
    stages_ever_started = 0
    stages_ever_finished = 0

    post_failure_counts: list[int] = []
    all_post_failure_start_ns: list[int] = []

    for stage_index in range(chain_length):
        entry = data.get(
            stage_index,
            {
                "START": [],
                "FINISH": [],
            },
        )

        starts = entry["START"]
        finishes = entry["FINISH"]

        if starts:
            stages_ever_started += 1

        if finishes:
            stages_ever_finished += 1

        if any(
            t_ns < failure_wall_ns
            for t_ns, _ in starts
        ):
            pre_failure_started += 1

        if any(
            t_ns < failure_wall_ns
            for t_ns, _ in finishes
        ):
            pre_failure_finished += 1

        post_failure_starts = [
            t_ns
            for t_ns, _ in starts
            if t_ns >= failure_wall_ns
        ]

        post_failure_counts.append(
            len(post_failure_starts)
        )

        all_post_failure_start_ns.extend(
            post_failure_starts
        )

    row["stages_started_before_failure"] = (
        pre_failure_started
    )
    row["stages_finished_before_failure"] = (
        pre_failure_finished
    )
    row["stages_ever_started"] = stages_ever_started
    row["stages_ever_finished"] = stages_ever_finished

    row["stages_replayed_after_failure"] = sum(
        1
        for count in post_failure_counts
        if count >= 1
    )

    row["total_post_failure_starts"] = sum(
        post_failure_counts
    )

    row["stages_with_duplicate_replay"] = sum(
        1
        for count in post_failure_counts
        if count > 1
    )

    row["max_post_failure_starts_for_one_stage"] = max(
        post_failure_counts,
        default=0,
    )

    if all_post_failure_start_ns:
        first_ns = min(all_post_failure_start_ns)
        last_ns = max(all_post_failure_start_ns)

        row["first_replay_start_s"] = max(
            0.0,
            (
                first_ns
                - failure_wall_ns
            )
            / 1e9,
        )

        row["last_replay_start_s"] = max(
            0.0,
            (
                last_ns
                - failure_wall_ns
            )
            / 1e9,
        )

        row["replay_start_span_s"] = (
            row["last_replay_start_s"]
            - row["first_replay_start_s"]
        )

        if (
            not math.isnan(
                row["failure_to_result_s"]
            )
            and row["failure_to_result_s"]
            >= row["first_replay_start_s"]
        ):
            row["replay_to_result_s"] = (
                row["failure_to_result_s"]
                - row["first_replay_start_s"]
            )


def run_trial(
    case: Case,
    args,
    chain_length: int,
    trial: int,
):
    row = make_row(
        case,
        args,
        chain_length,
        trial,
    )

    marker = (
        Path(tempfile.gettempdir())
        / (
            "ray_chain_recovery_"
            f"{os.getpid()}_"
            f"{uuid.uuid4().hex}.csv"
        )
    )

    cluster = None
    failure_wall_ns = 0

    try:
        layout = start_cluster(
            case=case,
            holders=args.holders,
            cpus_per_node=args.cpus_per_node,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
        )

        cluster = layout.cluster

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
            logging_level=logging.ERROR,
        )

        expected_nodes = (
            1  # head
            + 1  # failure node
            + args.holders
            + 1  # borrower node
            + args.witness_count
        )

        wait_for_cluster(
            expected_nodes,
            args.cluster_timeout,
        )

        Owner, Holder, Borrower = (
            make_remote_types()
        )

        owner = Owner.options(
            resources={"failure_node": 0.01},
            num_cpus=0,
        ).remote(
            layout.failure_node.node_id
        )

        ray.get(owner.ping.remote())

        holder_actors = [
            Holder.options(
                resources={
                    f"holder_{rank}": 0.01
                },
                num_cpus=0,
            ).remote()
            for rank in range(
                1,
                args.holders + 1,
            )
        ]

        if holder_actors:
            ray.get(
                [
                    holder.ping.remote()
                    for holder in holder_actors
                ]
            )

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        ray.get(borrower.ping.remote())

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        commit_counter = (
            IncrementalCommitLogCounter(
                session_dirs
            )
        )

        # Move offsets past startup logs.
        commit_counter.poll()

        cumulative_commit_targets: dict[
            int,
            int,
        ] = defaultdict(int)

        seed = (
            100_000 * trial
            + chain_length
        )

        # ------------------------------------------------------------
        # Construct source and protect source output.
        # ------------------------------------------------------------
        current_ref = ray.get(
            owner.submit_source.remote(
                seed,
                args.stage_delay,
                args.payload_bytes,
                str(marker),
            )
        )[0]

        if case.enabled:
            current_ref = protect_stage_output(
                stage_index=0,
                ref=current_ref,
                holders=holder_actors,
                commit_counter=commit_counter,
                cumulative_commit_targets=(
                    cumulative_commit_targets
                ),
                formation_timeout_s=(
                    args.formation_timeout
                ),
                log_poll_interval_s=(
                    args.log_poll_interval_ms
                    / 1000.0
                ),
            )

        # ------------------------------------------------------------
        # Build remaining stages one by one.
        #
        # Crucially, the protected ref from stage i becomes the dependency
        # embedded in stage i+1's TaskSpec.
        # ------------------------------------------------------------
        for stage_index in range(
            1,
            chain_length,
        ):
            current_ref = ray.get(
                owner.submit_stage.remote(
                    stage_index,
                    [current_ref],
                    args.stage_delay,
                    str(marker),
                )
            )[0]

            if case.enabled:
                current_ref = protect_stage_output(
                    stage_index=stage_index,
                    ref=current_ref,
                    holders=holder_actors,
                    commit_counter=commit_counter,
                    cumulative_commit_targets=(
                        cumulative_commit_targets
                    ),
                    formation_timeout_s=(
                        args.formation_timeout
                    ),
                    log_poll_interval_s=(
                        args.log_poll_interval_ms
                        / 1000.0
                    ),
                )

        if case.enabled:
            row["formation_success"] = True

        # Borrower retains only the terminal result. Upstream recovery must
        # therefore happen through dependency lineage, not because the borrower
        # explicitly asks for every intermediate stage output.
        ray.get(
            borrower.hold_final.remote(
                [current_ref]
            )
        )

        required_started = min(
            max(
                1,
                args.minimum_started_stages,
            ),
            chain_length,
        )

        observed_started = wait_for_started_stages(
            marker,
            required_started,
            args.start_timeout,
        )

        if observed_started < required_started:
            raise RuntimeError(
                f"Only {observed_started}/"
                f"{required_started} required stages "
                "started before failure"
            )

        if args.failure_delay > 0:
            time.sleep(args.failure_delay)

        # Reject an invalid trial in which the terminal result has already
        # completed before failure injection.
        pre_failure_data = read_marker(marker)

        final_entry = pre_failure_data.get(
            chain_length - 1,
            {
                "START": [],
                "FINISH": [],
            },
        )

        if final_entry["FINISH"]:
            raise RuntimeError(
                "Final chain stage finished before "
                "failure injection. Increase "
                "--stage-delay or fail earlier."
            )

        failure_wall_ns = time.time_ns()
        failure_t0 = time.perf_counter()

        cluster.remove_node(
            layout.failure_node,
            allow_graceful=False,
        )

        try:
            value = ray.get(
                borrower.read_final.remote(
                    args.get_timeout
                ),
                timeout=(
                    args.get_timeout
                    + 10.0
                ),
            )

            row["failure_to_result_s"] = (
                time.perf_counter()
                - failure_t0
            )

            correct = True

            if args.payload_bytes > 0:
                expected_byte = expected_first_byte(
                    seed,
                    chain_length,
                )

                if not value:
                    correct = False
                elif value[0] != expected_byte:
                    correct = False
                elif len(value) != args.payload_bytes:
                    correct = False

            row["final_value_correct"] = correct
            row["success"] = correct

            if not correct:
                row["error_type"] = (
                    "IncorrectRecoveredValue"
                )
                row["error_message"] = (
                    "Recovered final value did not "
                    "match deterministic expected output"
                )

        except Exception as exc:
            row["failure_to_result_s"] = (
                time.perf_counter()
                - failure_t0
            )

            row["error_type"] = (
                type(exc).__name__
            )

            # row["error_message"] = (
            #     str(exc)[:500]
            # )

        analyze_marker(
            row=row,
            marker=marker,
            chain_length=chain_length,
            failure_wall_ns=failure_wall_ns,
        )

        return row

    except Exception as exc:
        if not row["error_type"]:
            row["error_type"] = (
                type(exc).__name__
            )
            # row["error_message"] = (
            #     str(exc)[:500]
            # )

        if failure_wall_ns > 0:
            analyze_marker(
                row=row,
                marker=marker,
                chain_length=chain_length,
                failure_wall_ns=failure_wall_ns,
            )

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
            marker.unlink(
                missing_ok=True
            )
        except OSError:
            pass


def format_csv_value(value: Any):
    # Keep integer counts as integers and cap floating-point output at
    # two decimal places, as requested for the benchmark CSVs.
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return f"{value:.2f}"

    return value


def write_rows(
    path: Path,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    formatted_rows = []

    for row in rows:
        formatted_rows.append(
            {
                key: format_csv_value(value)
                for key, value in row.items()
            }
        )

    with path.open(
        "w",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )

        writer.writeheader()
        writer.writerows(formatted_rows)


def validate_args(args) -> None:
    if any(
        length < 2
        for length in args.chain_lengths
    ):
        raise SystemExit(
            "All --chain-lengths values must be >= 2"
        )

    if (
        args.holders < 1
        or args.holders > 4
    ):
        raise SystemExit(
            "--holders must be in 1..4"
        )

    if args.stage_delay <= 0:
        raise SystemExit(
            "--stage-delay must be > 0"
        )

    if args.payload_bytes < 0:
        raise SystemExit(
            "--payload-bytes must be >= 0"
        )

    if args.cpus_per_node <= 0:
        raise SystemExit(
            "--cpus-per-node must be > 0"
        )

    if args.witness_count <= 0:
        raise SystemExit(
            "--witness-count must be > 0"
        )

    if args.trials <= 0:
        raise SystemExit(
            "--trials must be > 0"
        )

    if args.minimum_started_stages <= 0:
        raise SystemExit(
            "--minimum-started-stages must be > 0"
        )

    if args.log_poll_interval_ms <= 0:
        raise SystemExit(
            "--log-poll-interval-ms must be > 0"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Fully protected recovery-succession "
            "chain/dependency benchmark."
        )
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "chain_recovery_results.csv"
        ),
    )

    parser.add_argument(
        "--trials",
        type=int,
        default=3,
    )

    parser.add_argument(
        "--chain-lengths",
        type=int,
        nargs="+",
        default=[2, 4, 8, 16],
        help=(
            "Number of task stages including the "
            "source task."
        ),
    )

    parser.add_argument(
        "--stage-delay",
        type=float,
        default=2.0,
        help=(
            "Execution time of each source/stage task "
            "in seconds."
        ),
    )

    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=1024 * 1024,
    )

    parser.add_argument(
        "--holders",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--cpus-per-node",
        type=int,
        default=2,
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
        "--minimum-started-stages",
        type=int,
        default=1,
        help=(
            "Distinct original stages that must have "
            "started before failure. Default 1 forces "
            "near-full-chain dependency reconstruction."
        ),
    )

    parser.add_argument(
        "--failure-delay",
        type=float,
        default=0.0,
        help=(
            "Optional extra seconds to wait after the "
            "minimum started-stage condition."
        ),
    )

    parser.add_argument(
        "--cluster-timeout",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--start-timeout",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--formation-timeout",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--get-timeout",
        type=float,
        default=120.0,
    )

    parser.add_argument(
        "--log-poll-interval-ms",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--systems",
        nargs="+",
        choices=[
            "disabled",
            "enabled",
        ],
        default=[
            "disabled",
            "enabled",
        ],
    )

    args = parser.parse_args()
    validate_args(args)

    requested_systems = set(
        args.systems
    )

    selected = [
        case
        for case in CASES
        if case.label.lower()
        in requested_systems
    ]

    rows: list[dict[str, Any]] = []

    total = (
        len(args.chain_lengths)
        * len(selected)
        * args.trials
    )

    run_no = 0

    for chain_length in args.chain_lengths:
        for case in selected:
            for trial in range(
                1,
                args.trials + 1,
            ):
                run_no += 1

                print(
                    f"\n[{run_no}/{total}] "
                    f"chain_length={chain_length} "
                    f"{case.label} "
                    f"trial={trial}"
                )

                row = run_trial(
                    case=case,
                    args=args,
                    chain_length=chain_length,
                    trial=trial,
                )

                rows.append(row)

                if row["success"]:
                    print(
                        "  "
                        f"success=True "
                        f"failure_to_result="
                        f"{row['failure_to_result_s']:.2f}s "
                        f"replayed_stages="
                        f"{row['stages_replayed_after_failure']}/"
                        f"{chain_length} "
                        f"duplicates="
                        f"{row['stages_with_duplicate_replay']}"
                    )
                else:
                    print(
                        "  "
                        f"success=False "
                        f"error={row['error_type']}: "
                    )

                write_rows(
                    args.output,
                    rows,
                )

    print(
        f"\nSaved to {args.output}"
    )


if __name__ == "__main__":
    main()