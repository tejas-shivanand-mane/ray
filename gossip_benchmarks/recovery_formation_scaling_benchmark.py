#!/usr/bin/env python3
"""
Recovery-succession formation scaling benchmark.

No failures are injected. An owner creates N recoverable task outputs and the
same ObjectRefs are passed through 1..R independent holder actors. The benchmark
measures the cost of constructing and committing recovery-succession metadata.

Primary metrics:
  * total holder-formation time
  * per-rank formation time
  * task-holder admissions per second

Secondary metadata metrics:
  * owner RSS delta
  * aggregate holder RSS delta
  * effect of inline TaskSpec argument size

Important measurement detail:
  Formation completion is confirmed from the current C++ INFO message:

    "Committed recovery succession manifest after witness publication with
     <K> total members"

  The benchmark therefore forces RAY_BACKEND_LOG_LEVEL=info. Log files are
  scanned incrementally; they are not reread from the beginning on every poll.
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

# Formation completion is detected from RAY_LOG(INFO) in CoreWorker.
# Force this rather than using setdefault(), because a pre-existing warning
# setting would make the benchmark falsely report zero committed holders.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


COMMIT_RE = re.compile(
    r"Committed recovery succession manifest after witness publication "
    r"with\s+(\d+)\s+total members"
)


def rss_bytes() -> int:
    """Return Linux resident-set size for the current worker process."""
    try:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        return -1


class IncrementalCommitLogCounter:
    """
    Incrementally count recovery-manifest commit messages in Ray session logs.

    The old benchmark reread every log file in full every 100 ms. That can make
    the benchmark's own polling cost grow with log volume. This watcher keeps a
    byte offset per file and reads only newly appended bytes.

    Counts are indexed by total succession members:
      2 total members -> rank 1 committed
      3 total members -> rank 2 committed
      ...
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
            for path in log_dir.glob("*"):
                if path.is_file():
                    yield path

    def poll(self) -> None:
        for path in self._candidate_files():
            try:
                size = path.stat().st_size
            except OSError:
                continue

            offset = self.offsets.get(path, 0)

            # Handle file truncation/rotation.
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

            text = self.partial.get(path, "") + chunk.decode(
                "utf-8", errors="replace"
            )

            # Keep an incomplete trailing line for the next poll.
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

    def count_for_total_members(self, total_members: int) -> int:
        self.poll()
        return self.commit_counts.get(total_members, 0)

    def wait_for_total_members(
        self,
        total_members: int,
        target: int,
        timeout_s: float,
        poll_interval_s: float,
    ) -> int:
        deadline = time.monotonic() + timeout_s
        last = 0

        while time.monotonic() < deadline:
            last = self.count_for_total_members(total_members)
            if last >= target:
                return last
            time.sleep(poll_interval_s)

        return self.count_for_total_members(total_members)


def start_cluster(
    holders: int,
    cpus_per_node: int,
    witness_count: int,
    object_timeout_ms: int,
):
    cluster = Cluster()

    config: dict[str, Any] = {
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

    for rank in range(1, holders + 1):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )

    # Extra logical nodes make witness placement less constrained.
    for i in range(witness_count):
        cluster.add_node(
            num_cpus=0,
            resources={f"extra_witness_{i + 1}": 1},
        )

    return cluster, owner_node


def wait_for_cluster(expected: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive = 0

    while time.monotonic() < deadline:
        alive = sum(1 for n in ray.nodes() if n["Alive"])
        if alive >= expected:
            return
        time.sleep(0.1)

    raise TimeoutError(
        f"Only {alive}/{expected} logical Ray nodes became alive"
    )


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(
        index: int,
        inline_blob: bytes,
        payload_bytes: int,
    ) -> bytes:
        # Touch the inline argument so it remains semantically part of the task.
        checksum = inline_blob[0] if inline_blob else 0
        prefix = (index ^ checksum).to_bytes(
            8, "little", signed=False
        )

        if payload_bytes <= 8:
            return prefix[:payload_bytes]

        return prefix + b"x" * (payload_bytes - 8)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, owner_node_id: str):
            self.owner_node_id = owner_node_id

        def create_many(
            self,
            count: int,
            inline_arg_bytes: int,
            payload_bytes: int,
        ):
            blob = b"a" * inline_arg_bytes

            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.owner_node_id,
                soft=True,
            )

            refs = []

            for i in range(count):
                refs.append(
                    produce.options(
                        scheduling_strategy=strategy,
                        num_cpus=1,
                    ).remote(
                        i,
                        blob,
                        payload_bytes,
                    )
                )

            # Returning nested ObjectRefs allows the driver to receive the refs
            # without fetching the produced values.
            return refs

        def rss(self):
            return rss_bytes()

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold_many(self, refs):
            # Receiving nested ObjectRefs causes recovery metadata to be
            # registered on this independent downstream borrower.
            self.refs = list(refs)
            return len(self.refs)

        def export_many(self):
            return list(self.refs)

        def rss(self):
            return rss_bytes()

        def ping(self):
            return os.getpid()

    return Owner, Holder


FIELDS = [
    "trial",
    "tasks",
    "holders",
    "inline_arg_bytes",
    "payload_bytes",
    "cpus_per_node",
    "witness_count",
    "creation_time_s",
    "formation_time_s",
    "admissions",
    "admissions_per_s",
    "formation_ms_per_admission",
    "owner_rss_before_bytes",
    "owner_rss_after_bytes",
    "owner_rss_delta_bytes",
    "holder_rss_sum_before_bytes",
    "holder_rss_sum_after_bytes",
    "holder_rss_sum_delta_bytes",
    "rank1_time_s",
    "rank2_time_s",
    "rank3_time_s",
    "rank4_time_s",
    "rank1_commits_observed",
    "rank2_commits_observed",
    "rank3_commits_observed",
    "rank4_commits_observed",
    "success",
    "error_type",
    "error_message",
]


def empty_row(
    args,
    tasks: int,
    holders: int,
    inline_arg_bytes: int,
    trial: int,
):
    return {
        "trial": trial,
        "tasks": tasks,
        "holders": holders,
        "inline_arg_bytes": inline_arg_bytes,
        "payload_bytes": args.payload_bytes,
        "cpus_per_node": args.cpus_per_node,
        "witness_count": args.witness_count,
        "creation_time_s": math.nan,
        "formation_time_s": math.nan,
        "admissions": tasks * holders,
        "admissions_per_s": math.nan,
        "formation_ms_per_admission": math.nan,
        "owner_rss_before_bytes": -1,
        "owner_rss_after_bytes": -1,
        "owner_rss_delta_bytes": math.nan,
        "holder_rss_sum_before_bytes": -1,
        "holder_rss_sum_after_bytes": -1,
        "holder_rss_sum_delta_bytes": math.nan,
        "rank1_time_s": math.nan,
        "rank2_time_s": math.nan,
        "rank3_time_s": math.nan,
        "rank4_time_s": math.nan,
        "rank1_commits_observed": 0,
        "rank2_commits_observed": 0,
        "rank3_commits_observed": 0,
        "rank4_commits_observed": 0,
        "success": False,
        "error_type": "",
        "error_message": "",
    }


def run_case(
    args,
    tasks: int,
    holders: int,
    inline_arg_bytes: int,
    trial: int,
):
    row = empty_row(
        args,
        tasks,
        holders,
        inline_arg_bytes,
        trial,
    )

    cluster = None

    try:
        cluster, owner_node = start_cluster(
            holders=holders,
            cpus_per_node=args.cpus_per_node,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
            logging_level=logging.ERROR,
        )

        expected_nodes = (
            1  # head
            + 1  # owner
            + holders
            + args.witness_count
        )

        wait_for_cluster(
            expected_nodes,
            args.cluster_timeout,
        )

        Owner, Holder = make_remote_types()

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(owner_node.node_id)

        ray.get(owner.ping.remote())

        holder_actors = [
            Holder.options(
                resources={f"holder_{rank}": 0.01},
                num_cpus=0,
            ).remote()
            for rank in range(1, holders + 1)
        ]

        ray.get(
            [holder.ping.remote() for holder in holder_actors]
        )

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        commit_counter = IncrementalCommitLogCounter(
            session_dirs
        )

        # Advance offsets past cluster/actor startup logs before measuring.
        commit_counter.poll()

        row["owner_rss_before_bytes"] = ray.get(
            owner.rss.remote()
        )

        holder_before = ray.get(
            [holder.rss.remote() for holder in holder_actors]
        )

        valid_holder_before = [
            value for value in holder_before if value >= 0
        ]

        row["holder_rss_sum_before_bytes"] = (
            sum(valid_holder_before)
            if len(valid_holder_before) == len(holder_before)
            else -1
        )

        # Create recoverable outputs. This is reported separately and is not
        # included in formation_time_s.
        creation_start = time.perf_counter()

        refs = ray.get(
            owner.create_many.remote(
                tasks,
                inline_arg_bytes,
                args.payload_bytes,
            )
        )

        row["creation_time_s"] = (
            time.perf_counter() - creation_start
        )

        if len(refs) != tasks:
            raise RuntimeError(
                f"Owner returned {len(refs)}/{tasks} ObjectRefs"
            )

        # Keep the most recent metadata-bearing ObjectRefs alive throughout
        # formation. Each holder receives the refs exported by the previous
        # holder, producing the ordered succession path.
        fresh_refs = refs
        rank_times: list[float] = []

        formation_start = time.perf_counter()

        for rank, holder in enumerate(
            holder_actors,
            start=1,
        ):
            rank_start = time.perf_counter()

            retained = ray.get(
                holder.hold_many.remote(fresh_refs)
            )

            if retained != tasks:
                raise RuntimeError(
                    f"Holder rank {rank} retained "
                    f"{retained}/{tasks} refs"
                )

            # Rank r corresponds to r+1 total succession members because
            # rank 0 is the original owner.
            total_members = rank + 1

            observed = commit_counter.wait_for_total_members(
                total_members=total_members,
                target=tasks,
                timeout_s=args.formation_timeout,
                poll_interval_s=(
                    args.log_poll_interval_ms / 1000.0
                ),
            )

            row[f"rank{rank}_commits_observed"] = observed

            if observed < tasks:
                raise RuntimeError(
                    f"Only {observed}/{tasks} "
                    f"holder-rank-{rank} commits observed"
                )

            fresh_refs = ray.get(
                holder.export_many.remote()
            )

            rank_elapsed = (
                time.perf_counter() - rank_start
            )

            rank_times.append(rank_elapsed)
            row[f"rank{rank}_time_s"] = rank_elapsed

        row["formation_time_s"] = (
            time.perf_counter() - formation_start
        )

        admissions = tasks * holders

        if row["formation_time_s"] > 0:
            row["admissions_per_s"] = (
                admissions / row["formation_time_s"]
            )

            if admissions > 0:
                row["formation_ms_per_admission"] = (
                    1000.0
                    * row["formation_time_s"]
                    / admissions
                )

        row["owner_rss_after_bytes"] = ray.get(
            owner.rss.remote()
        )

        holder_after = ray.get(
            [holder.rss.remote() for holder in holder_actors]
        )

        valid_holder_after = [
            value for value in holder_after if value >= 0
        ]

        row["holder_rss_sum_after_bytes"] = (
            sum(valid_holder_after)
            if len(valid_holder_after) == len(holder_after)
            else -1
        )

        if (
            row["owner_rss_before_bytes"] >= 0
            and row["owner_rss_after_bytes"] >= 0
        ):
            row["owner_rss_delta_bytes"] = (
                row["owner_rss_after_bytes"]
                - row["owner_rss_before_bytes"]
            )

        if (
            row["holder_rss_sum_before_bytes"] >= 0
            and row["holder_rss_sum_after_bytes"] >= 0
        ):
            row["holder_rss_sum_delta_bytes"] = (
                row["holder_rss_sum_after_bytes"]
                - row["holder_rss_sum_before_bytes"]
            )

        row["success"] = True
        return row

    except Exception as exc:
        row["error_type"] = type(exc).__name__
        row["error_message"] = str(exc)[:500]
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


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
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


def validate_args(args) -> None:
    if any(tasks <= 0 for tasks in args.tasks):
        raise SystemExit(
            "All --tasks values must be > 0"
        )

    if any(
        holder_count < 1 or holder_count > 4
        for holder_count in args.holders
    ):
        raise SystemExit(
            "This benchmark currently expects "
            "--holders values in 1..4"
        )

    if any(
        size < 0 for size in args.inline_arg_bytes
    ):
        raise SystemExit(
            "--inline-arg-bytes values must be >= 0"
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

    if args.log_poll_interval_ms <= 0:
        raise SystemExit(
            "--log-poll-interval-ms must be > 0"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Measure recovery-succession holder-formation "
            "scaling with no injected failures."
        )
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "recovery_formation_scaling_results.csv"
        ),
    )

    parser.add_argument(
        "--tasks",
        type=int,
        nargs="+",
        default=[1, 4, 8, 16, 32, 64],
        help="Numbers of protected task outputs.",
    )

    parser.add_argument(
        "--holders",
        type=int,
        nargs="+",
        default=[1, 2, 4],
        help="Numbers of non-owner recovery holders.",
    )

    parser.add_argument(
        "--inline-arg-bytes",
        type=int,
        nargs="+",
        default=[0],
        help=(
            "Inline bytes carried in each task's TaskSpec. "
            "Use one value for the primary scaling experiment; "
            "sweep this separately for metadata-size tests."
        ),
    )

    parser.add_argument(
        "--payload-bytes",
        type=int,
        default=1024,
        help=(
            "Produced object payload size. This experiment "
            "targets formation rather than object-transfer scaling."
        ),
    )

    parser.add_argument(
        "--trials",
        type=int,
        default=3,
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
        "--cluster-timeout",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--formation-timeout",
        type=float,
        default=60.0,
    )

    parser.add_argument(
        "--log-poll-interval-ms",
        type=float,
        default=20.0,
        help=(
            "Incremental log polling interval used only to "
            "observe INFO-level commit completion."
        ),
    )

    args = parser.parse_args()
    validate_args(args)

    rows: list[dict[str, Any]] = []

    total = (
        len(args.tasks)
        * len(args.holders)
        * len(args.inline_arg_bytes)
        * args.trials
    )

    run_no = 0

    for tasks in args.tasks:
        for holders in args.holders:
            for arg_bytes in args.inline_arg_bytes:
                for trial in range(
                    1,
                    args.trials + 1,
                ):
                    run_no += 1

                    print(
                        f"\n[{run_no}/{total}] "
                        f"tasks={tasks} "
                        f"holders={holders} "
                        f"inline_arg_bytes={arg_bytes} "
                        f"trial={trial}"
                    )

                    row = run_case(
                        args,
                        tasks,
                        holders,
                        arg_bytes,
                        trial,
                    )

                    rows.append(row)

                    if row["success"]:
                        print(
                            "  "
                            f"formation="
                            f"{row['formation_time_s']:.4f}s "
                            f"admissions/s="
                            f"{row['admissions_per_s']:.2f} "
                            f"ms/admission="
                            f"{row['formation_ms_per_admission']:.3f}"
                        )
                    else:
                        print(
                            "  FAILED "
                            f"{row['error_type']}: "
                            f"{row['error_message']}"
                        )

                    # Persist after every trial so partial results survive
                    # an interrupted long sweep.
                    write_rows(
                        args.output,
                        rows,
                    )

    print(
        f"\nSaved to {args.output}"
    )


if __name__ == "__main__":
    main()