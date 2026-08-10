#!/usr/bin/env python3
"""
Prototype benchmark: recovery-holder formation and metadata scaling.

No failures are injected. The owner creates N recoverable task outputs and the
same set of ObjectRefs is passed through 1..R holders. We measure:

  * time to commit each holder rank
  * total formation time
  * admission throughput (task-holder admissions / second)
  * owner and holder process RSS before/after formation
  * effect of small inline task-argument size on TaskSpec replication cost

This complements the steady-state throughput benchmark by isolating the
formation path itself.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import time
from pathlib import Path
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def rss_bytes() -> int:
    """Linux RSS without external dependencies."""
    try:
        pages = int(Path("/proc/self/statm").read_text().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except Exception:
        return -1


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
    target: int,
    timeout_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    count = 0
    while time.monotonic() < deadline:
        count = len(find_log_lines(session_dirs, text))
        if count >= target:
            return count
        time.sleep(0.1)
    return count


def start_cluster(
    holders: int,
    cpus_per_node: int,
    witness_count: int,
    object_timeout_ms: int,
):
    cluster = Cluster()
    config = {
        "enable_recovery_succession": True,
        "recovery_succession_target_holder_count": holders,
        "recovery_succession_witness_count": witness_count,
        "object_timeout_milliseconds": object_timeout_ms,
    }

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
    for i in range(witness_count):
        cluster.add_node(num_cpus=0, resources={f"extra_witness_{i+1}": 1})
    return cluster, owner_node


def wait_for_cluster(expected: int, timeout_s: float):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        alive = sum(1 for n in ray.nodes() if n["Alive"])
        if alive >= expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected} logical nodes became alive")


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(index: int, inline_blob: bytes, payload_bytes: int):
        # Touch the inline argument so Python cannot trivially ignore it.
        checksum = inline_blob[0] if inline_blob else 0
        prefix = (index ^ checksum).to_bytes(8, "little", signed=False)
        return prefix[:payload_bytes] if payload_bytes <= 8 else prefix + b"x" * (payload_bytes - 8)

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, owner_node_id: str):
            self.owner_node_id = owner_node_id

        def create_many(self, count: int, inline_arg_bytes: int, payload_bytes: int):
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
                    ).remote(i, blob, payload_bytes)
                )
            return refs

        def rss(self):
            return rss_bytes()

        def ping(self):
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold_many(self, refs):
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
    "creation_time_s",
    "formation_time_s",
    "admissions",
    "admissions_per_s",
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
    "success",
    "error_type",
    "error_message",
]


def run_case(args, tasks: int, holders: int, inline_arg_bytes: int, trial: int):
    row = {
        "trial": trial,
        "tasks": tasks,
        "holders": holders,
        "inline_arg_bytes": inline_arg_bytes,
        "payload_bytes": args.payload_bytes,
        "creation_time_s": math.nan,
        "formation_time_s": math.nan,
        "admissions": tasks * holders,
        "admissions_per_s": math.nan,
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
        "success": False,
        "error_type": "",
        "error_message": "",
    }

    cluster = None

    try:
        cluster, owner_node = start_cluster(
            holders,
            args.cpus_per_node,
            args.witness_count,
            args.object_timeout_ms,
        )
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)

        expected = 1 + 1 + holders + args.witness_count
        wait_for_cluster(expected, args.cluster_timeout)

        Owner, Holder = make_remote_types()

        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote(
            owner_node.node_id
        )
        ray.get(owner.ping.remote())

        holder_actors = [
            Holder.options(
                resources={f"holder_{rank}": 0.01},
                num_cpus=0,
            ).remote()
            for rank in range(1, holders + 1)
        ]
        ray.get([h.ping.remote() for h in holder_actors])

        session_dirs = {
            Path(n.get_session_dir_path())
            for n in cluster.list_all_nodes()
        }

        row["owner_rss_before_bytes"] = ray.get(owner.rss.remote())
        holder_before = ray.get([h.rss.remote() for h in holder_actors])
        row["holder_rss_sum_before_bytes"] = sum(x for x in holder_before if x >= 0)

        t0 = time.perf_counter()
        refs = ray.get(
            owner.create_many.remote(tasks, inline_arg_bytes, args.payload_bytes)
        )
        row["creation_time_s"] = time.perf_counter() - t0

        # Keep refs alive throughout formation.
        fresh_refs = refs
        rank_times: list[float] = []

        formation_start = time.perf_counter()

        for rank, holder in enumerate(holder_actors, start=1):
            rank_start = time.perf_counter()
            retained = ray.get(holder.hold_many.remote(fresh_refs))
            if retained != tasks:
                raise RuntimeError(f"Rank {rank} retained {retained}/{tasks} refs")

            needle = (
                "Committed recovery succession manifest after witness publication "
                f"with {rank + 1} total members"
            )
            observed = wait_for_log_count(
                session_dirs,
                needle,
                tasks,
                args.formation_timeout,
            )
            if observed < tasks:
                raise RuntimeError(
                    f"Only {observed}/{tasks} holder-rank-{rank} commits observed"
                )

            fresh_refs = ray.get(holder.export_many.remote())
            rank_times.append(time.perf_counter() - rank_start)

        row["formation_time_s"] = time.perf_counter() - formation_start

        for i, value in enumerate(rank_times[:4], start=1):
            row[f"rank{i}_time_s"] = value

        if row["formation_time_s"] > 0:
            row["admissions_per_s"] = (tasks * holders) / row["formation_time_s"]

        row["owner_rss_after_bytes"] = ray.get(owner.rss.remote())
        holder_after = ray.get([h.rss.remote() for h in holder_actors])
        row["holder_rss_sum_after_bytes"] = sum(x for x in holder_after if x >= 0)

        if row["owner_rss_before_bytes"] >= 0 and row["owner_rss_after_bytes"] >= 0:
            row["owner_rss_delta_bytes"] = (
                row["owner_rss_after_bytes"] - row["owner_rss_before_bytes"]
            )

        if row["holder_rss_sum_before_bytes"] >= 0 and row["holder_rss_sum_after_bytes"] >= 0:
            row["holder_rss_sum_delta_bytes"] = (
                row["holder_rss_sum_after_bytes"] - row["holder_rss_sum_before_bytes"]
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("formation_scaling_results.csv"))
    parser.add_argument("--tasks", type=int, nargs="+", default=[1, 10, 50])
    parser.add_argument("--holders", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument(
        "--inline-arg-bytes",
        type=int,
        nargs="+",
        default=[0, 4096, 32768],
        help="Small values remain inline and directly increase TaskSpec size.",
    )
    parser.add_argument("--payload-bytes", type=int, default=1024)
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--cpus-per-node", type=int, default=2)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--object-timeout-ms", type=int, default=100)
    parser.add_argument("--cluster-timeout", type=float, default=30.0)
    parser.add_argument("--formation-timeout", type=float, default=60.0)
    args = parser.parse_args()

    if any(h < 1 or h > 4 for h in args.holders):
        raise SystemExit("This prototype script expects holder counts in 1..4")

    rows = []
    total = len(args.tasks) * len(args.holders) * len(args.inline_arg_bytes) * args.trials
    run_no = 0

    for tasks in args.tasks:
        for holders in args.holders:
            for arg_bytes in args.inline_arg_bytes:
                for trial in range(1, args.trials + 1):
                    run_no += 1
                    print(
                        f"\n[{run_no}/{total}] tasks={tasks} holders={holders} "
                        f"inline_arg_bytes={arg_bytes} trial={trial}"
                    )
                    row = run_case(args, tasks, holders, arg_bytes, trial)
                    rows.append(row)
                    print(
                        f"  success={row['success']} formation={row['formation_time_s']} "
                        f"admissions/s={row['admissions_per_s']}"
                    )

                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    with args.output.open("w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=FIELDS)
                        writer.writeheader()
                        writer.writerows(rows)

    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
