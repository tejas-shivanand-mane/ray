#!/usr/bin/env python3
"""
Prototype benchmark: recovery when earlier succession holders are already dead.

What it measures
----------------
For a fixed succession length R, form the full holder list, then kill the first K
holder nodes before killing the owner/original-producer node. A persistent borrower
then reads the object. This measures:

  * recovery success rate vs. number of already-dead holders
  * which succession rank actually accepts recovery
  * failure-to-result latency
  * whether replay occurred (via a local execution marker)
  * execution count, useful for spotting duplicate replay

This is intended for local Cluster()-based prototype testing. Because all logical
nodes live on one physical host, the marker file in /tmp is visible to all workers.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
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


@dataclass
class ClusterLayout:
    cluster: Cluster
    owner_node: Any
    holder_nodes: list[Any]


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


def wait_for_log_line(session_dirs: set[Path], text: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if find_log_lines(session_dirs, text):
            return True
        time.sleep(0.05)
    return False


def accepted_rank(session_dirs: set[Path]) -> int:
    pat = re.compile(r"Recovery succession accepted by holder rank\s+(\d+)")
    ranks: list[int] = []
    for line in find_log_lines(session_dirs, "Recovery succession accepted by holder rank"):
        m = pat.search(line)
        if m:
            ranks.append(int(m.group(1)))
    return ranks[-1] if ranks else -1


def read_marker(path: Path) -> list[tuple[str, int, int]]:
    """Return (event, wall_time_ns, pid) entries."""
    if not path.exists():
        return []
    out: list[tuple[str, int, int]] = []
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(",")
        if len(parts) != 3 or parts[0] not in {"START", "FINISH"}:
            continue
        try:
            out.append((parts[0], int(parts[1]), int(parts[2])))
        except ValueError:
            pass
    return out


def wait_for_first_start(path: Path, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if any(e[0] == "START" for e in read_marker(path)):
            return
        time.sleep(0.02)
    raise TimeoutError("Original task did not begin before timeout")


def start_cluster(
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

    # Head/driver survives.
    cluster.add_node(num_cpus=0, _system_config=config, include_dashboard=False)

    owner_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"owner_node": 1},
    )

    holder_nodes = []
    for rank in range(1, holders + 1):
        holder_nodes.append(
            cluster.add_node(
                num_cpus=max(1, cpus_per_node),
                resources={f"holder_{rank}": 1},
            )
        )

    # Persistent borrower survives.
    cluster.add_node(
        num_cpus=1,
        resources={"borrower_node": 1},
    )

    # Extra witness capacity so witnesses do not have to be only holder/borrower nodes.
    for i in range(max(0, witness_count)):
        cluster.add_node(
            num_cpus=0,
            resources={f"extra_witness_{i+1}": 1},
        )

    return ClusterLayout(cluster=cluster, owner_node=owner_node, holder_nodes=holder_nodes)


def wait_for_cluster(expected: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        alive = sum(1 for n in ray.nodes() if n["Alive"])
        if alive >= expected:
            return
        time.sleep(0.1)
    raise TimeoutError(f"Only {alive}/{expected} logical nodes became alive")


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(seed: int, duration_s: float, payload_bytes: int, marker_path: str) -> bytes:
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()}\n")
        time.sleep(duration_s)
        prefix = seed.to_bytes(8, "little", signed=False)
        value = prefix[:payload_bytes] if payload_bytes <= 8 else prefix + b"x" * (payload_bytes - 8)
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()}\n")
        return value

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, owner_node_id: str):
            self.owner_node_id = owner_node_id

        def dispatch(self, seed: int, duration_s: float, payload_bytes: int, marker_path: str):
            # Soft affinity: original execution prefers the owner failure domain,
            # but replay can run elsewhere after that node disappears.
            ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.owner_node_id,
                    soft=True,
                ),
                num_cpus=1,
            ).remote(seed, duration_s, payload_bytes, marker_path)
            return [ref]

        def ping(self) -> int:
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
    initial_ref,
    holder_actors: list[Any],
    session_dirs: set[Path],
    timeout_s: float,
):
    fresh_ref = initial_ref
    rank_times: list[float] = []

    for rank, holder in enumerate(holder_actors, start=1):
        t0 = time.perf_counter()
        ray.get(holder.hold.remote([fresh_ref]))

        needle = (
            "Committed recovery succession manifest after witness publication "
            f"with {rank + 1} total members"
        )
        if not wait_for_log_line(session_dirs, needle, timeout_s):
            raise RuntimeError(f"Holder rank {rank} did not commit")

        fresh_ref = ray.get(holder.export.remote())[0]
        rank_times.append(time.perf_counter() - t0)

    return fresh_ref, rank_times


FIELDS = [
    "trial",
    "holders",
    "predead_holders",
    "task_duration_s",
    "payload_bytes",
    "formation_success",
    "formation_time_s",
    "success",
    "accepted_rank",
    "replayed",
    "executions_observed",
    "failure_to_result_s",
    "error_type",
    "error_message",
]


def run_trial(args, predead_holders: int, trial: int) -> dict[str, Any]:
    row: dict[str, Any] = {
        "trial": trial,
        "holders": args.holders,
        "predead_holders": predead_holders,
        "task_duration_s": args.task_duration,
        "payload_bytes": args.payload_bytes,
        "formation_success": False,
        "formation_time_s": math.nan,
        "success": False,
        "accepted_rank": -1,
        "replayed": False,
        "executions_observed": 0,
        "failure_to_result_s": math.nan,
        "error_type": "",
        "error_message": "",
    }

    marker = Path(tempfile.gettempdir()) / f"ray_holder_rank_{os.getpid()}_{uuid.uuid4().hex}.csv"
    layout: ClusterLayout | None = None

    try:
        layout = start_cluster(
            holders=args.holders,
            cpus_per_node=args.cpus_per_node,
            witness_count=args.witness_count,
            object_timeout_ms=args.object_timeout_ms,
        )
        ray.init(address=layout.cluster.address, log_to_driver=False, include_dashboard=False)

        expected_nodes = 1 + 1 + args.holders + 1 + args.witness_count
        wait_for_cluster(expected_nodes, args.cluster_timeout)

        Owner, Holder, Borrower = make_remote_types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(layout.owner_node.node_id)
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
            Path(node.get_session_dir_path())
            for node in layout.cluster.list_all_nodes()
        }

        seed = trial * 1000 + predead_holders
        ref = ray.get(
            owner.dispatch.remote(
                seed,
                args.task_duration,
                args.payload_bytes,
                str(marker),
            )
        )[0]

        wait_for_first_start(marker, args.start_timeout)

        formation_start = time.perf_counter()
        ref, _ = form_succession(ref, holders, session_dirs, args.formation_timeout)
        row["formation_success"] = True
        row["formation_time_s"] = time.perf_counter() - formation_start

        ray.get(borrower.hold.remote([ref]))

        # Kill ranks 1..K before the owner. K=holders intentionally tests the
        # boundary where no recovery holder remains.
        for idx in range(predead_holders):
            layout.cluster.remove_node(layout.holder_nodes[idx], allow_graceful=False)

        if args.holder_failure_settle > 0:
            time.sleep(args.holder_failure_settle)

        failure_t0 = time.perf_counter()
        layout.cluster.remove_node(layout.owner_node, allow_graceful=False)

        try:
            value = ray.get(
                borrower.read.remote(args.get_timeout),
                timeout=args.get_timeout + 10,
            )
            row["failure_to_result_s"] = time.perf_counter() - failure_t0
            expected_prefix = seed.to_bytes(8, "little", signed=False)[: min(8, args.payload_bytes)]
            if value[: len(expected_prefix)] != expected_prefix:
                raise RuntimeError("Recovered value has wrong deterministic prefix")
            row["success"] = True
        except Exception as exc:
            row["failure_to_result_s"] = time.perf_counter() - failure_t0
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)[:500]

        events = read_marker(marker)
        starts = [e for e in events if e[0] == "START"]
        row["executions_observed"] = len(starts)
        row["replayed"] = len(starts) >= 2
        row["accepted_rank"] = accepted_rank(session_dirs)

        return row

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("holder_failure_results.csv"))
    parser.add_argument("--holders", type=int, default=4)
    parser.add_argument(
        "--predead-holders",
        type=int,
        nargs="+",
        default=None,
        help="Numbers of earliest holder ranks to kill before owner failure. Default: 0..holders.",
    )
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--task-duration", type=float, default=30.0)
    parser.add_argument("--payload-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--cpus-per-node", type=int, default=1)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--object-timeout-ms", type=int, default=100)
    parser.add_argument("--cluster-timeout", type=float, default=30.0)
    parser.add_argument("--start-timeout", type=float, default=20.0)
    parser.add_argument("--formation-timeout", type=float, default=30.0)
    parser.add_argument("--holder-failure-settle", type=float, default=1.0)
    parser.add_argument("--get-timeout", type=float, default=120.0)
    args = parser.parse_args()

    if args.holders < 1:
        raise SystemExit("--holders must be >= 1")

    cases = args.predead_holders
    if cases is None:
        cases = list(range(0, args.holders + 1))

    for k in cases:
        if k < 0 or k > args.holders:
            raise SystemExit(f"Invalid predead holder count {k}; expected 0..{args.holders}")

    rows: list[dict[str, Any]] = []
    run_no = 0
    total = len(cases) * args.trials

    for k in cases:
        for trial in range(1, args.trials + 1):
            run_no += 1
            print(f"\n[{run_no}/{total}] predead_holders={k} trial={trial}")
            row = run_trial(args, k, trial)
            rows.append(row)
            print(
                f"  success={row['success']} accepted_rank={row['accepted_rank']} "
                f"executions={row['executions_observed']} "
                f"failure_to_result={row['failure_to_result_s']:.3f}s"
            )

            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)

    print(f"\nSaved {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
