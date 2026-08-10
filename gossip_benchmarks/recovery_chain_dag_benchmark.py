#!/usr/bin/env python3
"""
Experimental prototype benchmark: recovery of a chained DAG.

Inspired by reconstruction_hpcc.py, but adapted to the current recovery-succession
local Cluster() setup.

An owner actor submits an entire chain of normal tasks:
    source -> stage1 -> stage2 -> ... -> stageN

The final ObjectRef is retained by explicit recovery holders and a persistent
borrower. The owner/original-compute node is removed while the chain is still in
flight, then the borrower requests the final result.

Why this benchmark is useful:
  * it is not just a single independent task
  * replay of a downstream stage can require recovery of its dependencies
  * it stresses recovery metadata carried inside TaskSpec arguments
  * it can reveal stale-manifest and recursive-recovery bugs

This benchmark is intentionally "experimental": a failure is useful information
because it points to DAG/dependency cases that the algorithm still needs to fix.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
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


CASES = [Case("Disabled", False), Case("Enabled", True)]


def read_marker(path: Path) -> dict[int, dict[str, list[tuple[int, int]]]]:
    data: dict[int, dict[str, list[tuple[int, int]]]] = {}
    if not path.exists():
        return data

    for line in path.read_text(errors="replace").splitlines():
        parts = line.split(",")
        if len(parts) != 4 or parts[0] not in {"START", "FINISH"}:
            continue
        try:
            event = parts[0]
            stage = int(parts[1])
            t_ns = int(parts[2])
            pid = int(parts[3])
        except ValueError:
            continue
        data.setdefault(stage, {"START": [], "FINISH": []})[event].append((t_ns, pid))
    return data


def wait_for_started_stages(path: Path, minimum: int, timeout_s: float) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        data = read_marker(path)
        started = sum(1 for v in data.values() if v["START"])
        if started >= minimum:
            return started
        time.sleep(0.05)
    return sum(1 for v in read_marker(path).values() if v["START"])


def find_log_lines(session_dirs: set[Path], text: str) -> list[str]:
    out = []
    for session_dir in session_dirs:
        log_dir = session_dir / "logs"
        if not log_dir.exists():
            continue
        for p in log_dir.glob("*"):
            if not p.is_file():
                continue
            try:
                content = p.read_text(errors="replace")
            except OSError:
                continue
            out.extend(f"{p.name}: {line}" for line in content.splitlines() if text in line)
    return out


def wait_for_log_line(session_dirs: set[Path], text: str, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if find_log_lines(session_dirs, text):
            return True
        time.sleep(0.05)
    return False


def start_cluster(
    case: Case,
    holders: int,
    cpus_per_node: int,
    witness_count: int,
    object_timeout_ms: int,
):
    cluster = Cluster()
    config: dict[str, Any] = {
        "enable_recovery_succession": case.enabled,
        "recovery_succession_witness_count": witness_count,
        "object_timeout_milliseconds": object_timeout_ms,
    }
    if case.enabled:
        config["recovery_succession_target_holder_count"] = holders

    cluster.add_node(num_cpus=0, _system_config=config, include_dashboard=False)
    failure_node = cluster.add_node(
        num_cpus=max(1, cpus_per_node),
        resources={"failure_node": 1},
    )
    for rank in range(1, holders + 1):
        cluster.add_node(
            num_cpus=max(1, cpus_per_node),
            resources={f"holder_{rank}": 1},
        )
    cluster.add_node(num_cpus=1, resources={"borrower_node": 1})
    for i in range(witness_count):
        cluster.add_node(num_cpus=0, resources={f"extra_witness_{i+1}": 1})
    return cluster, failure_node


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
    def source(
        seed: int,
        delay_ms: int,
        payload_bytes: int,
        marker_path: str,
    ):
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"START,0,{time.time_ns()},{os.getpid()}\n")
        time.sleep(delay_ms / 1000.0)
        prefix = seed.to_bytes(8, "little", signed=False)
        value = prefix[:payload_bytes] if payload_bytes <= 8 else prefix + b"x" * (payload_bytes - 8)
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,0,{time.time_ns()},{os.getpid()}\n")
        return value

    @ray.remote(max_retries=2)
    def stage(stage_index: int, dep: bytes, delay_ms: int, marker_path: str):
        with open(marker_path, "a", buffering=1) as f:
            f.write(f"START,{stage_index},{time.time_ns()},{os.getpid()}\n")
        time.sleep(delay_ms / 1000.0)

        # Deterministic, cheap transformation that preserves payload size.
        if dep:
            first = bytes([(dep[0] + stage_index) % 256])
            value = first + dep[1:]
        else:
            value = dep

        with open(marker_path, "a", buffering=1) as f:
            f.write(f"FINISH,{stage_index},{time.time_ns()},{os.getpid()}\n")
        return value

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, failure_node_id: str):
            self.failure_node_id = failure_node_id

        def dispatch_chain(
            self,
            chain_length: int,
            seed: int,
            delay_ms: int,
            payload_bytes: int,
            marker_path: str,
        ):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=self.failure_node_id,
                soft=True,
            )
            ref = source.options(
                scheduling_strategy=strategy,
                num_cpus=1,
            ).remote(seed, delay_ms, payload_bytes, marker_path)

            for i in range(1, chain_length):
                ref = stage.options(
                    scheduling_strategy=strategy,
                    num_cpus=1,
                ).remote(i, ref, delay_ms, marker_path)

            return [ref]

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


def form_final_ref(ref, holders, session_dirs, timeout_s):
    fresh = ref
    for rank, holder in enumerate(holders, start=1):
        ray.get(holder.hold.remote([fresh]))
        needle = (
            "Committed recovery succession manifest after witness publication "
            f"with {rank + 1} total members"
        )
        if not wait_for_log_line(session_dirs, needle, timeout_s):
            raise RuntimeError(f"Final task did not commit holder rank {rank}")
        fresh = ray.get(holder.export.remote())[0]
    return fresh


def expected_first_byte(seed: int, chain_length: int) -> int:
    value = seed.to_bytes(8, "little", signed=False)[0]
    for i in range(1, chain_length):
        value = (value + i) % 256
    return value


FIELDS = [
    "trial",
    "config",
    "recovery_enabled",
    "chain_length",
    "delay_ms",
    "payload_bytes",
    "holders",
    "formation_success",
    "stages_started_before_failure",
    "success",
    "failure_to_result_s",
    "stages_ever_started",
    "stages_finished",
    "stages_with_replay",
    "stages_with_gt2_starts",
    "max_starts_for_one_stage",
    "error_type",
    "error_message",
]


def run_trial(case: Case, args, trial: int):
    row = {
        "trial": trial,
        "config": case.label,
        "recovery_enabled": int(case.enabled),
        "chain_length": args.chain_length,
        "delay_ms": args.delay_ms,
        "payload_bytes": args.payload_bytes,
        "holders": args.holders if case.enabled else 0,
        "formation_success": not case.enabled,
        "stages_started_before_failure": 0,
        "success": False,
        "failure_to_result_s": math.nan,
        "stages_ever_started": 0,
        "stages_finished": 0,
        "stages_with_replay": 0,
        "stages_with_gt2_starts": 0,
        "max_starts_for_one_stage": 0,
        "error_type": "",
        "error_message": "",
    }

    marker = Path(tempfile.gettempdir()) / f"ray_chain_recovery_{os.getpid()}_{uuid.uuid4().hex}.csv"
    cluster = None

    try:
        cluster, failure_node = start_cluster(
            case,
            args.holders,
            args.cpus_per_node,
            args.witness_count,
            args.object_timeout_ms,
        )
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)

        expected = 1 + 1 + args.holders + 1 + args.witness_count
        wait_for_cluster(expected, args.cluster_timeout)

        Owner, Holder, Borrower = make_remote_types()

        owner = Owner.options(
            resources={"failure_node": 0.01},
            num_cpus=0,
        ).remote(failure_node.node_id)
        ray.get(owner.ping.remote())

        holders = [
            Holder.options(resources={f"holder_{rank}": 0.01}, num_cpus=0).remote()
            for rank in range(1, args.holders + 1)
        ]
        if holders:
            ray.get([h.ping.remote() for h in holders])

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        session_dirs = {
            Path(n.get_session_dir_path())
            for n in cluster.list_all_nodes()
        }

        seed = 1000 * trial + args.chain_length
        final_ref = ray.get(
            owner.dispatch_chain.remote(
                args.chain_length,
                seed,
                args.delay_ms,
                args.payload_bytes,
                str(marker),
            )
        )[0]

        if case.enabled:
            final_ref = form_final_ref(
                final_ref, holders, session_dirs, args.formation_timeout
            )
            row["formation_success"] = True

        ray.get(borrower.hold.remote([final_ref]))

        # Wait until some prefix of the chain has started, then fail the node.
        row["stages_started_before_failure"] = wait_for_started_stages(
            marker,
            min(args.minimum_started_stages, args.chain_length),
            args.start_timeout,
        )

        if args.failure_delay > 0:
            time.sleep(args.failure_delay)

        failure_t0 = time.perf_counter()
        cluster.remove_node(failure_node, allow_graceful=False)

        try:
            value = ray.get(
                borrower.read.remote(args.get_timeout),
                timeout=args.get_timeout + 10,
            )
            row["failure_to_result_s"] = time.perf_counter() - failure_t0

            if args.payload_bytes > 0:
                expected = expected_first_byte(seed, args.chain_length)
                if value[0] != expected:
                    raise RuntimeError(
                        f"Wrong final value: first byte {value[0]} != expected {expected}"
                    )
            row["success"] = True
        except Exception as exc:
            row["failure_to_result_s"] = time.perf_counter() - failure_t0
            row["error_type"] = type(exc).__name__
            row["error_message"] = str(exc)[:500]

        data = read_marker(marker)
        starts = [len(v["START"]) for v in data.values()]
        finishes = [len(v["FINISH"]) for v in data.values()]
        row["stages_ever_started"] = sum(1 for n in starts if n > 0)
        row["stages_finished"] = sum(1 for n in finishes if n > 0)
        row["stages_with_replay"] = sum(1 for n in starts if n >= 2)
        row["stages_with_gt2_starts"] = sum(1 for n in starts if n > 2)
        row["max_starts_for_one_stage"] = max(starts, default=0)

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
        except OSError:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("chain_recovery_results.csv"))
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--chain-length", type=int, default=20)
    parser.add_argument("--delay-ms", type=int, default=500)
    parser.add_argument("--payload-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--holders", type=int, default=2)
    parser.add_argument("--cpus-per-node", type=int, default=2)
    parser.add_argument("--witness-count", type=int, default=2)
    parser.add_argument("--object-timeout-ms", type=int, default=100)
    parser.add_argument("--minimum-started-stages", type=int, default=2)
    parser.add_argument("--failure-delay", type=float, default=0.0)
    parser.add_argument("--cluster-timeout", type=float, default=30.0)
    parser.add_argument("--start-timeout", type=float, default=20.0)
    parser.add_argument("--formation-timeout", type=float, default=30.0)
    parser.add_argument("--get-timeout", type=float, default=150.0)
    parser.add_argument(
        "--systems",
        nargs="+",
        choices=["disabled", "enabled"],
        default=["disabled", "enabled"],
    )
    args = parser.parse_args()

    selected = [c for c in CASES if c.label.lower() in set(args.systems)]

    rows = []
    total = len(selected) * args.trials
    run_no = 0

    for case in selected:
        for trial in range(1, args.trials + 1):
            run_no += 1
            print(f"\n[{run_no}/{total}] {case.label} trial={trial}")
            row = run_trial(case, args, trial)
            rows.append(row)
            print(
                f"  success={row['success']} result={row['failure_to_result_s']:.3f}s "
                f"replayed_stages={row['stages_with_replay']} "
                f"gt2={row['stages_with_gt2_starts']}"
            )

            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)

    print(f"\nSaved to {args.output}")


if __name__ == "__main__":
    main()
