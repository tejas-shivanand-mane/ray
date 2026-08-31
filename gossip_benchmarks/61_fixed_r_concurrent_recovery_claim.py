#!/usr/bin/env python3
"""Concurrent Fixed-R recovery-claim safety regression.

Two node-distinct borrowers hold the same ObjectRef. After the owner dies,
both request it concurrently. Correct recovery preserves the ObjectID and
executes exactly one post-failure replay. Cross-witness timeout/failover fault
injection is intentionally left for a follow-up test after this core path builds.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    read_marker,
    safe_shutdown,
    session_dirs,
    system_config,
    wait_for_cluster,
    wait_for_log,
    wait_for_marker,
    wait_for_protection,
    witness_baseline,
)

R = 3
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 60.0
INITIAL_BLOCK_TIMEOUT_S = 120.0


def fixed_r_config() -> dict:
    return system_config(
        witness_baseline(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )


def count_starts(marker: Path, token: str, *, after_ns: int = 0) -> int:
    return sum(
        1
        for event, wall_ns, _pid, row_token in read_marker(marker)
        if event == "START" and wall_ns >= after_ns and row_token == token
    )


def types():
    @ray.remote(max_retries=2)
    def work(token: str, marker_path: str, payload_bytes: int):
        marker = Path(marker_path)
        prior_starts = 0
        if marker.exists():
            for line in marker.read_text(errors="replace").splitlines():
                parts = line.split(",", 3)
                if len(parts) == 4 and parts[0] == "START" and parts[3] == token:
                    prior_starts += 1

        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")

        if prior_starts == 0:
            release = Path(str(marker) + f".release.{token}")
            deadline = time.monotonic() + INITIAL_BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Initial execution for {token} was never released"
                    )
                time.sleep(0.05)

        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch(self, executor_node_id, token, marker_path, payload_bytes):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id,
                soft=False,
            )
            return work.options(
                scheduling_strategy=strategy,
                num_cpus=0.1,
            ).remote(token, marker_path, payload_bytes)

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return self.ref.hex()

        def read_after(self, barrier_path):
            barrier = Path(barrier_path)
            deadline = time.monotonic() + GET_TIMEOUT_S
            while not barrier.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Recovery barrier was never released")
                time.sleep(0.01)
            return self.ref.hex(), ray.get(self.ref)

    return Owner, Borrower


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_fixed_r_claim_race_{uuid.uuid4().hex}.csv"
    )
    barrier = Path(str(marker) + ".barrier")
    token = f"claim-race-{uuid.uuid4().hex}"
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=fixed_r_config(),
            include_dashboard=False,
        )
        cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(
            num_cpus=1,
            resources={"executor_node": 1},
        )
        for i in range(1, R + 2):
            cluster.add_node(
                num_cpus=0,
                resources={f"fixed_r_witness_{i}": 1},
            )
        cluster.add_node(num_cpus=1, resources={"borrower_a_node": 1})
        cluster.add_node(num_cpus=1, resources={"borrower_b_node": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        expected_nodes = 1 + 1 + 1 + (R + 1) + 2
        wait_for_cluster(ray, expected_nodes, 30.0)
        logs = session_dirs(cluster)

        Owner, Borrower = types()
        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote()
        borrower_a = Borrower.options(
            resources={"borrower_a_node": 0.01}, num_cpus=0
        ).remote()
        borrower_b = Borrower.options(
            resources={"borrower_b_node": 0.01}, num_cpus=0
        ).remote()

        ref = ray.get(
            owner.dispatch.remote(
                executor_node.node_id,
                token,
                str(marker),
                PAYLOAD_BYTES,
            )
        )
        original_object_id = ref.hex()
        wait_for_marker(marker, "START", timeout_s=10.0, min_count=1)
        assert count_starts(marker, token) == 1, read_marker(marker)
        assert ray.get(borrower_a.hold.remote([ref])) == original_object_id
        assert ray.get(borrower_b.hold.remote([ref])) == original_object_id

        wait_for_protection(
            method=witness_baseline(R),
            session_paths=logs,
            timeout_s=20.0,
        )

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)
        read_a = borrower_a.read_after.remote(str(barrier))
        read_b = borrower_b.read_after.remote(str(barrier))
        time.sleep(0.5)
        barrier.touch()

        result_a, result_b = ray.get([read_a, read_b], timeout=GET_TIMEOUT_S)
        id_a, value_a = result_a
        id_b, value_b = result_b
        assert id_a == original_object_id
        assert id_b == original_object_id
        assert value_a["token"] == token
        assert value_b["token"] == token
        assert len(value_a["payload"]) == PAYLOAD_BYTES
        assert len(value_b["payload"]) == PAYLOAD_BYTES

        post_failure_starts = count_starts(
            marker, token, after_ns=failure_wall_ns
        )
        assert post_failure_starts == 1, read_marker(marker)

        grant_logs = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication",
            timeout_s=5.0,
        )
        replicated_logs = wait_for_log(
            logs,
            "Fixed-R recovery claim replicated at witness index",
            timeout_s=5.0,
            min_count=R - 1,
        )
        assert grant_logs, "No replicated Fixed-R recovery grant was observed"
        assert len(replicated_logs) >= R - 1, replicated_logs

        print("PASS: Fixed-R concurrent recovery claim safety")
        print(f"  R                         = {R}")
        print(f"  original ObjectID         = {original_object_id}")
        print(f"  borrower A ObjectID       = {id_a}")
        print(f"  borrower B ObjectID       = {id_b}")
        print(f"  post-failure START count  = {post_failure_starts}")
        print(f"  replicated claim logs     = {len(replicated_logs)}")
        print(f"  grant logs                = {len(grant_logs)}")
    finally:
        safe_shutdown(ray, cluster)
        for path in (
            marker,
            barrier,
            Path(str(marker) + f".release.{token}"),
        ):
            try:
                path.unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()
