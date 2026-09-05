#!/usr/bin/env python3
"""Fixed-R live-but-unresponsive W1 must not authorize promotion.

The first witness raylet is SIGSTOP'ed but remains GCS-ALIVE. Owner failure and
two concurrent borrowers are then triggered. No replay may begin while W1 is
merely unresponsive. After SIGCONT, W1 must coordinate exactly one replay.
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

from common import (
    find_log_lines,
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
from fixed_common import (
    assert_node_alive,
    continue_raylet,
    fixed_r_witness_order,
    node_id_hex,
    same_node,
    stop_raylet,
)

R = 2
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 200
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 180.0
LIVE_STALL_S = 0.8
MAX_SELECTION_ATTEMPTS = 20


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
        prior = count_starts(marker, token)
        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")
        if prior == 0:
            release = Path(str(marker) + f".release.{token}")
            deadline = time.monotonic() + INITIAL_BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Initial execution for {token} was not released")
                time.sleep(0.05)
        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch(self, executor_node_id, token, marker_path, payload_bytes):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id, soft=False
            )
            return work.options(
                scheduling_strategy=strategy, num_cpus=0.1
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
        f"ray_fixed_r_live_stall_{uuid.uuid4().hex}.csv"
    )
    barrier = Path(str(marker) + ".barrier")
    tokens: list[str] = []
    stopped_node = None
    try:
        cluster = Cluster()
        head_node = cluster.add_node(
            num_cpus=0, _system_config=fixed_r_config(), include_dashboard=False
        )
        owner_node = cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(num_cpus=2, resources={"executor_node": 1})
        spare_nodes = [
            cluster.add_node(num_cpus=0, resources={f"witness_pool_{i}": 1})
            for i in range(6)
        ]
        borrower_a_node = cluster.add_node(
            num_cpus=1, resources={"borrower_a_node": 1}
        )
        borrower_b_node = cluster.add_node(
            num_cpus=1, resources={"borrower_b_node": 1}
        )
        all_nodes = [
            head_node,
            owner_node,
            executor_node,
            *spare_nodes,
            borrower_a_node,
            borrower_b_node,
        ]

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, len(all_nodes), 40.0)
        logs = session_dirs(cluster)
        Owner, Borrower = types()
        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote()

        selected = None
        for _ in range(MAX_SELECTION_ATTEMPTS):
            token = f"live-stall-{uuid.uuid4().hex}"
            tokens.append(token)
            ref = ray.get(
                owner.dispatch.remote(
                    executor_node.node_id, token, str(marker), PAYLOAD_BYTES
                )
            )
            wait_for_marker(marker, "START", timeout_s=10.0, min_count=len(tokens))
            order = fixed_r_witness_order(ref, all_nodes, R)
            w1 = order[0]
            if (
                not same_node(w1, head_node)
                and not same_node(w1, owner_node)
                and not same_node(w1, executor_node)
                and not same_node(w1, borrower_a_node)
                and not same_node(w1, borrower_b_node)
            ):
                selected = (token, ref, order)
                break
            Path(str(marker) + f".release.{token}").touch()
            ray.get(ref, timeout=15.0)

        assert selected is not None, "Could not select a task with an isolated W1"
        token, ref, order = selected
        w1, w2 = order
        original_object_id = ref.hex()

        borrower_a = Borrower.options(
            resources={"borrower_a_node": 0.01}, num_cpus=0
        ).remote()
        borrower_b = Borrower.options(
            resources={"borrower_b_node": 0.01}, num_cpus=0
        ).remote()
        assert ray.get(borrower_a.hold.remote([ref])) == original_object_id
        assert ray.get(borrower_b.hold.remote([ref])) == original_object_id
        wait_for_protection(
            method=witness_baseline(R), session_paths=logs, timeout_s=30.0
        )

        # Freeze only W1's raylet process. The node stays registered ALIVE,
        # distinguishing transport/unresponsiveness from authoritative death.
        stop_raylet(w1)
        stopped_node = w1
        assert_node_alive(ray, node_id_hex(w1))

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)
        read_a = borrower_a.read_after.remote(str(barrier))
        read_b = borrower_b.read_after.remote(str(barrier))
        barrier.touch()

        time.sleep(LIVE_STALL_S)
        assert_node_alive(ray, node_id_hex(w1))
        starts_while_stalled = count_starts(marker, token, after_ns=failure_wall_ns)
        assert starts_while_stalled == 0, read_marker(marker)
        grants_while_stalled = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert not grants_while_stalled, grants_while_stalled

        continue_raylet(w1)
        stopped_node = None

        result_a, result_b = ray.get([read_a, read_b], timeout=GET_TIMEOUT_S)
        id_a, value_a = result_a
        id_b, value_b = result_b
        assert id_a == original_object_id
        assert id_b == original_object_id
        assert value_a["token"] == token
        assert value_b["token"] == token

        post_failure_starts = count_starts(marker, token, after_ns=failure_wall_ns)
        assert post_failure_starts == 1, read_marker(marker)
        grant_lines = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication attempt 1 coordinator index 0",
            timeout_s=10.0,
        )
        assert grant_lines, find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        all_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert len(all_grants) == 1, all_grants

        print("PASS: Fixed-R live witness stall does not promote")
        print(f"  R                         = {R}")
        print(f"  stalled W1               = {node_id_hex(w1)}")
        print(f"  W2                        = {node_id_hex(w2)}")
        print(f"  W1 remained GCS-ALIVE     = 1")
        print(f"  stalled duration (s)      = {LIVE_STALL_S:.3f}")
        print(f"  START while stalled       = {starts_while_stalled}")
        print(f"  final post-failure START  = {post_failure_starts}")
        print(f"  grant logs                = {len(all_grants)}")
    finally:
        if stopped_node is not None:
            try:
                continue_raylet(stopped_node)
            except Exception:
                pass
        safe_shutdown(ray, cluster)
        for path in (marker, barrier):
            try:
                path.unlink()
            except OSError:
                pass
        for token in tokens:
            try:
                Path(str(marker) + f".release.{token}").unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()
