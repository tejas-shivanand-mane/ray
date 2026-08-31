#!/usr/bin/env python3
"""Fixed-R acting-borrower death must advance recovery attempt exactly once.

B1 wins recovery attempt N=1 and starts a replay that deliberately blocks. B2
requests the same object while B1 is still alive and must not start attempt 2.
After B1 is killed, fresh/retried B2 gets may advance to N+1=2 only once the
failure is authoritative, and exactly one replacement replay must complete.

This benchmark intentionally tests the witness claim state machine rather than
transparent migration of a single already-blocked ray.get. The latter is a
separate CoreWorker liveness property.
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
from ray.exceptions import GetTimeoutError
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
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

R = 3
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 300
GET_TIMEOUT_S = 120.0
BLOCK_TIMEOUT_S = 240.0
NO_ADVANCE_WINDOW_S = 0.8
POST_DEATH_PROBE_S = 0.5


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


def wait_for_token_starts(marker: Path, token: str, count: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if count_starts(marker, token) >= count:
            return
        time.sleep(0.02)
    raise TimeoutError(
        f"Expected {count} STARTs for {token}, got {count_starts(marker, token)}"
    )


def types():
    @ray.remote(max_retries=3)
    def work(token: str, marker_path: str, payload_bytes: int):
        marker = Path(marker_path)
        prior = count_starts(marker, token)
        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")

        # Execution 0 is the original task. Execution 1 is B1's recovery replay.
        # Both stay unavailable. Execution 2 (attempt N+1) returns immediately.
        if prior <= 1:
            release = Path(str(marker) + f".release.{token}.{prior}")
            deadline = time.monotonic() + BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Blocked execution {prior} for {token} was not released"
                    )
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

        def read(self):
            return self.ref.hex(), ray.get(self.ref)

        def read_with_timeout(self, timeout_s):
            try:
                value = ray.get(self.ref, timeout=timeout_s)
                return {
                    "ready": True,
                    "object_id": self.ref.hex(),
                    "value": value,
                }
            except GetTimeoutError:
                return {
                    "ready": False,
                    "object_id": self.ref.hex(),
                    "value": None,
                }

    return Owner, Borrower


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_fixed_r_acting_borrower_death_{uuid.uuid4().hex}.csv"
    )
    token = f"acting-owner-{uuid.uuid4().hex}"
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0, _system_config=fixed_r_config(), include_dashboard=False
        )
        cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(
            num_cpus=3, resources={"executor_node": 1}
        )
        for i in range(R + 2):
            cluster.add_node(num_cpus=0, resources={f"witness_pool_{i}": 1})
        cluster.add_node(num_cpus=1, resources={"borrower_1_node": 1})
        cluster.add_node(num_cpus=1, resources={"borrower_2_node": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        expected_nodes = 1 + 1 + 1 + (R + 2) + 2
        wait_for_cluster(ray, expected_nodes, 40.0)
        logs = session_dirs(cluster)

        Owner, Borrower = types()
        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote()
        borrower_1 = Borrower.options(
            resources={"borrower_1_node": 0.01}, num_cpus=0
        ).remote()
        borrower_2 = Borrower.options(
            resources={"borrower_2_node": 0.01}, num_cpus=0
        ).remote()

        ref = ray.get(
            owner.dispatch.remote(
                executor_node.node_id, token, str(marker), PAYLOAD_BYTES
            )
        )
        original_object_id = ref.hex()
        wait_for_marker(marker, "START", timeout_s=10.0, min_count=1)
        assert count_starts(marker, token) == 1, read_marker(marker)
        assert ray.get(borrower_1.hold.remote([ref])) == original_object_id
        assert ray.get(borrower_2.hold.remote([ref])) == original_object_id
        wait_for_protection(
            method=witness_baseline(R), session_paths=logs, timeout_s=30.0
        )

        owner_failure_ns = time.time_ns()
        ray.kill(owner, no_restart=True)

        # B1 wins attempt 1. Its replay starts and intentionally blocks.
        b1_read = borrower_1.read.remote()
        wait_for_token_starts(marker, token, 2, timeout_s=GET_TIMEOUT_S)
        attempt1_grants = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication attempt 1",
            timeout_s=10.0,
        )
        assert attempt1_grants, find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )

        # B2 asks while B1 is alive. It may follow CLAIM_ALREADY_GRANTED, but
        # it must neither produce a result nor create attempt 2.
        pre_death_probe = ray.get(
            borrower_2.read_with_timeout.remote(NO_ADVANCE_WINDOW_S),
            timeout=NO_ADVANCE_WINDOW_S + 10.0,
        )
        assert not pre_death_probe["ready"], pre_death_probe
        starts_before_b1_death = count_starts(marker, token)
        assert starts_before_b1_death == 2, read_marker(marker)
        attempt2_before_death = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication attempt 2"
        )
        assert not attempt2_before_death, attempt2_before_death

        b1_failure_ns = time.time_ns()
        ray.kill(borrower_1, no_restart=True)

        # Worker-failure knowledge is asynchronous. Re-issue bounded gets rather
        # than treating a timeout as authority to advance. The witness protocol
        # itself decides when B1 is authoritatively dead and only then may grant N+1.
        deadline = time.monotonic() + GET_TIMEOUT_S
        post_death_probe = None
        while time.monotonic() < deadline:
            post_death_probe = ray.get(
                borrower_2.read_with_timeout.remote(POST_DEATH_PROBE_S),
                timeout=POST_DEATH_PROBE_S + 10.0,
            )
            if post_death_probe["ready"]:
                break
            time.sleep(0.1)

        assert post_death_probe is not None and post_death_probe["ready"], post_death_probe
        recovered_object_id = post_death_probe["object_id"]
        value = post_death_probe["value"]
        assert recovered_object_id == original_object_id
        assert value["token"] == token
        assert len(value["payload"]) == PAYLOAD_BYTES

        wait_for_token_starts(marker, token, 3, timeout_s=10.0)
        post_owner_failure_starts = count_starts(
            marker, token, after_ns=owner_failure_ns
        )
        assert post_owner_failure_starts == 2, read_marker(marker)

        # The third execution (attempt 2 replay) must occur only after B1 dies.
        third_start_ns = [
            wall_ns
            for event, wall_ns, _pid, row_token in read_marker(marker)
            if event == "START" and row_token == token
        ][2]
        assert third_start_ns >= b1_failure_ns, read_marker(marker)

        attempt2_grants = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication attempt 2",
            timeout_s=10.0,
        )
        assert attempt2_grants, find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        all_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert len(all_grants) == 2, all_grants

        print("PASS: Fixed-R acting borrower death advances exactly one attempt")
        print(f"  R                         = {R}")
        print(f"  original ObjectID         = {original_object_id}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  STARTs before B1 death    = {starts_before_b1_death}")
        print(f"  post-owner START count    = {post_owner_failure_starts}")
        print(f"  attempt-1 grant logs      = {len(attempt1_grants)}")
        print(f"  attempt-2 grant logs      = {len(attempt2_grants)}")
        print(f"  total grant logs          = {len(all_grants)}")

        # B1's outstanding read is expected to fail because B1 was killed.
        _ = b1_read
    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass
        for execution in (0, 1):
            try:
                Path(str(marker) + f".release.{token}.{execution}").unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()