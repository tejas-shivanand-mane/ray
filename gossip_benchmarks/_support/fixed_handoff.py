#!/usr/bin/env python3
"""Fixed-R transparently hands off one already-blocked ray.get after acting-owner death.

B1 wins recovery attempt 1 and starts a replay that deliberately blocks. B2 then
starts exactly one ray.get on the same borrowed ObjectRef and follows B1 after
CLAIM_ALREADY_GRANTED. While that very same B2 ray.get is still blocked, B1 is
killed. B2 must transparently re-enter Fixed-R recovery, win/observe attempt 2,
and return the recovered value without an application-level retry.
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

R = 2
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 300
GET_TIMEOUT_S = 120.0
BLOCK_TIMEOUT_S = 240.0
NO_ADVANCE_WINDOW_S = 0.8

ALREADY_CLAIMED_LOG = (
    "Witness-holder baseline recovery already claimed by another acting owner"
)
REENTRY_LOG = (
    "Acting recovery owner became unreachable; re-entering Fixed-R recovery"
)
HANDOFF_LOG = "Transparent recovery-owner handoff kept blocked future alive"


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
        # Both remain unavailable. Execution 2 (attempt 2) returns immediately.
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
            # This method is deliberately invoked exactly once on B2. Benchmark
            # success therefore proves transparent continuation of one blocked
            # application-level ray.get rather than an application retry loop.
            return self.ref.hex(), ray.get(self.ref)

    return Owner, Borrower


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_fixed_r_inflight_owner_handoff_{uuid.uuid4().hex}.csv"
    )
    token = f"inflight-handoff-{uuid.uuid4().hex}"

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

        # B1 wins attempt 1. Its replay starts and blocks indefinitely for the
        # duration of this benchmark unless the B1 process is killed.
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

        # Start B2's one and only application-level get while B1 is still alive.
        # Wait until B2 has definitely observed CLAIM_ALREADY_GRANTED and followed
        # B1 as the acting owner before injecting B1's failure.
        b2_read = borrower_2.read.remote()
        already_claimed = wait_for_log(logs, ALREADY_CLAIMED_LOG, timeout_s=20.0)
        assert already_claimed, find_log_lines(logs, "Witness-holder baseline recovery")

        time.sleep(NO_ADVANCE_WINDOW_S)
        starts_before_b1_death = count_starts(marker, token)
        assert starts_before_b1_death == 2, read_marker(marker)
        assert not find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication attempt 2"
        )

        b1_failure_ns = time.time_ns()
        ray.kill(borrower_1, no_restart=True)

        # Critical assertion: do not issue another B2 read. This is the same
        # ObjectRef.get call started above, and it must survive B1's death.
        recovered_object_id, value = ray.get(b2_read, timeout=GET_TIMEOUT_S)
        assert recovered_object_id == original_object_id
        assert value["token"] == token
        assert len(value["payload"]) == PAYLOAD_BYTES

        wait_for_token_starts(marker, token, 3, timeout_s=10.0)
        starts = [
            wall_ns
            for event, wall_ns, _pid, row_token in read_marker(marker)
            if event == "START" and row_token == token
        ]
        assert len(starts) == 3, read_marker(marker)
        assert starts[2] >= b1_failure_ns, read_marker(marker)

        post_owner_failure_starts = count_starts(
            marker, token, after_ns=owner_failure_ns
        )
        assert post_owner_failure_starts == 2, read_marker(marker)

        attempt2_grants = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication attempt 2",
            timeout_s=10.0,
        )
        assert attempt2_grants, find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )

        reentry_logs = wait_for_log(logs, REENTRY_LOG, timeout_s=10.0)
        assert reentry_logs, find_log_lines(logs, "re-entering Fixed-R recovery")
        handoff_logs = wait_for_log(logs, HANDOFF_LOG, timeout_s=10.0)
        assert handoff_logs, find_log_lines(logs, "recovery-owner handoff")

        all_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert len(all_grants) == 2, all_grants

        print("PASS: Fixed-R transparent in-flight acting-owner handoff")
        print(f"  R                         = {R}")
        print(f"  original ObjectID         = {original_object_id}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  STARTs before B1 death    = {starts_before_b1_death}")
        print(f"  post-owner START count    = {post_owner_failure_starts}")
        print(f"  already-claimed logs      = {len(already_claimed)}")
        print(f"  recovery re-entry logs    = {len(reentry_logs)}")
        print(f"  transparent handoff logs  = {len(handoff_logs)}")
        print(f"  attempt-1 grant logs      = {len(attempt1_grants)}")
        print(f"  attempt-2 grant logs      = {len(attempt2_grants)}")

        # B1's outstanding actor call is intentionally abandoned with its actor.
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
