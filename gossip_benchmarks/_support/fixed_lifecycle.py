#!/usr/bin/env python3
"""End-to-end Recovery Frontier group-lifecycle regression.

This test targets the K>1 lifecycle bug that the throughput benchmark exposed.
Two independent tasks share one K=4 frontier group.  The driver then drops the
leader's final ObjectRef while a borrower keeps the non-leader member alive.
The owner must NOT tombstone the shared group at that point.  After the owner is
killed, the still-live non-leader must recover with the same ObjectID and exactly
one replay.

A pre-fix per-task cleanup path could publish the leader TaskID as a tombstone;
because the leader TaskID is also the frontier group ID, holders would erase the
entire group even though the non-leader was still live.  This regression proves
cleanup is now group-liveness aware.
"""
from __future__ import annotations

import gc
import os
import tempfile
import time
import uuid
from pathlib import Path

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from common import (
    read_marker,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    wait_for_marker,
    witness_baseline,
)

R = 2
K = 4
NUM_TASKS = 2
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 45.0
INITIAL_BLOCK_TIMEOUT_S = 120.0
RELEASE_WAIT_S = 20.0


def frontier_system_config() -> dict:
    cfg = system_config(
        witness_baseline(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    cfg.update(
        {
            "enable_recovery_frontier": True,
            "recovery_frontier_group_size": K,
            "recovery_baseline_perf_protect_every_n": 1,
        }
    )
    return cfg


def count_token_starts(marker: Path, token: str, *, after_ns: int = 0) -> int:
    return sum(
        1
        for event, wall_ns, _pid, row_token in read_marker(marker)
        if event == "START" and wall_ns >= after_ns and row_token == token
    )


def make_types():
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
                    raise TimeoutError(f"initial execution never released: {token}")
                time.sleep(0.05)

        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")

        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch(self, executor_node_id: str, tokens: list[str], marker_path: str):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id,
                soft=False,
            )
            return [
                work.options(
                    scheduling_strategy=strategy,
                    num_cpus=0.1,
                ).remote(token, marker_path, PAYLOAD_BYTES)
                for token in tokens
            ]

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return global_worker.core_worker.get_recovery_succession_profile()

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return self.ref.hex()

        def read(self):
            return self.ref.hex(), ray.get(self.ref)

    return Owner, Borrower


def wait_for_owner_release(owner, minimum: int, timeout_s: float) -> dict:
    deadline = time.monotonic() + timeout_s
    last = {}
    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        released = int(last.get("owner_retained_task_specs_released", 0))
        if released >= minimum:
            return last
        gc.collect()
        time.sleep(0.10)
    raise AssertionError(
        f"owner did not observe {minimum} retained-task release(s); last profile={last}"
    )


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_lifecycle_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_system_config(),
            include_dashboard=False,
        )
        cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(
            num_cpus=2,
            resources={"executor_node": 1},
        )
        for i in range(1, R + 2):
            cluster.add_node(
                num_cpus=0,
                resources={f"frontier_holder_{i}": 1},
            )
        cluster.add_node(num_cpus=1, resources={"borrower_node": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        expected_nodes = 1 + 1 + 1 + (R + 1) + 1
        wait_for_cluster(ray, expected_nodes, 30.0)

        Owner, Borrower = make_types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        tokens = [
            f"leader-{uuid.uuid4().hex}",
            f"member-{uuid.uuid4().hex}",
        ]

        refs = ray.get(
            owner.dispatch.remote(executor_node.node_id, tokens, str(marker))
        )
        assert len(refs) == NUM_TASKS

        starts = wait_for_marker(marker, "START", timeout_s=10.0, min_count=NUM_TASKS)
        assert len(starts) >= NUM_TASKS, read_marker(marker)
        for token in tokens:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        leader_ref = refs[0]
        member_ref = refs[1]
        leader_object_id = leader_ref.hex()
        member_object_id = member_ref.hex()
        assert leader_object_id != member_object_id

        borrower_object_id = ray.get(borrower.hold.remote([member_ref]))
        assert borrower_object_id == member_object_id

        before = ray.get(owner.recovery_profile.remote())
        assert int(before.get("initial_manifest_build_count", 0)) == 1, before
        assert int(before.get("witness_update_rpcs_sent", 0)) == R, before
        assert int(before.get("witness_update_rpcs_completed", 0)) == R, before

        # Drop every driver-side reference to the leader.  Keep the member live
        # only through Borrower so the owner's lifecycle callback must observe
        # one dead member and one still-live sibling in the same frontier group.
        refs.clear()
        del refs
        del leader_ref
        gc.collect()

        after_leader_delete = wait_for_owner_release(
            owner, minimum=1, timeout_s=RELEASE_WAIT_S
        )
        released = int(after_leader_delete.get("owner_retained_task_specs_released", 0))
        current = int(after_leader_delete.get("owner_retained_task_specs_current", 0))
        assert released >= 1, after_leader_delete
        assert current >= 1, after_leader_delete

        # The driver no longer needs its member ref either; Borrower keeps the
        # distributed reference live.  This must not trigger a terminal group
        # tombstone because Borrower is still a holder.
        del member_ref
        gc.collect()
        time.sleep(0.5)

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)

        recovered_object_id, value = ray.get(
            borrower.read.remote(),
            timeout=GET_TIMEOUT_S,
        )

        assert recovered_object_id == member_object_id
        assert value["token"] == tokens[1]
        assert len(value["payload"]) == PAYLOAD_BYTES

        member_post_failure = count_token_starts(
            marker, tokens[1], after_ns=failure_wall_ns
        )
        leader_post_failure = count_token_starts(
            marker, tokens[0], after_ns=failure_wall_ns
        )
        assert member_post_failure == 1, read_marker(marker)
        assert leader_post_failure == 0, read_marker(marker)

        print("PASS: Recovery Frontier keeps live sibling after leader ref deletion")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  leader ObjectID           = {leader_object_id}")
        print(f"  member ObjectID           = {member_object_id}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  retained tasks released   = {released}")
        print(f"  retained tasks current    = {current}")
        print(f"  member post-failure START = {member_post_failure}")
        print(f"  leader post-failure START = {leader_post_failure}")

    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass
        for token in locals().get("tokens", []):
            try:
                Path(str(marker) + f".release.{token}").unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()
