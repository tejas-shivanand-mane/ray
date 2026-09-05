#!/usr/bin/env python3
"""End-to-end Fixed-R + Recovery Frontier owner-node failure test.

This is the node-failure counterpart to the successful non-leader worker-failure
proof. The owner and executor are deliberately placed on different Ray nodes.
We remove the entire owner node, keep the executor node alive, and require a
borrower to recover task #2 from a K=4 frontier.

Correctness conditions:
  * two independent tasks share one Recovery Frontier protection topology,
  * the target is the non-leader task (#2),
  * Fixed-R publishes the grouped append to exactly R holders,
  * the owner node is removed ungracefully,
  * the original ObjectID is preserved,
  * exactly one post-failure replay of the target occurs,
  * the leader is not replayed.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

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
TARGET_INDEX = 1
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 180.0


def frontier_system_config() -> dict:
    config = system_config(
        witness_baseline(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    config.update(
        {
            "enable_recovery_frontier": True,
            "recovery_frontier_group_size": K,
            # Disable any interpretation of the old perf-only density proxy.
            "recovery_baseline_perf_protect_every_n": 1,
        }
    )
    return config


def count_token_starts(marker: Path, token: str, *, after_ns: int = 0) -> int:
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
            # The original execution stays unavailable. The replay sees the
            # existing START marker and returns immediately.
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
        def dispatch(
            self,
            executor_node_id: str,
            tokens: list[str],
            marker_path: str,
            payload_bytes: int,
        ):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id,
                soft=False,
            )
            return [
                work.options(
                    scheduling_strategy=strategy,
                    num_cpus=0.1,
                ).remote(token, marker_path, payload_bytes)
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


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_nonleader_node_failure_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_system_config(),
            include_dashboard=False,
        )
        owner_node = cluster.add_node(
            num_cpus=1,
            resources={"owner_node": 1},
        )
        executor_node = cluster.add_node(
            num_cpus=2,
            resources={"executor_node": 1},
        )
        for i in range(1, R + 2):
            cluster.add_node(
                num_cpus=0,
                resources={f"frontier_holder_{i}": 1},
            )
        cluster.add_node(
            num_cpus=1,
            resources={"borrower_node": 1},
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        expected_nodes = 1 + 1 + 1 + (R + 1) + 1
        wait_for_cluster(ray, expected_nodes, 30.0)

        Owner, Borrower = types()
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
            owner.dispatch.remote(
                executor_node.node_id,
                tokens,
                str(marker),
                PAYLOAD_BYTES,
            )
        )
        assert len(refs) == NUM_TASKS

        starts = wait_for_marker(
            marker,
            "START",
            timeout_s=10.0,
            min_count=NUM_TASKS,
        )
        assert len(starts) >= NUM_TASKS, read_marker(marker)
        for token in tokens:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        leader_ref = refs[0]
        target_ref = refs[TARGET_INDEX]
        leader_object_id = leader_ref.hex()
        target_object_id = target_ref.hex()
        assert target_object_id != leader_object_id

        borrower_object_id = ray.get(borrower.hold.remote([target_ref]))
        assert borrower_object_id == target_object_id

        # The export/ACK barrier completed before dispatch returned. Prove that
        # this was grouped protection rather than two independent baselines.
        profile = ray.get(owner.recovery_profile.remote())
        initial_manifests = int(profile.get("initial_manifest_build_count", 0))
        witness_sent = int(profile.get("witness_update_rpcs_sent", 0))
        witness_completed = int(profile.get("witness_update_rpcs_completed", 0))

        assert initial_manifests == 1, profile
        assert witness_sent == R, profile
        assert witness_completed == R, profile

        failure_wall_ns = time.time_ns()
        failure_start = time.perf_counter()

        # Remove the entire owner node. The executor node is deliberately not
        # removed, so this isolates owner-node loss from executor-node loss.
        cluster.remove_node(owner_node, allow_graceful=False)

        recovered_object_id, value = ray.get(
            borrower.read.remote(),
            timeout=GET_TIMEOUT_S,
        )
        failure_to_result_s = time.perf_counter() - failure_start

        assert recovered_object_id == target_object_id
        assert value["token"] == tokens[TARGET_INDEX]
        assert len(value["payload"]) == PAYLOAD_BYTES

        target_post_failure_starts = count_token_starts(
            marker,
            tokens[TARGET_INDEX],
            after_ns=failure_wall_ns,
        )
        leader_post_failure_starts = count_token_starts(
            marker,
            tokens[0],
            after_ns=failure_wall_ns,
        )

        assert target_post_failure_starts == 1, read_marker(marker)
        assert leader_post_failure_starts == 0, read_marker(marker)

        print("PASS: Fixed-R + Recovery Frontier non-leader owner-node failure")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  leader ObjectID           = {leader_object_id}")
        print(f"  target ObjectID           = {target_object_id}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  initial manifests         = {initial_manifests}")
        print(f"  witness updates sent      = {witness_sent}")
        print(f"  failure-to-result (s)     = {failure_to_result_s:.3f}")
        print(f"  target post-failure START = {target_post_failure_starts}")
        print(f"  leader post-failure START = {leader_post_failure_starts}")

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
