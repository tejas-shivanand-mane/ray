#!/usr/bin/env python3
"""End-to-end Recovery Frontier K=4 full-group + rollover correctness test.

Five independent normal tasks are submitted by the same owner:

  group 1: task 1, task 2, task 3, task 4
  group 2: task 5

With Fixed-R R=2 this should require exactly two protection topologies and four
witness update RPCs total, rather than five independent per-task protections.

After the owner worker is killed, the test recovers:
  * task 4: a non-leader member at the end of the full first group, and
  * task 5: the leader of the rollover group.

Both original ObjectIDs must be preserved. Each requested task must replay
exactly once, and tasks 1-3 must not replay as a side effect.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

os.environ["RAY_BACKEND_LOG_LEVEL"] = "info"
os.environ["RAY_DEDUP_LOGS"] = "0"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    read_marker,
    safe_shutdown,
    system_config,
    wait_for_cluster,
    wait_for_marker,
    witness_baseline,
)

R = 2
K = 4
NUM_TASKS = 5
GROUP1_TARGET_INDEX = 3  # task 4, non-leader in the first/full group
GROUP2_TARGET_INDEX = 4  # task 5, leader of the rollover group
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 50.0
INITIAL_BLOCK_TIMEOUT_S = 120.0


def frontier_config() -> dict[str, Any]:
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
            # Keep every initial execution unavailable. A recovery replay sees
            # the token already present and therefore returns immediately.
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

        return {
            "token": token,
            "payload": b"x" * payload_bytes,
        }

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
            # All five tasks are submitted before the ObjectRef list is returned,
            # so submission-order frontier membership is deterministic here.
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
        f"ray_frontier_k4_rollover_{uuid.uuid4().hex}.csv"
    )
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_config(),
            include_dashboard=False,
        )
        cluster.add_node(
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
            resources={"borrower_node_1": 1},
        )
        cluster.add_node(
            num_cpus=1,
            resources={"borrower_node_2": 1},
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        expected_nodes = 1 + 1 + 1 + (R + 1) + 2
        wait_for_cluster(ray, expected_nodes, 30.0)

        Owner, Borrower = types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        borrower_group1 = Borrower.options(
            resources={"borrower_node_1": 0.01},
            num_cpus=0,
        ).remote()
        borrower_group2 = Borrower.options(
            resources={"borrower_node_2": 0.01},
            num_cpus=0,
        ).remote()

        tokens = [f"task-{i + 1}-{uuid.uuid4().hex}" for i in range(NUM_TASKS)]

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

        group1_ref = refs[GROUP1_TARGET_INDEX]
        group2_ref = refs[GROUP2_TARGET_INDEX]
        group1_object_id = group1_ref.hex()
        group2_object_id = group2_ref.hex()
        assert group1_object_id != group2_object_id

        borrower_group1_id = ray.get(borrower_group1.hold.remote([group1_ref]))
        borrower_group2_id = ray.get(borrower_group2.hold.remote([group2_ref]))
        assert borrower_group1_id == group1_object_id
        assert borrower_group2_id == group2_object_id

        profile = ray.get(owner.recovery_profile.remote())
        initial_manifests = int(profile.get("initial_manifest_build_count", 0))
        witness_sent = int(profile.get("witness_update_rpcs_sent", 0))
        witness_completed = int(profile.get("witness_update_rpcs_completed", 0))

        expected_groups = 2
        expected_witness_updates = expected_groups * R
        assert initial_manifests == expected_groups, profile
        assert witness_sent == expected_witness_updates, profile
        assert witness_completed == expected_witness_updates, profile

        failure_wall_ns = time.time_ns()
        failure_perf = time.perf_counter()
        ray.kill(owner, no_restart=True)

        # Recover from both groups after the same owner loss. Running the reads
        # concurrently also checks that per-member recovery claims do not make
        # the two groups interfere with each other.
        recovered = ray.get(
            [
                borrower_group1.read.remote(),
                borrower_group2.read.remote(),
            ],
            timeout=GET_TIMEOUT_S,
        )
        failure_to_results_s = time.perf_counter() - failure_perf

        (recovered_group1_id, value_group1), (recovered_group2_id, value_group2) = recovered

        assert recovered_group1_id == group1_object_id
        assert recovered_group2_id == group2_object_id
        assert value_group1["token"] == tokens[GROUP1_TARGET_INDEX]
        assert value_group2["token"] == tokens[GROUP2_TARGET_INDEX]
        assert len(value_group1["payload"]) == PAYLOAD_BYTES
        assert len(value_group2["payload"]) == PAYLOAD_BYTES

        post_failure_starts = {
            token: count_token_starts(marker, token, after_ns=failure_wall_ns)
            for token in tokens
        }

        assert post_failure_starts[tokens[GROUP1_TARGET_INDEX]] == 1, read_marker(marker)
        assert post_failure_starts[tokens[GROUP2_TARGET_INDEX]] == 1, read_marker(marker)
        for index in range(3):
            assert post_failure_starts[tokens[index]] == 0, read_marker(marker)

        print("PASS: Recovery Frontier K=4 full-group + rollover")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  tasks                     = {NUM_TASKS}")
        print(f"  protection groups         = {initial_manifests}")
        print(f"  witness updates sent      = {witness_sent}")
        print(f"  witness updates done      = {witness_completed}")
        print(f"  group1 target ObjectID    = {group1_object_id}")
        print(f"  group1 recovered ObjectID = {recovered_group1_id}")
        print(f"  group2 target ObjectID    = {group2_object_id}")
        print(f"  group2 recovered ObjectID = {recovered_group2_id}")
        print(f"  failure-to-results (s)    = {failure_to_results_s:.3f}")
        for index, token in enumerate(tokens):
            print(
                f"  task {index + 1} post-failure START = "
                f"{post_failure_starts[token]}"
            )

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
