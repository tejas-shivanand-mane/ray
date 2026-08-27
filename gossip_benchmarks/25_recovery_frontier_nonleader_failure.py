#!/usr/bin/env python3
"""End-to-end Fixed-R + Recovery Frontier non-leader owner-failure test.

This is intentionally a correctness test, not a performance benchmark.

It proves the first K>1 grouped recovery path:
  1. Two independent normal tasks are owned by the same worker and placed in
     one Recovery Frontier group (K=4).
  2. The group's replay recipes are durably published to the same R fixed
     witness-holders before recovery metadata is exported.
  3. The original owner worker is killed.
  4. A borrower requests task #2, which is a non-leader member of the group.
  5. The same ObjectRef/ObjectID resolves after exactly one replay of task #2.
  6. The leader task is not replayed as a side effect.

The first executions block on purpose. A replay sees its token already present
in the marker file and returns immediately. Therefore a successful post-owner-
failure result cannot have come from the original execution.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path

# Keep the recovery/frontier INFO markers visible for the assertions below.
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
    witness_baseline,
)

R = 2
K = 4
NUM_TASKS = 2
TARGET_INDEX = 1
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 45.0
INITIAL_BLOCK_TIMEOUT_S = 120.0


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
            # The old density selector is a perf-only proxy and must not
            # participate in the correctness-capable frontier experiment.
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

        # Each task has a unique token, so this read is only deciding whether
        # this logical task has executed before. Appending the short marker line
        # is atomic enough for this single-host Cluster correctness harness.
        prior_starts = 0
        if marker.exists():
            for line in marker.read_text(errors="replace").splitlines():
                parts = line.split(",", 3)
                if len(parts) == 4 and parts[0] == "START" and parts[3] == token:
                    prior_starts += 1

        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")

        if prior_starts == 0:
            # Keep the original execution unavailable. A replay of the same
            # deterministic TaskSpec sees prior_starts > 0 and skips this wait.
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
            # Submission order defines frontier membership order. The second
            # ObjectRef is therefore deliberately the non-leader target.
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
        f"ray_frontier_nonleader_{uuid.uuid4().hex}.csv"
    )
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_system_config(),
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
        # Dedicated spare raylets make it easy for Fixed-R to choose R
        # node-distinct witnesses without consuming task CPUs.
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
        logs = session_dirs(cluster)

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

        # Both original executions must have started and must still be blocked.
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

        # Returning the refs from Owner.dispatch is itself the owner->borrower
        # export. The acknowledged-prefix barrier makes dispatch completion
        # sufficient for durability; the log assertion additionally proves that
        # the real frontier publication path ran.
        frontier_commits = wait_for_log(
            logs,
            "Committed Recovery Frontier append generation",
            timeout_s=15.0,
        )
        assert frontier_commits, "No committed Recovery Frontier append was observed"

        # Two tasks sharing one frontier should build exactly one protection
        # topology/initial manifest. This is a direct guard against accidentally
        # falling back to two independent per-task baselines.
        profile = ray.get(owner.recovery_profile.remote())
        initial_manifests = int(profile.get("initial_manifest_build_count", 0))
        assert initial_manifests == 1, profile

        witness_sent = int(profile.get("witness_update_rpcs_sent", 0))
        witness_completed = int(profile.get("witness_update_rpcs_completed", 0))
        assert witness_sent >= R, profile
        assert witness_sent <= NUM_TASKS * R, profile
        assert witness_sent == witness_completed, profile
        assert witness_sent % R == 0, profile

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)

        recovered_object_id, value = ray.get(
            borrower.read.remote(),
            timeout=GET_TIMEOUT_S,
        )

        # Object identity must stay deterministic across replay.
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

        # Exactly one replay of the requested non-leader member; no group-wide
        # replay of the leader.
        assert target_post_failure_starts == 1, read_marker(marker)
        assert leader_post_failure_starts == 0, read_marker(marker)

        print("PASS: Fixed-R + Recovery Frontier non-leader owner failure")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  leader ObjectID           = {leader_object_id}")
        print(f"  target ObjectID           = {target_object_id}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        print(f"  initial manifests         = {initial_manifests}")
        print(f"  witness updates sent      = {witness_sent}")
        print(f"  frontier commit log lines = {len(frontier_commits)}")
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
