#!/usr/bin/env python3
"""Recovery Frontier + Recovery Succession non-leader owner-failure target.

This is intentionally a correctness/integration benchmark, not a performance
benchmark.  It distinguishes true Frontier+Succession composition from simply
running ordinary task-centric Succession with the Frontier flag present.

Target semantics (R=2, K=4):
  1. Two independent normal tasks owned by the same worker join one Frontier.
  2. Two distinct borrower workers receive both member refs.
  3. The Frontier has ONE shared Succession topology.  Therefore the owner
     creates one initial recovery manifest and commits exactly R non-owner
     holder admissions for the group, not R admissions per member.
  4. Both member recipes are available at the admitted group holders.
  5. The original owner worker is killed.
  6. A borrower requests task #2, the non-leader member.
  7. The same ObjectRef/ObjectID resolves after exactly one replay of task #2.
  8. The leader task is not replayed as a side effect.

Before the source integration is complete this benchmark is expected to fail
its shared-topology assertions.  A recovery-only pass is NOT sufficient: the
point is to prove that Frontier is the Succession protection/recovery unit while
each member remains the replay unit.
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
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
)

R = 2
K = 4
NUM_TASKS = 2
TARGET_INDEX = 1
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 60.0
INITIAL_BLOCK_TIMEOUT_S = 180.0
PROTECTION_TIMEOUT_S = 30.0


def frontier_succession_system_config() -> dict:
    config = system_config(
        succession(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    config.update(
        {
            "enable_recovery_frontier": True,
            "recovery_frontier_group_size": K,
            # This knob belongs to the old fixed-R density proxy and must not
            # participate in the real Frontier+Succession experiment.
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
            # Keep every original execution unavailable.  A replay of this
            # deterministic logical task observes its prior START and returns.
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
        def hold(self, wrapped_refs):
            # Nested refs force normal task-argument recovery metadata transport
            # while keeping the ObjectRefs alive on this borrower.
            self.refs = list(wrapped_refs)
            return [ref.hex() for ref in self.refs]

        def read(self, index: int):
            ref = self.refs[index]
            return ref.hex(), ray.get(ref)

    return Owner, Borrower


def wait_for_shared_protection(owner, timeout_s: float) -> dict:
    deadline = time.monotonic() + timeout_s
    last: dict = {}
    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        manifests = int(last.get("initial_manifest_build_count", 0))
        admissions = int(last.get("holder_admissions_committed", 0))
        max_holders = int(last.get("max_non_owner_holders", 0))
        if manifests >= 1 and admissions >= R and max_holders >= R:
            return last
        time.sleep(0.05)
    raise TimeoutError(f"Frontier+Succession protection did not become ready: {last}")


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_succession_nonleader_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_succession_system_config(),
            include_dashboard=False,
        )
        cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(
            num_cpus=2,
            resources={"executor_node": 1},
        )
        # Succession witnesses are control-plane durability nodes.  They are
        # distinct from the CoreWorker holders admitted below.
        for i in range(R):
            cluster.add_node(
                num_cpus=0,
                resources={f"witness_node_{i}": 1},
            )
        for i in range(R):
            cluster.add_node(
                num_cpus=1,
                resources={f"borrower_node_{i}": 1},
            )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
        expected_nodes = 1 + 1 + 1 + R + R
        wait_for_cluster(ray, expected_nodes, 30.0)

        Owner, Borrower = types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        borrowers = [
            Borrower.options(
                resources={f"borrower_node_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(R)
        ]

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

        object_ids = [ref.hex() for ref in refs]
        assert len(set(object_ids)) == NUM_TASKS

        # Both distinct borrowers receive both Frontier members.  With ordinary
        # task-centric Succession this creates independent admissions per task;
        # with the intended composition each borrower is admitted once for the
        # shared Frontier topology and receives the grouped replay recipes.
        held_ids = ray.get(
            [borrower.hold.remote(refs) for borrower in borrowers]
        )
        for ids in held_ids:
            assert ids == object_ids, (ids, object_ids)

        profile = wait_for_shared_protection(owner, PROTECTION_TIMEOUT_S)
        initial_manifests = int(profile.get("initial_manifest_build_count", 0))
        admissions = int(profile.get("holder_admissions_committed", 0))
        max_holders = int(profile.get("max_non_owner_holders", 0))

        # These are the decisive composition assertions.  R holders protect one
        # Frontier, rather than R holders independently protecting each member.
        assert initial_manifests == 1, (
            "Expected one group recovery manifest; Frontier is still task-centric",
            profile,
        )
        assert admissions == R, (
            "Expected exactly R group-holder admissions; got per-task admissions",
            profile,
        )
        assert max_holders == R, profile

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)

        recovered_object_id, value = ray.get(
            borrowers[0].read.remote(TARGET_INDEX),
            timeout=GET_TIMEOUT_S,
        )

        assert recovered_object_id == object_ids[TARGET_INDEX]
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

        print("PASS: Recovery Frontier + Succession non-leader owner failure")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  initial manifests         = {initial_manifests}")
        print(f"  holder admissions         = {admissions}")
        print(f"  max non-owner holders     = {max_holders}")
        print(f"  leader ObjectID           = {object_ids[0]}")
        print(f"  target ObjectID           = {object_ids[TARGET_INDEX]}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
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
