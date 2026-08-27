#!/usr/bin/env python3
"""Regression test: Recovery Frontier K=1 degenerates to frozen Fixed-R.

This is a correctness/semantic regression, not a performance benchmark.

Two fresh clusters run the same owner-worker failure scenario:
  A. Frozen Fixed-R baseline: Recovery Frontier disabled.
  B. Fixed-R + Recovery Frontier enabled with K=1.

K=1 must not obtain grouped-protection savings. For one protected task, both
cases must build one initial manifest and perform exactly R full-lineage witness
updates. Both must preserve the ObjectID and recover via exactly one replay.
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
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 45.0
INITIAL_BLOCK_TIMEOUT_S = 120.0


def case_config(*, frontier_enabled: bool) -> dict[str, Any]:
    config = system_config(
        witness_baseline(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )
    config.update(
        {
            "enable_recovery_frontier": bool(frontier_enabled),
            "recovery_frontier_group_size": 1,
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
            # Keep the first execution unavailable. The replay sees the prior
            # START marker and returns immediately.
            release = Path(str(marker) + f".release.{token}")
            deadline = time.monotonic() + INITIAL_BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Initial execution for {token} was never released")
                time.sleep(0.05)

        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")

        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch(
            self,
            executor_node_id: str,
            token: str,
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


def run_case(*, frontier_enabled: bool) -> dict[str, Any]:
    label = "frontier_k1" if frontier_enabled else "fixed_r"
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_k1_{label}_{uuid.uuid4().hex}.csv"
    )
    token = f"{label}-{uuid.uuid4().hex}"

    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=case_config(frontier_enabled=frontier_enabled),
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
                resources={f"holder_{i}": 1},
            )
        cluster.add_node(num_cpus=1, resources={"borrower_node": 1})

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

        refs = ray.get(
            owner.dispatch.remote(
                executor_node.node_id,
                token,
                str(marker),
                PAYLOAD_BYTES,
            )
        )
        assert len(refs) == 1
        ref = refs[0]
        object_id = ref.hex()

        starts = wait_for_marker(marker, "START", timeout_s=10.0, min_count=1)
        assert starts, read_marker(marker)
        assert count_token_starts(marker, token) == 1, read_marker(marker)

        borrower_object_id = ray.get(borrower.hold.remote([ref]))
        assert borrower_object_id == object_id

        # The acknowledged export path is synchronous. By the time the borrower
        # receives the reference, fixed-R protection must already be complete.
        profile = ray.get(owner.recovery_profile.remote())
        initial_manifests = int(profile.get("initial_manifest_build_count", 0))
        witness_sent = int(profile.get("witness_update_rpcs_sent", 0))
        witness_completed = int(profile.get("witness_update_rpcs_completed", 0))
        task_spec_bytes = int(profile.get("task_spec_bytes_sent", 0))

        assert initial_manifests == 1, profile
        assert witness_sent == R, profile
        assert witness_completed == R, profile
        # K=1 is required to stay on the frozen full-TaskSpec baseline path.
        assert task_spec_bytes > 0, profile

        failure_wall_ns = time.time_ns()
        failure_perf = time.perf_counter()
        ray.kill(owner, no_restart=True)

        recovered_object_id, value = ray.get(
            borrower.read.remote(),
            timeout=GET_TIMEOUT_S,
        )
        failure_to_result_s = time.perf_counter() - failure_perf

        assert recovered_object_id == object_id
        assert value["token"] == token
        assert len(value["payload"]) == PAYLOAD_BYTES

        post_failure_starts = count_token_starts(
            marker,
            token,
            after_ns=failure_wall_ns,
        )
        assert post_failure_starts == 1, read_marker(marker)

        return {
            "label": label,
            "object_id": object_id,
            "recovered_object_id": recovered_object_id,
            "initial_manifests": initial_manifests,
            "witness_sent": witness_sent,
            "witness_completed": witness_completed,
            "task_spec_bytes": task_spec_bytes,
            "post_failure_starts": post_failure_starts,
            "failure_to_result_s": failure_to_result_s,
        }

    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass
        try:
            Path(str(marker) + f".release.{token}").unlink()
        except OSError:
            pass


def main() -> None:
    baseline = run_case(frontier_enabled=False)
    k1 = run_case(frontier_enabled=True)

    # Semantic degeneration invariant: enabling the Frontier layer at K=1 must
    # not reduce or multiply protection operations relative to Fixed-R.
    assert baseline["initial_manifests"] == k1["initial_manifests"] == 1
    assert baseline["witness_sent"] == k1["witness_sent"] == R
    assert baseline["witness_completed"] == k1["witness_completed"] == R
    assert baseline["post_failure_starts"] == k1["post_failure_starts"] == 1
    assert baseline["task_spec_bytes"] > 0
    assert k1["task_spec_bytes"] > 0

    print("PASS: Recovery Frontier K=1 degenerates to frozen Fixed-R")
    for row in (baseline, k1):
        print(f"  case                      = {row['label']}")
        print(f"    ObjectID                = {row['object_id']}")
        print(f"    recovered ObjectID      = {row['recovered_object_id']}")
        print(f"    initial manifests       = {row['initial_manifests']}")
        print(f"    witness updates sent    = {row['witness_sent']}")
        print(f"    witness updates done    = {row['witness_completed']}")
        print(f"    TaskSpec bytes sent     = {row['task_spec_bytes']}")
        print(f"    post-failure START      = {row['post_failure_starts']}")
        print(f"    failure-to-result (s)   = {row['failure_to_result_s']:.3f}")


if __name__ == "__main__":
    main()
