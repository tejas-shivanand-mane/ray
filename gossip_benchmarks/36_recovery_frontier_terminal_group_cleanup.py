#!/usr/bin/env python3
"""End-to-end terminal Recovery Frontier lifecycle regression.

This test complements 35_recovery_frontier_group_lifecycle.py.

It proves the other half of K>1 owner lifecycle semantics:
  1. Two tasks share one partially-filled K=4 Recovery Frontier group.
  2. Both are exported to a borrower, so the group is activated/protected.
  3. The driver drops its local refs, but the borrower keeps both globally live.
  4. The borrower releases both refs; the owner must observe both retained
     recipes becoming dead and terminalize the group.
  5. A later task must open a fresh group and therefore build a second initial
     protection manifest instead of reusing the terminal/tombstoned group.

The assertions deliberately avoid depending on INFO log visibility. Existing
profiling counts non-tombstone protection publications, so the fresh-group
manifest count is a clean signal that terminal lifecycle sealed/retired the old
open group.
"""
from __future__ import annotations

import gc
import os
import time

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster

from _benchmark_common import (
    safe_shutdown,
    system_config,
    wait_for_cluster,
    witness_baseline,
)

R = 2
K = 4
OBJECT_TIMEOUT_MS = 500
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


def make_types():
    @ray.remote(max_retries=2)
    def produce(value: int) -> int:
        return value

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Holder:
        def __init__(self):
            self.refs = []

        def hold(self, wrapped_refs):
            # Each item is [ObjectRef], so Ray transports the ObjectRef itself
            # instead of eagerly dereferencing it as a top-level task argument.
            self.refs = [item[0] for item in wrapped_refs]
            return [ref.hex() for ref in self.refs]

        def release(self):
            count = len(self.refs)
            self.refs.clear()
            gc.collect()
            return count

    return produce, Holder


def profile() -> dict:
    from ray._private.worker import global_worker

    return global_worker.core_worker.get_recovery_succession_profile()


def wait_for_profile(predicate, description: str, timeout_s: float) -> dict:
    deadline = time.monotonic() + timeout_s
    last = {}
    while time.monotonic() < deadline:
        gc.collect()
        last = profile()
        if predicate(last):
            return last
        time.sleep(0.10)
    raise AssertionError(f"Timed out waiting for {description}; last profile={last}")


def main() -> None:
    cluster = None
    try:
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=frontier_system_config(),
            include_dashboard=False,
        )
        cluster.add_node(num_cpus=2, resources={"producer_node": 1})
        for i in range(1, R + 2):
            cluster.add_node(
                num_cpus=0,
                resources={f"frontier_holder_{i}": 1},
            )
        cluster.add_node(num_cpus=1, resources={"borrower_node": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        expected_nodes = 1 + 1 + (R + 1) + 1
        wait_for_cluster(ray, expected_nodes, 30.0)

        produce, Holder = make_types()
        holder = Holder.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        # Register both tasks before either ref is exported so they share one
        # partially-filled K=4 group.
        first = produce.options(resources={"producer_node": 0.01}, num_cpus=0).remote(1)
        second = produce.options(resources={"producer_node": 0.01}, num_cpus=0).remote(2)
        first_id = first.hex()
        second_id = second.hex()
        assert first_id != second_id

        held_ids = ray.get(holder.hold.remote([[first], [second]]))
        assert held_ids == [first_id, second_id]

        activated = wait_for_profile(
            lambda p: int(p.get("initial_manifest_build_count", 0)) >= 1
            and int(p.get("witness_update_rpcs_completed", 0)) >= R,
            "first frontier group activation",
            RELEASE_WAIT_S,
        )
        assert int(activated.get("initial_manifest_build_count", 0)) == 1, activated
        assert int(activated.get("witness_update_rpcs_sent", 0)) == R, activated
        assert int(activated.get("witness_update_rpcs_completed", 0)) == R, activated

        # Drop local refs. The Holder still keeps both distributed references
        # alive, so terminal cleanup must not happen yet.
        del first
        del second
        gc.collect()
        time.sleep(0.5)
        while_borrowed = profile()
        assert int(while_borrowed.get("owner_retained_task_specs_current", 0)) >= 2, while_borrowed

        released_by_holder = ray.get(holder.release.remote())
        assert released_by_holder == 2

        terminal = wait_for_profile(
            lambda p: int(p.get("owner_retained_task_specs_released", 0)) >= 2
            and int(p.get("owner_retained_task_specs_current", 0)) == 0,
            "terminal group owner-return cleanup",
            RELEASE_WAIT_S,
        )

        # Give the asynchronous tombstone publication/application a brief chance
        # to finish before creating a later task. The correctness assertion below
        # is the fresh initial manifest, not this delay itself.
        time.sleep(0.5)

        third = produce.options(resources={"producer_node": 0.01}, num_cpus=0).remote(3)
        third_id = third.hex()
        assert third_id not in {first_id, second_id}

        held_third = ray.get(holder.hold.remote([[third]]))
        assert held_third == [third_id]

        fresh = wait_for_profile(
            lambda p: int(p.get("initial_manifest_build_count", 0)) >= 2
            and int(p.get("witness_update_rpcs_completed", 0)) >= 2 * R,
            "fresh post-terminal frontier group activation",
            RELEASE_WAIT_S,
        )

        # One manifest/R-way publication for the original {first,second} group,
        # then one for the later task's fresh group. If the terminal open group
        # were accidentally reused, this would remain at one initial manifest.
        assert int(fresh.get("initial_manifest_build_count", 0)) == 2, fresh
        assert int(fresh.get("witness_update_rpcs_sent", 0)) == 2 * R, fresh
        assert int(fresh.get("witness_update_rpcs_completed", 0)) == 2 * R, fresh

        print("PASS: Recovery Frontier terminal group cleanup forces fresh group")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  first ObjectID            = {first_id}")
        print(f"  second ObjectID           = {second_id}")
        print(f"  third ObjectID            = {third_id}")
        print(
            "  retained tasks released   = "
            f"{int(terminal.get('owner_retained_task_specs_released', 0))}"
        )
        print(
            "  retained tasks current    = "
            f"{int(terminal.get('owner_retained_task_specs_current', 0))}"
        )
        print(
            "  initial manifests total   = "
            f"{int(fresh.get('initial_manifest_build_count', 0))}"
        )
        print(
            "  witness updates total     = "
            f"{int(fresh.get('witness_update_rpcs_sent', 0))}"
        )

        ray.get(holder.release.remote())
        del third
        gc.collect()

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
