#!/usr/bin/env python3
"""Smoke test for lazy WitnessBaseline-R4 activation.

Expected:
  B=0: no recovery manifest construction, no witness update RPCs, no TaskSpec bytes.
  B=1: first downstream export activates the baseline and sends exactly R full
       TaskSpec-bearing witness updates per protected producer task.
"""
from __future__ import annotations

import os
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import time

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import safe_shutdown, system_config, wait_for_cluster, witness_baseline

R = 4


def profile():
    return global_worker.core_worker.get_recovery_succession_profile()


def wait_quiescent(timeout_s=20.0, stable_s=0.4):
    deadline = time.monotonic() + timeout_s
    last = None
    stable_since = None
    while time.monotonic() < deadline:
        p = profile()
        outstanding = (
            max(0, int(p.get("holder_install_rpcs_sent", 0)) -
                   int(p.get("holder_install_rpcs_completed", 0)))
            + max(0, int(p.get("holder_commit_rpcs_sent", 0)) -
                     int(p.get("holder_commit_rpcs_completed", 0)))
            + max(0, int(p.get("witness_update_rpcs_sent", 0)) -
                     int(p.get("witness_update_rpcs_completed", 0)))
        )
        sig = tuple(sorted(p.items()))
        now = time.monotonic()
        if outstanding == 0:
            if sig == last:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= stable_s:
                    return p
            else:
                stable_since = now
        else:
            stable_since = None
        last = sig
        time.sleep(0.05)
    raise TimeoutError("recovery profile did not quiesce")


def main():
    cluster = None
    try:
        method = witness_baseline(R)
        cluster = Cluster()
        cluster.add_node(
            num_cpus=0,
            _system_config=system_config(
                method,
                witness_count=2,
                profiling_enabled=True,
            ),
            include_dashboard=False,
        )
        producer_node = cluster.add_node(
            num_cpus=2, resources={"producer_node": 1}
        )
        cluster.add_node(num_cpus=1, resources={"borrower_node": 1})
        cluster.add_node(num_cpus=0, resources={"extra_witness_1": 1})
        cluster.add_node(num_cpus=0, resources={"extra_witness_2": 1})

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 5, 30)

        @ray.remote(max_retries=2)
        def produce(i):
            return i

        @ray.remote(max_restarts=0)
        class Borrower:
            def read(self, wrapped):
                return ray.get(wrapped[0])

        borrower = Borrower.options(
            resources={"borrower_node": 0.01}, num_cpus=0
        ).remote()

        strategy = NodeAffinitySchedulingStrategy(
            node_id=producer_node.node_id, soft=False
        )

        # B=0: owner uses its own refs, but never exports them to another worker.
        global_worker.core_worker.reset_recovery_succession_profile()
        refs = [produce.options(scheduling_strategy=strategy).remote(i) for i in range(4)]
        assert ray.get(refs) == list(range(4))
        p0 = wait_quiescent()

        assert int(p0.get("initial_manifest_build_count", 0)) == 0, p0
        assert int(p0.get("witness_update_rpcs_sent", 0)) == 0, p0
        assert int(p0.get("task_spec_bytes_sent", 0)) == 0, p0

        print("B=0 PASS")
        print("  initial_manifest_build_count = 0")
        print("  witness_update_rpcs_sent     = 0")
        print("  task_spec_bytes_sent         = 0")

        # B=1: submit the downstream borrower immediately; do not ray.get producer
        # refs first, because the export itself is the activation event.
        global_worker.core_worker.reset_recovery_succession_profile()
        refs = [produce.options(scheduling_strategy=strategy).remote(i) for i in range(4)]
        values = ray.get([borrower.read.remote([ref]) for ref in refs])
        assert values == list(range(4))
        p1 = wait_quiescent()

        expected_updates = len(refs) * R
        assert int(p1.get("initial_manifest_build_count", 0)) == len(refs), p1
        assert int(p1.get("witness_update_rpcs_sent", 0)) == expected_updates, p1
        assert int(p1.get("witness_update_rpcs_completed", 0)) == expected_updates, p1
        assert int(p1.get("task_spec_bytes_sent", 0)) > 0, p1

        print("B=1 PASS")
        print(f"  initial_manifest_build_count = {len(refs)}")
        print(f"  witness_update_rpcs_sent     = {expected_updates}")
        print(f"  task_spec_bytes_sent         = {p1.get('task_spec_bytes_sent', 0)}")
        print()
        print("Lazy WitnessBaseline-R4 behavior is correct.")

    finally:
        safe_shutdown(ray, cluster)


if __name__ == "__main__":
    main()
