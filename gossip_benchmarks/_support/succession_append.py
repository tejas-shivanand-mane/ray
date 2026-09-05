#!/usr/bin/env python3
"""Recovery Frontier + Succession dynamic-append correctness target.

This benchmark tests the next missing adaptive composition property after
Benchmarks 52-54:

    phase 1: Frontier[T1,T2] -> shared holders H1,H2
    phase 2: append T3,T4 to the SAME Frontier and reuse H1,H2
    failure: owner dies
    recovery: request appended non-leader T4 only

Target semantics (R=2, K=4):
  * Phase 1 establishes one shared adaptive Succession topology for T1/T2.
  * Phase 2 creates T3/T4 only AFTER that topology is fully committed.
  * T3/T4 must join the same open Frontier rather than starting a second group.
  * Existing H1/H2 receive only the new replay recipes; no new candidate
    topology formation or holder admission is allowed.
  * After owner failure, T4 must recover with the original ObjectID and exactly
    one replay. T1-T3 must not replay as a side effect.

Current pre-dynamic-append code is expected to fail this benchmark because the
adaptive Frontier is sealed on first Succession activation. The decisive
pre-failure assertion is that candidate reports and holder admissions remain at
R after phase 2; a second adaptive group would increase those counters.
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
    read_marker,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_marker,
)

R = 2
K = 4
PHASE_TASKS = 2
TOTAL_TASKS = 4
TARGET_INDEX = 3
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 180.0
PROTECTION_TIMEOUT_S = 30.0
PROTECTION_STABLE_S = 0.75


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
    class Borrower:
        def __init__(self):
            self.refs = []

        def append(self, wrapped_refs):
            refs = list(wrapped_refs)
            self.refs.extend(refs)
            return [ref.hex() for ref in refs]

        def read(self, index: int):
            ref = self.refs[index]
            return ref.hex(), ray.get(ref)

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch_and_export_batch(
            self,
            executor_node_id: str,
            tokens: list[str],
            marker_path: str,
            payload_bytes: int,
            borrowers,
        ):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id,
                soft=False,
            )
            refs = [
                work.options(
                    scheduling_strategy=strategy,
                    num_cpus=0.1,
                ).remote(token, marker_path, payload_bytes)
                for token in tokens
            ]

            # Export FROM THE OWNER so the producer manager controls Frontier
            # membership and recovery metadata transport in both phases.
            held = ray.get(
                [borrower.append.remote(refs) for borrower in borrowers]
            )
            return [ref.hex() for ref in refs], held

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    return Owner, Borrower


def _profile_signature(profile: dict) -> tuple[int, ...]:
    return tuple(
        int(profile.get(key, 0))
        for key in (
            "candidate_reports_received",
            "candidate_reports_accepted",
            "holder_admissions_committed",
            "holder_install_rpcs_sent",
            "holder_install_rpcs_completed",
            "holder_commit_rpcs_sent",
            "holder_commit_rpcs_completed",
            "witness_update_rpcs_sent",
            "witness_update_rpcs_completed",
            "manifest_generations_committed",
            "max_non_owner_holders",
        )
    )


def _async_outstanding(profile: dict) -> int:
    return sum(
        max(0, int(profile.get(sent, 0)) - int(profile.get(done, 0)))
        for sent, done in (
            ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
            ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
            ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
        )
    )


def wait_for_profile_stable(owner, timeout_s: float, *, min_admissions: int) -> dict:
    deadline = time.monotonic() + timeout_s
    last: dict = {}
    last_signature: tuple[int, ...] | None = None
    stable_since: float | None = None

    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        signature = _profile_signature(last)
        now = time.monotonic()
        ready = (
            int(last.get("holder_admissions_committed", 0)) >= min_admissions
            and _async_outstanding(last) == 0
        )

        if ready:
            if signature == last_signature:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= PROTECTION_STABLE_S:
                    return last
            else:
                stable_since = now
        else:
            stable_since = None

        last_signature = signature
        time.sleep(0.05)

    raise TimeoutError(
        f"Recovery protection/profile did not quiesce. Last profile={last}"
    )


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_succession_dynamic_append_{uuid.uuid4().hex}.csv"
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
        wait_for_cluster(ray, 1 + 1 + 1 + R + R, 30.0)

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

        tokens = [f"task-{i + 1}-{uuid.uuid4().hex}" for i in range(TOTAL_TASKS)]

        # Phase 1: establish the adaptive topology using only T1/T2.
        phase1_ids, phase1_held = ray.get(
            owner.dispatch_and_export_batch.remote(
                executor_node.node_id,
                tokens[:PHASE_TASKS],
                str(marker),
                PAYLOAD_BYTES,
                borrowers,
            )
        )
        for ids in phase1_held:
            assert ids == phase1_ids, (ids, phase1_ids)

        wait_for_marker(marker, "START", timeout_s=10.0, min_count=PHASE_TASKS)
        profile1 = wait_for_profile_stable(
            owner,
            PROTECTION_TIMEOUT_S,
            min_admissions=R,
        )

        phase1_reports = int(profile1.get("candidate_reports_received", 0))
        phase1_admissions = int(profile1.get("holder_admissions_committed", 0))
        phase1_max_holders = int(profile1.get("max_non_owner_holders", 0))

        assert phase1_reports == R, profile1
        assert phase1_admissions == R, profile1
        assert phase1_max_holders == R, profile1

        # Phase 2: only now create/export T3/T4. A correct dynamic append keeps
        # the same topology; borrowers must not become candidates for a new
        # group and the owner must not admit another R holders.
        phase2_ids, phase2_held = ray.get(
            owner.dispatch_and_export_batch.remote(
                executor_node.node_id,
                tokens[PHASE_TASKS:],
                str(marker),
                PAYLOAD_BYTES,
                borrowers,
            )
        )
        for ids in phase2_held:
            assert ids == phase2_ids, (ids, phase2_ids)

        wait_for_marker(marker, "START", timeout_s=10.0, min_count=TOTAL_TASKS)
        for token in tokens:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        profile2 = wait_for_profile_stable(
            owner,
            PROTECTION_TIMEOUT_S,
            min_admissions=R,
        )

        reports2 = int(profile2.get("candidate_reports_received", 0))
        accepted2 = int(profile2.get("candidate_reports_accepted", 0))
        admissions2 = int(profile2.get("holder_admissions_committed", 0))
        max_holders2 = int(profile2.get("max_non_owner_holders", 0))

        # Decisive dynamic-topology assertions. A sealed-first-slice design will
        # open a second adaptive group here and increase reports/admissions.
        assert reports2 == phase1_reports == R, (
            "T3/T4 triggered new candidate topology formation instead of a "
            "dynamic append to the existing Frontier",
            profile1,
            profile2,
        )
        assert admissions2 == phase1_admissions == R, (
            "T3/T4 caused new holder admissions instead of reusing H1/H2",
            profile1,
            profile2,
        )
        assert max_holders2 == R, profile2

        object_ids = phase1_ids + phase2_ids
        assert len(object_ids) == TOTAL_TASKS
        assert len(set(object_ids)) == TOTAL_TASKS

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)

        recovered_object_id, value = ray.get(
            borrowers[0].read.remote(TARGET_INDEX),
            timeout=GET_TIMEOUT_S,
        )

        assert recovered_object_id == object_ids[TARGET_INDEX]
        assert value["token"] == tokens[TARGET_INDEX]
        assert len(value["payload"]) == PAYLOAD_BYTES

        post_failure_starts = [
            count_token_starts(marker, token, after_ns=failure_wall_ns)
            for token in tokens
        ]
        assert post_failure_starts[TARGET_INDEX] == 1, read_marker(marker)
        for index in range(TARGET_INDEX):
            assert post_failure_starts[index] == 0, read_marker(marker)

        print("PASS: Recovery Frontier + Succession dynamic append")
        print(f"  R                         = {R}")
        print(f"  K                         = {K}")
        print(f"  phase1 candidate reports  = {phase1_reports}")
        print(f"  phase2 candidate reports  = {reports2}")
        print(f"  phase2 reports accepted   = {accepted2}")
        print(f"  phase1 holder admissions  = {phase1_admissions}")
        print(f"  phase2 holder admissions  = {admissions2}")
        print(f"  max non-owner holders     = {max_holders2}")
        print(f"  appended target ObjectID  = {object_ids[TARGET_INDEX]}")
        print(f"  recovered ObjectID        = {recovered_object_id}")
        for index, starts in enumerate(post_failure_starts):
            print(f"  task {index + 1} post-failure START = {starts}")

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
