#!/usr/bin/env python3
"""Recovery Frontier + Succession dynamic-append failure atomicity.

This benchmark extends Benchmark 55 with a real holder failure during a later
Frontier recipe append:

    phase 1: Frontier[T1,T2] -> H1,H2, fully committed
    failure: kill H2
    phase 2: append T3,T4 to the SAME Frontier
             H1 ACKs the append, H2 is dead and cannot ACK
    safety: owner must fail closed before T3/T4 ObjectRefs escape
    recovery: previously committed T2 must still recover from H1

The benchmark deliberately establishes H1 and H2 sequentially so their ranks
are deterministic.  H1 is admitted first and H2 second.  After H2 is killed,
the adaptive append publisher iterates the frozen Succession in rank order:
H1 therefore applies the new recipe suffix before publication fails at rank 2.
This creates a genuine partial-holder state without a synthetic transport hook.

Required semantics (R=2, K=4):
  * T1/T2 form one fully committed shared Frontier topology H1,H2.
  * Killing H2 does not invalidate the already committed T1/T2 prefix.
  * During the T3/T4 append, publication reaches H1 and then fails at dead H2.
  * The owner must not commit/advertise the T3/T4 suffix after that partial ACK.
  * The attempted borrower must still hold only T1/T2; T3/T4 never become
    externally visible ObjectRefs through that export.
  * Owner failure caused by the fail-closed append path must not corrupt the
    old prefix: requesting T2 replays T2 exactly once with the same ObjectID,
    while T1 does not replay.
"""
from __future__ import annotations

import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    read_marker,
    safe_shutdown,
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_log,
    wait_for_marker,
)

R = 2
K = 4
PHASE_TASKS = 2
TOTAL_TASKS = 4
TARGET_INDEX = 1  # Recover committed non-leader T2 after the failed append.
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 500
GET_TIMEOUT_S = 90.0
INITIAL_BLOCK_TIMEOUT_S = 180.0
PROFILE_TIMEOUT_S = 30.0
PROFILE_STABLE_S = 0.75
ACTOR_DEAD_TIMEOUT_S = 15.0
APPEND_FAILURE_TIMEOUT_S = 30.0

APPEND_FAILURE_LOG = "Failed adaptive Recovery Frontier recipe append generation"
FAIL_CLOSED_LOG = "Adaptive Recovery Frontier failed to install recipe append generation"


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
            # Sequential H1/H2 admission is part of the benchmark construction;
            # keep ordinary ordered admission even if the caller environment
            # enables the certificate experiment globally.
            "enable_recovery_succession_certificate_admission": False,
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

        def held_ids(self):
            return [ref.hex() for ref in self.refs]

        def read(self, index: int):
            ref = self.refs[index]
            return ref.hex(), ray.get(ref)

        def ping(self):
            return True

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def __init__(self):
            self.phase1_refs = []

        def create_phase1(
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
            self.phase1_refs = [
                work.options(
                    scheduling_strategy=strategy,
                    num_cpus=0.1,
                ).remote(token, marker_path, payload_bytes)
                for token in tokens
            ]
            return [ref.hex() for ref in self.phase1_refs]

        def export_phase1_to(self, borrower):
            # Keeping creation and export in separate owner-actor methods lets
            # the benchmark force candidate arrival/admission order: first H1,
            # then H2.  Only hex strings ever return to the driver.
            return ray.get(borrower.append.remote(self.phase1_refs))

        def create_and_export_phase2(
            self,
            executor_node_id: str,
            tokens: list[str],
            marker_path: str,
            payload_bytes: int,
            borrower,
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

            # T3/T4 are registered with the still-open K=4 Frontier before this
            # export.  Building the borrower task arguments drives the adaptive
            # suffix publication synchronously.  With H2 dead, publication
            # reaches H1 then fails closed before borrower.append can execute.
            held = ray.get(borrower.append.remote(refs))
            return [ref.hex() for ref in refs], held

        def recovery_profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    return Owner, Borrower


def wait_for_profile(
    owner,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_s: float,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}

    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        if predicate(last):
            return last
        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for {description}. Last profile={last}"
    )


def profile_signature(profile: dict[str, Any]) -> tuple[int, ...]:
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


def async_outstanding(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile.get(sent, 0)) - int(profile.get(done, 0)))
        for sent, done in (
            ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
            ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
            ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
        )
    )


def wait_for_profile_stable(
    owner,
    timeout_s: float,
    *,
    exact_admissions: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}
    last_signature: tuple[int, ...] | None = None
    stable_since: float | None = None

    while time.monotonic() < deadline:
        last = ray.get(owner.recovery_profile.remote())
        signature = profile_signature(last)
        now = time.monotonic()
        ready = (
            int(last.get("holder_admissions_committed", 0)) == exact_admissions
            and _candidate_count(last) == exact_admissions
            and async_outstanding(last) == 0
        )

        if ready:
            if signature == last_signature:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= PROFILE_STABLE_S:
                    return last
            else:
                stable_since = now
        else:
            stable_since = None

        last_signature = signature
        time.sleep(0.05)

    raise TimeoutError(
        f"Recovery protection/profile did not quiesce at "
        f"{exact_admissions} admissions. Last profile={last}"
    )


def _candidate_count(profile: dict[str, Any]) -> int:
    return int(profile.get("candidate_reports_received", 0))


def wait_for_actor_dead(actor, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: BaseException | None = None

    while time.monotonic() < deadline:
        try:
            ray.get(actor.ping.remote(), timeout=0.5)
        except ray.exceptions.GetTimeoutError as exc:
            last_error = exc
        except ray.exceptions.RayError:
            return
        time.sleep(0.05)

    raise TimeoutError(
        f"Actor did not become unavailable within {timeout_s}s; "
        f"last_error={last_error!r}"
    )


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_frontier_succession_append_atomicity_{uuid.uuid4().hex}.csv"
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
        sessions = session_dirs(cluster)

        Owner, Borrower = types()
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote()
        h1 = Borrower.options(
            resources={"borrower_node_0": 0.01},
            num_cpus=0,
        ).remote()
        h2 = Borrower.options(
            resources={"borrower_node_1": 0.01},
            num_cpus=0,
        ).remote()

        tokens = [f"task-{i + 1}-{uuid.uuid4().hex}" for i in range(TOTAL_TASKS)]

        # Create T1/T2 once and retain them only inside the owner actor.
        phase1_ids = ray.get(
            owner.create_phase1.remote(
                executor_node.node_id,
                tokens[:PHASE_TASKS],
                str(marker),
                PAYLOAD_BYTES,
            )
        )
        assert len(phase1_ids) == PHASE_TASKS
        assert len(set(phase1_ids)) == PHASE_TASKS
        wait_for_marker(marker, "START", timeout_s=10.0, min_count=PHASE_TASKS)

        # Deterministically establish H1 first.
        h1_ids = ray.get(owner.export_phase1_to.remote(h1))
        assert h1_ids == phase1_ids, (h1_ids, phase1_ids)
        profile_h1 = wait_for_profile_stable(
            owner,
            PROFILE_TIMEOUT_S,
            exact_admissions=1,
        )
        assert int(profile_h1.get("max_non_owner_holders", 0)) == 1, profile_h1

        # Only after H1 is fully admitted allow H2 to see the refs.  Ordered
        # admission therefore assigns borrower_node_1 rank 2 deterministically.
        h2_ids = ray.get(owner.export_phase1_to.remote(h2))
        assert h2_ids == phase1_ids, (h2_ids, phase1_ids)
        profile_h2 = wait_for_profile_stable(
            owner,
            PROFILE_TIMEOUT_S,
            exact_admissions=R,
        )

        reports = int(profile_h2.get("candidate_reports_received", 0))
        accepted = int(profile_h2.get("candidate_reports_accepted", 0))
        admissions = int(profile_h2.get("holder_admissions_committed", 0))
        max_holders = int(profile_h2.get("max_non_owner_holders", 0))
        assert reports == R, profile_h2
        assert accepted == R, profile_h2
        assert admissions == R, profile_h2
        assert max_holders == R, profile_h2

        for token in tokens[:PHASE_TASKS]:
            assert count_token_starts(marker, token) == 1, read_marker(marker)

        # Kill deterministic H2 before opening the suffix publication.  The
        # node stays alive; this is specifically a holder-worker failure.
        ray.kill(h2, no_restart=True)
        wait_for_actor_dead(h2, ACTOR_DEAD_TIMEOUT_S)

        assert ray.get(h1.held_ids.remote()) == phase1_ids

        failure_wall_ns = time.time_ns()
        phase2_error: BaseException | None = None
        phase2_future = owner.create_and_export_phase2.remote(
            executor_node.node_id,
            tokens[PHASE_TASKS:],
            str(marker),
            PAYLOAD_BYTES,
            h1,
        )

        try:
            ray.get(phase2_future, timeout=APPEND_FAILURE_TIMEOUT_S)
        except ray.exceptions.GetTimeoutError as exc:
            raise AssertionError(
                "Phase-2 export hung instead of failing closed after H2 loss"
            ) from exc
        except ray.exceptions.RayError as exc:
            phase2_error = exc

        assert phase2_error is not None, (
            "T3/T4 export unexpectedly succeeded despite dead H2; the append "
            "must not become visible without all established holders ACKing"
        )

        # The warning identifies the failed rank.  Because H1 was admitted
        # first and the publisher walks ranks in order, observing failure at
        # rank 2 proves rank 1 already ACKed/applied this append generation.
        append_failure_logs = wait_for_log(
            sessions,
            APPEND_FAILURE_LOG,
            APPEND_FAILURE_TIMEOUT_S,
        )
        assert append_failure_logs, (
            "Did not observe adaptive append publication failure"
        )
        rank2_failure_logs = [
            line for line in append_failure_logs if "holder rank 2" in line
        ]
        assert rank2_failure_logs, (
            "Append did not fail at deterministic H2/rank 2; logs="
            f"{append_failure_logs}"
        )

        fail_closed_logs = wait_for_log(
            sessions,
            FAIL_CLOSED_LOG,
            APPEND_FAILURE_TIMEOUT_S,
        )
        assert fail_closed_logs, (
            "Owner did not take the fail-closed path after partial append publication"
        )

        # Critical visibility assertion: although H1 internally accepted the
        # T3/T4 replay suffix, the borrower call itself was never dispatched.
        # Python-visible state therefore still contains only committed T1/T2.
        visible_after_failure = ray.get(h1.held_ids.remote())
        assert visible_after_failure == phase1_ids, (
            "Uncommitted T3/T4 escaped the owner visibility barrier",
            visible_after_failure,
            phase1_ids,
        )

        # The old committed prefix must survive both H2 loss and the owner's
        # fail-closed termination.  Recover only T2 from surviving H1.
        recovered_object_id, value = ray.get(
            h1.read.remote(TARGET_INDEX),
            timeout=GET_TIMEOUT_S,
        )
        assert recovered_object_id == phase1_ids[TARGET_INDEX]
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

        print("PASS: Recovery Frontier + Succession dynamic append atomicity")
        print(f"  R                              = {R}")
        print(f"  K                              = {K}")
        print(f"  phase1 candidate reports       = {reports}")
        print(f"  phase1 holder admissions       = {admissions}")
        print(f"  max non-owner holders          = {max_holders}")
        print(f"  partial append failure rank    = 2")
        print(f"  append failure log observed    = {len(rank2_failure_logs)}")
        print(f"  fail-closed log observed       = {len(fail_closed_logs)}")
        print(f"  H1 visible refs after failure  = {len(visible_after_failure)}")
        print(f"  committed target ObjectID      = {phase1_ids[TARGET_INDEX]}")
        print(f"  recovered ObjectID             = {recovered_object_id}")
        print(f"  target post-failure START      = {target_post_failure_starts}")
        print(f"  leader post-failure START      = {leader_post_failure_starts}")

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
