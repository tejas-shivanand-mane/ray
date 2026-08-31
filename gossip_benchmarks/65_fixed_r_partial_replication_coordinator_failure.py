#!/usr/bin/env python3
"""Fixed-R coordinator death during partial claim replication.

W3 is temporarily SIGSTOP'ed so W1 can replicate attempt 1 to W2 but cannot
finish replication to W3. W1 is then killed before any grant is possible.
After W3 resumes, W2 must stabilize the already-reserved attempt 1 rather than
create a competing attempt/winner. Two concurrent borrowers must still produce
exactly one replay.
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
    find_log_lines,
    read_marker,
    safe_shutdown,
    session_dirs,
    system_config,
    wait_for_cluster,
    wait_for_log,
    wait_for_marker,
    witness_baseline,
)
from _fixed_r_correctness_common import (
    assert_node_alive,
    continue_raylet,
    fixed_r_witness_order,
    node_id_hex,
    same_node,
    stop_raylet,
    wait_for_node_state,
)

R = 3
PAYLOAD_BYTES = 64 * 1024
OBJECT_TIMEOUT_MS = 300
GET_TIMEOUT_S = 120.0
INITIAL_BLOCK_TIMEOUT_S = 240.0
MAX_SELECTION_ATTEMPTS = 20

# This includes owner-death propagation, OWNER_DIED interception, claim creation,
# and the W1 -> W2 RPC. It is not a protocol timeout. The loop below continuously
# verifies that stopped W3 is still GCS-ALIVE; if GCS marks W3 dead before the
# W1 -> W2 reservation appears, the experiment fails instead of silently changing
# the protocol's authoritative-failure assumptions.
PARTIAL_WINDOW_TIMEOUT_S = 10.0

PROTECTION_LOG = "Installed full TaskSpec on all witness-holder baseline nodes"
FIRST_REPLICA_LOG = (
    "Fixed-R recovery claim replicated at witness index 1 "
    "attempt 1 coordinator index 0"
)


def fixed_r_config() -> dict:
    return system_config(
        witness_baseline(R),
        witness_count=R,
        object_timeout_ms=OBJECT_TIMEOUT_MS,
        profiling_enabled=True,
    )


def count_starts(marker: Path, token: str, *, after_ns: int = 0) -> int:
    return sum(
        1
        for event, wall_ns, _pid, row_token in read_marker(marker)
        if event == "START" and wall_ns >= after_ns and row_token == token
    )


def wait_for_live_w3_partial_replica(
    ray_module,
    logs: set[Path],
    w3,
    timeout_s: float,
) -> list[str]:
    """Wait for W2's durable attempt-1 reservation while W3 stays GCS-ALIVE."""
    deadline = time.monotonic() + timeout_s
    w3_id = node_id_hex(w3)
    last: list[str] = []
    while time.monotonic() < deadline:
        assert_node_alive(ray_module, w3_id)
        last = find_log_lines(logs, FIRST_REPLICA_LOG)
        if last:
            return last
        time.sleep(0.05)
    return last


def types():
    @ray.remote(max_retries=2)
    def work(token: str, marker_path: str, payload_bytes: int):
        marker = Path(marker_path)
        prior = count_starts(marker, token)
        with marker.open("a", buffering=1) as f:
            f.write(f"START,{time.time_ns()},{os.getpid()},{token}\n")
        if prior == 0:
            release = Path(str(marker) + f".release.{token}")
            deadline = time.monotonic() + INITIAL_BLOCK_TIMEOUT_S
            while not release.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Initial execution for {token} was not released")
                time.sleep(0.05)
        with marker.open("a", buffering=1) as f:
            f.write(f"FINISH,{time.time_ns()},{os.getpid()},{token}\n")
        return {"token": token, "payload": b"x" * payload_bytes}

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Owner:
        def dispatch(self, executor_node_id, token, marker_path, payload_bytes):
            strategy = NodeAffinitySchedulingStrategy(
                node_id=executor_node_id, soft=False
            )
            return work.options(
                scheduling_strategy=strategy, num_cpus=0.1
            ).remote(token, marker_path, payload_bytes)

    @ray.remote(max_restarts=0, max_task_retries=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return self.ref.hex()

        def read_after(self, barrier_path):
            barrier = Path(barrier_path)
            deadline = time.monotonic() + GET_TIMEOUT_S
            while not barrier.exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("Recovery barrier was never released")
                time.sleep(0.01)
            return self.ref.hex(), ray.get(self.ref)

    return Owner, Borrower


def main() -> None:
    cluster = None
    marker = Path(tempfile.gettempdir()) / (
        f"ray_fixed_r_partial_claim_{uuid.uuid4().hex}.csv"
    )
    barrier = Path(str(marker) + ".barrier")
    tokens: list[str] = []
    stopped_node = None
    try:
        cluster = Cluster()
        head_node = cluster.add_node(
            num_cpus=0, _system_config=fixed_r_config(), include_dashboard=False
        )
        owner_node = cluster.add_node(num_cpus=1, resources={"owner_node": 1})
        executor_node = cluster.add_node(
            num_cpus=3, resources={"executor_node": 1}
        )
        spare_nodes = [
            cluster.add_node(num_cpus=0, resources={f"witness_pool_{i}": 1})
            for i in range(4)
        ]
        borrower_candidates = [
            (
                cluster.add_node(
                    num_cpus=1, resources={f"borrower_pool_{i}": 1}
                ),
                f"borrower_pool_{i}",
            )
            for i in range(5)
        ]
        all_nodes = [
            head_node,
            owner_node,
            executor_node,
            *spare_nodes,
            *(node for node, _label in borrower_candidates),
        ]

        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, len(all_nodes), 40.0)
        logs = session_dirs(cluster)
        Owner, Borrower = types()
        owner = Owner.options(resources={"owner_node": 0.01}, num_cpus=0).remote()

        selected = None
        for _ in range(MAX_SELECTION_ATTEMPTS):
            token = f"partial-claim-{uuid.uuid4().hex}"
            tokens.append(token)

            # Each rejected candidate is a real protected task too. Record the
            # current count so this candidate cannot accidentally reuse an older
            # task's generic protection log as its readiness barrier.
            protection_before = len(find_log_lines(logs, PROTECTION_LOG))

            ref = ray.get(
                owner.dispatch.remote(
                    executor_node.node_id, token, str(marker), PAYLOAD_BYTES
                )
            )
            wait_for_marker(marker, "START", timeout_s=10.0, min_count=len(tokens))

            candidate_protection = wait_for_log(
                logs,
                PROTECTION_LOG,
                timeout_s=30.0,
                min_count=protection_before + 1,
            )
            assert len(candidate_protection) >= protection_before + 1, (
                "Selected candidate never completed its own all-R Fixed-R protection",
                candidate_protection,
            )

            order = fixed_r_witness_order(ref, all_nodes, R)
            w1, _w2, w3 = order
            if not same_node(w1, head_node) and not same_node(w3, head_node):
                selected = (token, ref, order)
                break
            Path(str(marker) + f".release.{token}").touch()
            ray.get(ref, timeout=15.0)

        assert selected is not None, "Could not select non-head W1/W3"
        token, ref, order = selected
        w1, w2, w3 = order
        original_object_id = ref.hex()

        non_witness_borrower_nodes = [
            (node, label)
            for node, label in borrower_candidates
            if all(not same_node(node, witness) for witness in order)
        ]
        assert len(non_witness_borrower_nodes) >= 2
        (borrower_a_node, borrower_a_label), (
            borrower_b_node,
            borrower_b_label,
        ) = non_witness_borrower_nodes[:2]

        borrower_a = Borrower.options(
            resources={borrower_a_label: 0.01}, num_cpus=0
        ).remote()
        borrower_b = Borrower.options(
            resources={borrower_b_label: 0.01}, num_cpus=0
        ).remote()
        assert ray.get(borrower_a.hold.remote([ref])) == original_object_id
        assert ray.get(borrower_b.hold.remote([ref])) == original_object_id

        # The selected task's protection was already established above. The
        # borrower exports are complete before the fault is injected as well.
        assert len(find_log_lines(logs, PROTECTION_LOG)) >= 1

        # Force a partial-replication window. W3 is stopped before recovery, so
        # W1 can obtain W2's ACK but cannot obtain W3's ACK. We do not alter the
        # health detector: if W3 ceases to be authoritatively alive before the
        # W2 reservation is observed, the benchmark fails as an invalid window.
        stop_raylet(w3)
        stopped_node = w3
        assert_node_alive(ray, node_id_hex(w3))

        failure_wall_ns = time.time_ns()
        ray.kill(owner, no_restart=True)
        read_a = borrower_a.read_after.remote(str(barrier))
        read_b = borrower_b.read_after.remote(str(barrier))
        barrier.touch()

        first_replica = wait_for_live_w3_partial_replica(
            ray,
            logs,
            w3,
            PARTIAL_WINDOW_TIMEOUT_S,
        )
        assert first_replica, find_log_lines(
            logs, "Fixed-R recovery claim replicated at witness index"
        )

        # W2 has durably stored attempt 1. Because W3 is still stopped and still
        # GCS-ALIVE, W1 cannot finish the ACK-before-grant barrier.
        assert_node_alive(ray, node_id_hex(w3))
        assert count_starts(marker, token, after_ns=failure_wall_ns) == 0, read_marker(marker)
        pre_kill_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert not pre_kill_grants, pre_kill_grants

        # Kill W1 while W3 remains stopped, so the exact partial state is:
        # W1: claim attempt 1, W2: claim attempt 1, W3: no claim.
        cluster.remove_node(w1, allow_graceful=False)

        # Resume W3 immediately so its short synthetic stall cannot itself turn
        # into an authoritative witness failure. With the normal health detector,
        # W1's real process death can now be learned promptly by GCS/raylets.
        continue_raylet(w3)
        stopped_node = None
        assert_node_alive(ray, node_id_hex(w3))
        wait_for_node_state(ray, node_id_hex(w1), alive=False, timeout_s=30.0)

        result_a, result_b = ray.get([read_a, read_b], timeout=GET_TIMEOUT_S)
        id_a, value_a = result_a
        id_b, value_b = result_b
        assert id_a == original_object_id
        assert id_b == original_object_id
        assert value_a["token"] == token
        assert value_b["token"] == token

        post_failure_starts = count_starts(marker, token, after_ns=failure_wall_ns)
        assert post_failure_starts == 1, read_marker(marker)

        attempt1_grants = wait_for_log(
            logs,
            "Fixed-R recovery claim granted after witness replication attempt 1 coordinator index 0",
            timeout_s=10.0,
        )
        assert attempt1_grants, find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        attempt2_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication attempt 2"
        )
        assert not attempt2_grants, attempt2_grants
        all_grants = find_log_lines(
            logs, "Fixed-R recovery claim granted after witness replication"
        )
        assert len(all_grants) == 1, all_grants

        print("PASS: Fixed-R coordinator death during partial claim replication")
        print(f"  R                         = {R}")
        print(f"  dead coordinator W1       = {node_id_hex(w1)}")
        print(f"  reservation holder W2     = {node_id_hex(w2)}")
        print(f"  stalled/resumed W3        = {node_id_hex(w3)}")
        print(f"  original ObjectID         = {original_object_id}")
        print(f"  borrower A ObjectID       = {id_a}")
        print(f"  borrower B ObjectID       = {id_b}")
        print(f"  post-failure START count  = {post_failure_starts}")
        print(f"  attempt-1 grant logs      = {len(attempt1_grants)}")
        print(f"  attempt-2 grant logs      = {len(attempt2_grants)}")
    finally:
        if stopped_node is not None:
            try:
                continue_raylet(stopped_node)
            except Exception:
                pass
        safe_shutdown(ray, cluster)
        for path in (marker, barrier):
            try:
                path.unlink()
            except OSError:
                pass
        for token in tokens:
            try:
                Path(str(marker) + f".release.{token}").unlink()
            except OSError:
                pass


if __name__ == "__main__":
    main()
