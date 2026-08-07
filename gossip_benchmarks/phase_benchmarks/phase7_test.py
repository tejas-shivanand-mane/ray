import os
import signal
import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


PAYLOAD_SIZE = 2 * 1024 * 1024


def find_log_lines(
    session_dirs: set[Path],
    text: str,
) -> list[str]:
    matches = []

    for session_dir in session_dirs:
        log_dir = session_dir / "logs"

        if not log_dir.exists():
            continue

        for path in log_dir.glob("*"):
            if not path.is_file():
                continue

            try:
                content = path.read_text(errors="replace")
            except OSError:
                continue

            for line in content.splitlines():
                if text in line:
                    matches.append(f"{path.name}: {line}")

    return matches


def wait_for_log_lines(
    session_dirs: set[Path],
    text: str,
    timeout: float = 20,
) -> list[str]:
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        matches = find_log_lines(
            session_dirs,
            text,
        )

        if matches:
            return matches

        time.sleep(0.25)

    return []


def print_matching_logs(
    session_dirs: set[Path],
    texts: list[str],
) -> None:
    printed = set()

    for text in texts:
        for line in find_log_lines(session_dirs, text):
            if line in printed:
                continue

            printed.add(line)
            print(f"  {line}")


def run_failure_case(failure_mode: str) -> None:
    cluster = Cluster()

    cluster.add_node(
        num_cpus=1,
        resources={"head_node": 1},
        _system_config={
            "enable_recovery_succession": True,
            "recovery_succession_witness_count": 2,
            "object_timeout_milliseconds": 200,
        },
    )

    cluster.add_node(
        num_cpus=1,
        resources={"owner_node": 1},
    )

    producer_node = cluster.add_node(
        num_cpus=1,
        resources={"producer_node": 1},
    )

    producer_node_id = producer_node.node_id

    cluster.add_node(
        num_cpus=1,
        resources={"rank2_node": 1},
    )

    cluster.add_node(
        num_cpus=1,
        resources={"borrower_node": 1},
    )

    ray.init(address=cluster.address)

    @ray.remote(max_retries=2)
    def produce():
        # The executor of this normal task automatically becomes
        # the first recovery holder.
        return b"x" * PAYLOAD_SIZE, os.getpid()

    @ray.remote(max_restarts=0)
    class Owner:
        def __init__(self, producer_node_id):
            self.producer_node_id = producer_node_id

        def create(self):
            payload_ref, pid_ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.producer_node_id,
                    soft=True,
                ),
                num_returns=2,
            ).remote()

            return [payload_ref, pid_ref]

    @ray.remote(max_restarts=0)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            return [self.ref]

    @ray.remote(max_restarts=0)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def read(self):
            # Keep the worker-side GetObjectsInternal timeout finite.
            # This lets the test distinguish a deletion-confirmation
            # stall from a replay or object-transfer stall.
            return ray.get(self.ref, timeout=45)

    try:
        owner = Owner.options(
            resources={"owner_node": 0.01},
        ).remote(producer_node_id)

        nested = ray.get(
            owner.create.remote()
        )

        payload_ref = nested[0]
        producer_pid_ref = nested[1]

        producer_pid = ray.get(
            producer_pid_ref
        )

        assert producer_pid > 0

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        # The produce-task executor should become rank 1.
        rank1_formed = wait_for_log_lines(
            session_dirs,
            "after witness publication with 2 total members",
            timeout=20,
        )

        if not rank1_formed:
            print("Rank-1 formation logs:")

            print_matching_logs(
                session_dirs,
                [
                    "Stored provisional recovery holder",
                    "Committed recovery succession manifest",
                    "Failed to commit recovery manifest",
                ],
            )

            raise AssertionError(
                "The producer worker did not become rank 1."
            )

        rank2_holder = Holder.options(
            resources={"rank2_node": 0.01},
        ).remote()

        # This actor receives the reference after the producer has
        # already become rank 1, so it should become rank 2.
        assert ray.get(
            rank2_holder.hold.remote(
                [payload_ref]
            )
        )

        frozen = wait_for_log_lines(
            session_dirs,
            "after witness publication with 3 total members",
            timeout=20,
        )

        if not frozen:
            print("Frozen-manifest logs:")

            print_matching_logs(
                session_dirs,
                [
                    "Stored provisional recovery holder",
                    "Committed recovery succession manifest",
                    "Applied committed recovery succession manifest",
                    "Failed to commit recovery manifest",
                ],
            )

            raise AssertionError(
                "The succession list did not freeze with "
                "the owner, producer rank 1, and holder rank 2."
            )

        print(
            f"{failure_mode} case manifest formation:"
        )

        for line in frozen:
            print(f"  {line}")

        # Allow the frozen manifest to reach rank 2.
        time.sleep(2)

        # Re-serialize from the confirmed rank-2 worker.
        fresh_nested = ray.get(
            rank2_holder.export.remote()
        )

        fresh_ref = fresh_nested[0]

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
        ).remote()

        assert ray.get(
            borrower.hold.remote(
                [fresh_ref]
            )
        )

        time.sleep(5)

        if failure_mode == "worker":
            # Kill only the rank-1 producer worker. The producer
            # node and its plasma object remain alive at this point.
            os.kill(
                producer_pid,
                signal.SIGKILL,
            )

            # Allow the worker-failure notification to reach
            # the borrower before recovery begins.
            time.sleep(5)

            # Lose the owner and physical object immediately afterward.
            ray.kill(
                owner,
                no_restart=True,
            )

            cluster.remove_node(
                producer_node,
                allow_graceful=True,
            )

        elif failure_mode == "node":
            # The producer node is rank 1 and also holds the
            # physical object. Kill the owner and immediately
            # remove that node.
            ray.kill(
                owner,
                no_restart=True,
            )

            cluster.remove_node(
                producer_node,
                allow_graceful=False,
            )

            # Allow the node-failure notification to reach
            # the borrower.
            time.sleep(5)

        else:
            raise ValueError(
                f"Unknown failure mode: {failure_mode}"
            )

        try:
            result = ray.get(
                borrower.read.remote(),
                timeout=60,
            )
        except Exception:
            print(
                f"{failure_mode} recovery failure logs:"
            )

            print_matching_logs(
                session_dirs,
                [
                    "Confirmed stale local OWNER_DIED",
                    "OWNER_DIED observed",
                    "OWNER_DIED intercepted",
                    "Skipping known-dead",
                    "Preparing recovery succession replay attempt",
                    "Promoted borrowed object to owned recovery return",
                    "Recovery succession replay accepted",
                    "Recovery succession accepted by holder",
                    "Failed to delete local OWNER_DIED",
                    "Timed out removing stale local OWNER_DIED",
                    "Trying to put an object that already existed in plasma",
                    "Failed to handle task return",
                    "Resolving task dependencies failed",
                    "Task dependencies resolved",
                    "Requesting lease",
                    "Lease granted",
                    "Pushing task",
                    "finished from worker",
                    "Completing task",
                    "Objects ",
                ],
            )

            raise

        assert len(result) == PAYLOAD_SIZE

        skipped = wait_for_log_lines(
            session_dirs,
            "Skipping known-dead recovery holder rank 1",
            timeout=10,
        )

        replayed = wait_for_log_lines(
            session_dirs,
            "Recovery succession replay accepted for return",
            timeout=10,
        )

        accepted = wait_for_log_lines(
            session_dirs,
            "Recovery succession accepted by holder rank 2",
            timeout=10,
        )

        if not skipped or not replayed or not accepted:
            print("Recovery-related logs:")

            print_matching_logs(
                session_dirs,
                [
                    "Confirmed stale local OWNER_DIED",
                    "OWNER_DIED observed",
                    "OWNER_DIED intercepted",
                    "Skipping known-dead",
                    "Preparing recovery succession replay attempt",
                    "Promoted borrowed object to owned recovery return",
                    "Recovery succession replay accepted",
                    "Recovery succession accepted by holder",
                    "Failed to delete local OWNER_DIED",
                    "Timed out removing stale local OWNER_DIED",
                    "Trying to put an object that already existed in plasma",
                    "Failed to handle task return",
                    "Resolving task dependencies failed",
                    "Task dependencies resolved",
                    "Requesting lease",
                    "Lease granted",
                    "Pushing task",
                    "finished from worker",
                    "Completing task",
                    "Objects ",
                ],
            )

        # For a worker-only failure, the producer node remains alive, so the
        # dead-worker notification should let the requester skip rank 1 directly.
        #
        # For a whole-node failure, recovery may reach the same result through
        # either path:
        #   1. rank 1 is already known dead and is skipped locally, or
        #   2. the RPC to rank 1 fails and TryRecoveryHolders advances to rank 2.
        #
        # Successful rank-2 replay and acceptance prove that rank 1 was bypassed.
        if failure_mode == "worker":
            assert skipped, (
                "worker: the known-dead rank-1 worker was not skipped."
            )

        assert replayed, (
            f"{failure_mode}: rank 2 did not replay "
            "the original task."
        )
        assert accepted, (
            f"{failure_mode}: the requester did not accept "
            "recovery through rank 2."
        )

        rank2_skipped = find_log_lines(
            session_dirs,
            "Skipping known-dead recovery holder rank 2",
        )

        assert not rank2_skipped, (
            f"{failure_mode}: the live rank-2 holder was "
            "incorrectly marked failed."
        )

        print(
            f"Phase 7 {failure_mode}-failure "
            "succession passed."
        )

    finally:
        ray.shutdown()
        cluster.shutdown()


run_failure_case("worker")
run_failure_case("node")

print(
    "Phase 7 fixed-manifest failure-aware "
    "succession passed."
)