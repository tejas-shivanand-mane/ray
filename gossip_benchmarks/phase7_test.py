import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster


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
        matches = find_log_lines(session_dirs, text)

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

    # The logical owner and physical producer must be separate.
    cluster.add_node(
        num_cpus=1,
        resources={"owner_node": 1},
    )

    producer_node = cluster.add_node(
        num_cpus=1,
        resources={"producer_node": 1},
    )

    holder_a_node = cluster.add_node(
        num_cpus=1,
        resources={"holder_a_node": 1},
    )

    cluster.add_node(
        num_cpus=1,
        resources={"holder_b_node": 1},
    )

    cluster.add_node(
        num_cpus=1,
        resources={"borrower_node": 1},
    )

    ray.init(address=cluster.address)

    @ray.remote(max_retries=2)
    def produce():
        # Force the return value into the plasma object store.
        return b"x" * PAYLOAD_SIZE

    @ray.remote(max_restarts=0)
    class Owner:
        def create(self):
            ref = produce.options(
                resources={"producer_node": 0.01},
            ).remote()

            # Keep the ObjectRef nested so Ray does not resolve
            # and fetch its value while returning from this call.
            return [ref]

    @ray.remote(max_restarts=0)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            # Re-serialize the reference after this worker has
            # received the committed frozen manifest.
            return [self.ref]

    @ray.remote(max_restarts=0)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def read(self):
            return ray.get(self.ref)

    try:
        owner = Owner.options(
            resources={"owner_node": 0.01},
        ).remote()

        nested = ray.get(owner.create.remote())
        produced_ref = nested[0]

        # Let produce() finish without fetching produced_ref.
        time.sleep(3)

        holder_a = Holder.options(
            resources={"holder_a_node": 0.01},
        ).remote()

        holder_b = Holder.options(
            resources={"holder_b_node": 0.01},
        ).remote()

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
        ).remote()

        # Holder A should report first and occupy rank 1.
        assert ray.get(
            holder_a.hold.remote([produced_ref])
        )

        time.sleep(3)

        # Holder B should then occupy rank 2.
        assert ray.get(
            holder_b.hold.remote([produced_ref])
        )

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        # Wait until the owner has committed the frozen manifest:
        # [owner, rank 1, rank 2].
        formed = wait_for_log_lines(
            session_dirs,
            "after witness publication with 3 total members",
            timeout=20,
        )

        if not formed:
            print("Manifest-formation logs:")
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
                "the owner, rank 1, and rank 2."
            )

        print(f"{failure_mode} case manifest formation:")

        for line in formed:
            print(f"  {line}")

        # Allow the committed-manifest RPC to reach rank 2.
        time.sleep(2)

        # Ask rank 2 to serialize the reference. This should carry
        # the complete frozen manifest rather than an earlier partial one.
        fresh_nested = ray.get(
            holder_b.export.remote()
        )
        fresh_ref = fresh_nested[0]

        assert ray.get(
            borrower.hold.remote([fresh_ref])
        )

        # Allow the borrower to resolve the owner status and cache
        # the recovery metadata while the owner is still alive.
        time.sleep(5)

        # Fail rank 1 while preserving rank 2.
        if failure_mode == "worker":
            ray.kill(holder_a, no_restart=True)

        elif failure_mode == "node":
            cluster.remove_node(
                holder_a_node,
                allow_graceful=False,
            )

        else:
            raise ValueError(
                f"Unknown failure mode: {failure_mode}"
            )

        # Allow the GCS worker/node failure notification to reach
        # the borrower before recovery begins.
        time.sleep(5)

        # Kill only the logical owner worker. The physical value
        # remains available on producer_node for now.
        ray.kill(owner, no_restart=True)

        # Allow owner-death notification to propagate before
        # removing the physical object.
        time.sleep(3)

        # Lose the sole physical object copy. This should enter
        # Ray's ordinary lost-object recovery path.
        cluster.remove_node(
            producer_node,
            allow_graceful=True,
        )

        time.sleep(3)

        result = ray.get(
            borrower.read.remote(),
            timeout=60,
        )

        assert len(result) == PAYLOAD_SIZE

        skipped = wait_for_log_lines(
            session_dirs,
            "Skipping known-dead recovery holder rank 1",
            timeout=10,
        )

        accepted = wait_for_log_lines(
            session_dirs,
            "Recovery succession accepted by holder rank 2",
            timeout=10,
        )

        replayed = wait_for_log_lines(
            session_dirs,
            "Recovery succession replay accepted for return",
            timeout=10,
        )

        if not skipped or not accepted or not replayed:
            print("Recovery-related logs:")

            print_matching_logs(
                session_dirs,
                [
                    "Attempting to recover",
                    "known-dead recovery holder",
                    "Recovery succession",
                    "recovery holder",
                    "OWNER_DIED",
                    "OwnerDiedError",
                ],
            )

        assert skipped, (
            f"{failure_mode}: rank 1 was not skipped "
            "using the failure cache."
        )

        assert replayed, (
            f"{failure_mode}: rank 2 did not initiate "
            "the recovery replay."
        )

        assert accepted, (
            f"{failure_mode}: the borrower did not accept "
            "the recovery result from rank 2."
        )

        print(f"Phase 7 {failure_mode}-failure succession passed.")

    finally:
        ray.shutdown()
        cluster.shutdown()


run_failure_case("worker")
run_failure_case("node")

print(
    "Phase 7 fixed-manifest failure-aware "
    "succession passed."
)