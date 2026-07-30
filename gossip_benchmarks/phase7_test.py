import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
)


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
                content = path.read_text(
                    errors="replace"
                )
            except OSError:
                continue

            for line in content.splitlines():
                if text in line:
                    matches.append(
                        f"{path.name}: {line}"
                    )

    return matches


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

    owner_node = cluster.add_node(
        num_cpus=2,
        resources={"owner_node": 1},
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
        # Force the output into the plasma object store.
        return b"x" * PAYLOAD_SIZE

    @ray.remote(max_restarts=0)
    class Owner:
        def __init__(self, node_id: str):
            self.node_id = node_id

        def create(self):
            ref = produce.options(
                scheduling_strategy=(
                    NodeAffinitySchedulingStrategy(
                        node_id=self.node_id,
                        soft=True,
                    )
                ),
            ).remote()

            # Keep the ref nested so its value is not fetched.
            return [ref]

    @ray.remote(max_restarts=0)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            # Re-serialize the ref after this holder has
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
        ).remote(owner_node.node_id)

        nested = ray.get(owner.create.remote())
        produced_ref = nested[0]

        # Let the producing task finish without fetching it.
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

        assert ray.get(
            holder_a.hold.remote([produced_ref])
        )

        # Ensure holder A occupies rank 1 before B reports.
        time.sleep(3)

        assert ray.get(
            holder_b.hold.remote([produced_ref])
        )

        # Allow rank-2 installation, witness publication,
        # and committed-manifest propagation.
        time.sleep(5)

        # Obtain a ref serialized by rank 2, which should
        # carry the complete frozen manifest.
        fresh_nested = ray.get(
            holder_b.export.remote()
        )
        fresh_ref = fresh_nested[0]

        assert ray.get(
            borrower.hold.remote([fresh_ref])
        )

        time.sleep(2)

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

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

        # Allow the borrower to receive the definitive
        # worker/node failure notification.
        time.sleep(5)

        # This destroys both the original owner and the
        # only physical plasma copy.
        cluster.remove_node(
            owner_node,
            allow_graceful=False,
        )

        time.sleep(5)

        result = ray.get(
            borrower.read.remote(),
            timeout=60,
        )

        assert len(result) == PAYLOAD_SIZE

        time.sleep(2)

        skipped = find_log_lines(
            session_dirs,
            "Skipping known-dead recovery holder rank 1",
        )

        accepted = find_log_lines(
            session_dirs,
            "Recovery succession accepted by holder rank 2",
        )

        if not skipped or not accepted:
            recovery_logs = find_log_lines(
                session_dirs,
                "recovery holder",
            )

            print("Recovery-holder logs:")

            for line in recovery_logs:
                print(f"  {line}")

        assert skipped, (
            f"{failure_mode}: rank 1 was not skipped "
            "using the failure cache."
        )

        assert accepted, (
            f"{failure_mode}: rank 2 did not accept "
            "the recovery request."
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