import gc
import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster
from ray._private.internal_api import global_gc


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
                    matches.append(
                        f"{path.name}: {line}"
                    )

    return matches


cluster = Cluster()

cluster.add_node(
    num_cpus=1,
    resources={"head_node": 1},
    _system_config={
        "enable_recovery_succession": True,
        "recovery_succession_witness_count": 2,
        "local_gc_min_interval_s": 1,
        "global_gc_min_interval_s": 1,
    },
)

cluster.add_node(
    num_cpus=1,
    resources={"producer_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"holder_a_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"holder_b_node": 1},
)

ray.init(address=cluster.address)


@ray.remote(max_retries=2)
def produce():
    # Force the task return into the plasma object store.
    return b"x" * PAYLOAD_SIZE


@ray.remote(
    resources={"holder_a_node": 0.01},
    max_retries=0,
)
def holder_a(wrapped_ref):
    return len(ray.get(wrapped_ref[0]))


@ray.remote(
    resources={"holder_b_node": 0.01},
    max_retries=0,
)
def holder_b(wrapped_ref):
    return len(ray.get(wrapped_ref[0]))


try:
    produced_ref = produce.options(
        resources={"producer_node": 0.01},
    ).remote()

    assert len(ray.get(produced_ref)) == PAYLOAD_SIZE

    holder_a_result = holder_a.remote(
        [produced_ref]
    )

    holder_b_result = holder_b.remote(
        [produced_ref]
    )

    assert ray.get(holder_a_result) == PAYLOAD_SIZE
    assert ray.get(holder_b_result) == PAYLOAD_SIZE

    # Allow holder admission and witness publication.
    time.sleep(5)

    del holder_a_result
    del holder_b_result

    gc.collect()
    global_gc()

    time.sleep(3)

    del produced_ref

    gc.collect()
    global_gc()

    time.sleep(3)

    gc.collect()
    global_gc()

    # Allow reference deletion, callback execution,
    # witness publication, and holder propagation.
    time.sleep(15)

    session_dirs = {
        Path(node.get_session_dir_path())
        for node in cluster.list_all_nodes()
    }

    lineage_released = find_log_lines(
        session_dirs,
        "Task lineage released; publishing recovery tombstone",
    )

    published = find_log_lines(
        session_dirs,
        "Published recovery succession tombstone",
    )

    applied = find_log_lines(
        session_dirs,
        "Applied recovery succession tombstone",
    )

    tombstone_logs = find_log_lines(
        session_dirs,
        "tombstone",
    )

    if lineage_released:
        print("Lineage-release callback:")

        for line in lineage_released:
            print(f"  {line}")

    if not published:
        print("All tombstone-related logs:")

        for line in tombstone_logs:
            print(f"  {line}")

        raise AssertionError(
            "The lineage-release callback did not result "
            "in successful tombstone publication."
        )

    if not applied:
        print("All tombstone-related logs:")

        for line in tombstone_logs:
            print(f"  {line}")

        raise AssertionError(
            "No committed holder applied the tombstone."
        )

    print("Tombstone publication:")

    for line in published:
        print(f"  {line}")

    print("Tombstone application:")

    for line in applied:
        print(f"  {line}")

    print("Phase 6 tombstone cleanup passed.")

finally:
    ray.shutdown()
    cluster.shutdown()