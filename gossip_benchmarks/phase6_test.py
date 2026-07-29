import gc
import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster
from ray._private.internal_api import global_gc


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
                    errors="replace",
                )
            except OSError:
                continue

            for line in content.splitlines():
                if text in line:
                    matches.append(
                        f"{path.name}: {line}",
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
    return 42


@ray.remote(
    resources={"holder_a_node": 0.01},
)
def holder_a(wrapped_ref):
    return ray.get(wrapped_ref[0])


@ray.remote(
    resources={"holder_b_node": 0.01},
)
def holder_b(wrapped_ref):
    return ray.get(wrapped_ref[0])


try:
    produced_ref = produce.options(
        resources={"producer_node": 0.01},
    ).remote()

    assert ray.get(produced_ref) == 42

    assert ray.get(
        holder_a.remote([produced_ref]),
    ) == 42

    assert ray.get(
        holder_b.remote([produced_ref]),
    ) == 42

    # Allow holder admission and witness publication.
    time.sleep(5)

    del produced_ref

    gc.collect()
    global_gc()

    time.sleep(3)

    gc.collect()
    global_gc()

    # Allow distributed reference deletion and cleanup passes.
    time.sleep(12)

    session_dirs = {
        Path(node.get_session_dir_path())
        for node in cluster.list_all_nodes()
    }

    published = find_log_lines(
        session_dirs,
        "Published recovery succession tombstone",
    )

    applied = find_log_lines(
        session_dirs,
        "Applied recovery succession tombstone",
    )

    cleanup_scans = find_log_lines(
        session_dirs,
        "Phase 6 cleanup scan",
    )

    if cleanup_scans:
        print("Cleanup scans:")

        for line in cleanup_scans:
            print(f"  {line}")

    if not published:
        raise AssertionError(
            "No owner-side tombstone publication was found.",
        )

    if not applied:
        raise AssertionError(
            "No holder applied the tombstone.",
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