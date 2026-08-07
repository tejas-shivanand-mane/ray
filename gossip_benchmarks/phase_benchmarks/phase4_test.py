import time
from pathlib import Path

import ray
from ray.cluster_utils import Cluster


def read_matching_logs(
    session_dir: Path,
    text: str,
) -> list[tuple[Path, str]]:
    matches = []
    log_dir = session_dir / "logs"

    if not log_dir.exists():
        return matches

    for path in log_dir.glob("*"):
        if not path.is_file():
            continue

        try:
            content = path.read_text(errors="replace")
        except OSError:
            continue

        for line in content.splitlines():
            if text in line:
                matches.append((path, line))

    return matches


cluster = Cluster()

cluster.add_node(
    num_cpus=1,
    resources={"head_node": 1},
    _system_config={
        "enable_recovery_succession": True,
        "recovery_succession_witness_count": 2,
    },
)

cluster.add_node(
    num_cpus=1,
    resources={"producer_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"consumer_a_node": 1},
)

cluster.add_node(
    num_cpus=1,
    resources={"consumer_b_node": 1},
)

ray.init(address=cluster.address)


@ray.remote(max_retries=1)
def produce():
    return 21


@ray.remote(max_retries=1)
def consume(value):
    return value * 2


try:
    produced = produce.options(
        resources={"producer_node": 0.01},
    ).remote()

    consumer_a = consume.options(
        resources={"consumer_a_node": 0.01},
    ).remote(produced)

    consumer_b = consume.options(
        resources={"consumer_b_node": 0.01},
    ).remote(produced)

    assert ray.get([consumer_a, consumer_b]) == [42, 42]

    # Allow asynchronous holder admission and witness publication to finish.
    time.sleep(5)

    session_dirs = {
        Path(node.get_session_dir_path())
        for node in cluster.list_all_nodes()
    }

    success_text = (
        "Committed recovery succession manifest "
        "after witness publication"
    )

    success_matches = []

    for session_dir in session_dirs:
        success_matches.extend(
            read_matching_logs(
                session_dir,
                success_text,
            )
        )

    if not success_matches:
        checked_dirs = "\n".join(
            f"  {session_dir / 'logs'}"
            for session_dir in sorted(session_dirs)
        )

        raise AssertionError(
            "No successful Phase 4 witness publication was found.\n"
            f"Checked log directories:\n{checked_dirs}"
        )

    print("Witness publication messages:")

    for path, line in success_matches:
        print(f"  {path.name}: {line}")

    failure_matches = []

    for session_dir in session_dirs:
        failure_matches.extend(
            read_matching_logs(
                session_dir,
                "Failed to commit recovery manifest",
            )
        )

    if failure_matches:
        print("Warning: holder propagation failures were found:")

        for path, line in failure_matches:
            print(f"  {path.name}: {line}")

    print("Phase 4 witness publication test passed.")

finally:
    ray.shutdown()
    cluster.shutdown()