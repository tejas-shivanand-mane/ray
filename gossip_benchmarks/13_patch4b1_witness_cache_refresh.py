#!/usr/bin/env python3
"""Patch 4B-1 correctness: DEAD notification removes a node from the witness cache.

Topology at startup:
    owner/head + W1 + W2

Protocol:
    1. Wait until all three nodes are visible.
    2. Hard-kill W1.
    3. Wait until GCS reports W1 dead.
    4. Allow a short subscription-settle interval.
    5. Submit a NEW recovery-enabled task from the owner.
       At this point the owner's witness cache should contain only W2.
    6. Add a holder node only after the task has started, so it cannot have
       been selected as an initial witness.
    7. Pass the task ObjectRef to that holder to trigger H1 admission.
    8. Inspect owner profiling counters.

Expected:
    witness_update_rpcs_sent == 1
    witness_update_rpcs_completed == 1
    holder_admissions_committed >= 1
    max_non_owner_holders >= 1

Why this proves cache refresh:
    At task submission time, the only possible non-owner witness nodes are W1
    and W2. W1 has already been declared DEAD. If W1 were still stale in the
    owner's cache, the new task's manifest would contain W1 and W2 and holder
    admission would send two witness RPCs. Exactly one witness RPC therefore
    shows that the DEAD notification removed W1 before the new task was built.
"""
from __future__ import annotations

import argparse
import os
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable

os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    add_method_columns,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

METHOD = succession(4)
WITNESS_COUNT = 2


def normalize_node_id(value: Any) -> str:
    return str(value).strip().lower()


def wait_for_node_dead(
    node_id: str,
    timeout_s: float,
) -> dict[str, Any] | None:
    """Wait until the GCS node view says node_id is dead.

    If the dead node disappears entirely from ray.nodes(), absence is also
    accepted once the alive-node count has dropped.
    """
    target = normalize_node_id(node_id)
    deadline = time.monotonic() + timeout_s
    last_nodes: list[dict[str, Any]] = []

    while time.monotonic() < deadline:
        last_nodes = ray.nodes()

        matched = None
        alive_count = 0

        for info in last_nodes:
            if bool(info.get("Alive", False)):
                alive_count += 1

            current = normalize_node_id(info.get("NodeID", ""))
            if current == target:
                matched = info

        if matched is not None:
            if not bool(matched.get("Alive", False)):
                return matched
        else:
            # Startup topology has exactly three nodes. Once one has been
            # removed, absence plus <=2 alive nodes is sufficient evidence
            # that the GCS view has incorporated the removal.
            if alive_count <= 2:
                return None

        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for node {node_id} to become DEAD. "
        f"Last ray.nodes()={last_nodes}"
    )


def wait_for_marker(
    path: Path,
    token: str,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        if path.exists():
            try:
                text = path.read_text(errors="replace")
            except OSError:
                text = ""

            if token in text:
                return

        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for marker {token!r} in {path}"
    )


def start_cluster(args: argparse.Namespace):
    cluster = Cluster()

    # Head == owner node so there is no extra head-raylet witness candidate.
    owner_node = cluster.add_node(
        num_cpus=2,
        resources={"owner_node": 1},
        _system_config=system_config(
            METHOD,
            witness_count=WITNESS_COUNT,
            profiling_enabled=True,
            object_timeout_ms=args.object_timeout_ms,
        ),
        include_dashboard=False,
    )

    witness_nodes = [
        cluster.add_node(
            num_cpus=0,
            resources={f"witness_{i}": 1},
        )
        for i in range(1, WITNESS_COUNT + 1)
    ]

    return cluster, owner_node, witness_nodes


def actor_types():
    @ray.remote(max_retries=2)
    def work(
        duration_s: float,
        payload_bytes: int,
        marker: str,
        token: str,
    ) -> bytes:
        with open(marker, "a", buffering=1) as f:
            f.write(
                f"START,{token},{time.time_ns()},{os.getpid()}\n"
            )

        time.sleep(duration_s)

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"FINISH,{token},{time.time_ns()},{os.getpid()}\n"
            )

        return b"x" * payload_bytes

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, node_id: str):
            self.node_id = node_id

        def dispatch(
            self,
            duration_s: float,
            payload_bytes: int,
            marker: str,
            token: str,
        ):
            return [
                work.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=self.node_id,
                        soft=False,
                    ),
                    num_cpus=1,
                ).remote(
                    duration_s,
                    payload_bytes,
                    marker,
                    token,
                )
            ]

        def reset_profile(self):
            from ray._private.worker import global_worker

            global_worker.core_worker.reset_recovery_succession_profile()
            return True

        def profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped):
            self.ref = wrapped[0]
            return True

        def ping(self):
            return True

    return Owner, Holder


def wait_for_profile(
    owner,
    predicate: Callable[[dict[str, Any]], bool],
    timeout_s: float,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, Any] = {}

    while time.monotonic() < deadline:
        last = ray.get(owner.profile.remote())

        if predicate(last):
            return last

        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for {description}. Last profile={last}"
    )


def profile_quiescent(profile: dict[str, Any]) -> bool:
    return (
        int(profile.get("holder_install_rpcs_sent", 0))
        == int(profile.get("holder_install_rpcs_completed", 0))
        and int(profile.get("witness_update_rpcs_sent", 0))
        == int(profile.get("witness_update_rpcs_completed", 0))
        and int(profile.get("holder_commit_rpcs_sent", 0))
        == int(profile.get("holder_commit_rpcs_completed", 0))
    )


def run_one(
    args: argparse.Namespace,
    trial: int,
) -> dict[str, Any]:
    cluster = None
    marker = (
        Path(tempfile.gettempdir())
        / f"ray_witness_cache_refresh_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster, owner_node, witness_nodes = start_cluster(args)

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(
            ray,
            1 + WITNESS_COUNT,
            args.cluster_timeout_seconds,
        )

        Owner, Holder = actor_types()

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(owner_node.node_id)

        # Make sure the owner actor is alive before injecting the node failure.
        ray.get(owner.profile.remote())

        dead_node = witness_nodes[0]
        surviving_node = witness_nodes[1]

        removal_start = time.perf_counter()

        cluster.remove_node(
            dead_node,
            allow_graceful=False,
        )

        wait_for_node_dead(
            dead_node.node_id,
            args.dead_detection_timeout_seconds,
        )

        gcs_dead_latency_s = time.perf_counter() - removal_start

        # ray.nodes() observing DEAD and the owner CoreWorker processing its
        # subscription callback are asynchronous operations. Give the owner's
        # subscription callback a small bounded interval to settle before
        # submitting the task whose manifest we inspect indirectly.
        if args.subscription_settle_seconds > 0:
            time.sleep(args.subscription_settle_seconds)

        token = uuid.uuid4().hex

        # IMPORTANT: this task is submitted AFTER W1 is known dead.
        wrapped = ray.get(
            owner.dispatch.remote(
                args.task_duration_seconds,
                args.payload_bytes,
                str(marker),
                token,
            )
        )
        ref = wrapped[0]

        wait_for_marker(
            marker,
            f"START,{token}",
            args.start_timeout_seconds,
        )

        # Add H1 only after the task's initial manifest has already been built.
        # Therefore the holder node cannot appear in that manifest's witness list.
        holder_node = cluster.add_node(
            num_cpus=1,
            resources={"holder_node": 1},
        )

        wait_for_cluster(
            ray,
            3,  # owner + surviving W2 + holder
            args.cluster_timeout_seconds,
        )

        holder = Holder.options(
            resources={"holder_node": 0.01},
            num_cpus=0,
        ).remote()

        ray.get(holder.ping.remote())

        # Reset AFTER task submission so admission counters describe only the
        # H1 admission below. The manifest itself is already fixed at this point.
        ray.get(owner.reset_profile.remote())

        admission_start = time.perf_counter()

        ray.get(holder.hold.remote([ref]))

        profile = wait_for_profile(
            owner,
            lambda p: int(
                p.get("holder_admissions_committed", 0)
            )
            >= 1,
            args.admission_timeout_seconds,
            "H1 holder admission commit",
        )

        try:
            profile = wait_for_profile(
                owner,
                profile_quiescent,
                args.quiescence_timeout_seconds,
                "RPC counter quiescence",
            )
        except TimeoutError:
            profile = ray.get(owner.profile.remote())

        admission_latency_s = time.perf_counter() - admission_start

        candidate_reports = int(
            profile.get("candidate_reports_received", 0)
        )
        candidate_accepted = int(
            profile.get("candidate_reports_accepted", 0)
        )
        install_sent = int(
            profile.get("holder_install_rpcs_sent", 0)
        )
        install_completed = int(
            profile.get("holder_install_rpcs_completed", 0)
        )
        witness_sent = int(
            profile.get("witness_update_rpcs_sent", 0)
        )
        witness_completed = int(
            profile.get("witness_update_rpcs_completed", 0)
        )
        commit_sent = int(
            profile.get("holder_commit_rpcs_sent", 0)
        )
        commit_completed = int(
            profile.get("holder_commit_rpcs_completed", 0)
        )
        committed = int(
            profile.get("holder_admissions_committed", 0)
        )
        max_holders = int(
            profile.get("max_non_owner_holders", 0)
        )

        # The decisive cache-refresh assertion is witness_sent == 1.
        #
        # If dead W1 were still present when the post-DEAD task was submitted,
        # its initial manifest would still carry both W1 and W2, and admission
        # would send two UpdateRecoveryWitness RPCs.
        passed = (
            candidate_reports >= 1
            and candidate_accepted >= 1
            and install_sent == 1
            and install_completed == 1
            and witness_sent == 1
            and witness_completed == 1
            and committed >= 1
            and max_holders >= 1
            and commit_sent == 1
        )

        row = {
            "trial": trial,
            "dead_witness_node_id": dead_node.node_id,
            "surviving_witness_node_id": surviving_node.node_id,
            "gcs_dead_latency_s": gcs_dead_latency_s,
            "subscription_settle_seconds": (
                args.subscription_settle_seconds
            ),
            "candidate_reports_received": candidate_reports,
            "candidate_reports_accepted": candidate_accepted,
            "holder_install_rpcs_sent": install_sent,
            "holder_install_rpcs_completed": install_completed,
            "witness_update_rpcs_sent": witness_sent,
            "witness_update_rpcs_completed": witness_completed,
            "holder_commit_rpcs_sent": commit_sent,
            "holder_commit_rpcs_completed": commit_completed,
            "holder_admissions_committed": committed,
            "max_non_owner_holders": max_holders,
            "profile_quiescent": int(profile_quiescent(profile)),
            "admission_latency_s": admission_latency_s,
            "expected_post_dead_witness_rpc_count": 1,
            "cache_refresh_inferred": int(witness_sent == 1),
            "pass": int(passed),
        }

        print(
            f"  GCS_DEAD={gcs_dead_latency_s:.3f}s "
            f"candidate={candidate_reports}/{candidate_accepted} "
            f"install={install_sent}/{install_completed} "
            f"witness={witness_sent}/{witness_completed} "
            f"commit_rpc={commit_sent}/{commit_completed} "
            f"committed={committed} "
            f"max_holders={max_holders} "
            f"CACHE_REFRESH={int(witness_sent == 1)} "
            f"PASS={int(passed)}"
        )

        return add_method_columns(row, METHOD)

    finally:
        safe_shutdown(ray, cluster)

        try:
            marker.unlink()
        except OSError:
            pass


def run(args: argparse.Namespace) -> None:
    rows = []

    for trial in range(1, args.trials + 1):
        print(f"[{trial}/{args.trials}] cache-refresh trial={trial}")
        rows.append(run_one(args, trial))

    output = (
        Path(args.output_dir)
        / "witness_cache_refresh.csv"
    )

    write_csv(output, rows)

    passed = sum(int(row["pass"]) for row in rows)

    print(f"\nPassed {passed}/{len(rows)} trials")
    print(f"Wrote {output}")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument(
        "command",
        choices=["run"],
        nargs="?",
        default="run",
    )

    p.add_argument(
        "--output-dir",
        default=(
            "gossip_benchmarks/results/"
            "13_patch4b1_witness_cache_refresh"
        ),
    )

    p.add_argument(
        "--trials",
        type=int,
        default=1,
    )

    p.add_argument(
        "--task-duration-seconds",
        type=float,
        default=20.0,
    )

    p.add_argument(
        "--payload-bytes",
        type=int,
        default=1024,
    )

    p.add_argument(
        "--object-timeout-ms",
        type=int,
        default=1000,
    )

    p.add_argument(
        "--cluster-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--dead-detection-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--subscription-settle-seconds",
        type=float,
        default=0.5,
    )

    p.add_argument(
        "--start-timeout-seconds",
        type=float,
        default=10.0,
    )

    p.add_argument(
        "--admission-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--quiescence-timeout-seconds",
        type=float,
        default=10.0,
    )

    return p


def main() -> None:
    args = parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
