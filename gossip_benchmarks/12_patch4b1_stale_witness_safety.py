#!/usr/bin/env python3
"""Patch 4B-1 correctness: stale/dead witnesses already embedded in a manifest.

This test intentionally makes witness selection deterministic:

  1. The head node is also the task owner.
  2. Exactly two other raylets exist when the owner submits the task.
  3. Therefore the task's initial witness list must be those two raylets.
  4. Only after the task has started do we add a holder node.
  5. We then kill one or both original witnesses before holder admission.

Cases:
  one_dead:
      W1 dead, W2 alive.
      Expected: witness publication gets one real ACK from W2 and commits H1.

  all_dead:
      W1 dead, W2 dead.
      Expected: no witness ACK is possible and H1 is NOT committed.

This validates the important safety property of the cache optimization:
a stale/dead address can hurt availability/latency, but cannot manufacture the
witness ACK required to commit a holder.

It is deliberately a protocol-safety test for stale selected witnesses. It does
not directly introspect the private witness-cache contents.
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


def wait_for_marker(path: Path, token: str, timeout_s: float) -> None:
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
    raise TimeoutError(f"Timed out waiting for marker {token!r} in {path}")


def start_cluster(args: argparse.Namespace):
    cluster = Cluster()

    # IMPORTANT: the head is also the owner node. This prevents the head raylet
    # from becoming an extra witness candidate.
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

    # These are the ONLY non-owner raylets when the task is submitted.
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
            f.write(f"START,{token},{time.time_ns()},{os.getpid()}\n")

        time.sleep(duration_s)

        with open(marker, "a", buffering=1) as f:
            f.write(f"FINISH,{token},{time.time_ns()},{os.getpid()}\n")

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
            # Owner actor and executor are on the same node. Since witness
            # selection excludes the owner's node, only witness_1 and witness_2
            # can appear in the initial manifest.
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

        def export(self):
            return [self.ref]

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
        f"Timed out waiting for {description}. Last profile: {last}"
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
    case_name: str,
    dead_witness_count: int,
    trial: int,
) -> dict[str, Any]:
    cluster = None
    marker = (
        Path(tempfile.gettempdir())
        / f"ray_stale_witness_{uuid.uuid4().hex}.csv"
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

        token = uuid.uuid4().hex

        wrapped = ray.get(
            owner.dispatch.remote(
                args.task_duration_seconds,
                args.payload_bytes,
                str(marker),
                token,
            )
        )
        ref = wrapped[0]

        # The task was submitted when the cluster contained exactly:
        #   owner/head, W1, W2
        # so its manifest's two witnesses are deterministic.
        wait_for_marker(
            marker,
            f"START,{token}",
            args.start_timeout_seconds,
        )

        # Add the candidate holder only AFTER the initial manifest exists.
        holder_node = cluster.add_node(
            num_cpus=1,
            resources={"holder_node": 1},
        )

        wait_for_cluster(
            ray,
            1 + WITNESS_COUNT + 1,
            args.cluster_timeout_seconds,
        )

        holder = Holder.options(
            resources={"holder_node": 0.01},
            num_cpus=0,
        ).remote()

        ray.get(holder.ping.remote())

        # Ignore task-submission profiling. From here on we care only about the
        # attempted H1 admission and witness publication.
        ray.get(owner.reset_profile.remote())

        failure_start_ns = time.time_ns()

        for i in range(dead_witness_count):
            cluster.remove_node(
                witness_nodes[i],
                allow_graceful=False,
            )

        # Do not intentionally wait for GCS propagation here. The manifest
        # already contains these witness addresses, and this also exercises the
        # realistic "node died just before admission" timing.
        holder_call_start = time.perf_counter()
        ray.get(holder.hold.remote([ref]))
        holder_call_latency_s = time.perf_counter() - holder_call_start

        if dead_witness_count < WITNESS_COUNT:
            # Normal succession needs one successful compact-witness ACK.
            profile = wait_for_profile(
                owner,
                lambda p: int(
                    p.get("holder_admissions_committed", 0)
                )
                >= 1,
                args.admission_timeout_seconds,
                "holder admission commit",
            )

            # Give the failed witness RPC callback time to settle too.
            try:
                profile = wait_for_profile(
                    owner,
                    profile_quiescent,
                    args.quiescence_timeout_seconds,
                    "RPC counter quiescence",
                )
            except TimeoutError:
                # Safety verdict is already known once the holder commits.
                profile = ray.get(owner.profile.remote())

            expected_commit = 1

        else:
            # With both selected witnesses dead, success is impossible. Wait
            # until both witness RPCs have completed, then verify no commit.
            profile = wait_for_profile(
                owner,
                lambda p: (
                    int(p.get("candidate_reports_received", 0)) >= 1
                    and int(p.get("witness_update_rpcs_sent", 0))
                    >= WITNESS_COUNT
                    and int(p.get("witness_update_rpcs_completed", 0))
                    == int(p.get("witness_update_rpcs_sent", 0))
                ),
                args.admission_timeout_seconds,
                "failed witness publication",
            )

            # Allow the publication callback / AbortHolderAdmission path to run.
            time.sleep(0.25)
            profile = ray.get(owner.profile.remote())
            expected_commit = 0

        committed = int(
            profile.get("holder_admissions_committed", 0)
        )
        max_holders = int(
            profile.get("max_non_owner_holders", 0)
        )
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

        if expected_commit:
            passed = (
                committed >= 1
                and max_holders >= 1
                and candidate_reports >= 1
                and witness_sent >= WITNESS_COUNT
            )
        else:
            passed = (
                committed == 0
                and max_holders == 0
                and candidate_reports >= 1
                and witness_sent >= WITNESS_COUNT
                and witness_completed == witness_sent
                and commit_sent == 0
            )

        row = {
            "trial": trial,
            "case": case_name,
            "dead_witness_count": dead_witness_count,
            "configured_witness_count": WITNESS_COUNT,
            "expected_commit": expected_commit,
            "observed_holder_admissions_committed": committed,
            "observed_max_non_owner_holders": max_holders,
            "candidate_reports_received": candidate_reports,
            "candidate_reports_accepted": candidate_accepted,
            "holder_install_rpcs_sent": install_sent,
            "holder_install_rpcs_completed": install_completed,
            "witness_update_rpcs_sent": witness_sent,
            "witness_update_rpcs_completed": witness_completed,
            "holder_commit_rpcs_sent": commit_sent,
            "holder_commit_rpcs_completed": commit_completed,
            "profile_quiescent": int(profile_quiescent(profile)),
            "holder_call_latency_s": holder_call_latency_s,
            "failure_start_ns": failure_start_ns,
            "pass": int(passed),
        }

        print(
            f"  case={case_name} "
            f"dead={dead_witness_count} "
            f"committed={committed} "
            f"max_holders={max_holders} "
            f"candidate={candidate_reports}/{candidate_accepted} "
            f"install={install_sent}/{install_completed} "
            f"witness={witness_sent}/{witness_completed} "
            f"commit_rpc={commit_sent}/{commit_completed} "
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

    cases = [
        ("one_dead_one_survives", 1),
        ("all_selected_dead", 2),
    ]

    total = args.trials * len(cases)
    index = 0

    for trial in range(1, args.trials + 1):
        for case_name, dead_count in cases:
            index += 1
            print(
                f"[{index}/{total}] "
                f"trial={trial} "
                f"case={case_name}"
            )
            rows.append(
                run_one(
                    args,
                    case_name,
                    dead_count,
                    trial,
                )
            )

    output = Path(args.output_dir) / "stale_witness_safety.csv"
    write_csv(output, rows)

    passed = sum(int(row["pass"]) for row in rows)
    print(f"\nPassed {passed}/{len(rows)} cases")
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
            "12_patch4b1_stale_witness_safety"
        ),
    )

    p.add_argument("--trials", type=int, default=1)

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
