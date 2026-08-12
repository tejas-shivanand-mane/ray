#!/usr/bin/env python3
"""Deterministic candidate-commit durability test for recovery succession.

This test targets the crash window:

    InstallRecoveryHolder(H1) succeeds
        ->
    witness stores proposed manifest containing H1
        ->
    OWNER FAILS before H1 becomes replay-capable

The test uses a default-off C++ fault-injection flag:
    recovery_succession_test_fail_after_witness_ack

The flag causes the owner to return an injected RPC error immediately after
successful witness publication, before owner-side CommitHolderAdmission and
before CommitRecoveryManifest is sent to H1. The benchmark then hard-kills the
owner node.

Cases
-----
1. fully_committed_control
   Fault injection disabled.
   H1 is fully committed before owner failure.
   Expected: recovery succeeds.

2. after_witness_before_candidate_commit
   Fault injection enabled.
   The witness has advertised H1, but H1 is still provisional.
   Safety requirement: recovery must still succeed if a witness advertises H1.
   Current code is expected to FAIL this case because PrepareTaskReplay rejects
   a provisional holder (manifest_committed == false).

This is deliberately deterministic: there is no timing race or artificial sleep
used to try to hit the window.
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
    session_dirs,
    succession,
    system_config,
    wait_for_cluster,
    wait_for_log,
    write_csv,
)

METHOD = succession(4)
WITNESS_COUNT = 1
FAULT_CONFIG_KEY = "recovery_succession_test_fail_after_witness_ack"
FAULT_LOG = (
    "TEST ONLY: injected recovery succession failure after witness ACK "
    "before candidate commit"
)


def normalize_node_id(value: Any) -> str:
    return str(value).strip().lower()


def wait_for_node_dead(node_id: str, timeout_s: float) -> None:
    target = normalize_node_id(node_id)
    deadline = time.monotonic() + timeout_s
    last_nodes: list[dict[str, Any]] = []

    while time.monotonic() < deadline:
        last_nodes = ray.nodes()
        for info in last_nodes:
            current = normalize_node_id(info.get("NodeID", ""))
            if current == target and not bool(info.get("Alive", False)):
                return

        # Some Ray versions remove the entry instead of retaining Alive=False.
        present = any(
            normalize_node_id(info.get("NodeID", "")) == target
            for info in last_nodes
        )
        if not present:
            return

        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for owner node {node_id} to become DEAD. "
        f"Last ray.nodes()={last_nodes}"
    )


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

    raise TimeoutError(
        f"Timed out waiting for marker {token!r} in {path}"
    )


def count_starts(path: Path, token: str) -> int:
    if not path.exists():
        return 0
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return 0
    prefix = f"START,{token},"
    return sum(1 for line in lines if line.startswith(prefix))


def start_cluster(
    args: argparse.Namespace,
    inject_fault: bool,
):
    cluster = Cluster()

    config = system_config(
        METHOD,
        witness_count=WITNESS_COUNT,
        profiling_enabled=True,
        object_timeout_ms=args.object_timeout_ms,
    )
    config[FAULT_CONFIG_KEY] = bool(inject_fault)

    # Head survives the experiment and is the only possible witness when the
    # original task is submitted.
    head_node = cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    # The task owner is deliberately NOT the head so we can hard-kill it while
    # keeping the GCS and witness alive.
    owner_node = cluster.add_node(
        num_cpus=2,
        resources={"owner_node": 1},
    )

    return cluster, head_node, owner_node


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
            # soft=True is intentional. The first execution is strongly
            # preferred on the owner node, but StartRecoveryReplay() clears
            # soft node affinity so the replay can run after that node dies.
            return [
                work.options(
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=self.node_id,
                        soft=True,
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
            # Keep the ObjectRef itself. Do not ray.get() it here.
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


def try_recover(ref, timeout_s: float) -> tuple[bool, int, str]:
    try:
        value = ray.get(ref, timeout=timeout_s)
        return True, len(value), ""
    except Exception as exc:  # benchmark intentionally records exact Ray failure
        return (
            False,
            0,
            f"{type(exc).__name__}: {exc}",
        )


def run_one(
    args: argparse.Namespace,
    case_name: str,
    inject_fault: bool,
    trial: int,
) -> dict[str, Any]:
    cluster = None
    marker = (
        Path(tempfile.gettempdir())
        / f"ray_candidate_commit_gap_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster, _head_node, owner_node = start_cluster(
            args,
            inject_fault,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(
            ray,
            2,
            args.cluster_timeout_seconds,
        )

        sessions = session_dirs(cluster)
        Owner, Holder = actor_types()

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(owner_node.node_id)

        # Ensure the owner actor is started on the intended node.
        ray.get(owner.profile.remote())

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

        # At task submission there are exactly two nodes:
        #   head/witness + owner.
        # Therefore witness_count=1 deterministically selects the head.
        wait_for_marker(
            marker,
            f"START,{token}",
            args.start_timeout_seconds,
        )

        # Add H1 only after the original manifest already exists, so H1 cannot
        # have been selected as an initial witness.
        holder_node = cluster.add_node(
            num_cpus=2,
            resources={"holder_node": 1},
        )

        wait_for_cluster(
            ray,
            3,
            args.cluster_timeout_seconds,
        )

        holder = Holder.options(
            resources={"holder_node": 0.01},
            num_cpus=0,
        ).remote()

        ray.get(holder.ping.remote())

        # Keep only H1-admission counters.
        ray.get(owner.reset_profile.remote())

        # Passing the nested ObjectRef to H1 triggers the candidate report.
        ray.get(holder.hold.remote([ref]))

        if inject_fault:
            # The C++ hook fires only after a real witness ACK has already
            # stored the proposed manifest containing H1.
            logs = wait_for_log(
                sessions,
                FAULT_LOG,
                args.admission_timeout_seconds,
            )
            if not logs:
                raise TimeoutError(
                    "Fault-injection log was not observed; the intended "
                    "post-witness/pre-candidate-commit state was not reached."
                )

            profile = wait_for_profile(
                owner,
                lambda p: (
                    int(p.get("candidate_reports_received", 0)) >= 1
                    and int(p.get("holder_install_rpcs_completed", 0)) >= 1
                    and int(p.get("witness_update_rpcs_completed", 0)) >= 1
                ),
                args.admission_timeout_seconds,
                "post-witness fault window",
            )
        else:
            profile = wait_for_profile(
                owner,
                lambda p: (
                    int(p.get("holder_admissions_committed", 0)) >= 1
                    and int(p.get("holder_commit_rpcs_completed", 0)) >= 1
                ),
                args.admission_timeout_seconds,
                "fully committed H1",
            )

            try:
                profile = wait_for_profile(
                    owner,
                    profile_quiescent,
                    args.quiescence_timeout_seconds,
                    "control RPC quiescence",
                )
            except TimeoutError:
                profile = ray.get(owner.profile.remote())

        profile_before_owner_kill = dict(profile)

        # Hard-kill the task owner and original executor while keeping the head
        # witness and H1 alive.
        cluster.remove_node(
            owner_node,
            allow_graceful=False,
        )

        wait_for_node_dead(
            owner_node.node_id,
            args.owner_dead_timeout_seconds,
        )

        recovered, recovered_bytes, recovery_error = try_recover(
            ref,
            args.recovery_timeout_seconds,
        )

        # If recovery replay happened, the same deterministic task writes a
        # second START line with the same token.
        starts = count_starts(marker, token)

        committed = int(
            profile_before_owner_kill.get(
                "holder_admissions_committed",
                0,
            )
        )
        install_sent = int(
            profile_before_owner_kill.get(
                "holder_install_rpcs_sent",
                0,
            )
        )
        install_completed = int(
            profile_before_owner_kill.get(
                "holder_install_rpcs_completed",
                0,
            )
        )
        witness_sent = int(
            profile_before_owner_kill.get(
                "witness_update_rpcs_sent",
                0,
            )
        )
        witness_completed = int(
            profile_before_owner_kill.get(
                "witness_update_rpcs_completed",
                0,
            )
        )
        commit_sent = int(
            profile_before_owner_kill.get(
                "holder_commit_rpcs_sent",
                0,
            )
        )
        commit_completed = int(
            profile_before_owner_kill.get(
                "holder_commit_rpcs_completed",
                0,
            )
        )

        # Safety property:
        # once a witness advertises H1, an owner failure must not leave H1
        # unusable. Both control and injected-window cases should recover.
        passed = (
            recovered
            and recovered_bytes == args.payload_bytes
            and starts >= 2
        )

        row = {
            "trial": trial,
            "case": case_name,
            "inject_fault": int(inject_fault),
            "candidate_reports_received": int(
                profile_before_owner_kill.get(
                    "candidate_reports_received",
                    0,
                )
            ),
            "candidate_reports_accepted": int(
                profile_before_owner_kill.get(
                    "candidate_reports_accepted",
                    0,
                )
            ),
            "holder_install_rpcs_sent": install_sent,
            "holder_install_rpcs_completed": install_completed,
            "witness_update_rpcs_sent": witness_sent,
            "witness_update_rpcs_completed": witness_completed,
            "holder_commit_rpcs_sent": commit_sent,
            "holder_commit_rpcs_completed": commit_completed,
            "holder_admissions_committed": committed,
            "execution_start_count": starts,
            "recovered": int(recovered),
            "recovered_bytes": recovered_bytes,
            "recovery_error": recovery_error,
            "pass": int(passed),
        }

        print(
            f"  case={case_name} "
            f"fault={int(inject_fault)} "
            f"install={install_sent}/{install_completed} "
            f"witness={witness_sent}/{witness_completed} "
            f"owner_commit={committed} "
            f"candidate_commit={commit_sent}/{commit_completed} "
            f"starts={starts} "
            f"recovered={int(recovered)} "
            f"PASS={int(passed)}"
        )

        if recovery_error:
            print(f"    recovery_error={recovery_error}")

        return add_method_columns(row, METHOD)

    finally:
        safe_shutdown(ray, cluster)
        try:
            marker.unlink()
        except OSError:
            pass


def run(args: argparse.Namespace) -> None:
    rows: list[dict[str, Any]] = []

    cases = [
        ("fully_committed_control", False),
        ("after_witness_before_candidate_commit", True),
    ]

    total = args.trials * len(cases)
    index = 0

    for trial in range(1, args.trials + 1):
        for case_name, inject_fault in cases:
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
                    inject_fault,
                    trial,
                )
            )

    output = (
        Path(args.output_dir)
        / "candidate_commit_durability.csv"
    )
    write_csv(output, rows)

    passed = sum(int(row["pass"]) for row in rows)

    print(f"\nSafety-property passes: {passed}/{len(rows)}")
    print(f"Wrote {output}")
    print(
        "\nExpected on the CURRENT implementation: "
        "control should PASS and the injected-window case should FAIL. "
        "After the protocol fix, both must PASS."
    )


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
            "14_candidate_commit_durability"
        ),
    )

    p.add_argument("--trials", type=int, default=1)
    p.add_argument(
        "--task-duration-seconds",
        type=float,
        default=8.0,
    )
    p.add_argument(
        "--payload-bytes",
        type=int,
        default=1024,
    )
    p.add_argument(
        "--object-timeout-ms",
        type=int,
        default=200,
    )
    p.add_argument(
        "--cluster-timeout-seconds",
        type=float,
        default=30.0,
    )
    p.add_argument(
        "--start-timeout-seconds",
        type=float,
        default=15.0,
    )
    p.add_argument(
        "--admission-timeout-seconds",
        type=float,
        default=15.0,
    )
    p.add_argument(
        "--quiescence-timeout-seconds",
        type=float,
        default=5.0,
    )
    p.add_argument(
        "--owner-dead-timeout-seconds",
        type=float,
        default=30.0,
    )
    p.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=30.0,
    )

    return p


def main() -> None:
    args = parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
