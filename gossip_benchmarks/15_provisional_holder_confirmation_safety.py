\
#!/usr/bin/env python3
"""Benchmark 15: provisional-holder witness-confirmation safety.

This is the negative companion to Benchmark 14.

Benchmark 14 proves:
    provisional H1
      + compact witness really stores manifest containing H1
      + owner dies before normal candidate commit
      -> H1 can independently confirm the manifest from the witness
      -> recovery succeeds

Benchmark 15 proves the converse:
    provisional H1
      + requester can still discover H1 from the real witness
      + H1's OWN witness confirmation is unavailable
      -> requester manifest alone must NOT promote H1
      -> no recovery replay is allowed

Why use a test hook instead of simply killing the witness first?
---------------------------------------------------------------
If the witness were killed before recovery starts, the requester would usually
fail to discover the newer manifest containing H1. That would not test the
security/safety boundary introduced by the fix.

Instead this benchmark keeps the real witness alive long enough for the
requester to discover H1, but uses a default-off test hook to suppress only
H1's own independent witness confirmation. This makes the intended state
deterministic.

Required test-only configs:
    recovery_succession_test_fail_after_witness_ack
    recovery_succession_test_fail_holder_witness_confirmation

Cases
-----
1. witness_confirmation_available
   - Benchmark 14 owner-side fault is enabled.
   - Owner dies after witness ACK but before owner/candidate commit.
   - H1 independently confirms with the witness.
   - Expected: recovery succeeds and task starts a second time.

2. requester_manifest_only
   - Same owner-side fault.
   - The requester may still learn H1 from the real witness.
   - H1's own confirmation is deterministically suppressed.
   - Expected: recovery fails and there is NO second task execution.

A PASS in requester_manifest_only therefore means recovery failure.
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

OWNER_FAULT_CONFIG_KEY = (
    "recovery_succession_test_fail_after_witness_ack"
)
CONFIRMATION_FAULT_CONFIG_KEY = (
    "recovery_succession_test_fail_holder_witness_confirmation"
)

OWNER_FAULT_LOG = (
    "TEST ONLY: injected recovery succession failure after witness ACK "
    "before candidate commit"
)
CONFIRMATION_FAULT_LOG = (
    "TEST ONLY: suppressing provisional holder witness confirmation"
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
    suppress_holder_confirmation: bool,
):
    cluster = Cluster()

    config = system_config(
        METHOD,
        witness_count=WITNESS_COUNT,
        profiling_enabled=True,
        object_timeout_ms=args.object_timeout_ms,
    )

    # Always force the Benchmark 14 crash window:
    # witness ACK succeeds, but owner-side and candidate-side commit do not.
    config[OWNER_FAULT_CONFIG_KEY] = True

    # Benchmark 15 toggles only H1's own independent witness confirmation.
    config[CONFIRMATION_FAULT_CONFIG_KEY] = bool(
        suppress_holder_confirmation
    )

    # The head survives and is the only possible witness at task submission.
    head_node = cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    # Owner + original executor live here and are hard-killed together.
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
            # soft=True is important. Initial execution runs on owner_node,
            # while StartRecoveryReplay() can clear soft affinity after failure.
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
            # Keep the ObjectRef without materializing it.
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


def try_recover(ref, timeout_s: float) -> tuple[bool, int, str]:
    try:
        value = ray.get(ref, timeout=timeout_s)
        return True, len(value), ""
    except Exception as exc:
        return (
            False,
            0,
            f"{type(exc).__name__}: {exc}",
        )


def run_one(
    args: argparse.Namespace,
    case_name: str,
    suppress_holder_confirmation: bool,
    expected_recovered: bool,
    trial: int,
) -> dict[str, Any]:
    cluster = None

    marker = (
        Path(tempfile.gettempdir())
        / f"ray_benchmark15_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster, _head_node, owner_node = start_cluster(
            args,
            suppress_holder_confirmation,
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

        Owner, Holder = actor_types()

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(owner_node.node_id)

        # Ensure the owner actor is alive on the intended node.
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

        # At submission there are exactly:
        #   head/witness + owner
        # so witness_count=1 deterministically assigns the head as witness.
        wait_for_marker(
            marker,
            f"START,{token}",
            args.start_timeout_seconds,
        )

        # Add H1 only after the initial manifest has been created.
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

        # Recompute session directories after H1 exists because the Benchmark
        # 15 confirmation-fault log is emitted by H1, not by the owner.
        sessions = session_dirs(cluster)

        # Keep profiling focused on H1 admission.
        ray.get(owner.reset_profile.remote())

        # Passing the nested ref to H1 triggers candidate admission.
        ray.get(holder.hold.remote([ref]))

        # The Benchmark 14 hook fires only AFTER a real witness ACK and BEFORE
        # owner-side CommitHolderAdmission / CommitRecoveryManifest(H1).
        owner_fault_logs = wait_for_log(
            sessions,
            OWNER_FAULT_LOG,
            args.admission_timeout_seconds,
        )

        if not owner_fault_logs:
            raise TimeoutError(
                "Benchmark 14 owner-side fault log was not observed."
            )

        profile = wait_for_profile(
            owner,
            lambda p: (
                int(p.get("candidate_reports_received", 0)) >= 1
                and int(p.get("holder_install_rpcs_completed", 0)) >= 1
                and int(p.get("witness_update_rpcs_completed", 0)) >= 1
            ),
            args.admission_timeout_seconds,
            "post-witness/pre-commit provisional H1 state",
        )

        profile_before_owner_kill = dict(profile)

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
        owner_commit = int(
            profile_before_owner_kill.get(
                "holder_admissions_committed",
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

        # Preconditions for both Benchmark 15 cases:
        # H1 installed + witness ACKed, but normal commit path never happened.
        preconditions_ok = (
            install_completed >= 1
            and witness_completed >= 1
            and owner_commit == 0
            and commit_sent == 0
            and commit_completed == 0
        )

        # Hard-kill the owner and original executor.
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

        confirmation_fault_seen = False

        if suppress_holder_confirmation:
            # This WARNING log proves H1 was actually contacted and reached the
            # provisional-holder confirmation branch. Therefore a failed
            # recovery is not merely "requester could not discover H1".
            try:
                confirmation_logs = wait_for_log(
                    sessions,
                    CONFIRMATION_FAULT_LOG,
                    args.confirmation_log_timeout_seconds,
                )
                confirmation_fault_seen = bool(confirmation_logs)
            except TimeoutError:
                confirmation_fault_seen = False

        starts = count_starts(marker, token)

        if expected_recovered:
            passed = (
                preconditions_ok
                and recovered
                and recovered_bytes == args.payload_bytes
                and starts >= 2
            )
        else:
            passed = (
                preconditions_ok
                and not recovered
                and starts == 1
                and confirmation_fault_seen
            )

        row = {
            "trial": trial,
            "case": case_name,
            "suppress_holder_witness_confirmation": int(
                suppress_holder_confirmation
            ),
            "expected_recovered": int(expected_recovered),
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
            "holder_admissions_committed": owner_commit,
            "holder_commit_rpcs_sent": commit_sent,
            "holder_commit_rpcs_completed": commit_completed,
            "preconditions_ok": int(preconditions_ok),
            "confirmation_fault_seen": int(confirmation_fault_seen),
            "execution_start_count": starts,
            "recovered": int(recovered),
            "recovered_bytes": recovered_bytes,
            "recovery_error": recovery_error,
            "pass": int(passed),
        }

        print(
            f"  case={case_name} "
            f"suppress_confirmation={int(suppress_holder_confirmation)} "
            f"install={install_sent}/{install_completed} "
            f"witness={witness_sent}/{witness_completed} "
            f"owner_commit={owner_commit} "
            f"candidate_commit={commit_sent}/{commit_completed} "
            f"confirm_fault_seen={int(confirmation_fault_seen)} "
            f"starts={starts} "
            f"recovered={int(recovered)} "
            f"expected_recovered={int(expected_recovered)} "
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
        (
            "witness_confirmation_available",
            False,
            True,
        ),
        (
            "requester_manifest_only",
            True,
            False,
        ),
    ]

    total = args.trials * len(cases)
    index = 0

    for trial in range(1, args.trials + 1):
        for (
            case_name,
            suppress_holder_confirmation,
            expected_recovered,
        ) in cases:
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
                    suppress_holder_confirmation,
                    expected_recovered,
                    trial,
                )
            )

    output = (
        Path(args.output_dir)
        / "provisional_holder_confirmation_safety.csv"
    )

    write_csv(output, rows)

    passed = sum(int(row["pass"]) for row in rows)

    print(
        f"\nSafety-property passes: "
        f"{passed}/{len(rows)}"
    )
    print(f"Wrote {output}")

    print(
        "\nExpected after the witness-backed promotion fix:\n"
        "  witness_confirmation_available -> recovery succeeds\n"
        "  requester_manifest_only         -> recovery fails, no replay\n"
        "Both cases must PASS."
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
            "15_provisional_holder_confirmation_safety"
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
        "--owner-dead-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--confirmation-log-timeout-seconds",
        type=float,
        default=5.0,
    )

    return p


def main() -> None:
    args = parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
