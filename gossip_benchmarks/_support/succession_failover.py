#!/usr/bin/env python3
"""Ordinary K=1 failover, concurrent recovery, retry-budget and late-borrower checks.

The owner independently exports to H1/H2, then late requesters after witness
confirmation. R=2/W=2. Each case uses a fresh cluster. Failures exit nonzero."""

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

from common import (
    add_method_columns,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

# Two non-owner holders are sufficient for A -> H1 -> H2.
METHOD = succession(2)
WITNESS_COUNT = 2

ALL_CASES = (
    "sequential_failover",
    "concurrent_recovery",
    "retry_version_fallback",
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

        # Some Ray versions remove a dead node entry entirely.
        present = any(
            normalize_node_id(info.get("NodeID", "")) == target
            for info in last_nodes
        )
        if not present:
            return

        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for node {node_id} to become DEAD. "
        f"Last ray.nodes()={last_nodes}"
    )


def marker_lines(path: Path, token: str) -> list[str]:
    if not path.exists():
        return []

    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []

    prefix = f"START,{token},"
    return [line for line in lines if line.startswith(prefix)]


def count_starts(path: Path, token: str) -> int:
    return len(marker_lines(path, token))


def wait_for_start_count(
    path: Path,
    token: str,
    expected: int,
    timeout_s: float,
) -> int:
    deadline = time.monotonic() + timeout_s
    last = 0

    while time.monotonic() < deadline:
        last = count_starts(path, token)
        if last >= expected:
            return last
        time.sleep(0.05)

    raise TimeoutError(
        f"Timed out waiting for {expected} START markers for token={token}. "
        f"Observed {last}. Lines={marker_lines(path, token)}"
    )


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


def try_ray_get(
    ref,
    timeout_s: float,
) -> tuple[bool, Any, str]:
    try:
        value = ray.get(ref, timeout=timeout_s)
        return True, value, ""
    except Exception as exc:
        return False, None, f"{type(exc).__name__}: {exc}"


def start_cluster(args: argparse.Namespace):
    cluster = Cluster()

    # Two witness nodes survive; H1/H2 are added after the initial manifest.
    head_node = cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            METHOD,
            witness_count=WITNESS_COUNT,
            profiling_enabled=True,
            object_timeout_ms=args.object_timeout_ms,
        ),
        include_dashboard=False,
    )

    cluster.add_node(num_cpus=0, resources={"second_witness": 1})
    owner_node = cluster.add_node(
        num_cpus=2,
        resources={"owner_node": 1},
    )

    return cluster, head_node, owner_node


def remote_types():
    @ray.remote
    def work(
        duration_s: float,
        payload_bytes: int,
        marker: str,
        token: str,
    ) -> bytes:
        # Include node/worker identity for later diagnosis while keeping the
        # START prefix easy to count.
        ctx = ray.get_runtime_context()
        try:
            node_id = ctx.get_node_id()
        except Exception:
            node_id = "unknown"

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"START,{token},{time.time_ns()},{os.getpid()},{node_id}\n"
            )

        time.sleep(duration_s)

        with open(marker, "a", buffering=1) as f:
            f.write(
                f"FINISH,{token},{time.time_ns()},{os.getpid()},{node_id}\n"
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
            max_retries: int,
        ):
            self.ref = work.options(
                    max_retries=max_retries,
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=self.node_id,
                        # Start on A, but allow StartRecoveryReplay() to clear
                        # affinity after A dies.
                        soft=True,
                    ),
                    num_cpus=1,
                ).remote(
                    duration_s,
                    payload_bytes,
                    marker,
                    token,
                )
            return self.ref.hex()

        def export_to(self, other):
            return ray.get(other.hold.remote([self.ref]))

        def reset_profile(self):
            from ray._private.worker import global_worker

            global_worker.core_worker.reset_recovery_succession_profile()
            return True

        def profile(self):
            from ray._private.worker import global_worker

            return dict(
                global_worker.core_worker.get_recovery_succession_profile()
            )

    @ray.remote(max_restarts=0, max_concurrency=2)
    class Holder:
        def __init__(self, label: str):
            self.label = label
            self.ref = None

        def hold(self, wrapped):
            self.ref = wrapped[0]
            return self.label

        def ping(self):
            return self.label

    @ray.remote(max_restarts=0, max_concurrency=4)
    class Requester:
        def __init__(self, label: str):
            self.label = label
            self.ref = None

        def hold(self, wrapped):
            self.ref = wrapped[0]
            return self.label

        def get_value(self):
            if self.ref is None:
                raise RuntimeError(f"{self.label} has no ObjectRef")
            value = ray.get(self.ref)
            return len(value)

        def ping(self):
            return self.label

    return work, Owner, Holder, Requester


def form_two_holders(
    *,
    args: argparse.Namespace,
    cluster: Cluster,
    owner,
    ref,
    Holder,
    Requester,
    requester_count: int,
):
    """Admit independent H1/H2 using direct owner exports, then late requesters."""
    h1_node = cluster.add_node(
        num_cpus=2,
        resources={"holder1_node": 1},
    )
    h2_node = cluster.add_node(
        num_cpus=2,
        resources={"holder2_node": 1},
    )
    requester_node = cluster.add_node(
        num_cpus=2,
        resources={"requester_node": 1},
    )

    wait_for_cluster(
        ray,
        6,
        args.cluster_timeout_seconds,
    )

    h1 = Holder.options(
        resources={"holder1_node": 0.01},
        num_cpus=0,
    ).remote("H1")
    h2 = Holder.options(
        resources={"holder2_node": 0.01},
        num_cpus=0,
    ).remote("H2")

    requesters = [
        Requester.options(
            resources={"requester_node": 0.01},
            num_cpus=0,
        ).remote(f"R{i + 1}")
        for i in range(requester_count)
    ]

    ray.get([h1.ping.remote(), h2.ping.remote()])
    if requesters:
        ray.get([requester.ping.remote() for requester in requesters])

    # Measure only holder formation.
    ray.get(owner.reset_profile.remote())

    # A -> H1.
    ray.get(owner.export_to.remote(h1))

    profile_h1 = wait_for_profile(
        owner,
        lambda p: (
            int(p.get("holder_admissions_committed", 0)) >= 1
            and int(p.get("max_non_owner_holders", 0)) >= 1
        ),
        args.admission_timeout_seconds,
        "committed H1",
    )

    # Owner independently installs/adopts H2 after H1 is durable.
    ray.get(owner.export_to.remote(h2))

    profile_h2 = wait_for_profile(
        owner,
        lambda p: (
            int(p.get("holder_admissions_committed", 0)) >= 2
            and int(p.get("max_non_owner_holders", 0)) >= 2
        ),
        args.admission_timeout_seconds,
        "committed H2",
    )

    # Late borrowers receive current metadata directly from the owner.
    for requester in requesters:
        ray.get(owner.export_to.remote(requester))

    return {
        "h1": h1,
        "h2": h2,
        "requesters": requesters,
        "h1_node": h1_node,
        "h2_node": h2_node,
        "requester_node": requester_node,
        "profile_h1": profile_h1,
        "profile_h2": profile_h2,
    }


def setup_case(
    args: argparse.Namespace,
    *,
    max_retries: int,
    requester_count: int,
):
    cluster = None
    marker = (
        Path(tempfile.gettempdir())
        / f"ray_recovery_correctness_{uuid.uuid4().hex}.csv"
    )

    try:
        cluster, _head_node, owner_node = start_cluster(args)
    
        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )
    
        wait_for_cluster(
            ray,
            3,
            args.cluster_timeout_seconds,
        )
    
        _work, Owner, Holder, Requester = remote_types()
    
        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(owner_node.node_id)
    
        ray.get(owner.profile.remote())
    
        token = uuid.uuid4().hex
    
        wrapped = ray.get(
            owner.dispatch.remote(
                args.task_duration_seconds,
                args.payload_bytes,
                str(marker),
                token,
                max_retries,
            )
        )
        ref = wrapped  # Object ID only; the driver never borrows the task output.
    
        # Ensure original execution has actually begun before creating H1/H2.
        wait_for_start_count(
            marker,
            token,
            1,
            args.start_timeout_seconds,
        )
    
        formed = form_two_holders(
            args=args,
            cluster=cluster,
            owner=owner,
            ref=ref,
            Holder=Holder,
            Requester=Requester,
            requester_count=requester_count,
        )
    
        return {
            "cluster": cluster,
            "owner_node": owner_node,
            "owner": owner,
            "ref": ref,
            "marker": marker,
            "token": token,
            **formed,
        }
    except BaseException:
        safe_shutdown(ray, cluster)
        marker.unlink(missing_ok=True)
        raise


def cleanup_case(state: dict[str, Any] | None) -> None:
    if not state:
        return

    cluster = state.get("cluster")
    marker = state.get("marker")

    safe_shutdown(ray, cluster)

    if isinstance(marker, Path):
        try:
            marker.unlink()
        except OSError:
            pass


def formation_fields(profile: dict[str, Any]) -> dict[str, int]:
    return {
        "holder_admissions_committed": int(
            profile.get("holder_admissions_committed", 0)
        ),
        "max_non_owner_holders": int(
            profile.get("max_non_owner_holders", 0)
        ),
        "max_generation": int(profile.get("max_generation", 0)),
        "holder_install_rpcs_completed": int(
            profile.get("holder_install_rpcs_completed", 0)
        ),
        "witness_update_rpcs_completed": int(
            profile.get("witness_update_rpcs_completed", 0)
        ),
        "holder_commit_rpcs_completed": int(
            profile.get("holder_commit_rpcs_completed", 0)
        ),
    }


def run_sequential_failover(
    args: argparse.Namespace,
    trial: int,
) -> dict[str, Any]:
    state: dict[str, Any] | None = None

    try:
        state = setup_case(
            args,
            max_retries=2,
            requester_count=2,
        )

        cluster = state["cluster"]
        owner_node = state["owner_node"]
        h1_node = state["h1_node"]
        requesters = state["requesters"]
        marker = state["marker"]
        token = state["token"]

        r1, r2 = requesters

        formation = formation_fields(state["profile_h2"])
        formation_ok = (
            formation["holder_admissions_committed"] >= 2
            and formation["max_non_owner_holders"] >= 2
        )

        # Failure 1: A dies.
        cluster.remove_node(
            owner_node,
            allow_graceful=False,
        )
        wait_for_node_dead(
            owner_node.node_id,
            args.node_dead_timeout_seconds,
        )

        # R1 has the fully committed A/H1/H2 manifest and should choose H1.
        r1_future = r1.get_value.remote()

        wait_for_start_count(
            marker,
            token,
            2,
            args.replay_start_timeout_seconds,
        )

        # Failure 2: kill H1 while its replay is in flight.
        cluster.remove_node(
            h1_node,
            allow_graceful=False,
        )
        wait_for_node_dead(
            h1_node.node_id,
            args.node_dead_timeout_seconds,
        )

        # R2 independently has the pre-failure fully committed A/H1/H2
        # manifest, so this isolates the basic succession A -> H1 -> H2.
        r2_future = r2.get_value.remote()

        third_start_seen = True
        third_start_error = ""
        try:
            wait_for_start_count(
                marker,
                token,
                3,
                args.replay_start_timeout_seconds,
            )
        except Exception as exc:
            third_start_seen = False
            third_start_error = f"{type(exc).__name__}: {exc}"

        r2_ok, r2_value, r2_error = try_ray_get(
            r2_future,
            args.recovery_timeout_seconds,
        )

        # R1 is diagnostic only; after H1 dies it may fail, remain pending, or
        # follow later recovery depending on Ray timing.
        r1_ok, r1_value, r1_error = try_ray_get(
            r1_future,
            args.diagnostic_get_timeout_seconds,
        )

        starts = count_starts(marker, token)

        final_payload_ok = (
            r2_ok and int(r2_value) == args.payload_bytes
        )

        passed = (
            formation_ok
            and third_start_seen
            and starts >= 3
            and final_payload_ok
        )

        row = {
            "trial": trial,
            "case": "sequential_failover",
            **formation,
            "formation_ok": int(formation_ok),
            "owner_dead": 1,
            "h1_dead": 1,
            "execution_start_count": starts,
            "third_start_seen": int(third_start_seen),
            "third_start_error": third_start_error,
            "r1_resolved": int(r1_ok),
            "r1_value": r1_value if r1_ok else "",
            "r1_error": r1_error,
            "r2_resolved": int(r2_ok),
            "r2_value": r2_value if r2_ok else "",
            "r2_error": r2_error,
            "final_recovered": int(final_payload_ok),
            "duplicate_replay_detected": int(starts > 3),
            "pass": int(passed),
        }

        print(
            f"  formation={formation['holder_admissions_committed']} "
            f"holders={formation['max_non_owner_holders']} "
            f"starts={starts} "
            f"R2_recovered={int(final_payload_ok)} "
            f"PASS={int(passed)}"
        )

        if r2_error:
            print(f"    R2 error: {r2_error}")
        if third_start_error:
            print(f"    third-start error: {third_start_error}")

        return add_method_columns(row, METHOD)

    finally:
        cleanup_case(state)


def run_concurrent_recovery(
    args: argparse.Namespace,
    trial: int,
) -> dict[str, Any]:
    state: dict[str, Any] | None = None

    try:
        state = setup_case(
            args,
            max_retries=2,
            requester_count=2,
        )

        cluster = state["cluster"]
        owner_node = state["owner_node"]
        requesters = state["requesters"]
        marker = state["marker"]
        token = state["token"]

        formation = formation_fields(state["profile_h2"])
        formation_ok = (
            formation["holder_admissions_committed"] >= 2
            and formation["max_non_owner_holders"] >= 2
        )

        cluster.remove_node(
            owner_node,
            allow_graceful=False,
        )
        wait_for_node_dead(
            owner_node.node_id,
            args.node_dead_timeout_seconds,
        )

        # Submit back-to-back before waiting for either result.
        f1 = requesters[0].get_value.remote()
        f2 = requesters[1].get_value.remote()

        ok1, value1, error1 = try_ray_get(
            f1,
            args.recovery_timeout_seconds,
        )
        ok2, value2, error2 = try_ray_get(
            f2,
            args.recovery_timeout_seconds,
        )

        # Allow any competing replay to write its START marker.
        time.sleep(args.post_result_observation_seconds)
        starts = count_starts(marker, token)

        both_payloads_ok = (
            ok1
            and ok2
            and int(value1) == args.payload_bytes
            and int(value2) == args.payload_bytes
        )

        # One original execution + exactly one recovery replay is desired.
        duplicate_replay = starts > 2

        passed = (
            formation_ok
            and both_payloads_ok
            and starts == 2
        )

        row = {
            "trial": trial,
            "case": "concurrent_recovery",
            **formation,
            "formation_ok": int(formation_ok),
            "execution_start_count": starts,
            "requester1_resolved": int(ok1),
            "requester1_value": value1 if ok1 else "",
            "requester1_error": error1,
            "requester2_resolved": int(ok2),
            "requester2_value": value2 if ok2 else "",
            "requester2_error": error2,
            "both_payloads_ok": int(both_payloads_ok),
            "duplicate_replay_detected": int(duplicate_replay),
            "pass": int(passed),
        }

        print(
            f"  starts={starts} "
            f"both_resolved={int(both_payloads_ok)} "
            f"duplicate={int(duplicate_replay)} "
            f"PASS={int(passed)}"
        )

        return add_method_columns(row, METHOD)

    finally:
        cleanup_case(state)


def run_retry_version_fallback(
    args: argparse.Namespace,
    trial: int,
) -> dict[str, Any]:
    state: dict[str, Any] | None = None

    try:
        # max_retries=1 means only ONE recovery replay should be permitted
        # after the original execution.
        state = setup_case(
            args,
            max_retries=1,
            requester_count=1,
        )

        cluster = state["cluster"]
        owner_node = state["owner_node"]
        h1_node = state["h1_node"]
        requester = state["requesters"][0]
        marker = state["marker"]
        token = state["token"]

        formation = formation_fields(state["profile_h2"])
        formation_ok = (
            formation["holder_admissions_committed"] >= 2
            and formation["max_non_owner_holders"] >= 2
        )

        cluster.remove_node(
            owner_node,
            allow_graceful=False,
        )
        wait_for_node_dead(
            owner_node.node_id,
            args.node_dead_timeout_seconds,
        )

        # First recovery consumes the single recovery attempt.
        first_future = requester.get_value.remote()

        wait_for_start_count(
            marker,
            token,
            2,
            args.replay_start_timeout_seconds,
        )

        # Kill H1 before its recovery replay finishes.
        cluster.remove_node(
            h1_node,
            allow_graceful=False,
        )
        wait_for_node_dead(
            h1_node.node_id,
            args.node_dead_timeout_seconds,
        )

        # Reuse the SAME requester/CoreWorker. Its local metadata should have
        # observed the first recovery attempt. A second recovery must therefore
        # be rejected when max_retries=1.
        second_future = requester.get_value.remote()

        second_ok, second_value, second_error = try_ray_get(
            second_future,
            args.retry_case_timeout_seconds,
        )

        # The first get is diagnostic.
        first_ok, first_value, first_error = try_ray_get(
            first_future,
            args.diagnostic_get_timeout_seconds,
        )

        time.sleep(args.post_result_observation_seconds)
        starts = count_starts(marker, token)

        illegal_second_replay = starts >= 3
        illegal_success = (
            second_ok and int(second_value) == args.payload_bytes
        )

        passed = (
            formation_ok
            and starts == 2
            and not illegal_success
            and not illegal_second_replay
        )

        row = {
            "trial": trial,
            "case": "retry_version_fallback",
            **formation,
            "formation_ok": int(formation_ok),
            "max_retries": 1,
            "execution_start_count": starts,
            "first_request_resolved": int(first_ok),
            "first_request_value": first_value if first_ok else "",
            "first_request_error": first_error,
            "second_request_resolved": int(second_ok),
            "second_request_value": second_value if second_ok else "",
            "second_request_error": second_error,
            "illegal_second_replay": int(illegal_second_replay),
            "illegal_second_recovery_success": int(illegal_success),
            "pass": int(passed),
        }

        print(
            f"  starts={starts} "
            f"second_replay={int(illegal_second_replay)} "
            f"second_success={int(illegal_success)} "
            f"PASS={int(passed)}"
        )

        return add_method_columns(row, METHOD)

    finally:
        cleanup_case(state)


CASE_RUNNERS = {
    "sequential_failover": run_sequential_failover,
    "concurrent_recovery": run_concurrent_recovery,
    "retry_version_fallback": run_retry_version_fallback,
}


def normalize_cases(raw_cases: list[str]) -> list[str]:
    if not raw_cases:
        return ["sequential_failover"]

    if "all" in raw_cases:
        return list(ALL_CASES)

    # Preserve user order but remove duplicates.
    seen: set[str] = set()
    result: list[str] = []

    for case in raw_cases:
        if case not in CASE_RUNNERS:
            raise ValueError(f"Unknown case: {case}")
        if case not in seen:
            seen.add(case)
            result.append(case)

    return result


def run(args: argparse.Namespace) -> None:
    cases = normalize_cases(args.cases)
    rows: list[dict[str, Any]] = []

    total = args.trials * len(cases)
    index = 0

    for trial in range(1, args.trials + 1):
        for case in cases:
            index += 1
            print(
                f"[{index}/{total}] "
                f"trial={trial} "
                f"case={case}"
            )

            try:
                row = CASE_RUNNERS[case](args, trial)
            except Exception as exc:
                # Record infrastructure/protocol failure instead of losing the
                # whole experimental run.
                error = f"{type(exc).__name__}: {exc}"
                print(f"  CASE ERROR: {error}")

                row = add_method_columns(
                    {
                        "trial": trial,
                        "case": case,
                        "case_error": error,
                        "pass": 0,
                    },
                    METHOD,
                )

            rows.append(row)

    output = (
        Path(args.output_dir)
        / "recovery_correctness_suite.csv"
    )
    write_csv(output, rows)

    passed = sum(int(row.get("pass", 0)) for row in rows)

    print(f"\nPassed {passed}/{len(rows)} selected cases")
    print(f"Wrote {output}")

    by_case: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_case.setdefault(str(row.get("case", "")), []).append(row)

    print("\nPer-case summary:")
    for case in cases:
        case_rows = by_case.get(case, [])
        case_passed = sum(int(row.get("pass", 0)) for row in case_rows)
        print(f"  {case}: {case_passed}/{len(case_rows)} PASS")

    if passed != len(rows):
        raise SystemExit(f"Failed {len(rows) - passed} correctness cases")



def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument(
        "command",
        choices=["run"],
        nargs="?",
        default="run",
    )

    p.add_argument(
        "--cases",
        nargs="+",
        choices=[*ALL_CASES, "all"],
        default=["sequential_failover"],
        help="Use --cases all for the full fixture.",
    )

    p.add_argument("--trials", type=int, default=1)

    p.add_argument(
        "--output-dir",
        default=(
            "gossip_benchmarks/results/"
            "succession_correctness"
        ),
    )

    # Long enough that A and H1 can be killed while their executions are
    # definitely still in flight.
    p.add_argument(
        "--task-duration-seconds",
        type=float,
        default=12.0,
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
        default=20.0,
    )

    p.add_argument(
        "--node-dead-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--replay-start-timeout-seconds",
        type=float,
        default=20.0,
    )

    p.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--diagnostic-get-timeout-seconds",
        type=float,
        default=2.0,
    )

    p.add_argument(
        "--retry-case-timeout-seconds",
        type=float,
        default=8.0,
    )

    p.add_argument(
        "--post-result-observation-seconds",
        type=float,
        default=1.0,
    )

    return p


def main() -> None:
    args = parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
