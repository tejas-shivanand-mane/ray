#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "info")
os.environ.setdefault("RAY_DEDUP_LOGS", "0")

import ray
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@dataclass(frozen=True)
class Case:
    label: str
    recovery_enabled: bool


CASES = [
    Case("Disabled", False),
    Case("Enabled", True),
]


def find_log_lines(session_dirs: set[Path], text: str) -> list[str]:
    out: list[str] = []
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
                    out.append(f"{path.name}: {line}")
    return out


def wait_for_log_line(
    session_dirs: set[Path],
    text: str,
    timeout_s: float,
) -> list[str]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        matches = find_log_lines(session_dirs, text)
        if matches:
            return matches
        time.sleep(0.05)
    return []


def print_recovery_diagnostics(session_dirs: set[Path]) -> None:
    terms = [
        "Committed recovery succession manifest",
        "Applied committed recovery succession manifest",
        "Stored provisional recovery holder",
        "OWNER_DIED observed",
        "OWNER_DIED intercepted",
        "Preparing recovery succession replay attempt",
        "Promoted borrowed object to owned recovery return",
        "Removed stale holder-local OWNER_DIED",
        "Recovery succession replay accepted",
        "Submitting recovery succession replay",
        "Recovery succession accepted by holder",
        "future resolution restarted against acting holder",
        "Skipping known-dead recovery holder",
        "Confirmed stale local OWNER_DIED",
    ]

    seen: set[str] = set()
    for term in terms:
        for line in find_log_lines(session_dirs, term):
            if line not in seen:
                seen.add(line)
                print(f"      {line}")


def start_cluster(
    recovery_enabled: bool,
    object_timeout_ms: int,
) -> tuple[Cluster, Any]:
    cluster = Cluster()

    config: dict[str, Any] = {
        "enable_recovery_succession": recovery_enabled,
        "recovery_succession_witness_count": 2,
        "object_timeout_milliseconds": object_timeout_ms,
    }

    if recovery_enabled:
        # One explicit non-owner recovery holder.
        config["recovery_succession_target_holder_count"] = 1

    # Head + driver. Survives.
    cluster.add_node(
        num_cpus=0,
        _system_config=config,
        include_dashboard=False,
    )

    # Owner + original producer. This entire node is failed.
    failure_node = cluster.add_node(
        num_cpus=1,
        resources={"owner_node": 1},
    )

    # Explicit recovery holder.
    cluster.add_node(
        num_cpus=1,
        resources={"holder_1": 1},
    )

    # Extra live nodes for witness placement.
    cluster.add_node(
        num_cpus=1,
        resources={"witness_1": 1},
    )
    cluster.add_node(
        num_cpus=1,
        resources={"witness_2": 1},
    )

    # Persistent borrower. This worker survives the failure and is the worker
    # that performs every ray.get(), so it retains the recovery plan.
    cluster.add_node(
        num_cpus=1,
        resources={"borrower_node": 1},
    )

    return cluster, failure_node


def wait_for_cluster(expected_nodes: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    alive_count = 0

    while time.monotonic() < deadline:
        alive_count = len(
            [node for node in ray.nodes() if node["Alive"]]
        )
        if alive_count >= expected_nodes:
            return
        time.sleep(0.1)

    raise TimeoutError(
        f"Only {alive_count}/{expected_nodes} Ray nodes became alive"
    )


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(payload_bytes: int) -> bytes:
        return b"x" * payload_bytes

    @ray.remote(max_restarts=0, max_task_retries=0)
    class Owner:
        def __init__(self, failure_node_id: str):
            self.failure_node_id = failure_node_id

        def ping(self) -> int:
            return os.getpid()

        def create(self, payload_bytes: int):
            ref = produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=self.failure_node_id,
                    soft=True,
                ),
                num_cpus=1,
            ).remote(payload_bytes)

            # Nested so the driver does not resolve/fetch it.
            return [ref]

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Holder:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def export(self):
            return [self.ref]

        def ping(self) -> int:
            return os.getpid()

    @ray.remote(max_restarts=0, max_concurrency=1)
    class Borrower:
        def hold(self, wrapped_ref):
            self.ref = wrapped_ref[0]
            return True

        def ping(self) -> int:
            return os.getpid()

        def read(self):
            # Recovery happens inside this persistent worker's GetObjectsInternal.
            value = ray.get(self.ref)
            return len(value)

    return Owner, Holder, Borrower


def percentile(values: list[float], q: float) -> float:
    if not values:
        return math.nan

    values = sorted(values)

    if len(values) == 1:
        return values[0]

    pos = (len(values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))

    if lo == hi:
        return values[lo]

    frac = pos - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def bucketize(
    events: list[tuple[float, bool, float]],
    duration_s: float,
    bucket_s: float,
    failure_at_s: float,
    case: Case,
    trial: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    num_buckets = int(math.ceil(duration_s / bucket_s))

    for bucket_idx in range(num_buckets):
        start = bucket_idx * bucket_s
        end = min(duration_s, start + bucket_s)

        bucket_events = [
            event
            for event in events
            if start <= event[0] < end
        ]

        successful_latencies_ms = [
            event[2] * 1000.0
            for event in bucket_events
            if event[1]
        ]

        successes = len(successful_latencies_ms)
        failures = sum(
            1
            for event in bucket_events
            if not event[1]
        )

        width = max(end - start, 1e-9)

        rows.append(
            {
                "trial": trial,
                "config": case.label,
                "recovery_enabled": int(case.recovery_enabled),
                "elapsed_seconds": start,
                "bucket_seconds": width,
                "failure_at_seconds": failure_at_s,
                "successful_requests": successes,
                "failed_requests": failures,
                "throughput_rps": successes / width,
                "latency_p50_ms": percentile(
                    successful_latencies_ms, 0.50
                ),
                "latency_p95_ms": percentile(
                    successful_latencies_ms, 0.95
                ),
            }
        )

    return rows


FIELDS = [
    "trial",
    "config",
    "recovery_enabled",
    "elapsed_seconds",
    "bucket_seconds",
    "failure_at_seconds",
    "successful_requests",
    "failed_requests",
    "throughput_rps",
    "latency_p50_ms",
    "latency_p95_ms",
]


def run_case(
    *,
    case: Case,
    trial: int,
    duration_s: float,
    failure_at_s: float,
    bucket_s: float,
    payload_bytes: int,
    object_timeout_ms: int,
    cluster_timeout_s: float,
    formation_timeout_s: float,
    borrower_settle_s: float,
    request_timeout_s: float,
    failed_request_backoff_s: float,
) -> list[dict[str, Any]]:
    cluster = None
    session_dirs: set[Path] = set()

    try:
        cluster, failure_node = start_cluster(
            case.recovery_enabled,
            object_timeout_ms,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(6, cluster_timeout_s)

        Owner, Holder, Borrower = make_remote_types()

        failure_node_id = failure_node.node_id

        owner = Owner.options(
            resources={"owner_node": 0.01},
            num_cpus=0,
        ).remote(failure_node_id)

        holder = Holder.options(
            resources={"holder_1": 0.01},
            num_cpus=0,
        ).remote()

        borrower = Borrower.options(
            resources={"borrower_node": 0.01},
            num_cpus=0,
        ).remote()

        ray.get(
            [
                owner.ping.remote(),
                holder.ping.remote(),
                borrower.ping.remote(),
            ]
        )

        session_dirs = {
            Path(node.get_session_dir_path())
            for node in cluster.list_all_nodes()
        }

        # Create one reusable 2 MiB object on the failure node.
        base_ref = ray.get(
            owner.create.remote(payload_bytes)
        )[0]

        # Wait for the producer to finish, without fetching the object locally.
        ready, _ = ray.wait(
            [base_ref],
            num_returns=1,
            timeout=30.0,
            fetch_local=False,
        )

        if not ready:
            raise TimeoutError(
                "Base object did not finish production"
            )

        ref_for_borrower = base_ref

        if case.recovery_enabled:
            # Explicit holder becomes rank 1.
            if not ray.get(holder.hold.remote([base_ref])):
                raise RuntimeError(
                    "Recovery holder failed to hold ObjectRef"
                )

            needle = (
                "Committed recovery succession manifest after witness publication "
                "with 2 total members"
            )

            if not wait_for_log_line(
                session_dirs,
                needle,
                formation_timeout_s,
            ):
                print("  Formation diagnostics:")
                print_recovery_diagnostics(session_dirs)
                raise RuntimeError(
                    "Recovery holder did not commit before benchmark"
                )

            # Re-serialize from confirmed holder so the persistent borrower gets
            # the committed recovery manifest.
            ref_for_borrower = ray.get(
                holder.export.remote()
            )[0]

        # Most important difference from the old benchmark:
        # the SAME surviving borrower holds the reference before failure and
        # performs every get before and after failure.
        if not ray.get(
            borrower.hold.remote([ref_for_borrower])
        ):
            raise RuntimeError(
                "Borrower failed to hold ObjectRef"
            )

        # Give FutureResolver time to resolve/cache ownership and recovery
        # metadata before the failure.
        if borrower_settle_s > 0:
            time.sleep(borrower_settle_s)

        # Confirm normal operation before starting timed measurement.
        warmup_value = ray.get(
            borrower.read.remote(),
            timeout=10.0,
        )
        if warmup_value != payload_bytes:
            raise RuntimeError(
                "Warmup returned wrong object size"
            )

        print(
            f"  {case.label}: failure at {failure_at_s:.1f}s, "
            f"duration={duration_s:.1f}s"
        )

        events: list[tuple[float, bool, float]] = []
        benchmark_start = time.perf_counter()
        failure_injected = False

        while True:
            elapsed = time.perf_counter() - benchmark_start

            if elapsed >= duration_s:
                break

            if (
                not failure_injected
                and elapsed >= failure_at_s
            ):
                print(
                    f"  Injecting failure at t={elapsed:.3f}s"
                )

                cluster.remove_node(
                    failure_node,
                    allow_graceful=False,
                )

                failure_injected = True
                continue

            request_start = time.perf_counter()

            try:
                value = ray.get(
                    borrower.read.remote(),
                    timeout=request_timeout_s,
                )

                request_end = time.perf_counter()

                if value != payload_bytes:
                    raise RuntimeError(
                        f"Wrong result: expected {payload_bytes}, got {value}"
                    )

                events.append(
                    (
                        request_end - benchmark_start,
                        True,
                        request_end - request_start,
                    )
                )

            except Exception as exc:
                request_end = time.perf_counter()

                events.append(
                    (
                        request_end - benchmark_start,
                        False,
                        request_end - request_start,
                    )
                )

                # Print only the first few post-failure errors.
                post_failure_failures = sum(
                    1
                    for event in events
                    if (
                        not event[1]
                        and event[0] >= failure_at_s
                    )
                )

                if post_failure_failures <= 3:
                    print(
                        f"  Post-failure request failed: "
                        f"{type(exc).__name__}: {exc}"
                    )

                if failed_request_backoff_s > 0:
                    time.sleep(
                        failed_request_backoff_s
                    )

        print("  Recovery diagnostics:")
        print_recovery_diagnostics(
            session_dirs
        )

        rows = bucketize(
            events,
            duration_s,
            bucket_s,
            failure_at_s,
            case,
            trial,
        )

        pre_rows = [
            row
            for row in rows
            if row["elapsed_seconds"] < failure_at_s
        ]

        post_rows = [
            row
            for row in rows
            if row["elapsed_seconds"] >= failure_at_s
        ]

        pre_mean = (
            sum(
                row["throughput_rps"]
                for row in pre_rows
            )
            / max(len(pre_rows), 1)
        )

        post_max = max(
            (
                row["throughput_rps"]
                for row in post_rows
            ),
            default=0.0,
        )

        print(
            f"  {case.label}: "
            f"pre-failure mean={pre_mean:.2f} req/s, "
            f"post-failure max={post_max:.2f} req/s"
        )

        return rows

    finally:
        try:
            ray.shutdown()
        except Exception:
            pass

        if cluster is not None:
            try:
                cluster.shutdown()
            except Exception:
                pass


def write_csv(
    path: Path,
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        newline="",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser()

    p.add_argument(
        "--output",
        type=Path,
        default=Path(
            "recovery_availability_results.csv"
        ),
    )
    p.add_argument(
        "--trials",
        type=int,
        default=3,
    )
    p.add_argument(
        "--duration-seconds",
        type=float,
        default=50.0,
    )
    p.add_argument(
        "--failure-at-seconds",
        type=float,
        default=15.0,
    )
    p.add_argument(
        "--bucket-seconds",
        type=float,
        default=1.0,
    )
    p.add_argument(
        "--payload-bytes",
        type=int,
        default=2 * 1024 * 1024,
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
        "--formation-timeout-seconds",
        type=float,
        default=20.0,
    )
    p.add_argument(
        "--borrower-settle-seconds",
        type=float,
        default=2.0,
    )
    p.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=30.0,
    )
    p.add_argument(
        "--failed-request-backoff-seconds",
        type=float,
        default=0.02,
    )
    p.add_argument(
        "--seed",
        type=int,
        default=20260806,
    )
    p.add_argument(
        "--fixed-order",
        action="store_true",
    )

    args = p.parse_args()

    if args.failure_at_seconds <= 0:
        raise ValueError(
            "--failure-at-seconds must be > 0"
        )

    if (
        args.failure_at_seconds
        >= args.duration_seconds
    ):
        raise ValueError(
            "--failure-at-seconds must be smaller "
            "than --duration-seconds"
        )

    specs = [
        (case, trial)
        for trial in range(
            1,
            args.trials + 1,
        )
        for case in CASES
    ]

    if not args.fixed_order:
        random.Random(
            args.seed
        ).shuffle(specs)

    all_rows: list[dict[str, Any]] = []

    for i, (case, trial) in enumerate(
        specs,
        1,
    ):
        print(
            f"\n{'=' * 76}\n"
            f"Run {i}/{len(specs)} | "
            f"{case.label} | "
            f"trial={trial}/{args.trials}\n"
            f"{'=' * 76}",
            flush=True,
        )

        rows = run_case(
            case=case,
            trial=trial,
            duration_s=args.duration_seconds,
            failure_at_s=args.failure_at_seconds,
            bucket_s=args.bucket_seconds,
            payload_bytes=args.payload_bytes,
            object_timeout_ms=args.object_timeout_ms,
            cluster_timeout_s=args.cluster_timeout_seconds,
            formation_timeout_s=args.formation_timeout_seconds,
            borrower_settle_s=args.borrower_settle_seconds,
            request_timeout_s=args.request_timeout_seconds,
            failed_request_backoff_s=args.failed_request_backoff_seconds,
        )

        all_rows.extend(rows)

        write_csv(
            args.output,
            all_rows,
        )

    print(
        f"\nWrote {args.output.resolve()}"
    )


if __name__ == "__main__":
    main()