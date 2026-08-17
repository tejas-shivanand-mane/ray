#!/usr/bin/env python3
"""Benchmark Recovery Succession vs the fixed witness-holder baseline.

This benchmark is intentionally centered on the architectural difference rather
than only wall-clock performance:

  * how many complete replay TaskSpecs are transmitted,
  * how many bytes of complete lineage are transmitted,
  * how many recovery-control request messages are generated,
  * how much recovery metadata is transmitted,
  * how many non-owner full-lineage holders are actually achieved.

No C++ changes are required.  The benchmark uses the profiling counters already
exported by the current Recovery Succession implementation.

Topology
--------
The driver is the object owner.  Each producer task executes on a dedicated
producer node.  The same returned ObjectRef is then borrowed directly from the
owner by B different consumer actors on B different nodes:

                       C1
                     /
    owner ---- O --- C2
                     \
                      C3 ...

Borrowers are introduced one stage at a time.  For Succession, the benchmark
waits for each holder admission stage to commit before introducing the next
borrower.  This measures the intended state-amplification property without
mixing in the separate "100 simultaneous stale candidates" problem.

For target R=4:
    Succession expected full TaskSpec copies/task = min(B, 4)
    Baseline   expected full TaskSpec copies/task = 4

The TaskSpec-size sweep pads the *producer task arguments*, while the producer
returns only a tiny integer.  The benchmark reports the measured serialized
TaskSpec bytes/copy, so the result does not rely on the requested padding size
being exactly equal to the protobuf size.

Important baseline note
-----------------------
The current witness-holder baseline is eager: it installs R full TaskSpecs
before normal task submission.  Therefore B=0 is intentionally excluded by
default.  For B>=1, the full-lineage copy/byte comparison is still the same
architectural comparison a lazy fixed-R baseline would incur after activation.

Outputs
-------
  recovery_state_runs.csv
  recovery_state_summary.csv
  recovery_state_paired.csv
  plots/lineage_byte_amplification.png
  plots/full_lineage_copies.png
  plots/recovery_control_messages.png
  plots/achieved_holders.png
  plots/end_to_end_rate.png
"""
from __future__ import annotations

import os

# Set before importing ray.
os.environ["RAY_BACKEND_LOG_LEVEL"] = "warning"
os.environ["RAY_DEDUP_LOGS"] = "1"

import argparse
import math
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ray
from ray._private.worker import global_worker
from ray.cluster_utils import Cluster
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from _benchmark_common import (
    Method,
    add_method_columns,
    mean_ci95,
    read_csv,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    witness_baseline,
    write_csv,
)

TARGET_HOLDERS = 4

# These are all present in the current repository's profile JSON.
PROFILE_KEYS = [
    "profiling_enabled",
    "candidate_reports_received",
    "candidate_reports_accepted",
    "holder_install_rpcs_sent",
    "holder_install_rpcs_completed",
    "holder_commit_rpcs_sent",
    "holder_commit_rpcs_completed",
    "witness_update_rpcs_sent",
    "witness_update_rpcs_completed",
    "task_spec_bytes_sent",
    "manifest_bytes_sent",
    "owner_task_spec_copy_count",
    "owner_task_spec_copy_time_ns",
    "holder_install_rpc_time_ns",
    "holder_commit_rpc_time_ns",
    "witness_update_rpc_time_ns",
    "witness_publish_count",
    "witness_publish_time_ns",
    "witness_publish_max_time_ns",
    "holder_admissions_committed",
    "holder_admission_time_ns",
    "holder_admission_max_time_ns",
    "manifest_generations_committed",
    "max_generation",
    "max_non_owner_holders",
    "frozen_commits",
]

ASYNC_PAIRS = [
    ("holder_install_rpcs_sent", "holder_install_rpcs_completed"),
    ("holder_commit_rpcs_sent", "holder_commit_rpcs_completed"),
    ("witness_update_rpcs_sent", "witness_update_rpcs_completed"),
]


@dataclass(frozen=True)
class SpecPadding:
    name: str
    size_bytes: int


def methods() -> list[Method]:
    return [
        succession(TARGET_HOLDERS),
        witness_baseline(TARGET_HOLDERS),
    ]


def parse_spec_padding(text: str) -> SpecPadding:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "TaskSpec padding must be NAME:BYTES"
        ) from exc

    if not name or size < 0:
        raise argparse.ArgumentTypeError(
            "TaskSpec padding needs a non-empty NAME and BYTES >= 0"
        )

    return SpecPadding(name, size)


def start_cluster(
    method: Method,
    cpus_per_node: int,
    witness_count: int,
) -> tuple[Cluster, list[str]]:
    """Create one owner node plus producer + four borrower nodes."""
    cluster = Cluster()

    # Driver/head: logical owner. Keep it CPU-less so user tasks do not run here.
    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=witness_count,
            profiling_enabled=True,
        ),
        include_dashboard=False,
    )

    workers = [
        cluster.add_node(
            num_cpus=cpus_per_node,
            resources={"producer_node": 1},
        )
    ]

    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(
                num_cpus=cpus_per_node,
                resources={f"consumer_{i}": 1},
            )
        )

    return cluster, [node.node_id for node in workers]


def make_remote_types():
    @ray.remote(max_retries=2)
    def produce(request_id: int, *padding: bytes) -> int:
        # Return stays tiny.  Padding exists only to enlarge the replay TaskSpec.
        if padding and padding[0]:
            # Force Python to touch the input; serialization already happened.
            _ = padding[0][0]
        return request_id

    @ray.remote(max_restarts=0, max_concurrency=128)
    class Consumer:
        def touch(self, wrapped_ref):
            # Keep the ObjectRef nested so the worker receives the reference
            # itself and Recovery Succession metadata, rather than only a
            # dereferenced Python value.
            ref = wrapped_ref[0]
            return int(ray.get(ref))

        def ping(self) -> int:
            import os
            return os.getpid()

    return produce, Consumer


def build_padding(total_bytes: int, chunk_bytes: int) -> tuple[bytes, ...]:
    """Use many small arguments so each argument remains an inline candidate."""
    if total_bytes <= 0:
        return ()

    chunks: list[bytes] = []
    remaining = total_bytes
    token = 1

    while remaining > 0:
        n = min(remaining, chunk_bytes)
        chunks.append(bytes([token % 251]) * n)
        token += 1
        remaining -= n

    return tuple(chunks)


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }

    if raw:
        for key in PROFILE_KEYS:
            if key in raw:
                out[key] = raw[key]

    return out


def get_profile() -> dict[str, Any]:
    return profile_defaults(
        global_worker.core_worker.get_recovery_succession_profile()
    )


def outstanding_async(profile: dict[str, Any]) -> int:
    return sum(
        max(0, int(profile[sent]) - int(profile[completed]))
        for sent, completed in ASYNC_PAIRS
    )


def wait_for_profile_quiescence(
    timeout_s: float,
    stable_s: float,
) -> tuple[dict[str, Any], bool]:
    deadline = time.monotonic() + timeout_s
    last_signature: tuple[Any, ...] | None = None
    stable_since: float | None = None
    last = get_profile()

    while time.monotonic() < deadline:
        last = get_profile()
        signature = tuple(last[key] for key in PROFILE_KEYS)
        now = time.monotonic()

        if outstanding_async(last) == 0:
            if signature == last_signature:
                if stable_since is None:
                    stable_since = now
                elif now - stable_since >= stable_s:
                    return last, True
            else:
                stable_since = now
        else:
            stable_since = None

        last_signature = signature
        time.sleep(0.05)

    return last, False


def wait_for_succession_admissions(
    expected_total: int,
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    got = 0

    while time.monotonic() < deadline:
        got = int(get_profile()["holder_admissions_committed"])
        if got >= expected_total:
            return
        time.sleep(0.02)

    raise TimeoutError(
        "Succession holder admissions did not complete: "
        f"expected >= {expected_total}, observed {got}"
    )


def safe_div(numer: float, denom: float) -> float:
    return numer / denom if denom else math.nan


def derived_counts(
    method: Method,
    profile: dict[str, Any],
    task_count: int,
) -> dict[str, Any]:
    """Derive comparable metrics using only existing runtime counters."""
    if method.baseline_enabled:
        # Baseline sends the full TaskSpec in UpdateRecoveryWitness.
        full_lineage_transfers = int(profile["witness_update_rpcs_sent"])
        achieved_holders = safe_div(full_lineage_transfers, task_count)
    else:
        # Succession sends the full TaskSpec in InstallRecoveryHolder.
        full_lineage_transfers = int(profile["holder_install_rpcs_sent"])
        achieved_holders = safe_div(
            int(profile["holder_admissions_committed"]),
            task_count,
        )

    full_lineage_bytes = int(profile["task_spec_bytes_sent"])
    recovery_metadata_bytes = int(profile["manifest_bytes_sent"])

    # Request-message count only.  This intentionally excludes replies and
    # transport framing because the current profile does not expose them.
    recovery_control_requests = (
        int(profile["candidate_reports_received"])
        + int(profile["holder_install_rpcs_sent"])
        + int(profile["holder_commit_rpcs_sent"])
        + int(profile["witness_update_rpcs_sent"])
    )

    return {
        "full_lineage_transfers": full_lineage_transfers,
        "full_lineage_copies_per_task": safe_div(
            full_lineage_transfers, task_count
        ),
        "full_lineage_bytes_total": full_lineage_bytes,
        "full_lineage_bytes_per_task": safe_div(
            full_lineage_bytes, task_count
        ),
        "measured_task_spec_bytes_per_copy": safe_div(
            full_lineage_bytes, full_lineage_transfers
        ),
        "recovery_metadata_bytes_total": recovery_metadata_bytes,
        "recovery_metadata_bytes_per_task": safe_div(
            recovery_metadata_bytes, task_count
        ),
        "recovery_control_requests_total": recovery_control_requests,
        "recovery_control_requests_per_task": safe_div(
            recovery_control_requests, task_count
        ),
        "candidate_reports_per_task": safe_div(
            int(profile["candidate_reports_received"]), task_count
        ),
        "candidate_accepts_per_task": safe_div(
            int(profile["candidate_reports_accepted"]), task_count
        ),
        "holder_install_rpcs_per_task": safe_div(
            int(profile["holder_install_rpcs_sent"]), task_count
        ),
        "witness_update_rpcs_per_task": safe_div(
            int(profile["witness_update_rpcs_sent"]), task_count
        ),
        "achieved_full_lineage_holders_per_task": achieved_holders,
    }


def run_one(
    args: argparse.Namespace,
    method: Method,
    padding_case: SpecPadding,
    borrower_count: int,
    repetition: int,
) -> dict[str, Any]:
    cluster = None

    try:
        cluster, node_ids = start_cluster(
            method,
            args.cpus_per_node,
            args.witness_count,
        )

        ray.init(
            address=cluster.address,
            log_to_driver=False,
            include_dashboard=False,
        )

        wait_for_cluster(
            ray,
            TARGET_HOLDERS + 2,
            args.cluster_timeout_seconds,
        )

        produce, Consumer = make_remote_types()

        consumers = [
            Consumer.options(
                resources={f"consumer_{i}": 0.01},
                num_cpus=0,
            ).remote()
            for i in range(1, TARGET_HOLDERS + 1)
        ]
        ray.get([consumer.ping.remote() for consumer in consumers])

        padding = build_padding(
            padding_case.size_bytes,
            args.inline_chunk_bytes,
        )

        # Exclude cluster/actor startup.
        global_worker.core_worker.reset_recovery_succession_profile()

        start_ns = time.perf_counter_ns()

        refs = [
            produce.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node_ids[0],
                    soft=False,
                ),
                num_cpus=1,
            ).remote(request_id, *padding)
            for request_id in range(args.task_count)
        ]

        # IMPORTANT: do NOT ray.get(refs) here.
        #
        # Recovery Succession uses lazy activation: the producer task is
        # protected when its returned ObjectRef is first exported/serialized to
        # a downstream borrower.  Waiting for every producer result before the
        # first export can let normal task-lineage cleanup happen first, leaving
        # no recoverable producer TaskSpec to activate.
        #
        # Submitting the first consumer stage below serializes the original refs
        # while the producer lineage is still recoverable.  ray.get() on the
        # consumer results then naturally waits for both producer completion and
        # downstream consumption.
        #
        # Shallow-wide direct-owner borrowing:
        # each stage gets the same original refs directly from the driver.
        for stage in range(borrower_count):
            touched = ray.get(
                [
                    consumers[stage].touch.remote([ref])
                    for ref in refs
                ]
            )

            if touched != list(range(args.task_count)):
                raise RuntimeError(
                    f"consumer stage {stage + 1} validation failed"
                )

            if not method.baseline_enabled:
                wait_for_succession_admissions(
                    args.task_count * (stage + 1),
                    args.admission_timeout_seconds,
                )

        profile, quiescent = wait_for_profile_quiescence(
            args.profile_quiescence_timeout_seconds,
            args.profile_stable_seconds,
        )

        elapsed_s = (time.perf_counter_ns() - start_ns) / 1e9
        derived = derived_counts(
            method,
            profile,
            args.task_count,
        )

        expected_transfers = args.task_count * (
            TARGET_HOLDERS
            if method.baseline_enabled
            else min(borrower_count, TARGET_HOLDERS)
        )

        row: dict[str, Any] = {
            "repetition": repetition,
            "target_holders": TARGET_HOLDERS,
            "borrower_count": borrower_count,
            "task_count": args.task_count,
            "task_spec_padding_name": padding_case.name,
            "task_spec_padding_bytes": padding_case.size_bytes,
            "inline_chunk_bytes": args.inline_chunk_bytes,
            "expected_full_lineage_transfers": expected_transfers,
            "full_lineage_transfer_count_ok": int(
                int(derived["full_lineage_transfers"])
                == expected_transfers
            ),
            "profile_quiescent": int(quiescent),
            "profile_async_outstanding": outstanding_async(profile),
            "elapsed_seconds": elapsed_s,
            "end_to_end_tasks_per_second": args.task_count / elapsed_s,
            **derived,
        }

        for key in PROFILE_KEYS:
            row[f"profile_{key}"] = profile[key]

        row = add_method_columns(row, method)

        print(
            "  "
            f"copies/task={row['full_lineage_copies_per_task']:.2f} "
            f"lineage_KiB/task="
            f"{row['full_lineage_bytes_per_task'] / 1024.0:.1f} "
            f"messages/task={row['recovery_control_requests_per_task']:.2f} "
            f"achieved={row['achieved_full_lineage_holders_per_task']:.2f} "
            f"valid={row['full_lineage_transfer_count_ok']} "
            f"quiescent={row['profile_quiescent']}"
        )

        return row

    finally:
        safe_shutdown(ray, cluster)


RUN_METRICS = [
    "full_lineage_copies_per_task",
    "full_lineage_bytes_per_task",
    "measured_task_spec_bytes_per_copy",
    "recovery_metadata_bytes_per_task",
    "recovery_control_requests_per_task",
    "candidate_reports_per_task",
    "candidate_accepts_per_task",
    "holder_install_rpcs_per_task",
    "witness_update_rpcs_per_task",
    "achieved_full_lineage_holders_per_task",
    "elapsed_seconds",
    "end_to_end_tasks_per_second",
]


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    groups = sorted(
        {
            (
                row["method"],
                row["method_label"],
                int(row["borrower_count"]),
                row["task_spec_padding_name"],
                int(row["task_spec_padding_bytes"]),
            )
            for row in rows
        },
        key=lambda x: (x[4], x[2], x[0]),
    )

    for method, method_label, borrowers, pad_name, pad_bytes in groups:
        matched = [
            row
            for row in rows
            if row["method"] == method
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == pad_name
        ]

        summary: dict[str, Any] = {
            "method": method,
            "method_label": method_label,
            "borrower_count": borrowers,
            "target_holders": TARGET_HOLDERS,
            "task_spec_padding_name": pad_name,
            "task_spec_padding_bytes": pad_bytes,
            "repetitions": len(matched),
            "all_runs_valid": int(
                all(
                    int(row["full_lineage_transfer_count_ok"]) == 1
                    and int(row["profile_quiescent"]) == 1
                    for row in matched
                )
            ),
        }

        for metric in RUN_METRICS:
            mean, ci95 = mean_ci95(
                float(row[metric]) for row in matched
            )
            summary[f"{metric}_mean"] = mean
            summary[f"{metric}_ci95"] = ci95

        out.append(summary)

    return out


def paired_rows(
    summary: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    cases = sorted(
        {
            (
                int(row["borrower_count"]),
                row["task_spec_padding_name"],
                int(row["task_spec_padding_bytes"]),
            )
            for row in summary
        },
        key=lambda x: (x[2], x[0]),
    )

    for borrowers, pad_name, pad_bytes in cases:
        succession_row = next(
            row
            for row in summary
            if row["method"] == "succession"
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == pad_name
        )
        baseline_row = next(
            row
            for row in summary
            if row["method"] == "witness_baseline"
            and int(row["borrower_count"]) == borrowers
            and row["task_spec_padding_name"] == pad_name
        )

        s_bytes = float(
            succession_row["full_lineage_bytes_per_task_mean"]
        )
        b_bytes = float(
            baseline_row["full_lineage_bytes_per_task_mean"]
        )

        s_messages = float(
            succession_row["recovery_control_requests_per_task_mean"]
        )
        b_messages = float(
            baseline_row["recovery_control_requests_per_task_mean"]
        )

        out.append(
            {
                "borrower_count": borrowers,
                "target_holders": TARGET_HOLDERS,
                "task_spec_padding_name": pad_name,
                "task_spec_padding_bytes": pad_bytes,
                "expected_lineage_amplification_baseline_over_succession": (
                    TARGET_HOLDERS
                    / max(1, min(borrowers, TARGET_HOLDERS))
                ),
                "measured_lineage_bytes_amplification_baseline_over_succession": (
                    safe_div(b_bytes, s_bytes)
                ),
                "succession_full_lineage_bytes_per_task": s_bytes,
                "baseline_full_lineage_bytes_per_task": b_bytes,
                "succession_full_lineage_copies_per_task": float(
                    succession_row["full_lineage_copies_per_task_mean"]
                ),
                "baseline_full_lineage_copies_per_task": float(
                    baseline_row["full_lineage_copies_per_task_mean"]
                ),
                "succession_recovery_control_requests_per_task": s_messages,
                "baseline_recovery_control_requests_per_task": b_messages,
                "message_ratio_succession_over_baseline": safe_div(
                    s_messages, b_messages
                ),
                "succession_achieved_holders_per_task": float(
                    succession_row[
                        "achieved_full_lineage_holders_per_task_mean"
                    ]
                ),
                "baseline_achieved_holders_per_task": float(
                    baseline_row[
                        "achieved_full_lineage_holders_per_task_mean"
                    ]
                ),
                "succession_end_to_end_tasks_per_second": float(
                    succession_row["end_to_end_tasks_per_second_mean"]
                ),
                "baseline_end_to_end_tasks_per_second": float(
                    baseline_row["end_to_end_tasks_per_second_mean"]
                ),
            }
        )

    return out


def run_experiment(args: argparse.Namespace) -> None:
    if any(
        borrowers < 1 or borrowers > TARGET_HOLDERS
        for borrowers in args.borrowers
    ):
        raise ValueError(
            "Use borrower counts from 1 through 4.  B=0 is intentionally "
            "excluded because the current fixed baseline is eager."
        )

    cases = [
        (method, padding, borrowers)
        for padding in args.task_spec_padding
        for borrowers in args.borrowers
        for method in methods()
    ]

    rng = random.Random(args.seed)
    rows: list[dict[str, Any]] = []

    for repetition in range(1, args.repetitions + 1):
        order = cases[:]

        if not args.fixed_order:
            rng.shuffle(order)

        for method, padding, borrowers in order:
            print(
                f"rep={repetition} "
                f"method={method.label} "
                f"B={borrowers} "
                f"padding={padding.name} "
                f"tasks={args.task_count}"
            )

            rows.append(
                run_one(
                    args,
                    method,
                    padding,
                    borrowers,
                    repetition,
                )
            )

    output_dir = Path(args.output_dir)
    write_csv(
        output_dir / "recovery_state_runs.csv",
        rows,
    )

    summary = summarize(rows)
    write_csv(
        output_dir / "recovery_state_summary.csv",
        summary,
    )

    write_csv(
        output_dir / "recovery_state_paired.csv",
        paired_rows(summary),
    )


def plot_results(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    output_dir = Path(args.output_dir)
    summary = read_csv(
        output_dir / "recovery_state_summary.csv"
    )
    paired = read_csv(
        output_dir / "recovery_state_paired.csv"
    )

    plot_dir = output_dir / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    borrower_values = sorted(
        {int(row["borrower_count"]) for row in paired}
    )

    size_cases = sorted(
        {
            (
                int(row["task_spec_padding_bytes"]),
                row["task_spec_padding_name"],
            )
            for row in paired
        }
    )

    # 1. Most important paper plot: byte amplification.
    plt.figure(figsize=(8, 5))

    for _, size_name in size_cases:
        rows = sorted(
            [
                row
                for row in paired
                if row["task_spec_padding_name"] == size_name
            ],
            key=lambda row: int(row["borrower_count"]),
        )

        plt.plot(
            [int(row["borrower_count"]) for row in rows],
            [
                float(
                    row[
                        "measured_lineage_bytes_amplification_"
                        "baseline_over_succession"
                    ]
                )
                for row in rows
            ],
            marker="o",
            label=size_name,
        )

    plt.plot(
        borrower_values,
        [
            TARGET_HOLDERS
            / max(1, min(b, TARGET_HOLDERS))
            for b in borrower_values
        ],
        marker="x",
        linestyle="--",
        label="R / min(B,R)",
    )

    plt.xlabel("Distinct downstream borrowers B")
    plt.ylabel("Baseline / Succession full-lineage bytes")
    plt.xticks(borrower_values)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        plot_dir / "lineage_byte_amplification.png",
        dpi=200,
    )
    plt.close()

    # 2. Full TaskSpec copies.
    plt.figure(figsize=(8, 5))

    for method, label in [
        ("succession", "Succession-R4"),
        ("witness_baseline", "WitnessBaseline-R4"),
    ]:
        xs: list[int] = []
        ys: list[float] = []

        for borrowers in borrower_values:
            values = [
                float(row["full_lineage_copies_per_task_mean"])
                for row in summary
                if row["method"] == method
                and int(row["borrower_count"]) == borrowers
            ]

            if values:
                xs.append(borrowers)
                ys.append(statistics.fmean(values))

        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Distinct downstream borrowers B")
    plt.ylabel("Complete TaskSpec transfers / task")
    plt.xticks(borrower_values)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        plot_dir / "full_lineage_copies.png",
        dpi=200,
    )
    plt.close()

    # 3. Control request messages: may favor baseline and exposes the tradeoff.
    plt.figure(figsize=(8, 5))

    for method, label in [
        ("succession", "Succession-R4"),
        ("witness_baseline", "WitnessBaseline-R4"),
    ]:
        xs = []
        ys = []

        for borrowers in borrower_values:
            values = [
                float(
                    row["recovery_control_requests_per_task_mean"]
                )
                for row in summary
                if row["method"] == method
                and int(row["borrower_count"]) == borrowers
            ]

            if values:
                xs.append(borrowers)
                ys.append(statistics.fmean(values))

        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Distinct downstream borrowers B")
    plt.ylabel("Recovery-control request messages / task")
    plt.xticks(borrower_values)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        plot_dir / "recovery_control_messages.png",
        dpi=200,
    )
    plt.close()

    # 4. Always pair storage savings with actual achieved protection.
    plt.figure(figsize=(8, 5))

    for method, label in [
        ("succession", "Succession-R4"),
        ("witness_baseline", "WitnessBaseline-R4"),
    ]:
        xs = []
        ys = []

        for borrowers in borrower_values:
            values = [
                float(
                    row[
                        "achieved_full_lineage_holders_per_task_mean"
                    ]
                )
                for row in summary
                if row["method"] == method
                and int(row["borrower_count"]) == borrowers
            ]

            if values:
                xs.append(borrowers)
                ys.append(statistics.fmean(values))

        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Distinct downstream borrowers B")
    plt.ylabel("Achieved non-owner full-lineage holders / task")
    plt.xticks(borrower_values)
    plt.ylim(bottom=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        plot_dir / "achieved_holders.png",
        dpi=200,
    )
    plt.close()

    # 5. Secondary end-to-end rate.
    plt.figure(figsize=(8, 5))

    for method, label in [
        ("succession", "Succession-R4"),
        ("witness_baseline", "WitnessBaseline-R4"),
    ]:
        xs = []
        ys = []

        for borrowers in borrower_values:
            values = [
                float(row["end_to_end_tasks_per_second_mean"])
                for row in summary
                if row["method"] == method
                and int(row["borrower_count"]) == borrowers
            ]

            if values:
                xs.append(borrowers)
                ys.append(statistics.fmean(values))

        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Distinct downstream borrowers B")
    plt.ylabel("End-to-end producer tasks / s")
    plt.xticks(borrower_values)
    plt.legend()
    plt.tight_layout()
    plt.savefig(
        plot_dir / "end_to_end_rate.png",
        dpi=200,
    )
    plt.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot"],
        nargs="?",
        default="run-and-plot",
    )

    parser.add_argument(
        "--output-dir",
        default=(
            "gossip_benchmarks/results/"
            "18_recovery_state_amplification"
        ),
    )

    parser.add_argument(
        "--task-count",
        type=int,
        default=50,
    )

    parser.add_argument(
        "--borrowers",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
    )

    parser.add_argument(
        "--task-spec-padding",
        type=parse_spec_padding,
        nargs="+",
        default=[
            SpecPadding("1KiB", 1024),
            SpecPadding("16KiB", 16 * 1024),
            SpecPadding("256KiB", 256 * 1024),
            SpecPadding("1MiB", 1024 * 1024),
        ],
        help=(
            "One or more NAME:BYTES cases.  The measured serialized "
            "TaskSpec bytes/copy are written to CSV."
        ),
    )

    parser.add_argument(
        "--inline-chunk-bytes",
        type=int,
        default=4096,
        help=(
            "Split TaskSpec padding into small by-value arguments. "
            "4096 is intentionally well below normal large-object thresholds."
        ),
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--cpus-per-node",
        type=int,
        default=2,
    )

    parser.add_argument(
        "--witness-count",
        type=int,
        default=2,
        help="Compact witnesses used by Succession. Baseline still uses R holders.",
    )

    parser.add_argument(
        "--cluster-timeout-seconds",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--admission-timeout-seconds",
        type=float,
        default=60.0,
    )

    parser.add_argument(
        "--profile-quiescence-timeout-seconds",
        type=float,
        default=30.0,
    )

    parser.add_argument(
        "--profile-stable-seconds",
        type=float,
        default=0.5,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    parser.add_argument(
        "--fixed-order",
        action="store_true",
    )

    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.task_count <= 0:
        raise ValueError("--task-count must be positive")

    if args.inline_chunk_bytes <= 0:
        raise ValueError("--inline-chunk-bytes must be positive")

    if args.command in {"run", "run-and-plot"}:
        run_experiment(args)

    if args.command in {"plot", "run-and-plot"}:
        plot_results(args)


if __name__ == "__main__":
    main()
