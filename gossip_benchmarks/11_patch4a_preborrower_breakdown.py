#!/usr/bin/env python3
"""Patch 4A: pre-borrower recovery setup breakdown.

Fixed topology:
    head + producer + 4 spare worker nodes

Cases:
    Disabled
    Succession-R4

Actual downstream borrowers:
    0

The Succession-R4 run enables Patch 4A profiling and reports the recovery-specific
work that happens before any holder admission:
    - task argument recovery-metadata propagation
    - initial manifest construction
    - witness selection
      - synchronous GCS GetAllNoCache subset
    - TaskSpec recovery-manifest attachment
    - RegisterOwnedTask / return-ref recovery metadata

Outputs:
    preborrower_runs.csv
    preborrower_summary.csv
    plots/preborrower_stage_breakdown.png
    plots/preborrower_throughput.png
"""
from __future__ import annotations

import os

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
    disabled,
    mean_ci95,
    percentile,
    read_csv,
    safe_shutdown,
    succession,
    system_config,
    wait_for_cluster,
    write_csv,
)

TARGET_HOLDERS = 4

PROFILE_KEYS = [
    "profiling_enabled",
    "task_argument_metadata_calls",
    "task_argument_metadata_time_ns",
    "initial_manifest_build_count",
    "initial_manifest_build_time_ns",
    "initial_manifest_bytes",
    "witness_selection_count",
    "witness_selection_time_ns",
    "witness_gcs_query_count",
    "witness_gcs_query_time_ns",
    "task_spec_manifest_attach_count",
    "task_spec_manifest_attach_time_ns",
    "register_owned_task_count",
    "register_owned_task_time_ns",
]

COUNT_KEYS = [
    "initial_manifest_build_count",
    "witness_selection_count",
    "witness_gcs_query_count",
    "task_spec_manifest_attach_count",
    "register_owned_task_count",
]


@dataclass(frozen=True)
class Payload:
    name: str
    size_bytes: int


@dataclass
class Pending:
    submitted_ns: int


def parse_payload(text: str) -> Payload:
    try:
        name, raw = text.split(":", 1)
        size = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("payload must be NAME:BYTES") from exc

    if not name or size <= 0:
        raise argparse.ArgumentTypeError(
            "payload must have non-empty NAME and positive BYTES"
        )
    return Payload(name, size)


def methods() -> list[Method]:
    return [disabled(), succession(TARGET_HOLDERS)]


def start_cluster(
    method: Method,
    cpus_per_node: int,
    witness_count: int,
) -> tuple[Cluster, list[str]]:
    cluster = Cluster()

    cluster.add_node(
        num_cpus=0,
        _system_config=system_config(
            method,
            witness_count=witness_count,
            profiling_enabled=method.recovery_enabled,
        ),
        include_dashboard=False,
    )

    workers = [
        cluster.add_node(
            num_cpus=cpus_per_node,
            resources={"producer_node": 1},
        )
    ]

    # Keep the same six-logical-node topology used in the prior Patch 4A tests,
    # even though borrower_count is fixed at zero.
    for i in range(1, TARGET_HOLDERS + 1):
        workers.append(
            cluster.add_node(
                num_cpus=cpus_per_node,
                resources={f"spare_{i}": 1},
            )
        )

    return cluster, [node.node_id for node in workers]


def make_producer():
    @ray.remote(max_retries=2)
    def produce(request_id: int, payload_bytes: int) -> bytes:
        prefix = request_id.to_bytes(8, "little", signed=False)
        return prefix + b"x" * max(0, payload_bytes - len(prefix))

    return produce


def run_phase(
    *,
    produce: Any,
    producer_strategy: Any,
    payload_bytes: int,
    duration_s: float,
    inflight: int,
    wait_timeout_s: float,
    drain_timeout_s: float,
    collect_metrics: bool,
) -> dict[str, Any]:
    pending: dict[ray.ObjectRef, Pending] = {}
    request_id = 0
    completed_in_window = 0
    submitted_in_window = 0
    latencies_ms: list[float] = []

    start_ns = time.perf_counter_ns()
    end_ns = start_ns + int(duration_s * 1e9)

    def submit_one() -> None:
        nonlocal request_id, submitted_in_window

        submitted_ns = time.perf_counter_ns()
        ref = produce.options(
            scheduling_strategy=producer_strategy,
            num_cpus=1,
        ).remote(request_id, payload_bytes)

        pending[ref] = Pending(submitted_ns=submitted_ns)
        request_id += 1

        if submitted_ns < end_ns:
            submitted_in_window += 1

    for _ in range(inflight):
        submit_one()

    while time.perf_counter_ns() < end_ns:
        ready, _ = ray.wait(
            list(pending),
            num_returns=1,
            timeout=wait_timeout_s,
        )
        if not ready:
            continue

        ref = ready[0]
        state = pending.pop(ref)
        value = ray.get(ref)

        if not value:
            raise RuntimeError("empty payload")

        completed_ns = time.perf_counter_ns()

        if completed_ns < end_ns:
            completed_in_window += 1

        if collect_metrics:
            latencies_ms.append(
                (completed_ns - state.submitted_ns) / 1e6
            )

        if time.perf_counter_ns() < end_ns:
            submit_one()

    deadline = time.monotonic() + drain_timeout_s

    while pending:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"drain timeout with {len(pending)} tasks pending"
            )

        ready, _ = ray.wait(
            list(pending),
            num_returns=1,
            timeout=wait_timeout_s,
        )
        if not ready:
            continue

        ref = ready[0]
        state = pending.pop(ref)
        value = ray.get(ref)

        if not value:
            raise RuntimeError("empty payload")

        if collect_metrics:
            latencies_ms.append(
                (time.perf_counter_ns() - state.submitted_ns) / 1e6
            )

    if not collect_metrics:
        return {}

    return {
        "submitted_in_window": submitted_in_window,
        "completed_in_window": completed_in_window,
        "throughput_rps": completed_in_window / duration_s,
        "logical_payload_throughput_mib_s": (
            completed_in_window
            * payload_bytes
            / duration_s
            / (1024.0 * 1024.0)
        ),
        "latency_sample_count": len(latencies_ms),
        "latency_mean_ms": (
            statistics.fmean(latencies_ms)
            if latencies_ms
            else math.nan
        ),
        "latency_p50_ms": percentile(latencies_ms, 0.50),
        "latency_p95_ms": percentile(latencies_ms, 0.95),
        "latency_p99_ms": percentile(latencies_ms, 0.99),
    }


def profile_defaults(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    result = {
        key: (False if key == "profiling_enabled" else 0)
        for key in PROFILE_KEYS
    }
    if raw:
        for key in PROFILE_KEYS:
            if key in raw:
                result[key] = raw[key]
    return result


def avg_us(total_ns: Any, count: Any) -> float:
    count_i = int(count)
    if count_i <= 0:
        return math.nan
    return float(total_ns) / count_i / 1_000.0


def add_profile_derived(
    summary: dict[str, Any],
    profile: dict[str, Any],
) -> None:
    for key in PROFILE_KEYS:
        summary[f"profile_{key}"] = profile[key]

    arg_us = avg_us(
        profile["task_argument_metadata_time_ns"],
        profile["task_argument_metadata_calls"],
    )
    manifest_us = avg_us(
        profile["initial_manifest_build_time_ns"],
        profile["initial_manifest_build_count"],
    )
    witness_us = avg_us(
        profile["witness_selection_time_ns"],
        profile["witness_selection_count"],
    )
    gcs_us = avg_us(
        profile["witness_gcs_query_time_ns"],
        profile["witness_gcs_query_count"],
    )
    attach_us = avg_us(
        profile["task_spec_manifest_attach_time_ns"],
        profile["task_spec_manifest_attach_count"],
    )
    register_us = avg_us(
        profile["register_owned_task_time_ns"],
        profile["register_owned_task_count"],
    )

    summary["profile_task_argument_metadata_avg_us"] = arg_us
    summary["profile_initial_manifest_build_avg_us"] = manifest_us
    summary["profile_witness_selection_avg_us"] = witness_us
    summary["profile_witness_gcs_query_avg_us"] = gcs_us
    summary["profile_task_spec_manifest_attach_avg_us"] = attach_us
    summary["profile_register_owned_task_avg_us"] = register_us

    # GCS is a subset of witness selection. Do not double-count it in total setup.
    if not math.isnan(witness_us) and not math.isnan(gcs_us):
        summary["profile_witness_non_gcs_avg_us"] = max(
            0.0, witness_us - gcs_us
        )
    else:
        summary["profile_witness_non_gcs_avg_us"] = math.nan

    serial_components = [
        arg_us,
        manifest_us,
        witness_us,
        attach_us,
        register_us,
    ]
    finite_components = [
        value
        for value in serial_components
        if not math.isnan(value)
    ]

    explicit_total_us = (
        sum(finite_components)
        if finite_components
        else math.nan
    )

    summary["profile_explicit_recovery_setup_avg_us"] = explicit_total_us

    if not math.isnan(witness_us) and witness_us > 0 and not math.isnan(gcs_us):
        summary["profile_gcs_share_of_witness_selection_pct"] = (
            100.0 * gcs_us / witness_us
        )
    else:
        summary["profile_gcs_share_of_witness_selection_pct"] = math.nan

    if (
        not math.isnan(explicit_total_us)
        and explicit_total_us > 0
        and not math.isnan(gcs_us)
    ):
        summary["profile_gcs_share_of_explicit_setup_pct"] = (
            100.0 * gcs_us / explicit_total_us
        )
    else:
        summary["profile_gcs_share_of_explicit_setup_pct"] = math.nan

    build_count = int(profile["initial_manifest_build_count"])

    if build_count > 0:
        # Current C++ instrumentation records this before witness addresses
        # are appended, so name the derived value explicitly.
        summary["profile_pre_witness_manifest_avg_bytes"] = (
            float(profile["initial_manifest_bytes"]) / build_count
        )
    else:
        summary["profile_pre_witness_manifest_avg_bytes"] = math.nan

    counts = [int(profile[key]) for key in COUNT_KEYS]

    summary["profile_eligible_stage_counts_match"] = int(
        bool(counts) and len(set(counts)) == 1
    )

    # PopulateTaskArgumentMetadata is called from BuildCommonTaskSpec whenever
    # succession is enabled, so it may count more calls than eligible tasks in
    # a general workload. This benchmark contains only eligible producer tasks
    # after the profile reset, so equality is expected here too.
    summary["profile_argument_count_matches_eligible"] = int(
        int(profile["task_argument_metadata_calls"]) == build_count
    )


def run_one(
    args: argparse.Namespace,
    method: Method,
    payload: Payload,
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

        produce = make_producer()

        producer_strategy = NodeAffinitySchedulingStrategy(
            node_id=node_ids[0],
            soft=False,
        )

        # Warmup is a completely separate drained phase so the profile can be
        # reset immediately before the measured phase.
        run_phase(
            produce=produce,
            producer_strategy=producer_strategy,
            payload_bytes=payload.size_bytes,
            duration_s=args.warmup_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
            collect_metrics=False,
        )

        if method.recovery_enabled:
            global_worker.core_worker.reset_recovery_succession_profile()

        summary = run_phase(
            produce=produce,
            producer_strategy=producer_strategy,
            payload_bytes=payload.size_bytes,
            duration_s=args.duration_seconds,
            inflight=args.inflight,
            wait_timeout_s=args.wait_timeout_seconds,
            drain_timeout_s=args.drain_timeout_seconds,
            collect_metrics=True,
        )

        if method.recovery_enabled:
            profile = profile_defaults(
                global_worker.core_worker
                .get_recovery_succession_profile()
            )
        else:
            profile = profile_defaults()

        add_profile_derived(summary, profile)

        row = add_method_columns(
            {
                "repetition": repetition,
                "payload_name": payload.name,
                "payload_bytes": payload.size_bytes,
                "borrower_count": 0,
                "target_holders": (
                    TARGET_HOLDERS if method.recovery_enabled else 0
                ),
                **summary,
            },
            method,
        )

        if method.recovery_enabled:
            print(
                "  "
                f"tasks={profile['initial_manifest_build_count']} "
                f"arg={summary['profile_task_argument_metadata_avg_us']:.2f}us "
                f"manifest={summary['profile_initial_manifest_build_avg_us']:.2f}us "
                f"witness={summary['profile_witness_selection_avg_us']:.2f}us "
                f"gcs={summary['profile_witness_gcs_query_avg_us']:.2f}us "
                f"attach={summary['profile_task_spec_manifest_attach_avg_us']:.2f}us "
                f"register={summary['profile_register_owned_task_avg_us']:.2f}us "
                f"explicit_total={summary['profile_explicit_recovery_setup_avg_us']:.2f}us "
                f"counts_ok={summary['profile_eligible_stage_counts_match']}"
            )

        return row

    finally:
        safe_shutdown(ray, cluster)


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    groups = sorted(
        {
            (
                row["method_label"],
                row["method"],
                int(row["holders"]),
                row["payload_name"],
                int(row["payload_bytes"]),
            )
            for row in rows
        }
    )

    derived_profile_metrics = [
        "profile_task_argument_metadata_avg_us",
        "profile_initial_manifest_build_avg_us",
        "profile_witness_selection_avg_us",
        "profile_witness_gcs_query_avg_us",
        "profile_witness_non_gcs_avg_us",
        "profile_task_spec_manifest_attach_avg_us",
        "profile_register_owned_task_avg_us",
        "profile_explicit_recovery_setup_avg_us",
        "profile_gcs_share_of_witness_selection_pct",
        "profile_gcs_share_of_explicit_setup_pct",
        "profile_pre_witness_manifest_avg_bytes",
    ]

    for label, method_key, holders, payload_name, payload_bytes in groups:
        group = [
            row
            for row in rows
            if (
                row["method_label"] == label
                and row["payload_name"] == payload_name
            )
        ]

        t_mean, t_ci = mean_ci95(
            float(row["throughput_rps"]) for row in group
        )
        p50_mean, p50_ci = mean_ci95(
            float(row["latency_p50_ms"]) for row in group
        )
        p95_mean, p95_ci = mean_ci95(
            float(row["latency_p95_ms"]) for row in group
        )

        item: dict[str, Any] = {
            "method": method_key,
            "method_label": label,
            "holders": holders,
            "borrower_count": 0,
            "payload_name": payload_name,
            "payload_bytes": payload_bytes,
            "repetitions": len(group),
            "throughput_mean_rps": t_mean,
            "throughput_ci95_rps": t_ci,
            "p50_latency_mean_ms": p50_mean,
            "p50_latency_ci95_ms": p50_ci,
            "p95_latency_mean_ms": p95_mean,
            "p95_latency_ci95_ms": p95_ci,
            "profile_eligible_stage_counts_match_all": min(
                int(row["profile_eligible_stage_counts_match"])
                for row in group
            ),
            "profile_argument_count_matches_eligible_all": min(
                int(row["profile_argument_count_matches_eligible"])
                for row in group
            ),
        }

        for metric in derived_profile_metrics:
            values = [
                float(row[metric])
                for row in group
                if not math.isnan(float(row[metric]))
            ]

            item[f"{metric}_mean"] = (
                statistics.fmean(values)
                if values
                else math.nan
            )

        out.append(item)

    # Direct Disabled-vs-Succession overhead.
    for item in out:
        item["succession_throughput_loss_pct"] = ""
        item["succession_p50_latency_increase_pct"] = ""
        item["succession_p95_latency_increase_pct"] = ""

        if item["method"] != "succession":
            continue

        matches = [
            candidate
            for candidate in out
            if (
                candidate["method"] == "disabled"
                and candidate["payload_name"] == item["payload_name"]
            )
        ]

        if not matches:
            continue

        baseline = matches[0]

        base_t = float(baseline["throughput_mean_rps"])
        succ_t = float(item["throughput_mean_rps"])

        base_p50 = float(baseline["p50_latency_mean_ms"])
        succ_p50 = float(item["p50_latency_mean_ms"])

        base_p95 = float(baseline["p95_latency_mean_ms"])
        succ_p95 = float(item["p95_latency_mean_ms"])

        if base_t > 0:
            item["succession_throughput_loss_pct"] = (
                100.0 * (base_t - succ_t) / base_t
            )

        if base_p50 > 0:
            item["succession_p50_latency_increase_pct"] = (
                100.0 * (succ_p50 - base_p50) / base_p50
            )

        if base_p95 > 0:
            item["succession_p95_latency_increase_pct"] = (
                100.0 * (succ_p95 - base_p95) / base_p95
            )

    return out


def run(args: argparse.Namespace) -> None:
    rows: list[dict[str, Any]] = []

    cases = [
        (method, payload)
        for payload in args.payloads
        for method in methods()
    ]

    rng = random.Random(args.seed)
    total = args.repetitions * len(cases)
    index = 0

    for repetition in range(1, args.repetitions + 1):
        order = cases[:]

        if not args.fixed_order:
            rng.shuffle(order)

        for method, payload in order:
            index += 1

            print(
                f"[{index}/{total}] "
                f"rep={repetition} "
                f"payload={payload.name} "
                f"method={method.label} "
                f"borrowers=0"
            )

            rows.append(
                run_one(
                    args,
                    method,
                    payload,
                    repetition,
                )
            )

    root = Path(args.output_dir)
    write_csv(root / "preborrower_runs.csv", rows)
    write_csv(
        root / "preborrower_summary.csv",
        summarize(rows),
    )

    print(f"Wrote results to {root}")


def plot(args: argparse.Namespace) -> None:
    import matplotlib.pyplot as plt

    root = Path(args.output_dir)
    rows = read_csv(root / "preborrower_summary.csv")

    plot_dir = root / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)

    payloads = sorted(
        {
            (int(row["payload_bytes"]), row["payload_name"])
            for row in rows
        }
    )

    for payload_bytes, payload_name in payloads:
        subset = [
            row
            for row in rows
            if row["payload_name"] == payload_name
        ]

        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in payload_name
        )

        labels = []
        values = []

        for method in methods():
            found = [
                row
                for row in subset
                if row["method_label"] == method.label
            ]

            if found:
                labels.append(method.label)
                values.append(
                    float(found[0]["throughput_mean_rps"])
                )

        plt.figure(figsize=(6.4, 4.6))
        plt.bar(labels, values)
        plt.ylabel("Completed tasks / s")
        plt.title(
            f"Zero-borrower throughput — "
            f"{payload_name} ({payload_bytes} B)"
        )
        plt.tight_layout()
        plt.savefig(
            plot_dir / f"preborrower_throughput_{safe_name}.png",
            dpi=200,
        )
        plt.close()

        succession_rows = [
            row
            for row in subset
            if row["method"] == "succession"
        ]

        if not succession_rows:
            continue

        row = succession_rows[0]

        stage_labels = [
            "Arg metadata",
            "Manifest build",
            "Witness GCS",
            "Witness non-GCS",
            "Manifest attach",
            "Register owner",
        ]

        stage_values = [
            float(
                row[
                    "profile_task_argument_metadata_avg_us_mean"
                ]
            ),
            float(
                row[
                    "profile_initial_manifest_build_avg_us_mean"
                ]
            ),
            float(
                row[
                    "profile_witness_gcs_query_avg_us_mean"
                ]
            ),
            float(
                row[
                    "profile_witness_non_gcs_avg_us_mean"
                ]
            ),
            float(
                row[
                    "profile_task_spec_manifest_attach_avg_us_mean"
                ]
            ),
            float(
                row[
                    "profile_register_owned_task_avg_us_mean"
                ]
            ),
        ]

        plt.figure(figsize=(8.6, 4.8))
        plt.bar(stage_labels, stage_values)
        plt.ylabel("Average time per task (µs)")
        plt.title(
            f"Succession-R4 pre-borrower recovery setup — "
            f"{payload_name}"
        )
        plt.xticks(rotation=25, ha="right")
        plt.tight_layout()
        plt.savefig(
            plot_dir / f"preborrower_stage_breakdown_{safe_name}.png",
            dpi=200,
        )
        plt.close()


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()

    p.add_argument(
        "command",
        choices=["run", "plot", "run-and-plot"],
        nargs="?",
        default="run-and-plot",
    )

    p.add_argument(
        "--output-dir",
        default=(
            "gossip_benchmarks/results/"
            "11_patch4a_preborrower_breakdown"
        ),
    )

    p.add_argument(
        "--warmup-seconds",
        type=float,
        default=5.0,
    )

    p.add_argument(
        "--duration-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--inflight",
        type=int,
        default=64,
    )

    p.add_argument(
        "--repetitions",
        type=int,
        default=3,
    )

    p.add_argument(
        "--payloads",
        type=parse_payload,
        nargs="+",
        default=[Payload("1KiB", 1024)],
    )

    p.add_argument(
        "--cpus-per-node",
        type=int,
        default=3,
    )

    p.add_argument(
        "--witness-count",
        type=int,
        default=2,
    )

    p.add_argument(
        "--cluster-timeout-seconds",
        type=float,
        default=30.0,
    )

    p.add_argument(
        "--wait-timeout-seconds",
        type=float,
        default=1.0,
    )

    p.add_argument(
        "--drain-timeout-seconds",
        type=float,
        default=120.0,
    )

    p.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    p.add_argument(
        "--fixed-order",
        action="store_true",
    )

    return p


def main() -> None:
    args = parser().parse_args()

    if args.command in {"run", "run-and-plot"}:
        run(args)

    if args.command in {"plot", "run-and-plot"}:
        plot(args)


if __name__ == "__main__":
    main()
