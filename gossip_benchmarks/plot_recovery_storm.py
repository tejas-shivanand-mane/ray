#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def finite(values):
    return [v for v in values if not math.isnan(v)]


def mean_sd(values):
    vals = finite(values)
    if not vals:
        return math.nan, 0.0
    return (
        statistics.fmean(vals),
        statistics.stdev(vals) if len(vals) > 1 else 0.0,
    )


def load_rows(path: Path):
    rows = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "trial": int(row["trial"]),
                    "config": row["config"],
                    "tasks": int(row["tasks"]),
                    "success_rate": to_float(row["success_rate"]),
                    "failure_to_first_success_s": to_float(
                        row["failure_to_first_success_s"]
                    ),
                    "failure_to_p50_success_s": to_float(
                        row["failure_to_p50_success_s"]
                    ),
                    "failure_to_p95_success_s": to_float(
                        row["failure_to_p95_success_s"]
                    ),
                    "failure_to_last_success_s": to_float(
                        row["failure_to_last_success_s"]
                    ),
                    "first_replay_start_s": to_float(
                        row["first_replay_start_s"]
                    ),
                    "last_replay_start_s": to_float(
                        row["last_replay_start_s"]
                    ),
                    "replay_start_spread_s": to_float(
                        row["replay_start_spread_s"]
                    ),
                    "recovery_throughput_objects_s": to_float(
                        row["recovery_throughput_objects_s"]
                    ),
                    "tasks_with_duplicate_replay": to_float(
                        row["tasks_with_duplicate_replay"]
                    ),
                }
            )
    return rows


def grouped_metric(rows, metric, config=None):
    grouped = defaultdict(list)
    for row in rows:
        if config is not None and row["config"] != config:
            continue
        grouped[row["tasks"]].append(row[metric])

    out = {}
    for tasks, values in grouped.items():
        out[tasks] = mean_sd(values)
    return out


def errorbar_series(ax, data, label, marker="o"):
    xs = sorted(data)
    ys = [data[x][0] for x in xs]
    es = [data[x][1] for x in xs]
    ax.errorbar(xs, ys, yerr=es, marker=marker, capsize=3, label=label)


def plot_success_rate(rows, output_dir: Path):
    fig, ax = plt.subplots(figsize=(8.2, 5.2))

    for config in ["Disabled", "Enabled"]:
        data = grouped_metric(rows, "success_rate", config=config)
        if data:
            errorbar_series(ax, data, config)

    task_counts = sorted({r["tasks"] for r in rows})
    ax.set_xticks(task_counts)
    ax.set_ylim(-0.05, 1.05)
    ax.set_xlabel("Recovery storm size (lost task outputs)")
    ax.set_ylabel("Recovery success rate")
    ax.set_title("Recovery success under correlated owner/producer failure")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "recovery_storm_success_rate.png", dpi=200)
    plt.close(fig)


def plot_latency_scaling(rows, output_dir: Path):
    fig, ax = plt.subplots(figsize=(8.5, 5.4))

    metrics = [
        ("failure_to_first_success_s", "First recovered result"),
        # ("failure_to_p50_success_s", "P50 recovered result"),
        ("failure_to_p95_success_s", "P95 recovered result"),
        ("failure_to_last_success_s", "Last recovered result"),
    ]

    for metric, label in metrics:
        data = grouped_metric(rows, metric, config="Enabled")
        if data:
            errorbar_series(ax, data, label)

    task_counts = sorted(
        {r["tasks"] for r in rows if r["config"] == "Enabled"}
    )
    ax.set_xticks(task_counts)
    ax.set_xlabel("Recovery storm size (lost task outputs)")
    ax.set_ylabel("Time from failure injection (s)")
    ax.set_title("Recovery latency as recovery-storm size increases")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "recovery_storm_latency_scaling.png", dpi=200)
    plt.close(fig)


def plot_replay_timing(rows, output_dir: Path):
    fig, ax = plt.subplots(figsize=(8.5, 5.4))

    metrics = [
        ("first_replay_start_s", "First replay start"),
        ("last_replay_start_s", "Last replay start"),
        ("replay_start_spread_s", "Replay-start spread"),
    ]

    for metric, label in metrics:
        data = grouped_metric(rows, metric, config="Enabled")
        if data:
            errorbar_series(ax, data, label)

    task_counts = sorted(
        {r["tasks"] for r in rows if r["config"] == "Enabled"}
    )
    ax.set_xticks(task_counts)
    ax.set_xlabel("Recovery storm size (lost task outputs)")
    ax.set_ylabel("Time (s)")
    ax.set_title("Replay initiation under a recovery storm")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "recovery_storm_replay_timing.png", dpi=200)
    plt.close(fig)


def plot_recovery_throughput(rows, output_dir: Path):
    fig, ax = plt.subplots(figsize=(8.2, 5.2))

    data = grouped_metric(
        rows,
        "recovery_throughput_objects_s",
        config="Enabled",
    )
    if data:
        errorbar_series(ax, data, "Enabled recovery")

    task_counts = sorted(
        {r["tasks"] for r in rows if r["config"] == "Enabled"}
    )
    ax.set_xticks(task_counts)
    ax.set_xlabel("Recovery storm size (lost task outputs)")
    ax.set_ylabel("Recovered outputs/s")
    ax.set_title("Effective recovery throughput")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_dir / "recovery_storm_throughput.png", dpi=200)
    plt.close(fig)


def print_summary(rows):
    enabled = [r for r in rows if r["config"] == "Enabled"]
    grouped = defaultdict(list)
    for row in enabled:
        grouped[row["tasks"]].append(row)

    print(
        "tasks,success_rate,first_replay_s,replay_spread_s,"
        "p95_result_s,last_result_s,recovery_throughput_objects_s,"
        "duplicate_replays"
    )

    for tasks in sorted(grouped):
        group = grouped[tasks]

        def avg(field):
            vals = finite([r[field] for r in group])
            return statistics.fmean(vals) if vals else math.nan

        print(
            f"{tasks},"
            f"{avg('success_rate'):.4f},"
            f"{avg('first_replay_start_s'):.4f},"
            f"{avg('replay_start_spread_s'):.4f},"
            f"{avg('failure_to_p95_success_s'):.4f},"
            f"{avg('failure_to_last_success_s'):.4f},"
            f"{avg('recovery_throughput_objects_s'):.4f},"
            f"{avg('tasks_with_duplicate_replay'):.2f}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("recovery_storm_plots"),
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    rows = load_rows(args.csv_path)

    plot_success_rate(rows, args.output_dir)
    plot_latency_scaling(rows, args.output_dir)
    plot_replay_timing(rows, args.output_dir)
    plot_recovery_throughput(rows, args.output_dir)
    print_summary(rows)

    print(f"\nPlots written to: {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
