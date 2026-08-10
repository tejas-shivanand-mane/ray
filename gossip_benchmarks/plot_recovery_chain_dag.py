#!/usr/bin/env python3
"""
Plot recovery-chain/DAG scaling results.

Recommended paper figure:
  x-axis: chain length
  y-axis: time since failure (s)
  series:
    - first replay start
    - last replay start
    - final recovered result

Only successful enabled runs are used for the latency figure.
Means are plotted with sample-standard-deviation error bars.

A second optional figure is also generated showing recovery success rate
for Disabled vs Enabled.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes"}


def parse_float(value: str) -> float:
    value = value.strip()
    if not value or value.lower() == "nan":
        return math.nan
    return float(value)


def load_rows(path: Path):
    rows = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "trial": int(row["trial"]),
                    "config": row["config"],
                    "enabled": parse_bool(row["recovery_enabled"]),
                    "chain_length": int(row["chain_length"]),
                    "success": parse_bool(row["success"]),
                    "first_replay_start_s": parse_float(row["first_replay_start_s"]),
                    "last_replay_start_s": parse_float(row["last_replay_start_s"]),
                    "failure_to_result_s": parse_float(row["failure_to_result_s"]),
                    "replay_to_result_s": parse_float(row["replay_to_result_s"]),
                    "stages_replayed": int(row["stages_replayed_after_failure"]),
                    "duplicates": int(row["stages_with_duplicate_replay"]),
                }
            )
    return rows


def mean_std(values):
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return math.nan, math.nan
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return mean, std


def aggregate_enabled(rows):
    grouped = defaultdict(list)
    for row in rows:
        if row["enabled"] and row["success"]:
            grouped[row["chain_length"]].append(row)

    summary = []
    for chain_length in sorted(grouped):
        group = grouped[chain_length]

        first_mean, first_std = mean_std(
            [r["first_replay_start_s"] for r in group]
        )
        last_mean, last_std = mean_std(
            [r["last_replay_start_s"] for r in group]
        )
        result_mean, result_std = mean_std(
            [r["failure_to_result_s"] for r in group]
        )
        replay_mean, replay_std = mean_std(
            [r["replay_to_result_s"] for r in group]
        )

        summary.append(
            {
                "chain_length": chain_length,
                "first_mean": first_mean,
                "first_std": first_std,
                "last_mean": last_mean,
                "last_std": last_std,
                "result_mean": result_mean,
                "result_std": result_std,
                "replay_mean": replay_mean,
                "replay_std": replay_std,
                "trials": len(group),
            }
        )

    return summary


def aggregate_success(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["config"], row["chain_length"])].append(row)

    configs = sorted({r["config"] for r in rows})
    chain_lengths = sorted({r["chain_length"] for r in rows})

    rates = {}
    for config in configs:
        for chain_length in chain_lengths:
            group = grouped.get((config, chain_length), [])
            if not group:
                rates[(config, chain_length)] = math.nan
            else:
                rates[(config, chain_length)] = (
                    100.0 * sum(r["success"] for r in group) / len(group)
                )

    return configs, chain_lengths, rates


def plot_latency(summary, output: Path):
    if not summary:
        raise RuntimeError("No successful enabled rows found.")

    x = [r["chain_length"] for r in summary]

    fig, ax = plt.subplots(figsize=(8.2, 5.4))

    ax.errorbar(
        x,
        [r["first_mean"] for r in summary],
        yerr=[r["first_std"] for r in summary],
        marker="o",
        linewidth=1.8,
        capsize=3,
        label="First replay start",
    )

    ax.errorbar(
        x,
        [r["last_mean"] for r in summary],
        yerr=[r["last_std"] for r in summary],
        marker="s",
        linewidth=1.8,
        capsize=3,
        label="Last replay start",
    )

    ax.errorbar(
        x,
        [r["result_mean"] for r in summary],
        yerr=[r["result_std"] for r in summary],
        marker="^",
        linewidth=1.8,
        capsize=3,
        label="Final recovered result",
    )

    ax.set_xlabel("Chain length (protected task stages)")
    ax.set_ylabel("Time since failure (s)")
    ax.set_title("Recursive recovery latency vs. chain length")
    ax.set_xticks(x)
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=250, bbox_inches="tight")
    plt.close(fig)


def plot_success(rows, output: Path):
    configs, chain_lengths, rates = aggregate_success(rows)

    fig, ax = plt.subplots(figsize=(8.2, 5.4))

    markers = ["o", "s", "^", "D"]

    for i, config in enumerate(configs):
        ax.plot(
            chain_lengths,
            [rates[(config, n)] for n in chain_lengths],
            marker=markers[i % len(markers)],
            linewidth=1.8,
            label=config,
        )

    ax.set_xlabel("Chain length (protected task stages)")
    ax.set_ylabel("Final-result recovery success rate (%)")
    ax.set_title("Recursive recovery success vs. chain length")
    ax.set_xticks(chain_lengths)
    ax.set_ylim(-5, 105)
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=250, bbox_inches="tight")
    plt.close(fig)


def print_summary(summary, rows):
    print(
        "chain_length,first_replay_mean_s,first_replay_std_s,"
        "last_replay_mean_s,last_replay_std_s,"
        "final_result_mean_s,final_result_std_s,"
        "replay_to_result_mean_s,replay_to_result_std_s"
    )

    for r in summary:
        print(
            f"{r['chain_length']},"
            f"{r['first_mean']:.3f},{r['first_std']:.3f},"
            f"{r['last_mean']:.3f},{r['last_std']:.3f},"
            f"{r['result_mean']:.3f},{r['result_std']:.3f},"
            f"{r['replay_mean']:.3f},{r['replay_std']:.3f}"
        )

    print("\nSuccess summary:")
    configs, chain_lengths, rates = aggregate_success(rows)
    for n in chain_lengths:
        pieces = [f"chain={n}"]
        for config in configs:
            pieces.append(f"{config}={rates[(config, n)]:.1f}%")
        print("  " + ", ".join(pieces))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to recovery_chain_dag_results.csv",
    )
    parser.add_argument(
        "--latency-output",
        type=Path,
        default=Path("recovery_chain_dag_latency.png"),
    )
    parser.add_argument(
        "--success-output",
        type=Path,
        default=Path("recovery_chain_dag_success.png"),
    )
    args = parser.parse_args()

    rows = load_rows(args.csv_path)
    summary = aggregate_enabled(rows)

    plot_latency(summary, args.latency_output)
    plot_success(rows, args.success_output)
    print_summary(summary, rows)

    print(f"\nSaved latency plot to {args.latency_output}")
    print(f"Saved success plot to {args.success_output}")


if __name__ == "__main__":
    main()
