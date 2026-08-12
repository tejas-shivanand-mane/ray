#!/usr/bin/env python3
"""
Create combined no-failure recovery-succession plots from benchmark_runs.csv.

Outputs:
    throughput_all_payloads.png
    p95_latency_all_payloads.png

Each plot compares:
    Disabled, 1 holder, 2 holders, 3 holders, 4 holders

with one line per payload size.

Error bars show 95% confidence intervals across independent repetitions.
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


CONFIG_ORDER = [
    "Disabled",
    "Enabled-1-holder",
    "Enabled-2-holders",
    "Enabled-3-holders",
    "Enabled-4-holders",
]

X = [0, 1, 2, 3, 4]
X_LABELS = ["Disabled", "1", "2", "3", "4"]


_T95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}


def mean_ci95(values: list[float]) -> tuple[float, float]:
    values = [v for v in values if not math.isnan(v)]

    if not values:
        return math.nan, math.nan

    mean = statistics.fmean(values)

    if len(values) == 1:
        return mean, 0.0

    stdev = statistics.stdev(values)
    df = len(values) - 1
    tcrit = _T95.get(df, 1.96)
    ci95 = tcrit * stdev / math.sqrt(len(values))

    return mean, ci95


def human_size(size_bytes: int) -> str:
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):g} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:g} KiB"
    return f"{size_bytes} B"


def load_runs(path: Path):
    rows = []

    with path.open(newline="") as f:
        reader = csv.DictReader(f)

        required = {
            "config",
            "payload_name",
            "payload_bytes",
            "throughput_rps",
            "latency_p95_ms",
        }

        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"{path} is missing required columns: {sorted(missing)}"
            )

        for row in reader:
            rows.append(
                {
                    "config": row["config"],
                    "payload_name": row["payload_name"],
                    "payload_bytes": int(row["payload_bytes"]),
                    "throughput_rps": float(row["throughput_rps"]),
                    "latency_p95_ms": float(row["latency_p95_ms"]),
                }
            )

    if not rows:
        raise ValueError(f"No benchmark rows found in {path}")

    return rows


def aggregate(rows):
    grouped = defaultdict(lambda: defaultdict(list))
    payload_sizes = {}

    for row in rows:
        payload_name = row["payload_name"]
        payload_sizes[payload_name] = row["payload_bytes"]

        key = (payload_name, row["config"])
        grouped[key]["throughput_rps"].append(row["throughput_rps"])
        grouped[key]["latency_p95_ms"].append(row["latency_p95_ms"])

    payloads = sorted(
        payload_sizes,
        key=lambda name: payload_sizes[name],
    )

    summary = {}

    for payload in payloads:
        for config in CONFIG_ORDER:
            values = grouped.get((payload, config))
            if not values:
                raise ValueError(
                    f"Missing data for payload={payload!r}, config={config!r}"
                )

            tp_mean, tp_ci = mean_ci95(values["throughput_rps"])
            lat_mean, lat_ci = mean_ci95(values["latency_p95_ms"])

            summary[(payload, config)] = {
                "throughput_mean": tp_mean,
                "throughput_ci95": tp_ci,
                "p95_latency_mean": lat_mean,
                "p95_latency_ci95": lat_ci,
            }

    return payloads, payload_sizes, summary


def plot_throughput(
    *,
    payloads,
    payload_sizes,
    summary,
    output: Path,
):
    fig, ax = plt.subplots(figsize=(9.2, 5.8))

    for payload in payloads:
        y = [
            summary[(payload, config)]["throughput_mean"]
            for config in CONFIG_ORDER
        ]

        yerr = [
            summary[(payload, config)]["throughput_ci95"]
            for config in CONFIG_ORDER
        ]

        ax.errorbar(
            X,
            y,
            yerr=yerr,
            marker="o",
            linewidth=1.8,
            capsize=3,
            label=f"{payload} ({human_size(payload_sizes[payload])})",
        )
    # ax.set_yscale("log")
    ax.set_xticks(X, X_LABELS)
    ax.set_xlabel("Recovery configuration (non-owner holders)")
    ax.set_ylabel("Completed pipelines/s")
    ax.set_title("No-failure throughput vs. recovery redundancy")
    # ax.grid(True, alpha=0.3)
    ax.legend(title="Payload")

    fig.tight_layout()

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=250, bbox_inches="tight")
    plt.close(fig)


def plot_latency(
    *,
    payloads,
    payload_sizes,
    summary,
    output: Path,
):
    fig, ax = plt.subplots(figsize=(9.2, 5.8))

    for payload in payloads:
        y = [
            summary[(payload, config)]["p95_latency_mean"]
            for config in CONFIG_ORDER
        ]

        yerr = [
            summary[(payload, config)]["p95_latency_ci95"]
            for config in CONFIG_ORDER
        ]

        ax.errorbar(
            X,
            y,
            yerr=yerr,
            marker="o",
            linewidth=1.8,
            capsize=3,
            label=f"{payload} ({human_size(payload_sizes[payload])})",
        )
    ax.set_yscale("log")
    ax.set_xticks(X, X_LABELS)
    ax.set_xlabel("Recovery configuration (non-owner holders)")
    ax.set_ylabel("P95 end-to-end latency (ms)")
    ax.set_title("No-failure P95 latency vs. recovery redundancy")
    # ax.grid(True, alpha=0.3)
    ax.legend(title="Payload")

    fig.tight_layout()

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=250, bbox_inches="tight")
    plt.close(fig)


def print_summary(payloads, payload_sizes, summary):
    print(
        "payload,config,throughput_mean,throughput_ci95,"
        "p95_latency_mean_ms,p95_latency_ci95_ms"
    )

    for payload in payloads:
        for config in CONFIG_ORDER:
            row = summary[(payload, config)]

            print(
                f"{payload} ({human_size(payload_sizes[payload])}),"
                f"{config},"
                f"{row['throughput_mean']:.6f},"
                f"{row['throughput_ci95']:.6f},"
                f"{row['p95_latency_mean']:.6f},"
                f"{row['p95_latency_ci95']:.6f}"
            )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "runs_csv",
        type=Path,
        help="Path to benchmark_runs.csv",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("recovery_benchmark_plots"),
    )

    args = parser.parse_args()

    rows = load_runs(args.runs_csv)

    payloads, payload_sizes, summary = aggregate(rows)

    throughput_output = (
        args.output_dir / "throughput_all_payloads.png"
    )

    latency_output = (
        args.output_dir / "p95_latency_all_payloads.png"
    )

    plot_throughput(
        payloads=payloads,
        payload_sizes=payload_sizes,
        summary=summary,
        output=throughput_output,
    )

    plot_latency(
        payloads=payloads,
        payload_sizes=payload_sizes,
        summary=summary,
        output=latency_output,
    )

    print_summary(
        payloads,
        payload_sizes,
        summary,
    )

    print(f"\nSaved: {throughput_output.resolve()}")
    print(f"Saved: {latency_output.resolve()}")


if __name__ == "__main__":
    main()
