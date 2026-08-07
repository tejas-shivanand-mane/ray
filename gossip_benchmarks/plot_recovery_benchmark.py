#!/usr/bin/env python3
"""Create aggregate plots from benchmark_runs.csv."""

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np


CONFIG_ORDER = [
    "Disabled",
    "Enabled-1-holder",
    "Enabled-2-holders",
    "Enabled-3-holders",
    "Enabled-4-holders",
]


def read_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="") as file:
        for row in csv.DictReader(file):
            rows.append(
                {
                    "config": row["config"],
                    "payload_name": row["payload_name"],
                    "payload_bytes": int(row["payload_bytes"]),
                    "throughput_rps": float(row["throughput_rps"]),
                    "data_throughput_mib_s": float(row["data_throughput_mib_s"]),
                    "latency_mean_ms": float(row["latency_mean_ms"]),
                    "latency_p95_ms": float(row["latency_p95_ms"]),
                }
            )
    if not rows:
        raise ValueError(f"No benchmark rows found in {path}")
    return rows


def payload_order(rows: list[dict[str, Any]]) -> list[tuple[str, int]]:
    unique = {(row["payload_name"], row["payload_bytes"]) for row in rows}
    return sorted(unique, key=lambda item: item[1])


def aggregate(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, float]]:
    values: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for row in rows:
        key = (row["payload_name"], row["config"])
        for metric in (
            "throughput_rps",
            "data_throughput_mib_s",
            "latency_mean_ms",
            "latency_p95_ms",
        ):
            value = row[metric]
            if not math.isnan(value):
                values[key][metric].append(value)

    result: dict[tuple[str, str], dict[str, float]] = {}
    for key, metrics in values.items():
        result[key] = {}
        for metric, samples in metrics.items():
            result[key][f"{metric}_mean"] = statistics.fmean(samples)
            result[key][f"{metric}_stdev"] = (
                statistics.stdev(samples) if len(samples) >= 2 else 0.0
            )
            result[key][f"{metric}_n"] = float(len(samples))
    return result


def human_size(size_bytes: int) -> str:
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):g} MiB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:g} KiB"
    return f"{size_bytes} B"


def draw_grouped_bars(
    *,
    aggregate_data: dict[tuple[str, str], dict[str, float]],
    payloads: list[tuple[str, int]],
    metric: str,
    ylabel: str,
    title: str,
    output: Path,
) -> None:
    x = np.arange(len(CONFIG_ORDER), dtype=float)
    width = 0.8 / max(1, len(payloads))

    fig, ax = plt.subplots(figsize=(11, 6))

    for payload_index, (payload_name, payload_bytes) in enumerate(payloads):
        offset = (payload_index - (len(payloads) - 1) / 2.0) * width
        means = []
        stdevs = []
        for config in CONFIG_ORDER:
            stats = aggregate_data.get((payload_name, config), {})
            means.append(stats.get(f"{metric}_mean", math.nan))
            stdevs.append(stats.get(f"{metric}_stdev", 0.0))

        ax.bar(
            x + offset,
            means,
            width,
            yerr=stdevs,
            capsize=3,
            label=f"{payload_name} ({human_size(payload_bytes)})",
        )

    ax.set_title(title)
    ax.set_xlabel("Recovery configuration")
    ax.set_ylabel(ylabel)
    ax.set_xticks(x)
    ax.set_xticklabels(CONFIG_ORDER, rotation=18, ha="right")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    plt.close(fig)


def write_summary_csv(
    path: Path,
    aggregate_data: dict[tuple[str, str], dict[str, float]],
    payloads: list[tuple[str, int]],
) -> None:
    fields = [
        "payload_name",
        "payload_bytes",
        "config",
        "throughput_rps_mean",
        "throughput_rps_stdev",
        "data_throughput_mib_s_mean",
        "data_throughput_mib_s_stdev",
        "latency_mean_ms_mean",
        "latency_mean_ms_stdev",
        "latency_p95_ms_mean",
        "latency_p95_ms_stdev",
        "repetitions",
    ]

    rows = []
    payload_sizes = dict(payloads)
    for payload_name, _ in payloads:
        for config in CONFIG_ORDER:
            stats = aggregate_data.get((payload_name, config))
            if stats is None:
                continue
            rows.append(
                {
                    "payload_name": payload_name,
                    "payload_bytes": payload_sizes[payload_name],
                    "config": config,
                    "throughput_rps_mean": stats.get("throughput_rps_mean", math.nan),
                    "throughput_rps_stdev": stats.get("throughput_rps_stdev", math.nan),
                    "data_throughput_mib_s_mean": stats.get(
                        "data_throughput_mib_s_mean", math.nan
                    ),
                    "data_throughput_mib_s_stdev": stats.get(
                        "data_throughput_mib_s_stdev", math.nan
                    ),
                    "latency_mean_ms_mean": stats.get(
                        "latency_mean_ms_mean", math.nan
                    ),
                    "latency_mean_ms_stdev": stats.get(
                        "latency_mean_ms_stdev", math.nan
                    ),
                    "latency_p95_ms_mean": stats.get(
                        "latency_p95_ms_mean", math.nan
                    ),
                    "latency_p95_ms_stdev": stats.get(
                        "latency_p95_ms_stdev", math.nan
                    ),
                    "repetitions": int(stats.get("throughput_rps_n", 0.0)),
                }
            )

    with path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("runs_csv", type=Path, help="Path to benchmark_runs.csv")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("recovery_benchmark_plots"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    rows = read_rows(args.runs_csv)
    payloads = payload_order(rows)
    data = aggregate(rows)

    plots = [
        (
            "throughput_rps",
            "Completed pipelines per second",
            "Average end-to-end throughput",
            "average_throughput_rps.png",
        ),
        (
            "data_throughput_mib_s",
            "Payload throughput (MiB/s)",
            "Average payload throughput",
            "average_data_throughput_mib_s.png",
        ),
        (
            "latency_mean_ms",
            "Mean end-to-end latency (ms)",
            "Average end-to-end latency",
            "average_latency_mean_ms.png",
        ),
        (
            "latency_p95_ms",
            "P95 end-to-end latency (ms)",
            "Average P95 end-to-end latency",
            "average_latency_p95_ms.png",
        ),
    ]

    for metric, ylabel, title, filename in plots:
        output = args.output_dir / filename
        draw_grouped_bars(
            aggregate_data=data,
            payloads=payloads,
            metric=metric,
            ylabel=ylabel,
            title=title,
            output=output,
        )
        print(output.resolve())

    summary_path = args.output_dir / "benchmark_summary.csv"
    write_summary_csv(summary_path, data, payloads)
    print(summary_path.resolve())


if __name__ == "__main__":
    main()
