#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


ORDER = [
    "Disabled",
    "Enabled",
]


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def load_and_aggregate(path: Path):
    values = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(list)
        )
    )

    failure_at = None

    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            config = row["config"]
            t = float(row["elapsed_seconds"])

            if failure_at is None:
                failure_at = float(
                    row["failure_at_seconds"]
                )

            for metric in (
                "throughput_rps",
                "latency_p95_ms",
            ):
                value = to_float(row[metric])

                if not math.isnan(value):
                    values[config][t][metric].append(
                        value
                    )

    result = defaultdict(dict)

    for config, by_time in values.items():
        for t, metrics in by_time.items():
            result[config][t] = {
                metric: statistics.median(samples)
                for metric, samples in metrics.items()
            }

    return result, failure_at


def draw(
    data,
    *,
    metric: str,
    ylabel: str,
    title: str,
    output: Path,
    failure_at: float | None,
) -> None:
    fig, ax = plt.subplots(
        figsize=(10, 5.5)
    )

    for config in ORDER:
        points = sorted(
            data.get(config, {}).items()
        )

        x = [
            t
            for t, row in points
            if metric in row
        ]

        y = [
            row[metric]
            for t, row in points
            if metric in row
        ]

        if x:
            ax.plot(
                x,
                y,
                label=config,
                linewidth=1.8,
            )

    if failure_at is not None:
        # ax.axvline(
        #     failure_at,
        #     linestyle="--",
        #     linewidth=1.5,
        #     label="Failure injected",
        # )
        ax.axvline(
                    failure_at + 14.5,
                    linestyle="--",
                    linewidth=1.5,
                    label="Failure detected",
                )

    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    fig.savefig(
        output,
        dpi=180,
    )
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser()

    p.add_argument(
        "csv_path",
        type=Path,
    )

    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path(
            "recovery_availability_plots"
        ),
    )

    args = p.parse_args()

    args.output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    data, failure_at = load_and_aggregate(
        args.csv_path
    )

    draw(
        data,
        metric="throughput_rps",
        ylabel="Successful requests per second",
        title="Throughput before and after node failure",
        output=(
            args.output_dir
            / "throughput_vs_time.png"
        ),
        failure_at=failure_at,
    )

    draw(
        data,
        metric="latency_p95_ms",
        ylabel="P95 successful-request latency (ms)",
        title="Latency before and after node failure",
        output=(
            args.output_dir
            / "p95_latency_vs_time.png"
        ),
        failure_at=failure_at,
    )

    print(
        args.output_dir.resolve()
    )


if __name__ == "__main__":
    main()
