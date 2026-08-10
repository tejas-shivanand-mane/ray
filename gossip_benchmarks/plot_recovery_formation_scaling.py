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


def load_rows(path: Path):
    rows = []

    with path.open(newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            success = str(row.get("success", "")).strip().lower() == "true"
            if not success:
                continue

            rows.append(
                {
                    "trial": int(row["trial"]),
                    "tasks": int(row["tasks"]),
                    "holders": int(row["holders"]),
                    "formation_time_s": to_float(row["formation_time_s"]),
                    "admissions_per_s": to_float(row["admissions_per_s"]),
                    "formation_ms_per_admission": to_float(
                        row["formation_ms_per_admission"]
                    ),
                }
            )

    return rows


def mean_std(values):
    values = [v for v in values if not math.isnan(v)]

    if not values:
        return math.nan, 0.0

    mean = statistics.fmean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0

    return mean, std


def aggregate(rows):
    grouped = defaultdict(list)

    for row in rows:
        key = (row["holders"], row["tasks"])
        grouped[key].append(row["formation_time_s"] * 1000.0)

    aggregated = {}

    for key, values in grouped.items():
        aggregated[key] = mean_std(values)

    return aggregated


def plot_formation_scaling(rows, output: Path):
    aggregated = aggregate(rows)

    holder_counts = sorted({row["holders"] for row in rows})
    task_counts = sorted({row["tasks"] for row in rows})

    fig, ax = plt.subplots(figsize=(8.2, 5.4))

    markers = ["o", "s", "^", "D", "v"]

    for i, holders in enumerate(holder_counts):
        xs = []
        ys = []
        yerr = []

        for tasks in task_counts:
            key = (holders, tasks)

            if key not in aggregated:
                continue

            mean, std = aggregated[key]

            xs.append(tasks)
            ys.append(mean)
            yerr.append(std)

        ax.errorbar(
            xs,
            ys,
            yerr=yerr,
            marker=markers[i % len(markers)],
            capsize=3,
            linewidth=1.8,
            label=f"{holders} holder" if holders == 1 else f"{holders} holders",
        )

    ax.set_xlabel("Number of protected task outputs")
    ax.set_ylabel("Formation time (ms)")
    ax.set_title("Recovery-succession formation scaling")
    ax.set_xticks(task_counts)
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=250, bbox_inches="tight")
    plt.close(fig)


def print_summary(rows):
    aggregated = aggregate(rows)

    holder_counts = sorted({row["holders"] for row in rows})
    task_counts = sorted({row["tasks"] for row in rows})

    print("tasks,holders,mean_formation_ms,std_formation_ms")

    for tasks in task_counts:
        for holders in holder_counts:
            key = (holders, tasks)

            if key not in aggregated:
                continue

            mean, std = aggregated[key]

            print(
                f"{tasks},"
                f"{holders},"
                f"{mean:.3f},"
                f"{std:.3f}"
            )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Plot recovery-succession formation time versus "
            "number of protected task outputs."
        )
    )

    parser.add_argument(
        "csv_path",
        type=Path,
        help="Formation-scaling benchmark CSV.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("recovery_formation_scaling.png"),
        help="Output PNG path.",
    )

    args = parser.parse_args()

    rows = load_rows(args.csv_path)

    if not rows:
        raise SystemExit(
            "No successful benchmark rows were found in the CSV."
        )

    plot_formation_scaling(rows, args.output)
    print_summary(rows)

    print(f"\nSaved plot to: {args.output}")


if __name__ == "__main__":
    main()
