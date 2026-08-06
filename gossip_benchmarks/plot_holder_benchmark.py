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
    "Enabled-1-holder",
    "Enabled-2-holders",
    "Enabled-3-holders",
    "Enabled-4-holders",
]


def aggregate(path: Path):
    values = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    with path.open(newline="") as f:
        for row in csv.DictReader(f):
            config = row["config"]
            t = float(row["elapsed_seconds"])
            for metric in ("throughput_rps", "latency_p95_ms"):
                value = float(row[metric])
                if not math.isnan(value):
                    values[config][t][metric].append(value)

    result = defaultdict(dict)
    for config, by_time in values.items():
        for t, metrics in by_time.items():
            result[config][t] = {
                metric: statistics.median(samples)
                for metric, samples in metrics.items()
            }
    return result


def draw(data, metric: str, ylabel: str, output: Path) -> None:
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for config in ORDER:
        points = sorted(data.get(config, {}).items())
        x = [t for t, row in points if metric in row]
        y = [row[metric] for _, row in points if metric in row]
        if x:
            ax.plot(x, y, label=config, linewidth=1.8)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    plt.close(fig)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path", type=Path)
    p.add_argument("--output-dir", type=Path, default=Path("holder_benchmark_plots"))
    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    data = aggregate(args.csv_path)
    draw(
        data, "throughput_rps", "Completed pipelines per second",
        args.output_dir / "throughput_vs_time.png",
    )
    draw(
        data, "latency_p95_ms", "P95 end-to-end latency (ms)",
        args.output_dir / "p95_latency_vs_time.png",
    )


if __name__ == "__main__":
    main()
