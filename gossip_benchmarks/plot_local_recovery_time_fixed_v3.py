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
    "Enabled-1-holder",
    "Enabled-2-holders",
    "Enabled-3-holders",
    "Enabled-4-holders",
]


def b(v: str) -> bool:
    return v.strip().lower() == "true"


def f(v: str) -> float:
    try:
        return float(v)
    except Exception:
        return math.nan


def load(path: Path):
    rows = []
    with path.open(newline="") as file:
        for r in csv.DictReader(file):
            rows.append(
                {
                    "config": r["config"],
                    "duration": float(r["task_duration_s"]),
                    "formation": b(r["formation_success"]),
                    "success": b(r["success"]),
                    "replayed": b(r["replayed"]),
                    "replay_start": f(r["failure_to_replay_start_s"]),
                    "result": f(r["failure_to_result_s"]),
                }
            )
    return rows


def aggregate(rows, field):
    grouped = defaultdict(list)
    for r in rows:
        if (
            r["config"] in ORDER
            and r["formation"]
            and r["success"]
            and r["replayed"]
            and not math.isnan(r[field])
        ):
            grouped[(r["config"], r["duration"])].append(r[field])

    out = {}
    for key, values in grouped.items():
        out[key] = (
            statistics.fmean(values),
            statistics.stdev(values) if len(values) > 1 else 0.0,
        )
    return out


def draw(rows, field, ylabel, title, path):
    data = aggregate(rows, field)
    durations = sorted({r["duration"] for r in rows})

    fig, ax = plt.subplots(figsize=(9, 5.5))
    for config in ORDER:
        xs, ys, es = [], [], []
        for d in durations:
            if (config, d) not in data:
                continue
            mean, sd = data[(config, d)]
            xs.append(d)
            ys.append(mean)
            es.append(sd)
        if xs:
            ax.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label=config)

    ax.set_xlabel("Task duration (s)")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=180)
    plt.close(fig)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("csv_path", type=Path)
    p.add_argument("--output-dir", type=Path, default=Path("recovery_time_plots"))
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = load(args.csv_path)

    draw(
        rows,
        "replay_start",
        "Failure-to-replay-start time (s)",
        "Recovery control-plane latency",
        args.output_dir / "recovery_detection_time.png",
    )
    draw(
        rows,
        "result",
        "Failure-to-result time (s)",
        "End-to-end recovery time",
        args.output_dir / "recovery_total_time.png",
    )

    print(args.output_dir.resolve())


if __name__ == "__main__":
    main()
