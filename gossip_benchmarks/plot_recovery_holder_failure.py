#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def mean_sd(values: list[float]) -> tuple[float, float]:
    values = [v for v in values if not math.isnan(v)]
    if not values:
        return math.nan, 0.0
    return (
        statistics.fmean(values),
        statistics.stdev(values) if len(values) > 1 else 0.0,
    )


def load_rows(path: Path) -> list[dict]:
    rows = []
    with path.open(newline="") as f:
        for r in csv.DictReader(f):
            rows.append(
                {
                    "trial": int(r["trial"]),
                    "holders": int(r["holders"]),
                    "predead": int(r["predead_holders"]),
                    "formation_success": to_bool(r["formation_success"]),
                    "formation_time": to_float(r["formation_time_s"]),
                    "success": to_bool(r["success"]),
                    "accepted_rank": int(r["accepted_rank"]),
                    "replayed": to_bool(r["replayed"]),
                    "executions": int(r["executions_observed"]),
                    "failure_to_result": to_float(r["failure_to_result_s"]),
                    "error_type": r.get("error_type", ""),
                }
            )
    if not rows:
        raise ValueError(f"No rows found in {path}")
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path", type=Path)
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("holder_failure_plots"),
    )
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = load_rows(args.csv_path)

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["predead"]].append(row)

    ks = sorted(grouped)

    # ------------------------------------------------------------------
    # 1. Recovery success rate.
    # ------------------------------------------------------------------
    success_rates = [
        100.0 * sum(r["success"] for r in grouped[k]) / len(grouped[k])
        for k in ks
    ]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(ks, success_rates)
    ax.set_xlabel("Number of pre-dead earliest holders")
    ax.set_ylabel("Recovery success rate (%)")
    ax.set_ylim(0, 105)
    ax.set_xticks(ks)
    ax.set_title("Recovery success as succession holders fail")
    fig.tight_layout()
    fig.savefig(
        args.output_dir / "holder_failure_success_rate.png",
        dpi=180,
    )
    plt.close(fig)

    # ------------------------------------------------------------------
    # 2. Actual accepted rank vs expected fallback rank.
    #    Failed trials (accepted_rank=-1) are excluded from the mean.
    # ------------------------------------------------------------------
    actual_means = []
    actual_sds = []
    expected = []

    for k in ks:
        successful = [
            r for r in grouped[k]
            if r["success"] and r["accepted_rank"] >= 1
        ]
        mean, sd = mean_sd(
            [float(r["accepted_rank"]) for r in successful]
        )
        actual_means.append(mean)
        actual_sds.append(sd)

        holders = grouped[k][0]["holders"]
        expected.append(k + 1 if k < holders else math.nan)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(
        ks,
        actual_means,
        yerr=actual_sds,
        marker="o",
        label="Observed accepted rank",
    )
    ax.plot(
        ks,
        expected,
        linestyle="--",
        marker="x",
        label="Expected next surviving rank",
    )
    ax.set_xlabel("Number of pre-dead earliest holders")
    ax.set_ylabel("Recovery holder rank")
    ax.set_xticks(ks)
    ax.set_title("Succession fallback chooses the next surviving holder")
    ax.legend()
    fig.tight_layout()
    fig.savefig(
        args.output_dir / "accepted_rank_vs_predead_holders.png",
        dpi=180,
    )
    plt.close(fig)

    # ------------------------------------------------------------------
    # 3. Successful recovery latency only.
    #
    # Do NOT include failed OwnerDied trials here. Their failure_to_result
    # is time-to-terminal-error, not successful recovery latency.
    # ------------------------------------------------------------------
    latency_means = []
    latency_sds = []

    for k in ks:
        values = [
            r["failure_to_result"]
            for r in grouped[k]
            if r["success"] and not math.isnan(r["failure_to_result"])
        ]
        mean, sd = mean_sd(values)
        latency_means.append(mean)
        latency_sds.append(sd)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(
        ks,
        latency_means,
        yerr=latency_sds,
        marker="o",
    )
    ax.set_xlabel("Number of pre-dead earliest holders")
    ax.set_ylabel("Failure-to-result latency (s)")
    ax.set_xticks(ks)
    ax.set_title("Successful recovery latency under holder failures")
    fig.tight_layout()
    fig.savefig(
        args.output_dir / "successful_recovery_latency.png",
        dpi=180,
    )
    plt.close(fig)

    # ------------------------------------------------------------------
    # 4. Observed execution count.
    #    Normal successful replay should usually produce two STARTs:
    #    original execution + one replay.
    # ------------------------------------------------------------------
    exec_means = []
    exec_sds = []

    for k in ks:
        mean, sd = mean_sd(
            [float(r["executions"]) for r in grouped[k]]
        )
        exec_means.append(mean)
        exec_sds.append(sd)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(
        ks,
        exec_means,
        yerr=exec_sds,
        marker="o",
    )
    ax.axhline(
        2,
        linestyle="--",
        linewidth=1.2,
        label="Original + one replay",
    )
    ax.set_xlabel("Number of pre-dead earliest holders")
    ax.set_ylabel("Observed task executions")
    ax.set_xticks(ks)
    ax.set_title("Task execution count during recovery")
    ax.legend()
    fig.tight_layout()
    fig.savefig(
        args.output_dir / "execution_count_vs_predead_holders.png",
        dpi=180,
    )
    plt.close(fig)

    # ------------------------------------------------------------------
    # 5. Holder formation time.
    #    This is diagnostic only for this benchmark because holder failure
    #    occurs after formation.
    # ------------------------------------------------------------------
    formation_means = []
    formation_sds = []

    for k in ks:
        mean, sd = mean_sd(
            [
                r["formation_time"]
                for r in grouped[k]
                if r["formation_success"]
            ]
        )
        formation_means.append(mean)
        formation_sds.append(sd)

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(
        ks,
        formation_means,
        yerr=formation_sds,
        marker="o",
    )
    ax.set_xlabel("Number of holders killed after formation")
    ax.set_ylabel("Holder formation time (s)")
    ax.set_xticks(ks)
    ax.set_title("Formation-time sanity check")
    fig.tight_layout()
    fig.savefig(
        args.output_dir / "formation_time_sanity_check.png",
        dpi=180,
    )
    plt.close(fig)

    print(f"Wrote plots to {args.output_dir.resolve()}")


if __name__ == "__main__":
    main()
