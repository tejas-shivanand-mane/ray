#!/usr/bin/env python3

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


MODE_ORDER = [
    "owner_worker",
    "owner_node",
    "owner_plus_producer_node",
]

MODE_LABELS = {
    "owner_worker": "Owner worker\nfailure",
    "owner_node": "Owner node\nfailure",
    "owner_plus_producer_node": "Owner + producer\nnode failure",
}


def mean_std(series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    if len(values) == 0:
        return np.nan, np.nan
    if len(values) == 1:
        return values.mean(), 0.0
    return values.mean(), values.std(ddof=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv")
    parser.add_argument(
        "--output-dir",
        default="gossip_benchmarks/failure_type_plots",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(csv_path)

    # Only successful recovery runs should contribute to recovery latency.
    success_df = df[df["success"].astype(str).str.lower() == "true"].copy()

    rows = []

    for mode in MODE_ORDER:
        part = success_df[success_df["failure_mode"] == mode]

        replay_start_mean, replay_start_std = mean_std(
            part["failure_to_replay_start_s"]
        )
        result_mean, result_std = mean_std(
            part["failure_to_result_s"]
        )

        post_start = (
            pd.to_numeric(
                part["failure_to_result_s"],
                errors="coerce",
            )
            - pd.to_numeric(
                part["failure_to_replay_start_s"],
                errors="coerce",
            )
        )

        post_start_mean, post_start_std = mean_std(post_start)

        all_mode = df[df["failure_mode"] == mode]

        successes = (
            all_mode["success"]
            .astype(str)
            .str.lower()
            .eq("true")
            .sum()
        )

        success_rate = (
            100.0 * successes / len(all_mode)
            if len(all_mode)
            else np.nan
        )

        executions = pd.to_numeric(
            all_mode["executions_observed"],
            errors="coerce",
        )

        rows.append(
            {
                "failure_mode": mode,
                "label": MODE_LABELS[mode],
                "trials": len(all_mode),
                "success_rate_pct": success_rate,
                "replay_start_mean_s": replay_start_mean,
                "replay_start_std_s": replay_start_std,
                "post_replay_start_mean_s": post_start_mean,
                "post_replay_start_std_s": post_start_std,
                "total_mean_s": result_mean,
                "total_std_s": result_std,
                "executions_mean": executions.mean(),
            }
        )

    summary = pd.DataFrame(rows)
    summary.to_csv(
        output_dir / "failure_type_summary.csv",
        index=False,
    )

    # ------------------------------------------------------------
    # Main paper plot:
    # stacked failure-to-replay-start + replay-to-result latency
    # ------------------------------------------------------------

    valid = summary["total_mean_s"].notna()
    plot_df = summary[valid].reset_index(drop=True)

    x = np.arange(len(plot_df))

    detection = plot_df["replay_start_mean_s"].to_numpy()
    replay = plot_df["post_replay_start_mean_s"].to_numpy()
    total_std = plot_df["total_std_s"].fillna(0).to_numpy()

    fig, ax = plt.subplots(figsize=(7.2, 4.6))

    ax.bar(
        x,
        detection,
        label="Failure to replay start",
    )

    ax.bar(
        x,
        replay,
        bottom=detection,
        label="Replay start to result",
    )

    # Error bars represent variation in total recovery time.
    total = detection + replay

    ax.errorbar(
        x,
        total,
        yerr=total_std,
        fmt="none",
        capsize=4,
    )

    for i, value in enumerate(total):
        if np.isfinite(value):
            ax.text(
                i,
                value + max(total) * 0.025,
                f"{value:.1f}s",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    ax.set_xticks(x)
    ax.set_xticklabels(plot_df["label"])
    ax.set_ylabel("Recovery time (s)")
    ax.set_title("Recovery latency by failure type")
    ax.legend()
    ax.grid(axis="y", alpha=0.25)

    fig.tight_layout()
    fig.savefig(
        output_dir / "failure_type_recovery_latency.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)

    # ------------------------------------------------------------
    # Success rate
    # ------------------------------------------------------------

    fig, ax = plt.subplots(figsize=(7.2, 4.2))

    x = np.arange(len(summary))

    rates = summary["success_rate_pct"].to_numpy()

    ax.bar(x, rates)

    for i, value in enumerate(rates):
        if np.isfinite(value):
            ax.text(
                i,
                min(value + 2, 103),
                f"{value:.0f}%",
                ha="center",
                va="bottom",
            )

    ax.set_xticks(x)
    ax.set_xticklabels(summary["label"])
    ax.set_ylabel("Successful recoveries (%)")
    ax.set_ylim(0, 110)
    ax.set_title("Recovery success by failure type")
    ax.grid(axis="y", alpha=0.25)

    fig.tight_layout()
    fig.savefig(
        output_dir / "failure_type_success_rate.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)

    # ------------------------------------------------------------
    # Execution count sanity/correctness plot
    # ------------------------------------------------------------

    fig, ax = plt.subplots(figsize=(7.2, 4.2))

    executions = summary["executions_mean"].to_numpy()

    ax.bar(x, executions)

    ax.axhline(
        2,
        linestyle="--",
        linewidth=1.5,
        label="Expected: original + one replay",
    )

    ax.set_xticks(x)
    ax.set_xticklabels(summary["label"])
    ax.set_ylabel("Mean executions per task")
    ax.set_title("Task executions under recovery")
    ax.legend()
    ax.grid(axis="y", alpha=0.25)

    fig.tight_layout()
    fig.savefig(
        output_dir / "failure_type_execution_count.png",
        dpi=300,
        bbox_inches="tight",
    )
    plt.close(fig)

    print(summary.to_string(index=False))
    print(f"\nPlots saved to: {output_dir}")


if __name__ == "__main__":
    main()