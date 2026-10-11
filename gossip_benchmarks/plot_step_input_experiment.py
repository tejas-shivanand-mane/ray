"""Plot saved input-recovery ablation evidence; never launches training."""
import argparse
import json
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.expanduser().read_text())
    if report.get("profile") != "step-input-ablation":
        parser.error("Expected a step-input-ablation report")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    groups = [("independent", False), ("aligned", False),
              ("independent", True), ("aligned", True)]
    labels = ["Independent\nhealthy", "Aligned\nhealthy", "Independent\nnode loss", "Aligned\nnode loss"]
    colors = ["#CC6677", "#4477AA", "#CC6677", "#4477AA"]
    fields = [("max_parameter_error", "Final model error vs healthy aligned reference", "Max absolute parameter error"),
              ("input_mismatches", "Input batches inconsistent with model step", "Mismatched batch requests"),
              ("fit_s", "End-to-end fit time, including recovery", "Seconds (lower is better)")]
    for ax, (field, title, ylabel) in zip(axes, fields):
        for index, (mode, fault) in enumerate(groups):
            samples = [s for s in report["samples"] if s["mode"] == mode and s["fault"] == fault]
            measured = [s for s in samples if s.get("status") == "completed" and field in s]
            values = [s[field] for s in measured]
            if values:
                ax.bar(index, statistics.median(values), color=colors[index], alpha=.5, width=.65)
                for j, sample in enumerate(measured):
                    offset = (j - (len(measured) - 1) / 2) * .08
                    valid = sample.get("matches_reference") is True and sample.get("input_mismatches") == 0
                    ax.scatter(index + offset, sample[field], color=colors[index],
                               marker="o" if valid else "x", zorder=3)
            else:
                ax.text(index, .08, "No measurement", rotation=90, ha="center",
                        transform=ax.get_xaxis_transform(), fontsize=8)
            ax.text(index, 1.01, f"{len(measured)}/{len(samples)} measured",
                    ha="center", transform=ax.get_xaxis_transform(), fontsize=8)
        ax.set_xticks(range(4), labels, fontsize=8)
        ax.set_title(title, fontsize=10, pad=24)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=.2)
        ax.set_axisbelow(True)
    axes[0].set_yscale("symlog", linthresh=1e-5)
    axes[0].axhline(1e-5, color="black", linestyle=":", linewidth=1, label="Tolerance 1e-5")
    axes[0].legend(fontsize=8)
    axes[1].yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.suptitle("Contribution ablation: same TorchFT peer recovery, different input recovery")
    p = report["parameters"]
    fig.text(.5, .025,
        f"CPU linear model; two logical worker nodes; rank {p['failed_rank']} killed before update {p['fault_step']}; "
        f"{p['repeats']} repetitions. No durable model checkpoints.\n"
        "Bars: medians; points: individual runs; crosses: incorrect or unvalidated completion. "
        "Missing/failed runs are not plotted as zero.\n"
        "Independent input is a TorchFT ablation, not ordinary Ray full-group checkpoint recovery. "
        "Runtime includes worker startup; no production throughput claim.",
        ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .16, 1, .94))
    output = args.output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    fig.savefig(output.with_suffix(".pdf"))
    plt.close(fig)
    print(output)


if __name__ == "__main__":
    main()
