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
    if report.get("profile") == "ordinary-ray-peer":
        plot_ordinary(report, args.output)
        return
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


def plot_ordinary(report, output):
    from _support.ordinary_ray_comparison import recovery_errors

    for sample in report["samples"]:
        sample["recovery_contract_valid"] = not recovery_errors(sample, report["parameters"])
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    panels = [(False, "fit_s", "Healthy completion time", "Seconds"),
              (True, "fit_s", "Completion time with worker-node loss", "Seconds"),
              (True, "recovery_to_new_progress_s", "Node loss to new model progress", "Seconds"),
              (True, "repeated_updates_per_rank", "Committed updates repeated after rollback", "Mean repeated updates per worker")]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8))
    for ax, (fault, field, title, ylabel) in zip(axes.flat, panels):
        for index, (mode, color) in enumerate((("ordinary", "#CC6677"), ("aligned", "#4477AA"))):
            samples = [s for s in report["samples"] if s["mode"] == mode and s["fault"] == fault]
            validated = [s for s in samples if s.get("matches_reference") is True and s["recovery_contract_valid"] and field in s]
            values = [(sum(s[field].values()) / 2 if field == "repeated_updates_per_rank" else s[field])
                      for s in validated]
            if values:
                ax.bar(index, statistics.median(values), color=color, alpha=.6, width=.55)
                for j, value in enumerate(values):
                    ax.scatter(index + .07 * (j - (len(values) - 1) / 2), value, color=color, zorder=3)
            else:
                ax.text(index, .1, "No validated result", rotation=90, ha="center",
                        transform=ax.get_xaxis_transform())
            ax.text(index, 1.02, f"{len(validated)}/{len(samples)} validated", ha="center",
                    transform=ax.get_xaxis_transform(), fontsize=9)
        ax.set_xticks([0, 1], ["Ordinary Ray\nDDP + checkpoint retry", "Our integrated path\nTorchFT + aligned input"])
        ax.set_title(title, pad=25)
        ax.set_ylabel(ylabel)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=.2)
        ax.set_axisbelow(True)
    pairs = [p for p in report.get("pairs", []) if p.get("valid") and
             all(s.get("matches_reference") and s["recovery_contract_valid"]
                 for s in report["samples"] if s["repeat"] == p["repeat"])]
    if pairs:
        improvement = statistics.median(p["fault_runtime_reduction_pct"] for p in pairs)
        overhead = statistics.median(p["healthy_fit_overhead_pct"] for p in pairs)
        conclusion = (f"Paired median fault-runtime reduction: {improvement:+.1f}% "
                      f"(negative means slower); healthy fit overhead: {overhead:+.1f}%.")
    else:
        conclusion = "No complete correctness-validated pair; no speedup claim."
    p = report["parameters"]
    delay = p.get("startup_delay_s", 0)
    fig.suptitle(f"Ordinary Ray comparison | simulated startup: {delay:g} s per worker invocation")
    fig.text(.5, .025,
        conclusion + "\n" +
        f"Synthetic CPU MLP width {p['model_width']}; {p['steps']} steps; checkpoint every {p['checkpoint_every']}; "
        f"rank {p['failed_rank']} lost before update {p['fault_step']}; {p['repeats']} repeats.\n" +
        "Same checkpoints and simulated delay in both arms. Ordinary Ray directly resumes input at its checkpoint step.\n" +
        "Bars: medians; points: runs. Only correctness and recovery validated runs contribute timing. Head/storage survive; one physical host.\n" +
        "Combined-system comparison includes TorchFT benefits; the separate input ablation isolates our input contribution.",
        ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .17, 1, .95))
    output = Path(output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=180)
    fig.savefig(output.with_suffix(".pdf"))
    plt.close(fig)
    print(output)


if __name__ == "__main__":
    main()
