"""Complete Matplotlib code for Benchmark 07; edit draw() in this file."""
from plots import pyplot, summary_values, size_label


def plot_borrowers(summaries, paired, out, config):
    # Data preparation only: the runner already validated saved cases.
    x = sorted(config["borrower_counts"])
    sm = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in summaries}
    pr = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in paired}
    data = {}
    for variant in ("disabled", "fixed_k32", "succession_k32"):
        throughput, throughput_ci = summary_values(
            [sm[value, variant] for value in x], "throughput_rps")
        overhead, overhead_ci = summary_values(
            [pr[value, variant] for value in x], "throughput_overhead_pct_vs_disabled")
        data[variant] = {"throughput": throughput, "throughput_ci": throughput_ci,
                         "overhead": overhead, "overhead_ci": overhead_ci}
    context = {
        "tick_labels": [str(value) for value in x],
        "payload_label": size_label(config["payload_bytes"]),
        "padding_label": size_label(config["task_spec_padding_bytes"]),
        "repetitions": config["repetitions"],
    }
    draw(x, data, out, context)


def draw(x, data, out, context):
    """Edit this function freely: all Matplotlib operations are visible here."""
    plt = pyplot()

    # 1. Figure-wide defaults. Put any Matplotlib rcParams here.
    with plt.rc_context({}):
        # 2. Figure and axes. Replace with GridSpec, subfigures, extra axes, etc.
        fig, (ax_throughput, ax_overhead) = plt.subplots(
            1, 2, figsize=(13, 5))

        # 3. Curves and error bars. These are ordinary Matplotlib kwargs.
        # Change the loop or use separate calls for per-panel/per-curve styling.
        styles = {
            "disabled": {"label": "Disabled", "color": "#555555",
                         "marker": "s", "linestyle": "--"},
            "fixed_k32": {"label": "Fixed-R K=32", "color": "#0072B2",
                          "marker": "o", "linestyle": "-"},
            "succession_k32": {"label": "Succession K=32", "color": "#D55E00",
                               "marker": "^", "linestyle": "-"},
        }
        throughput_artists = {}
        overhead_artists = {}
        for variant, style in styles.items():
            throughput_artists[variant] = ax_throughput.errorbar(
                x, data[variant]["throughput"], yerr=data[variant]["throughput_ci"],
                **({"linewidth": 1.8, "markersize": 5, "capsize": 3} | style))
            overhead_artists[variant] = ax_overhead.errorbar(
                x, data[variant]["overhead"], yerr=data[variant]["overhead_ci"],
                **({"linewidth": 1.8, "markersize": 5, "capsize": 3} | style))

        # 4. Throughput axis: scales, ticks, formatters, labels, limits, spines.
        ax_throughput.set_xscale("log", base=2)
        ax_throughput.set_xticks(x, context["tick_labels"], rotation=0)
        ax_throughput.set_xlabel("Application borrowers per object")
        ax_throughput.set_ylabel("Application throughput (pipelines/s)")
        ax_throughput.set_ylim(bottom=0)  # Set limits after ticks.
        ax_throughput.grid(axis="y", alpha=0.22)
        ax_throughput.spines["top"].set_visible(False)
        ax_throughput.spines["right"].set_visible(False)
        ax_throughput.tick_params(labelsize=10)

        # 5. Overhead axis: independent from the throughput axis.
        ax_overhead.set_xscale("log", base=2)
        ax_overhead.set_xticks(x, context["tick_labels"], rotation=0)
        ax_overhead.set_xlabel("Application borrowers per object")
        ax_overhead.set_ylabel("Throughput overhead versus same-count disabled (%)")
        ax_overhead.grid(axis="y", alpha=0.22)
        ax_overhead.spines["top"].set_visible(False)
        ax_overhead.spines["right"].set_visible(False)
        ax_overhead.tick_params(labelsize=10)

        # 6. Legends and annotations. Handles remain available for further edits.
        throughput_legend = ax_throughput.legend(fontsize=9)
        overhead_legend = ax_overhead.legend(fontsize=9)
        title = fig.suptitle(
            f"Borrower-count effect — object {context['payload_label']}, "
            f"TaskSpec padding {context['padding_label']}", fontsize=13)
        footer = fig.text(
            0.5, 0.01,
            f"Target R=2, W=2 · K=32 · {context['repetitions']} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
            "All borrowers must consume each object. B<R may leave Succession below target R; durability is not measured.",
            ha="center", fontsize=9)

        # 7. Layout. Adjust spacing here, or replace tight_layout altogether.
        fig.tight_layout(rect=(0, 0.12, 1, 0.94))

        # 8. Final custom edits. Nothing below reapplies labels, axes or layout.
        # Add inset axes, secondary axes, annotations, custom locators/formatters,
        # artist edits, or any other Matplotlib operation before saving.
        # Example: ax_throughput.annotate("Note", xy=(x[0], data["disabled"]["throughput"][0]))

        # 9. Exports: edit each savefig call independently or add other formats.
        basename = "07_borrower_count_performance"
        png = out / (basename + ".png")
        pdf = out / (basename + ".pdf")
        fig.savefig(png, dpi=200, bbox_inches="tight")
        fig.savefig(pdf, dpi=200, bbox_inches="tight")
        print(f"Plot: {png}", flush=True)
        print(f"Plot: {pdf}", flush=True)
        plt.close(fig)
