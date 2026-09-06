"""Complete Matplotlib code for Benchmark 02; edit draw() in this file."""
from plots import pyplot, require_complete_blocks, summary_values, size_label


def plot(rows, summaries, paired, out, variants):
    # Data preparation only; statistics are supplied by the benchmark.
    require_complete_blocks(rows, variants)
    x = sorted({int(row["payload_bytes"]) for row in rows})
    if len({int(row["task_spec_padding_bytes"]) for row in rows}) != 1:
        raise ValueError("Object-size sweep must keep TaskSpec padding fixed")
    padding = int(rows[0]["task_spec_padding_bytes"])
    sm = {(int(row["payload_bytes"]), row["variant"]): row for row in summaries}
    pr = {(int(row["payload_bytes"]), row["variant"]): row for row in paired}
    data = {}
    for variant in ("disabled", "fixed_r", "fixed_k32", "succession_k1", "succession_k32"):
        throughput, throughput_ci = summary_values(
            [sm[value, variant] for value in x], "throughput_rps")
        overhead, overhead_ci = summary_values(
            [pr[value, variant] for value in x], "throughput_overhead_pct_vs_disabled")
        data[variant] = {"throughput": throughput, "throughput_ci": throughput_ci,
                         "overhead": overhead, "overhead_ci": overhead_ci}
    context = {"tick_labels": [size_label(value) for value in x],
               "padding_label": size_label(padding),
               "repetitions": sm[x[0], "disabled"]["repetitions"]}
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
            "fixed_r": {"label": "Fixed-R K=1", "color": "#0072B2",
                        "marker": "o", "linestyle": "--"},
            "fixed_k32": {"label": "Fixed-R K=32", "color": "#0072B2",
                          "marker": "o", "linestyle": "-"},
            "succession_k1": {"label": "Succession K=1", "color": "#D55E00",
                              "marker": "^", "linestyle": "--"},
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
        ax_throughput.set_xticks(x, context["tick_labels"], rotation=25)
        ax_throughput.set_xlabel("Returned object payload size")
        ax_throughput.set_ylabel("Application throughput (tasks/s)")
        ax_throughput.set_ylim(bottom=0)  # Set limits after ticks.
        ax_throughput.grid(axis="y", alpha=0.22)
        ax_throughput.spines["top"].set_visible(False)
        ax_throughput.spines["right"].set_visible(False)
        ax_throughput.tick_params(labelsize=10)

        # 5. Overhead axis: independent from the throughput axis.
        ax_overhead.set_xscale("log", base=2)
        ax_overhead.set_xticks(x, context["tick_labels"], rotation=25)
        ax_overhead.set_xlabel("Returned object payload size")
        ax_overhead.set_ylabel("Throughput overhead versus same-size disabled (%)")
        ax_overhead.grid(axis="y", alpha=0.22)
        ax_overhead.spines["top"].set_visible(False)
        ax_overhead.spines["right"].set_visible(False)
        ax_overhead.tick_params(labelsize=10)

        # 6. Legends and annotations. Handles remain available for further edits.
        throughput_legend = ax_throughput.legend(fontsize=9)
        overhead_legend = ax_overhead.legend(fontsize=9)
        title = fig.suptitle(
            f"Object-size effect — fixed TaskSpec padding {context['padding_label']}", fontsize=13)
        footer = fig.text(
            0.5, 0.01,
            f"R=2, W=2 · K=1/32 · {context['repetitions']} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
            "Each overhead uses the disabled run at the same object size and repetition.",
            ha="center", fontsize=9)

        # 7. Layout. Adjust spacing here, or replace tight_layout altogether.
        fig.tight_layout(rect=(0, 0.12, 1, 0.94))

        # 8. Final custom edits. Nothing below reapplies labels, axes or layout.
        # Add inset axes, secondary axes, annotations, custom locators/formatters,
        # artist edits, or any other Matplotlib operation before saving.
        # Example: ax_throughput.annotate("Note", xy=(x[0], data["disabled"]["throughput"][0]))

        # 9. Exports: edit each savefig call independently or add other formats.
        basename = "02_object_size_performance"
        png = out / (basename + ".png")
        pdf = out / (basename + ".pdf")
        fig.savefig(png, dpi=200, bbox_inches="tight")
        fig.savefig(pdf, dpi=200, bbox_inches="tight")
        print(f"Plot: {png}", flush=True)
        print(f"Plot: {pdf}", flush=True)
        plt.close(fig)
