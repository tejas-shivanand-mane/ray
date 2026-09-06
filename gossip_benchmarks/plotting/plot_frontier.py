"""Complete Matplotlib code for Benchmark 01; edit draw() in this file."""
from plots import pyplot, require_complete_blocks, summary_values, size_label


def plot(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k):
    # Data preparation only; the same disabled reference is repeated across K.
    require_complete_blocks(rows, variants)
    payloads = {int(row["payload_bytes"]) for row in rows}
    if len(payloads) != 1:
        raise ValueError("B59 plot contains mixed object sizes; use separate output directories")
    payload = next(iter(payloads))
    ks = sorted(fixed_for_k)
    x = list(range(len(ks)))
    for padding in sorted({int(row["task_spec_padding_bytes"]) for row in rows}):
        sm = {row["variant"]: row for row in summaries
              if int(row["task_spec_padding_bytes"]) == padding}
        pr = {row["variant"]: row for row in paired
              if int(row["task_spec_padding_bytes"]) == padding}
        means, errors = summary_values([sm["disabled"]] * len(ks), "throughput_rps")
        data = {"disabled": {"throughput": means, "throughput_ci": errors}}
        for mapping, method in ((fixed_for_k, "fixed_r"), (succession_for_k, "succession")):
            means, errors = summary_values([sm[mapping[k]] for k in ks], "throughput_rps")
            overhead, overhead_ci = summary_values(
                [pr[mapping[k]] for k in ks], "throughput_overhead_pct_vs_disabled")
            data[method] = {"throughput": means, "throughput_ci": errors,
                            "overhead": overhead, "overhead_ci": overhead_ci}
        context = {"tick_labels": [str(k) for k in ks], "padding": padding,
                   "payload_label": size_label(payload), "padding_label": size_label(padding),
                   "repetitions": sm["disabled"]["repetitions"]}
        draw(x, data, out, context)


def draw(x, data, out, context):
    """Edit this function freely: all Matplotlib operations are visible here."""
    plt = pyplot()

    # 1. Figure-wide defaults. Put any Matplotlib rcParams here.
    with plt.rc_context({}):
        # 2. Figure and axes. Replace with GridSpec, subfigures, extra axes, etc.
        fig, (ax_throughput, ax_overhead) = plt.subplots(
            1, 2, figsize=(12, 4.8))

        # 3. Curves and error bars. These are ordinary Matplotlib kwargs.
        # Change the loop or use separate calls for per-panel/per-curve styling.
        styles = {
            "disabled": {"label": "Disabled (shared reference)", "color": "#555555",
                         "marker": "", "linestyle": "--"},
            "fixed_r": {"label": "Fixed-R", "color": "#0072B2",
                        "marker": "o", "linestyle": "-"},
            "succession": {"label": "Succession", "color": "#D55E00",
                           "marker": "o", "linestyle": "-"},
        }
        throughput_artists = {}
        overhead_artists = {}
        for variant, style in styles.items():
            throughput_artists[variant] = ax_throughput.errorbar(
                x, data[variant]["throughput"], yerr=data[variant]["throughput_ci"],
                **({"linewidth": 1.8, "markersize": 5, "capsize": 3} | style))
            if variant != "disabled":
                overhead_artists[variant] = ax_overhead.errorbar(
                    x, data[variant]["overhead"], yerr=data[variant]["overhead_ci"],
                    **({"linewidth": 1.8, "markersize": 5, "capsize": 3} | style))
        zero_line = ax_overhead.axhline(
            0, color="#555555", linestyle="--", label="Disabled")

        # 4. Throughput axis: scales, ticks, formatters, labels, limits, spines.
        ax_throughput.set_xticks(x, context["tick_labels"], rotation=0)
        ax_throughput.set_xlabel("Frontier group size K")
        ax_throughput.set_ylabel("Application throughput (tasks/s)")
        ax_throughput.set_ylim(bottom=0)  # Set limits after ticks.
        ax_throughput.grid(axis="y", alpha=0.22)
        ax_throughput.spines["top"].set_visible(False)
        ax_throughput.spines["right"].set_visible(False)
        ax_throughput.tick_params(labelsize=10)

        # 5. Overhead axis: independent from the throughput axis.
        ax_overhead.set_xticks(x, context["tick_labels"], rotation=0)
        ax_overhead.set_xlabel("Frontier group size K")
        ax_overhead.set_ylabel("Throughput overhead versus disabled (%)")
        ax_overhead.grid(axis="y", alpha=0.22)
        ax_overhead.spines["top"].set_visible(False)
        ax_overhead.spines["right"].set_visible(False)
        ax_overhead.tick_params(labelsize=10)

        # 6. Legends and annotations. Handles remain available for further edits.
        throughput_legend = ax_throughput.legend(fontsize=9)
        overhead_legend = ax_overhead.legend(fontsize=9)
        title = fig.suptitle(
            f"Fixed-R and Succession across K — object {context['payload_label']}, "
            f"TaskSpec padding {context['padding_label']}", fontsize=12)
        footer = fig.text(
            0.5, 0.01,
            f"R=2, W=2 · {context['repetitions']} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
            "Overhead is paired within repetition; disabled is one shared reference, not a K sweep.",
            ha="center", fontsize=9)

        # 7. Layout. Adjust spacing here, or replace tight_layout altogether.
        fig.tight_layout(rect=(0, 0.10, 1, 0.94))

        # 8. Final custom edits. Nothing below reapplies labels, axes or layout.
        # Add inset axes, secondary axes, annotations, custom locators/formatters,
        # artist edits, or any other Matplotlib operation before saving.
        # Example: ax_throughput.annotate("Note", xy=(x[0], data["disabled"]["throughput"][0]))

        # 9. Exports: edit each savefig call independently or add other formats.
        basename = f"fixed_vs_succession_k_padding_{context['padding']}"
        png = out / (basename + ".png")
        pdf = out / (basename + ".pdf")
        fig.savefig(png, dpi=200, bbox_inches="tight")
        fig.savefig(pdf, dpi=200, bbox_inches="tight")
        print(f"Plot: {png}", flush=True)
        print(f"Plot: {pdf}", flush=True)
        plt.close(fig)
