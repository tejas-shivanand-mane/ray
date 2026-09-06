"""Benchmark 07 layout. Edit BORROWER_COUNTS in plot_settings.py."""
from plot_settings import BORROWER_COUNTS, BORROWER_COUNT_SERIES, RC_PARAMS
from plots import apply_axis, finish, pyplot, series, size_label


def plot_borrowers(summaries, paired, out, config):
    counts = sorted(config["borrower_counts"])
    sm = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in summaries}
    pr = {(int(row["borrowers_per_pipeline"]), row["variant"]): row for row in paired}
    plt = pyplot()
    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 2, figsize=BORROWER_COUNTS["figsize"])
        for variant, style in BORROWER_COUNT_SERIES.items():
            series(axes[0], counts, [sm[count, variant] for count in counts],
                   "throughput_rps", style)
            series(axes[1], counts, [pr[count, variant] for count in counts],
                   "throughput_overhead_pct_vs_disabled", style)
        for ax, panel in zip(axes, ("throughput", "overhead")):
            apply_axis(ax, BORROWER_COUNTS[panel],
                       ticks=counts, labels=[str(count) for count in counts])
        finish(plt, fig, out, BORROWER_COUNTS,
               payload_label=size_label(config["payload_bytes"]),
               padding_label=size_label(config["task_spec_padding_bytes"]),
               repetitions=config["repetitions"])
