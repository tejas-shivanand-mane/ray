"""Benchmark 02 layout. For labels, ticks and styles edit plot_settings.py."""
from plot_settings import OBJECT_SIZES, OBJECT_SIZE_SERIES, RC_PARAMS
from plots import apply_axis, finish, pyplot, require_complete_blocks, series, size_label


def plot(rows, summaries, paired, out, variants):
    require_complete_blocks(rows, variants)
    plt = pyplot()
    sizes = sorted({int(row["payload_bytes"]) for row in rows})
    if len({int(row["task_spec_padding_bytes"]) for row in rows}) != 1:
        raise ValueError("Object-size sweep must keep TaskSpec padding fixed")
    padding = int(rows[0]["task_spec_padding_bytes"])
    sm = {(int(row["payload_bytes"]), row["variant"]): row for row in summaries}
    pr = {(int(row["payload_bytes"]), row["variant"]): row for row in paired}
    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 2, figsize=OBJECT_SIZES["figsize"])
        for variant, style in OBJECT_SIZE_SERIES.items():
            series(axes[0], sizes, [sm[size, variant] for size in sizes],
                   "throughput_rps", style)
            series(axes[1], sizes, [pr[size, variant] for size in sizes],
                   "throughput_overhead_pct_vs_disabled", style)
        for ax, panel in zip(axes, ("throughput", "overhead")):
            apply_axis(ax, OBJECT_SIZES[panel],
                       ticks=sizes, labels=[size_label(size) for size in sizes])
        finish(plt, fig, out, OBJECT_SIZES, padding_label=size_label(padding),
               repetitions=sm[sizes[0], "disabled"]["repetitions"])
