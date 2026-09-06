"""Benchmark 01 layout. For labels, ticks and styles edit plot_settings.py."""
from plot_settings import FRONTIER, FRONTIER_SERIES, FRONTIER_ZERO_LINE, RC_PARAMS
from plots import apply_axis, finish, pyplot, require_complete_blocks, series, size_label


def plot(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k):
    require_complete_blocks(rows, variants)
    payloads = {int(row["payload_bytes"]) for row in rows}
    if len(payloads) != 1:
        raise ValueError("B59 plot contains mixed object sizes; use separate output directories")
    payload = next(iter(payloads))
    plt = pyplot()
    ks = sorted(fixed_for_k)
    x = list(range(len(ks)))
    for padding in sorted({int(row["task_spec_padding_bytes"]) for row in rows}):
        sm = {row["variant"]: row for row in summaries
              if int(row["task_spec_padding_bytes"]) == padding}
        pr = {row["variant"]: row for row in paired
              if int(row["task_spec_padding_bytes"]) == padding}
        with plt.rc_context(RC_PARAMS):
            fig, axes = plt.subplots(1, 2, figsize=FRONTIER["figsize"])
            series(axes[0], x, [sm["disabled"]] * len(ks), "throughput_rps",
                   FRONTIER_SERIES["disabled"])
            for mapping, method in ((fixed_for_k, "fixed_r"),
                                    (succession_for_k, "succession")):
                series(axes[0], x, [sm[mapping[k]] for k in ks],
                       "throughput_rps", FRONTIER_SERIES[method])
                series(axes[1], x, [pr[mapping[k]] for k in ks],
                       "throughput_overhead_pct_vs_disabled", FRONTIER_SERIES[method])
            axes[1].axhline(0, **FRONTIER_ZERO_LINE)
            for ax, panel in zip(axes, ("throughput", "overhead")):
                apply_axis(ax, FRONTIER[panel], ticks=x, labels=[str(k) for k in ks])
            finish(plt, fig, out, FRONTIER, padding=padding,
                   payload_label=size_label(payload), padding_label=size_label(padding),
                   repetitions=sm["disabled"]["repetitions"])
