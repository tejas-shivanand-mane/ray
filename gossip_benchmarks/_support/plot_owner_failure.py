"""Benchmark 04 layout, separate from event loading and bucket CSV creation."""
from plot_settings import (
    OWNER_BAR, OWNER_FAILURE, OWNER_FAILURE_SERIES, OWNER_KILL_LINE, RC_PARAMS,
)
from plots import apply_axis, finish, pyplot


def draw(rows, runs, out, methods):
    plt = pyplot()
    with plt.rc_context(RC_PARAMS):
        fig, axes = plt.subplots(1, 2, figsize=OWNER_FAILURE["figsize"])
        for position, method in enumerate(methods):
            style = OWNER_FAILURE_SERIES[method]
            selected = [r for r in rows if r["method"] == method]
            xs = sorted({r["seconds_from_failure"] for r in selected})
            means = [
                sum(r["throughput_rps"] for r in selected if r["seconds_from_failure"] == x)
                / sum(r["seconds_from_failure"] == x for r in selected)
                for x in xs
            ]
            axes[0].plot(xs, means, **style)
            selected_runs = [r for r in runs if r["method"] == method]
            if selected_runs:
                fractions = [
                    100 * r["post_failure_successes"] / (r["tasks"] - r["before_tasks"])
                    for r in selected_runs
                ]
                axes[1].bar(position, sum(fractions) / len(fractions),
                            color=style["color"], label=style["label"], **OWNER_BAR)
        axes[0].axvline(0, **OWNER_KILL_LINE)
        apply_axis(axes[0], OWNER_FAILURE["throughput"])
        apply_axis(axes[1], OWNER_FAILURE["recovery"],
                   ticks=list(range(len(methods))),
                   labels=[OWNER_FAILURE_SERIES[method]["label"] for method in methods])
        finish(plt, fig, out, OWNER_FAILURE)
