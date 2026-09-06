"""Plot application-throughput comparisons from completed benchmark runs.

No Ray calls or benchmark execution occur here. Confidence intervals are
pointwise 95% Student-t intervals supplied by Benchmark 59, not simultaneous
confidence bands or latency-to-durable-protection measurements.
"""
from __future__ import annotations

import math

from plot_settings import ERROR_BARS


def pyplot():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "Plots require matplotlib. Install it in your Ray Python environment "
            "with: python -m pip install matplotlib"
        ) from exc
    return plt


def size_label(size: int) -> str:
    for unit, divisor in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if size >= divisor and size % divisor == 0:
            return f"{size // divisor} {unit}"
    return f"{size} B"


def require_complete_blocks(rows: list[dict], variants: list[str]) -> None:
    if not rows:
        raise ValueError("No saved runs to plot")
    settings = {
        (int(row["holders"]), int(row["borrowers_per_pipeline"]),
         int(row["burst_size"]), int(row["inflight_tasks"]))
        for row in rows
    }
    if len(settings) != 1 or next(iter(settings))[:2] != (2, 2):
        raise ValueError("Mixed workload settings or a workload other than R=2/two borrowers")
    blocks = {}
    for row in rows:
        key = (int(row["payload_bytes"]), int(row["task_spec_padding_bytes"]),
               int(row["repetition"]))
        variant = str(row["variant"])
        block = blocks.setdefault(key, set())
        if variant not in variants or variant in block:
            raise ValueError(f"Unknown/duplicate variant in block {key}: {variant}")
        block.add(variant)
        if not math.isfinite(float(row["throughput_rps"])) or float(row["throughput_rps"]) <= 0:
            raise ValueError(f"Invalid throughput in block {key}: {variant}")
        if int(row["profiling_enabled"]) != 0:
            raise ValueError("Throughput plots require profiling-OFF runs")
    counts = {}
    for key, block in blocks.items():
        if block != set(variants):
            raise ValueError(f"Incomplete block {key}; resume the benchmark before plotting")
        counts[key[:2]] = counts.get(key[:2], 0) + 1
    if any(n < 2 for n in counts.values()):
        raise ValueError("At least two complete paired repetitions are required")


def apply_axis(ax, settings, *, ticks=None, labels=None):
    """Apply presentation after drawing; explicit settings win over data defaults."""
    for dimension in ("x", "y"):
        scale = settings[dimension + "scale"]
        if scale is not None:
            getattr(ax, "set_" + dimension + "scale")(
                scale, **settings[dimension + "scale_kwargs"])
        positions = settings[dimension + "ticks"]
        tick_labels = settings[dimension + "ticklabels"]
        if dimension == "x" and positions is None:
            positions = ticks
            if tick_labels is None:
                tick_labels = labels
        if tick_labels is not None and positions is None:
            raise ValueError(f"{dimension}ticklabels requires explicit {dimension}ticks")
        if positions is not None:
            if tick_labels is not None and len(positions) != len(tick_labels):
                raise ValueError(f"{dimension}ticks and {dimension}ticklabels must have equal lengths")
            getattr(ax, "set_" + dimension + "ticks")(positions, labels=tick_labels)
        getattr(ax, "set_" + dimension + "label")(
            settings[dimension + "label"], fontsize=settings["label_fontsize"])
        ax.tick_params(axis=dimension, labelrotation=settings[dimension + "rotation"])
        # Apply limits last: setting ticks can otherwise expand the view limits.
        limits = settings[dimension + "lim"]
        if limits is not None:
            getattr(ax, "set_" + dimension + "lim")(limits)
    ax.set_title(settings["title"], fontsize=settings["title_fontsize"])
    if settings["tick_fontsize"] is not None:
        ax.tick_params(labelsize=settings["tick_fontsize"])
    if settings["grid"] is None:
        ax.grid(False)
    else:
        ax.grid(True, **settings["grid"])
    for spine in settings["hide_spines"]:
        ax.spines[spine].set_visible(False)
    if settings["legend"] is not None:
        ax.legend(**settings["legend"])


def series(ax, x, rows, metric, style):
    """Draw the supplied means/CIs without recalculating statistics."""
    means = [float(row[metric + "_mean"]) for row in rows]
    errors = [float(row[metric + "_ci95"]) for row in rows]
    if not all(math.isfinite(v) for v in means + errors):
        raise ValueError(f"Missing finite mean/95% CI for {style['label']}: {metric}")
    ax.errorbar(x, means, yerr=errors, **(ERROR_BARS | style))


def finish(plt, fig, out, settings, **context):
    """Format annotations and save the configured formats, then close the figure."""
    if settings["title"]:
        fig.suptitle(settings["title"].format(**context), **settings["title_kwargs"])
    if settings["footer"]:
        fig.text(*settings["footer_position"], settings["footer"].format(**context),
                 **settings["footer_kwargs"])
    fig.tight_layout(**settings["tight_layout"])
    filename = settings["filename"].format(**context)
    for extension in settings["formats"]:
        path = out / (filename + "." + extension)
        fig.savefig(path, dpi=settings["dpi"], **settings["savefig"])
        print(f"Plot: {path}", flush=True)
    plt.close(fig)


# Keep existing callers stable. Imports are lazy to avoid circular imports with
# the per-benchmark layout modules, which use the shared helpers above.
def plot_k(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k):
    from plot_frontier import plot
    return plot(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k)


def plot_sizes(rows, summaries, paired, out, variants):
    from plot_object_sizes import plot
    return plot(rows, summaries, paired, out, variants)
