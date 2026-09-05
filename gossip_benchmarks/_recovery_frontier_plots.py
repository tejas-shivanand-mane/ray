"""Plot application-throughput comparisons from completed benchmark runs.

No Ray calls or benchmark execution occur here. Confidence intervals are
pointwise 95% Student-t intervals supplied by Benchmark 59, not simultaneous
confidence bands or latency-to-durable-protection measurements.
"""
from __future__ import annotations

import math
from pathlib import Path

FIXED = "#0072B2"
SUCCESSION = "#D55E00"
DISABLED = "#555555"


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


def _style(axes):
    for ax in axes:
        ax.grid(axis="y", alpha=0.22)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.tick_params(labelsize=10)


def _series(ax, x, rows, metric, label, color, marker="o", linestyle="-"):
    means = [float(row[metric + "_mean"]) for row in rows]
    errors = [float(row[metric + "_ci95"]) for row in rows]
    if not all(math.isfinite(v) for v in means + errors):
        raise ValueError(f"Missing finite mean/95% CI for {label}: {metric}")
    ax.errorbar(x, means, yerr=errors, label=label, color=color,
                marker=marker, linestyle=linestyle, linewidth=1.8,
                markersize=5, capsize=3)


def _save(plt, fig, output: Path):
    for extension in ("png", "pdf"):
        path = output.with_suffix("." + extension)
        fig.savefig(path, dpi=200, bbox_inches="tight")
        print(f"Plot: {path}", flush=True)
    plt.close(fig)


def plot_k(rows, summaries, paired, out: Path, variants, fixed_for_k, succession_for_k):
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
        fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
        _series(axes[0], x, [sm["disabled"]] * len(ks), "throughput_rps",
                "Disabled (shared reference)", DISABLED, marker="", linestyle="--")
        for mapping, label, color in (
            (fixed_for_k, "Fixed-R", FIXED),
            (succession_for_k, "Succession", SUCCESSION),
        ):
            _series(axes[0], x, [sm[mapping[k]] for k in ks],
                    "throughput_rps", label, color)
            _series(axes[1], x, [pr[mapping[k]] for k in ks],
                    "throughput_overhead_pct_vs_disabled", label, color)
        axes[1].axhline(0, color=DISABLED, linestyle="--", label="Disabled")
        for ax in axes:
            ax.set_xticks(x, [str(k) for k in ks])
            ax.set_xlabel("Frontier group size K")
            ax.legend(fontsize=9)
        axes[0].set_ylabel("Application throughput (tasks/s)")
        axes[0].set_ylim(bottom=0)
        axes[1].set_ylabel("Throughput overhead versus disabled (%)")
        _style(axes)
        fig.suptitle(
            f"Fixed-R and Succession across K — object {size_label(payload)}, "
            f"TaskSpec padding {size_label(padding)}", fontsize=12)
        fig.text(0.5, 0.01,
                 f"R=2, W=2 · {sm['disabled']['repetitions']} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
                 "Overhead is paired within repetition; disabled is one shared reference, not a K sweep.",
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, 0.10, 1, 0.94))
        _save(plt, fig, out / f"fixed_vs_succession_k_padding_{padding}")


def plot_sizes(rows, summaries, paired, out: Path, variants):
    require_complete_blocks(rows, variants)
    plt = pyplot()
    sizes = sorted({int(row["payload_bytes"]) for row in rows})
    if len({int(row["task_spec_padding_bytes"]) for row in rows}) != 1:
        raise ValueError("Object-size sweep must keep TaskSpec padding fixed")
    padding = int(rows[0]["task_spec_padding_bytes"])
    sm = {(int(row["payload_bytes"]), row["variant"]): row for row in summaries}
    pr = {(int(row["payload_bytes"]), row["variant"]): row for row in paired}
    styles = (
        ("disabled", "Disabled", DISABLED, "s", "--"),
        ("fixed_r", "Fixed-R K=1", FIXED, "o", "--"),
        ("fixed_k32", "Fixed-R K=32", FIXED, "o", "-"),
        ("succession_k1", "Succession K=1", SUCCESSION, "^", "--"),
        ("succession_k32", "Succession K=32", SUCCESSION, "^", "-"),
    )
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for variant, label, color, marker, linestyle in styles:
        _series(axes[0], sizes, [sm[size, variant] for size in sizes],
                "throughput_rps", label, color, marker, linestyle)
        _series(axes[1], sizes, [pr[size, variant] for size in sizes],
                "throughput_overhead_pct_vs_disabled", label, color, marker, linestyle)
    for ax in axes:
        ax.set_xscale("log", base=2)
        ax.set_xticks(sizes, [size_label(size) for size in sizes], rotation=25)
        ax.set_xlabel("Returned object payload size")
        ax.legend(fontsize=9)
    axes[0].set_ylabel("Application throughput (tasks/s)")
    axes[0].set_ylim(bottom=0)
    axes[1].set_ylabel("Throughput overhead versus same-size disabled (%)")
    _style(axes)
    fig.suptitle(
        f"Object-size effect — fixed TaskSpec padding {size_label(padding)}", fontsize=13)
    fig.text(0.5, 0.01,
             f"R=2, W=2 · K=1/32 · {sm[sizes[0], 'disabled']['repetitions']} repetitions · profiling OFF · bars: pointwise 95% CIs\n"
             "Each overhead uses the disabled run at the same object size and repetition.",
             ha="center", fontsize=9)
    fig.tight_layout(rect=(0, 0.12, 1, 0.94))
    _save(plt, fig, out / "object_size_comparison")
