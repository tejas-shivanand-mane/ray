"""Plot application-throughput comparisons from completed benchmark runs.

No Ray calls or benchmark execution occur here. Confidence intervals are
pointwise 95% Student-t intervals supplied by Benchmark 59, not simultaneous
confidence bands or latency-to-durable-protection measurements.
"""
from __future__ import annotations

import math


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


def summary_values(rows, metric):
    """Extract already-computed means/CIs; no styling or statistical changes."""
    means = [float(row[metric + "_mean"]) for row in rows]
    errors = [float(row[metric + "_ci95"]) for row in rows]
    if not all(math.isfinite(value) for value in means + errors):
        raise ValueError(f"Missing finite mean/95% CI for {metric}")
    return means, errors


# Entry-point adapters only. Every drawing operation lives in the named file.
def plot_k(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k):
    from plot_frontier import plot
    return plot(rows, summaries, paired, out, variants, fixed_for_k, succession_for_k)


def plot_sizes(rows, summaries, paired, out, variants):
    from plot_object_sizes import plot
    return plot(rows, summaries, paired, out, variants)
