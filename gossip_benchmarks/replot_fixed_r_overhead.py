"""Editable plots from saved overhead JSON reports; no Ray imports or workloads.

Run from the repository:
    python gossip_benchmarks/replot_fixed_r_overhead.py

By default, overwrite overhead.png and overhead-long-all.png in ~/ray-coverage
(or RAY_RECOVERY_OUTPUT_DIR). To use other reports or preserve the old PNGs:
    python gossip_benchmarks/replot_fixed_r_overhead.py short.json long.json \
        --output-dir ~/ray-coverage/custom-plots

Requires matplotlib. Edit the settings below and plot_report() to customize.
"""

import argparse
import json
import math
import os
from pathlib import Path


# ======================= EDIT PLOT SETTINGS HERE =======================
REPORT_DIRECTORY = Path(os.environ.get("RAY_RECOVERY_OUTPUT_DIR", "~/ray-coverage"))
REPORT_FILENAMES = ("overhead.json", "overhead-long-all.json")
OUTPUT_FORMATS = ("png",)  # For both formats, use ("png", "pdf").
DPI = 200
FIGURE_SIZE = (12, 6.5)
FONT_FAMILY = "DejaVu Sans"
FONT_SIZE = 11
TITLE_SIZE = 16
BAR_HEIGHT = 0.60
VALUE_FORMAT = "+.2f"  # Signed percentage with two decimal places.
SHOW_VALUES = True
SHOW_PAIR_COUNTS = True
SHOW_ERROR_BARS = True  # +/- one sample SD of paired overhead, when available.
SHOW_FOOTER = True
SHOW_GRID = True
SHARE_X_LIMITS = False  # True gives all supplied reports the same axis range.
X_LIMITS = None  # Explicit limits override auto/shared limits, e.g. (-10, 1600).
X_LABEL = "Runtime overhead (%)"
TITLES = {
    "overhead": "Fixed-R overhead — short workloads",
    "overhead-long-all": "Fixed-R overhead — long workloads",
}
COLORS = {
    "backpressure": "#0072B2",
    "worker-scaling-actors": "#E69F00",
    "xgboost-single": "#009E73",
    "xgboost-multi": "#CC79A7",
}

# Each entry is (case in JSON, phase in JSON, displayed label).
# Reorder, remove, or rename entries here. Keep only the four "total" entries
# if you want one bar per benchmark. Missing results are labeled, never zeroed.
PLOT_ROWS = [
    ("backpressure", "total", "Backpressure"),
    ("worker-scaling-actors", "total", "Actor worker scaling"),
    ("xgboost-single", "training", "XGBoost: 1 worker / training"),
    ("xgboost-single", "prediction", "XGBoost: 1 worker / prediction"),
    ("xgboost-single", "total", "XGBoost: 1 worker / total"),
    ("xgboost-multi", "training", "XGBoost: 2 workers / training"),
    ("xgboost-multi", "prediction", "XGBoost: 2 workers / prediction"),
    ("xgboost-multi", "total", "XGBoost: 2 workers / total"),
]
# ======================================================================


def load_report(path):
    report = json.loads(path.read_text())
    if report.get("profile") != "fixed-r-no-failure-overhead":
        raise ValueError(f"{path}: expected a fixed-r-no-failure-overhead JSON report")
    rows = {}
    for row in report.get("summary", []):
        key = (row["case"], row["phase"])
        if key in rows:
            raise ValueError(f"{path}: duplicate summary row {key}")
        value = row["runtime_overhead_pct_mean"]
        deviation = row.get("runtime_overhead_pct_stdev")
        if not math.isfinite(value) or (deviation is not None and (
            not math.isfinite(deviation) or deviation < 0
        )) or row["pairs"] < 1:
            raise ValueError(f"{path}: invalid overhead, deviation, or pair count for {key}")
        rows[key] = row
    return report, rows


def axis_limits(all_rows):
    if X_LIMITS is not None:
        return X_LIMITS
    low = high = 0.0
    for rows in all_rows:
        for case, phase, _ in PLOT_ROWS:
            row = rows.get((case, phase))
            if row is None:
                continue
            value = row["runtime_overhead_pct_mean"]
            deviation = (row.get("runtime_overhead_pct_stdev") or 0) if SHOW_ERROR_BARS else 0
            low = min(low, value - deviation)
            high = max(high, value + deviation)
    span = max(high - low, 1)
    return low - 0.15 * span, high + 0.25 * span


def plot_report(path, report, rows, output_directory, limits):
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    plt.rcParams.update({"font.family": FONT_FAMILY, "font.size": FONT_SIZE})
    fig, ax = plt.subplots(figsize=FIGURE_SIZE)
    fig.subplots_adjust(left=0.34, right=0.97, top=0.88, bottom=0.24 if SHOW_FOOTER else 0.13)
    span = limits[1] - limits[0]
    labels = []
    try:
        for position, (case, phase, label) in enumerate(PLOT_ROWS):
            row = rows.get((case, phase))
            if row is None:
                labels.append(label)
                ax.text(0.02, position, "No complete OFF/ON pair",
                        transform=ax.get_yaxis_transform(), va="center", color="#777777")
                continue
            labels.append(f"{label}  (n={row['pairs']})" if SHOW_PAIR_COUNTS else label)
            value = row["runtime_overhead_pct_mean"]
            deviation = row.get("runtime_overhead_pct_stdev") if SHOW_ERROR_BARS else None
            ax.barh(position, value, height=BAR_HEIGHT, color=COLORS.get(case, "#0072B2"), zorder=3)
            if deviation is not None:
                ax.errorbar(value, position, xerr=deviation, fmt="none",
                            ecolor="#333333", capsize=4, linewidth=1.2, zorder=4)
            if SHOW_VALUES:
                direction = 1 if value >= 0 else -1
                text_x = value + direction * ((deviation or 0) + 0.02 * span)
                ax.text(text_x, position, f"{value:{VALUE_FORMAT}}%", va="center",
                        ha="left" if direction > 0 else "right")

        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels)
        ax.set_ylim(len(labels) - 0.5, -0.5)
        ax.set_xlim(*limits)
        ax.axvline(0, color="#444444", linewidth=1)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}%"))
        ax.set_xlabel(X_LABEL)
        ax.set_title(TITLES.get(path.stem, path.stem), fontsize=TITLE_SIZE, pad=16)
        if SHOW_GRID:
            ax.grid(axis="x", color="#DDDDDD", linewidth=0.7, zorder=0)
        ax.tick_params(axis="both", length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)

        if SHOW_FOOTER:
            selected = [rows[(case, phase)] for case, phase, _ in PLOT_ROWS if (case, phase) in rows]
            note = "100 × (ON time / OFF time − 1); positive = slower. No failures injected."
            if selected and all(row["pairs"] == 1 for row in selected):
                note += "\nOne OFF/ON pair per displayed result; no variance estimate."
            elif SHOW_ERROR_BARS:
                note += "\nError bars, when available: ±1 sample SD of paired overhead (not a confidence interval)."
            rounds = report.get("xgboost_num_boost_round", "unspecified")
            multiplier = report.get("data_block_multiplier", "unspecified")
            note += f"\nXGBoost rounds: {rounds}; Data block multiplier: {multiplier}. Cluster startup/cleanup excluded."
            fig.text(0.02, 0.025, note, fontsize=9, color="#555555", va="bottom")

        destination = output_directory or path.parent
        destination.mkdir(parents=True, exist_ok=True)
        for extension in OUTPUT_FORMATS:
            output = destination / f"{path.stem}.{extension}"
            fig.savefig(output, dpi=DPI, bbox_inches="tight", facecolor="white")
            print(f"Saved {output}")
    finally:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("reports", nargs="*", type=Path, help="Saved JSON reports; defaults to both overhead reports")
    parser.add_argument("--output-dir", type=Path, help="Optional output directory; default beside each report")
    args = parser.parse_args()
    paths = [path.expanduser() for path in args.reports] or [
        REPORT_DIRECTORY.expanduser() / name for name in REPORT_FILENAMES
    ]
    # Read all requested reports before overwriting either plot.
    loaded = []
    for path in paths:
        if not path.is_file():
            parser.error(f"Report not found: {path}. Pass the existing JSON file paths explicitly.")
        report, rows = load_report(path)
        loaded.append((path, report, rows))
    if not PLOT_ROWS:
        parser.error("PLOT_ROWS must contain at least one entry")
    try:
        import matplotlib
    except ImportError:
        parser.error("Install matplotlib in your Python environment: python -m pip install matplotlib")
    matplotlib.use("Agg")
    shared_limits = axis_limits([rows for _, _, rows in loaded]) if SHARE_X_LIMITS else None
    output_directory = args.output_dir.expanduser() if args.output_dir else None
    for path, report, rows in loaded:
        plot_report(path, report, rows, output_directory, shared_limits or axis_limits([rows]))


if __name__ == "__main__":
    main()
