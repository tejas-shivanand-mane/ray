"""Plot recorded no-failure runtime overhead without rerunning Ray workloads."""

import argparse
import json
import math
from pathlib import Path


LABELS = {
    "backpressure": "Backpressure",
    "worker-scaling-actors": "Actor worker-scaling",
    "xgboost-single": "XGBoost: 1 worker",
    "xgboost-multi": "XGBoost: 2 workers",
}


def require_matplotlib():
    try:
        import matplotlib
    except ImportError as exc:
        raise RuntimeError(
            "Plotting requires matplotlib in ray-dev. Install it once with "
            "python -m pip install matplotlib; no Ray rebuild is required."
        ) from exc
    matplotlib.use("Agg")


def render_report(report, prefix):
    require_matplotlib()
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    rows = report.get("summary", [])
    for row in rows:
        value = row["runtime_overhead_pct_mean"]
        deviation = row.get("runtime_overhead_pct_stdev")
        if not math.isfinite(value) or (deviation is not None and (
            not math.isfinite(deviation) or deviation < 0
        )):
            raise ValueError("Cannot plot nonfinite overhead or invalid deviation")
    prefix = Path(prefix)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, max(4.2, 0.58 * len(rows) + 2.2)))
    fig.subplots_adjust(left=0.32, right=0.95, top=0.80, bottom=0.22)
    fig.suptitle("Fixed-R overhead without failures", fontsize=17, fontweight="bold", y=0.96)
    fig.text(0.5, 0.885, "Runtime change relative to recovery OFF · positive = slower",
             ha="center", fontsize=11, color="#444444")
    if rows:
        values = [row["runtime_overhead_pct_mean"] for row in rows]
        deviations = [row.get("runtime_overhead_pct_stdev") or 0 for row in rows]
        labels = [f"{LABELS.get(row['case'], row['case'])} / {row['phase']}  (n={row['pairs']})"
                  for row in rows]
        positions = list(range(len(rows)))
        ax.barh(positions, values, height=0.60,
                color=["#0072B2" if value >= 0 else "#009E73" for value in values], zorder=3)
        for index, (value, deviation) in enumerate(zip(values, deviations)):
            if rows[index].get("runtime_overhead_pct_stdev") is not None:
                ax.errorbar(value, index, xerr=deviation, fmt="none", ecolor="#222222",
                            capsize=4, linewidth=1.2, zorder=4)
        lo = min(0, *(value - deviation for value, deviation in zip(values, deviations)))
        hi = max(0, *(value + deviation for value, deviation in zip(values, deviations)))
        span = max(hi - lo, 1)
        ax.set_xlim(lo - 0.22 * span, hi + 0.25 * span)
        for index, (value, deviation) in enumerate(zip(values, deviations)):
            direction = 1 if value >= 0 else -1
            ax.text(value + direction * (deviation + 0.025 * span), index,
                    f"{value:+.2f}%", va="center", ha="left" if direction > 0 else "right",
                    fontsize=10, color="#222222")
        ax.set_yticks(positions)
        ax.set_yticklabels(labels)
        ax.invert_yaxis()
        ax.axvline(0, color="#333333", linewidth=1, zorder=2)
        ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}%"))
        ax.set_xlabel("100 × (ON time / OFF time − 1)", fontsize=11)
        ax.grid(axis="x", color="#DDDDDD", linewidth=0.7, zorder=0)
        ax.tick_params(axis="both", labelsize=10, length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)
    else:
        ax.set_axis_off()
        ax.text(0.5, 0.5, "No completed OFF/ON pairs yet", transform=ax.transAxes,
                ha="center", va="center", fontsize=14)
    counts = [row["pairs"] for row in rows]
    note = ("One pair per case: preliminary; no variance estimate."
            if counts and max(counts) == 1 else
            "n = paired observations. Error bars: ±1 sample SD of paired overhead; not confidence intervals.")
    completed = {row["case"] for row in rows if row["phase"] == "total"}
    missing = [LABELS.get(name, name) for name in report.get("cases", []) if name not in completed]
    status = str(report.get("status", "unknown")).upper()
    footer = f"{status} · {note}\nLocal benchmark timers exclude cluster startup/cleanup; no warmup."
    if missing:
        footer += "\nNo complete pair: " + ", ".join(missing)
    fig.text(0.02, 0.03, footer, fontsize=9, color="#555555", va="bottom")
    try:
        for extension in (".png", ".pdf"):
            fig.savefig(Path(str(prefix) + extension), dpi=180, bbox_inches="tight", facecolor="white")
    finally:
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output-prefix", type=Path)
    args = parser.parse_args()
    report = json.loads(args.report.read_text())
    if report.get("profile") != "fixed-r-no-failure-overhead":
        parser.error("Expected a fixed-r-no-failure-overhead report")
    prefix = args.output_prefix or args.report.with_suffix("")
    render_report(report, prefix)
    print(f"Plots: {prefix}.png and {prefix}.pdf")


if __name__ == "__main__":
    main()
