"""Standalone plots of measured end-to-end restart trials; never runs workloads."""

import argparse
import json
from pathlib import Path
import statistics

from plot_train_failure_progress import progress_trace


def trial_trace(sample):
    origin = sample["observation_started_ns"]
    seconds, epochs, faults, restarts = [0.0], [0], [], []
    for index, attempt in enumerate(sample.get("attempts", [])):
        if index:
            restarts.append((attempt["attempt_started_ns"] - origin) / 1e9)
        if not attempt.get("workload_started_ns"):
            continue
        trace = progress_trace(attempt)
        offset = (attempt["workload_started_ns"] - origin) / 1e9
        if offset < seconds[-1]:
            raise ValueError("Overlapping application attempts")
        seconds.extend(offset + t for t in trace["seconds"])
        epochs.extend(trace["epochs"])
        if trace["fault_s"] is not None:
            faults.append(offset + trace["fault_s"])
    if sample["status"] == "passed":
        if (sample.get("timeout") or not sample.get("workload_completed")
                or epochs[-1] != sample["training_epochs"] or sample["observation_wall_s"] < seconds[-1]):
            raise ValueError("Missing verified completion evidence")
        seconds.append(sample["observation_wall_s"])
        epochs.append(epochs[-1])
    return {"seconds": seconds, "epochs": epochs, "faults": faults, "restarts": restarts}


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    if report.get("profile") != "fashion-mnist-owner-restart":
        raise ValueError("Expected a whole-workload restart comparison report")
    points = list(report["failure_points"])
    fig = plt.figure(figsize=(max(11, 4.8 * len(points)), 9))
    grid = fig.add_gridspec(2, len(points))
    timing = fig.add_subplot(grid[0, :])
    try:
        arms = (("off", "#D55E00", "--", "Ordinary Ray + same-cluster application restart"),
                ("on", "#0072B2", "-", "Fixed-R recovery"))
        for i, point in enumerate(points):
            ax = fig.add_subplot(grid[1, i])
            pairs = [p for p in report["pairs"] if p["failure_point"] == point]
            for arm_index, (mode, color, style, label) in enumerate(arms):
                values = [p[f"{mode}_completion_wall_s"] for p in pairs]
                position = i + (arm_index - .5) * .36
                if values:
                    mean = statistics.mean(values)
                    timing.bar(position, mean, width=.34, color=color, alpha=.5,
                               label=label if i == 0 else None)
                    timing.scatter([position] * len(values), values, color=color, s=22)
                    timing.annotate(f"{mean:.1f}s", (position, mean), xytext=(0, 5), textcoords="offset points", ha="center", fontsize=9)
                else:
                    timing.text(position, 0, "no valid pair", rotation=90, va="bottom", ha="center", fontsize=8)
                samples = [s for s in report["samples"] if s["failure_point"] == point and s["mode"] == mode]
                if not samples and report["status"] == "passed":
                    raise ValueError("Verified report is missing a requested arm")
                ax.plot([], [], color=color, linestyle=style,
                        label=("Ordinary + restart" if mode == "off" else "Fixed-R")
                        + f" ({sum(s['status'] == 'passed' for s in samples)}/{len(samples)} completed)")
                for sample in samples:
                    trace = trial_trace(sample)
                    ax.step(trace["seconds"], trace["epochs"], where="post", color=color, linestyle=style, alpha=.8)
                    for t in trace["faults"]:
                        ax.axvline(t, color=color, linestyle=":", alpha=.5)
                    for t in trace["restarts"]:
                        ax.plot(t, 0, marker="^", color=color)
                    ax.plot(trace["seconds"][-1], trace["epochs"][-1], color=color,
                            marker="o" if sample["status"] == "passed" else "x")
            ax.set_title(point.capitalize())
            ax.set_ylim(-.4, report["training_epochs"] + .5)
            ax.set_xlabel("Seconds since original trial start")
            ax.set_ylabel("Committed training epochs")
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.grid(alpha=.2)
            ax.legend(fontsize=8)
        timing.set_xticks(range(len(points)), [p.capitalize() for p in points])
        timing.set_ylabel("Seconds to verified completion")
        timing.set_title("Initial startup + all application attempts + correctness validation + cleanup")
        timing.legend(fontsize=9)
        timing.grid(axis="y", alpha=.2)
        timing.margins(y=.2)
        status = "verified" if report["status"] == "passed" else "INCOMPLETE / INVALID — inspect JSON"
        workload = "Frozen MobileNet features + MLP" if report.get("feature_identity") else "Raw pixels + MLP"
        fig.suptitle(f"{workload}: ordinary restart versus Fixed-R · {status}")
        fig.text(.5, .02,
                 "Bars: means of valid matched pairs; points: individual timings. Failed or unmatched trials are excluded from means, never treated as completions.\n"
                 "Dotted lines: owner failure; triangles: application restart on the same repaired cluster; dots: verified completion; crosses: failed/censored.\n"
                 "Both arms retain standard Train retries. Controlled head ownership and shuffle-map order; faults precede training.\n"
                 "GCS disk, driver, executors and original inputs survive. No durable feature cache, physical-machine loss or driver recovery.\n"
                 + ("One pair per case: preliminary evidence." if report.get("preliminary") else "Every trial is retained in the report."),
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, .15, 1, .95))
        output = Path(output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        paths = [output.with_suffix(suffix) for suffix in (".png", ".pdf")]
        for path in paths:
            fig.savefig(path, dpi=180)
        return [str(path) for path in paths]
    finally:
        plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print("\n".join(plot_report(json.loads(args.report.read_text()), args.output)))
