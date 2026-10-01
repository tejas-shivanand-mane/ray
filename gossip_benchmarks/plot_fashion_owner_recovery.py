"""Plot measured owner-recovery JSON only; never imports or runs the benchmark."""

import argparse
import json
import math
from pathlib import Path

from plot_train_failure_progress import progress_trace


def map_trace(sample):
    training = progress_trace(sample)
    start = sample["workload_started_ns"]
    end = sample.get("workload_finished_ns")
    if sample.get("expected_owner_loss"):
        end = sample["ordinary_owner_loss"]["observed_ns"]
    times, counts = [0.0], [0]
    for event in sample.get("map_progress", []):
        if end is not None and not sample.get("timeout") and event["time_ns"] > end:
            # An orphaned producer can finish during cleanup after owner loss
            # has already failed the workload. It is not subsequent progress.
            break
        elapsed = (event["time_ns"] - start) / 1e9
        if not math.isfinite(elapsed) or elapsed < times[-1] or event["index"] != counts[-1]:
            raise ValueError("Invalid ordered shuffle computation timeline")
        times.append(elapsed)
        counts.append(counts[-1] + 1)
    if sample["status"] == "passed" and counts[-1] != 4:
        raise ValueError("Completed workload lacks four map computations")
    if end is not None and not sample.get("timeout"):
        elapsed = (end - start) / 1e9
        if elapsed < times[-1]:
            raise ValueError("Map computation follows workload termination")
        times.append(elapsed)
        counts.append(counts[-1])
    return {**training, "seconds": times, "epochs": counts}


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    if report.get("profile") != "fashion-mnist-owner-replay":
        raise ValueError("Expected a Fashion-MNIST owner-replay report")
    points = list(report["failure_points"])
    if not points or points[0] != "none":
        raise ValueError("Report needs matched controls")
    fig, axes = plt.subplots(2, len(points), figsize=(max(11, 5 * len(points)), 9),
                             sharex=True, squeeze=False)
    try:
        for col, point in enumerate(points):
            title = "No failure: matched ownership" if point == "none" else (
                f"{point.capitalize()}: after {report['failure_points'][point]}/4 map computations")
            for row, trace_fn in enumerate((map_trace, progress_trace)):
                ax = axes[row, col]
                for mode, color, style, label in (
                    ("off", "#D55E00", "--", "Ordinary Ray: Fixed-R OFF"),
                    ("on", "#0072B2", "-", "Recovery: Fixed-R ON"),
                ):
                    samples = [s for s in report["samples"] if s["mode"] == mode and s["failure_point"] == point]
                    if not samples and report["status"] == "passed":
                        raise ValueError("Verified comparison is missing a requested arm")
                    if any(s.get("restart_scope") != "full" or s.get("owner_placement") != "head"
                           for s in samples if s["status"] == "passed"):
                        raise ValueError("Mismatched measured arm")
                    completed = sum(s["status"] == "passed" for s in samples)
                    ax.plot([], [], color=color, linestyle=style,
                            label=label + (f" ({completed}/{len(samples)} completed)" if samples else " (not run)"))
                    for sample in samples:
                        if not sample.get("workload_started_ns"):
                            if sample["status"] == "passed":
                                raise ValueError("Completed observation lacks telemetry")
                            ax.text(.02, .9 if mode == "off" else .8, label + ": missing telemetry",
                                    transform=ax.transAxes, fontsize=8, color=color)
                            continue
                        trace = trace_fn(sample)
                        ax.step(trace["seconds"], trace["epochs"], where="post", color=color,
                                linestyle=style, alpha=.8)
                        if trace["fault_s"] is not None:
                            ax.axvline(trace["fault_s"], color=color, linestyle=":", alpha=.5)
                        completed = trace["outcome"] == "completed"
                        xy = trace["seconds"][-1], trace["epochs"][-1]
                        ax.plot(*xy, marker="o" if completed else "x", color=color)
                        if not completed:
                            ax.annotate(trace["outcome"], xy, xytext=(4, 7), textcoords="offset points", fontsize=8)
                ax.set_title(title)
                ax.set_ylabel("Shuffle maps computed (not copied outputs)" if row == 0 else "Committed training epochs")
                ax.set_ylim(-.25, (4 if row == 0 else report["training_epochs"]) + .5)
                ax.set_xlabel("Seconds since workload start (cluster startup excluded)")
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))
                ax.legend(fontsize=8)
                ax.grid(alpha=.2)
        status = "comparison criteria verified" if report["status"] == "passed" else "INCOMPLETE / INVALID — inspect JSON"
        fig.suptitle("Fashion-MNIST: matched head-owner loss during preprocessing · " + status)
        fig.text(.5, .02,
                 "Both arms: standard Train retries and application checkpoints; same head ownership and controlled map order.\n"
                 "Dotted lines: head-process failure. Crosses: failed/censored workloads, including verified OFF OwnerDiedError; dots: completion.\n"
                 "Early/middle/late describe map computations, not elapsed-time fractions. Faults precede training; no interrupted epoch is resumed.\n"
                 "GCS disk, off-head driver and executors survive on one machine. Head replacement is identical in both arms.\n"
                 + ("One pair per case: preliminary evidence." if report.get("preliminary") else "Individual trials shown; no averaged trajectory."),
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, .15, 1, .95))
        output = Path(output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        paths = [output.with_suffix(s) for s in (".png", ".pdf")]
        for path in paths:
            fig.savefig(path, dpi=180)
        return [str(p) for p in paths]
    finally:
        plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print("\n".join(plot_report(json.loads(args.report.read_text()), args.output)))
