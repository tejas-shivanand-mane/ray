"""Plot embedded measured Fashion-MNIST timelines; never runs training."""

import argparse
import json
from pathlib import Path

from plot_train_failure_progress import progress_trace


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    if report.get("profile") != "fashion-mnist-integrated-training":
        raise ValueError("Expected a Fashion-MNIST integrated training report")
    points = [p for p in report["failure_epochs"]
              if any(s["failure_point"] == p for s in report["samples"])]
    if not points:
        raise ValueError("No observations to plot")
    columns = min(2, len(points))
    rows = (len(points) + columns - 1) // columns
    fig, axes = plt.subplots(rows, columns, figsize=(13, 4 * rows + 1),
                             squeeze=False, sharex=True, sharey=True)
    try:
        for ax, point in zip(axes.flat, points):
            for arm, mode, scope, color, style, title in (
                ("ordinary", "off", "full", "#D55E00", "--", "Ordinary Ray: OFF/full retry"),
                ("integrated", "on", "selective", "#0072B2", "-", "Fixed-R ON/selective retry"),
            ):
                samples = [s for s in report["samples"] if s["failure_point"] == point and s["arm"] == arm]
                if not samples or any(s["mode"] != mode or s["restart_scope"] != scope for s in samples):
                    raise ValueError("Missing or mismatched measured comparison arm")
                label = title + f" ({sum(s['status'] == 'passed' for s in samples)}/{len(samples)} validated)"
                ax.plot([], [], color=color, linestyle=style, label=label)
                for sample in samples:
                    if not sample.get("workload_started_ns"):
                        if sample["status"] == "passed":
                            raise ValueError("Passing sample lacks workload telemetry")
                        continue
                    trace = progress_trace(sample)
                    ax.step(trace["seconds"], trace["epochs"], where="post", color=color,
                            linestyle=style, alpha=0.75)
                    if trace["fault_s"] is not None:
                        ax.axvline(trace["fault_s"], color=color, linestyle=":", alpha=0.5)
                    marker = "o" if trace["outcome"] == "completed" else "x"
                    ax.plot(trace["seconds"][-1], trace["epochs"][-1], marker=marker, color=color)
                    if trace["outcome"] != "completed":
                        ax.annotate(trace["outcome"], (trace["seconds"][-1], trace["epochs"][-1]),
                                    xytext=(4, 8), textcoords="offset points", fontsize=8)
            title = "No failure" if point == "none" else f"{point.capitalize()}: worker killed after epoch {report['failure_epochs'][point]}"
            ax.set_title(title)
            ax.set_xlabel("Seconds since workload start (cluster startup excluded)")
            ax.set_ylabel("Committed training epochs")
            ax.set_ylim(-0.25, report["training_epochs"] + 0.5)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.legend(fontsize=8)
            ax.grid(alpha=0.2)
        for ax in list(axes.flat)[len(points):]:
            ax.set_visible(False)
        status = "validated" if report["status"] == "passed" else "VALIDATION FAILED — inspect report"
        fig.suptitle(f"Fashion-MNIST: ordinary Ray versus integrated recovery · {status}")
        fig.text(0.5, 0.02,
                 "Full 60,000/10,000 split; same model, epochs and application checkpoints. Two CPU workers on one machine.\n"
                 "Both arms allow checkpoint recovery. Dotted lines: worker-process failure at a committed epoch; dots: completed, crosses: failed/censored.\n"
                 "Fixed-R protects preprocessing; this fault does not demonstrate owner-loss replay. Each line is one measured trial.\n"
                 + ("One pair per case: preliminary evidence." if report.get("preliminary") else "All repetitions shown; no averaged trajectory."),
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, 0.19 if rows == 1 else 0.13, 1, 0.95))
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
