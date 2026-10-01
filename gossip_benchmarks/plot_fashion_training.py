"""Plot embedded measured Fashion-MNIST timelines; never runs training."""

import argparse
import json
from pathlib import Path

from plot_train_failure_progress import progress_trace


def panel_layout(report):
    """Keep node scope and timing separate; controls are explicitly shared."""
    if report.get("profile") == "fashion-mnist-failure-matrix":
        points = list(report["failure_epochs"])
        kinds = report["failure_kinds"]
        if not points or not kinds or points[0] != "none":
            raise ValueError("Matrix requires a shared control and selected failure kinds")
        return len(kinds), len(points), [(kind, point) for kind in kinds for point in points]
    if report.get("profile") == "fashion-mnist-integrated-training":
        points = [p for p in report["failure_epochs"]
                  if any(s["failure_point"] == p for s in report["samples"])]
        if not points:
            raise ValueError("No observations to plot")
        columns = min(2, len(points))
        return (len(points) + columns - 1) // columns, columns, [("worker", p) for p in points]
    raise ValueError("Expected a Fashion-MNIST comparison report")


def panel_samples(report, kind, point, arm):
    scenario = "none" if point == "none" else kind
    return [s for s in report["samples"]
            if s["scenario"] == scenario and s["failure_point"] == point and s["arm"] == arm]


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    rows, columns, panels = panel_layout(report)
    node_matrix = report["profile"] == "fashion-mnist-failure-matrix"
    names = {"head-node": "Head-node processes", "worker-node": "Worker node", "worker": "Worker process"}
    fig, axes = plt.subplots(rows, columns, figsize=(max(10, 5 * columns), 4 * rows + 1.5),
                             squeeze=False, sharex=True, sharey=True)
    try:
        for ax, (kind, point) in zip(axes.flat, panels):
            for arm, mode, scope, color, style, title in (
                ("ordinary", "off", "full", "#D55E00", "--", "Ordinary Ray: OFF/full retry"),
                ("integrated", "on", "selective", "#0072B2", "-", "Fixed-R ON/selective retry"),
            ):
                samples = panel_samples(report, kind, point, arm)
                if any(s["mode"] != mode or s["restart_scope"] != scope for s in samples):
                    raise ValueError("Mismatched measured comparison arm")
                if not samples:
                    if report["status"] == "passed":
                        raise ValueError("Passing report is missing a requested comparison arm")
                    ax.plot([], [], color=color, linestyle=style, label=title + " (not run)")
                    continue
                label = title + f" ({sum(s['status'] == 'passed' for s in samples)}/{len(samples)} validated)"
                ax.plot([], [], color=color, linestyle=style, label=label)
                for sample in samples:
                    if not sample.get("workload_started_ns"):
                        if sample["status"] == "passed":
                            raise ValueError("Passing sample lacks workload telemetry")
                        ax.text(0.02, 0.92 if arm == "ordinary" else 0.82,
                                title + ": failed before telemetry", transform=ax.transAxes,
                                color=color, fontsize=8)
                        continue
                    # The supervisor file survives even if a failed controller
                    # could not publish its final timeline after node loss.
                    trace = progress_trace({**sample, "fault": sample.get("node_fault") or sample.get("fault")})
                    ax.step(trace["seconds"], trace["epochs"], where="post", color=color,
                            linestyle=style, alpha=0.75)
                    if trace["fault_s"] is not None:
                        ax.axvline(trace["fault_s"], color=color, linestyle=":", alpha=0.5)
                    marker = "o" if trace["outcome"] == "completed" else "x"
                    ax.plot(trace["seconds"][-1], trace["epochs"][-1], marker=marker, color=color)
                    if trace["outcome"] != "completed":
                        ax.annotate(trace["outcome"], (trace["seconds"][-1], trace["epochs"][-1]),
                                    xytext=(4, 8), textcoords="offset points", fontsize=8)
            title = ("Shared no-failure control" if node_matrix else "No failure") if point == "none" else (
                f"{point.capitalize()}: after epoch {report['failure_epochs'][point]}"
            )
            title = f"{names[kind]}\n{title}"
            ax.set_title(title)
            ax.set_xlabel("Seconds since workload start (cluster startup excluded)")
            ax.set_ylabel("Committed training epochs")
            ax.set_ylim(-0.25, report["training_epochs"] + 0.5)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.legend(fontsize=8)
            ax.grid(alpha=0.2)
        for ax in list(axes.flat)[len(panels):]:
            ax.set_visible(False)
        status = "validated" if report["status"] == "passed" else "VALIDATION FAILED — inspect report"
        fig.suptitle(f"Fashion-MNIST: ordinary Ray versus integrated recovery · {status}")
        if node_matrix:
            scopes = {
                "head-node": "Head: processes replaced; GCS storage and driver survive.",
                "worker-node": "Worker node: raylet, object store and children killed; reduced resources.",
                "worker": "Worker process: one actor killed; its node survives.",
            }
            scope = (" ".join(scopes[kind] for kind in report["failure_kinds"]) + "\n"
                     "Control trials are shared across rows; no physical-machine failure. Preprocessing is materialized before the training fault.\n")
        else:
            scope = "Worker-process loss only; no node failure. Fixed-R protects preprocessing; this fault does not demonstrate owner-loss replay.\n"
        fig.text(0.5, 0.02,
                 "Full 60,000/10,000 split; same model, epochs and application checkpoints. Two CPU workers on one machine.\n"
                 "Both arms allow checkpoint recovery. Dotted lines: failure at a committed epoch; dots: completed, crosses: failed/censored. Each line is one trial.\n"
                 + scope
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
