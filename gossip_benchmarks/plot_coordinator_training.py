"""Plot embedded optimizer timelines; never launch training or read result folders."""

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from coordinator_comparison import progress_trace


LABELS = {"ordinary": "Ordinary Ray + checkpoint retry", "resume": "Coordinator input resume",
          "deterministic": "Deterministic splitter + checkpoint retry"}
COLORS = {"ordinary": "#d97706", "resume": "#0072b2", "deterministic": "#7b3294"}


def comparison_caption(report):
    if set(report["modes"]) == {"deterministic", "resume"}:
        return "Identical deterministic sharding; coordinator restart disabled versus enabled. "
    return "Ordinary and resume sharding differ; each fault is checked against its own control. "


def plot(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if report.get("profile") != "cifar-coordinator-resume":
        raise ValueError("Expected a coordinator-training comparison report")
    scenarios = report["scenarios"]
    fig, axes = plt.subplots(1, len(scenarios), figsize=(7 * len(scenarios), 5.2), squeeze=False, sharey=True)
    steps = report["steps_per_epoch"]
    for ax, scenario in zip(axes[0], scenarios):
        samples = [s for s in report["samples"] if s["scenario"] == scenario]
        for sample in samples:
            trace = progress_trace(sample, steps)
            mode = sample["mode"]
            label = LABELS[mode] + (f" (repeat {sample['pair']})" if report["repeats"] > 1 else "")
            label += f" — {trace['outcome']}"
            if not trace["seconds"]:
                ax.plot([], [], color=COLORS[mode], label=label)
                continue
            ax.step(trace["seconds"], trace["updates"], where="post", color=COLORS[mode],
                    alpha=.85, linewidth=1.8, label=label)
            ax.plot(trace["seconds"][-1], trace["updates"][-1],
                    "o" if trace["outcome"] == "completed" else "x", color=COLORS[mode])
            fault = sample.get("coordinator_fault")
            if fault and fault.get("request_ns"):
                ax.axvline((fault["request_ns"] - sample["workload_started_ns"]) / 1e9,
                           color=COLORS[mode], alpha=.45, linestyle=":")
        if not samples:
            ax.text(.5, .5, "No observations", ha="center", transform=ax.transAxes)
        ax.set_title("No failure" if scenario == "none" else "Coordinator-process failure during training")
        ax.set_xlabel("Workload elapsed time (s)")
        ax.set_ylim(bottom=0)
        ax.grid(alpha=.2)
        if samples:
            ax.legend(loc="lower right", fontsize=8)
    axes[0][0].set_ylabel("Current optimizer progress\n(minimum completed updates across two ranks)")
    fig.suptitle("CIFAR-10 / ResNet-18: checkpoint retry versus coordinator input resume")
    fig.text(.5, .02, "Fixed-R OFF; full Train retry enabled; one physical machine. Dotted lines: fault requests.\n"
             + comparison_caption(report)
             + ("One repetition is preliminary." if report["preliminary"] else "Individual repetitions shown."),
             ha="center", fontsize=8)
    fig.tight_layout(rect=(0, .1, 1, .94))
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    for suffix in (".png", ".pdf"):
        path = output.with_suffix(suffix)
        fig.savefig(path, dpi=180)
        print(path)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plot(json.loads(args.report.read_text()), args.output)


if __name__ == "__main__":
    main()
