"""Plot saved CIFAR streaming evidence only; never imports or runs the workload."""

import argparse
import json
from pathlib import Path

from plot_train_failure_progress import progress_trace


def event_trace(sample, kind):
    start = sample["workload_started_ns"]
    events = [e for e in sample.get("stream_events", []) if e["kind"] == kind
              and (kind != "decode" or e["split"] == "train")]
    key = "finished_ns" if kind == "decode" else "time_ns"
    times, counts = [0.0], [0]
    for event in sorted(events, key=lambda e: e[key]):
        elapsed = (event[key] - start) / 1e9
        if elapsed < 0:
            raise ValueError("Event predates this workload")
        times.append(elapsed)
        counts.append(counts[-1] + len(event["sample_ids"]))
    return times, counts


def learning_trace(sample):
    trace = progress_trace({**sample, "fault": sample.get("node_fault") or sample.get("fault")})
    if sample.get("timeout") or not sample.get("workload_finished_ns"):
        end = sample.get("observation_finished_ns")
        if end is not None:
            stop = (end - sample["workload_started_ns"]) / 1e9
            if stop < trace["seconds"][-1]:
                raise ValueError("Parent stop predates recorded progress")
            trace["seconds"].append(stop)
            trace["epochs"].append(trace["epochs"][-1])
    return trace


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator

    if report.get("profile") == "cifar-checkpoint-frequency":
        return plot_checkpoint_report(report, output)
    if report.get("profile") != "cifar-streaming-learning":
        raise ValueError("Expected a CIFAR streaming learning report")
    cases = report["cases"]
    fig, axes = plt.subplots(2, len(cases), figsize=(max(10, 5 * len(cases)), 8), squeeze=False)
    try:
        for column, case in enumerate(cases):
            top, bottom = axes[:, column]
            for mode, color in (("off", "#D55E00"), ("on", "#0072B2")):
                samples = [s for s in report["samples"] if s["scenario"] == case["scenario"]
                           and s["failure_point"] == case["failure_point"] and s["mode"] == mode]
                if any(s["restart_scope"] != "full" for s in samples):
                    raise ValueError("Both plotted arms must use standard Train retry")
                title = f"Fixed-R {mode.upper()} / full retry"
                top.plot([], [], color=color, label=title + (
                    f" ({sum(s['status'] == 'passed' for s in samples)}/{len(samples)} validated)" if samples else " (not run)"))
                for kind, style in (("decode", ":"), ("batch", "-")):
                    bottom.plot([], [], color=color, linestyle=style,
                                label=f"{mode.upper()}: {'decoded' if kind == 'decode' else 'delivered'} images")
                for sample in samples:
                    if not sample.get("workload_started_ns"):
                        top.text(.02, .9 if mode == "off" else .8, title + ": no progress telemetry",
                                 color=color, transform=top.transAxes, fontsize=8)
                        continue
                    trace = learning_trace(sample)
                    top.step(trace["seconds"], trace["epochs"], where="post", color=color, alpha=.7)
                    top.plot(trace["seconds"][-1], trace["epochs"][-1], color=color,
                             marker="o" if trace["outcome"] == "completed" else "x")
                    if trace["outcome"] != "completed":
                        top.annotate(trace["outcome"], (trace["seconds"][-1], trace["epochs"][-1]), fontsize=8)
                    for kind, style in (("decode", ":"), ("batch", "-")):
                        x, y = event_trace(sample, kind)
                        bottom.step(x, y, where="post", color=color, linestyle=style, alpha=.7)
                    if trace["fault_s"] is not None:
                        for ax in (top, bottom):
                            ax.axvline(trace["fault_s"], color=color, linestyle="--", alpha=.4)
            title = "No failure" if case["scenario"] == "none" else (
                f"{case['scenario']} / {case['failure_point']}\n"
                f"epoch {case['fault_after_epoch'] + 1}, step {report['fault_after_step']}")
            top.set_title(title)
            top.set_ylabel("Committed epochs")
            top.yaxis.set_major_locator(MaxNLocator(integer=True))
            top.set_ylim(-.1, report["training_epochs"] + .3)
            bottom.set_ylabel("Images decoded / delivered, including repeats")
            for ax in (top, bottom):
                ax.set_xlabel("Seconds since workload start")
                ax.legend(fontsize=7)
                ax.grid(alpha=.2)
        identity = report["input_identity"]
        fig.suptitle(f"CIFAR-10 / ResNet-18: streaming learning · {report['status'].upper()}")
        fig.text(.5, .02,
                 f"{identity['training_rows']} training / {identity['validation_rows']} held-out images; two CPU workers, one physical machine.\n"
                 "Default ownership; both arms allow checkpoint retry. Head cases replace processes using surviving GCS storage.\n"
                 "Dots: validated completion; crosses: failure/censoring. Timeout stops include parent cleanup. Delivery precedes the optimizer update.\n"
                 "Batch assignment can differ: equal final weights/accuracy are not claimed. "
                 + ("One pair is preliminary." if report.get("preliminary") else "All repetitions shown."),
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, .13, 1, .95))
        output = Path(output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        paths = [output.with_suffix(suffix) for suffix in (".png", ".pdf")]
        for path in paths:
            fig.savefig(path, dpi=180)
        return paths
    finally:
        plt.close(fig)



def plot_checkpoint_report(report, output):
    """Embedded measured values only; failed runs never become timing bars."""
    import statistics
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    panels = [
        (axes[0, 0], "Workload seconds", lambda s: s["workload_s"], ("none", "worker-node")),
        (axes[0, 1], "Repeated optimizer updates per rank", lambda s: s["recovery"]["repeated_updates_per_rank"], ("worker-node",)),
        (axes[1, 0], "Completed training-image decodes", lambda s: s["decoded_training_rows"], ("none", "worker-node")),
        (axes[1, 1], "Serialized checkpoint MiB, summed across ranks", lambda s: s["checkpoint_metrics"]["serialized_bytes"] / 1024**2, ("none",)),
    ]
    try:
        for ax, title, value, scenarios in panels:
            for offset, policy, color in ((-.18, "epoch", "#D55E00"), (.18, "mid_epoch", "#0072B2")):
                for index, scenario in enumerate(scenarios):
                    samples = [s for s in report["samples"] if s["scenario"] == scenario and s["policy"] == policy]
                    passing = [s for s in samples if s["status"] == "passed"]
                    x = index + offset
                    label = ("Epoch checkpoints" if policy == "epoch" else "Mid-epoch checkpoints") if index == 0 else None
                    if passing:
                        values = [value(s) for s in passing]
                        ax.bar(x, statistics.mean(values), width=.32, color=color, alpha=.65, label=label)
                        ax.scatter([x]*len(values), values, color=color, s=18)
                    else:
                        ax.plot([], [], color=color, label=label)
                    if len(passing) != len(samples) or not samples:
                        ax.text(x, .04, f"{len(passing)}/{len(samples)} passed", rotation=90,
                                transform=ax.get_xaxis_transform(), ha="center", fontsize=8)
            ax.set_title(title, fontsize=10)
            ax.set_xticks(range(len(scenarios)), ["Healthy" if s == "none" else "Worker-node loss" for s in scenarios])
            ax.set_ylim(bottom=0)
            ax.legend(fontsize=8)
            ax.grid(axis="y", alpha=.2)
        status = "passed" if report["status"] == "passed" else "INCOMPLETE / FAILED — inspect JSON"
        fig.suptitle(f"CIFAR checkpoint frequency · {status}")
        fig.text(.5, .025,
                 "Ordinary full-group retry in both arms; Fixed-R and coordinator resume OFF. Same deterministic file stripes.\n"
                 "Dots: individual passing trials. Bars: means. Decodes include verified prefix replay and prefetch.\n"
                 "Single-machine logical-node loss; storage and spare capacity survive. Checkpoint times/bytes include per-rank state.",
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, .11, 1, .94))
        output = Path(output).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        paths = [output.with_suffix(suffix) for suffix in (".png", ".pdf")]
        for path in paths:
            fig.savefig(path, dpi=180)
        return paths
    finally:
        plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for path in plot_report(json.loads(args.report.read_text()), args.output):
        print(path)
