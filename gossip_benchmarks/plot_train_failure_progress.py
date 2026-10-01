"""Plot measured committed epochs from --comparison failures (PNG and PDF).

Replot a copied report without its original result directories:
  python gossip_benchmarks/plot_train_failure_progress.py report.json --output progress.png
No training is run by this script. Curves are individual trials, not averages.
"""

import argparse
import json
import math
from pathlib import Path


def progress_trace(sample):
    """Use only recorded progress; an expected baseline failure stays failed."""
    start = sample.get("workload_started_ns")
    if not isinstance(start, int) or start <= 0:
        raise ValueError("Missing workload start timestamp")
    times, epochs = [0.0], [0]
    for report in sample.get("reports", []):
        metrics = report["metrics"]
        epoch = metrics[0]["epoch"]
        elapsed = (report["time_ns"] - start) / 1e9
        if (not math.isfinite(elapsed) or elapsed < times[-1]
                or epoch != epochs[-1] + 1
                or any(m["epoch"] != epoch for m in metrics)):
            raise ValueError("Invalid committed-epoch timeline")
        times.append(elapsed)
        epochs.append(epoch)
    fault = sample.get("data_owner_fault") or sample.get("fault") or {}
    fault_s = ((fault["request_ns"] - start) / 1e9
               if fault.get("request_ns") is not None else None)
    if fault_s is not None and (not math.isfinite(fault_s) or fault_s < 0):
        raise ValueError("Failure predates workload")
    passed = sample.get("status") == "passed"
    if passed and (sample.get("timeout") or not sample.get("workload_completed")
                   or epochs[-1] != sample.get("training_epochs")):
        raise ValueError("Completed curve lacks completed-workload evidence")
    # A timeout is censored at the last observed progress, never extended to an
    # invented completion or failure timestamp. Owner loss has its own evidence.
    end = sample.get("workload_finished_ns")
    if sample.get("expected_owner_loss") and not sample.get("timeout"):
        end = sample["ordinary_owner_loss"]["observed_ns"]
    if end is not None and not sample.get("timeout"):
        elapsed = (end - start) / 1e9
        if not math.isfinite(elapsed) or elapsed < times[-1]:
            raise ValueError("Workload ended before its last report")
        times.append(elapsed)
        epochs.append(epochs[-1])
    outcome = ("completed" if passed else "timeout (censored)" if sample.get("timeout")
               else "OwnerDiedError" if sample.get("expected_owner_loss") else "failed")
    return {"seconds": times, "epochs": epochs, "fault_s": fault_s,
            "outcome": outcome}


def plot_report(report, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.ticker import MaxNLocator

    if report.get("profile") != "torch-failure-progress":
        raise ValueError("Expected a --comparison failures report")
    panels = [
        ("worker", "none", "Worker comparison: no failure"),
        ("worker", "worker", "Worker process killed after epoch 1"),
        ("head", "none", "Head ownership comparison: no failure"),
        ("head", "data-owner", "Head/owner processes killed during preprocessing"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(13, 9), sharey=True)
    try:
        for ax, (experiment, scenario, title) in zip(axes.flat, panels):
            samples = [s for s in report["experiments"][experiment]["samples"]
                       if s["scenario"] == scenario]
            if not samples:
                raise ValueError(f"Missing samples for {experiment}/{scenario}")
            labels = set()
            for sample in samples:
                enhanced = (sample["restart_scope"] == "selective" if experiment == "worker"
                            else sample["mode"] == "on")
                color = "#0072B2" if enhanced else "#D55E00"
                label = ("Selective retry (Fixed-R OFF)" if enhanced else "Ordinary Ray: full retry") if experiment == "worker" else (
                    "Fixed-R ON: full retry" if enhanced else "Ordinary Ray: matched head owner")
                arm_key = "restart_scope" if experiment == "worker" else "mode"
                arm = [s for s in samples if s[arm_key] == sample[arm_key]]
                completed = sum(s["status"] == "passed" for s in arm)
                label += f" — {completed}/{len(arm)} completed"
                if not sample.get("workload_started_ns") and sample.get("status") != "passed":
                    ax.plot([], [], color=color, label=label + " (missing telemetry)")
                    continue
                trace = progress_trace(sample)
                ax.step(trace["seconds"], trace["epochs"], where="post", color=color,
                        linestyle="-" if enhanced else "--", linewidth=1.7,
                        alpha=0.8, label=label if label not in labels else None)
                labels.add(label)
                if trace["fault_s"] is not None:
                    ax.axvline(trace["fault_s"], color=color, linestyle=":", alpha=0.65)
                marker = "o" if trace["outcome"] == "completed" else "x"
                ax.plot(trace["seconds"][-1], trace["epochs"][-1], marker=marker,
                        color=color, markersize=7, clip_on=False)
                if trace["outcome"] != "completed":
                    ax.annotate(trace["outcome"], (trace["seconds"][-1], trace["epochs"][-1]),
                                xytext=(5, 12), textcoords="offset points", color=color, fontsize=9)
            ax.set_title(title, fontsize=11)
            ax.set_xlabel("Seconds since workload start (cluster startup excluded)")
            ax.set_ylabel("Committed training epochs")
            ax.set_ylim(-0.5, report["training_epochs"] * 1.08)
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
            ax.grid(alpha=0.2)
            ax.legend(loc="upper left", fontsize=8)
        status = "validated" if report["status"] == "passed" else "VALIDATION FAILED — inspect report"
        fig.suptitle(f"PyTorch workload progress through failures · {status}", fontsize=15)
        fig.legend(handles=[
            Line2D([], [], color="0.4", linestyle=":", label="Failure injection (per run)"),
            Line2D([], [], color="0.4", marker="o", linestyle="none", label="Completed workload"),
            Line2D([], [], color="0.4", marker="x", linestyle="none", label="Failed / censored"),
        ], loc="lower center", bbox_to_anchor=(0.5, 0.075), ncol=3, frameon=False)
        fig.text(0.5, 0.025,
                 "Each line is one trial; both worker policies allow checkpoint recovery. Head ownership is explicitly matched.\n"
                 "Head loss precedes training; external head replacement, GCS disk and driver survive on one physical machine.\n"
                 "Small regression workload; checkpoint/report costs included. "
                 + ("One pair per case: preliminary evidence." if report.get("preliminary") else "All repetitions shown; no averaged trajectory."),
                 ha="center", fontsize=9)
        fig.tight_layout(rect=(0, 0.13, 1, 0.95))
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
