"""Small tables and plots; modeled costs and measured timings stay separate."""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def report(folder):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    folder = Path(folder)
    searched = json.loads((folder / "search.json").read_text())
    lines = [
        "# Selective replication feasibility study",
        "",
        "Real digits training; local worker-process loss only. No Ray/TorchFT, "
        "Fixed-R, SMT, or GCP VM-loss claim. Monetary rates and finite scenario "
        "probabilities are user assumptions. This is a warm-worker experiment; "
        "dataset preparation, pool startup, cloud provisioning, and external "
        "storage costs are outside the measured interval.",
        "",
        "## Modeled policy selection",
        "",
        "Best means minimum modeled cost among enumerated assignments and "
        "priority orders meeting the supplied probability/deadline constraint. "
        "It does not mean an optimal adaptive schedule. A machine lease starts "
        "on first use and includes subsequent idle time until completion/loss.",
        "",
        "| Case | Joint cost | Best baseline cost | Modeled reduction |",
        "|---|---:|---:|---:|",
    ]
    gains, labels = [], []
    for i, case in enumerate(searched["cases"]):
        joint = case["policies"]["joint_enumeration"]
        baselines = [
            r
            for name, r in case["policies"].items()
            if name != "joint_enumeration" and r
        ]
        if joint is None:
            lines.append(f"| {i}: {case['name']} | infeasible | infeasible | — |")
        elif not baselines:
            lines.append(
                f"| {i}: {case['name']} | {joint['expected_cost']:.7f} | infeasible | — |"
            )
        else:
            baseline = min(r["expected_cost"] for r in baselines)
            gain = 100 * (1 - joint["expected_cost"] / baseline)
            lines.append(
                f"| {i}: {case['name']} | {joint['expected_cost']:.7f} | {baseline:.7f} | {gain:.2f}% |"
            )
            gains.append(gain)
            labels.append(str(i))
    lines.extend(
        [
            "",
            "All individual policies, plans, feasibility values, and scenarios are in search.json.",
            "",
        ]
    )
    if gains:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.bar(labels, gains)
        ax.set(
            xlabel="Case index",
            ylabel="Modeled cost reduction (%)",
            title="Joint enumeration vs best feasible baseline (modeled)",
        )
        ax.set_ylim(bottom=0, top=max(1, max(gains) * 1.15))
        fig.tight_layout()
        fig.savefig(folder / "modeled-cost.png", dpi=160)
        plt.close(fig)
    path = folder / "replay.json"
    if path.exists():
        replay = json.loads(path.read_text())
        lines.extend(
            [
                "## Measured process-loss replay",
                "",
                "Selected cases, not a representative sample. Policies producing "
                "the same plan share the same replay. Holdout correlated losses use "
                "a time excluded from optimization. Deadline hit counts below are "
                "observations, not estimates certifying the requested probability.",
                "",
                "| Case/policies/scenario | Runs | Valid complete | Deadline hits | Median seconds [min, max] |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        grouped = defaultdict(list)
        for row in replay["rows"]:
            key = f"{row['case_index']}/{' + '.join(row['policies'])}/{row['scenario']['name']}"
            grouped[key].append(row)
        for key, rows in grouped.items():
            times = [r["elapsed_s"] for r in rows]
            lines.append(
                f"| {key} | {len(rows)} | {sum(r['correct'] for r in rows)} | {sum(r['met_deadline'] for r in rows)} | {statistics.median(times):.3f} [{min(times):.3f}, {max(times):.3f}] |"
            )
        rows = replay["rows"]
        if rows:
            percentage_errors = [
                100
                * abs(r["elapsed_s"] - r["predicted"]["elapsed_s"])
                / max(r["elapsed_s"], 1e-9)
                for r in rows
            ]
            optimistic = sum(
                r["predicted"]["complete"]
                and r["predicted"]["elapsed_s"] <= r["deadline_s"]
                and not r["met_deadline"]
                for r in rows
            )
            lines.extend(
                [
                    "",
                    f"Median absolute timing prediction error: {statistics.median(percentage_errors):.1f}%. "
                    f"Predicted deadline hits that missed in replay: {optimistic}/{len(rows)}. "
                    "This is a calibration check on selected scenarios, not a reliability estimate.",
                ]
            )
        lines.extend(
            [
                "",
                "Validation compares every committed model and optimizer tensor with its "
                "healthy reference using absolute tolerance 1e-5, plus exact epoch and "
                "input-order history. It also checks the selected hyperparameter trial. "
                "Events record PID/exit code, requested and actual fault time, "
                "checkpoint restoration, and one commit per trial.",
            ]
        )
    lines.extend(
        [
            "",
            "A worthwhile next step requires reproducible measured improvements over "
            "the strongest baseline across more workloads and held-out faults. A modeled "
            "advantage alone is insufficient evidence for adding an SMT solver.",
            "",
        ]
    )
    (folder / "summary.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder")
    report(parser.parse_args().folder)
