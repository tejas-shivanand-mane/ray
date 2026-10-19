"""Small tables and plots; modeled costs and measured timings stay separate."""

import argparse
import json
import math
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
        "storage costs are outside the measured interval in legacy runs. Version 2 "
        "can charge all warm workers and a shared-storage hourly rate; see each case accounting. "
        "Runtime includes initialization/restoration and completion; cost continues through cancellation acknowledgments. "
        "Validation runs afterward. These are assumed charges, not cloud bills.",
        "",
        "## Modeled policy selection",
        "",
        "Best means minimum modeled cost among enumerated assignments and "
        "priority orders meeting the supplied probability/deadline constraint. "
        "It does not mean an optimal adaptive schedule. A machine lease starts "
        "on first use in legacy accounting. With bill_warm_pool=true, all workers "
        "are billed from time zero until completion/loss, including idle reserve capacity.",
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
        if rows and rows[0].get("schema_version") == 2:
            lines.extend(
                [
                    "",
                    "## Measured cost and failure exposure",
                    "",
                    "Scenario-weighted means use every planning scenario and its supplied weight. "
                    "Held-out scenarios are excluded from that expectation and remain in the table above. "
                    "Deadline frequencies do not certify reliability. Identical plans share measurements.",
                    "",
                    "| Case / policy | Modeled feasible | Weighted assumed cost | Weighted deadline frequency | Median worker-busy seconds | Active / total injected losses |",
                    "|---|---|---:|---:|---:|---:|",
                ]
            )
            by_policy = defaultdict(list)
            for row in rows:
                for policy in row["policies"]:
                    by_policy[(row["case_index"], policy)].append(row)
            for (case_index, policy), measurements in sorted(by_policy.items()):
                by_scenario = defaultdict(list)
                for row in measurements:
                    by_scenario[row["scenario"]["name"]].append(row)
                expected_cost, expected_hit = 0.0, 0.0
                for scenario in searched["cases"][case_index]["scenarios"]:
                    samples = by_scenario.get(scenario["name"], [])
                    if not samples:
                        expected_cost = expected_hit = math.nan
                        break
                    expected_cost += scenario["probability"] * statistics.mean(
                        r["assumed_cost"] for r in samples
                    )
                    expected_hit += scenario["probability"] * statistics.mean(
                        r["met_deadline"] for r in samples
                    )
                faults = [e for r in measurements for e in r["fault_evidence"].values()]
                active = sum(e["active_task"] is not None for e in faults)
                busy = statistics.median(r["worker_busy_s"] for r in measurements)
                feasible = searched["cases"][case_index]["policies"][policy] is not None
                lines.append(
                    f"| {case_index} / {policy} | {feasible} | {expected_cost:.7f} | {expected_hit:.3f} | {busy:.3f} | {active}/{len(faults)} |"
                )
            injections = [e for r in rows for e in r["fault_evidence"].values()]
            delays = [e["observed_s"] - e["requested_s"] for e in injections]
            cancel = [
                c["latency_s"] for r in rows for c in r["cancellations"] if c["loser"]
            ]
            restore = [
                e["restore_init_s"]
                for r in rows
                for e in r["restorations"]
                if e["restored_epoch"] > 0
            ]
            uncovered = sum(not r["fault_coverage_complete"] for r in rows)
            lines.extend(
                [
                    "",
                    f"Runs finishing before all requested faults: {uncovered}/{len(rows)}. "
                    "These runs cannot demonstrate recovery from those uninjected faults.",
                    f"Fault delay maximum: {max(delays, default=0):.4f}s. "
                    f"Median losing-copy cancellation acknowledgment: {statistics.median(cancel) if cancel else 0:.4f}s. "
                    f"Median restore plus initialization: {statistics.median(restore) if restore else 0:.4f}s.",
                ]
            )
            fig, axes = plt.subplots(1, 2, figsize=(12, 4))
            selected = [
                (key, values)
                for key, values in sorted(by_policy.items())
                if key[1]
                in (
                    "round_robin_retry",
                    "checkpoint_retry",
                    "fixed_placement_replication",
                    "joint_enumeration",
                )
            ]
            labels = [f"{key[0]} / {key[1]}" for key, _ in selected]
            axes[0].barh(
                labels,
                [
                    statistics.median(r["elapsed_s"] for r in values)
                    for _, values in selected
                ],
            )
            axes[1].barh(
                labels,
                [
                    statistics.median(r["worker_busy_s"] for r in values)
                    for _, values in selected
                ],
            )
            axes[0].set_xlabel("Median workflow seconds (selected scenarios)")
            axes[1].set_xlabel("Median summed worker-busy seconds")
            fig.tight_layout()
            fig.savefig(folder / "measured-comparison.png", dpi=160)
            plt.close(fig)
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
                "input-order history. Final workflow accuracies and the legacy trial selection are recorded. "
                "Events record PID/exit code, requested and actual fault time, "
                "checkpoint restoration, and one commit per trial.",
            ]
        )
    lines.extend(
        [
            "",
            "This restricted workload cannot establish that selective recovery is ineffective. "
            "Compare joint synthesis with both conventional scheduling and optimized checkpoint retry, "
            "and use the fixed-primary ablation to isolate replication. Constructed controls validate "
            "the mechanism only; workload benefit and solver scalability are separate questions.",
            "",
        ]
    )
    (folder / "summary.md").write_text("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder")
    report(parser.parse_args().folder)
