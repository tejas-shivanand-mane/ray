"""Visible experiment matrix and evidence gates (no cloud dependencies)."""

import copy
import random
import statistics
from collections import defaultdict

from planner import plan_key

POLICIES = [
    "round_robin_retry",
    "fixed_placement_replication",
    "checkpoint_retry",
    "uniform_replication",
    "joint_enumeration",
]


def config(epochs=2400, checkpoint_every=200, profile_repeats=2):
    task = dict(
        width=256,
        epochs=epochs,
        batch_size=32,
        seed=123,
        lr=0.03,
        checkpoint_every=checkpoint_every,
    )
    return dict(
        machines={m: dict(cost_per_hour=1.0) for m in ("safe", "spot_a", "spot_b")},
        tasks={
            "shard-a": dict(task, shard=[0, 2]),
            "shard-b": dict(task, shard=[1, 2]),
            "finetune": dict(
                task, epochs=2 * epochs, depends_on=["shard-a", "shard-b"]
            ),
        },
        final_tasks=["finetune"],
        profile_repeats=profile_repeats,
        profile_timeout_s=3600,
        failure_machines=["spot_a", "spot_b"],
        spot_cost_multipliers=[0.25, 1.0],
        failure_probabilities=[0.1, 0.5],
        deadline_multipliers=[1.25, 2.0],
        fault_time_fractions=[0.2, 0.7],
        success_target=0.95,
        accounting=dict(bill_warm_pool=True, shared_storage_per_hour=2.0),
    )


def jobs(search, repeats, smoke=False, policies=None, case_indices=None):
    """Choose cases BEFORE looking at measured outcomes; identical plans share runs."""
    if repeats < 1:
        raise ValueError("Need positive repetitions")
    selected = [0] if smoke else list(range(len(search["cases"])))
    if case_indices is not None:
        selected = list(case_indices)
    result = []
    for index in selected:
        case = search["cases"][index]
        unique = {}
        for policy in policies or POLICIES:
            row = case["policies"][policy] or case["unconstrained_policies"].get(policy)
            if row is None:
                raise ValueError(f"Missing policy {policy}")
            entry = unique.setdefault(
                plan_key(row["plan"]), dict(plan=row["plan"], policies=[])
            )
            entry["policies"].append(policy)
        scenarios = list(case["scenarios"])
        if smoke:
            scenarios = [
                s
                for s in scenarios
                if s["name"] in ("healthy", "loss-spot_a-t0.2", "correlated-loss-t0.2")
            ]
        else:
            for label, failed in [
                ("single", ["spot_a"]),
                ("correlated", ["spot_a", "spot_b"]),
            ]:
                scenarios.append(
                    dict(
                        name="holdout-" + label,
                        probability=None,
                        failures=dict.fromkeys(
                            failed, search["parallel_lower_bound_s"] * 0.45
                        ),
                    )
                )
        for entry in unique.values():
            for scenario in scenarios:
                for repeat in range(repeats):
                    result.append(
                        dict(
                            case_index=index,
                            case=case,
                            **copy.deepcopy(entry),
                            scenario=scenario,
                            repeat=repeat,
                        )
                    )
    random.Random(735).shuffle(result)
    return [dict(id=f"run-{i:04d}", **job) for i, job in enumerate(result)]


def smoke_jobs(search):
    # Explicit placement controls ensure both vulnerable VMs have useful work.
    # These are mechanism controls, not policies selected by the optimizer.
    case = search["cases"][0]
    plans = {
        "control_retry": {
            "order": ["shard-a", "shard-b", "finetune"],
            "copies": {
                "shard-a": ["spot_a"],
                "shard-b": ["spot_b"],
                "finetune": ["spot_a"],
            },
        },
        "control_replica": {
            "order": ["shard-a", "shard-b", "finetune"],
            "copies": {
                "shard-a": ["spot_a", "safe"],
                "shard-b": ["spot_b", "safe"],
                "finetune": ["spot_a", "safe"],
            },
        },
    }
    at = search["parallel_lower_bound_s"] * 0.06
    scenarios = [
        dict(name="control-healthy", failures={}, probability=None),
        dict(name="control-single", failures={"spot_a": at}, probability=None),
        dict(
            name="control-correlated",
            failures={"spot_a": at, "spot_b": at},
            probability=None,
        ),
    ]
    result = [
        dict(
            case_index=0,
            case=case,
            plan=plan,
            policies=[name],
            scenario=scenario,
            repeat=0,
        )
        for name, plan in plans.items()
        for scenario in scenarios
    ]
    random.Random(735).shuffle(result)
    return [dict(id=f"run-{i:04d}", **job) for i, job in enumerate(result)]


def assess(rows, expected, require_mechanisms=True):
    """Operational validity and comparative benefit are deliberately separate."""
    checks = dict(
        all_runs_present=len(rows) == expected and expected > 0,
        all_states_correct=bool(rows) and all(r["correct"] for r in rows),
        all_faults_injected=bool(rows)
        and all(r["fault_coverage_complete"] for r in rows),
        all_vm_stops_verified=all(
            e.get("status") == "TERMINATED" and e.get("instance_id")
            for r in rows
            for e in r["fault_evidence"].values()
        ),
        active_single_vm_loss=any(
            len(r["scenario"]["failures"]) == 1
            and any(
                e.get("active_task") or e.get("active_at_actor_loss")
                for e in r["fault_evidence"].values()
            )
            for r in rows
        ),
        active_correlated_vm_loss=any(
            len(r["scenario"]["failures"]) == 2
            and all(
                e.get("active_task") or e.get("active_at_actor_loss")
                for e in r["fault_evidence"].values()
            )
            and len(r["fault_evidence"]) == 2
            for r in rows
        ),
        checkpoint_restoration_observed=any(
            e.get("restored_epoch", 0) > 0 for r in rows for e in r["restorations"]
        ),
    )
    coverage = {
        name: checks[name]
        for name in (
            "active_single_vm_loss",
            "active_correlated_vm_loss",
            "checkpoint_restoration_observed",
        )
    }
    if not require_mechanisms:
        # A policy may legitimately keep all useful work off vulnerable machines.
        # Smoke controls establish the mechanism; do not force optimized policies
        # to incur failures just to obtain a favorable recovery narrative.
        for name in coverage:
            del checks[name]
    groups = defaultdict(list)
    for row in rows:
        for policy in row["policies"]:
            groups[(row["case_index"], row["scenario"]["name"], policy)].append(row)
    table = []
    for (case, scenario, policy), members in sorted(groups.items()):
        values = [r["elapsed_s"] for r in members]
        table.append(
            dict(
                case=case,
                scenario=scenario,
                policy=policy,
                n=len(members),
                median_s=statistics.median(values),
                min_s=min(values),
                max_s=max(values),
                deadline_hits=sum(r["met_deadline"] for r in members),
                injected_vm_losses=sum(len(r["fault_evidence"]) for r in members),
                active_vm_losses=sum(
                    bool(e.get("active_task") or e.get("active_at_actor_loss"))
                    for r in members
                    for e in r["fault_evidence"].values()
                ),
                mean_assumed_cost=statistics.mean(r["assumed_cost"] for r in members),
                median_worker_busy_s=statistics.median(
                    r["worker_busy_s"] for r in members
                ),
            )
        )
    return dict(
        valid=all(checks.values()),
        checks=checks,
        mechanism_coverage=coverage,
        comparisons=table,
        paired_comparisons=paired_comparisons(rows, bootstrap=len(rows) == expected),
        weighted_planning_results=weighted_results(rows),
        interpretation="Validity is not a speedup/cost-benefit verdict; compare matched scenarios and repeats.",
    )


def paired_comparisons(rows, bootstrap=True):
    indexed = {}
    for row in rows:
        for policy in row["policies"]:
            indexed[
                (row["case_index"], row["scenario"]["name"], row["repeat"], policy)
            ] = row
    comparisons = []
    rng = random.Random(441)
    for baseline, candidate in [
        ("round_robin_retry", "fixed_placement_replication"),
        ("checkpoint_retry", "joint_enumeration"),
        ("uniform_replication", "joint_enumeration"),
        ("control_retry", "control_replica"),
    ]:
        groups = defaultdict(list)
        for (case, scenario, repeat, policy), left in indexed.items():
            right = indexed.get((case, scenario, repeat, candidate))
            if policy == baseline and right is not None:
                groups[(case, scenario)].append((left, right))
        for (case, scenario), pairs in sorted(groups.items()):
            for metric in ("elapsed_s", "assumed_cost", "worker_busy_s"):
                deltas = [
                    100 * (right[metric] / left[metric] - 1)
                    for left, right in pairs
                    if left[metric] > 0
                ]
                mean = statistics.mean(deltas)
                interval = None
                if bootstrap and len(deltas) >= 3:
                    samples = sorted(
                        statistics.mean(rng.choices(deltas, k=len(deltas)))
                        for _ in range(2000)
                    )
                    interval = [samples[49], samples[1949]]
                comparisons.append(
                    dict(
                        case=case,
                        scenario=scenario,
                        baseline=baseline,
                        candidate=candidate,
                        metric=metric,
                        n=len(deltas),
                        mean_percent_change=mean,
                        bootstrap_95_percent=interval,
                    )
                )
    return comparisons


def weighted_results(rows):
    """Average within each scenario first; exclude held-out cases from weights."""
    groups = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if row["scenario"].get("probability") is not None:
            for policy in row["policies"]:
                groups[(row["case_index"], policy)][row["scenario"]["name"]].append(row)
    result = []
    for (case, policy), scenarios in sorted(groups.items()):
        total = sum(
            members[0]["scenario"]["probability"] for members in scenarios.values()
        )
        # Partial runs must not masquerade as a full expected-cost comparison.
        if abs(total - 1) > 1e-9:
            continue
        values = {}
        for metric in ("assumed_cost", "elapsed_s", "met_deadline"):
            values[metric] = sum(
                members[0]["scenario"]["probability"]
                * statistics.mean(r[metric] for r in members)
                for members in scenarios.values()
            )
        result.append(
            dict(
                case=case,
                policy=policy,
                scenario_count=len(scenarios),
                repeats_per_scenario={s: len(m) for s, m in scenarios.items()},
                **values,
            )
        )
    return result
