"""Prespecified confirmation of checkpoint planners; owner-run only.

No adaptive budget search. No work, downloads or solver imports on import.
"""

import csv
import gzip
import json
import math
import random
import statistics
import time

import benchmark as base
import budget_planner as planner
import real_workloads as workloads
import sharded_dedup


REPEATS = 7
PLAN_REPEATS = 3
COHORTS = ("replication", "heldout")
# The discovered budget ratio is frozen from the previous exploratory run.
TARGET_RATIO = 579970 / 901133
DESIGNS = (
    ("target", "reduce", TARGET_RATIO, 300),
    ("lower_budget", "reduce", .60, 300),
    ("lower_risk", "reduce", TARGET_RATIO, 100),
    ("progress_control", "progress", TARGET_RATIO, 300),
    ("unrestricted_control", "reduce", 1., 300),
)
METHODS = ("greedy", "greedy_swap", "knapsack_dp", "z3")
PRIMARY_BASELINES = ("greedy_swap", "knapsack_dp")


def protocol():
    return {"version": 1, "cohorts": list(COHORTS), "documents_per_cohort": 1500,
        "data_seed": 20261026, "order_seed": 271828, "repeats": REPEATS,
        "planning_repeats": PLAN_REPEATS, "designs": [list(d) for d in DESIGNS],
        "evaluation_fractions": [.2, .6, .9], "plan_reuse_jobs": 20,
        "primary_metric": "equal-case mean percent improvement in expected execution plus planning time",
        "primary_baselines": list(PRIMARY_BASELINES), "family_alpha": .05,
        "practical_margin_percent": 1., "stop_rule": "fixed seven blocks; never stop because of significance",
        "scope": "Two document samples of one local application, conditional on fixed profiles and assumed risks"}


def exact_knapsack(stages, faults, probability, budget):
    """Sparse Pareto-frontier DP for independent producers and one final join.

    Values are the independently evaluated singleton cost differences. This
    specialized baseline is valid only for this additive task/failure model.
    Neither the exhaustive table nor Z3's answer is used by this algorithm.
    """
    n = len(stages) - 1
    if any(s["deps"] for s in stages[:-1]) or stages[-1]["deps"] != list(range(n)):
        raise ValueError("Knapsack DP requires independent producers consumed by one final task")
    scores = planner.LazyScores(stages, faults, probability)
    origin = scores[0][0]
    frontier = {0: (0, 0)}  # bytes -> (objective difference, checkpoint mask)
    for i, stage in enumerate(stages[:-1]):
        delta = scores[1 << i][0] - origin
        if delta >= 0:
            continue
        candidates = dict(frontier)
        for size, (cost, mask) in frontier.items():
            added = size + stage["bytes"]
            if added <= budget:
                candidate = (cost + delta, mask | (1 << i))
                if added not in candidates or candidate < candidates[added]:
                    candidates[added] = candidate
        frontier, best_cost = {}, None
        for size in sorted(candidates):
            cost, mask = candidates[size]
            if best_cost is None or cost < best_cost:
                frontier[size] = (cost, mask)
                best_cost = cost
    return min((origin + cost, size, mask) for size, (cost, mask) in frontier.items())[2]


def prepare_cohorts(output, settings):
    path = output / "cohorts.json"
    if path.exists():
        cohorts = json.loads(path.read_text())
        for inputs in cohorts.values():
            for info in inputs.values():
                if workloads.sha_file(info["path"]) != info["sha256"]:
                    raise RuntimeError("Cohort input changed: " + info["path"])
        return cohorts
    source_settings = dict(settings, news_documents=3000)
    source = workloads.prepare(output / "inputs", source_settings)
    with gzip.open(source["news"]["path"], "rt", encoding="utf-8") as stream:
        records = json.load(stream)
    if len(records) != 3000 or len({r["id"] for r in records}) != 3000:
        raise RuntimeError("Confirmation requires 3000 distinct document records")
    cohorts = {}
    for index, name in enumerate(COHORTS):
        directory = output / "cohorts" / name
        directory.mkdir(parents=True, exist_ok=True)
        subset = records[index * 1500:(index + 1) * 1500]
        data_path = directory / "news.json.gz"
        with gzip.open(data_path, "wt", encoding="utf-8") as stream:
            json.dump(subset, stream)
        inputs = {"news": {"path": str(data_path), "sha256": workloads.sha_file(data_path),
                  "rows": len(subset), "document_ids": [r["id"] for r in subset],
                  "source": "Disjoint slices of seeded 20 Newsgroups shuffle"}}
        cohorts[name] = sharded_dedup.prepare(directory, inputs)
    base.atomic_json(path, cohorts)
    return cohorts


def plan_case(output, cohort, design, profile, settings):
    label, scope, fraction, probability = design
    stages = profile["stages"]
    case_id = cohort + "-" + label
    n = len(stages) - 1
    if scope == "reduce":
        faults = [{"stage": n, "fraction": f} for f in (.15, .35, .55, .75, .95)]
        evaluation = [{"stage": n, "fraction": f} for f in (.2, .6, .9)]
    else:
        faults = planner.fault_points(stages, (.15, .35, .55, .75, .95), include_final=True)
        evaluation = planner.fault_points(stages, (.2, .6, .9), include_final=True)
    budget = round(sum(s["bytes"] for s in stages[:-1]) * fraction)
    # Independent correctness audit, outside every reported planning timer.
    table = planner.make_table(stages, faults, probability)
    optimum = min(score for score in table.values() if score[1] <= budget)
    base.atomic_json(output / ("audit-" + case_id + ".json"), {
        "case": case_id, "budget_bytes": budget, "probability_per_thousand": probability,
        "planning_faults": faults, "oracle_score": optimum,
        "scores": [table[mask] for mask in sorted(table)]})
    origin = table[0][0]
    for mask, score in table.items():
        additive = origin + sum(table[1 << i][0] - origin for i in range(n) if mask & (1 << i))
        if additive != score[0]:
            raise RuntimeError("Cost model is not additive; knapsack baseline is invalid")
    observations = {name: [] for name in METHODS}
    rng = random.Random("confirmation-planning-" + case_id)
    for repeat in range(PLAN_REPEATS):
        order = list(METHODS)
        rng.shuffle(order)
        for name in order:
            if name == "z3":
                result = planner.solve_z3(stages, faults, probability, budget,
                    settings["solver_timeout_ms"], output / "models" / (case_id + "-%d.smt2" % repeat))
                if not result["optimality_proven"]:
                    raise RuntimeError("Confirmation Z3 solve unresolved: " + case_id + "; see models diagnostics")
                mask, elapsed = result["mask"], result["elapsed_s"]
                if (result["objective"], result["bytes"], mask) != optimum:
                    raise RuntimeError("Z3/exhaustive disagreement: " + case_id)
                if result["healthy_us"] != planner.simulate(stages, mask) or result["failed_us"] != [
                        planner.simulate(stages, mask, f) for f in faults]:
                    raise RuntimeError("Z3 scenario cost disagreement: " + case_id)
            else:
                began = time.perf_counter()
                if name == "knapsack_dp":
                    mask = exact_knapsack(stages, faults, probability, budget)
                else:
                    mask = planner.greedy(planner.LazyScores(stages, faults, probability), budget,
                                          n, swaps=name == "greedy_swap")
                elapsed = time.perf_counter() - began
            if table[mask][1] > budget or (name == "knapsack_dp" and table[mask] != optimum):
                raise RuntimeError("Planner correctness failure: " + case_id + ":" + name)
            observations[name].append({"mask": mask, "planning_s": elapsed})
    plans = {}
    for name in ("recompute", "checkpoint_all") + METHODS:
        if name in observations:
            masks = {r["mask"] for r in observations[name]}
            if len(masks) != 1:
                raise RuntimeError("Planner returned inconsistent choices for a frozen profile")
            mask = next(iter(masks))
            elapsed = statistics.median(r["planning_s"] for r in observations[name])
        else:
            mask, elapsed = (0 if name == "recompute" else (1 << n) - 1), 0.
        plans[name] = {"mask": mask, "bytes": table[mask][1], "feasible": table[mask][1] <= budget,
            "planning_s": elapsed, "planning_observations": observations.get(name, []),
            "selected": [s["name"] for i, s in enumerate(stages[:-1]) if mask & (1 << i)],
            "modeled_expected_s": table[mask][0] / (1000 * len(faults) * 1e6)}
    return {"id": case_id, "cohort": cohort, "label": label, "scope": scope,
            "budget_fraction": fraction, "budget_bytes": budget, "probability_per_thousand": probability,
            "evaluation_faults": evaluation, "planning_faults": faults, "plans": plans,
            "oracle_score": optimum, "profile_s": profile["elapsed_s"]}


def load_cases(output):
    return [json.loads(path.read_text()) for path in sorted(output.glob("plan-*.json"))]


def requirements(cases):
    jobs = {}
    for case in cases:
        for plan in case["plans"].values():
            if plan["feasible"]:
                key = (case["cohort"], case["scope"], plan["mask"])
                if key not in jobs:
                    jobs[key] = {"budget": case["budget_bytes"], "faults": case["evaluation_faults"]}
                elif jobs[key]["faults"] != case["evaluation_faults"]:
                    raise RuntimeError("Cannot share replay observations across different fault locations")
                jobs[key]["budget"] = min(jobs[key]["budget"], case["budget_bytes"])
    return jobs


def run_controller(output, settings):
    import comprehensive as bench
    cohorts = prepare_cohorts(output, settings)
    (output / "models").mkdir(exist_ok=True)
    profiles = {}
    for cohort in COHORTS:
        path = output / ("profile-" + cohort + ".json")
        if not path.exists():
            print("Profiling confirmation cohort: " + cohort, flush=True)
            base.atomic_json(path, bench.measure_profile(output, "news_dedup_reduce", cohorts[cohort], settings))
        profiles[cohort] = json.loads(path.read_text())
        for design in DESIGNS:
            path = output / ("plan-" + cohort + "-" + design[0] + ".json")
            if not path.exists():
                print("Planning prespecified case: " + path.stem, flush=True)
                base.atomic_json(path, plan_case(output, cohort, design, profiles[cohort], settings))
    cases = load_cases(output)
    jobs = requirements(cases)
    previous = base.read_rows(output / "runs.jsonl")
    if any(not r.get("valid") for r in previous):
        raise RuntimeError("Prior replay failed correctness; do not silently repeat it")
    done = {(r["cohort"], r["scope"], r["mask"], r["repeat"], r["scenario"]) for r in previous}
    report(output, settings, "running")
    # Each block covers all cases. Shared plans have shared observations; risk
    # mixtures within a block are never treated as independent repetitions.
    for repeat in range(REPEATS):
        order = [(key, scenario) for key in sorted(jobs) for scenario in range(4)]
        random.Random(271828 + repeat).shuffle(order)
        for (cohort, scope, mask), scenario in order:
            key = (cohort, scope, mask, repeat, scenario)
            if key in done:
                continue
            job = jobs[cohort, scope, mask]
            fault = None if scenario == 0 else job["faults"][scenario - 1]
            row = bench.execute(output, "news_dedup_" + scope, cohorts[cohort], settings,
                mask, job["budget"], fault=fault, profile=profiles[cohort]["stages"])
            row.update(cohort=cohort, scope=scope, repeat=repeat, scenario=scenario,
                       valid=row["result"] == profiles[cohort]["reference"])
            base.append_json(output / "runs.jsonl", row)
            if not row["valid"]:
                raise RuntimeError("Output mismatch: " + str(key))
            done.add(key)
            print("Confirmation replay %d/%d: %s %.3fs" %
                  (len(done), len(jobs) * 4 * REPEATS, key, row["elapsed_s"]), flush=True)
        report(output, settings, "running")


def interval(values, alpha=.05):
    from scipy.stats import t
    if len(values) < 2:
        return None
    mean = statistics.mean(values)
    half = float(t.ppf(1 - alpha / 2, len(values) - 1)) * statistics.stdev(values) / math.sqrt(len(values))
    return [mean - half, mean + half]


def report(output, settings, status, error=None):
    import comprehensive as bench
    cases = load_cases(output)
    rows = base.read_rows(output / "runs.jsonl")
    indexed = {(r["cohort"], r["scope"], r["mask"], r["repeat"], r["scenario"]): r
               for r in rows if r["valid"]}
    aggregates, comparisons, measurements = [], [], {}
    for case in cases:
        feasible = {name: plan for name, plan in case["plans"].items() if plan["feasible"]}
        repeats = [r for r in range(REPEATS) if all(
            (case["cohort"], case["scope"], p["mask"], r, s) in indexed
            for p in feasible.values() for s in range(4))]
        for name, plan in feasible.items():
            measurements[case["id"], name] = {}
            for repeat in repeats:
                def row(scenario):
                    return indexed[case["cohort"], case["scope"], plan["mask"], repeat, scenario]
                healthy = row(0)["elapsed_s"]
                failure = statistics.mean(row(s)["elapsed_s"] for s in (1, 2, 3))
                q = case["probability_per_thousand"] / 1000
                expected = (1 - q) * healthy + q * failure
                measurements[case["id"], name][repeat] = {
                    "healthy_s": healthy, "failure_s": failure, "execution_s": expected,
                    "one_job_s": expected + plan["planning_s"],
                    "reuse_20_s": expected + plan["planning_s"] / 20}
            if repeats:
                values = measurements[case["id"], name]
                means = {metric: statistics.mean(values[r][metric] for r in repeats)
                         for metric in values[repeats[0]]}
                aggregates.append({"case": case["id"], "cohort": case["cohort"], "scope": case["scope"],
                    "policy": name, "n": len(repeats), "mask": plan["mask"], "bytes": plan["bytes"],
                    "planning_s": plan["planning_s"], **means,
                    "one_job_with_profile_s": means["one_job_s"] + (case["profile_s"] if name in METHODS else 0)})
        for baseline in ("recompute", "checkpoint_all", "greedy", "greedy_swap", "knapsack_dp"):
            if baseline not in feasible or not repeats:
                continue
            for metric in ("execution_s", "one_job_s", "reuse_20_s"):
                gains = [100 * (measurements[case["id"], baseline][r][metric] -
                               measurements[case["id"], "z3"][r][metric]) /
                               measurements[case["id"], baseline][r][metric] for r in repeats]
                comparisons.append({"case": case["id"], "baseline": baseline, "metric": metric,
                    "n": len(repeats), "gain_percent": statistics.mean(gains),
                    "descriptive_95_ci": interval(gains), "paired_gains_percent": gains,
                    "same_plan": feasible[baseline]["mask"] == feasible["z3"]["mask"]})
    primary = []
    for baseline in PRIMARY_BASELINES:
        complete = [r for r in range(REPEATS) if len(cases) == len(COHORTS) * len(DESIGNS)
                    and all(r in measurements.get((c["id"], name), {}) for c in cases for name in (baseline, "z3"))]
        gains = [statistics.mean(100 * (measurements[c["id"], baseline][r]["one_job_s"] -
                               measurements[c["id"], "z3"][r]["one_job_s"]) /
                               measurements[c["id"], baseline][r]["one_job_s"] for c in cases) for r in complete]
        bounds = interval(gains, alpha=.05 / len(PRIMARY_BASELINES))
        verdict = "incomplete"
        if len(complete) == REPEATS and status == "complete":
            verdict = "inconclusive"
            if bounds[0] > 0:
                verdict = "supports_z3_faster"
            elif bounds[1] < 0:
                verdict = "supports_baseline_faster"
        primary.append({"baseline": baseline, "metric": "one_job_s", "n_blocks": len(complete),
            "mean_gain_percent": statistics.mean(gains) if gains else None,
            "bonferroni_97_5_ci": bounds, "verdict": verdict,
            "practical_superiority_above_1_percent": verdict == "supports_z3_faster" and bounds[0] > 1,
            "paired_block_gains_percent": gains})
    result = {"status": status, "error": error, "protocol": settings["confirmation_protocol"],
        "unique_replays_completed": len(indexed), "unique_replays_planned": len(requirements(cases)) * 4 * REPEATS,
        "planned_cases_completed": len(cases), "primary": primary, "aggregates": aggregates,
        "comparisons": comparisons, "valid_rows": sum(r["valid"] for r in rows),
        "fault_injections": sum(bool(r.get("fault")) for r in rows),
        "fault_fallbacks": sum(bool(r.get("fault")) and r["fault"]["mode"] != "timed_during_task" for r in rows),
        "limits": "Intervals describe repeated timings conditional on fixed profiles/plans and observed planning medians. "
                  "Two samples are not independent applications. Reduce-only faults and risk probabilities are assumptions. "
                  "No adaptive case selection; per-case and amortized comparisons are descriptive, not primary tests."}
    base.atomic_json(output / "summary.json", result)
    base.atomic_json(output / "status.json", {k: result[k] for k in
        ("status", "error", "unique_replays_completed", "unique_replays_planned")})
    lines = ["# Z3 confirmation", "", "Status: %s; replays %d/%d." %
        (status, result["unique_replays_completed"], result["unique_replays_planned"]), "",
        "## Prespecified primary comparisons", "",
        "Expected execution + one planning decision; equal weight for all ten cases. Positive means Z3 is faster.", ""]
    for row in primary:
        lines.append("- {baseline}: {verdict}; gain {mean_gain_percent}%; corrected interval {bonferroni_97_5_ci}; "
                     "complete blocks {n_blocks}/7.".format(**row))
    lines += ["", "## Completion times", "",
        "Planning is the median of three independent planner calls on the same fixed profile. "
        "20-job reuse is an amortization estimate, not an executed batch. Common profiling is excluded below and "
        "included in JSON/CSV one_job_with_profile_s.", "",
        "| Case | Method | n | Healthy s | Failure s | Expected s | Plan s | Expected + plan s | Reuse 20 s/job |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in aggregates:
        lines.append("| {case} | {policy} | {n} | {healthy_s:.4f} | {failure_s:.4f} | {execution_s:.4f} | "
                     "{planning_s:.4f} | {one_job_s:.4f} | {reuse_20_s:.4f} |".format(**row))
    lines += ["", "## Execution-only Z3 comparisons (descriptive)", ""]
    for row in comparisons:
        if row["metric"] == "execution_s" and row["baseline"] in PRIMARY_BASELINES:
            lines.append("- {case} vs {baseline}: {gain_percent:.3f}%; 95% interval {descriptive_95_ci}; "
                         "identical plan: {same_plan}.".format(**row))
    lines += ["", result["limits"], "", "Failure injections: %d; timer fallbacks: %d." %
              (result["fault_injections"], result["fault_fallbacks"])]
    if error:
        lines += ["", "Error: " + error]
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    with (output / "aggregates.csv").open("w", newline="") as stream:
        if aggregates:
            writer = csv.DictWriter(stream, fieldnames=list(aggregates[0]))
            writer.writeheader()
            writer.writerows(aggregates)
    bench.archive_output(output)
    return result
