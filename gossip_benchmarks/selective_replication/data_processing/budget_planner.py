"""Budgeted checkpoint selection with a symbolic Z3 model and independent oracle.

No workload runs on import. All costs come from the user's measured profiles.
Optimality refers to the fixed DAG/order/failure model, not real execution time.
"""

import itertools
import json
import time


FRACTIONS = (0, .001, .01, .03, .1, .3, 1)
PROBABILITIES = (10, 100, 300)  # per thousand, not observed failure frequencies


def fault_points(stages, fractions):
    """Map useful-compute progress to a task and fractional lost computation."""
    total = sum(s["compute_us"] for s in stages[:-1])
    points = []
    for fraction in fractions:
        target, prefix = fraction * total, 0
        for index, stage in enumerate(stages[:-1]):
            if prefix + stage["compute_us"] >= target:
                points.append({"stage": index, "fraction": (target - prefix) / stage["compute_us"],
                               "progress_fraction": fraction})
                break
            prefix += stage["compute_us"]
    return points


def simulate(stages, mask, fault=None):
    """Independent imperative replay, with one cached value per task per worker."""
    n = len(stages)
    cache, durable = set(), set()
    total = 0

    def selected(i):
        return i == n - 1 or bool(mask & (1 << i))

    def ensure(i):
        nonlocal total
        if i in cache:
            return
        s = stages[i]
        if i in durable:
            total += s["read_us"]
        else:
            for parent in s["deps"]:
                ensure(parent)
            total += s["compute_us"]
            if selected(i):
                total += s["write_us"]
                durable.add(i)
        cache.add(i)

    for i in range(n):
        if fault is not None and i == fault["stage"]:
            total += round(stages[i]["compute_us"] * fault["fraction"])
            cache.clear()
        ensure(i)
    return total


def make_table(stages, faults, probability):
    """Oracle scores are cached once; Z3 encodes dependencies independently."""
    table = {}
    for mask in range(1 << (len(stages) - 1)):
        size = sum(s["bytes"] for i, s in enumerate(stages[:-1]) if mask & (1 << i))
        objective = ((1000 - probability) * len(faults) * simulate(stages, mask)
                     + probability * sum(simulate(stages, mask, f) for f in faults))
        table[mask] = (objective, size, mask)
    return table


class LazyScores(dict):
    """Greedy pays to evaluate only the plans it actually visits.

    The exhaustive table is only a separate correctness oracle/discovery aid;
    it must not make the reported greedy planning cost artificially cheap.
    """

    def __init__(self, stages, faults, probability):
        super().__init__()
        self.stages, self.faults, self.probability = stages, faults, probability

    def __missing__(self, mask):
        stages, faults, probability = self.stages, self.faults, self.probability
        size = sum(s["bytes"] for i, s in enumerate(stages[:-1]) if mask & (1 << i))
        objective = ((1000 - probability) * len(faults) * simulate(stages, mask)
                     + probability * sum(simulate(stages, mask, f) for f in faults))
        self[mask] = (objective, size, mask)
        return self[mask]

    def get(self, key, default=None):
        return self[key]


def greedy(table, budget, n, swaps=False):
    def feasible(mask):
        return table[mask][1] <= budget

    def add(density):
        mask = 0
        while True:
            candidates = [mask | (1 << i) for i in range(n) if not mask & (1 << i)]
            candidates = [m for m in candidates if feasible(m) and table[m][0] < table[mask][0]]
            if not candidates:
                return mask
            if density:
                mask = max(candidates, key=lambda m: ((table[mask][0] - table[m][0]) /
                           max(1, table[m][1] - table[mask][1]), -table[m][0], -m))
            else:
                mask = min(candidates, key=table.get)

    singleton = min([0] + [1 << i for i in range(n) if feasible(1 << i)], key=table.get)
    seeds = {0, singleton, add(False), add(True)}
    if swaps:
        # Do not let a different local-search path make the stronger baseline
        # worse than the ordinary greedy result under the identical model.
        seeds.add(greedy(table, budget, n, swaps=False))
    if feasible((1 << n) - 1):
        seeds.add((1 << n) - 1)
    solutions = []
    for seed in sorted(seeds):
        mask = seed
        while True:
            neighbors = {mask ^ (1 << i) for i in range(n)}
            if swaps:
                neighbors.update(mask ^ (1 << i) ^ (1 << j)
                                 for i in range(n) if mask & (1 << i)
                                 for j in range(n) if not mask & (1 << j))
            best = min([mask] + [m for m in neighbors if feasible(m)], key=table.get)
            if table[best] >= table[mask]:
                break
            mask = best
        solutions.append(mask)
    return min(solutions, key=table.get)


def solve_z3(stages, faults, probability, budget, timeout_ms, smt_path):
    import z3
    began = time.perf_counter()
    n = len(stages)
    x = [z3.Bool("save_%d" % i) for i in range(n - 1)] + [z3.BoolVal(True)]
    solver = z3.Solver()
    size = z3.Sum([z3.If(x[i], stages[i]["bytes"], 0) for i in range(n - 1)])
    solver.add(size <= budget)
    healthy = z3.Sum([s["compute_us"] + z3.If(x[i], s["write_us"], 0)
                      for i, s in enumerate(stages)])
    failed = []
    children = [[j for j, s in enumerate(stages) if i in s["deps"]] for i in range(n)]
    for fault in faults:
        f = fault["stage"] - 1
        computes = [None] * n
        terms = []
        for i in reversed(range(n)):
            needed = z3.Or(z3.BoolVal(i > f), *[computes[j] for j in children[i]])
            available = z3.And(x[i], z3.BoolVal(i <= f))
            computes[i] = z3.And(needed, z3.Not(available))
            s = stages[i]
            terms.append(z3.If(computes[i], s["compute_us"] + z3.If(x[i], s["write_us"], 0),
                               z3.If(z3.And(needed, available), s["read_us"], 0)))
        prefix = z3.Sum([stages[i]["compute_us"] + z3.If(x[i], stages[i]["write_us"], 0)
                         for i in range(f + 1)])
        lost = round(stages[fault["stage"]]["compute_us"] * fault["fraction"])
        failed.append(prefix + lost + z3.Sum(terms))
    objective = (1000 - probability) * len(faults) * healthy + probability * z3.Sum(failed)
    mask_expr = z3.Sum([z3.If(x[i], 1 << i, 0) for i in range(n - 1)])
    # Name the objectives so the exported SAT queries are directly inspectable.
    objectives = []
    for name, expression in (("objective", objective), ("bytes", size), ("mask", mask_expr)):
        variable = z3.Int("plan_" + name)
        solver.add(variable == expression)
        objectives.append((name, variable))
    smt_path.write_text(solver.to_smt2())
    result = {"status": "unknown", "optimality_proven": False, "mask": None,
              "method": "integer_binary_search_with_strict_improvement_checks",
              "queries": [], "certificates": {}}
    deadline = began + timeout_ms / 1000

    def check(target, label, bound=None):
        remaining = int((deadline - time.perf_counter()) * 1000)
        if remaining <= 0:
            result["reason"] = "Per-case solver time budget exhausted"
            return z3.unknown
        target.set(timeout=remaining)
        status = target.check()
        result["queries"].append({"label": label, "bound": bound, "status": str(status)})
        if status == z3.unknown:
            result["reason"] = target.reason_unknown()
        return status

    def remember(model):
        result.update({name: model.eval(expression, model_completion=True).as_long()
                       for name, expression in objectives})
        result["healthy_us"] = model.eval(healthy, model_completion=True).as_long()
        result["failed_us"] = [model.eval(term, model_completion=True).as_long() for term in failed]

    # Use ordinary SMT feasibility queries instead of trusting Optimize's
    # reported lower/upper bounds. All three integer objectives are nonnegative.
    status = check(solver, "initial_feasibility")
    if status == z3.sat:
        best = solver.model()
        remember(best)
        for name, expression in objectives:
            low, high = 0, best.eval(expression, model_completion=True).as_long()
            while low < high:
                middle = (low + high) // 2
                solver.push()
                solver.add(expression <= middle)
                status = check(solver, name + "_search", middle)
                if status == z3.sat:
                    best = solver.model()
                    high = best.eval(expression, model_completion=True).as_long()
                    remember(best)
                solver.pop()
                if status == z3.unsat:
                    low = middle + 1
                elif status != z3.sat:
                    break
            if status == z3.unknown:
                break
            # Rebuild a fresh solver for the strict-improvement check. Save the
            # exact query, the returned status, and the feasible witness value.
            verifier = z3.Solver()
            verifier.add(*solver.assertions())
            verifier.add(expression < high)
            smt_path.with_name(smt_path.stem + "-better-" + name + ".smt2").write_text(verifier.to_smt2())
            status = check(verifier, name + "_strict_improvement", high)
            result["certificates"][name] = {"value": high, "strict_improvement": str(status)}
            if status != z3.unsat:
                if status == z3.sat:
                    result["reason"] = "Strict-improvement check contradicted binary search"
                    result["counterexample"] = str(verifier.model())
                break
            solver.add(expression == high)
        else:
            # The retained witness satisfies all fixed lexicographic optima.
            result["status"] = "sat"
            result["optimality_proven"] = True
    elif status == z3.unsat:
        result["status"] = "unsat"
        result["reason"] = "No feasible checkpoint set"
    result["elapsed_s"] = time.perf_counter() - began
    # Persist before the caller's exhaustive audit, including timeout answers.
    smt_path.with_suffix(".json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return result


def study(workload, stages, directory, timeout_ms=5000):
    """Fixed grid + labeled discovery of storage tradeoffs on measured operators."""
    faults = fault_points(stages, (.15, .35, .55, .75, .95))
    evaluation = fault_points(stages, (.25, .50, .80))
    total_bytes = sum(s["bytes"] for s in stages[:-1])
    fixed = [(f, round(total_bytes * f)) for f in FRACTIONS]
    sizes = [s["bytes"] for s in stages[:-1]]
    # Scan real checkpoint-size breakpoints, not hand-written task benefits.
    discovery_budgets = sorted(set(sizes + [a + b for a, b in itertools.combinations(sizes, 2)]))
    cases, scan = [], []
    for probability in PROBABILITIES:
        table = make_table(stages, faults, probability)
        candidates = []
        for budget in discovery_budgets:
            best = min((m for m in table if table[m][1] <= budget), key=table.get)
            g = greedy(table, budget, len(sizes))
            swap = greedy(table, budget, len(sizes), swaps=True)
            gap = (table[swap][0] - table[best][0]) / max(1, table[swap][0])
            row = {"probability_per_thousand": probability, "budget_bytes": budget,
                   "greedy_mask": g, "swap_mask": swap, "oracle_mask": best,
                   "greedy_gap_fraction": (table[g][0] - table[best][0]) / max(1, table[g][0]),
                   "swap_gap_fraction": gap}
            scan.append(row)
            candidates.append(row)
        extras = sorted(candidates, key=lambda r: (-r["swap_gap_fraction"], -r["greedy_gap_fraction"], r["budget_bytes"]))[:1]
        jobs = [("fixed", f, b) for f, b in fixed]
        for r in extras:
            if r["greedy_gap_fraction"] > 0:
                jobs.append(("discovered_gap", None, r["budget_bytes"]))
        for origin, fraction, budget in jobs:
            case_id = "%s-p%d-%s-b%d" % (workload, probability, origin, budget)
            record = {"id": case_id, "origin": origin, "budget_fraction": fraction,
                      "workload": workload, "budget_bytes": budget,
                      "probability_per_thousand": probability, "planning_faults": faults,
                      "evaluation_faults": evaluation, "plans": {}}
            masks = {"recompute": 0, "checkpoint_all": (1 << len(sizes)) - 1}
            seconds = {"recompute": 0., "checkpoint_all": 0.}
            for name, swap in (("greedy", False), ("greedy_swap", True)):
                began = time.perf_counter()
                masks[name] = greedy(LazyScores(stages, faults, probability), budget, len(sizes), swaps=swap)
                seconds[name] = time.perf_counter() - began
            z = solve_z3(stages, faults, probability, budget, timeout_ms, directory / (case_id + ".smt2"))
            record["solver"] = z
            oracle = min((m for m in table if table[m][1] <= budget), key=table.get)
            record["oracle_mask"] = oracle
            if z["optimality_proven"]:
                returned_healthy = simulate(stages, z["mask"])
                returned_failed = [simulate(stages, z["mask"], f) for f in faults]
                if (z["objective"] != table[oracle][0] or table[z["mask"]] != table[oracle]
                        or z["bytes"] != table[z["mask"]][1] or z["bytes"] > budget
                        or z["healthy_us"] != returned_healthy or z["failed_us"] != returned_failed):
                    diagnostic = {"case": record, "stages": stages, "solver": z,
                        "oracle_score": table[oracle], "returned_mask_score": table[z["mask"]],
                        "returned_mask_healthy_us": returned_healthy,
                        "returned_mask_failed_us": returned_failed,
                        "feasible_scores": [table[m] for m in table if table[m][1] <= budget]}
                    diagnostic_path = directory / (case_id + ".audit.json")
                    diagnostic_path.write_text(json.dumps(diagnostic, indent=2, sort_keys=True) + "\n")
                    raise RuntimeError("Z3 and independent exhaustive replay disagree: " + case_id
                                       + "; full diagnostic: " + str(diagnostic_path))
                masks["z3"] = z["mask"]
                seconds["z3"] = z["elapsed_s"]
            for name, mask in masks.items():
                record["plans"][name] = {"mask": mask, "bytes": table[mask][1],
                    "feasible": table[mask][1] <= budget, "planning_s": seconds[name],
                    "selected": [s["name"] for i, s in enumerate(stages[:-1]) if mask & (1 << i)],
                    "modeled_expected_s": table[mask][0] / (1000 * len(faults) * 1e6),
                    "model_gap_fraction": (table[mask][0] - table[oracle][0]) / max(1, table[mask][0])}
            cases.append(record)
    # Predefined measurements: moderate budget and unconstrained, q=.1.
    # At most one extra case per workload, chosen BEFORE measured evaluations.
    for case in cases:
        case["measure"] = (case["origin"] == "fixed" and case["probability_per_thousand"] == 100
                           and case["budget_fraction"] in (.03, 1))
    gaps = [c for c in cases if c["origin"] == "discovered_gap" and "z3" in c["plans"]]
    if gaps:
        chosen = max(gaps, key=lambda c: (c["plans"]["greedy_swap"]["model_gap_fraction"],
                                         c["plans"]["greedy"]["model_gap_fraction"]))
        chosen["measure"] = True
    return {"cases": cases, "discovery_scan": scan, "total_checkpoint_bytes": total_bytes,
            "evaluation_faults": evaluation}
