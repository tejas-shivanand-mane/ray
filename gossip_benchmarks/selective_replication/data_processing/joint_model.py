"""Joint placement/checkpoint model and bounded planners. No work on import.

This is a deliberately limited post-map node-loss model, not a cloud simulator.
Costs and times are integers; one node event loses all its unprotected outputs.
"""

import itertools
import json
import math
import time
from fractions import Fraction
from pathlib import Path


COST_DENOMINATOR = 3600 * 10**12  # microseconds * micro-USD/hour -> USD


def make_case(profile, config, risk, budget_factor):
    nodes = config["nodes"]
    if not 2 <= len(nodes) <= 4 or len({n["name"] for n in nodes}) != len(nodes):
        raise ValueError("Use 2..4 distinctly named nodes (all 2**M loss sets are evaluated)")
    if not any(n["kind"] == "regular" for n in nodes):
        raise ValueError("A regular node is required for the reference budget and controls")
    for node in nodes:
        if node["kind"] not in ("spot", "regular"):
            raise ValueError("Node kind must be spot or regular")
        if not 0 <= node["failure_probability"] <= 1 or node["restart_s"] < 0:
            raise ValueError("Invalid node probability or restart time")
        if any(node[k] <= 0 for k in ("speed", "usd_per_hour", "transfer_mib_s")):
            raise ValueError("Node speed, price and bandwidth must be positive")
    if not (0 <= config["storage_fraction"] <= 1 and
            0 <= config["minimum_deadline_success"] <= 1 and
            config["deadline_factor"] > 0 and config["checkpoint_mib_s"] > 0 and
            config["reducer_usd_per_hour"] >= 0 and budget_factor > 0 and
            risk["spot_multiplier"] >= 0):
        raise ValueError("Invalid budget, deadline, bandwidth or risk setting")
    probabilities = [round(1000 * min(1., n["failure_probability"] *
        (risk["spot_multiplier"] if n["kind"] == "spot" else 1.))) for n in nodes]
    scenarios = []
    for mask in range(1 << len(nodes)):
        weight = math.prod(p if mask & (1 << j) else 1000 - p
                           for j, p in enumerate(probabilities))
        if weight:
            scenarios.append({"lost_nodes": mask, "weight": weight})
    divisor = math.gcd(*(s["weight"] for s in scenarios))
    for scenario in scenarios:
        scenario["weight"] //= divisor
    options = []
    for task in profile["tasks"]:
        choices = []
        for j, node in enumerate(nodes):
            transfer = math.ceil(task["bytes"] / (node["transfer_mib_s"] * 2**20) * 1e6)
            input_transfer = math.ceil(task.get("input_bytes", 0) / (node["transfer_mib_s"] * 2**20) * 1e6)
            compute = math.ceil(task["compute_us"] / node["speed"])
            durable = math.ceil(task["bytes"] / (config["checkpoint_mib_s"] * 2**20) * 1e6)
            for checkpoint in (False, True):
                choices.append({"node": j, "checkpoint": checkpoint,
                    "bytes": task["bytes"] if checkpoint else 0,
                    "transfer_us": transfer,
                    "map_us": compute + input_transfer + (task["write_us"] + durable if checkpoint else 0),
                    "recover_us": task["read_us"] + durable if checkpoint else compute + input_transfer})
        options.append(choices)
    case = {"nodes": nodes, "probabilities_per_thousand": probabilities,
        "options": options, "scenarios": scenarios, "weight_total": sum(s["weight"] for s in scenarios),
        "restart_us": [round(n["restart_s"] * 1e6) for n in nodes],
        "rates": [max(1, round(n["usd_per_hour"] * 1e6)) for n in nodes],
        "reducer_rate": round(config["reducer_usd_per_hour"] * 1e6),
        "reduce_us": profile["reduce_us"],
        "storage_budget": int(sum(t["bytes"] for t in profile["tasks"]) * config["storage_fraction"]),
        "deadline_us": 10**30, "required_success_weight": 0, "cost_budget": 10**60}
    regular = min((j for j, n in enumerate(nodes) if n["kind"] == "regular"),
                  key=lambda j: (-nodes[j]["speed"], nodes[j]["usd_per_hour"], j))
    reference = [2 * regular] * len(options)
    metrics = evaluate(case, reference)
    case.update({"reference_plan": reference, "risk_profile": risk["name"],
        "budget_factor": budget_factor,
        "cost_budget": math.ceil(metrics["cost_score"] * Fraction(str(budget_factor))),
        "deadline_us": math.ceil(metrics["healthy_us"] * Fraction(str(config["deadline_factor"]))),
        "required_success_weight": math.ceil(Fraction(str(config["minimum_deadline_success"])) * case["weight_total"])})
    return case


def state_for(case, plan):
    m = len(case["nodes"])
    loads, recovery = [0] * m, [0] * m
    size, active, transfer = 0, 0, 0
    for options, choice in zip(case["options"], plan):
        option = options[choice]
        j = option["node"]
        active |= 1 << j
        size += option["bytes"]
        transfer += option["transfer_us"]
        loads[j] += option["map_us"]
        recovery[j] += option["recover_us"]
    return (active, size, transfer, *loads, *recovery)


def evaluate_state(case, state):
    m = len(case["nodes"])
    active, size, transfer = state[:3]
    loads, recovery = state[3:3 + m], state[3 + m:]
    healthy = max(loads, default=0) + transfer + case["reduce_us"]
    rate = case["reducer_rate"] + sum(r for j, r in enumerate(case["rates"]) if active & (1 << j))
    expected, success, durations = 0, 0, []
    for scenario in case["scenarios"]:
        lost = scenario["lost_nodes"] & active
        extra = max((case["restart_us"][j] + recovery[j]
                     for j in range(m) if lost & (1 << j)), default=0)
        duration = healthy + extra
        expected += scenario["weight"] * duration
        success += scenario["weight"] * (duration <= case["deadline_us"])
        durations.append(duration)
    cost = expected * rate
    feasible = (size <= case["storage_budget"] and cost <= case["cost_budget"] and
                success >= case["required_success_weight"])
    return {"feasible": feasible, "objective": expected, "cost_score": cost,
        "bytes": size, "active_nodes": active, "healthy_us": healthy,
        "scenario_us": durations, "success_weight": success,
        "expected_s": expected / case["weight_total"] / 1e6,
        "expected_usd": cost / case["weight_total"] / COST_DENOMINATOR,
        "deadline_success": success / case["weight_total"]}


def evaluate(case, plan):
    if len(plan) != len(case["options"]) or any(
            type(x) is not int or not 0 <= x < len(options)
            for x, options in zip(plan, case["options"])):
        raise ValueError("Invalid joint plan")
    return evaluate_state(case, state_for(case, plan))


def rank(case, plan):
    value = evaluate(case, plan)
    violations = (max(0, value["bytes"] - case["storage_budget"]) / max(1, case["storage_budget"]) +
        max(0, value["cost_score"] - case["cost_budget"]) / max(1, case["cost_budget"]) +
        max(0, case["required_success_weight"] - value["success_weight"]) / case["weight_total"])
    return (violations, value["objective"], value["cost_score"], value["bytes"], tuple(plan))


def seeds(case, protected=True):
    n, m = len(case["options"]), len(case["nodes"])
    return [[2 * j + cp for _ in range(n)] for j in range(m)
            for cp in ((0, 1) if protected else (0,))] + [
            [2 * (i % m) for i in range(n)], list(case["reference_plan"])]


class Limit(Exception):
    pass


def check_time(deadline):
    if time.perf_counter() >= deadline:
        raise Limit("timeout")


def greedy(case, deadline, swaps=False, placement_only=False):
    candidates = seeds(case, protected=not placement_only)
    best = min(candidates, key=lambda p: rank(case, p))
    best_rank = rank(case, best)
    status = "heuristic"
    try:
        for seed in candidates:
            current, score = list(seed), rank(case, seed)
            while True:
                changed = False
                for i, options in enumerate(case["options"]):
                    for choice in range(0, len(options), 2 if placement_only else 1):
                        check_time(deadline)
                        trial = current[:]
                        trial[i] = choice
                        value = rank(case, trial)
                        if value < score:
                            current, score, changed = trial, value, True
                            if score < best_rank:
                                best, best_rank = current[:], score
                if not changed:
                    break
            if swaps:
                # Exchanges complete node/checkpoint assignments between two tasks.
                # This is a bounded local heuristic, not a published algorithm.
                improved = True
                while improved:
                    improved = False
                    for i in range(len(current)):
                        for j in range(i):
                            check_time(deadline)
                            trial = current[:]
                            trial[i], trial[j] = trial[j], trial[i]
                            value = rank(case, trial)
                            if value < score:
                                current, score, improved = trial, value, True
                                if score < best_rank:
                                    best, best_rank = current[:], score
    except Limit:
        status = "timeout"
    return {"plan": best, "status": status, "optimality_proven": False}


def exact_dp(case, deadline, max_states, placement=None):
    """Exact multidimensional frontier DP; interrupted results are NOT exact.

    Same active-node mask and componentwise smaller storage/map/recovery loads
    dominate a state: every possible continuation has no higher time/cost and
    no lower deadline success. No approximate rounding or beam pruning.
    """
    m = len(case["nodes"])
    initial = (0,) * (3 + 2 * m)
    frontier = {initial: ()}
    best = min(seeds(case) if placement is None else [placement, [x | 1 for x in placement]],
               key=lambda p: rank(case, p))
    peak, status = 1, "optimal"
    try:
        for i, options in enumerate(case["options"]):
            candidates = {}
            choices = range(len(options)) if placement is None else (placement[i] & ~1, placement[i] | 1)
            for state, plan in frontier.items():
                for choice in choices:
                    check_time(deadline)
                    option, new = options[choice], list(state)
                    j = option["node"]
                    new[0] |= 1 << j
                    new[1] += option["bytes"]
                    new[2] += option["transfer_us"]
                    new[3 + j] += option["map_us"]
                    new[3 + m + j] += option["recover_us"]
                    if new[1] > case["storage_budget"]:
                        continue
                    new, path = tuple(new), plan + (choice,)
                    if new not in candidates or path < candidates[new]:
                        candidates[new] = path
                    if len(candidates) > max_states:
                        raise Limit("state_limit")
            peak = max(peak, len(candidates))
            frontier, groups = {}, {}
            for state in sorted(candidates, key=lambda s: (sum(s[1:]), s)):
                check_time(deadline)
                group = groups.setdefault(state[0], [])
                dominated = False
                for index, other in enumerate(group):
                    if index % 64 == 0:
                        check_time(deadline)
                    if all(a <= b for a, b in zip(other[1:], state[1:])):
                        dominated = True
                        break
                if not dominated:
                    group.append(state)
                    frontier[state] = candidates[state]
            if not frontier:
                return {"plan": None, "status": "infeasible", "optimality_proven": True,
                        "peak_states": peak}
        feasible = []
        for state, plan in frontier.items():
            check_time(deadline)
            value = evaluate_state(case, state)
            if value["feasible"]:
                feasible.append((value["objective"], value["cost_score"], value["bytes"], plan))
        if not feasible:
            return {"plan": None, "status": "infeasible", "optimality_proven": True,
                    "peak_states": peak}
        best = list(min(feasible)[-1])
    except Limit as error:
        status = str(error)
        # Common initial incumbent only; never call an unfinished frontier exact.
    return {"plan": best, "status": status, "optimality_proven": status == "optimal",
            "peak_states": peak}


def solve_z3(case, deadline, smt_path):
    import z3
    solver = z3.Solver()
    variables = [[z3.Bool("task_%d_choice_%d" % (i, k)) for k in range(len(options))]
                 for i, options in enumerate(case["options"])]
    for row in variables:
        solver.add(z3.PbEq([(x, 1) for x in row], 1))
    m = len(case["nodes"])
    used, loads, recovery = [], [], []
    for j in range(m):
        used.append(z3.Or([variables[i][k] for i, opts in enumerate(case["options"])
                          for k, o in enumerate(opts) if o["node"] == j]))
        for key, target in (("map_us", loads), ("recover_us", recovery)):
            target.append(z3.Sum([z3.If(variables[i][k], o[key], 0)
                for i, opts in enumerate(case["options"]) for k, o in enumerate(opts) if o["node"] == j]))
    size = z3.Sum([z3.If(variables[i][k], o["bytes"], 0)
                   for i, opts in enumerate(case["options"]) for k, o in enumerate(opts)])
    transfer = z3.Sum([z3.If(variables[i][k], o["transfer_us"], 0)
                      for i, opts in enumerate(case["options"]) for k, o in enumerate(opts)])

    def maximum(values):
        result = z3.IntVal(0)
        for value in values:
            result = z3.If(value > result, value, result)
        return result

    healthy = maximum(loads) + transfer + case["reduce_us"]
    durations = [healthy + maximum([z3.If(used[j], case["restart_us"][j] + recovery[j], 0)
        for j in range(m) if s["lost_nodes"] & (1 << j)]) for s in case["scenarios"]]
    objective = z3.Sum([s["weight"] * duration for s, duration in zip(case["scenarios"], durations)])
    cost = objective * case["reducer_rate"] + z3.Sum([
        z3.If(used[j], objective * case["rates"][j], 0) for j in range(m)])
    success = z3.Sum([z3.If(duration <= case["deadline_us"], s["weight"], 0)
                     for s, duration in zip(case["scenarios"], durations)])
    solver.add(size <= case["storage_budget"], cost <= case["cost_budget"],
               success >= case["required_success_weight"])
    Path(smt_path).write_text(solver.to_smt2())
    candidates = [p for p in seeds(case) if evaluate(case, p)["feasible"]]
    best = min(candidates, key=lambda p: rank(case, p)) if candidates else None
    lower, upper = 0, evaluate(case, best)["objective"] if best is not None else None
    trace, reason, status = [], None, "timeout"

    def query(bound=None):
        check_time(deadline)
        solver.push()
        if bound is not None:
            solver.add(objective <= bound)
        solver.set(timeout=max(1, int((deadline - time.perf_counter()) * 1000)))
        result = solver.check()
        plan, unknown = None, None
        if result == z3.sat:
            model = solver.model()
            plan = [next(k for k, x in enumerate(row) if z3.is_true(model.eval(x))) for row in variables]
            value = evaluate(case, plan)
            encoded = model.eval(objective).as_long()
            encoded_scenarios = [model.eval(t).as_long() for t in durations]
            if not value["feasible"] or encoded != value["objective"] or encoded_scenarios != value["scenario_us"]:
                Path(smt_path).with_suffix(".error.json").write_text(json.dumps({
                    "plan": plan, "evaluated": value, "encoded_objective": encoded,
                    "encoded_scenario_us": encoded_scenarios, "bound": bound}, indent=2))
                raise RuntimeError("Z3 model disagrees with independent evaluator")
        elif result == z3.unknown:
            unknown = solver.reason_unknown()
        trace.append({"upper_bound": bound, "status": str(result), "reason": unknown})
        solver.pop()
        return result, plan, unknown

    try:
        if best is None:
            result, best, reason = query()
            if result == z3.unsat:
                status = "infeasible"
            elif result == z3.sat:
                upper = evaluate(case, best)["objective"]
        if best is not None:
            while lower < upper:
                bound = (lower + upper) // 2
                result, plan, reason = query(bound)
                if result == z3.sat:
                    best, upper = plan, evaluate(case, plan)["objective"]
                elif result == z3.unsat:
                    lower = bound + 1
                else:
                    break
            if lower == upper:
                status = "optimal"
    except Limit:
        pass
    return {"plan": best, "status": status, "optimality_proven": status in ("optimal", "infeasible"),
        "lower_bound": lower, "upper_bound": upper, "queries": trace, "unknown_reason": reason,
        "decision_booleans": sum(map(len, variables))}


def exhaustive(case, deadline):
    """Independent small-case audit only, never a seed for other planners."""
    best, score = None, None
    for plan in itertools.product(*(range(len(o)) for o in case["options"])):
        check_time(deadline)
        value = evaluate(case, plan)
        if value["feasible"]:
            candidate = (value["objective"], value["cost_score"], value["bytes"], plan)
            if score is None or candidate < score:
                best, score = list(plan), candidate
    return best
