"""Owner-run synthetic placement/checkpoint study. Standard library; no Z3 calls.

Only planner wall time is measured. Everything about application execution is
modeled by joint_model's post-map loss evaluator. Nothing runs on import.
"""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import sys
import time
import zipfile

from joint_model import evaluate, make_case


REGIMES = (
    "homogeneous_low_risk", "independent", "cheap_risky", "fast_risky",
    "checkpoint_expensive",
)
METHODS = (
    "random_none", "random_uniform", "random_selective",
    "ranked_none", "ranked_uniform", "ranked_selective",
    "balanced_uniform", "balanced_selective",
)
COMPARISONS = (
    ("ranked_selective", "random_uniform"),
    ("ranked_uniform", "random_uniform"),
    ("random_selective", "random_uniform"),
    ("ranked_selective", "ranked_uniform"),
    ("ranked_selective", "ranked_none"),
    ("ranked_selective", "balanced_uniform"),
    ("ranked_selective", "balanced_selective"),
)
DEFAULTS = dict(cases=100, seed=20261027, random_repeats=10,
                weights=dict(time=1.0, risk=1.0, cost=1.0),
                bootstrap_repeats=1000)


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def fingerprints():
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in ("ranked_simulation.py", "joint_model.py")}


def generate_case(index, config):
    """Fixed distribution; never reject a case because a method loses."""
    rng = random.Random(config["seed"] + 100003 * index)
    regime = REGIMES[index % len(REGIMES)]
    count = rng.choice((20, 50, 100))
    nodes = []
    for j in range(3):
        speed = rng.uniform(0.6, 2.0)
        price = rng.uniform(0.04, 0.20)
        risk = rng.uniform(0.01, 0.30)
        if regime == "homogeneous_low_risk":
            speed, price, risk = 1.0, 0.10, 0.005
        elif regime == "cheap_risky":
            price = 0.20 - 0.50 * risk
        elif regime == "fast_risky":
            speed = 0.6 + 4.0 * risk
        nodes.append(dict(name="node_%d" % j, kind="regular",
                          speed=speed, usd_per_hour=price,
                          failure_probability=risk,
                          restart_s=5.0 if regime == "homogeneous_low_risk"
                          else rng.uniform(1.0, 15.0), transfer_mib_s=200.0))
    tasks = []
    for i in range(count):
        compute_s = math.exp(rng.uniform(math.log(0.2), math.log(20.0)))
        size = round(2**20 * math.exp(rng.uniform(math.log(0.1), math.log(32.0))))
        local_rate = 10.0 if regime == "checkpoint_expensive" else 200.0
        tasks.append(dict(name="task_%d" % i, compute_us=round(compute_s * 1e6),
                          bytes=size, input_bytes=size // 4,
                          write_us=round(size / (local_rate * 2**20) * 1e6),
                          read_us=round(size / (400.0 * 2**20) * 1e6)))
    assumptions = dict(nodes=nodes, storage_fraction=rng.choice((0.25, 0.50, 0.75)),
                       minimum_deadline_success=0.95, deadline_factor=2.0,
                       checkpoint_mib_s=200.0, reducer_usd_per_hour=0.10)
    profile = dict(tasks=tasks, reduce_us=1_000_000)
    case = make_case(profile, assumptions, dict(name=regime, spot_multiplier=1.0), 1.0)
    return dict(index=index, regime=regime, profile=profile,
                assumptions=assumptions, case=case)


def placement(case, kind, rng, weights):
    """Common longest-task-first order; scoring is a greedy heuristic."""
    n, m = len(case["options"]), len(case["nodes"])
    plan, loads = [0] * n, [0] * m
    active = set()
    reference = evaluate(case, case["reference_plan"])
    time_ref = reference["healthy_us"]
    cost_ref = reference["expected_usd"]
    order = sorted(range(n), key=lambda i: (-max(o["map_us"] for o in
                   case["options"][i] if not o["checkpoint"]), i))
    for i in order:
        def score(j):
            projected = loads[:]
            projected[j] += case["options"][i][2*j]["map_us"]
            if kind == "balanced":
                return projected[j]
            used = active | {j}
            healthy = max(projected)
            # Sum is an upper bound on expected maximum recovery delay.
            risk = sum(case["probabilities_per_thousand"][k] / 1000 *
                       (case["restart_us"][k] + projected[k]) for k in used)
            rate = case["reducer_rate"] + sum(case["rates"][k] for k in used)
            dollars = (healthy + risk) * rate / (3600 * 10**12)
            return (weights["time"] * healthy / time_ref +
                    weights["risk"] * risk / time_ref +
                    weights["cost"] * dollars / cost_ref)
        node = rng.randrange(m) if kind == "random" else min(range(m), key=lambda j: (score(j), j))
        plan[i] = 2 * node
        loads[node] += case["options"][i][2*node]["map_us"]
        active.add(node)
    return plan


def checkpoints(case, plan, policy, offset):
    result, used = plan[:], 0
    n = len(plan)
    if policy == "none":
        return result
    if policy == "uniform":
        # Spread decisions across submission indices. Only output size/budget
        # matter; compute costs, node risk and price are deliberately ignored.
        total = sum(case["options"][i][choice+1]["bytes"] for i, choice in enumerate(plan))
        fraction = case["storage_budget"] / total
        order = [i for i in range(n) if math.floor((i+1)*fraction + offset) >
                 math.floor(i*fraction + offset)]
    else:
        gains = []
        for i, choice in enumerate(plan):
            plain, protected = case["options"][i][choice:choice+2]
            probability = case["probabilities_per_thousand"][plain["node"]] / 1000
            saved = probability * (plain["recover_us"] - protected["recover_us"])
            overhead = protected["map_us"] - plain["map_us"]
            if saved > overhead:
                gains.append(((saved - overhead) / protected["bytes"], i))
        order = [i for _, i in sorted(gains, key=lambda x: (-x[0], x[1]))]
    for i in order:
        size = case["options"][i][plan[i]+1]["bytes"]
        if used + size <= case["storage_budget"]:
            result[i] += 1
            used += size
    return result


def observation(case, method, repeat, seed, weights, offset):
    kind, policy = method.split("_")
    start = time.perf_counter()
    plan = placement(case, kind, random.Random(seed), weights)
    plan = checkpoints(case, plan, policy, offset)
    planning_s = time.perf_counter() - start
    metrics = evaluate(case, plan)
    return dict(method=method, repeat=repeat, plan=plan, planning_s=planning_s,
                expected_plus_planning_s=metrics["expected_s"] + planning_s,
                metrics=metrics)


def execute_case(index, config):
    record = generate_case(index, config)
    case = record["case"]
    record["observations"] = []
    # Same random placements in all three random-policy arms. The uniform
    # offset is shared by all placement arms, and is frozen before evaluation.
    offset = random.Random(config["seed"] + index * 71).random()
    for method in METHODS:
        repeats = config["random_repeats"] if method.startswith("random_") else 1
        for repeat in range(repeats):
            seed = config["seed"] + index * 1009 + repeat * 1000003
            record["observations"].append(observation(case, method, repeat, seed,
                                                       config["weights"], offset))
    return record


def mean_metrics(record, method):
    rows = [r for r in record["observations"] if r["method"] == method]
    return dict(time=statistics.mean(r["metrics"]["expected_s"] for r in rows),
                cost=statistics.mean(r["metrics"]["expected_usd"] for r in rows),
                planning_inclusive=statistics.mean(r["expected_plus_planning_s"] for r in rows),
                feasible_fraction=statistics.mean(r["metrics"]["feasible"] for r in rows))


def interval(values, repeats, seed):
    if len(values) < 2:
        return None
    rng = random.Random(seed)
    means = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(repeats))
    return [means[int(0.025*(repeats-1))], means[int(0.975*(repeats-1))]]


def summarize(records, config):
    comparisons = []
    for selected, baseline in COMPARISONS:
        for regime in ("all", *REGIMES):
            rows = [r for r in records if regime == "all" or r["regime"] == regime]
            if not rows:
                continue
            pairs = [(mean_metrics(r, selected), mean_metrics(r, baseline)) for r in rows]
            for metric in ("time", "cost", "planning_inclusive"):
                values = [100 * (b[metric] - a[metric]) / b[metric] for a, b in pairs]
                comparisons.append(dict(selected=selected, baseline=baseline, regime=regime,
                    metric=metric, cases=len(rows), mean_gain_percent=statistics.mean(values),
                    median_gain_percent=statistics.median(values),
                    wins=sum(v > 1e-9 for v in values), ties=sum(abs(v) <= 1e-9 for v in values),
                    losses=sum(v < -1e-9 for v in values),
                    bootstrap_95_percent=interval(values, config["bootstrap_repeats"], config["seed"]),
                    selected_feasible_fraction=statistics.mean(a["feasible_fraction"] for a, b in pairs),
                    baseline_feasible_fraction=statistics.mean(b["feasible_fraction"] for a, b in pairs)))
    return dict(completed_cases=len(records), planned_cases=config["cases"],
                finished=len(records) == config["cases"], comparisons=comparisons,
                interpretation="Synthetic modeled execution; measured local planning only. "
                "Random replicates averaged within case. Intervals are descriptive, "
                "uncorrected for multiple comparisons, conditional on this generator. "
                "No optimality, production reliability, or research novelty claim.")


def export(root, config):
    records = [json.loads(p.read_text()) for p in sorted((root / "cases").glob("*.json"))]
    summary = summarize(records, config)
    write_json(root / "summary.json", summary)
    with (root / "results.csv").open("w", newline="") as stream:
        fields = ["case", "regime", "method", "repeat", "planning_s", "expected_s",
                  "expected_plus_planning_s", "expected_usd", "deadline_success", "feasible", "bytes"]
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for record in records:
            for row in record["observations"]:
                flat = dict(row["metrics"], **{k: row[k] for k in
                            ("method", "repeat", "planning_s", "expected_plus_planning_s")})
                flat.update(case=record["index"], regime=record["regime"])
                writer.writerow({k: flat[k] for k in fields})
    lines = ["# Ranked placement and selective checkpoint simulation", "",
             f"Completed {len(records)}/{config['cases']} cases.", "", summary["interpretation"], "",
             "Positive gain means selection is better. All cases, including infeasible plans, are retained.", "",
             "| Selected / baseline | Metric | Mean gain % | Wins / ties / losses | Feasible selected / baseline |",
             "|---|---|---:|---|---|"]
    for c in summary["comparisons"]:
        if c["regime"] == "all":
            lines.append(f"| {c['selected']} / {c['baseline']} | {c['metric']} | "
                         f"{c['mean_gain_percent']:.3f} | {c['wins']} / {c['ties']} / {c['losses']} | "
                         f"{c['selected_feasible_fraction']:.1%} / {c['baseline_feasible_fraction']:.1%} |")
    (root / "summary.md").write_text("\n".join(lines) + "\n")
    with zipfile.ZipFile(root / "analysis.tmp.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file() and path.name not in ("controller.lock", "analysis.zip", "analysis.tmp.zip"):
                archive.write(path, path.relative_to(root))
    (root / "analysis.tmp.zip").replace(root / "analysis.zip")
    return summary


def run(output, config=None, minutes=10):
    """Resume only with identical source/config; one case is the atomic unit."""
    if not 0 < minutes <= 28:
        raise ValueError("Use a session budget greater than zero and at most 28 minutes")
    config = json.loads(json.dumps(DEFAULTS if config is None else config))
    if (type(config["cases"]) is not int or not 5 <= config["cases"] <= 1000 or
            config["cases"] % 5 or type(config["seed"]) is not int or
            type(config["random_repeats"]) is not int or not 1 <= config["random_repeats"] <= 100 or
            type(config["bootstrap_repeats"]) is not int or not 100 <= config["bootstrap_repeats"] <= 10000):
        raise ValueError("Cases: 5..1000 in multiples of 5; random repeats: 1..100; bootstrap: 100..10000")
    if set(config["weights"]) != {"time", "risk", "cost"} or any(
            not math.isfinite(v) or v < 0 for v in config["weights"].values()) or not sum(config["weights"].values()):
        raise ValueError("Provide finite nonnegative time/risk/cost weights, at least one positive")
    root = Path(output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / "controller.lock"
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise RuntimeError(f"Controller lock exists: {lock}. Confirm the old process is stopped before removing it.") from None
    os.close(descriptor)
    try:
        manifest = dict(config=config, sources=fingerprints(), schema=1,
                        python=sys.version, failure_model="Independent post-map node loss; one loss per node; durable inputs/checkpoints and reducer survive")
        manifest_path = root / "manifest.json"
        if manifest_path.exists():
            previous = json.loads(manifest_path.read_text())
            if any(previous[k] != manifest[k] for k in ("config", "sources", "schema", "python")):
                raise ValueError("Configuration/source/Python changed. Choose a new output directory.")
        else:
            if any(p != lock for p in root.iterdir()):
                raise ValueError("No manifest found in nonempty output directory")
            write_json(manifest_path, manifest)
        (root / "cases").mkdir(exist_ok=True)
        (root / "source").mkdir(exist_ok=True)
        for name in manifest["sources"]:
            (root / "source" / name).write_bytes((Path(__file__).parent / name).read_bytes())
        end = time.monotonic() + minutes * 60
        status = "complete"
        try:
            for index in range(config["cases"]):
                path = root / "cases" / ("case-%04d.json" % index)
                if path.exists():
                    continue
                if time.monotonic() >= end:
                    status = "paused_budget"
                    break
                record = execute_case(index, config)
                write_json(path, record)
                print(f"Case {index+1}/{config['cases']}: {record['regime']}", flush=True)
        except KeyboardInterrupt:
            status = "paused_interrupt"
        except Exception as exc:
            write_json(root / "status.json", dict(status="error", error=repr(exc)))
            export(root, config)
            raise
        write_json(root / "status.json", dict(status=status))
        summary = export(root, config)
        print(f"Recorded: {summary['completed_cases']}/{config['cases']}; {root / 'analysis.zip'}", flush=True)
        return summary
    finally:
        lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--minutes", type=float, default=10)
    args = parser.parse_args()
    saved = Path(args.output) / "manifest.json"
    config = json.loads(args.config.read_text()) if args.config else (
        json.loads(saved.read_text())["config"] if saved.exists() else DEFAULTS)
    run(args.output, config, args.minutes)


if __name__ == "__main__":
    main()
