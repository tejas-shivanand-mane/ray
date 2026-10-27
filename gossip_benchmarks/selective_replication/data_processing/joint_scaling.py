"""Owner-run joint node-placement/checkpoint scaling study (offline model).

Profiles real MinHash partitions locally. Node loss, prices, speed differences,
network costs and parallel execution are modeled, not measured on a cluster.
"""

import argparse
import csv
import gzip
import hashlib
import importlib.metadata
import json
import math
from pathlib import Path
import pickle
import random
import statistics
import subprocess
import sys
import time
import uuid
import zipfile

import benchmark as base
import joint_model as model
import real_workloads as workloads
import sharded_dedup


HERE = Path(__file__).resolve().parent
SOURCES = ("joint_scaling.py", "joint_model.py", "sharded_dedup.py", "real_workloads.py", "benchmark.py")
METHODS = ("cheapest_unprotected", "regular_unprotected", "regular_checkpoint_all",
           "greedy_joint", "greedy_swap", "sequential_dp", "joint_dp", "z3")


def read_json(path):
    return json.loads(Path(path).read_text())


def hashes():
    return {name: workloads.sha_file(HERE / name) for name in SOURCES}


def versions():
    return {name: importlib.metadata.version(name) for name in ("scikit-learn", "z3-solver")}


def partitions(records, count):
    """Seven source families, or 20 categories split into count disjoint shards."""
    groups = {}
    for row in records:
        key = row["category"].split(".")[0] if count == 7 else row["category"]
        groups.setdefault(key, []).append(row)
    if count < len(groups):
        raise ValueError("Use 7 or at least 20 partitions")
    allocation = {key: 1 for key in groups}
    while sum(allocation.values()) < count:
        eligible = [key for key in groups if allocation[key] < len(groups[key])]
        if not eligible:
            raise ValueError("Not enough records for requested partitions")
        key = max(eligible, key=lambda k: (len(groups[k]) / allocation[k], k))
        allocation[key] += 1
    return [(key + "-%03d" % part, groups[key][part::allocation[key]])
            for key in sorted(groups) for part in range(allocation[key])]


def profile(output, inputs, documents, count, settings):
    profile_path = output / ("profile-d%d-n%d.json" % (documents, count))
    if profile_path.exists():
        return read_json(profile_path)
    with gzip.open(inputs["news"]["path"], "rt", encoding="utf-8") as stream:
        records = json.load(stream)[:documents]
    if len(records) != documents:
        raise ValueError("Dataset has fewer nonempty documents than requested")
    directory = output / "work"
    directory.mkdir(exist_ok=True)
    news = directory / "news.json.gz"
    with gzip.open(news, "wt", encoding="utf-8") as stream:
        json.dump(records, stream)
    tasks, parents = [], []
    began = time.perf_counter()
    for name, rows in partitions(records, count):
        data = directory / "partition.json.gz"
        with gzip.open(data, "wt", encoding="utf-8") as stream:
            json.dump(rows, stream)
        observations, fingerprint = [], None
        for _ in range(settings["profile_repeats"]):
            start = time.perf_counter()
            value = sharded_dedup.compute(0, [], {"dedup_alt": {"path": str(data)}},
                                          {"news_tokens": settings["tokens"]})
            compute = time.perf_counter() - start
            current = workloads.fingerprint(value)
            if fingerprint is not None and current != fingerprint:
                raise RuntimeError("Nondeterministic task output")
            fingerprint = current
            checkpoint = directory / "checkpoint.pkl"
            start = time.perf_counter()
            base.save_checkpoint(checkpoint, value)
            write = time.perf_counter() - start
            start = time.perf_counter()
            with checkpoint.open("rb") as stream:
                restored = pickle.load(stream)  # Only our own just-created checkpoint.
            read = time.perf_counter() - start
            if workloads.fingerprint(restored) != fingerprint:
                raise RuntimeError("Checkpoint roundtrip mismatch")
            observations.append({"compute_us": max(1, round(compute * 1e6)),
                "write_us": max(1, round(write * 1e6)), "read_us": max(1, round(read * 1e6)),
                "bytes": checkpoint.stat().st_size})
        tasks.append({"name": name, "documents": len(rows), "input_bytes": data.stat().st_size,
            "output_sha256": fingerprint,
            **{key: round(statistics.median(o[key] for o in observations)) for key in observations[0]},
            "observations": observations})
        parents.append(value)
    reductions, result = [], None
    for _ in range(settings["profile_repeats"]):
        start = time.perf_counter()
        reduced = sharded_dedup.compute(7, parents, {"news": {"path": str(news)}},
                                        {"news_tokens": settings["tokens"]})
        reductions.append(max(1, round((time.perf_counter() - start) * 1e6)))
        if result is not None and result != reduced:
            raise RuntimeError("Nondeterministic reducer")
        result = reduced
    for previous in output.glob("profile-d%d-n*.json" % documents):
        if read_json(previous)["result"] != result:
            raise RuntimeError("Changing partition count changed the application result")
    value = {"documents": documents, "partitions": count, "tasks": tasks,
        "reduce_us": round(statistics.median(reductions)), "reduce_observations_us": reductions,
        "result": result, "profile_s": time.perf_counter() - began,
        "document_ids_sha256": hashlib.sha256(json.dumps([r["id"] for r in records]).encode()).hexdigest(),
        "input_sha256": inputs["news"]["sha256"]}
    base.atomic_json(profile_path, value)
    return value


def plan(method, case, settings, smt_path):
    began = time.perf_counter()
    deadline = began + settings["planner_seconds"]
    n = len(case["options"])
    if method == "z3":
        result = model.solve_z3(case, None if settings.get("z3_unlimited", False) else deadline, smt_path)
    elif method == "joint_dp":
        result = model.exact_dp(case, deadline, settings["dp_max_states"])
    elif method == "sequential_dp":
        placement = model.greedy(case, began + settings["planner_seconds"] / 2, placement_only=True)
        result = model.exact_dp(case, deadline, settings["dp_max_states"], placement["plan"])
        result["placement_status"] = placement["status"]
        result["checkpoint_subproblem_optimality_proven"] = result["optimality_proven"]
        result["optimality_proven"] = False
        result["status"] = "fixed_placement_" + result["status"]
    elif method.startswith("greedy"):
        result = model.greedy(case, deadline, swaps=method == "greedy_swap")
    else:
        if method == "cheapest_unprotected":
            node = min(range(len(case["nodes"])), key=lambda j: (case["rates"][j], j))
            selected = [2 * node] * n
        else:
            selected = [x + int(method == "regular_checkpoint_all") for x in case["reference_plan"]]
        result = {"plan": selected, "status": "fixed_control", "optimality_proven": False}
    result["planning_s"] = time.perf_counter() - began
    if result["plan"] is not None:
        result["metrics"] = model.evaluate(case, result["plan"])
        result["assignments"] = [{"task": i, "node": case["nodes"][choice // 2]["name"],
                                   "checkpoint": bool(choice % 2)} for i, choice in enumerate(result["plan"])]
        result["expected_plus_planning_s"] = result["metrics"]["expected_s"] + result["planning_s"]
        result["reuse_20_s"] = result["metrics"]["expected_s"] + result["planning_s"] / 20
        success = sum(s["weight"] for s, duration in zip(case["scenarios"], result["metrics"]["scenario_us"])
                      if duration + math.ceil(result["planning_s"] * 1e6) <= case["deadline_us"])
        result["deadline_success_including_planning"] = success / case["weight_total"]
    return result


def audit_small_case(output, profile_value, config, settings):
    path = output / "audit.json"
    if path.exists():
        if read_json(path)["status"] != "passed":
            raise RuntimeError("Existing small-case audit did not pass")
        return
    small = dict(profile_value, tasks=profile_value["tasks"][:3])
    audits = []
    for risk in config["risk_profiles"]:
        case = model.make_case(small, config, risk, 1.)
        optimum = model.exhaustive(case, time.perf_counter() + 20)
        expected = None if optimum is None else model.evaluate(case, optimum)["objective"]
        for method in ("joint_dp", "z3"):
            result = plan(method, case, dict(settings, planner_seconds=10), output / "models" / ("audit-" + risk["name"] + ".smt2"))
            actual = result.get("metrics", {}).get("objective")
            if not result["optimality_proven"] or actual != expected:
                base.atomic_json(path, {"status": "failed", "method": method,
                    "oracle_objective": expected, "result": result})
                raise RuntimeError("Small-case exactness audit failed; see audit.json")
            audits.append({"risk": risk["name"], "method": method, "oracle_objective": expected,
                           "result": result})
    base.atomic_json(path, {"status": "passed", "audits": audits})


def report(output):
    manifest = read_json(output / "manifest.json")
    rows = [read_json(path) for path in sorted((output / "results").glob("*.json"))]
    status = read_json(output / "status.json")
    comparisons, validation_errors = [], []
    by_key = {(r["case_id"], r["repeat"], r["method"]): r for r in rows}
    for row in rows:
        if row["method"] != "z3":
            continue
        for method in METHODS:
            if method == "z3":
                continue
            other = by_key.get((row["case_id"], row["repeat"], method))
            if not other:
                continue
            z_ok = row.get("metrics", {}).get("feasible", False)
            b_ok = other.get("metrics", {}).get("feasible", False)
            comparison = {"case_id": row["case_id"], "repeat": row["repeat"], "baseline": method,
                "z3_feasible": z_ok, "baseline_feasible": b_ok,
                "z3_optimality_proven": row["optimality_proven"],
                "baseline_optimality_proven": other["optimality_proven"]}
            if z_ok and b_ok:
                comparison.update({"modeled_execution_gain_percent": 100 * (
                    other["metrics"]["expected_s"] - row["metrics"]["expected_s"]) / other["metrics"]["expected_s"],
                    "including_planning_gain_percent": 100 * (
                    other["expected_plus_planning_s"] - row["expected_plus_planning_s"]) / other["expected_plus_planning_s"]})
                if row["optimality_proven"] and other["optimality_proven"] and (
                        row["metrics"]["objective"] != other["metrics"]["objective"]):
                    validation_errors.append("Exact planner disagreement: " + row["case_id"])
            if ((row["status"] == "infeasible" and b_ok) or
                    (other["status"] == "infeasible" and z_ok)):
                validation_errors.append("Infeasibility contradiction: " + row["case_id"])
            comparisons.append(comparison)
    summary = {"status": status, "validation_errors": validation_errors, "results_completed": len(rows),
        "z3_unlimited": manifest["settings"].get("z3_unlimited", False),
        "results_planned": manifest["results_planned"], "comparisons": comparisons,
        "limitations": ["Offline model calibrated from local real-data task profiles; no cloud failures executed.",
            "Repeated planner timings are not repeated distributed workload measurements.",
            "Failure probabilities and prices are configurable assumptions, not provider measurements.",
            "All checkpoint outputs are durable before one post-map node-loss window.",
            "Exact DP/Z3 labels require completed proofs; timed-out incumbents are not certified optimal."]}
    base.atomic_json(output / "summary.json", summary)
    fields = ("case", "repeat", "method", "status", "optimality_proven", "feasible", "planning_s",
              "modeled_expected_s", "modeled_expected_usd", "deadline_success", "expected_plus_planning_s",
              "deadline_success_including_planning", "reuse_20_s", "checkpoint_bytes")
    with (output / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fields)
        writer.writeheader()
        for r in rows:
            metrics = r.get("metrics", {})
            writer.writerow(dict(zip(fields, (r["case_id"], r["repeat"], r["method"], r["status"],
                r["optimality_proven"], metrics.get("feasible", False), r["planning_s"],
                metrics.get("expected_s"), metrics.get("expected_usd"), metrics.get("deadline_success"),
                r.get("expected_plus_planning_s"), r.get("deadline_success_including_planning"),
                r.get("reuse_20_s"), metrics.get("bytes")))))
    lines = ["# Joint placement and recovery scaling", "", "Status: " + status["status"],
        "Completed planner results: %d/%d." % (len(rows), manifest["results_planned"]), "",
        "Execution times, dollar costs and deadline probabilities below are MODEL outputs.",
        "Only local profiling and planner durations were measured.", "",
        "| Case | Method | Completed repeats | Feasible | Proven optimal | Median planning s | Median modeled execution s |",
        "|---|---|---:|---:|---:|---:|---:|"]
    if validation_errors:
        lines.insert(4, "INVALID RESULTS: " + "; ".join(validation_errors))
    if manifest["settings"].get("z3_unlimited", False):
        lines.insert(4, "Z3 has no time limit; other search methods retain their configured budgets. This is not an equal-planning-budget comparison.")
    groups = {}
    for row in rows:
        groups.setdefault((row["case_id"], row["method"]), []).append(row)
    for (case_id, method), group in sorted(groups.items()):
        feasible = [r for r in group if r.get("metrics", {}).get("feasible")]
        runtime = "%.6f" % statistics.median(r["metrics"]["expected_s"] for r in feasible) if feasible else "—"
        lines.append("| %s | %s | %d | %d | %d | %.6f | %s |" % (case_id, method, len(group),
            len(feasible), sum(r["optimality_proven"] and r["status"] == "optimal" for r in group),
            statistics.median(r["planning_s"] for r in group), runtime))
    lines += ["", "See results.csv for cost, deadline success and planning-inclusive times.",
        "Use matched case/repetition comparisons; do not drop timeouts or infeasible methods.",
        "No significance claim is made from repeats of this deterministic cost model."]
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    with zipfile.ZipFile(output / "analysis.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for pattern in ("*.json", "*.csv", "*.md", "*.log", "results/*.json", "cases/*.json", "models/*.smt2", "models/*.json", "inputs/inputs.json"):
            for path in sorted(output.glob(pattern)):
                archive.write(path, str(path.relative_to(output)))
        for name in SOURCES + ("JOINT_SCALING.md", "joint_nodes.example.json", "requirements-comprehensive.txt"):
            archive.write(HERE / name, "source/" + name)
    return validation_errors


def worker(output):
    manifest = read_json(output / "manifest.json")
    settings, config = manifest["settings"], manifest["config"]
    inputs = workloads.prepare(output / "inputs", {"workloads": ["news_dedup_reduce"],
        "seed": settings["seed"], "news_documents": max(settings["documents"])})
    cases = []
    for documents in settings["documents"]:
        for count in settings["tasks"]:
            print("Profile %d documents / %d tasks" % (documents, count), flush=True)
            profile_value = profile(output, inputs, documents, count, settings)
            audit_small_case(output, profile_value, config, settings)
            for risk in config["risk_profiles"]:
                for factor in config["budget_factors"]:
                    case_id = "d%d-n%d-%s-b%s" % (documents, count, risk["name"], str(factor))
                    case = model.make_case(profile_value, config, risk, factor)
                    case.update({"id": case_id, "documents": documents, "tasks": count,
                                 "profile_s": profile_value["profile_s"]})
                    base.atomic_json(output / "cases" / (case_id + ".json"), case)
                    cases.append(case)
    # Repeat outermost: a partial run does not use all repeats on only small cases.
    for repeat in range(settings["repeats"]):
        jobs = [(case, method) for case in cases for method in METHODS]
        random.Random(settings["seed"] + repeat).shuffle(jobs)
        for case, method in jobs:
            ident = "%s-r%d-%s" % (case["id"], repeat, method)
            path = output / "results" / (ident + ".json")
            if path.exists():
                continue
            print(ident, flush=True)
            result = plan(method, case, settings, output / "models" / (ident + ".smt2"))
            result.update({"case_id": case["id"], "repeat": repeat, "method": method,
                           "profile_s": case["profile_s"]})
            if result.get("expected_plus_planning_s") is not None:
                result["including_profile_s"] = result["expected_plus_planning_s"] + case["profile_s"]
            base.atomic_json(path, result)
        if report(output):
            raise RuntimeError("Planner validation errors; see summary.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=Path, default=HERE / "joint_nodes.example.json")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--tasks", type=int, nargs="+", default=[7, 20, 50, 100])
    parser.add_argument("--documents", type=int, nargs="+", default=[1500, 6000])
    parser.add_argument("--tokens", type=int, default=512)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--profile-repeats", type=int, default=2)
    parser.add_argument("--planner-seconds", type=float, default=5.)
    parser.add_argument("--z3-unlimited", action="store_true",
                        help="Remove Z3 query/search time limits and the overall controller timer")
    parser.add_argument("--dp-max-states", type=int, default=10000)
    parser.add_argument("--minutes", type=float, default=28.)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(args.worker.resolve())
        return
    if not 0 < args.minutes <= 29:
        parser.error("--minutes must be in (0,29]")
    if args.resume:
        output = args.resume.resolve()
        manifest = read_json(output / "manifest.json")
        if manifest["source_hashes"] != hashes() or manifest["versions"] != versions():
            parser.error("Source or dependency versions changed; start a fresh run")
        if args.z3_unlimited and not manifest["settings"].get("z3_unlimited", False):
            parser.error("Cannot change a bounded run on resume; start a fresh --z3-unlimited run")
        print("Resuming saved settings; only --minutes changes the session budget", flush=True)
    else:
        if (any(n != 7 and n < 20 for n in args.tasks) or min(args.documents) < max(args.tasks) or
                min(args.tokens, args.repeats, args.profile_repeats, args.dp_max_states) < 1 or
                not 0 < args.planner_seconds <= 60 or len(set(args.tasks)) != len(args.tasks) or
                len(set(args.documents)) != len(args.documents)):
            parser.error("Invalid sizes/repeats/time limit; use distinct task counts 7 or >=20")
        config = read_json(args.nodes)
        if (not config["risk_profiles"] or not config["budget_factors"] or
                len({r["name"] for r in config["risk_profiles"]}) != len(config["risk_profiles"]) or
                any(not r["name"] or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for c in r["name"])
                    for r in config["risk_profiles"])):
            parser.error("Use distinct lowercase risk profile names without path separators")
        settings = {key: getattr(args, key) for key in ("tasks", "documents", "tokens", "repeats",
            "profile_repeats", "planner_seconds", "dp_max_states", "seed", "z3_unlimited")}
        # Validate config before downloading or measuring anything.
        tiny = {"tasks": [{"compute_us": 1000, "write_us": 10, "read_us": 10, "bytes": 100}], "reduce_us": 100}
        for risk in config["risk_profiles"]:
            for factor in config["budget_factors"]:
                model.make_case(tiny, config, risk, factor)
        output = (args.output or Path("ray_gcp_results/data-processing-joint") /
                  (time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])).resolve()
        output.mkdir(parents=True, exist_ok=False)
        for directory in ("models", "cases", "results"):
            (output / directory).mkdir()
        manifest = {"settings": settings, "config": config, "source_hashes": hashes(),
            "versions": versions(), "mode": "offline_joint_node_model",
            "results_planned": len(args.tasks) * len(args.documents) * len(config["risk_profiles"]) *
                len(config["budget_factors"]) * args.repeats * len(METHODS),
            "methods": METHODS, "price_source": config.get("description", "user-provided assumptions")}
        base.atomic_json(output / "manifest.json", manifest)
    print("Output: " + str(output), flush=True)
    unlimited = manifest["settings"].get("z3_unlimited", False)
    if unlimited:
        print("Z3 unlimited: no solver or controller timeout. Ctrl+C saves completed results and diagnostics.", flush=True)
    lock = output / "controller.lock"
    try:
        with lock.open("x") as stream:
            stream.write("Supervisor active; do not run a second writer.\n")
    except FileExistsError:
        parser.error("Controller lock exists; confirm no old controller is running before removing it")
    process = None
    try:
        base.atomic_json(output / "status.json", {"status": "running"})
        with (output / "controller.log").open("a") as log:
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--worker", str(output)],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=None if unlimited else args.minutes * 60)
                status = {"status": "complete" if code == 0 else "failed", "exit_code": code}
            except subprocess.TimeoutExpired:
                base.stop_group(process)
                status = {"status": "partial_timeout", "resume": str(output)}
            except KeyboardInterrupt:
                base.stop_group(process)
                status = {"status": "interrupted", "resume": str(output)}
        base.atomic_json(output / "status.json", status)
        report(output)
        print(status["status"] + "; send " + str(output / "analysis.zip"), flush=True)
    finally:
        if process is not None and process.poll() is None:
            base.stop_group(process)
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
