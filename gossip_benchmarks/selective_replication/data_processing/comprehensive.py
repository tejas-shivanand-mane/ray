#!/usr/bin/env python3
"""Real-data checkpoint-budget benchmark; explicitly run by the project owner."""

import argparse
import csv
import importlib.metadata
import json
import math
import os
from pathlib import Path
import pickle
import platform
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time
import uuid
import zipfile

import benchmark as base
import budget_planner as planner
import real_workloads as workloads
import sharded_dedup


SOURCE_FILES = ("comprehensive.py", "budget_planner.py", "real_workloads.py", "benchmark.py", "sharded_dedup.py")
POLICIES = ("recompute", "checkpoint_all", "greedy", "greedy_swap", "z3")


def child_environment():
    env = dict(os.environ)
    for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        env[key] = "1"
    return env


def source_hashes():
    return {name: workloads.sha_file(Path(__file__).with_name(name)) for name in SOURCE_FILES}


def worker(cfg):
    graph = workloads.graph(cfg["workload"])
    last = len(graph) - 1
    cache = {}
    root = Path(cfg["job"])
    durable = root / "checkpoints"
    durable.mkdir(exist_ok=True)
    events = root / "events.jsonl"
    con = workloads.context(cfg["workload"])
    start = cfg["start_index"]
    point = cfg.get("fault")
    began = None
    lock = threading.Lock()

    def crash(mode):
        with lock:
            base.atomic_json(root / "fault.json", {"mode": mode, "stage": point["stage"],
                "requested_fraction": point["fraction"],
                "lost_compute_s": time.perf_counter() - began})
            os._exit(75)

    def ensure(i):
        nonlocal began
        if i in cache:
            return cache[i]
        path = durable / ("%02d.pickle" % i)
        if path.exists():
            t = time.perf_counter()
            with path.open("rb") as stream:
                value = pickle.load(stream)
            base.append_json(events, {"kind": "restore", "stage": i,
                                      "seconds": time.perf_counter() - t, "bytes": path.stat().st_size})
        else:
            parents = [ensure(p) for p in graph[i][1]]
            began = time.perf_counter()
            inject = point is not None and i == point["stage"]
            if inject:
                delay = max(.000001, point["fraction"] * cfg["profile"][i]["compute_us"] / 1e6)
                timer = threading.Timer(delay, crash, args=("timed_during_task",))
                timer.daemon = True
                timer.start()
            value = workloads.compute(cfg["workload"], i, parents, cfg["inputs"], cfg["settings"], con)
            elapsed = time.perf_counter() - began
            if inject:
                timer.cancel()
                # Short tasks may finish before Python can schedule the timer.
                # Do not pad the job: crash before saving and label the fallback.
                crash("task_end_before_save_fallback")
            base.append_json(events, {"kind": "compute", "stage": i, "seconds": elapsed,
                                      "recomputed": i < start})
            if i == last or cfg["mask"] & (1 << i):
                t = time.perf_counter()
                base.save_checkpoint(path, value)
                size = path.stat().st_size
                base.append_json(events, {"kind": "save", "stage": i,
                                          "seconds": time.perf_counter() - t, "bytes": size})
                used = sum(p.stat().st_size for p in durable.glob("*.pickle")
                           if p.name != "%02d.pickle" % last)
                if cfg.get("budget") is not None and used > cfg["budget"]:
                    raise RuntimeError("Measured checkpoint bytes %d exceed selected case budget %d" % (used, cfg["budget"]))
                if cfg.get("profiling"):
                    t = time.perf_counter()
                    with path.open("rb") as stream:
                        restored = pickle.load(stream)
                    elapsed = time.perf_counter() - t
                    if workloads.fingerprint(restored) != workloads.fingerprint(value):
                        raise RuntimeError("Checkpoint round-trip mismatch")
                    del restored
                    base.append_json(events, {"kind": "profile_read", "stage": i, "seconds": elapsed})
        cache[i] = value
        return value

    for i in range(start, len(graph)):
        ensure(i)
        base.atomic_json(root / "progress.json", {"last_complete": i})
    base.atomic_json(root / "result.json", cache[last])
    if con is not None:
        con.close()


def run_process(cfg, attempt):
    root = Path(cfg["job"])
    path = root / ("config-%d.json" % attempt)
    base.atomic_json(path, cfg)
    with (root / ("worker-%d.log" % attempt)).open("w") as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--worker", str(path)],
                                   stdout=log, stderr=subprocess.STDOUT, env=child_environment())
        expired = threading.Event()

        def expire():
            if process.poll() is None:
                expired.set()
                process.kill()

        timer = threading.Timer(180, expire)
        timer.daemon = True
        timer.start()
        try:
            code = process.wait()
            if expired.is_set():
                raise RuntimeError("Worker exceeded 180 seconds; see " + str(root))
            return code
        finally:
            timer.cancel()


def execute(output, workload, inputs, settings, mask, budget=None, fault=None, profile=None, profiling=False):
    root = output / "work" / uuid.uuid4().hex
    root.mkdir(parents=True)
    cfg = {"job": str(root), "workload": workload, "inputs": inputs, "settings": settings,
           "mask": mask, "budget": budget, "fault": fault, "profile": profile,
           "profiling": profiling, "start_index": 0}
    began = time.perf_counter()
    code = run_process(cfg, 0)
    fault_info = None
    recovery_start = None
    if fault is not None:
        if code != 75 or not (root / "fault.json").exists():
            raise RuntimeError("Requested fault was not observed: " + str(root))
        fault_info = json.loads((root / "fault.json").read_text())
        progress = root / "progress.json"
        completed = json.loads(progress.read_text())["last_complete"] if progress.exists() else -1
        cfg.update(start_index=completed + 1, fault=None)
        recovery_start = time.perf_counter()
        code = run_process(cfg, 1)
    ended = time.perf_counter()
    elapsed = ended - began
    if code:
        raise RuntimeError("Worker exit %d: %s" % (code, root))
    result = json.loads((root / "result.json").read_text())
    events = base.read_rows(root / "events.jsonl")
    last = len(workloads.graph(workload)) - 1
    sizes = {int(p.stem): p.stat().st_size for p in (root / "checkpoints").glob("*.pickle")}
    row = {"workload": workload, "mask": mask, "elapsed_s": elapsed, "result": result,
           "recovery_s": None if recovery_start is None else ended - recovery_start,
           "fault": fault_info, "events": events,
           "checkpoint_bytes": sum(size for i, size in sizes.items() if i != last),
           "checkpoint_s": sum(e["seconds"] for e in events if e["kind"] == "save"),
           "restore_s": sum(e["seconds"] for e in events if e["kind"] == "restore"),
           "recompute_s": sum(e["seconds"] for e in events if e["kind"] == "compute" and e["recomputed"])}
    # Keep failures for diagnosis, but finished payloads can be safely discarded.
    shutil.rmtree(root)
    return row


def measure_profile(output, workload, inputs, settings):
    graph = workloads.graph(workload)
    observations = [execute(output, workload, inputs, settings, (1 << (len(graph) - 1)) - 1,
                            profiling=True) for _ in range(2)]
    if observations[0]["result"] != observations[1]["result"]:
        raise RuntimeError("Profiling runs have different outputs: " + workload)
    stages = []
    for i, (name, deps) in enumerate(graph):
        def times(kind):
            return [sum(e["seconds"] for e in r["events"] if e["stage"] == i and e["kind"] == kind)
                    for r in observations]
        stages.append({"name": name, "deps": deps,
                       "compute_us": max(1, round(statistics.median(times("compute")) * 1e6)),
                       "write_us": max(1, round(statistics.median(times("save")) * 1e6)),
                       "read_us": max(1, round(statistics.median(times("profile_read")) * 1e6)),
                       "bytes": max(e["bytes"] for r in observations for e in r["events"]
                                    if e["stage"] == i and e["kind"] == "save")})
    return {"stages": stages, "reference": observations[0]["result"],
            "elapsed_s": sum(r["elapsed_s"] for r in observations), "raw": observations}


def gather(output):
    profiles, studies = {}, {}
    for path in sorted(output.glob("profile-*.json")):
        profiles[path.stem[len("profile-"):]] = json.loads(path.read_text())
    for path in sorted(output.glob("study-*.json")):
        studies[path.stem[len("study-"):]] = json.loads(path.read_text())
    return profiles, studies


def replay_requirements(studies):
    masks = {}
    for workload, study in studies.items():
        masks[workload] = {}
        for case in study["cases"]:
            if not case["measure"]:
                continue
            for plan in case["plans"].values():
                if plan["feasible"]:
                    mask = plan["mask"]
                    masks[workload][mask] = min(masks[workload].get(mask, case["budget_bytes"]), case["budget_bytes"])
    return masks


def report(output, manifest, status, error=None):
    profiles, studies = gather(output)
    rows = base.read_rows(output / "runs.jsonl")
    indexed = {(r["workload"], r["mask"], r["repeat"], r["scenario"]): r for r in rows if r["valid"]}
    requirements = replay_requirements(studies)
    planned = sum(len(masks) * manifest["repeats"] * 4 for masks in requirements.values())
    all_cases = [case for study in studies.values() for case in study["cases"]]
    aggregates, comparisons = [], []
    for case in all_cases:
        if not case["measure"]:
            continue
        workload = case["workload"]
        feasible = {name: plan for name, plan in case["plans"].items() if plan["feasible"]}
        repeats = [r for r in range(manifest["repeats"])
                   if all((workload, plan["mask"], r, scenario) in indexed
                          for plan in feasible.values() for scenario in range(4))]
        values = {}
        for name, plan in feasible.items():
            values[name] = []
            for repeat in repeats:
                h = indexed[workload, plan["mask"], repeat, 0]["elapsed_s"]
                f = statistics.mean(indexed[workload, plan["mask"], repeat, s]["elapsed_s"] for s in (1, 2, 3))
                p = case["probability_per_thousand"] / 1000
                values[name].append((h, f, (1 - p) * h + p * f))
            if repeats:
                mean = [statistics.mean(v[i] for v in values[name]) for i in range(3)]
                aggregates.append({"case": case["id"], "origin": case["origin"], "policy": name,
                                   "mask": plan["mask"], "n": len(repeats), "healthy_s": mean[0],
                                   "failure_s": mean[1], "expected_s": mean[2],
                                   "checkpoint_bytes": plan["bytes"], "planning_s": plan["planning_s"],
                                   "expected_plus_planning_s": mean[2] + plan["planning_s"],
                                   "one_off_with_profile_s": mean[2] + plan["planning_s"] + profiles[workload]["elapsed_s"]})
        for name in ("greedy", "greedy_swap", "z3"):
            for baseline in ("recompute", "checkpoint_all", "greedy", "greedy_swap"):
                if name == baseline or not repeats or name not in values or baseline not in values:
                    continue
                a, b = values[baseline], values[name]
                gains = [100 * (x[2] - y[2]) / x[2] for x, y in zip(a, b)]
                mean = statistics.mean(gains)
                critical = {3: 4.30265273, 5: 2.77644511, 7: 2.44691185}.get(len(gains))
                half = None if critical is None else critical * statistics.stdev(gains) / math.sqrt(len(gains))
                savings = statistics.mean(x[2] - y[2] for x, y in zip(a, b))
                extra_plan = feasible[name]["planning_s"] - feasible[baseline]["planning_s"]
                comparisons.append({"case": case["id"], "policy": name, "baseline": baseline,
                    "origin": case["origin"], "n": len(gains), "gain_percent": mean,
                    "paired_gains_percent": gains, "exploratory_95_ci": None if half is None else [mean - half, mean + half],
                    "same_plan": feasible[name]["mask"] == feasible[baseline]["mask"],
                    "jobs_to_amortize_extra_planning": math.ceil(max(0., extra_plan) / savings) if savings > 0 else None})
    model_counts = {}
    for origin in ("fixed", "discovered_gap"):
        subset = [c for c in all_cases if c["origin"] == origin]
        model_counts[origin] = {"cases": len(subset), "z3_proven": sum(c["solver"]["optimality_proven"] for c in subset),
            "greedy_suboptimal": sum(c["plans"]["greedy"]["model_gap_fraction"] > 0 for c in subset),
            "swap_suboptimal": sum(c["plans"]["greedy_swap"]["model_gap_fraction"] > 0 for c in subset)}
    witnesses = [{"case": c["id"], "origin": c["origin"], "selected_for_measurement": c["measure"],
                  "failure_scope": c.get("failure_scope", "compute_progress"),
                  "budget_bytes": c["budget_bytes"],
                  "empirical_comparison": next((r for r in comparisons if r["case"] == c["id"]
                      and r["policy"] == "z3" and r["baseline"] == w["baseline"]), None), **w}
                 for c in all_cases for w in c.get("counterexamples", [])]
    base.atomic_json(output / "counterexamples.json", {
        "status": status, "modeled_counterexamples": witnesses,
        "meaning": "Strict modeled gaps certified by Z3 and exhaustive enumeration; empirical gains are separate.",
        "discovery_counts": {w: {"scanned": len(s["discovery_scan"]),
            "greedy_gaps": sum(r["greedy_gap_fraction"] > 0 for r in s["discovery_scan"]),
            "swap_gaps": sum(r["swap_gap_fraction"] > 0 for r in s["discovery_scan"])} for w, s in studies.items()}})
    summary = {"status": status, "error": error, "unique_replays_completed": len(indexed),
               "unique_replays_planned": planned, "modeled_cases": model_counts,
               "counterexamples": witnesses,
               "failure_scopes": {w: s.get("failure_scope", "compute_progress") for w, s in studies.items()},
               "aggregates": aggregates, "comparisons": comparisons,
               "fault_fallbacks": sum(bool(r.get("fault")) and r["fault"]["mode"] != "timed_during_task" for r in rows),
               "fault_injections": sum(bool(r.get("fault")) for r in rows),
               "warning": "Fixed grid and adaptively discovered gaps are separate. Identical plans share replay observations. "
               "Model optimality is not an empirical speedup or novelty claim. Storage budget and failure probability are assumptions."}
    base.atomic_json(output / "summary.json", summary)
    base.atomic_json(output / "status.json", {k: summary[k] for k in ("status", "error", "unique_replays_completed", "unique_replays_planned")})
    lines = ["# Real-data selective checkpoint benchmark", "", "Status: **%s**. Unique replays: %d/%d." %
             (status, len(indexed), planned), "", summary["warning"], "", "## Modeled choices", ""]
    for origin, counts in model_counts.items():
        lines.append("- %s: %s" % (origin, counts))
    lines += ["", "## Greedy counterexamples", "",
              "Failure scopes: " + str(summary["failure_scopes"]), "",
              "Reduce-only cases condition failures on the final combine task. They do not represent a uniform job-wide hazard."]
    if not witnesses:
        lines.append("No certified greedy counterexample in the completed modeled cases; see counterexamples.json for scan counts.")
    for witness in witnesses:
        lines.append("- {case}, {baseline}: remove {remove}, add {add}; modeled time "
                     "{baseline_expected_s:.6f}s -> {optimal_expected_s:.6f}s; minimum changed choices "
                     "to improve: {minimum_changed_choices_to_improve}; selected for measurement: "
                     "{selected_for_measurement}.".format(**witness))
    lines += ["", "## Measured completion time", "",
              "Only complete matched repetitions enter each case. With-failure means the three evaluation fault locations; "
              "expected time mixes those with healthy time using that case's assumed probability. "
              "Checkpoint-all is omitted where it exceeds the storage budget.", "",
              "| Case | Policy | n | Healthy s | With failure s | Expected s | Plan s | Expected + plan s |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for row in aggregates:
        lines.append("| {case} | {policy} | {n} | {healthy_s:.4f} | {failure_s:.4f} | {expected_s:.4f} | "
                     "{planning_s:.4f} | {expected_plus_planning_s:.4f} |".format(**row))
    lines += ["", "## Paired comparisons", "", "Positive is faster. Intervals are exploratory paired-t intervals, "
              "without multiple-comparison correction; cases reuse data and sometimes the same timings.", ""]
    for row in comparisons:
        lines.append("- {case}: {policy} vs {baseline}: {gain_percent:.2f}%; "
                     "95% interval {exploratory_95_ci}; identical plan: {same_plan}.".format(**row))
    lines += ["", "Fault injections: %d; short-task end-of-task fallbacks: %d." %
              (summary["fault_injections"], summary["fault_fallbacks"]), "",
              "Local single-worker processes with surviving inputs, checkpoints and controller. "
              "No cluster/network billing or production deployment claim. SMT only optimizes output checkpoint subsets, "
              "not placement. Profiling time is excluded from the main timings but included in JSON one-off-job totals."]
    if error:
        lines += ["", "Error: " + error]
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    with (output / "aggregates.csv").open("w", newline="") as stream:
        if aggregates:
            writer = csv.DictWriter(stream, fieldnames=list(aggregates[0]))
            writer.writeheader()
            writer.writerows(aggregates)
    archive_tmp = output / "analysis.zip.tmp"
    with zipfile.ZipFile(archive_tmp, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(output.iterdir()):
            if path.is_file() and path.suffix in (".json", ".jsonl", ".md", ".csv", ".log"):
                archive.write(path, path.name)
        for path in sorted((output / "models").glob("*")):
            if path.is_file() and path.suffix in (".smt2", ".json"):
                archive.write(path, "models/" + path.name)
        if (output / "inputs" / "inputs.json").exists():
            archive.write(output / "inputs" / "inputs.json", "inputs/inputs.json")
        # Only small diagnostic files from unfinished/failed workers. Never
        # include raw data or checkpoint payloads in the analysis bundle.
        for path in sorted((output / "work").glob("*/*")):
            if path.is_file() and path.suffix in (".json", ".jsonl", ".log"):
                archive.write(path, str(path.relative_to(output)))
        for name in SOURCE_FILES + ("COMPREHENSIVE.md", "requirements-comprehensive.txt"):
            archive.write(Path(__file__).with_name(name), "source/" + name)
    archive_tmp.replace(output / "analysis.zip")
    return summary


def reuse_prepared_inputs(output, settings):
    """Copy verified input payloads into a NEW run; never reuse plans/results."""
    source = settings.get("reuse_inputs_from")
    if not source or (output / "inputs" / "inputs.json").exists():
        return
    source = Path(source)
    previous = json.loads((source / "manifest.json").read_text())
    keys = ("workloads", "taxi_rows", "news_documents", "news_tokens", "seed")
    if any(previous[key] != settings[key] for key in keys):
        raise RuntimeError("Reused input settings differ; match the original data settings or start without --reuse-inputs-from")
    if previous["source_hashes"]["real_workloads.py"] != settings["source_hashes"]["real_workloads.py"]:
        raise RuntimeError("Input/workload implementation changed; prepare fresh data")
    metadata = json.loads((source / "inputs" / "inputs.json").read_text())
    directory = output / "inputs"
    directory.mkdir(exist_ok=True)
    for name, info in metadata.items():
        old = Path(info["path"])
        target = directory / (name + "-reused" + "".join(old.suffixes))
        temp = target.with_name(target.name + ".partial")
        shutil.copyfile(old, temp)
        if workloads.sha_file(temp) != info["sha256"]:
            raise RuntimeError("Reused input fingerprint mismatch: " + str(old))
        temp.replace(target)
        info["path"] = str(target)
    base.atomic_json(directory / "inputs.json", metadata)


def controller(output):
    settings = json.loads((output / "manifest.json").read_text())
    reuse_prepared_inputs(output, settings)
    inputs = workloads.prepare(output / "inputs", settings)
    if any(w in sharded_dedup.WORKLOADS for w in settings["workloads"]):
        inputs = sharded_dedup.prepare(output / "inputs", inputs)
    (output / "models").mkdir(exist_ok=True)
    for workload in settings["workloads"]:
        path = output / ("profile-" + workload + ".json")
        if not path.exists():
            print("Profiling " + workload, flush=True)
            base.atomic_json(path, measure_profile(output, workload, inputs, settings))
        profile = json.loads(path.read_text())
        study_path = output / ("study-" + workload + ".json")
        if not study_path.exists():
            print("Planning fixed grid and labeled gap discovery: " + workload, flush=True)
            study = planner.study(workload, profile["stages"], output / "models", settings["solver_timeout_ms"])
            base.atomic_json(study_path, study)
        report(output, settings, "running")
    profiles, studies = gather(output)
    requirements = replay_requirements(studies)
    previous = base.read_rows(output / "runs.jsonl")
    if any(not r.get("valid") for r in previous):
        raise RuntimeError("A prior replay failed correctness; investigate before starting a fresh run")
    completed = {(r["workload"], r["mask"], r["repeat"], r["scenario"]) for r in previous}
    rng = random.Random(settings["seed"])
    blocks = [(w, r) for w in settings["workloads"] for r in range(settings["repeats"])]
    rng.shuffle(blocks)
    for workload, repeat in blocks:
        jobs = [(mask, scenario) for mask in sorted(requirements[workload]) for scenario in range(4)]
        rng.shuffle(jobs)
        for mask, scenario in jobs:
            key = (workload, mask, repeat, scenario)
            if key in completed:
                continue
            fault = None if scenario == 0 else studies[workload]["evaluation_faults"][scenario - 1]
            row = execute(output, workload, inputs, settings, mask, requirements[workload][mask],
                          fault=fault, profile=profiles[workload]["stages"])
            row.update(repeat=repeat, scenario=scenario, valid=row["result"] == profiles[workload]["reference"])
            base.append_json(output / "runs.jsonl", row)
            if not row["valid"]:
                raise RuntimeError("Recovery output mismatch: %s" % (key,))
            completed.add(key)
            print("Replay %d: %s mask=%d repeat=%d scenario=%d %.3fs" %
                  (len(completed), workload, mask, repeat + 1, scenario, row["elapsed_s"]), flush=True)
        report(output, settings, "running")


def repair_interrupted_append(path):
    """Preserve an interrupted last line separately before continuing append."""
    if not path.exists():
        return
    lines = path.read_bytes().splitlines(keepends=True)
    for index, line in enumerate(lines):
        try:
            json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            if index != len(lines) - 1:
                raise RuntimeError("Corrupt non-final result line; refusing to discard recorded work")
            backup = path.with_name("interrupted-tail-" + uuid.uuid4().hex[:8] + ".txt")
            backup.write_bytes(line)
            temp = path.with_suffix(".repair")
            temp.write_bytes(b"".join(lines[:index]))
            temp.replace(path)
            return
    if lines and not lines[-1].endswith(b"\n"):
        with path.open("ab") as stream:
            stream.write(b"\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--reuse-inputs-from", type=Path,
                        help="Start a fresh run using verified prepared inputs from an old run directory")
    parser.add_argument("--max-minutes", type=float, default=28)
    parser.add_argument("--workloads", nargs="+", choices=workloads.WORKLOADS, default=list(workloads.DEFAULT_WORKLOADS))
    parser.add_argument("--taxi-rows", type=int, default=200000)
    parser.add_argument("--news-documents", type=int, default=1500)
    parser.add_argument("--news-tokens", type=int, default=512)
    parser.add_argument("--taxi-file", type=Path)
    parser.add_argument("--zones-file", type=Path)
    parser.add_argument("--repeats", type=int, choices=(3, 5, 7), default=5)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--solver-timeout-ms", type=int, default=5000)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--controller", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker.read_text()))
        return 0
    if args.controller:
        controller(args.controller)
        return 0
    if os.name != "posix" or not 0 < args.max_minutes <= 29:
        parser.error("Use Linux/macOS and a max-minutes value in (0,29]")
    if min(args.taxi_rows, args.news_documents, args.news_tokens, args.solver_timeout_ms) <= 0:
        parser.error("Input sizes and solver timeout must be positive")
    if len(set(args.workloads)) != len(args.workloads):
        parser.error("Duplicate workload names")
    if args.reuse_inputs_from and (args.resume or args.taxi_file or args.zones_file):
        parser.error("--reuse-inputs-from starts a fresh run and cannot be combined with --resume or local input overrides")
    versions = {}
    for package in ("duckdb", "pyarrow", "scikit-learn", "z3-solver"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            parser.error("Missing %s. Install requirements-comprehensive.txt in your environment." % package)
    if args.resume:
        output = args.resume.resolve()
        manifest = json.loads((output / "manifest.json").read_text())
        if manifest["source_hashes"] != source_hashes() or manifest["versions"] != versions:
            parser.error("Source/dependency versions changed; start a fresh run rather than mixing results")
    else:
        output = (args.output or Path("ray_gcp_results/data-processing-comprehensive") /
                  (time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])).resolve()
        output.mkdir(parents=True, exist_ok=False)
        manifest = {"schema_version": 1, "workloads": args.workloads, "taxi_rows": args.taxi_rows,
                    "news_documents": args.news_documents, "news_tokens": args.news_tokens,
                    "taxi_file": str(args.taxi_file.resolve()) if args.taxi_file else None,
                    "zones_file": str(args.zones_file.resolve()) if args.zones_file else None,
                    "reuse_inputs_from": str(args.reuse_inputs_from.resolve()) if args.reuse_inputs_from else None,
                    "repeats": args.repeats, "seed": args.seed, "solver_timeout_ms": args.solver_timeout_ms,
                    "source_hashes": source_hashes(), "versions": versions, "python": sys.version,
                    "platform": platform.platform(), "command": sys.argv,
                    "budget_fractions": planner.FRACTIONS, "probabilities_per_thousand": planner.PROBABILITIES}
        base.atomic_json(output / "manifest.json", manifest)
    lock = output / "run.lock"
    try:
        with lock.open("x") as stream:
            stream.write(str(os.getpid()))
    except FileExistsError:
        parser.error("Run lock exists. Check that the previous controller has stopped before removing " + str(lock))
    try:
        if args.resume:
            repair_interrupted_append(output / "runs.jsonl")
        report(output, manifest, "starting")
        print("Output: %s\nBudget per invocation: %.1f minutes. Setup/downloads are included.\n"
              "Resume preserves frozen profiles/plans and completed replays." % (output, args.max_minutes), flush=True)
        with (output / "controller.log").open("a") as log:
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--controller", str(output)],
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                                       env=child_environment())
            status, error, exit_code = "failed", None, 1
            began = time.monotonic()
            try:
                while True:
                    remaining = args.max_minutes * 60 - (time.monotonic() - began)
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(process.args, args.max_minutes * 60)
                    try:
                        code = process.wait(timeout=min(15, remaining))
                        if code == 0:
                            status, exit_code = "complete", 0
                        else:
                            error = "Controller exited %d; see controller.log in analysis.zip" % code
                        break
                    except subprocess.TimeoutExpired:
                        count = len(base.read_rows(output / "runs.jsonl"))
                        print("%.1f min: %d completed replays; setup/planning details in controller.log" %
                              ((time.monotonic() - began) / 60, count), flush=True)
            except subprocess.TimeoutExpired:
                status, error, exit_code = "time_limit", "Budget reached; resume to finish pending work", 124
            except KeyboardInterrupt:
                status, error, exit_code = "interrupted", "Interrupted; completed work is resumable", 130
            finally:
                base.stop_group(process)
        result = report(output, manifest, status, error)
        print("%s: %d/%d unique replays.\nRead: %s\nShare: %s" %
              (status, result["unique_replays_completed"], result["unique_replays_planned"],
               output / "summary.md", output / "analysis.zip"))
        return exit_code
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
