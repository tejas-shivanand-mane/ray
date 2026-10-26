#!/usr/bin/env python3
"""Measured, local data-processing recovery experiment; standard library only.

Run explicitly by the project owner. No Ray, cloud resources, sleeps, or simulated
compute. Planning uses measured task costs; evaluation executes real pipelines.
"""

import argparse
import collections
import csv
import gzip
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import traceback
import uuid
import zipfile
import pickle


STAGES = ("read", "normalize", "features", "counts", "groups", "report")
DEPS = ((), (0,), (1,), (2,), (2,), (3, 4))
WORKLOADS = ("log_etl", "document_minhash", "wide_tokens")
POLICIES = ("recompute", "checkpoint_all", "greedy_selective", "optimal_selective")
# Fail after a completed stage and its selected checkpoint have been committed.
FAULTS = {"healthy": None, "after_normalize": 1, "after_features": 2, "after_counts": 3}
PRIME = (1 << 61) - 1
PERMUTATIONS = tuple((2 * i + 3, (i + 1) * 104729) for i in range(32))


def atomic_json(path, value):
    path = Path(path)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temp, path)


def append_json(path, value):
    with Path(path).open("a") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()


def read_rows(path):
    if not Path(path).exists():
        return []
    rows = []
    for line in Path(path).read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            # A watchdog may interrupt the final append; preserve complete lines.
            break
    return rows


def make_input(path, documents, tokens, seed):
    """Synthetic records; fixed, useful operators, not padded computation."""
    rng = random.Random(seed)
    vocabulary = ("cloud", "river", "invoice", "customer", "market", "order",
                  "sensor", "weather", "service", "region", "account", "energy")
    with gzip.open(path, "wt", encoding="utf-8") as stream:
        for i in range(documents):
            words = [rng.choice(vocabulary) + str(rng.randrange(128))
                     for _ in range(tokens)]
            row = {"id": i, "category": "group_" + str(i % 12),
                   "text": " ".join(words), "status": 200 if i % 7 else 404}
            stream.write(json.dumps(row) + "\n")


def compute(stage, parents, cfg):
    kind = cfg["workload"]
    if stage == 0:
        with gzip.open(cfg["input"], "rt", encoding="utf-8") as stream:
            return [json.loads(line) for line in stream]
    if stage == 1:
        return [(r["id"], r["category"], " ".join(re.findall(r"[a-z0-9]+", r["text"].lower())))
                for r in parents[0] if r["status"] == 200]
    if stage == 2:
        rows = []
        for ident, category, text in parents[0]:
            words = text.split()
            if kind == "document_minhash":
                # Word 3-shingles and 32 MinHash components: actual document
                # feature extraction. This is not a full deduplication system.
                shingles = {int.from_bytes(hashlib.blake2b(
                    " ".join(words[j:j + 3]).encode(), digest_size=8).digest(), "big") % PRIME
                    for j in range(max(0, len(words) - 2))}
                signature = tuple(min(((a * x + b) % PRIME for x in shingles), default=0)
                                  for a, b in PERMUTATIONS)
                rows.append((ident, category, signature, len(words)))
            elif kind == "wide_tokens":
                # A concordance-style exploded table carrying a context window.
                rows.extend((ident, category, pos, token,
                             " ".join(words[max(0, pos - 4):pos + 5]))
                            for pos, token in enumerate(words))
            else:
                rows.append((ident, category, tuple(words)))
        return rows
    if stage == 3:
        counts = collections.Counter()
        for row in parents[0]:
            if kind == "document_minhash":
                # First four components define an LSH-style signature bucket.
                counts.update(["-".join(str(x) for x in row[2][:4])])
            elif kind == "wide_tokens":
                counts.update([row[3]])
            else:
                counts.update(row[2])
        return dict(counts)
    if stage == 4:
        groups = collections.defaultdict(lambda: [0, 0])
        for row in parents[0]:
            groups[row[1]][0] += 1
            groups[row[1]][1] += (row[3] if kind == "document_minhash"
                                  else len(row[4]) if kind == "wide_tokens" else len(row[2]))
        return dict(groups)
    counts, groups = parents
    # Hash all aggregate contents; checking only row counts would miss errors.
    canonical = json.dumps({"counts": counts, "groups": groups}, sort_keys=True,
                           separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(canonical).hexdigest(),
            "distinct_keys": len(counts), "total_count": sum(counts.values()),
            "groups": groups}


def save_checkpoint(path, value):
    """Only load this benchmark's own locally generated pickle files."""
    temp = path.with_suffix(".tmp")
    with temp.open("wb") as stream:
        pickle.dump(value, stream, protocol=5)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    directory = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def worker(cfg):
    cache = {}
    durable = Path(cfg["checkpoints"])
    durable.mkdir(exist_ok=True)
    events = Path(cfg["events"])
    protected = set(cfg["protected"]) | {5}  # Same durable final output for all.
    start_index = cfg.get("start_index", 0)

    def ensure(stage):
        if stage in cache:
            return cache[stage]
        path = durable / (STAGES[stage] + ".pickle")
        if path.exists():
            began = time.perf_counter()
            with path.open("rb") as stream:
                value = pickle.load(stream)
            append_json(events, {"type": "restore", "stage": stage,
                                 "seconds": time.perf_counter() - began,
                                 "bytes": path.stat().st_size})
        else:
            parents = [ensure(parent) for parent in DEPS[stage]]
            began = time.perf_counter()
            value = compute(stage, parents, cfg)
            append_json(events, {"type": "compute", "stage": stage,
                                 "seconds": time.perf_counter() - began,
                                 "recomputed": stage < start_index})
            if stage in protected:
                began = time.perf_counter()
                save_checkpoint(path, value)
                append_json(events, {"type": "save", "stage": stage,
                                     "seconds": time.perf_counter() - began,
                                     "bytes": path.stat().st_size})
                if cfg.get("profile"):
                    began = time.perf_counter()
                    with path.open("rb") as stream:
                        restored = pickle.load(stream)
                    seconds = time.perf_counter() - began
                    if restored != value:
                        raise RuntimeError("Checkpoint round-trip changed stage output")
                    del restored
                    append_json(events, {"type": "profile_read", "stage": stage,
                                         "seconds": seconds})
        cache[stage] = value
        return value

    for stage in range(start_index, len(STAGES)):
        ensure(stage)
        append_json(events, {"type": "stage_complete", "stage": stage})
        if stage == cfg.get("fault_after"):
            append_json(events, {"type": "fault", "stage": stage,
                                 "mechanism": "os._exit(75); all worker memory lost"})
            os._exit(75)
    atomic_json(cfg["result"], cache[5])


def run_worker(cfg, jobdir, attempt):
    config = jobdir / ("worker-%d.json" % attempt)
    atomic_json(config, cfg)
    with (jobdir / ("worker-%d.log" % attempt)).open("w") as log:
        # Inherit the controller's process group so the outer watchdog kills
        # the controller AND its active worker on timeout or interruption.
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                                    "--worker", str(config)], stdout=log,
                                   stderr=subprocess.STDOUT)
        timed_out = threading.Event()

        def expire():
            if process.poll() is None:
                timed_out.set()
                process.kill()

        timer = threading.Timer(120, expire)
        timer.daemon = True
        timer.start()
        try:
            # Blocking wait avoids the polling quantization of wait(timeout=)
            # in the small measured runs; the timer enforces the same deadline.
            code = process.wait()
            if timed_out.is_set():
                raise RuntimeError("An individual worker exceeded 120 seconds; reduce --documents")
            return code
        finally:
            timer.cancel()


def execute(cfg, jobdir, protected, fault, profile=False):
    jobdir.mkdir()
    cfg = dict(cfg, protected=list(protected), fault_after=fault, profile=profile,
               checkpoints=str(jobdir / "checkpoints"), events=str(jobdir / "events.jsonl"),
               result=str(jobdir / "result.json"), start_index=0)
    began = time.perf_counter()
    code = run_worker(cfg, jobdir, 0)
    restart_s = None
    if fault is not None:
        rows = read_rows(cfg["events"])
        if code != 75 or not rows or rows[-1] != {
                "type": "fault", "stage": fault,
                "mechanism": "os._exit(75); all worker memory lost"}:
            raise RuntimeError("Expected worker loss did not occur at the requested boundary")
        restart_s = time.perf_counter() - began
        cfg.update(start_index=fault + 1, fault_after=None)
        code = run_worker(cfg, jobdir, 1)
    elapsed = time.perf_counter() - began
    if code:
        raise RuntimeError("Worker exited with code %s; see %s" % (code, jobdir))
    result = json.loads(Path(cfg["result"]).read_text())
    rows = read_rows(cfg["events"])
    return {"elapsed_s": elapsed, "recovery_s": None if restart_s is None else elapsed - restart_s,
            "result": result, "events": rows,
            "compute_s": sum(r["seconds"] for r in rows if r["type"] == "compute"),
            "recompute_s": sum(r["seconds"] for r in rows
                               if r["type"] == "compute" and r["recomputed"]),
            "checkpoint_s": sum(r["seconds"] for r in rows if r["type"] == "save"),
            "restore_s": sum(r["seconds"] for r in rows if r["type"] == "restore"),
            "checkpoint_bytes_written": sum(r["bytes"] for r in rows if r["type"] == "save"),
            "restore_bytes": sum(r["bytes"] for r in rows if r["type"] == "restore"),
            "fault_injected": fault is not None}


def model_time(profile, protected, fault):
    """Replay DAG dependencies with volatile memory loss and surviving saves."""
    cached, durable = set(), set()
    total = 0.0
    protected = set(protected) | {5}

    def ensure(stage):
        nonlocal total
        if stage in cached:
            return
        if stage in durable:
            total += profile[stage]["read_s"]
        else:
            for parent in DEPS[stage]:
                ensure(parent)
            total += profile[stage]["compute_s"]
            if stage in protected:
                total += profile[stage]["write_s"]
                durable.add(stage)
        cached.add(stage)

    for stage in range(6):
        ensure(stage)
        if stage == fault:
            cached.clear()
    return total


def choose_plans(profile, probability):
    def score(mask):
        healthy = model_time(profile, mask, None)
        failed = statistics.mean(model_time(profile, mask, f)
                                 for f in FAULTS.values() if f is not None)
        return ((1 - probability) * healthy + probability * failed, len(mask), mask)

    masks = [tuple(i for i in range(5) if bits & (1 << i)) for bits in range(32)]
    all_stages = tuple(range(5))
    local = []
    for seed in ((), all_stages):
        current = seed
        while True:
            neighbors = [tuple(sorted(set(current) ^ {i})) for i in range(5)]
            best = min([current] + neighbors, key=score)
            if score(best) >= score(current):
                break
            current = best
        local.append(current)
    plans = {"recompute": (), "checkpoint_all": all_stages,
             "greedy_selective": min(local, key=score),
             "optimal_selective": min(masks, key=score)}
    return {name: {"protected": list(mask), "protected_names": [STAGES[i] for i in mask],
                   "predicted_expected_s": score(mask)[0]}
            for name, mask in plans.items()}


def summarize(output, manifest, status, error=None):
    rows = read_rows(output / "runs.jsonl")
    valid = [r for r in rows if r.get("valid")]
    indexed = {(r["workload"], r["repeat"], r["scenario"], r["policy"]): r for r in valid}
    probability = manifest["failure_probability"]
    summaries, comparisons = [], []
    complete_blocks = {}
    for workload in manifest["workloads"]:
        repeats = [rep for rep in range(manifest["repeats"])
                   if all((workload, rep, scenario, policy) in indexed
                          for scenario in FAULTS for policy in POLICIES)]
        complete_blocks[workload] = repeats
        metrics = {}
        for policy in POLICIES:
            metrics[policy] = []
            for rep in repeats:
                healthy = indexed[workload, rep, "healthy", policy]["elapsed_s"]
                failed = statistics.mean(indexed[workload, rep, s, policy]["elapsed_s"]
                                         for s in FAULTS if s != "healthy")
                metrics[policy].append((healthy, failed,
                                        (1 - probability) * healthy + probability * failed))
            if repeats:
                means = [statistics.mean(x[k] for x in metrics[policy]) for k in range(3)]
                summaries.append({"workload": workload, "policy": policy, "repeats": len(repeats),
                                  "healthy_s": means[0], "failure_s": means[1],
                                  "weighted_expected_s": means[2]})
        pairs = [(policy, "recompute") for policy in POLICIES[1:]]
        pairs += [(policy, "checkpoint_all") for policy in POLICIES[2:]]
        pairs += [("optimal_selective", "greedy_selective")]
        for policy, baseline in pairs:
            if not repeats:
                continue
            a, b = metrics[baseline], metrics[policy]
            gains = [100 * (base[2] - candidate[2]) / base[2] for base, candidate in zip(a, b)]
            mean = statistics.mean(gains)
            # Default 3 paired blocks. Do not fabricate a CI for a partial run.
            half = 4.302652729911275 * statistics.stdev(gains) / math.sqrt(3) if len(gains) == 3 else None
            healthy_gain = statistics.mean(x[0] - y[0] for x, y in zip(a, b))
            failure_gain = statistics.mean(x[1] - y[1] for x, y in zip(a, b))
            slope = failure_gain - healthy_gain
            threshold = -healthy_gain / slope if slope else None
            if healthy_gain >= 0 and failure_gain >= 0:
                regime = "no slower for all assumed failure probabilities"
            elif healthy_gain <= 0 and failure_gain <= 0:
                regime = "no faster for any assumed failure probability"
            else:
                regime = "faster when failure probability %s %.6f" % (">" if slope > 0 else "<", threshold)
            comparisons.append({"workload": workload, "policy": policy,
                                "baseline": baseline, "paired_gain_percent": gains,
                                "mean_paired_gain_percent": mean,
                                "exploratory_95_ci_percent": None if half is None else [mean - half, mean + half],
                                "break_even_regime": regime})
    summary = {"status": status, "error": error, "planned_runs": manifest["planned_runs"],
               "recorded_runs": len(rows), "valid_runs": len(valid),
               "complete_blocks": complete_blocks, "failure_probability": probability,
               "aggregates": summaries, "paired_comparisons": comparisons,
               "interpretation": "Local process-loss mechanism test, not Ray/cluster recovery or novelty evidence. "
               "Only complete matched workload/repeat blocks enter aggregates. Failure probability is assumed."}
    atomic_json(output / "summary.json", summary)
    atomic_json(output / "status.json", {"status": status, "valid_runs": len(valid),
                                         "planned_runs": manifest["planned_runs"], "error": error})
    fields = ("workload", "repeat", "scenario", "policy", "valid", "elapsed_s", "recovery_s",
              "compute_s", "recompute_s", "checkpoint_s", "restore_s",
              "checkpoint_bytes_written", "restore_bytes", "fault_injected")
    with (output / "runs.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    lines = ["# Data-processing selective recovery", "", "Status: **%s**; valid runs: %d/%d." %
             (status, len(valid), manifest["planned_runs"]), "",
             "Measured process-wall times include startup, checkpointing and recovery. "
             "Failure means one abrupt worker loss at a completed-stage boundary. "
             "Inputs and committed checkpoints survive. All failed runs must recover the exact output.", "",
             "Expected time uses an **assumed %.1f%%** failure probability and equal weights for the three "
             "failure boundaries. This is not an observed failure rate." % (100 * probability), "",
             "Only complete matched blocks enter this table; all observations remain in runs.jsonl.", "",
             "| Workload | Policy | Repeats | Healthy (s) | With failure (s) | Weighted expected (s) |",
             "|---|---|---:|---:|---:|---:|"]
    for r in summaries:
        lines.append("| {workload} | {policy} | {repeats} | {healthy_s:.4f} | {failure_s:.4f} | "
                     "{weighted_expected_s:.4f} |".format(**r))
    lines += ["", "## Paired comparisons", "",
              "Positive values mean lower expected time. Three repetitions provide only exploratory uncertainty; "
              "the t intervals assume independent, approximately normal paired percentage differences.", ""]
    for r in comparisons:
        interval = r["exploratory_95_ci_percent"]
        ci = "not available (requires all 3 repeats)" if interval is None else "[%.2f, %.2f]%%" % tuple(interval)
        lines.append("- %s / %s versus %s: %.2f%% mean gain; 95%% interval %s; %s." %
                     (r["workload"], r["policy"], r["baseline"], r["mean_paired_gain_percent"], ci, r["break_even_regime"]))
    if error:
        lines += ["", "Error: " + error]
    lines += ["", "Checkpoint selections were frozen using a separate profiling input. "
              "Failure locations were specified in advance and used for planning as well as evaluation. "
              "This is not a held-out failure-distribution experiment. Local storage/page cache, "
              "synthetic inputs, serial scheduling and one-process loss limit generalization. "
              "No distributed billing, deadlines, transfer contention or controller loss are tested."]
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    temp = output / "analysis.zip.tmp"
    with zipfile.ZipFile(temp, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in ("manifest.json", "status.json", "summary.json", "summary.md", "runs.csv", "runs.jsonl",
                     "profiles.json", "controller.log"):
            path = output / name
            if path.exists():
                archive.write(path, name)
        archive.write(__file__, "source/benchmark.py")
        readme = Path(__file__).with_name("README.md")
        if readme.exists():
            archive.write(readme, "source/README.md")
        # Failed/interrupted worker logs are useful for diagnosis; input data
        # and checkpoint payloads are intentionally excluded from this archive.
        for path in (output / "work").glob("*/worker-*.log"):
            archive.write(path, str(path.relative_to(output)))
    os.replace(temp, output / "analysis.zip")
    return summary


def controller(output):
    manifest = json.loads((output / "manifest.json").read_text())
    work = output / "work"
    work.mkdir()
    profiles = {}
    for workload in manifest["workloads"]:
        input_path = work / (workload + "-profile.jsonl.gz")
        make_input(input_path, manifest["documents"], manifest["tokens"], manifest["seed"])
        cfg = {"workload": workload, "input": str(input_path)}
        job = work / (workload + "-profile")
        result = execute(cfg, job, range(5), None, profile=True)
        stages = []
        for stage in range(6):
            events = [r for r in result["events"] if r.get("stage") == stage]
            def timing(kind):
                return sum(r["seconds"] for r in events if r["type"] == kind)
            stages.append({"stage": STAGES[stage], "compute_s": timing("compute"),
                           "write_s": timing("save"), "read_s": timing("profile_read"),
                           "bytes": sum(r["bytes"] for r in events if r["type"] == "save")})
        began = time.perf_counter()
        plans = choose_plans(stages, manifest["failure_probability"])
        profiles[workload] = {"stages": stages, "plans": plans,
                              "planning_s": time.perf_counter() - began,
                              "profile_elapsed_s": result["elapsed_s"], "profile_seed": manifest["seed"]}
        atomic_json(output / "profiles.json", profiles)
        shutil.rmtree(job)
        input_path.unlink()
        print("Profiled %s; checkpoints: %s" % (workload, {k: v["protected_names"] for k, v in plans.items()}), flush=True)
    rng = random.Random(manifest["seed"])
    blocks = list(itertools.product(range(manifest["repeats"]), manifest["workloads"]))
    rng.shuffle(blocks)
    completed = 0
    for repeat, workload in blocks:
        input_path = work / "evaluation.jsonl.gz"
        seed = manifest["seed"] + 1000 + repeat
        make_input(input_path, manifest["documents"], manifest["tokens"], seed)
        digest = hashlib.sha256(input_path.read_bytes()).hexdigest()
        cfg = {"workload": workload, "input": str(input_path)}
        jobs = list(itertools.product(FAULTS, POLICIES))
        rng.shuffle(jobs)
        reference = None
        for scenario, policy in jobs:
            job = work / ("run-%03d" % completed)
            plan = profiles[workload]["plans"][policy]
            result = execute(cfg, job, plan["protected"], FAULTS[scenario])
            if reference is None:
                reference = result["result"]
            valid = result["result"] == reference
            row = dict(result, workload=workload, repeat=repeat, scenario=scenario, policy=policy,
                       valid=valid, input_seed=seed, input_sha256=digest, protected=plan["protected_names"])
            append_json(output / "runs.jsonl", row)
            if not valid:
                raise RuntimeError("Output mismatch for %s/%d/%s/%s" % (workload, repeat, scenario, policy))
            completed += 1
            print("%d/%d %s repeat=%d %s %s %.3fs" %
                  (completed, manifest["planned_runs"], workload, repeat + 1, scenario, policy,
                   result["elapsed_s"]), flush=True)
            shutil.rmtree(job)
        # The first result may have come from recovery; recompute/healthy must
        # also match it before this block can enter any reported aggregate.
        summarize(output, manifest, "running")
        input_path.unlink()
    summarize(output, manifest, "complete")


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    # Kill remaining children even if the controller already exited.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--documents", type=int, default=4000)
    parser.add_argument("--tokens", type=int, default=96)
    parser.add_argument("--repeats", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--failure-probability", type=float, default=0.10)
    parser.add_argument("--max-minutes", type=float, default=28)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS))
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--controller", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(args.worker.read_text()))
        return 0
    if args.controller:
        try:
            controller(args.controller)
            return 0
        except Exception:
            traceback.print_exc()
            return 1
    if os.name != "posix":
        parser.error("Requires Linux/macOS process groups; run in your ray-dev environment")
    if not (1 <= args.documents <= 50000 and 3 <= args.tokens <= 512):
        parser.error("Use 1..50000 documents and 3..512 tokens")
    if not 0 <= args.failure_probability <= 1 or not 0 < args.max_minutes <= 29:
        parser.error("Failure probability must be in [0,1]; max-minutes must be in (0,29]")
    if len(args.workloads) != len(set(args.workloads)):
        parser.error("Do not repeat workload names")
    output = (args.output or Path("ray_gcp_results/data-processing-recovery") /
              (time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])).resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = {"schema_version": 1, "documents": args.documents, "tokens": args.tokens,
                "workloads": args.workloads, "repeats": args.repeats, "seed": args.seed,
                "failure_probability": args.failure_probability, "max_minutes": args.max_minutes,
                "planned_runs": len(args.workloads) * args.repeats * len(FAULTS) * len(POLICIES),
                "python": sys.version, "platform": platform.platform(),
                "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "policies": list(POLICIES), "failure_boundaries": FAULTS,
                "command": sys.argv, "fault_scope": "single worker process; input/checkpoint disk/controller survive"}
    atomic_json(output / "manifest.json", manifest)
    summarize(output, manifest, "starting")
    print("Output: %s\nUp to %d evaluations; controller budget %.1f minutes." %
          (output, manifest["planned_runs"], args.max_minutes), flush=True)
    status, error, exit_code = "failed", None, 1
    with (output / "controller.log").open("w") as log:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                                    "--controller", str(output)], stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
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
                        error = "Controller exited %d; see controller.log and any retained work/run-* logs" % code
                    break
                except subprocess.TimeoutExpired:
                    progress = read_rows(output / "runs.jsonl")
                    print("Progress: %d/%d observations (%.1f minutes)" %
                          (len(progress), manifest["planned_runs"], (time.monotonic() - began) / 60), flush=True)
        except subprocess.TimeoutExpired:
            status, error, exit_code = "time_limit", "Time budget reached; partial results retained", 124
        except KeyboardInterrupt:
            status, error, exit_code = "interrupted", "Interrupted; partial results retained", 130
        finally:
            stop_group(process)
    summary = summarize(output, manifest, status, error)
    print("%s: %d/%d valid observations.\nRead: %s\nShare: %s" %
          (status, summary["valid_runs"], manifest["planned_runs"], output / "summary.md", output / "analysis.zip"))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
