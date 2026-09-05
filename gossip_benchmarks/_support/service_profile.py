#!/usr/bin/env python3
"""Native service counters and asynchronous control timings for both methods.

Run through 03_profile.py. Retain references until control work is quiescent.
Save every exported per-role counter, including elapsed service, copies/bytes,
fan-out, callback phases, readiness and state gauges. Nested timings overlap;
they are not an additive CPU subtotal. Diagnostic, not throughput acceptance."""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

os.environ.setdefault("RAY_BACKEND_LOG_LEVEL", "warning")
os.environ.setdefault("RAY_DEDUP_LOGS", "1")
os.environ["RAY_RECOVERY_PROFILING"] = "1"
os.environ["RAY_RECOVERY_CERTIFICATE_ADMISSION"] = "0"
os.environ["RAY_RECOVERY_TASKMANAGER_PIN"] = "0"
os.environ["RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE"] = "0"

HERE = Path(__file__).resolve().parent
VARIANTS = tuple(v for k in (1, 2, 4, 8, 16, 32) for v in (("fixed_r" if k == 1 else f"fixed_k{k}"), f"succession_k{k}"))
PROFILE_FIELDS = (
    "frontier_recipe_encode_calls",
    "frontier_recipe_encode_time_ns",
    "frontier_recipe_encode_members",
    "frontier_recipe_encode_bytes",
    "holder_install_handler_calls",
    "holder_install_handler_time_ns",
    "frontier_holder_materialize_calls",
    "frontier_holder_materialize_time_ns",
    "frontier_holder_materialize_members",
    "holder_install_callback_calls",
    "holder_install_callback_time_ns",
    "frontier_recipe_piggybacks_sent",
    "frontier_recipe_piggyback_bytes_sent",
    "frontier_recipe_piggybacks_stored",
    "frontier_recipe_piggyback_store_time_ns",
    "frontier_recipe_piggyback_admissions",
)


def _load_b59():
    path = HERE / "comparison.py"
    spec = importlib.util.spec_from_file_location("initial_install_profile_b59", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


b59 = _load_b59()
b58 = b59.b58

import ray  # noqa: E402
from ray._private.worker import global_worker  # noqa: E402
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy  # noqa: E402
from common import safe_shutdown, wait_for_cluster  # noqa: E402


def _profile(raw: Any, role: str) -> dict[str, int]:
    profile = {key: int(value) for key, value in dict(raw).items()}
    missing = set(PROFILE_FIELDS) - profile.keys()
    if profile.get("initial_install_profile_version") != 3 or missing:
        raise RuntimeError(
            f"{role}: initial installation counters unavailable; rebuild Ray "
            f"and check the imported ray._raylet binary. Missing: {sorted(missing)}"
        )
    return profile


def _owner_profile() -> dict[str, int]:
    return _profile(
        global_worker.core_worker.get_recovery_succession_profile(), "owner"
    )


def _sum_profiles(profiles: list[dict[str, int]]) -> dict[str, int]:
    # Maxima/gauges remain available in the individual raw profiles. Only sums
    # used by the report are interpreted as counters; never sum a version.
    return {
        key: sum(profile[key] for profile in profiles)
        for key in profiles[0]
        if key != "initial_install_profile_version"
    }


def _balanced(profile: dict[str, int]) -> bool:
    return all(profile[sent] == profile[done] for sent, done in b58.ASYNC_PAIRS)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def _coverage_errors(result: dict[str, Any]) -> list[str]:
    owner, borrowers = result["owner"], result["borrower_total"]
    groups, k = result["groups"], result["k"]
    adaptive = result["variant"].startswith("succession")
    expected = {
        "candidate_reports_accepted": 2 * groups if adaptive else 0,
        "holder_admissions_committed": 2 * groups if adaptive else 0,
        "witness_update_rpcs_completed": (4 if adaptive else 2) * groups,
    }
    if k > 1 and not adaptive:
        expected["holder_install_rpcs_completed"] = 0
        expected["frontier_recipe_encode_calls"] = groups
        expected["frontier_recipe_encode_members"] = result["tasks"]
    elif k == 1:
        expected["frontier_recipe_encode_calls"] = 0
    errors = [
        f"owner {key}: expected {value}, got {owner[key]}"
        for key, value in expected.items()
        if owner[key] != value
    ]
    installs = owner["holder_install_rpcs_completed"]
    if borrowers["holder_install_handler_calls"] != installs:
        errors.append("borrower install-handler coverage does not match owner completions")
    if owner["holder_install_callback_calls"] != installs:
        errors.append("owner install-callback coverage does not match completions")
    if adaptive and k > 1:
        piggyback_admissions = owner["frontier_recipe_piggyback_admissions"]
        piggyback_stores = borrowers["frontier_recipe_piggybacks_stored"]
        if installs + piggyback_admissions != 2 * groups:
            errors.append("RPC installs plus verified piggyback admissions do not match R=2")
        if piggyback_admissions > piggyback_stores:
            errors.append("more piggyback admissions than receiver storage events")
        materializations = installs + piggyback_stores
        if borrowers["frontier_holder_materialize_calls"] != materializations:
            errors.append("materialization coverage does not match RPC and piggyback stores")
        if borrowers["frontier_holder_materialize_members"] != k * materializations:
            errors.append("materialized member count does not match the measured stores")
        builds = installs + owner["frontier_recipe_piggybacks_sent"]
        if owner["frontier_recipe_encode_calls"] != builds:
            errors.append("recipe build count does not match sends and fallback installs")
        if owner["frontier_recipe_encode_members"] != k * builds:
            errors.append("recipe build member count does not match full groups")
    if owner["witness_update_handler_samples"] != owner["witness_update_rpcs_completed"]:
        errors.append(
            "witness receiver timing coverage is incomplete; rebuild the raylet "
            "as well as CoreWorker and check that profiling is enabled on all nodes"
        )
    if not _balanced(owner) or not _balanced(borrowers):
        errors.append("asynchronous control counters are not balanced")
    return errors


def single_profile(args: argparse.Namespace) -> dict[str, Any]:
    variant = args.single_variant
    k = b59.k_for(variant)
    groups = args.tasks // k
    adaptive = variant.startswith("succession")
    expected_admissions = 2 * groups if adaptive else 0
    expected_witness_updates = (4 if adaptive else 2) * groups
    cluster = None
    try:
        print(f"  {variant}: starting fresh cluster", flush=True)
        cluster, producer_node = b58.start_cluster(
            SimpleNamespace(holders=2, witness_count=2, cpus_per_node=args.cpus_per_node),
            variant,
            profiling=True,
        )
        ray.init(address=cluster.address, log_to_driver=False, include_dashboard=False)
        wait_for_cluster(ray, 6, args.cluster_timeout_seconds)
        produce, Borrower = b58.remote_types()
        borrowers = [
            Borrower.options(resources={f"borrower_node_{i}": 0.01}, num_cpus=0).remote()
            for i in range(2)
        ]
        ray.get([borrower.ping.remote() for borrower in borrowers], timeout=args.timeout_seconds)
        # Detect an old native build before submitting diagnostic producer work.
        _owner_profile()
        for i, raw in enumerate(ray.get(
            [borrower.profile.remote() for borrower in borrowers], timeout=args.timeout_seconds
        )):
            _profile(raw, f"borrower {i}")
        global_worker.core_worker.reset_recovery_succession_profile()
        ray.get(
            [borrower.reset_profile.remote() for borrower in borrowers],
            timeout=args.timeout_seconds,
        )
        strategy = NodeAffinitySchedulingStrategy(node_id=producer_node, soft=False)
        padding = b58.build_padding(args.task_spec_padding_bytes, args.inline_chunk_bytes)
        print(f"  {variant}: registering {args.tasks} producers, then exporting references", flush=True)
        refs = [
            produce.options(scheduling_strategy=strategy, num_cpus=1).remote(
                74_000_000 + i, args.payload_bytes, *padding
            )
            for i in range(args.tasks)
        ]
        start_ns = time.perf_counter_ns()
        deliveries = [
            borrower.hold.remote([ref]) for ref in refs for borrower in borrowers
        ]
        held = ray.get(deliveries, timeout=args.timeout_seconds)
        delivery_ms = (time.perf_counter_ns() - start_ns) / 1e6
        expected_ids = [[ref.hex()] for ref in refs for _ in borrowers]
        if held != expected_ids:
            raise RuntimeError("borrower did not receive the exact live references")
        # Producer completion cannot release the retained owner/borrower refs.
        ray.get(refs, timeout=args.timeout_seconds)
        print(f"  {variant}: waiting for installation and witness callbacks", flush=True)
        deadline = time.monotonic() + args.timeout_seconds
        stable_since = None
        last_signature = None
        owner, borrower_profiles, borrower_total = {}, [], {}
        quiesced = False
        # profile.remote() itself creates actor calls. Do not use all profile
        # counters for stability: generic task/argument counters keep changing.
        owner_stability_keys = (
            "candidate_reports_received", "candidate_reports_accepted",
            "holder_admissions_committed", "witness_publish_count",
            "witness_logical_callback_cpu_calls", *PROFILE_FIELDS,
            *(key for pair in b58.ASYNC_PAIRS for key in pair),
        )
        borrower_stability_keys = (
            *PROFILE_FIELDS,
            *(key for pair in b58.ASYNC_PAIRS for key in pair),
        )
        while time.monotonic() < deadline:
            owner = _owner_profile()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            borrower_profiles = [
                _profile(raw, f"borrower {i}")
                for i, raw in enumerate(ray.get(
                    [borrower.profile.remote() for borrower in borrowers],
                    timeout=remaining,
                ))
            ]
            borrower_total = _sum_profiles(borrower_profiles)
            ready = (
                owner["holder_admissions_committed"] >= expected_admissions
                and owner["witness_update_rpcs_completed"] >= expected_witness_updates
                and _balanced(owner)
                and _balanced(borrower_total)
                and owner["holder_install_callback_calls"] == owner["holder_install_rpcs_completed"]
            )
            signature = (
                tuple(owner[key] for key in owner_stability_keys),
                tuple(borrower_total[key] for key in borrower_stability_keys),
            )
            now = time.monotonic()
            if ready:
                if signature != last_signature or stable_since is None:
                    stable_since = now
                elif now - stable_since >= args.stable_seconds:
                    quiesced = True
                    break
            else:
                stable_since = None
            last_signature = signature
            time.sleep(0.02)
        if not quiesced:
            raise TimeoutError(
                f"{variant}: installation did not quiesce; owner={owner}; "
                f"borrowers={borrower_total}"
            )
        result = {
            "variant": variant,
            "k": k,
            "tasks": args.tasks,
            "groups": groups,
            "profiling": True,
            "r": 2,
            "w": 2,
            "delivery_ms": delivery_ms,
            "payload_bytes": args.payload_bytes,
            "task_spec_padding_bytes": args.task_spec_padding_bytes,
            "ray_version": ray.__version__,
            "ray_commit": getattr(ray, "__commit__", "unknown"),
            "raylet_extension": str(ray._raylet.__file__),
            "owner": owner,
            "borrowers": borrower_profiles,
            "borrower_total": borrower_total,
        }
        result["coverage_errors"] = _coverage_errors(result)
        # Preserve raw measurements even when coverage/count validation fails.
        _write_json(Path(args.single_output_json), result)
        if result["coverage_errors"]:
            raise RuntimeError(
                f"{variant}: cannot interpret initial installation profile: "
                + "; ".join(result["coverage_errors"])
                + f". Raw snapshot: {args.single_output_json}"
            )
        ray.get([borrower.clear.remote() for borrower in borrowers], timeout=args.timeout_seconds)
        return result
    finally:
        safe_shutdown(ray, cluster)


# scope, label, accumulated time, matching invocation/sample count.
STAGES = (
    ("owner", "initial manifest build", "initial_manifest_build_time_ns", "initial_manifest_build_count"),
    ("owner", "K=1 piggyback serialization", "first_holder_piggyback_serialize_time_ns", "first_holder_piggyback_copies_sent"),
    ("owner", "Frontier recipe build/encode", "frontier_recipe_encode_time_ns", "frontier_recipe_encode_calls"),
    ("owner", "owner admission preparation", "holder_admission_prepare_cpu_time_ns", "holder_admission_prepare_cpu_calls"),
    ("borrower_total", "provisional piggyback store", "frontier_recipe_piggyback_store_time_ns", "frontier_recipe_piggybacks_stored"),
    ("borrower_total", "holder install handler", "holder_install_handler_time_ns", "holder_install_handler_calls"),
    ("borrower_total", "  Frontier member materialization", "frontier_holder_materialize_time_ns", "frontier_holder_materialize_calls"),
    ("owner", "owner install callback", "holder_install_callback_time_ns", "holder_install_callback_calls"),
    ("owner", "owner witness request build", "witness_request_build_cpu_time_ns", "witness_request_build_cpu_calls"),
    ("owner", "witness receiver (amortized)", "witness_update_handler_time_ns", "witness_update_handler_samples"),
    ("owner", "owner witness callback", "witness_logical_callback_cpu_time_ns", "witness_logical_callback_cpu_calls"),
    ("owner", "holder install RPC RTT", "holder_install_rpc_time_ns", "holder_install_rpcs_completed"),
    ("owner", "witness update RPC RTT", "witness_update_rpc_time_ns", "witness_update_rpcs_completed"),
    ("owner", "witness publication stage", "witness_publish_time_ns", "witness_publish_count"),
    ("owner", "whole holder admission", "holder_admission_time_ns", "holder_admissions_committed"),
    ("owner", "witness client batch queue", "witness_update_client_queue_time_ns", "witness_update_rpcs_completed"),
    ("owner", "witness submit -> CQ", "witness_update_client_submit_to_cq_time_ns", "witness_update_client_phase_samples"),
    ("owner", "witness CQ -> main loop", "witness_update_client_cq_to_main_loop_time_ns", "witness_update_client_phase_samples"),
)


def report(results: list[dict[str, Any]], output_dir: Path) -> None:
    rows = []
    print("\nFinal initial installation profile (service elapsed time, not CPU):")
    for result in results:
        owner = result["owner"]
        tasks = result["tasks"]
        print(f"\n  {result['variant']}: K={result['k']} tasks={tasks} groups={result['groups']}")
        print(f"    reference delivery={result['delivery_ms']:.2f} ms (diagnostic)")
        for key in (
            "candidate_reports_received", "holder_install_rpcs_sent",
            "witness_update_rpcs_sent", "holder_admissions_committed",
            "task_spec_bytes_sent", "manifest_bytes_sent",
            "first_holder_piggyback_copies_sent", "first_holder_piggyback_bytes_sent",
            "frontier_recipe_piggybacks_sent", "frontier_recipe_piggyback_bytes_sent",
            "frontier_recipe_piggyback_admissions",
            "frontier_recipe_encode_members", "frontier_recipe_encode_bytes",
            "witness_update_physical_batches_completed",
        ):
            print(f"    {key:43s} {owner[key]:8d}  {owner[key] / tasks:9.3f}/task")
        print(
            "    borrower Frontier piggyback stores: "
            f"{result['borrower_total']['frontier_recipe_piggybacks_stored']}"
        )
        print(
            "    witness client/server timing coverage: "
            f"{owner['witness_update_client_phase_samples']}/"
            f"{owner['witness_update_rpcs_completed']} client; "
            f"{owner['witness_update_handler_samples']}/"
            f"{owner['witness_update_rpcs_completed']} server"
        )
        for scope, label, time_key, count_key in STAGES:
            profile = result[scope]
            calls = profile[count_key]
            total_us = profile[time_key] / 1e3
            average = total_us / calls if calls else None
            per_task = total_us / tasks if calls else None
            print(
                f"    {label:36s} "
                + (f"{average:9.2f} us/call  {per_task:9.2f} us/task  n={calls}"
                   if calls else "      N/A (no calls)")
            )
            rows.append({
                "variant": result["variant"], "k": result["k"], "tasks": tasks,
                "scope": scope, "stage": label.strip(), "calls": calls,
                "service_us_per_call": average, "accumulated_us_per_task": per_task,
            })
    with (output_dir / "initial_install_stages.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    _write_json(output_dir / "initial_install_profiles.json", results)
    print("\nInterpretation:")
    print("  Fixed-R installs full recipes on witnesses; Succession installs on borrowers.")
    print("  K=1 Succession uses TaskSpec piggybacks; full K>1 groups can piggyback recipes.")
    print("  Frontier admission can mix verified recipe piggybacks and ordinary install fallback.")
    print("  Piggyback recipe-build timing includes recipe serialization, not the later PushTask envelope.")
    print("  Encoding, callbacks, handler work and admission latencies overlap; do not sum them.")
    print("  Elapsed service includes lock waits/preemption; it is not process/thread CPU.")
    print("  Use concentrated service/byte costs to choose the next source change.")
    print("  Use profiling-OFF Benchmark 01 to judge throughput; these are single diagnostic snapshots at each selected K.")
    print(f"  Raw profiles and stage CSV: {output_dir}")


def run(args: argparse.Namespace) -> None:
    output_dir = args.output_dir.resolve()
    variants = [v for v in VARIANTS if b59.k_for(v) in args.ks]
    paths = [output_dir / f"{variant}.json" for variant in variants]
    paths += [output_dir / "initial_install_profiles.json", output_dir / "initial_install_stages.csv"]
    existing = [path for path in paths if path.exists()]
    if existing and not args.overwrite:
        raise FileExistsError(f"results already exist in {output_dir}; use --overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Remove only this benchmark's named outputs, never an arbitrary directory.
    for path in existing:
        path.unlink()
    print("Initial installation diagnostic: Fixed-R and Succession, K=1/2/4/8/16/32, R=2 W=2", flush=True)
    print("  Profiling ON; fresh cluster per case; no throughput acceptance measurement", flush=True)
    results = []
    for i, variant in enumerate(variants, 1):
        path = output_dir / f"{variant}.json"
        print(f"[{i}/{len(variants)}] {variant}", flush=True)
        command = [
            sys.executable, "-u", str(Path(__file__).resolve()),
            "--single-variant", variant, "--single-output-json", str(path),
        ]
        for name in (
            "tasks", "payload_bytes", "task_spec_padding_bytes", "inline_chunk_bytes",
            "cpus_per_node", "cluster_timeout_seconds", "timeout_seconds", "stable_seconds",
        ):
            command += ["--" + name.replace("_", "-"), str(getattr(args, name))]
        subprocess.run(
            command, env=b58.child_env(profiling=True), check=True,
            timeout=args.case_timeout_seconds,
        )
        results.append(json.loads(path.read_text()))
    counter_rows = []
    for result in results:
        profiles = [("owner", result["owner"])]
        profiles.extend((f"borrower_{i}", profile)
                        for i, profile in enumerate(result["borrowers"]))
        for role, profile in profiles:
            for counter, value in sorted(profile.items()):
                counter_rows.append({"variant": result["variant"], "k": result["k"],
                                     "role": role, "counter": counter, "value": value})
    with (output_dir / "all_counters.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["variant", "k", "role", "counter", "value"])
        writer.writeheader()
        writer.writerows(counter_rows)
    report(results, output_dir)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ks", nargs="+", type=int, choices=(1, 2, 4, 8, 16, 32), default=[1, 2, 4, 8, 16, 32])
    p.add_argument("--tasks", type=int, default=32)
    p.add_argument("--payload-bytes", type=int, default=1024)
    p.add_argument("--task-spec-padding-bytes", type=int, default=1024)
    p.add_argument("--inline-chunk-bytes", type=int, default=4096)
    p.add_argument("--cpus-per-node", type=int, default=4)
    p.add_argument("--cluster-timeout-seconds", type=float, default=30.0)
    p.add_argument("--timeout-seconds", type=float, default=60.0)
    p.add_argument("--stable-seconds", type=float, default=0.25)
    p.add_argument("--case-timeout-seconds", type=float, default=300.0)
    p.add_argument("--output-dir", type=Path, default=HERE.parent / "results" / "profile_service")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--single-variant", choices=VARIANTS, help=argparse.SUPPRESS)
    p.add_argument("--single-output-json", help=argparse.SUPPRESS)
    return p


def main() -> None:
    p = parser()
    args = p.parse_args()
    if args.tasks <= 0 or args.tasks % 32:
        p.error("--tasks must be a positive multiple of 32")
    if args.payload_bytes < 8 or args.task_spec_padding_bytes < 0:
        p.error("payload must be at least 8 bytes; padding must be nonnegative")
    if args.inline_chunk_bytes <= 0 or args.cpus_per_node <= 0:
        p.error("inline chunk size and CPUs per node must be positive")
    for name in ("cluster_timeout_seconds", "timeout_seconds", "stable_seconds", "case_timeout_seconds"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            p.error(f"--{name.replace('_', '-')} must be finite and positive")
    if args.stable_seconds >= args.timeout_seconds:
        p.error("--stable-seconds must be less than --timeout-seconds")
    if bool(args.single_variant) != bool(args.single_output_json):
        p.error("internal single-case options must be supplied together")
    if args.single_variant:
        single_profile(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
