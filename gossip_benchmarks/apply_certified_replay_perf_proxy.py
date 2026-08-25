#!/usr/bin/env python3
"""Apply the R=1 certification-only performance proxy to the Ray checkout.

This is deliberately PERF-ONLY.  When enabled, WitnessBaseline-R1 keeps the
normal baseline manifest selection/publication/ACK/bookkeeping path but omits
transport and storage of the full replayable TaskSpec.  It therefore models the
normal-path cost we would expect if the executor already retained replay state.

It MUST NOT be used for failure/correctness tests: recovery is intentionally
invalid because the witness has no replayable TaskSpec.

Usage:
    python apply_certified_replay_perf_proxy.py /home/tejas/Downloads/ray --check
    python apply_certified_replay_perf_proxy.py /home/tejas/Downloads/ray

Run the performance proxy with:
    RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY=1 \
      python gossip_benchmarks/02_no_failure_performance.py run-and-plot ...

Only R=1 is changed by the env var. Disabled and WitnessBaseline-R2..R4 remain
unchanged, so an existing Benchmark 02 run can be reused directly.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"{label}: expected exactly one match, found {count}. "
            "Your checkout likely differs from the version this patch targets."
        )
    return text.replace(old, new, 1)


def patch_ray_config(text: str) -> str:
    old = '''/// Serialize complete baseline lineage once at activation and transport those
/// bytes to all R holders instead of traversing the protobuf independently R times.
RAY_CONFIG(bool, enable_recovery_baseline_serialize_task_spec_once, false)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
'''
    new = '''/// Serialize complete baseline lineage once at activation and transport those
/// bytes to all R holders instead of traversing the protobuf independently R times.
RAY_CONFIG(bool, enable_recovery_baseline_serialize_task_spec_once, false)

/// PERF-ONLY experiment for the R=1 witness-holder baseline. When true, the
/// baseline performs normal witness selection, manifest publication, batching,
/// ACK waiting, and bookkeeping, but deliberately omits the full TaskSpec from
/// the witness update. This approximates a design in which the executor already
/// retained replay state and only needs to certify that retention.
///
/// The resulting witness state is NOT replayable. Never enable this for failure
/// or correctness testing. Default false preserves the real baseline exactly.
RAY_CONFIG(bool, recovery_baseline_perf_certification_only, false)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
'''
    return replace_once(text, old, new, "ray_config_def.h")


def patch_core_worker(text: str) -> str:
    old = '''    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();

    rpc::TaskSpec serialized_task_spec_proto;
'''
    new = '''    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();
    const bool certification_only =
        RayConfig::instance().recovery_baseline_perf_certification_only();

    rpc::TaskSpec serialized_task_spec_proto;
'''
    text = replace_once(text, old, new, "core_worker.cc: add proxy flag")

    old = '''    if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
'''
    new = '''    if (certification_only) {
      // PERF-ONLY proxy: keep the real baseline control path but omit the
      // redundant full TaskSpec installation. This models the case where an
      // executor already retained replay state and only needs certification.
      // Leaving both pointers null sends only the authoritative manifest.
    } else if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
'''
    text = replace_once(text, old, new, "core_worker.cc: suppress TaskSpec")

    old = '''        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns](
'''
    new = '''        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns,
         certification_only](
'''
    text = replace_once(text, old, new, "core_worker.cc: capture proxy flag")

    old = '''          RAY_LOG(INFO)
              .WithField(task_id)
              << "Installed full TaskSpec on all "
                 "witness-holder baseline nodes";
'''
    new = '''          if (certification_only) {
            RAY_LOG(INFO)
                .WithField(task_id)
                << "Installed certification-only PERF proxy on all "
                   "witness-holder baseline nodes (no TaskSpec retained)";
          } else {
            RAY_LOG(INFO)
                .WithField(task_id)
                << "Installed full TaskSpec on all "
                   "witness-holder baseline nodes";
          }
'''
    text = replace_once(text, old, new, "core_worker.cc: proxy log")
    return text


def patch_benchmark_common(text: str) -> str:
    old = '''def witness_baseline(holders: int) -> Method:
    return Method(
        "witness_baseline",
        f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )
'''
    new = '''def witness_baseline(holders: int) -> Method:
    certification_proxy = (
        holders == 1
        and os.environ.get("RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY", "0") == "1"
    )
    return Method(
        "witness_baseline",
        "CertificationProxy-R1" if certification_proxy else f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )
'''
    text = replace_once(text, old, new, "_benchmark_common.py: proxy label")

    old = '''    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
'''
    new = '''    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    # PERF-ONLY: model an R=1 holder whose replay state already exists at the
    # executor, so protection needs only manifest/certification traffic. Keep
    # this strictly R=1; R2..R4 remain the real full-lineage baseline.
    baseline_certification_only = (
        method.baseline_enabled
        and method.holders == 1
        and os.environ.get("RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY", "0") == "1"
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
'''
    text = replace_once(text, old, new, "_benchmark_common.py: proxy env")

    old = '''        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_succession_witness_count": max(1, int(witness_count)),
'''
    new = '''        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_baseline_perf_certification_only": baseline_certification_only,
        "recovery_succession_witness_count": max(1, int(witness_count)),
'''
    text = replace_once(text, old, new, "_benchmark_common.py: system config")
    return text


def patch_no_failure_benchmark(text: str) -> str:
    old = """def methods() -> list[Method]:
"""
    new = """def methods() -> list[Method]:
    # Keep the feasibility experiment tiny: with the proxy env enabled, run
    # only normal Ray and the R=1 certification proxy. All ordinary benchmark
    # behavior is unchanged when the env var is absent.
    if os.environ.get("RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY", "0") == "1":
        return [disabled(), witness_baseline(1)]
"""
    return replace_once(text, old, new, "02_no_failure_performance.py: proxy-only methods")


PATCHES = {
    Path("src/ray/common/ray_config_def.h"): patch_ray_config,
    Path("src/ray/core_worker/core_worker.cc"): patch_core_worker,
    Path("gossip_benchmarks/_benchmark_common.py"): patch_benchmark_common,
    Path("gossip_benchmarks/02_no_failure_performance.py"): patch_no_failure_benchmark,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ray_root", type=Path)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that every expected source fragment matches without writing files.",
    )
    args = parser.parse_args()

    root = args.ray_root.resolve()
    changed: list[tuple[Path, str]] = []

    for rel, patch_fn in PATCHES.items():
        path = root / rel
        if not path.is_file():
            raise FileNotFoundError(path)
        original = path.read_text()

        # Make repeated execution fail loudly rather than duplicating the patch.
        if rel.name == "ray_config_def.h" and "recovery_baseline_perf_certification_only" in original:
            raise RuntimeError(f"{rel}: proxy patch already appears to be applied")

        updated = patch_fn(original)
        if updated == original:
            raise RuntimeError(f"{rel}: patch made no changes")
        changed.append((path, updated))
        print(f"OK: {rel}")

    if args.check:
        print("\nCheck passed. No files were changed.")
        return 0

    for path, updated in changed:
        path.write_text(updated)

    print("\nApplied certification-only performance proxy.")
    print("IMPORTANT: this mode is intentionally NOT recovery-correct.")
    print("Use it only for no-failure performance measurements.")
    print("\nEnable it with:")
    print("  export RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY=1")
    print("Only WitnessBaseline-R1 becomes CertificationProxy-R1; R2..R4 are unchanged.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
