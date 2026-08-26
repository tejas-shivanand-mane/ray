#!/usr/bin/env python3
"""
Replace the failed certification-only performance proxy with a
frontier-density performance proxy in a Ray checkout.

What this does
--------------
1. Removes the previous certification-only proxy completely.
2. Adds a PERF-ONLY frontier-density proxy:
   - recovery_baseline_perf_protect_every_n (default 1)
   - for baseline mode, only ~1/K tasks enter the recovery-protection path
   - non-selected tasks also skip owner-side recovery-state retention
   - Benchmark 02 can run Disabled + K=1,4,8,16,32 in one command.

This is deliberately NOT a correct Recovery Frontiers implementation.
It only tests the normal-path performance hypothesis:
"How much overhead disappears if only 1/K tasks require protection?"

Selection is deterministic by TaskID hash, giving approximately 1/K protection
 density without a shared counter or synchronization cost.

Usage:
  python apply_frontier_density_proxy.py /home/tejas/Downloads/ray --check
  python apply_frontier_density_proxy.py /home/tejas/Downloads/ray
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
            "The checkout differs from the expected source state."
        )
    return text.replace(old, new, 1)


def remove_once_if_present(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count == 0:
        return text
    if count != 1:
        raise RuntimeError(f"{label}: expected at most one match, found {count}")
    return text.replace(old, new, 1)


# ---------------------------------------------------------------------------
# 1. ray_config_def.h
# ---------------------------------------------------------------------------

def patch_ray_config(text: str) -> str:
    cert = '''/// PERF-ONLY experiment for the R=1 witness-holder baseline. When true, the
/// baseline performs normal witness selection, manifest publication, batching,
/// ACK waiting, and bookkeeping, but deliberately omits the full TaskSpec from
/// the witness update. This approximates a design in which the executor already
/// retained replay state and only needs to certify that retention.
///
/// The resulting witness state is NOT replayable. Never enable this for failure
/// or correctness testing. Default false preserves the real baseline exactly.
RAY_CONFIG(bool, recovery_baseline_perf_certification_only, false)

'''
    text = remove_once_if_present(
        text, cert, "", "ray_config_def.h: remove certification proxy"
    )

    if "recovery_baseline_perf_protect_every_n" in text:
        raise RuntimeError(
            "ray_config_def.h: frontier-density proxy already appears to be applied"
        )

    anchor = '''RAY_CONFIG(bool, enable_recovery_baseline_serialize_task_spec_once, false)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
'''
    replacement = '''RAY_CONFIG(bool, enable_recovery_baseline_serialize_task_spec_once, false)

/// PERF-ONLY frontier-density experiment for the fixed-R baseline.
/// A value K>1 makes only approximately 1/K eligible tasks enter the baseline
/// protection path, selected deterministically from TaskID. Non-selected tasks
/// also skip baseline owner-side retained recovery state.
///
/// K=1 is exactly the real baseline behavior.
/// This setting is NOT recovery-correct for K>1 and must never be used for
/// failure/correctness testing.
RAY_CONFIG(uint32_t, recovery_baseline_perf_protect_every_n, 1)

RAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)
'''
    return replace_once(
        text, anchor, replacement, "ray_config_def.h: add frontier density config"
    )


# ---------------------------------------------------------------------------
# 2. core_worker.cc
# ---------------------------------------------------------------------------

def undo_cert_core_worker(text: str) -> str:
    text = remove_once_if_present(
        text,
        '''    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();
    const bool certification_only =
        RayConfig::instance().recovery_baseline_perf_certification_only();

    rpc::TaskSpec serialized_task_spec_proto;
''',
        '''    const bool serialize_task_spec_once =
        RayConfig::instance().enable_recovery_baseline_serialize_task_spec_once();

    rpc::TaskSpec serialized_task_spec_proto;
''',
        "core_worker.cc: remove certification flag",
    )

    text = remove_once_if_present(
        text,
        '''    if (certification_only) {
      // PERF-ONLY proxy: keep the real baseline control path but omit the
      // redundant full TaskSpec installation. This models the case where an
      // executor already retained replay state and only needs certification.
      // Leaving both pointers null sends only the authoritative manifest.
    } else if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
''',
        '''    if (serialize_task_spec_once) {
      // Experimental crossover path. The wire contract remains a complete
''',
        "core_worker.cc: restore TaskSpec publication",
    )

    text = remove_once_if_present(
        text,
        '''        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns,
         certification_only](
''',
        '''        [manager = recovery_succession_manager_,
         task_id,
         publish_start_ns](
''',
        "core_worker.cc: remove certification capture",
    )

    text = remove_once_if_present(
        text,
        '''          if (certification_only) {
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
''',
        '''          RAY_LOG(INFO)
              .WithField(task_id)
              << "Installed full TaskSpec on all "
                 "witness-holder baseline nodes";
''',
        "core_worker.cc: restore baseline install log",
    )
    return text


def patch_core_worker(text: str) -> str:
    text = undo_cert_core_worker(text)

    marker = "PERF-ONLY frontier-density selector"
    if marker in text:
        raise RuntimeError("core_worker.cc: frontier-density proxy already applied")

    anchor = '''  const TaskID task_id = object_id.TaskId();
  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);
'''
    replacement = '''  const TaskID task_id = object_id.TaskId();

  // PERF-ONLY frontier-density selector.
  //
  // For K>1, only approximately 1/K baseline tasks pay the protection cost.
  // Selection is deterministic by TaskID so repeated exports of the same
  // object make the same decision without shared counters or synchronization.
  //
  // Returning false here exports the ordinary ObjectRef without recovery
  // metadata. This is intentionally NOT recovery-correct for K>1.
  if (recovery_witness_holder_baseline_enabled_) {
    const uint32_t protect_every_n =
        RayConfig::instance().recovery_baseline_perf_protect_every_n();
    if (protect_every_n > 1) {
      constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
      constexpr uint64_t kPrime = 1099511628211ULL;
      uint64_t task_hash = kOffsetBasis;
      const std::string task_id_binary = task_id.Binary();
      for (const unsigned char byte : task_id_binary) {
        task_hash ^= static_cast<uint64_t>(byte);
        task_hash *= kPrime;
      }
      if ((task_hash % protect_every_n) != 0) {
        return false;
      }
    }
  }

  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);
'''
    return replace_once(
        text, anchor, replacement, "core_worker.cc: add frontier density selector"
    )


# ---------------------------------------------------------------------------
# 3. recovery_succession_manager.cc
# ---------------------------------------------------------------------------

def patch_recovery_manager(text: str) -> str:
    marker = "PERF-ONLY frontier-density owner-state selector"
    if marker in text:
        raise RuntimeError(
            "recovery_succession_manager.cc: frontier-density proxy already applied"
        )

    anchor = '''  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());
  const bool task_manager_pin =
      RayConfig::instance().enable_recovery_witness_holder_baseline() ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();

  OwnerRetainedTaskState retained;
'''
    replacement = '''  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  const bool baseline_enabled =
      RayConfig::instance().enable_recovery_witness_holder_baseline();

  // PERF-ONLY frontier-density owner-state selector.
  //
  // Match CoreWorker's TaskID selector so non-frontier tasks do not silently
  // retain baseline TaskManager/recovery state. This makes the experiment test
  // the cost of protecting only ~1/K tasks, not merely suppressing their
  // witness RPCs.
  if (baseline_enabled) {
    const uint32_t protect_every_n =
        RayConfig::instance().recovery_baseline_perf_protect_every_n();
    if (protect_every_n > 1) {
      constexpr uint64_t kOffsetBasis = 1469598103934665603ULL;
      constexpr uint64_t kPrime = 1099511628211ULL;
      uint64_t task_hash = kOffsetBasis;
      const std::string task_id_binary = task_id.Binary();
      for (const unsigned char byte : task_id_binary) {
        task_hash ^= static_cast<uint64_t>(byte);
        task_hash *= kPrime;
      }
      if ((task_hash % protect_every_n) != 0) {
        return;
      }
    }
  }

  const bool task_manager_pin =
      baseline_enabled ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();

  OwnerRetainedTaskState retained;
'''
    return replace_once(
        text,
        anchor,
        replacement,
        "recovery_succession_manager.cc: add owner-state density selector",
    )


# ---------------------------------------------------------------------------
# 4. _benchmark_common.py
# ---------------------------------------------------------------------------

def undo_cert_common(text: str) -> str:
    text = remove_once_if_present(
        text,
        '''def witness_baseline(holders: int) -> Method:
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
''',
        '''def witness_baseline(holders: int) -> Method:
    return Method(
        "witness_baseline",
        f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )
''',
        "_benchmark_common.py: restore witness_baseline",
    )

    text = remove_once_if_present(
        text,
        '''    # PERF-ONLY: model an R=1 holder whose replay state already exists at the
    # executor, so protection needs only manifest/certification traffic. Keep
    # this strictly R=1; R2..R4 remain the real full-lineage baseline.
    baseline_certification_only = (
        method.baseline_enabled
        and method.holders == 1
        and os.environ.get("RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY", "0") == "1"
    )

''',
        "",
        "_benchmark_common.py: remove certification env handling",
    )

    text = remove_once_if_present(
        text,
        '''        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_baseline_perf_certification_only": baseline_certification_only,
        "recovery_succession_witness_count": max(1, int(witness_count)),
''',
        '''        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_succession_witness_count": max(1, int(witness_count)),
''',
        "_benchmark_common.py: remove certification system config",
    )
    return text


def patch_benchmark_common(text: str) -> str:
    text = undo_cert_common(text)

    if "def frontier_proxy(" in text:
        raise RuntimeError("_benchmark_common.py: frontier proxy already applied")

    text = replace_once(
        text,
        '''class Method:
    key: str
    label: str
    recovery_enabled: bool
    baseline_enabled: bool
    holders: int
''',
        '''class Method:
    key: str
    label: str
    recovery_enabled: bool
    baseline_enabled: bool
    holders: int
    protection_interval: int = 1
''',
        "_benchmark_common.py: extend Method",
    )

    anchor = '''def witness_baseline(holders: int) -> Method:
    return Method(
        "witness_baseline",
        f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )


def recovery_methods(holders: int, include_disabled: bool = False) -> list[Method]:
'''
    replacement = '''def witness_baseline(holders: int) -> Method:
    return Method(
        "witness_baseline",
        f"WitnessBaseline-R{holders}",
        True,
        True,
        holders,
    )


def frontier_proxy(protection_interval: int) -> Method:
    if protection_interval <= 0:
        raise ValueError("protection_interval must be positive")
    return Method(
        f"frontier_proxy_k{protection_interval}",
        f"FrontierProxy-K{protection_interval}",
        True,
        True,
        1,
        protection_interval,
    )


def recovery_methods(holders: int, include_disabled: bool = False) -> list[Method]:
'''
    text = replace_once(
        text, anchor, replacement, "_benchmark_common.py: add frontier_proxy"
    )

    anchor = '''    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
'''
    replacement = '''    baseline_serialize_taskspec_once = (
        method.baseline_enabled
        and os.environ.get("RAY_RECOVERY_BASELINE_SERIALIZE_TASKSPEC_ONCE", "0") == "1"
    )

    baseline_protect_every_n = (
        int(method.protection_interval)
        if method.key.startswith("frontier_proxy_k")
        else 1
    )

    # The fixed-R baseline pins TaskManager unconditionally in C++ after cleanup.
'''
    text = replace_once(
        text,
        anchor,
        replacement,
        "_benchmark_common.py: add frontier density config value",
    )

    anchor = '''        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_succession_witness_count": max(1, int(witness_count)),
'''
    replacement = '''        "enable_recovery_baseline_serialize_task_spec_once": baseline_serialize_taskspec_once,
        "recovery_baseline_perf_protect_every_n": baseline_protect_every_n,
        "recovery_succession_witness_count": max(1, int(witness_count)),
'''
    text = replace_once(
        text,
        anchor,
        replacement,
        "_benchmark_common.py: expose frontier density system config",
    )

    anchor = '''            "baseline_enabled": int(method.baseline_enabled),
            "holders": method.holders,
'''
    replacement = '''            "baseline_enabled": int(method.baseline_enabled),
            "holders": method.holders,
            "protection_interval": method.protection_interval,
'''
    text = replace_once(
        text,
        anchor,
        replacement,
        "_benchmark_common.py: add protection_interval output column",
    )

    return text


# ---------------------------------------------------------------------------
# 5. 02_no_failure_performance.py
# ---------------------------------------------------------------------------

def undo_cert_benchmark02(text: str) -> str:
    return remove_once_if_present(
        text,
        '''def methods() -> list[Method]:
    # Keep the feasibility experiment tiny: with the proxy env enabled, run
    # only normal Ray and the R=1 certification proxy. All ordinary benchmark
    # behavior is unchanged when the env var is absent.
    if os.environ.get("RAY_RECOVERY_BASELINE_CERTIFICATION_ONLY", "0") == "1":
        return [disabled(), witness_baseline(1)]
''',
        '''def methods() -> list[Method]:
''',
        "02_no_failure_performance.py: remove certification-only method branch",
    )


def patch_benchmark02(text: str) -> str:
    text = undo_cert_benchmark02(text)

    if "RAY_RECOVERY_FRONTIER_PROXY" in text:
        raise RuntimeError("02_no_failure_performance.py: frontier proxy already applied")

    text = replace_once(
        text,
        '''    disabled,
    mean_ci95,
''',
        '''    disabled,
    frontier_proxy,
    mean_ci95,
''',
        "02_no_failure_performance.py: import frontier_proxy",
    )

    anchor = '''def methods() -> list[Method]:
    # return [disabled()] + [succession(r) for r in range(1, 5)] + [witness_baseline(r) for r in range(1, 5)]
'''
    replacement = '''def methods() -> list[Method]:
    # PERF-ONLY feasibility experiment for Recovery Frontiers.
    # K=1 is the real optimized WitnessBaseline-R1. K>1 protects only
    # approximately 1/K eligible tasks. This is NOT recovery-correct.
    if os.environ.get("RAY_RECOVERY_FRONTIER_PROXY", "0") == "1":
        return [disabled()] + [
            frontier_proxy(k) for k in (1, 4, 8, 16, 32)
        ]

    # return [disabled()] + [succession(r) for r in range(1, 5)] + [witness_baseline(r) for r in range(1, 5)]
'''
    return replace_once(
        text,
        anchor,
        replacement,
        "02_no_failure_performance.py: add frontier methods",
    )


PATCHES = {
    Path("src/ray/common/ray_config_def.h"): patch_ray_config,
    Path("src/ray/core_worker/core_worker.cc"): patch_core_worker,
    Path("src/ray/core_worker/recovery_succession_manager.cc"): patch_recovery_manager,
    Path("gossip_benchmarks/_benchmark_common.py"): patch_benchmark_common,
    Path("gossip_benchmarks/02_no_failure_performance.py"): patch_benchmark02,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("ray_root", type=Path)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify all source anchors without writing files.",
    )
    args = parser.parse_args()

    root = args.ray_root.resolve()
    changed: list[tuple[Path, str]] = []

    for rel, patch_fn in PATCHES.items():
        path = root / rel
        if not path.is_file():
            raise FileNotFoundError(path)

        original = path.read_text()
        updated = patch_fn(original)

        if updated == original:
            raise RuntimeError(f"{rel}: patch made no changes")

        changed.append((path, updated))
        print(f"OK: {rel}")

    if args.check:
        print("\nCheck passed. No files were changed.")
        print("The old certification-only experiment will be removed.")
        print("The frontier-density performance proxy is ready to apply.")
        return 0

    for path, updated in changed:
        path.write_text(updated)

    print("\nApplied frontier-density performance proxy.")
    print("Removed the previous certification-only proxy.")
    print("\nIMPORTANT:")
    print("  K=1 is the real baseline.")
    print("  K>1 is PERF-ONLY and is NOT recovery-correct.")
    print("  Do not run failure/correctness benchmarks in frontier-proxy mode.")
    print("\nRun after rebuilding Ray:")
    print("  RAY_RECOVERY_FRONTIER_PROXY=1 \\")
    print("  python gossip_benchmarks/02_no_failure_performance.py run \\")
    print("    --repetitions 3 \\")
    print("    --payloads 1KiB:1024 16KiB:16384 256KiB:262144 1MiB:1048576 \\")
    print("    --output-dir gossip_benchmarks/results/frontier_density_proxy")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
