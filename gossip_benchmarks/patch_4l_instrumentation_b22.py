#!/usr/bin/env python3
# Patch 4L instrumentation + Benchmark 22 accounting.
#
# Target: current main after the correctness-preserving Patch 4L.
#
# Run from either the Ray repo root or gossip_benchmarks:
#   python patch_4l_instrumentation_b22.py
#
# Backups are created once with suffix .pre_4l_instrumentation.bak.

from __future__ import annotations

import shutil
import sys
from pathlib import Path


FILES = {
    "manager_h": Path("src/ray/core_worker/recovery_succession_manager.h"),
    "manager_cc": Path("src/ray/core_worker/recovery_succession_manager.cc"),
    "core_worker_cc": Path("src/ray/core_worker/core_worker.cc"),
    "benchmark": Path("gossip_benchmarks/22_succession_vs_lazy_baseline_v2.py"),
}


def die(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def repo_root() -> Path:
    cwd = Path.cwd().resolve()
    for candidate in (cwd, cwd.parent):
        if all((candidate / rel).exists() for rel in FILES.values()):
            return candidate
    die(
        "Could not find the Ray repo. Run this script from the repo root "
        "or from gossip_benchmarks/."
    )


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        print(f"[already patched] {label}")
        return text
    n = text.count(old)
    if n != 1:
        die(
            f"{label}: expected exactly one source match, found {n}. "
            "No files have been written."
        )
    print(f"[patching] {label}")
    return text.replace(old, new, 1)


def backup(path: Path) -> None:
    out = path.with_name(path.name + ".pre_4l_instrumentation.bak")
    if out.exists():
        print(f"[backup exists] {out}")
        return
    shutil.copy2(path, out)
    print(f"[backup] {out}")


def patch_manager_h(text: str) -> str:
    if "RetainOwnerTaskSpecForLazyRecovery" not in text:
        die(
            "Patch 4L is not present in recovery_succession_manager.h. "
            "Apply the 4L correctness patch first."
        )

    old_profile = '''    // Patch 4J: owner first-borrow activation deliberately does not retain
    // another full TaskSpec in RecoverySuccessionManager.
    uint64_t owner_lazy_task_spec_copies_avoided = 0;
    uint64_t task_centric_metadata_builds = 0;
'''

    new_profile = '''    // Legacy Patch-4J metric. Patch 4L deliberately retains one dormant
    // owner TaskSpec, so this remains zero under the 4L design.
    uint64_t owner_lazy_task_spec_copies_avoided = 0;

    // Patch 4L owner-retained lineage accounting. "current" and "peak" are
    // gauges/state high-water marks; created/released are cumulative events
    // since the last profile reset.
    uint64_t owner_retained_task_specs_current = 0;
    uint64_t owner_retained_task_specs_peak = 0;
    uint64_t owner_retained_task_spec_bytes_current = 0;
    uint64_t owner_retained_task_spec_bytes_peak = 0;
    uint64_t owner_retained_task_specs_created = 0;
    uint64_t owner_retained_task_specs_released = 0;
    uint64_t owner_retained_task_spec_copy_time_ns = 0;

    uint64_t task_centric_metadata_builds = 0;
'''
    text = replace_once(text, old_profile, new_profile, "4L profile counters")

    old_struct = '''  struct OwnerRetainedTaskState {
    rpc::TaskSpec task_spec;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };
'''
    new_struct = '''  struct OwnerRetainedTaskState {
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };
'''
    text = replace_once(text, old_struct, new_struct, "retained TaskSpec byte size")
    return text


def patch_manager_cc(text: str) -> str:
    if "RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery" not in text:
        die("Patch 4L implementation is not present in recovery_succession_manager.cc.")

    old_avoided = '''  if (profiling_enabled_) {
    ++profile_.owner_lazy_task_spec_copies_avoided;
  }

  return true;
'''
    new_avoided = '''  // Patch 4L deliberately retains one dormant owner TaskSpec copy, so the
  // legacy Patch-4J "copy avoided" counter must remain zero.

  return true;
'''
    text = replace_once(
        text,
        old_avoided,
        new_avoided,
        "disable obsolete 4J copy-avoided increment",
    )

    old_copy = '''  OwnerRetainedTaskState retained;
  retained.task_spec.CopyFrom(task_proto);
  ClearFirstHolderTaskSpecPiggybacks(&retained.task_spec);

  for (const rpc::ObjectReference &returned_ref : returned_refs) {
'''
    new_copy = '''  const auto retained_copy_start = std::chrono::steady_clock::now();

  OwnerRetainedTaskState retained;
  retained.task_spec.CopyFrom(task_proto);
  ClearFirstHolderTaskSpecPiggybacks(&retained.task_spec);
  retained.task_spec_bytes =
      static_cast<uint64_t>(retained.task_spec.ByteSizeLong());

  const auto retained_copy_end = std::chrono::steady_clock::now();
  const uint64_t retained_copy_ns = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          retained_copy_end - retained_copy_start)
          .count());

  for (const rpc::ObjectReference &returned_ref : returned_refs) {
'''
    text = replace_once(text, old_copy, new_copy, "measure retained TaskSpec copy")

    old_insert = '''  auto existing = owner_retained_tasks_.find(task_id);
  if (existing == owner_retained_tasks_.end()) {
    owner_retained_tasks_[task_id] = std::move(retained);
    return;
  }

  for (const ObjectID &object_id : retained.live_return_ids) {
'''
    new_insert = '''  auto existing = owner_retained_tasks_.find(task_id);
  if (existing == owner_retained_tasks_.end()) {
    if (profiling_enabled_) {
      ++profile_.owner_retained_task_specs_created;
      ++profile_.owner_retained_task_specs_current;
      profile_.owner_retained_task_spec_bytes_current +=
          retained.task_spec_bytes;
      profile_.owner_retained_task_spec_copy_time_ns += retained_copy_ns;

      if (profile_.owner_retained_task_specs_current >
          profile_.owner_retained_task_specs_peak) {
        profile_.owner_retained_task_specs_peak =
            profile_.owner_retained_task_specs_current;
      }

      if (profile_.owner_retained_task_spec_bytes_current >
          profile_.owner_retained_task_spec_bytes_peak) {
        profile_.owner_retained_task_spec_bytes_peak =
            profile_.owner_retained_task_spec_bytes_current;
      }
    }

    owner_retained_tasks_[task_id] = std::move(retained);
    return;
  }

  for (const ObjectID &object_id : retained.live_return_ids) {
'''
    text = replace_once(
        text,
        old_insert,
        new_insert,
        "account retained TaskSpec creation/current/peak",
    )

    old_release = '''  if (!retained_it->second.live_return_ids.empty()) {
    return false;
  }

  owner_retained_tasks_.erase(retained_it);

  const auto task_it = task_states_.find(task_id);
'''
    new_release = '''  if (!retained_it->second.live_return_ids.empty()) {
    return false;
  }

  const uint64_t retained_bytes = retained_it->second.task_spec_bytes;

  if (profiling_enabled_) {
    ++profile_.owner_retained_task_specs_released;

    if (profile_.owner_retained_task_specs_current > 0) {
      --profile_.owner_retained_task_specs_current;
    }

    if (profile_.owner_retained_task_spec_bytes_current >= retained_bytes) {
      profile_.owner_retained_task_spec_bytes_current -= retained_bytes;
    } else {
      // Profiling resets reconstruct current state, so this should only be a
      // defensive fallback rather than a normal path.
      profile_.owner_retained_task_spec_bytes_current = 0;
    }
  }

  owner_retained_tasks_.erase(retained_it);

  const auto task_it = task_states_.find(task_id);
'''
    text = replace_once(text, old_release, new_release, "account retained TaskSpec release")

    old_reset = '''  absl::MutexLock lock(&mutex_);
  profile_ = RecoverySuccessionProfile{};
}

void RecoverySuccessionManager::RecordCandidateReport(bool accepted) {
'''
    new_reset = '''  absl::MutexLock lock(&mutex_);
  profile_ = RecoverySuccessionProfile{};

  // Patch 4L gauges describe real retained state, not just events after reset.
  // Reconstruct them so a benchmark profile reset cannot make a later release
  // underflow or hide already-live owner lineage.
  profile_.owner_retained_task_specs_current =
      static_cast<uint64_t>(owner_retained_tasks_.size());

  for (const auto &entry : owner_retained_tasks_) {
    profile_.owner_retained_task_spec_bytes_current +=
        entry.second.task_spec_bytes;
  }

  profile_.owner_retained_task_specs_peak =
      profile_.owner_retained_task_specs_current;
  profile_.owner_retained_task_spec_bytes_peak =
      profile_.owner_retained_task_spec_bytes_current;
}

void RecoverySuccessionManager::RecordCandidateReport(bool accepted) {
'''
    text = replace_once(
        text,
        old_reset,
        new_reset,
        "reconstruct retained gauges on profile reset",
    )

    return text


def patch_core_worker_cc(text: str) -> str:
    if "Patch 4L: correctness-preserving retained owner TaskSpec" not in text:
        die("The updated Patch-4L core_worker.cc was not detected.")
    if "const TaskID deleted_task_id = deleted_object_id.TaskId();" not in text:
        die(
            "The expected deleted_task_id shadowing fix is not present on main. "
            "Update/push that fix first."
        )

    old_export = '''  result["owner_lazy_task_spec_copies_avoided"] =
      profile.owner_lazy_task_spec_copies_avoided;
  result["task_centric_metadata_builds"] =
      profile.task_centric_metadata_builds;
'''
    new_export = '''  result["owner_lazy_task_spec_copies_avoided"] =
      profile.owner_lazy_task_spec_copies_avoided;

  result["owner_retained_task_specs_current"] =
      profile.owner_retained_task_specs_current;
  result["owner_retained_task_specs_peak"] =
      profile.owner_retained_task_specs_peak;
  result["owner_retained_task_spec_bytes_current"] =
      profile.owner_retained_task_spec_bytes_current;
  result["owner_retained_task_spec_bytes_peak"] =
      profile.owner_retained_task_spec_bytes_peak;
  result["owner_retained_task_specs_created"] =
      profile.owner_retained_task_specs_created;
  result["owner_retained_task_specs_released"] =
      profile.owner_retained_task_specs_released;
  result["owner_retained_task_spec_copy_time_ns"] =
      profile.owner_retained_task_spec_copy_time_ns;

  result["task_centric_metadata_builds"] =
      profile.task_centric_metadata_builds;
'''
    return replace_once(text, old_export, new_export, "export 4L retained-lineage profile")


def patch_benchmark(text: str) -> str:
    if "Wait for producer completion before forwarding the ObjectRef." not in text:
        die(
            "Benchmark 22 v2 does not contain the completed-producer/live-ref "
            "diagnostic fix."
        )

    text = text.replace(
        "Benchmark 22 v2: Recovery Succession 4K vs lazy fixed-R witness-holder baseline.",
        "Benchmark 22 v2: Recovery Succession 4L vs lazy fixed-R witness-holder baseline.",
        1,
    )

    old_b0_doc = '''  * B=0:
      no recovery activation, no full TaskSpec replication.
'''
    new_b0_doc = '''  * B=0:
      no recovery activation and no remote full TaskSpec replication.
      Patch 4L may retain one dormant owner-side TaskSpec copy while the
      producer ObjectRef remains live; this is measured separately.
'''
    if old_b0_doc in text:
        text = text.replace(old_b0_doc, new_b0_doc, 1)

    old_metrics_doc = '''Recovery state/traffic:
  * complete TaskSpec copies and bytes per pipeline
  * measured TaskSpec bytes/copy
'''
    new_metrics_doc = '''Recovery state/traffic:
  * remote complete TaskSpec copies and bytes per pipeline
  * Patch-4L owner-retained TaskSpec count/bytes (current and peak)
  * combined live TaskSpec state = owner-retained + remote holder copies
  * measured TaskSpec bytes/copy
'''
    if old_metrics_doc in text:
        text = text.replace(old_metrics_doc, new_metrics_doc, 1)

    old_profile_keys = '''    "owner_lazy_task_spec_copies_avoided",
    "task_centric_metadata_builds",
'''
    new_profile_keys = '''    "owner_lazy_task_spec_copies_avoided",
    "owner_retained_task_specs_current",
    "owner_retained_task_specs_peak",
    "owner_retained_task_spec_bytes_current",
    "owner_retained_task_spec_bytes_peak",
    "owner_retained_task_specs_created",
    "owner_retained_task_specs_released",
    "owner_retained_task_spec_copy_time_ns",
    "task_centric_metadata_builds",
'''
    text = replace_once(text, old_profile_keys, new_profile_keys, "Benchmark PROFILE_KEYS")

    old_lineage_locals = '''    full_lineage_bytes = int(owner["task_spec_bytes_sent"])
    manifest_bytes = int(owner["manifest_bytes_sent"])
'''
    new_lineage_locals = '''    full_lineage_bytes = int(owner["task_spec_bytes_sent"])

    owner_retained_task_specs_current = int(
        owner["owner_retained_task_specs_current"]
    )
    owner_retained_task_specs_peak = int(
        owner["owner_retained_task_specs_peak"]
    )
    owner_retained_task_spec_bytes_current = int(
        owner["owner_retained_task_spec_bytes_current"]
    )
    owner_retained_task_spec_bytes_peak = int(
        owner["owner_retained_task_spec_bytes_peak"]
    )
    owner_retained_task_specs_created = int(
        owner["owner_retained_task_specs_created"]
    )
    owner_retained_task_specs_released = int(
        owner["owner_retained_task_specs_released"]
    )
    owner_retained_task_spec_copy_time_ns = int(
        owner["owner_retained_task_spec_copy_time_ns"]
    )

    manifest_bytes = int(owner["manifest_bytes_sent"])
'''
    text = replace_once(
        text,
        old_lineage_locals,
        new_lineage_locals,
        "derive retained-lineage locals",
    )

    old_derived_return = '''        "measured_task_spec_bytes_per_copy": safe_div(
            full_lineage_bytes, full_lineage_transfers
        ),
        "manifest_bytes_total": manifest_bytes,
'''
    new_derived_return = '''        "measured_task_spec_bytes_per_copy": safe_div(
            full_lineage_bytes, full_lineage_transfers
        ),

        # Patch 4L owner-side retained lineage is memory/state, not network
        # replication, so keep it separate from task_spec_bytes_sent.
        "owner_retained_task_specs_current": owner_retained_task_specs_current,
        "owner_retained_task_specs_peak": owner_retained_task_specs_peak,
        "owner_retained_task_specs_current_per_pipeline": safe_div(
            owner_retained_task_specs_current, pipeline_count
        ),
        "owner_retained_task_specs_peak_per_pipeline": safe_div(
            owner_retained_task_specs_peak, pipeline_count
        ),
        "owner_retained_task_spec_bytes_current": (
            owner_retained_task_spec_bytes_current
        ),
        "owner_retained_task_spec_bytes_peak": owner_retained_task_spec_bytes_peak,
        "owner_retained_task_spec_bytes_current_per_pipeline": safe_div(
            owner_retained_task_spec_bytes_current, pipeline_count
        ),
        "owner_retained_task_spec_bytes_peak_per_pipeline": safe_div(
            owner_retained_task_spec_bytes_peak, pipeline_count
        ),
        "measured_owner_retained_task_spec_bytes_per_copy": safe_div(
            owner_retained_task_spec_bytes_current,
            owner_retained_task_specs_current,
        ),
        "owner_retained_task_specs_created": owner_retained_task_specs_created,
        "owner_retained_task_specs_released": owner_retained_task_specs_released,
        "owner_retained_task_specs_created_per_pipeline": safe_div(
            owner_retained_task_specs_created, pipeline_count
        ),
        "owner_retained_task_specs_released_per_pipeline": safe_div(
            owner_retained_task_specs_released, pipeline_count
        ),
        "owner_retained_task_spec_copy_time_ns": (
            owner_retained_task_spec_copy_time_ns
        ),
        "owner_retained_task_spec_copy_time_us_per_created": safe_div(
            owner_retained_task_spec_copy_time_ns / 1e3,
            owner_retained_task_specs_created,
        ),

        "manifest_bytes_total": manifest_bytes,
'''
    text = replace_once(
        text,
        old_derived_return,
        new_derived_return,
        "derived retained-lineage metrics",
    )

    old_live_result = '''    result: dict[str, Any] = {
        **derived,
        "live_state_task_count": args.state_task_count,
        "live_state_formation_ms": formation_ms,
'''
    new_live_result = '''    live_total_lineage_state_bytes = (
        int(derived["full_lineage_bytes_total"])
        + int(derived["owner_retained_task_spec_bytes_current"])
    )
    live_total_taskspec_copies = (
        float(derived["full_lineage_copies_per_pipeline"])
        + float(derived["owner_retained_task_specs_current_per_pipeline"])
    )

    result: dict[str, Any] = {
        **derived,
        "live_total_full_taskspec_copies_per_pipeline": live_total_taskspec_copies,
        "live_total_lineage_state_bytes_current": live_total_lineage_state_bytes,
        "live_total_lineage_state_bytes_per_pipeline": safe_div(
            live_total_lineage_state_bytes, args.state_task_count
        ),
        "live_state_task_count": args.state_task_count,
        "live_state_formation_ms": formation_ms,
'''
    text = replace_once(
        text,
        old_live_result,
        new_live_result,
        "live combined lineage-state accounting",
    )

    old_live_valid_field = '''        "live_state_succession_4k_no_piggyback_ok": int(succession_4k_ok),
        "live_state_valid": int(live_valid),
'''
    new_live_valid_field = '''        # Keep the old 4K field for CSV compatibility; 4L preserves the same
        # no-piggyback transport condition.
        "live_state_succession_4k_no_piggyback_ok": int(succession_4k_ok),
        "live_state_succession_4l_no_piggyback_ok": int(succession_4k_ok),
        "live_state_valid": int(live_valid),
'''
    text = replace_once(
        text,
        old_live_valid_field,
        new_live_valid_field,
        "4L live-state validity alias",
    )

    old_summary_slice = '''    "full_lineage_copies_per_pipeline",
    "full_lineage_bytes_per_pipeline",
    "measured_task_spec_bytes_per_copy",
    "manifest_bytes_per_pipeline",
'''
    new_summary_slice = '''    "full_lineage_copies_per_pipeline",
    "full_lineage_bytes_per_pipeline",
    "measured_task_spec_bytes_per_copy",
    "owner_retained_task_specs_current_per_pipeline",
    "owner_retained_task_specs_peak_per_pipeline",
    "owner_retained_task_spec_bytes_current_per_pipeline",
    "owner_retained_task_spec_bytes_peak_per_pipeline",
    "measured_owner_retained_task_spec_bytes_per_copy",
    "owner_retained_task_specs_created_per_pipeline",
    "owner_retained_task_specs_released_per_pipeline",
    "owner_retained_task_spec_copy_time_us_per_created",
    "live_total_full_taskspec_copies_per_pipeline",
    "live_total_lineage_state_bytes_per_pipeline",
    "manifest_bytes_per_pipeline",
'''
    text = replace_once(
        text,
        old_summary_slice,
        new_summary_slice,
        "SUMMARY_METRICS retained/total lineage fields",
    )

    old_pair_locals = '''        s_lineage = float(s["full_lineage_bytes_per_pipeline_mean"])
        b_lineage = float(b["full_lineage_bytes_per_pipeline_mean"])
        s_wire = float(s["measured_recovery_wire_payload_bytes_per_pipeline_mean"])
'''
    new_pair_locals = '''        s_lineage = float(s["full_lineage_bytes_per_pipeline_mean"])
        b_lineage = float(b["full_lineage_bytes_per_pipeline_mean"])
        s_retained = float(
            s["owner_retained_task_spec_bytes_current_per_pipeline_mean"]
        )
        b_retained = float(
            b["owner_retained_task_spec_bytes_current_per_pipeline_mean"]
        )
        s_total_state = float(
            s["live_total_lineage_state_bytes_per_pipeline_mean"]
        )
        b_total_state = float(
            b["live_total_lineage_state_bytes_per_pipeline_mean"]
        )
        s_wire = float(s["measured_recovery_wire_payload_bytes_per_pipeline_mean"])
'''
    text = replace_once(
        text,
        old_pair_locals,
        new_pair_locals,
        "paired retained/total lineage locals",
    )

    old_pair_fields = '''                "lineage_bytes_reduction_pct_succession_vs_baseline": (
                    100.0 * (b_lineage - s_lineage) / b_lineage
                    if b_lineage > 0
                    else math.nan
                ),
                "succession_full_lineage_copies_per_pipeline": float(
'''
    new_pair_fields = '''                "lineage_bytes_reduction_pct_succession_vs_baseline": (
                    100.0 * (b_lineage - s_lineage) / b_lineage
                    if b_lineage > 0
                    else math.nan
                ),
                "succession_owner_retained_task_spec_bytes_per_pipeline": s_retained,
                "baseline_owner_retained_task_spec_bytes_per_pipeline": b_retained,
                "succession_live_total_lineage_state_bytes_per_pipeline": (
                    s_total_state
                ),
                "baseline_live_total_lineage_state_bytes_per_pipeline": (
                    b_total_state
                ),
                "live_total_lineage_state_bytes_saved_by_succession_per_pipeline": (
                    b_total_state - s_total_state
                ),
                "live_total_lineage_state_reduction_pct_succession_vs_baseline": (
                    100.0 * (b_total_state - s_total_state) / b_total_state
                    if b_total_state > 0
                    else math.nan
                ),
                "succession_full_lineage_copies_per_pipeline": float(
'''
    text = replace_once(
        text,
        old_pair_fields,
        new_pair_fields,
        "paired total live lineage comparison",
    )

    old_plot = '''            (
                "full_lineage_bytes_per_pipeline_mean",
                "full_lineage_bytes_per_pipeline_ci95",
                "Full TaskSpec bytes / pipeline",
                "full_lineage_bytes_vs_borrowers",
            ),
            (
                "measured_recovery_wire_payload_bytes_per_pipeline_mean",
'''
    new_plot = '''            (
                "full_lineage_bytes_per_pipeline_mean",
                "full_lineage_bytes_per_pipeline_ci95",
                "Remote full TaskSpec bytes / pipeline",
                "full_lineage_bytes_vs_borrowers",
            ),
            (
                "live_total_lineage_state_bytes_per_pipeline_mean",
                "live_total_lineage_state_bytes_per_pipeline_ci95",
                "Live total TaskSpec state bytes / pipeline",
                "live_total_lineage_state_bytes_vs_borrowers",
            ),
            (
                "measured_recovery_wire_payload_bytes_per_pipeline_mean",
'''
    text = replace_once(
        text,
        old_plot,
        new_plot,
        "plot total live lineage state",
    )

    return text


def validate(files: dict[str, str]) -> None:
    checks = {
        "manager_h": [
            "owner_retained_task_specs_current",
            "owner_retained_task_spec_bytes_peak",
            "owner_retained_task_spec_copy_time_ns",
            "uint64_t task_spec_bytes = 0;",
        ],
        "manager_cc": [
            "retained.task_spec.ByteSizeLong()",
            "owner_retained_task_specs_created",
            "owner_retained_task_specs_released",
            "owner_retained_task_specs_current =",
        ],
        "core_worker_cc": [
            'result["owner_retained_task_specs_current"]',
            'result["owner_retained_task_spec_bytes_peak"]',
            'result["owner_retained_task_spec_copy_time_ns"]',
        ],
        "benchmark": [
            '"owner_retained_task_specs_current"',
            '"live_total_lineage_state_bytes_per_pipeline"',
            '"live_total_full_taskspec_copies_per_pipeline"',
            "live_total_lineage_state_bytes_vs_borrowers",
        ],
    }

    for name, needles in checks.items():
        missing = [needle for needle in needles if needle not in files[name]]
        if missing:
            die(
                f"Internal validation failed for {name}: missing "
                + ", ".join(repr(x) for x in missing)
            )


def main() -> None:
    root = repo_root()
    print(f"[repo] {root}")

    paths = {name: root / rel for name, rel in FILES.items()}
    original = {
        name: path.read_text(encoding="utf-8")
        for name, path in paths.items()
    }

    patched = {
        "manager_h": patch_manager_h(original["manager_h"]),
        "manager_cc": patch_manager_cc(original["manager_cc"]),
        "core_worker_cc": patch_core_worker_cc(original["core_worker_cc"]),
        "benchmark": patch_benchmark(original["benchmark"]),
    }

    validate(patched)

    changed = {
        name: patched[name] != original[name]
        for name in FILES
    }

    if not any(changed.values()):
        print("Instrumentation patch already applied; nothing to do.")
        return

    for name, did_change in changed.items():
        if did_change:
            backup(paths[name])

    for name, did_change in changed.items():
        if did_change:
            paths[name].write_text(patched[name], encoding="utf-8")
            print(f"[done] {paths[name].relative_to(root)}")

    print()
    print("Patch 4L instrumentation applied.")
    print()
    print("New owner retention profile fields:")
    print("  owner_retained_task_specs_current")
    print("  owner_retained_task_specs_peak")
    print("  owner_retained_task_spec_bytes_current")
    print("  owner_retained_task_spec_bytes_peak")
    print("  owner_retained_task_specs_created")
    print("  owner_retained_task_specs_released")
    print("  owner_retained_task_spec_copy_time_ns")
    print()
    print("Benchmark 22 now separates:")
    print("  remote full-lineage TaskSpec traffic")
    print("  owner-retained TaskSpec state")
    print("  combined live TaskSpec state")
    print()
    print("Rebuild Ray before running Benchmark 22.")


if __name__ == "__main__":
    main()
