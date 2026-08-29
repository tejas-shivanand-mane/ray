#!/usr/bin/env python3
"""Apply the Recovery Succession owner-lifetime fast-path optimization.

This is intentionally a source patcher rather than a benchmark-only switch.
It makes the production CoreWorker Recovery Succession path:

1. Always use the existing TaskManager recovery pin as the sole dormant
   TaskSpec retention source.
2. Keep the legacy manager-owned TaskSpec fallback for direct manager tests and
   non-CoreWorker callers.
3. Replace OwnerRetainedTaskState.live_return_ids (a flat_hash_set<ObjectID>)
   with a uint32_t remaining_live_returns counter.
4. Make RecoveryFrontierEnabled() lock-free because the planner pointer is
   fixed after RecoverySuccessionManager construction.

Fixed-R semantics are not changed: it already pins TaskManager and continues to
use the same witness-holder baseline storage/recovery protocol.

Usage from the Ray repository root:

    python gossip_benchmarks/apply_61_succession_owner_lifetime_fastpath.py

The script is idempotent and runs `git diff --check` after writing the files.
"""
from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def replace_once(text: str, old: str, new: str, label: str) -> tuple[str, bool]:
    if new in text:
        return text, False
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one old block, found {count}")
    return text.replace(old, new, 1), True


def patch_file(relpath: str, replacements: list[tuple[str, str, str]]) -> bool:
    path = ROOT / relpath
    text = path.read_text()
    changed = False
    for label, old, new in replacements:
        text, did_change = replace_once(text, old, new, f"{relpath}: {label}")
        changed |= did_change
    if changed:
        path.write_text(text)
    return changed


def main() -> None:
    changed_files: list[str] = []

    header_replacements = [
        (
            "RetainOwnerTaskSpec declaration",
            '''  /// Patch 4L: retain one dormant owner TaskSpec copy while at least one static
  /// return ObjectRef is truly in scope. This does not activate recovery or
  /// construct a manifest.
  void RetainOwnerTaskSpecForLazyRecovery(
      const TaskSpecification &task_spec,
      const std::vector<rpc::ObjectReference> &returned_refs);
''',
            '''  /// Retain owner-side lifetime state while at least one static return ObjectRef
  /// is truly in scope. Production CoreWorker callers set
  /// task_manager_owns_recipe=true and use TaskManager as the sole dormant
  /// TaskSpec owner; the manager-owned TaskSpec remains only as a compatibility
  /// fallback for direct manager tests/non-CoreWorker callers.
  void RetainOwnerTaskSpecForLazyRecovery(
      const TaskSpecification &task_spec,
      const std::vector<rpc::ObjectReference> &returned_refs,
      bool task_manager_owns_recipe = false);
''',
        ),
        (
            "OwnerRetainedTaskState counter",
            '''  struct OwnerRetainedTaskState {
    // Legacy Patch-4L mode stores the duplicate TaskSpec here. In 4N-PIN mode
    // this remains empty because TaskManager owns the sole dormant recipe.
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };
''',
            '''  struct OwnerRetainedTaskState {
    // Production CoreWorker mode leaves this empty because TaskManager owns the
    // sole dormant recipe. Direct manager tests/non-CoreWorker callers may use
    // this compatibility fallback.
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;

    // Static owner returns have one deletion callback each, so a count is
    // sufficient. Avoid allocating/hashing an ObjectID set for every task.
    uint32_t remaining_live_returns = 0;
  };
''',
        ),
    ]

    manager_replacements = [
        (
            "lock-free RecoveryFrontierEnabled",
            '''bool RecoverySuccessionManager::RecoveryFrontierEnabled() const {
  absl::MutexLock lock(&mutex_);
  return recovery_frontier_planner_ != nullptr;
}
''',
            '''bool RecoverySuccessionManager::RecoveryFrontierEnabled() const {
  // recovery_frontier_planner_ is initialized in the constructor and never
  // replaced afterwards, so this immutable feature check does not need the
  // manager mutex on every completed owner task.
  return recovery_frontier_planner_ != nullptr;
}
''',
        ),
        (
            "RetainOwnerTaskSpec signature",
            '''void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(
    const TaskSpecification &task_spec,
    const std::vector<rpc::ObjectReference> &returned_refs) {
''',
            '''void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(
    const TaskSpecification &task_spec,
    const std::vector<rpc::ObjectReference> &returned_refs,
    bool task_manager_owns_recipe) {
''',
        ),
        (
            "TaskManager recipe selector",
            '''  const bool task_manager_pin =
      baseline_enabled ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();
''',
            '''  // Production CoreWorker Recovery Succession always pins the existing
  // TaskManager entry. Keep the config/baseline checks for compatibility with
  // direct manager callers and older experiments.
  const bool task_manager_pin =
      task_manager_owns_recipe || baseline_enabled ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();
''',
        ),
        (
            "replace live return set construction",
            '''  for (const rpc::ObjectReference &returned_ref : returned_refs) {
    if (returned_ref.object_id().size() != ObjectID::Size()) {
      continue;
    }

    const ObjectID object_id = ObjectID::FromBinary(returned_ref.object_id());
    if (object_id.TaskId() != task_id) {
      continue;
    }

    retained.live_return_ids.insert(object_id);
  }

  if (retained.live_return_ids.empty()) {
    return;
  }
''',
            '''  uint32_t live_return_count = 0;
  for (const rpc::ObjectReference &returned_ref : returned_refs) {
    if (returned_ref.object_id().size() != ObjectID::Size()) {
      continue;
    }

    const ObjectID object_id = ObjectID::FromBinary(returned_ref.object_id());
    if (object_id.TaskId() != task_id) {
      continue;
    }

    ++live_return_count;
  }

  if (live_return_count == 0) {
    return;
  }
  retained.remaining_live_returns = live_return_count;
''',
        ),
        (
            "idempotent retained state registration",
            '''  for (const ObjectID &object_id : retained.live_return_ids) {
    existing->second.live_return_ids.insert(object_id);
  }
}
''',
            '''  // Static return registration is complete on the first call. A repeated
  // registration for the same TaskID must not re-inflate the counter after
  // deletion callbacks may already have fired.
  return;
}
''',
        ),
        (
            "GetRetainedOwnerTaskSpec live check",
            '''  if (it == owner_retained_tasks_.end() ||
      it->second.live_return_ids.empty() ||
      it->second.task_spec.task_id().empty()) {
''',
            '''  if (it == owner_retained_tasks_.end() ||
      it->second.remaining_live_returns == 0 ||
      it->second.task_spec.task_id().empty()) {
''',
        ),
        (
            "OwnerTaskHasLiveReturns live check",
            '''  return it != owner_retained_tasks_.end() &&
         !it->second.live_return_ids.empty();
''',
            '''  return it != owner_retained_tasks_.end() &&
         it->second.remaining_live_returns > 0;
''',
        ),
        (
            "decrement remaining return counter",
            '''  if (retained_it->second.live_return_ids.erase(object_id) == 0) {
    return false;
  }

  if (!retained_it->second.live_return_ids.empty()) {
    return false;
  }
''',
            '''  // CoreWorker registers exactly one deletion callback for each static
  // returned ObjectRef counted above. No per-return ObjectID set is required.
  if (retained_it->second.remaining_live_returns == 0) {
    return false;
  }

  --retained_it->second.remaining_live_returns;
  if (retained_it->second.remaining_live_returns > 0) {
    return false;
  }
''',
        ),
        (
            "frontier final-live-member check after deletion",
            '''        if (live_it != owner_retained_tasks_.end() &&
            !live_it->second.live_return_ids.empty()) {
          return false;
        }
''',
            '''        if (live_it != owner_retained_tasks_.end() &&
            live_it->second.remaining_live_returns > 0) {
          return false;
        }
''',
        ),
        (
            "PrepareHolderAdmission fallback live check",
            '''      if (retained_it != owner_retained_tasks_.end() &&
          !retained_it->second.live_return_ids.empty() &&
          !retained_it->second.task_spec.task_id().empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
''',
            '''      if (retained_it != owner_retained_tasks_.end() &&
          retained_it->second.remaining_live_returns > 0 &&
          !retained_it->second.task_spec.task_id().empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
''',
        ),
        (
            "PrepareTaskReplay fallback live check",
            '''    if (retained_it != owner_retained_tasks_.end() &&
        !retained_it->second.live_return_ids.empty() &&
        !retained_it->second.task_spec.task_id().empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
''',
            '''    if (retained_it != owner_retained_tasks_.end() &&
        retained_it->second.remaining_live_returns > 0 &&
        !retained_it->second.task_spec.task_id().empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
''',
        ),
        (
            "BuildTombstone frontier live-member check",
            '''        if (live_it != owner_retained_tasks_.end() &&
            !live_it->second.live_return_ids.empty()) {
          return std::nullopt;
        }
''',
            '''        if (live_it != owner_retained_tasks_.end() &&
            live_it->second.remaining_live_returns > 0) {
          return std::nullopt;
        }
''',
        ),
    ]

    core_worker_replacements = [
        (
            "always pin production Succession recipe",
            '''    if (recovery_witness_holder_baseline_enabled_ ||
        RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
      RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
          << "Eligible recovery task disappeared before TaskManager pin: "
          << task_spec.TaskId();
    }

    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs);
''',
            '''    // TaskManager already owns this immutable TaskSpec. Pin that existing
    // entry rather than maintaining a second dormant protobuf in Recovery
    // Succession. This is now the normal production path, not a perf-only mode.
    RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
        << "Eligible recovery task disappeared before TaskManager pin: "
        << task_spec.TaskId();

    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs, /*task_manager_owns_recipe=*/true);
''',
        ),
        (
            "always release production recovery pin on final return",
            '''      if (final_return_deleted &&
          (recovery_witness_holder_baseline_enabled_ ||
           RayConfig::instance().enable_recovery_succession_task_manager_pin())) {
        task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
      }
''',
            '''      if (final_return_deleted) {
        task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
      }
''',
        ),
    ]

    for relpath, replacements in [
        ("src/ray/core_worker/recovery_succession_manager.h", header_replacements),
        ("src/ray/core_worker/recovery_succession_manager.cc", manager_replacements),
        ("src/ray/core_worker/core_worker.cc", core_worker_replacements),
    ]:
        if patch_file(relpath, replacements):
            changed_files.append(relpath)

    # The old ObjectID set should no longer exist in owner-lifetime code. Other
    # flat_hash_sets in RecoverySuccessionManager are intentional.
    manager_text = (ROOT / "src/ray/core_worker/recovery_succession_manager.cc").read_text()
    header_text = (ROOT / "src/ray/core_worker/recovery_succession_manager.h").read_text()
    if "live_return_ids" in manager_text or "live_return_ids" in header_text:
        raise RuntimeError("live_return_ids still appears after patch")

    subprocess.run(["git", "diff", "--check"], cwd=ROOT, check=True)

    if changed_files:
        print("Applied Succession owner-lifetime fast path:")
        for path in changed_files:
            print(f"  {path}")
    else:
        print("Succession owner-lifetime fast path is already applied.")

    print("\nNext: rebuild Ray, run behavior benchmarks 55/56/57, then rerun Benchmark 58.")


if __name__ == "__main__":
    main()
