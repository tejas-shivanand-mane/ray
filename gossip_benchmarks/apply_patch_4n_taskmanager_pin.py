#!/usr/bin/env python3
"""
Apply Patch 4N-PIN: TaskManager-backed dormant owner TaskSpec retention.

Purpose
-------
Patch 4L fixed late-borrow correctness by eagerly deep-copying every eligible
owner TaskSpec into RecoverySuccessionManager while any static return ObjectRef
was live. That is correct, but it adds a second TaskSpec copy even for B=0.

4N-PIN keeps the *existing* TaskManager entry alive instead. Normal Ray lineage
references are still released exactly as before; only the TaskSpec-bearing
TaskEntry is retained. On first recovery activation, existing code obtains the
TaskSpec from TaskManager. When the final owner return ObjectRef dies, the pin
is released.

The experiment is feature-flagged:
    enable_recovery_succession_task_manager_pin = false  (default)

Benchmark helper:
    export RAY_RECOVERY_TASKMANAGER_PIN=1

Certificate admission is independent and should normally be disabled while
measuring this patch:
    unset RAY_RECOVERY_CERTIFICATE_ADMISSION

This script is intended for the current project tree after Patch 4M-CERT.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def replace_once(path: Path, old: str, new: str, label: str) -> None:
    text = path.read_text()
    if new in text:
        print(f"[already] {label}")
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"{label}: expected exactly one old block in {path}, found {count}"
        )
    path.write_text(text.replace(old, new, 1))
    print(f"[patched] {label}")


def replace_all_expected(
    path: Path, old: str, new: str, expected: int, label: str
) -> None:
    text = path.read_text()
    if new in text and old not in text:
        print(f"[already] {label}")
        return
    count = text.count(old)
    if count != expected:
        raise RuntimeError(
            f"{label}: expected {expected} old blocks in {path}, found {count}"
        )
    path.write_text(text.replace(old, new))
    print(f"[patched] {label} ({expected} occurrence(s))")


def replace_between(
    path: Path, start_marker: str, end_marker: str, replacement: str, label: str
) -> None:
    text = path.read_text()
    if replacement in text:
        print(f"[already] {label}")
        return
    start = text.find(start_marker)
    if start < 0:
        raise RuntimeError(f"{label}: start marker not found in {path}")
    end = text.find(end_marker, start)
    if end < 0:
        raise RuntimeError(f"{label}: end marker not found in {path}")
    path.write_text(text[:start] + replacement + text[end:])
    print(f"[patched] {label}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo",
        nargs="?",
        default=".",
        help="Ray repository root (default: current directory)",
    )
    args = parser.parse_args()
    root = Path(args.repo).resolve()

    required = [
        root / "src/ray/common/ray_config_def.h",
        root / "src/ray/core_worker/task_manager.h",
        root / "src/ray/core_worker/task_manager.cc",
        root / "src/ray/core_worker/core_worker.cc",
        root / "src/ray/core_worker/recovery_succession_manager.h",
        root / "src/ray/core_worker/recovery_succession_manager.cc",
        root / "gossip_benchmarks/_benchmark_common.py",
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)

    # 1. Config flag.
    config = root / "src/ray/common/ray_config_def.h"
    replace_once(
        config,
        '''RAY_CONFIG(bool, enable_recovery_succession_certificate_admission, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        '''RAY_CONFIG(bool, enable_recovery_succession_certificate_admission, false)


/// Patch 4N-PIN experimental mode. When true, an eligible owner's existing
/// TaskManager entry is retained while any static return ObjectRef is live,
/// avoiding Patch-4L's eager second TaskSpec copy. Normal Ray dependency
/// lineage is released exactly as before; only the TaskSpec-bearing TaskEntry
/// remains pinned for lazy Recovery Succession activation.
RAY_CONFIG(bool, enable_recovery_succession_task_manager_pin, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        "ray config flag",
    )

    # 2. TaskManager API + TaskEntry bit.
    tm_h = root / "src/ray/core_worker/task_manager.h"
    replace_once(
        tm_h,
        '''  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override;

  /// Return specs for pending children tasks of the given parent task.
''',
        '''  std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override;

  /// Patch 4N-PIN: retain only this TaskManager entry for lazy Recovery
  /// Succession. This does not retain normal dependency-lineage references.
  /// Returns false if the task no longer exists.
  bool PinTaskForRecoverySuccession(const TaskID &task_id);

  /// Releases the Patch-4N recovery pin. If normal Ray lineage is already gone
  /// and the task is finished, the dormant TaskEntry is erased immediately.
  void ReleaseTaskForRecoverySuccession(const TaskID &task_id);

  /// Return specs for pending children tasks of the given parent task.
''',
        "TaskManager pin API",
    )

    replace_once(
        tm_h,
        '''    int64_t lineage_footprint_bytes_ = 0;
    // Number of times this task successfully completed execution so far.
''',
        '''    int64_t lineage_footprint_bytes_ = 0;

    // Patch 4N-PIN. This protects only the TaskEntry/spec_ from erasure.
    // Ordinary Ray dependency lineage is still released normally.
    bool recovery_succession_pinned_ = false;

    // Number of times this task successfully completed execution so far.
''',
        "TaskEntry recovery pin bit",
    )

    # 3. TaskManager implementation.
    tm_cc = root / "src/ray/core_worker/task_manager.cc"

    replace_once(
        tm_cc,
        '''std::optional<TaskSpecification> TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return std::optional<TaskSpecification>();
  }
  return it->second.spec_;
}

std::vector<TaskID> TaskManager::GetPendingChildrenTasks(
''',
        '''std::optional<TaskSpecification> TaskManager::GetTaskSpec(const TaskID &task_id) const {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return std::optional<TaskSpecification>();
  }
  return it->second.spec_;
}

bool TaskManager::PinTaskForRecoverySuccession(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return false;
  }

  it->second.recovery_succession_pinned_ = true;
  return true;
}

void TaskManager::ReleaseTaskForRecoverySuccession(const TaskID &task_id) {
  absl::MutexLock lock(&mu_);
  auto it = submissible_tasks_.find(task_id);
  if (it == submissible_tasks_.end()) {
    return;
  }

  it->second.recovery_succession_pinned_ = false;

  // Two valid orderings exist:
  // 1. Normal Ray lineage disappeared first. RemoveLineageReference retained
  //    this TaskEntry only because the recovery pin was set. Erase it now.
  // 2. The recovery/ObjectRef pin disappeared first. If normal lineage still
  //    exists, leave the entry alone; RemoveLineageReference erases it later.
  if (!it->second.IsPending() &&
      it->second.reconstructable_return_ids_.empty()) {
    RAY_LOG(DEBUG).WithField(task_id)
        << "Releasing dormant Recovery Succession TaskManager pin";
    submissible_tasks_.erase(it);
  }
}

std::vector<TaskID> TaskManager::GetPendingChildrenTasks(
''',
        "TaskManager pin implementation",
    )

    replace_once(
        tm_cc,
        '''    } else {
      submissible_tasks_.erase(it);
    }
  }

  // If it is a streaming generator, mark the end of stream since the task is finished.
''',
        '''    } else if (!it->second.recovery_succession_pinned_) {
      submissible_tasks_.erase(it);
    } else {
      // Patch 4N-PIN: the result does not need ordinary Ray lineage, but a
      // live owner ObjectRef still requires the producer recipe for possible
      // lazy Recovery Succession activation. release_lineage intentionally
      // remains true, so dependency-lineage accounting is unchanged.
      RAY_LOG(DEBUG).WithField(task_id)
          << "Keeping finished TaskSpec for lazy Recovery Succession";
    }
  }

  // If it is a streaming generator, mark the end of stream since the task is finished.
''',
        "retain finished direct TaskSpec",
    )

    replace_once(
        tm_cc,
        '''    total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes_;

    // The task has finished and none of its returns are in scope.
    submissible_tasks_.erase(it);
  }

  return total_lineage_footprint_bytes_ - total_lineage_footprint_bytes_prev;
''',
        '''    total_lineage_footprint_bytes_ -= it->second.lineage_footprint_bytes_;
    it->second.lineage_footprint_bytes_ = 0;

    // Normal Ray lineage is now gone. Patch 4N may still need the TaskSpec
    // solely because the owner-side ObjectRef remains live.
    if (!it->second.recovery_succession_pinned_) {
      submissible_tasks_.erase(it);
    } else {
      RAY_LOG(DEBUG).WithField(task_id)
          << "Normal lineage released; retaining TaskSpec for lazy recovery";
    }
  }

  return total_lineage_footprint_bytes_ - total_lineage_footprint_bytes_prev;
''',
        "preserve pinned TaskEntry after normal lineage release",
    )

    # 4. RecoverySuccessionManager.
    rsm_h = root / "src/ray/core_worker/recovery_succession_manager.h"

    replace_once(
        rsm_h,
        '''  /// Records actual ObjectRef deletion. Returns true iff this was the final
  /// owner return and an activated recovery task should now be tombstoned.
  bool HandleOwnerReturnRefDeleted(const ObjectID &object_id);
''',
        '''  /// Records actual ObjectRef deletion. Returns true iff this was the final
  /// owner return and an activated recovery task should now be tombstoned.
  /// If final_return_deleted is non-null, it is set when this deletion removed
  /// the final tracked owner return regardless of whether recovery was activated.
  bool HandleOwnerReturnRefDeleted(const ObjectID &object_id,
                                   bool *final_return_deleted = nullptr);
''',
        "owner return deletion result",
    )

    replace_once(
        rsm_h,
        '''  struct OwnerRetainedTaskState {
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };
''',
        '''  struct OwnerRetainedTaskState {
    // Legacy Patch-4L mode stores the duplicate TaskSpec here. In 4N-PIN mode
    // this remains empty because TaskManager owns the sole dormant recipe.
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };
''',
        "retained-state comment",
    )

    rsm_cc = root / "src/ray/core_worker/recovery_succession_manager.cc"

    new_retain = r'''void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(
    const TaskSpecification &task_spec,
    const std::vector<rpc::ObjectReference> &returned_refs) {
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (task_proto.task_id().empty() ||
      task_proto.has_recovery_manifest() ||
      !IsEligibleTask(task_proto)) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());
  const bool task_manager_pin =
      RayConfig::instance().enable_recovery_succession_task_manager_pin();

  OwnerRetainedTaskState retained;
  uint64_t retained_copy_ns = 0;

  if (task_manager_pin) {
    // TaskManager already owns the TaskSpec. Keep only lifetime bookkeeping
    // here. ByteSizeLong remains for apples-to-apples benchmark accounting.
    retained.task_spec_bytes =
        static_cast<uint64_t>(task_proto.ByteSizeLong());
  } else {
    const auto retained_copy_start = std::chrono::steady_clock::now();

    retained.task_spec.CopyFrom(task_proto);
    ClearFirstHolderTaskSpecPiggybacks(&retained.task_spec);
    retained.task_spec_bytes =
        static_cast<uint64_t>(retained.task_spec.ByteSizeLong());

    const auto retained_copy_end = std::chrono::steady_clock::now();
    retained_copy_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            retained_copy_end - retained_copy_start)
            .count());
  }

  for (const rpc::ObjectReference &returned_ref : returned_refs) {
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

  absl::MutexLock lock(&mutex_);

  auto existing = owner_retained_tasks_.find(task_id);
  if (existing == owner_retained_tasks_.end()) {
    if (profiling_enabled_) {
      ++profile_.owner_retained_task_specs_created;
      ++profile_.owner_retained_task_specs_current;
      profile_.owner_retained_task_spec_bytes_current +=
          retained.task_spec_bytes;
      profile_.owner_retained_task_spec_copy_time_ns += retained_copy_ns;

      if (task_manager_pin) {
        ++profile_.owner_lazy_task_spec_copies_avoided;
      }

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
    existing->second.live_return_ids.insert(object_id);
  }
}

'''
    replace_between(
        rsm_cc,
        "void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(",
        "bool RecoverySuccessionManager::GetRetainedOwnerTaskSpec(",
        new_retain,
        "zero-copy retained owner bookkeeping",
    )

    replace_once(
        rsm_cc,
        '''  if (it == owner_retained_tasks_.end() ||
      it->second.live_return_ids.empty()) {
    return false;
  }

  task_spec->CopyFrom(it->second.task_spec);
''',
        '''  if (it == owner_retained_tasks_.end() ||
      it->second.live_return_ids.empty() ||
      it->second.task_spec.task_id().empty()) {
    // In 4N-PIN mode the dormant TaskSpec lives in TaskManager.
    return false;
  }

  task_spec->CopyFrom(it->second.task_spec);
''',
        "legacy retained-copy getter guard",
    )

    new_deleted = r'''bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(
    const ObjectID &object_id,
    bool *final_return_deleted) {
  if (final_return_deleted != nullptr) {
    *final_return_deleted = false;
  }

  if (object_id.IsNil()) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();

  absl::MutexLock lock(&mutex_);

  auto retained_it = owner_retained_tasks_.find(task_id);
  if (retained_it == owner_retained_tasks_.end()) {
    return false;
  }

  if (retained_it->second.live_return_ids.erase(object_id) == 0) {
    return false;
  }

  if (!retained_it->second.live_return_ids.empty()) {
    return false;
  }

  if (final_return_deleted != nullptr) {
    *final_return_deleted = true;
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
      profile_.owner_retained_task_spec_bytes_current = 0;
    }
  }

  owner_retained_tasks_.erase(retained_it);

  const auto task_it = task_states_.find(task_id);
  return task_it != task_states_.end() &&
         !task_it->second.manifest.tombstoned();
}


'''
    replace_between(
        rsm_cc,
        "bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(",
        "std::vector<RecoverySuccessionManager::CandidateReport>",
        new_deleted,
        "final owner-return signal",
    )

    replace_all_expected(
        rsm_cc,
        '''      if (retained_it != owner_retained_tasks_.end() &&
          !retained_it->second.live_return_ids.empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
''',
        '''      if (retained_it != owner_retained_tasks_.end() &&
          !retained_it->second.live_return_ids.empty() &&
          !retained_it->second.task_spec.task_id().empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
''',
        1,
        "admission legacy-copy fallback guard",
    )

    replace_all_expected(
        rsm_cc,
        '''    if (retained_it != owner_retained_tasks_.end() &&
        !retained_it->second.live_return_ids.empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
''',
        '''    if (retained_it != owner_retained_tasks_.end() &&
        !retained_it->second.live_return_ids.empty() &&
        !retained_it->second.task_spec.task_id().empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
''',
        1,
        "replay legacy-copy fallback guard",
    )

    # 5. CoreWorker pin/unpin.
    cw = root / "src/ray/core_worker/core_worker.cc"

    replace_once(
        cw,
        '''    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs);
''',
        '''    if (RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
      RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
          << "Eligible recovery task disappeared before TaskManager pin: "
          << task_spec.TaskId();
    }

    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs);
''',
        "owner TaskManager pin on submit",
    )

    replace_once(
        cw,
        '''      if (!recovery_succession_manager_->HandleOwnerReturnRefDeleted(
              deleted_object_id)) {
        return;
      }

      io_service_.post(
''',
        '''      bool final_return_deleted = false;
      const bool should_tombstone =
          recovery_succession_manager_->HandleOwnerReturnRefDeleted(
              deleted_object_id, &final_return_deleted);

      if (final_return_deleted &&
          RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
        task_manager_->ReleaseTaskForRecoverySuccession(deleted_task_id);
      }

      if (!should_tombstone) {
        return;
      }

      io_service_.post(
''',
        "release TaskManager pin on final owner return",
    )

    # 6. Benchmark helper switch; fair for both recovery methods.
    bench = root / "gossip_benchmarks/_benchmark_common.py"

    replace_once(
        bench,
        '''    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )
    config: dict[str, Any] = {
''',
        '''    certificate_admission = (
        os.environ.get("RAY_RECOVERY_CERTIFICATE_ADMISSION", "0") == "1"
        and method.recovery_enabled
        and not method.baseline_enabled
    )
    task_manager_pin = (
        os.environ.get("RAY_RECOVERY_TASKMANAGER_PIN", "0") == "1"
        and method.recovery_enabled
    )
    config: dict[str, Any] = {
''',
        "benchmark pin env switch",
    )

    replace_once(
        bench,
        '''        "enable_recovery_succession_certificate_admission": certificate_admission,
        "recovery_succession_witness_count": max(1, int(witness_count)),
''',
        '''        "enable_recovery_succession_certificate_admission": certificate_admission,
        "enable_recovery_succession_task_manager_pin": task_manager_pin,
        "recovery_succession_witness_count": max(1, int(witness_count)),
''',
        "benchmark pin config propagation",
    )

    print()
    print("Patch 4N-PIN applied.")
    print("Rebuild Ray before benchmarking.")
    print()
    print("Pin experiment:")
    print("  unset RAY_RECOVERY_CERTIFICATE_ADMISSION")
    print("  export RAY_RECOVERY_TASKMANAGER_PIN=1")
    print()
    print("Old Patch-4L-copy control:")
    print("  unset RAY_RECOVERY_TASKMANAGER_PIN")


if __name__ == "__main__":
    main()
