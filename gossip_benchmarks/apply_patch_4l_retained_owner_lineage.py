#!/usr/bin/env python3
# Apply Patch 4L: conservative owner TaskSpec retention for Recovery Succession.
#
# Run from either the Ray repo root or its gossip_benchmarks folder:
#
#     python apply_patch_4l_retained_owner_lineage.py
#
# Backups are created once with suffix .pre_patch4l.bak.
#
# Patch 4L deliberately allows one duplicate owner-side TaskSpec copy while an
# eligible task's returned ObjectRefs are still live. It does NOT activate
# recovery or send recovery control traffic at B=0.

from __future__ import annotations

import shutil
import sys
from pathlib import Path


FILES = {
    "manager_h": Path("src/ray/core_worker/recovery_succession_manager.h"),
    "manager_cc": Path("src/ray/core_worker/recovery_succession_manager.cc"),
    "core_worker_cc": Path("src/ray/core_worker/core_worker.cc"),
}


def die(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def find_repo_root() -> Path:
    cwd = Path.cwd().resolve()
    for root in (cwd, cwd.parent):
        if all((root / rel).exists() for rel in FILES.values()):
            return root
    die(
        "Could not find the Ray source tree.\n"
        "Run from the Ray repo root or from its gossip_benchmarks folder."
    )


def backup(path: Path) -> None:
    bak = path.with_name(path.name + ".pre_patch4l.bak")
    if bak.exists():
        print(f"[backup exists] {bak}")
        return
    shutil.copy2(path, bak)
    print(f"[backup] {bak}")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count == 0:
        if new in text:
            print(f"[already patched] {label}")
            return text
        die(
            f"Could not find expected block for: {label}\n"
            "Your source differs from the Patch-4K main version targeted by this patcher.\n"
            "No files have been written."
        )
    if count != 1:
        die(
            f"Found {count} matches for {label}; expected exactly one.\n"
            "No files have been written."
        )
    print(f"[patching] {label}")
    return text.replace(old, new, 1)


def insert_after_once(text: str, marker: str, addition: str, label: str) -> str:
    if addition.strip() in text:
        print(f"[already patched] {label}")
        return text

    count = text.count(marker)
    if count != 1:
        die(
            f"Expected exactly one marker for {label}, found {count}.\n"
            "No files have been written."
        )

    print(f"[patching] {label}")
    return text.replace(marker, marker + addition, 1)


def patch_manager_h(text: str) -> str:
    if "Patch 4L: correctness-preserving retained owner lineage" not in text:
        text = text.replace(
            "/// Patch 4J: task-centric recovery state and on-demand owner lineage.\n",
            "/// Patch 4J: task-centric recovery state and on-demand owner lineage.\n"
            "/// Patch 4L: correctness-preserving retained owner lineage for late borrow.\n",
            1,
        )

    declaration_marker = '''  bool RegisterOwnedTaskLazy(const TaskSpecification &task_spec,
                             const rpc::RecoveryManifest &manifest);
'''

    declarations = '''
  /// Patch 4L: retain one dormant owner TaskSpec copy while at least one static
  /// return ObjectRef is truly in scope. This does not activate recovery or
  /// construct a manifest.
  void RetainOwnerTaskSpecForLazyRecovery(
      const TaskSpecification &task_spec,
      const std::vector<rpc::ObjectReference> &returned_refs);

  /// Copies the retained owner TaskSpec if one is still live.
  bool GetRetainedOwnerTaskSpec(const TaskID &task_id,
                                rpc::TaskSpec *task_spec) const;

  /// True while this owner task still has at least one live returned ObjectRef.
  bool OwnerTaskHasLiveReturns(const TaskID &task_id) const;

  /// Records actual ObjectRef deletion. Returns true iff this was the final
  /// owner return and an activated recovery task should now be tombstoned.
  bool HandleOwnerReturnRefDeleted(const ObjectID &object_id);
'''
    text = insert_after_once(
        text,
        declaration_marker,
        declarations,
        "RecoverySuccessionManager Patch-4L public API",
    )

    struct_marker = '''  struct BorrowedObjectRecoveryState {
'''
    retained_struct = '''  struct OwnerRetainedTaskState {
    rpc::TaskSpec task_spec;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };

'''
    if retained_struct.strip() not in text:
        count = text.count(struct_marker)
        if count != 1:
            die(
                "Could not uniquely locate BorrowedObjectRecoveryState in "
                "recovery_succession_manager.h"
            )
        print("[patching] Patch-4L retained-owner state struct")
        text = text.replace(struct_marker, retained_struct + struct_marker, 1)
    else:
        print("[already patched] Patch-4L retained-owner state struct")

    map_marker = '''  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_ ABSL_GUARDED_BY(mutex_);

'''
    retained_map = '''  /// Patch 4L: one correctness-preserving owner TaskSpec copy retained
  /// independently of TaskManager's ordinary lineage lifetime. Presence here
  /// does not mean recovery has been activated.
  absl::flat_hash_map<TaskID, OwnerRetainedTaskState> owner_retained_tasks_
      ABSL_GUARDED_BY(mutex_);

'''
    text = insert_after_once(
        text,
        map_marker,
        retained_map,
        "Patch-4L retained-owner map",
    )
    return text


def patch_manager_cc(text: str) -> str:
    if "// Patch 4L: retain one owner TaskSpec copy" not in text:
        text = text.replace(
            "// Patch 4K: full mode uses async holder install; no H1 TaskSpec piggyback.\n",
            "// Patch 4K: full mode uses async holder install; no H1 TaskSpec piggyback.\n"
            "// Patch 4L: retain one owner TaskSpec copy until returned refs truly die.\n",
            1,
        )

    insertion_marker = '''std::vector<RecoverySuccessionManager::CandidateReport>
RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
'''

    methods = r'''
void RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(
    const TaskSpecification &task_spec,
    const std::vector<rpc::ObjectReference> &returned_refs) {
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (task_proto.task_id().empty() ||
      task_proto.has_recovery_manifest() ||
      !IsEligibleTask(task_proto)) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  OwnerRetainedTaskState retained;
  retained.task_spec.CopyFrom(task_proto);
  ClearFirstHolderTaskSpecPiggybacks(&retained.task_spec);

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
    owner_retained_tasks_[task_id] = std::move(retained);
    return;
  }

  for (const ObjectID &object_id : retained.live_return_ids) {
    existing->second.live_return_ids.insert(object_id);
  }
}

bool RecoverySuccessionManager::GetRetainedOwnerTaskSpec(
    const TaskID &task_id,
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr || task_id.IsNil()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto it = owner_retained_tasks_.find(task_id);
  if (it == owner_retained_tasks_.end() ||
      it->second.live_return_ids.empty()) {
    return false;
  }

  task_spec->CopyFrom(it->second.task_spec);
  return true;
}

bool RecoverySuccessionManager::OwnerTaskHasLiveReturns(
    const TaskID &task_id) const {
  if (task_id.IsNil()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);
  const auto it = owner_retained_tasks_.find(task_id);
  return it != owner_retained_tasks_.end() &&
         !it->second.live_return_ids.empty();
}

bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(
    const ObjectID &object_id) {
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

  owner_retained_tasks_.erase(retained_it);

  const auto task_it = task_states_.find(task_id);
  return task_it != task_states_.end() &&
         !task_it->second.manifest.tombstoned();
}


'''
    if "RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery(" not in text:
        count = text.count(insertion_marker)
        if count != 1:
            die(
                "Could not uniquely locate RegisterExecutorTask insertion point "
                "in recovery_succession_manager.cc"
            )
        print("[patching] Patch-4L retained-owner methods")
        text = text.replace(insertion_marker, methods + insertion_marker, 1)
    else:
        print("[already patched] Patch-4L retained-owner methods")

    admission_old = '''    const rpc::TaskSpec *lineage_task_spec =
        task_it->second.task_spec.has_value()
            ? &task_it->second.task_spec.value()
            : owner_task_spec;

    if (lineage_task_spec == nullptr ||
'''
    admission_new = '''    const rpc::TaskSpec *lineage_task_spec = nullptr;

    if (task_it->second.task_spec.has_value()) {
      lineage_task_spec = &task_it->second.task_spec.value();
    } else if (owner_task_spec != nullptr) {
      lineage_task_spec = owner_task_spec;
    } else {
      // Patch 4L: TaskManager may have legitimately dropped ordinary lineage
      // while the application still owns a return ObjectRef.
      const auto retained_it = owner_retained_tasks_.find(task_id);
      if (retained_it != owner_retained_tasks_.end() &&
          !retained_it->second.live_return_ids.empty()) {
        lineage_task_spec = &retained_it->second.task_spec;
      }
    }

    if (lineage_task_spec == nullptr ||
'''
    text = replace_once(
        text,
        admission_old,
        admission_new,
        "PrepareHolderAdmission retained-owner fallback",
    )

    replay_old = '''  const rpc::TaskSpec *lineage_task_spec =
      state.task_spec.has_value() ? &state.task_spec.value() : owner_task_spec;
  if (lineage_task_spec == nullptr ||
'''
    replay_new = '''  const rpc::TaskSpec *lineage_task_spec = nullptr;

  if (state.task_spec.has_value()) {
    lineage_task_spec = &state.task_spec.value();
  } else if (owner_task_spec != nullptr) {
    lineage_task_spec = owner_task_spec;
  } else {
    // Patch 4L: owner replay may outlive TaskManager's ordinary lineage entry.
    const auto retained_it = owner_retained_tasks_.find(task_id);
    if (retained_it != owner_retained_tasks_.end() &&
        !retained_it->second.live_return_ids.empty()) {
      lineage_task_spec = &retained_it->second.task_spec;
    }
  }

  if (lineage_task_spec == nullptr ||
'''
    text = replace_once(
        text,
        replay_old,
        replay_new,
        "PrepareTaskReplay retained-owner fallback",
    )

    return text


def patch_core_worker_cc(text: str) -> str:
    if "// Patch 4L: correctness-preserving retained owner TaskSpec" not in text:
        text = text.replace(
            "// Patch 4K: batched H1 candidate/install path.\n",
            "// Patch 4K: batched H1 candidate/install path.\n"
            "// Patch 4L: correctness-preserving retained owner TaskSpec for late borrow.\n",
            1,
        )

    cb_start = text.find("task_manager_->SetLineageReleasedCallback")
    if cb_start < 0:
        die("Could not find TaskManager lineage-release callback in core_worker.cc")

    cb_window_end = min(len(text), cb_start + 3500)
    cb_window = text[cb_start:cb_window_end]

    guard_old = '''            if (!recovery_succession_enabled_ ||
                recovery_succession_manager_ == nullptr) {
              return;
            }

            auto tombstone = recovery_succession_manager_->BuildTombstoneForTask(task_id);
'''
    guard_new = '''            if (!recovery_succession_enabled_ ||
                recovery_succession_manager_ == nullptr) {
              return;
            }

            // Patch 4L: TaskManager lineage can be released after producer
            // completion even while a returned ObjectRef is still in scope.
            // Actual owner-return lifetime is authoritative for recovery cleanup.
            if (recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              return;
            }

            auto tombstone = recovery_succession_manager_->BuildTombstoneForTask(task_id);
'''
    if guard_new in cb_window:
        print("[already patched] lineage-release live-ref guard")
    else:
        if guard_old not in cb_window:
            die(
                "Could not find the expected lineage-release callback body in "
                "core_worker.cc"
            )
        print("[patching] lineage-release live-ref guard")
        cb_window = cb_window.replace(guard_old, guard_new, 1)
        text = text[:cb_start] + cb_window + text[cb_window_end:]

    submit_marker = '''  returned_refs = task_manager_->AddPendingTask(
      task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);

'''
    retention_block = r'''  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for
  // eligible lazy-recovery tasks. This does NOT activate recovery: no manifest,
  // witness, candidate, holder, or control RPC is created here.
  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs);

    auto on_owner_return_deleted = [this](const ObjectID &deleted_object_id) {
      if (!recovery_succession_enabled_ ||
          recovery_succession_manager_ == nullptr) {
        return;
      }

      const TaskID task_id = deleted_object_id.TaskId();

      if (!recovery_succession_manager_->HandleOwnerReturnRefDeleted(
              deleted_object_id)) {
        return;
      }

      io_service_.post(
          [this, task_id] {
            if (!recovery_succession_enabled_ ||
                recovery_succession_manager_ == nullptr) {
              return;
            }

            auto tombstone =
                recovery_succession_manager_->BuildTombstoneForTask(task_id);
            if (!tombstone.has_value()) {
              return;
            }

            if (!recovery_tombstones_in_flight_.insert(task_id).second) {
              return;
            }

            RAY_LOG(INFO).WithField(task_id)
                << "Owner return refs released; publishing recovery tombstone";

            PublishRecoveryTombstone(std::move(tombstone.value()));
          },
          "CoreWorker.PublishRecoveryTombstone");
    };

    for (const rpc::ObjectReference &returned_ref : returned_refs) {
      if (returned_ref.object_id().size() != ObjectID::Size()) {
        continue;
      }

      const ObjectID object_id =
          ObjectID::FromBinary(returned_ref.object_id());

      const bool callback_added =
          reference_counter_->AddObjectRefDeletedCallback(
              object_id, on_owner_return_deleted);

      if (!callback_added) {
        on_owner_return_deleted(object_id);
      }
    }
  }

'''
    if retention_block.strip() in text:
        print("[already patched] normal-task retained lineage registration")
    else:
        count = text.count(submit_marker)
        if count != 1:
            die(
                f"Expected exactly one normal-task AddPendingTask block, found {count}"
            )
        print("[patching] normal-task retained lineage registration")
        text = text.replace(submit_marker, submit_marker + retention_block, 1)

    lazy_old = '''  const TaskID task_id = object_id.TaskId();
  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);
  if (!task_spec_opt.has_value()) {
    return false;
  }

  const TaskSpecification &task_spec = task_spec_opt.value();
'''
    lazy_new = '''  const TaskID task_id = object_id.TaskId();
  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);

  if (!task_spec_opt.has_value()) {
    // Patch 4L: producer completion may have removed TaskManager's ordinary
    // lineage even though the returned ObjectRef is still strongly live.
    rpc::TaskSpec retained_task_spec;
    if (!recovery_succession_manager_->GetRetainedOwnerTaskSpec(
            task_id, &retained_task_spec)) {
      return false;
    }

    task_spec_opt.emplace(std::move(retained_task_spec));
  }

  const TaskSpecification &task_spec = task_spec_opt.value();
'''
    text = replace_once(
        text,
        lazy_old,
        lazy_new,
        "lazy first-borrow retained TaskSpec fallback",
    )

    return text


def validate(manager_h: str, manager_cc: str, core_worker_cc: str) -> None:
    required_h = [
        "RetainOwnerTaskSpecForLazyRecovery",
        "GetRetainedOwnerTaskSpec",
        "OwnerTaskHasLiveReturns",
        "HandleOwnerReturnRefDeleted",
        "owner_retained_tasks_",
    ]
    required_cc = [
        "RecoverySuccessionManager::RetainOwnerTaskSpecForLazyRecovery",
        "RecoverySuccessionManager::GetRetainedOwnerTaskSpec",
        "RecoverySuccessionManager::OwnerTaskHasLiveReturns",
        "RecoverySuccessionManager::HandleOwnerReturnRefDeleted",
        "owner_retained_tasks_.find(task_id)",
    ]
    required_core = [
        "RetainOwnerTaskSpecForLazyRecovery(",
        "AddObjectRefDeletedCallback(",
        "OwnerTaskHasLiveReturns(task_id)",
        "GetRetainedOwnerTaskSpec(",
        "Owner return refs released; publishing recovery tombstone",
    ]

    for label, text, required in [
        ("manager header", manager_h, required_h),
        ("manager implementation", manager_cc, required_cc),
        ("core_worker", core_worker_cc, required_core),
    ]:
        missing = [s for s in required if s not in text]
        if missing:
            die(
                f"Internal validation failed for {label}; missing: "
                + ", ".join(repr(x) for x in missing)
            )


def main() -> None:
    root = find_repo_root()
    print(f"[repo] {root}")

    paths = {name: root / rel for name, rel in FILES.items()}
    originals = {
        name: path.read_text(encoding="utf-8")
        for name, path in paths.items()
    }

    patched_h = patch_manager_h(originals["manager_h"])
    patched_cc = patch_manager_cc(originals["manager_cc"])
    patched_core = patch_core_worker_cc(originals["core_worker_cc"])

    validate(patched_h, patched_cc, patched_core)

    changed = {
        "manager_h": patched_h != originals["manager_h"],
        "manager_cc": patched_cc != originals["manager_cc"],
        "core_worker_cc": patched_core != originals["core_worker_cc"],
    }

    if not any(changed.values()):
        print("Patch 4L is already applied. Nothing to do.")
        return

    for name, did_change in changed.items():
        if did_change:
            backup(paths[name])

    for name, content in [
        ("manager_h", patched_h),
        ("manager_cc", patched_cc),
        ("core_worker_cc", patched_core),
    ]:
        if changed[name]:
            paths[name].write_text(content, encoding="utf-8")
            print(f"[done] {paths[name].relative_to(root)}")

    print()
    print("Patch 4L applied.")
    print()
    print("Next:")
    print("  1. Rebuild Ray with your usual development build command.")
    print("  2. Re-run the completed-but-live Benchmark 22 v2 B1/B4 diagnostic.")
    print("  3. Do not change candidate retry/progress yet.")
    print()
    print("Expected:")
    print("  Succession B1: LIVE copies/pipeline ~= 1.00, valid=1")
    print("  Baseline   B1: LIVE copies/pipeline ~= 4.00, valid=1")
    print("  Succession B4: LIVE copies/pipeline ~= 4.00, valid=1")
    print("  Baseline   B4: LIVE copies/pipeline ~= 4.00, valid=1")


if __name__ == "__main__":
    main()
