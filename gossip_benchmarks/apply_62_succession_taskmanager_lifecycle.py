#!/usr/bin/env python3
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


def patch_simple(relpath: str, replacements: list[tuple[str, str, str]]) -> bool:
    path = ROOT / relpath
    text = path.read_text()
    changed = False
    for label, old, new in replacements:
        text, did = replace_once(text, old, new, f"{relpath}: {label}")
        changed |= did
    if changed:
        path.write_text(text)
    return changed


def patch_core_worker() -> bool:
    path = ROOT / "src/ray/core_worker/core_worker.cc"
    text = path.read_text()
    changed = False

    old_lineage = '''            // Patch 4L: TaskManager lineage can be released after producer
            // completion even while a returned ObjectRef is still in scope.
            // Actual owner-return lifetime is authoritative for recovery cleanup.
            if (recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              return;
            }

            auto tombstone = recovery_succession_manager_->BuildTombstoneForTask(task_id);
'''
    new_lineage = '''            // Adaptive Succession reuses TaskManager's native
            // reconstructable_return_ids_ lifetime. This callback fires only
            // after the language frontend and all dependent tasks release every
            // reconstructable return, so no second owner-return tracker is needed.
            if (!recovery_witness_holder_baseline_enabled_) {
              const bool should_tombstone =
                  recovery_succession_manager_->HandleOwnerTaskLineageReleased(
                      task_id);
              task_manager_->ReleaseTaskForRecoverySuccession(task_id);
              if (!should_tombstone) {
                return;
              }
            } else if (
                recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              // Fixed-R deliberately keeps its existing exact ObjectID callback
              // lifetime path for an uncontaminated baseline comparison.
              return;
            }

            auto tombstone = recovery_succession_manager_->BuildTombstoneForTask(task_id);
'''
    text, did = replace_once(
        text, old_lineage, new_lineage, "core_worker lineage-release handoff"
    )
    changed |= did

    new_guard_marker = '''    if (recovery_witness_holder_baseline_enabled_) {
      auto on_owner_return_deleted = [this](const ObjectID &deleted_object_id) {
'''
    if new_guard_marker not in text:
        start_marker = '''    auto on_owner_return_deleted = [this](const ObjectID &deleted_object_id) {
'''
        start = text.find(start_marker)
        if start < 0:
            raise RuntimeError("core_worker: owner return callback start not found")

        next_section = text.find("\n  if (recovery_succession_enabled_ &&", start)
        if next_section < 0:
            raise RuntimeError("core_worker: section after owner callback not found")

        outer_close = text.rfind("\n  }\n", start, next_section)
        if outer_close < 0:
            raise RuntimeError("core_worker: enclosing recovery completion block not found")

        block = text[start:outer_close]
        indented = "".join(
            ("  " + line if line.strip() else line)
            for line in block.splitlines(keepends=True)
        )
        wrapped = (
            "    // Fixed-R retains the old per-return callback lifetime path. Adaptive\n"
            "    // Succession relies on TaskManager's existing lineage-release signal instead.\n"
            "    if (recovery_witness_holder_baseline_enabled_) {\n"
            + indented
            + "    }\n"
        )
        text = text[:start] + wrapped + text[outer_close:]
        changed = True

    if changed:
        path.write_text(text)
    return changed


def main() -> None:
    changed_files: list[str] = []

    frontier_h = [
        (
            "member owner-liveness bit",
            '''  uint32_t num_returns = 0;
};
''',
            '''  uint32_t num_returns = 0;

  // Owner-local lifetime bit. It is intentionally not serialized: holders
  // store replay recipes, while only the producer owner decides group cleanup.
  bool owner_returns_live = true;
};
''',
        ),
        (
            "mark owner task released declaration",
            '''  bool IsTaskCommitted(const TaskID &task_id) const;

  /// Look up a member by its producer TaskID.
''',
            '''  bool IsTaskCommitted(const TaskID &task_id) const;

  /// Mark this owner's TaskManager lineage for one member as released.
  /// Idempotent. Returns true iff no registered owner member remains live.
  bool MarkOwnerTaskReleased(const TaskID &task_id);

  /// Look up a member by its producer TaskID.
''',
        ),
        (
            "live owner member counter",
            '''  uint32_t committed_member_count_ = 0;
  uint64_t generation_ = 0;
''',
            '''  uint32_t committed_member_count_ = 0;
  uint32_t live_owner_members_ = 0;
  uint64_t generation_ = 0;
''',
        ),
    ]

    frontier_cc = [
        (
            "increment live owner members",
            '''  task_to_member_index_.emplace(task_id, member_index);
  members_.push_back(std::move(member));

  return RecoveryFrontierMembership{
''',
            '''  task_to_member_index_.emplace(task_id, member_index);
  members_.push_back(std::move(member));
  ++live_owner_members_;

  return RecoveryFrontierMembership{
''',
        ),
        (
            "mark released implementation",
            '''bool RecoveryFrontierGroup::IsTaskCommitted(const TaskID &task_id) const {
  const auto it = task_to_member_index_.find(task_id);
  return it != task_to_member_index_.end() && it->second < committed_member_count_;
}

std::optional<RecoveryFrontierMembership> RecoveryFrontierGroup::FindTask(
''',
            '''bool RecoveryFrontierGroup::IsTaskCommitted(const TaskID &task_id) const {
  const auto it = task_to_member_index_.find(task_id);
  return it != task_to_member_index_.end() && it->second < committed_member_count_;
}

bool RecoveryFrontierGroup::MarkOwnerTaskReleased(const TaskID &task_id) {
  const auto it = task_to_member_index_.find(task_id);
  if (it == task_to_member_index_.end()) {
    return false;
  }

  RecoveryFrontierMember &member = members_[it->second];
  if (!member.owner_returns_live) {
    return live_owner_members_ == 0;
  }

  RAY_CHECK_GT(live_owner_members_, 0U);
  member.owner_returns_live = false;
  --live_owner_members_;
  return live_owner_members_ == 0;
}

std::optional<RecoveryFrontierMembership> RecoveryFrontierGroup::FindTask(
''',
        ),
    ]

    manager_h = [
        (
            "lineage released declaration",
            '''  /// True while this owner task still has at least one live returned ObjectRef.
  bool OwnerTaskHasLiveReturns(const TaskID &task_id) const;

  /// Records actual ObjectRef deletion. Returns true iff this was the final
''',
            '''  /// True while a legacy/Fixed-R owner-retained task still has at least
  /// one live returned ObjectRef.
  bool OwnerTaskHasLiveReturns(const TaskID &task_id) const;

  /// Adaptive-Succession owner cleanup driven by TaskManager's existing
  /// reconstructable-return lifetime. Returns true iff remote recovery state
  /// for this task/frontier group should now be tombstoned.
  bool HandleOwnerTaskLineageReleased(const TaskID &task_id);

  /// Records actual ObjectRef deletion. Returns true iff this was the final
''',
        ),
    ]

    manager_cc = [
        (
            "production adaptive selector before frontier check",
            '''  // Register every eligible live owner task with the shared frontier planner
  // before any backend-specific activation/filtering. This is owner-local only:
  // no holder, witness, manifest, or candidate RPC is emitted here.
  const bool frontier_enabled = RecoveryFrontierEnabled();
  if (frontier_enabled && !returned_refs.empty()) {
    static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));
  }

  const bool baseline_enabled =
      RayConfig::instance().enable_recovery_witness_holder_baseline();
''',
            '''  const bool baseline_enabled =
      RayConfig::instance().enable_recovery_witness_holder_baseline();
  const bool production_adaptive =
      task_manager_owns_recipe && !baseline_enabled;

  // Register every eligible live owner task with the shared frontier planner
  // before any backend-specific activation/filtering. For production adaptive
  // Succession the RayConfig bit is immutable and avoids an extra manager-lock
  // probe just to discover whether the planner exists.
  const bool frontier_enabled =
      production_adaptive
          ? RayConfig::instance().enable_recovery_frontier()
          : RecoveryFrontierEnabled();
  if (frontier_enabled && !returned_refs.empty()) {
    static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));
  }
''',
        ),
        (
            "remove adaptive retained state",
            '''  // Production CoreWorker Recovery Succession always pins the existing
  // TaskManager entry. Keep the config/baseline checks for compatibility with
  // direct manager callers and older experiments.
  const bool task_manager_pin =
      task_manager_owns_recipe || baseline_enabled ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();

  // Deliberately keep Fixed-R on its old ObjectID-set lifetime path. The first
  // optimization pass is scoped to adaptive Succession only.
  const bool succession_counter_lifetime =
      task_manager_owns_recipe && !baseline_enabled;

  OwnerRetainedTaskState retained;
''',
            '''  // Production adaptive Succession owns neither a duplicate TaskSpec nor
  // duplicate return-lifetime state here. TaskManager already owns both the
  // immutable recipe and reconstructable_return_ids_. Fixed-R/direct-manager
  // paths deliberately retain the old manager state for baseline isolation.
  if (production_adaptive) {
    return;
  }

  const bool task_manager_pin =
      baseline_enabled ||
      RayConfig::instance().enable_recovery_succession_task_manager_pin();

  // Legacy/direct-manager and Fixed-R paths use the exact ObjectID set.
  const bool succession_counter_lifetime = false;

  OwnerRetainedTaskState retained;
''',
        ),
        (
            "insert lineage released implementation",
            '''bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(
    const ObjectID &object_id,
''',
            '''bool RecoverySuccessionManager::HandleOwnerTaskLineageReleased(
    const TaskID &task_id) {
  if (task_id.IsNil() ||
      RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  if (recovery_frontier_planner_ != nullptr &&
      recovery_frontier_planner_->GroupSize() > 1) {
    const auto membership = recovery_frontier_planner_->FindTask(task_id);
    if (membership.has_value()) {
      RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetMutableGroup(membership->group_id);
      RAY_CHECK(group != nullptr);

      if (!group->MarkOwnerTaskReleased(task_id)) {
        return false;
      }

      // The final live member released its native TaskManager lineage. Close a
      // partial group before a future task can append to a terminal capsule.
      RAY_CHECK(recovery_frontier_planner_->SealGroup(membership->group_id));

      if (recovery_frontier_protection_manifests_.contains(
              membership->group_id)) {
        return true;
      }

      // Never activated/exported: there is no remote state to tombstone.
      RAY_CHECK(recovery_frontier_planner_->EraseGroup(membership->group_id));
      return false;
    }
  }

  const auto task_it = task_states_.find(task_id);
  return task_it != task_states_.end() &&
         !task_it->second.manifest.tombstoned();
}

bool RecoverySuccessionManager::HandleOwnerReturnRefDeleted(
    const ObjectID &object_id,
''',
        ),
    ]

    for relpath, repls in [
        ("src/ray/core_worker/recovery_frontier.h", frontier_h),
        ("src/ray/core_worker/recovery_frontier.cc", frontier_cc),
        ("src/ray/core_worker/recovery_succession_manager.h", manager_h),
        ("src/ray/core_worker/recovery_succession_manager.cc", manager_cc),
    ]:
        if patch_simple(relpath, repls):
            changed_files.append(relpath)

    if patch_core_worker():
        changed_files.append("src/ray/core_worker/core_worker.cc")

    core = (ROOT / "src/ray/core_worker/core_worker.cc").read_text()
    manager = (ROOT / "src/ray/core_worker/recovery_succession_manager.cc").read_text()
    frontier = (ROOT / "src/ray/core_worker/recovery_frontier.cc").read_text()

    required = [
        ("core baseline-only owner callback", "if (recovery_witness_holder_baseline_enabled_) {" in core),
        ("core TaskManager lineage release", "HandleOwnerTaskLineageReleased" in core),
        ("core adaptive pin release", "ReleaseTaskForRecoverySuccession(task_id)" in core),
        ("manager production early return", "if (production_adaptive)" in manager),
        ("manager lineage handler", "HandleOwnerTaskLineageReleased" in manager),
        ("frontier owner liveness", "MarkOwnerTaskReleased" in frontier),
    ]
    missing = [name for name, ok in required if not ok]
    if missing:
        raise RuntimeError("post-patch structural checks failed: " + ", ".join(missing))

    subprocess.run(["git", "diff", "--check"], cwd=ROOT, check=True)

    if changed_files:
        print("Applied TaskManager-native adaptive-Succession lifecycle:")
        for relpath in changed_files:
            print(f"  {relpath}")
    else:
        print("TaskManager-native adaptive-Succession lifecycle already applied.")

    print(
        "\nNext: rebuild Ray; run correctness benchmarks 55, 56, 57 "
        "(and 52/53 if convenient), then rerun Benchmark 58 with 3 reps."
    )


if __name__ == "__main__":
    main()
