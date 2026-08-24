#!/usr/bin/env python3
"""
Remove temporary Recovery Succession diagnostic code while preserving:
  * correctness-preserving Recovery Succession (4L/4K path)
  * witness-holder baseline
  * 4M certificate-admission research variant
  * 4N TaskManager-pin research variant
  * all Recovery Succession profiling counters/timers

Removed:
  * Patch-4G benchmark ablation modes / RAY_RECOVERY_ABLATION_MODE plumbing
  * metadata_no_receiver / metadata_no_transport
  * activation_only / dormant_only
  * 4O metadata-reuse experiment
  * 4P deferred-ObjectRef-metadata experiment
  * 4Q TaskManager-native-lifetime experiment
  * unsafe owner-lifetime-skip diagnostic

All changes are staged in memory. Files are written only after every expected
transform and post-cleanup validation succeeds.

Usage:
  python cleanup_recovery_succession_diagnostics.py /path/to/ray --check
  python cleanup_recovery_succession_diagnostics.py /path/to/ray
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

TEXT_FILES = [
    "src/ray/common/ray_config_def.h",
    "src/ray/core_worker/core_worker.cc",
    "src/ray/core_worker/recovery_succession_manager.cc",
    "src/ray/core_worker/recovery_succession_manager.h",
    "src/ray/core_worker/reference_counter_interface.h",
    "src/ray/core_worker/reference_counter.h",
    "src/ray/core_worker/reference_counter.cc",
    "src/ray/core_worker/task_manager.h",
    "src/ray/core_worker/task_manager.cc",
    "gossip_benchmarks/_benchmark_common.py",
    "gossip_benchmarks/22_succession_vs_lazy_baseline_v2.py",
]

DIAGNOSTIC_TOKENS = [
    "RecoveryBenchmarkAblationMode",
    "recovery_succession_benchmark_ablation_mode",
    "metadata_no_receiver",
    "metadata_no_transport",
    "metadata_only",
    "no_piggyback",
    "patch4g_mode",
    "owner_recovery_task_specs",
    "owner_task_specs",
    "preserve_legacy_h1_fast_path",
    "activation_only",
    "dormant_only",
    "piggyback_no_candidate",
    "candidate_rpc_no_admit",
    "RAY_RECOVERY_ABLATION_MODE",
    "enable_recovery_succession_metadata_reuse",
    "RAY_RECOVERY_METADATA_REUSE",
    "enable_recovery_succession_defer_objectref_metadata",
    "RAY_RECOVERY_DEFER_OBJECTREF_METADATA",
    "enable_recovery_succession_task_manager_lifetime",
    "RAY_RECOVERY_TASKMANAGER_LIFETIME",
    "enable_recovery_succession_skip_owner_lifetime_for_benchmark",
    "RAY_RECOVERY_SKIP_OWNER_LIFETIME",
    "Patch 4O-META-REUSE",
    "Patch 4P experimental diagnostic",
    "Patch 4Q-TM-LIFETIME",
]


class CleanupError(RuntimeError):
    pass


def exact_replace(files, rel, old, new, label):
    text = files[rel]
    count = text.count(old)
    if count != 1:
        raise CleanupError(
            f"{label}: expected exactly one match in {rel}, found {count}"
        )
    files[rel] = text.replace(old, new, 1)


def regex_replace(files, rel, pattern, replacement, label, flags=0):
    text = files[rel]
    new, count = re.subn(pattern, replacement, text, count=1, flags=flags)
    if count != 1:
        raise CleanupError(
            f"{label}: expected exactly one regex match in {rel}, found {count}"
        )
    files[rel] = new


def remove_braced_if_after_marker(files, rel, marker, label):
    """Remove marker text and the immediately following balanced if block."""
    text = files[rel]
    start = text.find(marker)
    if start < 0:
        raise CleanupError(f"{label}: marker not found in {rel}")
    if_pos = text.find("if (", start + len(marker))
    if if_pos < 0:
        raise CleanupError(f"{label}: if block not found in {rel}")
    brace = text.find("{", if_pos)
    if brace < 0:
        raise CleanupError(f"{label}: opening brace not found in {rel}")

    depth = 0
    in_string = False
    escape = False
    quote = ""
    end = None
    i = brace
    while i < len(text):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                in_string = False
        else:
            if ch in ('"', "'"):
                in_string = True
                quote = ch
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        i += 1
    if end is None:
        raise CleanupError(f"{label}: unterminated if block in {rel}")

    while end < len(text) and text[end] in " \t":
        end += 1
    if end < len(text) and text[end] == "\n":
        end += 1
    if end < len(text) and text[end] == "\n":
        end += 1
    files[rel] = text[:start] + text[end:]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo", nargs="?", default=".")
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    root = Path(args.repo).resolve()

    files = {}
    originals = {}
    for rel in TEXT_FILES:
        path = root / rel
        if not path.exists():
            raise CleanupError(f"missing expected file: {path}")
        text = path.read_text()
        files[rel] = text
        originals[rel] = text

    # --------------------------------------------------------------
    # Final unsafe owner-lifetime-skip diagnostic.
    # --------------------------------------------------------------
    exact_replace(
        files,
        "src/ray/common/ray_config_def.h",
        '''RAY_CONFIG(bool, enable_recovery_succession_task_manager_lifetime, false)

/// BENCHMARK ONLY. Intentionally removes all owner-side dormant lifetime
/// retention. This breaks late-borrow correctness and must never be enabled
/// outside the dormant_only diagnostic experiment.
RAY_CONFIG(bool,
           enable_recovery_succession_skip_owner_lifetime_for_benchmark,
           false)''',
        '''RAY_CONFIG(bool, enable_recovery_succession_task_manager_lifetime, false)''',
        "remove unsafe owner-lifetime config",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''    skip_owner_lifetime = (
        os.environ.get("RAY_RECOVERY_SKIP_OWNER_LIFETIME", "0") == "1"
        and method.recovery_enabled
    )
''',
        "",
        "remove unsafe owner-lifetime env",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''        "enable_recovery_succession_skip_owner_lifetime_for_benchmark": skip_owner_lifetime,
''',
        "",
        "remove unsafe owner-lifetime config plumbing",
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !RayConfig::instance()
           .enable_recovery_succession_skip_owner_lifetime_for_benchmark() &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
''',
        '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
''',
        "restore normal owner-lifetime condition",
    )

    # --------------------------------------------------------------
    # Remove 4Q TaskManager-native lifetime experiment.
    # --------------------------------------------------------------
    exact_replace(
        files,
        "src/ray/common/ray_config_def.h",
        '''/// Patch 4Q-TM-LIFETIME. Keep the existing TaskManager TaskEntry/spec
/// while any static return ObjectRef is live. Lifetime ends through one
/// ReferenceCounter-wide true-deletion hook instead of per-return callbacks.
/// Default false so Patch 4L remains the control behavior.
RAY_CONFIG(bool, enable_recovery_succession_task_manager_lifetime, false)

/// Enables lightweight profiling of recovery-succession holder formation.
''',
        '''/// Enables lightweight profiling of recovery-succession holder formation.
''',
        "remove 4Q config",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''    task_manager_lifetime = (
        os.environ.get("RAY_RECOVERY_TASKMANAGER_LIFETIME", "0") == "1"
        and method.recovery_enabled
    )
''',
        "",
        "remove 4Q benchmark env",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''        "enable_recovery_succession_task_manager_lifetime": task_manager_lifetime,
''',
        "",
        "remove 4Q benchmark config",
    )
    exact_replace(
        files,
        "src/ray/core_worker/reference_counter_interface.h",
        '''  virtual bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) = 0;

  /// Install one process-wide callback invoked when an owned Reference entry
  /// is actually erased. Default no-op keeps alternate/test implementations
  /// source compatible.
  virtual void SetOwnedObjectRefDeletedCallback(
      const std::function<void(const ObjectID &)> &) {}

''',
        '''  virtual bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) = 0;

''',
        "remove 4Q ReferenceCounterInterface hook",
    )
    exact_replace(
        files,
        "src/ray/core_worker/reference_counter.h",
        '''  bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) override
      ABSL_LOCKS_EXCLUDED(mutex_);

  void SetOwnedObjectRefDeletedCallback(
      const std::function<void(const ObjectID &)> &callback) override
      ABSL_LOCKS_EXCLUDED(mutex_);

''',
        '''  bool AddObjectRefDeletedCallback(
      const ObjectID &object_id, std::function<void(const ObjectID &)> callback) override
      ABSL_LOCKS_EXCLUDED(mutex_);

''',
        "remove 4Q ReferenceCounter declaration",
    )
    exact_replace(
        files,
        "src/ray/core_worker/reference_counter.h",
        '''  LineageReleasedCallback on_lineage_released_;

  /// Patch 4Q: fires only from EraseReference for a truly deleted owned ref.
  std::function<void(const ObjectID &)> on_owned_object_ref_deleted_
      ABSL_GUARDED_BY(mutex_) = nullptr;

  /// Optional shutdown hook to call when all references have gone
''',
        '''  LineageReleasedCallback on_lineage_released_;
  /// Optional shutdown hook to call when all references have gone
''',
        "remove 4Q ReferenceCounter storage",
    )
    exact_replace(
        files,
        "src/ray/core_worker/reference_counter.cc",
        '''  if (it->second.owned_by_us_ && on_owned_object_ref_deleted_) {
    on_owned_object_ref_deleted_(it->first);
  }

  for (const auto &callback : it->second.object_ref_deleted_callbacks) {
    callback(it->first);
  }

  object_id_refs_.erase(it);
''',
        '''  for (const auto &callback : it->second.object_ref_deleted_callbacks) {
    callback(it->first);
  }

  object_id_refs_.erase(it);
''',
        "remove 4Q ReferenceCounter callback invocation",
    )
    exact_replace(
        files,
        "src/ray/core_worker/reference_counter.cc",
        '''void ReferenceCounter::SetReleaseLineageCallback(
    const LineageReleasedCallback &callback) {
  RAY_CHECK(on_lineage_released_ == nullptr);
  on_lineage_released_ = callback;
}

void ReferenceCounter::SetOwnedObjectRefDeletedCallback(
    const std::function<void(const ObjectID &)> &callback) {
  absl::MutexLock lock(&mutex_);
  RAY_CHECK(on_owned_object_ref_deleted_ == nullptr);
  on_owned_object_ref_deleted_ = callback;
}

''',
        '''void ReferenceCounter::SetReleaseLineageCallback(
    const LineageReleasedCallback &callback) {
  RAY_CHECK(on_lineage_released_ == nullptr);
  on_lineage_released_ = callback;
}

''',
        "remove 4Q ReferenceCounter setter",
    )
    exact_replace(
        files,
        "src/ray/core_worker/task_manager.h",
        '''  bool PinTaskForRecoverySuccession(const TaskID &task_id);

  /// Patch 4Q: called when an owned static return Reference is truly erased.
  /// Returns true exactly when this was the final tracked live return.
  bool ReleaseRecoverySuccessionReturn(const ObjectID &object_id);

  /// Whether this TaskEntry is retained solely/partly for live recovery returns.
  bool RecoverySuccessionTaskHasLiveReturns(const TaskID &task_id) const;

  /// Releases the Patch-4N recovery pin. If normal Ray lineage is already gone
''',
        '''  bool PinTaskForRecoverySuccession(const TaskID &task_id);

  /// Releases the Patch-4N recovery pin. If normal Ray lineage is already gone
''',
        "remove 4Q TaskManager API",
    )
    exact_replace(
        files,
        "src/ray/core_worker/task_manager.h",
        '''    // Patch 4N-PIN. This protects only the TaskEntry/spec_ from erasure.
    // Ordinary Ray dependency lineage is still released normally.
    bool recovery_succession_pinned_ = false;

    // Patch 4Q. Counts live static return ObjectRefs, including direct/in-memory
    // returns that are not present in reconstructable_return_ids_.
    size_t recovery_succession_live_return_count_ = 0;

''',
        '''    // Patch 4N-PIN. This protects only the TaskEntry/spec_ from erasure.
    // Ordinary Ray dependency lineage is still released normally.
    bool recovery_succession_pinned_ = false;

''',
        "remove 4Q TaskEntry counter",
    )
    exact_replace(
        files,
        "src/ray/core_worker/task_manager.cc",
        '''    auto inserted = submissible_tasks_.try_emplace(
        spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
    RAY_CHECK(inserted.second);

    // Patch 4Q: piggyback lifetime retention on the TaskEntry insertion we are
    // already doing. No additional TaskManager lock or second TaskSpec copy.
    if (!recovery_replay &&
        RayConfig::instance().enable_recovery_succession_task_manager_lifetime() &&
        spec.GetMessage().type() == rpc::TaskType::NORMAL_TASK &&
        !spec.GetMessage().returns_dynamic() &&
        !spec.GetMessage().streaming_generator() &&
        max_retries != 0 &&
        num_returns > 0) {
      inserted.first->second.recovery_succession_pinned_ = true;
      inserted.first->second.recovery_succession_live_return_count_ = num_returns;
    }

    num_pending_tasks_++;
''',
        '''    auto inserted = submissible_tasks_.try_emplace(
        spec.TaskId(), spec, max_retries, num_returns, task_counter_, max_oom_retries);
    RAY_CHECK(inserted.second);
    num_pending_tasks_++;
''',
        "remove 4Q TaskManager insertion hook",
    )
    regex_replace(
        files,
        "src/ray/core_worker/task_manager.cc",
        r'''\nbool TaskManager::ReleaseRecoverySuccessionReturn\(const ObjectID &object_id\) \{.*?\n\}\n\nbool TaskManager::RecoverySuccessionTaskHasLiveReturns\(\n    const TaskID &task_id\) const \{.*?\n\}\n\n''',
        "\n",
        "remove 4Q TaskManager methods",
        flags=re.S,
    )
    exact_replace(
        files,
        "src/ray/core_worker/task_manager.cc",
        '''  it->second.recovery_succession_pinned_ = false;
  it->second.recovery_succession_live_return_count_ = 0;

  // Two valid orderings exist:
''',
        '''  it->second.recovery_succession_pinned_ = false;

  // Two valid orderings exist:
''',
        "remove 4Q counter clear",
    )

    # Remove the 4Q process-wide callback inserted before the lineage callback.
    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        r'''    // Patch 4Q: one process-wide true-deletion hook replaces per-return\n    // AddObjectRefDeletedCallback registrations\.\n    reference_counter_->SetOwnedObjectRefDeletedCallback\(.*?\n        \}\);\n\n    task_manager_->SetLineageReleasedCallback''',
        '''    task_manager_->SetLineageReleasedCallback''',
        "remove 4Q CoreWorker global callback",
        flags=re.S,
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''            if (RayConfig::instance()
                    .enable_recovery_succession_task_manager_lifetime()) {
              if (task_manager_->RecoverySuccessionTaskHasLiveReturns(task_id)) {
                return;
              }
            } else if (
                recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              return;
            }

''',
        '''            if (recovery_succession_manager_->OwnerTaskHasLiveReturns(task_id)) {
              return;
            }

''',
        "restore 4L lineage lifetime check",
    )

    # Replace the entire SubmitTask owner-lifetime section with the clean 4L/4N path.
    submit_clean = '''  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for
  // eligible lazy-recovery tasks. This does NOT activate recovery: no manifest,
  // witness, candidate, holder, or control RPC is created here.
  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !task_spec.GetMessage().has_recovery_manifest() &&
      RecoverySuccessionManager::IsEligibleTask(task_spec.GetMessage())) {
    if (RayConfig::instance().enable_recovery_succession_task_manager_pin()) {
      RAY_CHECK(task_manager_->PinTaskForRecoverySuccession(task_spec.TaskId()))
          << "Eligible recovery task disappeared before TaskManager pin: "
          << task_spec.TaskId();
    }

    recovery_succession_manager_->RetainOwnerTaskSpecForLazyRecovery(
        task_spec, returned_refs);

    auto on_owner_return_deleted = [this](const ObjectID &deleted_object_id) {
      if (!recovery_succession_enabled_ ||
          recovery_succession_manager_ == nullptr) {
        return;
      }

      const TaskID deleted_task_id = deleted_object_id.TaskId();

      bool final_return_deleted = false;
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
          [this, deleted_task_id] {
            if (!recovery_succession_enabled_ ||
                recovery_succession_manager_ == nullptr) {
              return;
            }

            auto tombstone =
                recovery_succession_manager_->BuildTombstoneForTask(deleted_task_id);
            if (!tombstone.has_value()) {
              return;
            }

            if (!recovery_tombstones_in_flight_.insert(deleted_task_id).second) {
              return;
            }

            RAY_LOG(INFO).WithField(deleted_task_id)
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
    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        r'''  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for\n.*?\n  if \(recovery_succession_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      task_spec\.GetMessage\(\)\.has_recovery_manifest\(\)\) \{''',
        submit_clean + '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      task_spec.GetMessage().has_recovery_manifest()) {''',
        "restore clean 4L/4N SubmitTask lifetime path",
        flags=re.S,
    )

    # --------------------------------------------------------------
    # Remove 4P deferred ObjectRef metadata experiment.
    # --------------------------------------------------------------
    exact_replace(
        files,
        "src/ray/common/ray_config_def.h",
        '''RAY_CONFIG(bool, enable_recovery_succession_metadata_reuse, false)


/// Patch 4P experimental diagnostic. When true, CoreWorker::GetObjectRefs()
/// does not eagerly materialize recovery metadata. Task submission still
/// activates recovery and constructs the Patch-4I TaskSpec sidecar later.
RAY_CONFIG(bool, enable_recovery_succession_defer_objectref_metadata, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        '''RAY_CONFIG(bool, enable_recovery_succession_metadata_reuse, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        "remove 4P config",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''    defer_objectref_metadata = (
        os.environ.get("RAY_RECOVERY_DEFER_OBJECTREF_METADATA", "0") == "1"
        and method.recovery_enabled
    )
''',
        "",
        "remove 4P benchmark env",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''        "enable_recovery_succession_defer_objectref_metadata": defer_objectref_metadata,
''',
        "",
        "remove 4P benchmark config",
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''    if (recovery_succession_enabled_ &&
        recovery_succession_manager_ != nullptr &&
        !RayConfig::instance()
             .enable_recovery_succession_defer_objectref_metadata()) {
      rpc::RecoveryObjectMetadata metadata;

      if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {
        ref.mutable_recovery_metadata()->CopyFrom(metadata);
      }
    }
''',
        '''    if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
      rpc::RecoveryObjectMetadata metadata;

      if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {
        ref.mutable_recovery_metadata()->CopyFrom(metadata);
      }
    }
''',
        "remove 4P GetObjectRefs branch",
    )

    # --------------------------------------------------------------
    # Remove 4O metadata reuse experiment.
    # --------------------------------------------------------------
    exact_replace(
        files,
        "src/ray/common/ray_config_def.h",
        '''RAY_CONFIG(bool, enable_recovery_succession_task_manager_pin, false)


/// Patch 4O-META-REUSE experimental optimization. When true, task construction
/// reuses valid recovery metadata already carried by nested ObjectReferences
/// instead of rebuilding the same metadata from manager state.
RAY_CONFIG(bool, enable_recovery_succession_metadata_reuse, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        '''RAY_CONFIG(bool, enable_recovery_succession_task_manager_pin, false)


/// Enables lightweight profiling of recovery-succession holder formation.
''',
        "remove 4O config",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''    metadata_reuse = (
        os.environ.get("RAY_RECOVERY_METADATA_REUSE", "0") == "1"
        and method.recovery_enabled
    )
''',
        "",
        "remove 4O benchmark env",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''        "enable_recovery_succession_metadata_reuse": metadata_reuse,
''',
        "",
        "remove 4O benchmark config",
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''    if (arg.has_object_ref() && !arg.object_ref().object_id().empty()) {
      const rpc::ObjectReference &object_ref = arg.object_ref();
      if (!RayConfig::instance().enable_recovery_succession_metadata_reuse() ||
          !object_ref.has_recovery_metadata()) {
        const ObjectID object_id =
            ObjectID::FromBinary(object_ref.object_id());
        TryPopulateRecoveryMetadataForObject(object_id, nullptr);
      }
    }
''',
        '''    if (arg.has_object_ref() && !arg.object_ref().object_id().empty()) {
      const ObjectID object_id =
          ObjectID::FromBinary(arg.object_ref().object_id());
      TryPopulateRecoveryMetadataForObject(object_id, nullptr);
    }
''',
        "remove 4O direct-ref reuse",
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''      if (!RayConfig::instance().enable_recovery_succession_metadata_reuse() ||
          !nested_ref.has_recovery_metadata()) {
        const ObjectID nested_id =
            ObjectID::FromBinary(nested_ref.object_id());
        TryPopulateRecoveryMetadataForObject(nested_id, nullptr);
      }
''',
        '''      const ObjectID nested_id =
          ObjectID::FromBinary(nested_ref.object_id());
      TryPopulateRecoveryMetadataForObject(nested_id, nullptr);
''',
        "remove 4O nested-ref reuse",
    )
    exact_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        '''    rpc::RecoveryObjectMetadata source_storage;
    rpc::RecoveryObjectMetadata legacy_expanded;
    const rpc::RecoveryObjectMetadata *source = nullptr;

    const bool reuse_carried_metadata =
        RayConfig::instance().enable_recovery_succession_metadata_reuse();

    if (reuse_carried_metadata && had_legacy_transport) {
      if (!legacy_transport.task_id().empty() && legacy_transport.has_manifest()) {
        source = &legacy_transport;
      } else {
        rpc::ObjectReference synthetic_ref;
        synthetic_ref.set_object_id(object_id.Binary());
        if (object_ref->has_owner_address()) {
          synthetic_ref.mutable_owner_address()->CopyFrom(object_ref->owner_address());
        }
        synthetic_ref.mutable_recovery_metadata()->CopyFrom(legacy_transport);
        if (ExpandTaskArgumentRecoveryMetadata(synthetic_ref, &legacy_expanded)) {
          source = &legacy_expanded;
        }
      }
    }

    if (source == nullptr && BuildRecoveryMetadataLocked(object_id, &source_storage)) {
      source = &source_storage;
    } else if (source == nullptr && had_legacy_transport) {
      rpc::ObjectReference synthetic_ref;
      synthetic_ref.set_object_id(object_id.Binary());
      if (object_ref->has_owner_address()) {
        synthetic_ref.mutable_owner_address()->CopyFrom(object_ref->owner_address());
      }
      synthetic_ref.mutable_recovery_metadata()->CopyFrom(legacy_transport);
      if (ExpandTaskArgumentRecoveryMetadata(synthetic_ref, &legacy_expanded)) {
        source = &legacy_expanded;
      }
    }
''',
        '''    rpc::RecoveryObjectMetadata source_storage;
    rpc::RecoveryObjectMetadata legacy_expanded;
    const rpc::RecoveryObjectMetadata *source = nullptr;

    if (BuildRecoveryMetadataLocked(object_id, &source_storage)) {
      source = &source_storage;
    } else if (had_legacy_transport) {
      rpc::ObjectReference synthetic_ref;
      synthetic_ref.set_object_id(object_id.Binary());
      if (object_ref->has_owner_address()) {
        synthetic_ref.mutable_owner_address()->CopyFrom(object_ref->owner_address());
      }
      synthetic_ref.mutable_recovery_metadata()->CopyFrom(legacy_transport);
      if (ExpandTaskArgumentRecoveryMetadata(synthetic_ref, &legacy_expanded)) {
        source = &legacy_expanded;
      }
    }
''',
        "remove 4O manager reuse",
    )

    # --------------------------------------------------------------
    # Remove all Patch-4G benchmark ablation machinery; keep profiling.
    # --------------------------------------------------------------
    regex_replace(
        files,
        "src/ray/common/ray_config_def.h",
        r'''/// Patch 4G BENCHMARK ONLY\. Selects a diagnostic B1 ablation\. Production and\n/// correctness runs must use the default "full" mode\.\n/// Supported values:\n(?:.*\n)*?RAY_CONFIG\(std::string, recovery_succession_benchmark_ablation_mode, "full"\)\n\n''',
        "",
        "remove benchmark ablation RayConfig",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''    profiling_enabled: bool = False,
    ablation_mode: str = "full",
) -> dict[str, Any]:
''',
        '''    profiling_enabled: bool = False,
) -> dict[str, Any]:
''',
        "remove system_config ablation parameter",
    )
    exact_replace(
        files,
        "gossip_benchmarks/_benchmark_common.py",
        '''        "recovery_succession_benchmark_ablation_mode": str(ablation_mode),
''',
        "",
        "remove ablation config plumbing",
    )
    exact_replace(
        files,
        "gossip_benchmarks/22_succession_vs_lazy_baseline_v2.py",
        '''            profiling_enabled=(
    method.recovery_enabled
    and os.environ.get("RAY_RECOVERY_PROFILING", "1") == "1"
),
            ablation_mode=os.environ.get("RAY_RECOVERY_ABLATION_MODE", "full"),
''',
        '''            profiling_enabled=(
                method.recovery_enabled
                and os.environ.get("RAY_RECOVERY_PROFILING", "1") == "1"
            ),
''',
        "remove Benchmark 22 ablation env",
    )

    helper_pattern = r'''const std::string &RecoveryBenchmarkAblationMode\(\) \{\n  static const std::string mode =\n      RayConfig::instance\(\)\.recovery_succession_benchmark_ablation_mode\(\);\n  RAY_CHECK\(.*?\n  return mode;\n\}\n\n'''
    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        helper_pattern,
        "",
        "remove CoreWorker ablation helper",
        flags=re.S,
    )
    regex_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        helper_pattern,
        "",
        "remove manager ablation helper",
        flags=re.S,
    )

    # Canonical production Full task-argument path.
    clean_build_block = '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !args.empty()) {
    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());

    if (recovery_succession_profiling_enabled_) {
      const uint64_t start_ns = RecoveryProfileNowNs();

      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage());

      recovery_succession_manager_->RecordTaskArgumentMetadataLatency(
          RecoveryProfileNowNs() - start_ns);
    } else {
      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage());
    }
  }

}

void CoreWorker::PrestartWorkers'''
    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        r'''  if \(recovery_succession_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      !args\.empty\(\)\) \{\n.*?\n  \}\n\n\}\n\nvoid CoreWorker::PrestartWorkers''',
        clean_build_block,
        "replace BuildCommonTaskSpec diagnostics with Full path",
        flags=re.S,
    )

    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        r'''  if \(recovery_succession_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n(?:      RecoveryBenchmarkAblationMode\(\) != "metadata_no_receiver" &&\n)?(?:      RecoveryBenchmarkAblationMode\(\) != "metadata_no_transport" &&\n)?      RecoverySuccessionManager::CarriesRecoveryMetadata\(\n          request\.task_spec\(\)\)\) \{''',
        '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      RecoverySuccessionManager::CarriesRecoveryMetadata(
          request.task_spec())) {''',
        "restore receiver Full path",
    )

    remove_braced_if_after_marker(
        files,
        "src/ray/core_worker/core_worker.cc",
        "  // Patch 4G BENCHMARK ONLY: preserve candidate-report construction and the\n",
        "remove candidate_rpc_no_admit control",
    )
    regex_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        r'''  // Keep Patch-4E-1's old single-H1 behavior only in the explicit\n  // no_piggyback benchmark control so we can isolate the batching effect\.\n  const bool first_holder_candidate =\n      !request\.has_cached_manifest\(\) \|\|\n      request\.cached_manifest\(\)\.succession_size\(\) <= 1;\n  const bool preserve_legacy_h1_fast_path =\n      first_holder_candidate &&\n      RecoveryBenchmarkAblationMode\(\) == "no_piggyback";\n\n''',
        "",
        "remove no_piggyback fast-path variables",
    )
    exact_replace(
        files,
        "src/ray/core_worker/core_worker.cc",
        '''      coordinator_address.worker_id().empty() ||
      preserve_legacy_h1_fast_path) {
''',
        '''      coordinator_address.worker_id().empty()) {
''',
        "remove no_piggyback condition",
    )

    # Manager Full path no longer accepts a diagnostic owner-TaskSpec map.
    exact_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        '''void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec,
    const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs) {
''',
        '''void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec) {
''',
        "simplify PopulateTaskArgumentMetadata definition",
    )
    exact_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        '''  auto populate_one = [this, task_spec, &attached_object_ids, owner_task_specs](
''',
        '''  auto populate_one = [this, task_spec, &attached_object_ids](
''',
        "remove diagnostic owner TaskSpec capture",
    )

    rel = "src/ray/core_worker/recovery_succession_manager.cc"
    text = files[rel]
    start_marker = '''    // Keep the witness-as-holder baseline unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      return;
    }

    // Patch 4G benchmark ablations'''
    start = text.find(start_marker)
    if start < 0:
        raise CleanupError("retired piggyback diagnostic block start not found")
    tail = '''  };

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {'''
    end = text.find(tail, start)
    if end < 0:
        raise CleanupError("retired piggyback diagnostic block end not found")
    files[rel] = text[:start] + tail + text[end + len(tail):]

    regex_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.cc",
        r'''  const std::string &patch4g_mode = RecoveryBenchmarkAblationMode\(\);\n  if \(patch4g_mode == "metadata_only" \|\|\n      patch4g_mode == "metadata_no_receiver" \|\|\n      patch4g_mode == "metadata_no_transport" \|\|\n      patch4g_mode == "piggyback_no_candidate"\) \{\n    return;\n  \}\n\n''',
        "",
        "remove candidate-report ablation gate",
    )
    exact_replace(
        files,
        "src/ray/core_worker/recovery_succession_manager.h",
        '''  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  /// Patch 4F may atomically claim the one-shot H1 TaskSpec piggyback, so this
  /// method intentionally mutates manager state.
  void PopulateTaskArgumentMetadata(
      rpc::TaskSpec *task_spec,
      const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs = nullptr);
''',
        '''  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec);
''',
        "simplify manager header API",
    )

    # Keep Patch-4G profiling, remove only the ablation wording.
    for rel in (
        "src/ray/core_worker/core_worker.cc",
        "src/ray/core_worker/recovery_succession_manager.cc",
        "src/ray/core_worker/recovery_succession_manager.h",
    ):
        files[rel] = files[rel].replace(
            "Patch 4G: hot-path profiling and B1 ablations.",
            "Patch 4G: hot-path profiling.",
        )

    # --------------------------------------------------------------
    # Validation before any write.
    # --------------------------------------------------------------
    required = {
        "src/ray/common/ray_config_def.h": [
            "enable_recovery_succession_profiling",
            "enable_recovery_succession_certificate_admission",
            "enable_recovery_succession_task_manager_pin",
            "enable_recovery_witness_holder_baseline",
        ],
        "src/ray/core_worker/recovery_succession_manager.h": [
            "RecoverySuccessionProfile",
        ],
        "gossip_benchmarks/22_succession_vs_lazy_baseline_v2.py": [
            "PROFILE_KEYS",
            "RAY_RECOVERY_PROFILING",
        ],
        "gossip_benchmarks/_benchmark_common.py": [
            "RAY_RECOVERY_CERTIFICATE_ADMISSION",
            "RAY_RECOVERY_TASKMANAGER_PIN",
        ],
    }
    for rel, tokens in required.items():
        for token in tokens:
            if token not in files[rel]:
                raise CleanupError(f"preservation check failed: {rel} missing {token}")

    leftovers = []
    for rel, text in files.items():
        for token in DIAGNOSTIC_TOKENS:
            if token in text:
                leftovers.append(f"{rel}: {token}")

    # Also make sure no other source/benchmark file still depends on a removed
    # diagnostic flag or ablation API. This catches older benchmark callers
    # before we write anything and avoids leaving the tree uncompilable.
    global_tokens = [
        "RecoveryBenchmarkAblationMode",
        "recovery_succession_benchmark_ablation_mode",
        "RAY_RECOVERY_ABLATION_MODE",
        "enable_recovery_succession_metadata_reuse",
        "RAY_RECOVERY_METADATA_REUSE",
        "enable_recovery_succession_defer_objectref_metadata",
        "RAY_RECOVERY_DEFER_OBJECTREF_METADATA",
        "enable_recovery_succession_task_manager_lifetime",
        "RAY_RECOVERY_TASKMANAGER_LIFETIME",
        "enable_recovery_succession_skip_owner_lifetime_for_benchmark",
        "RAY_RECOVERY_SKIP_OWNER_LIFETIME",
    ]
    scan_roots = [root / "src" / "ray", root / "gossip_benchmarks", root / "python" / "ray"]
    known = {str((root / rel).resolve()): files[rel] for rel in TEXT_FILES}
    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for path in scan_root.rglob("*"):
            if not path.is_file() or path.suffix not in {".cc", ".h", ".py", ".proto"}:
                continue
            if any(part in {"results", "__pycache__", ".git"} for part in path.parts):
                continue
            resolved = str(path.resolve())
            text = known.get(resolved)
            if text is None:
                try:
                    text = path.read_text(errors="replace")
                except OSError:
                    continue
            for token in global_tokens:
                if token in text:
                    leftovers.append(f"{path.relative_to(root)}: {token}")
            if path.suffix == ".py" and "gossip_benchmarks" in path.parts and "ablation_mode=" in text:
                leftovers.append(f"{path.relative_to(root)}: ablation_mode=")

    if leftovers:
        # Deduplicate while retaining stable order.
        leftovers = list(dict.fromkeys(leftovers))
        raise CleanupError(
            "diagnostic tokens remain after staged cleanup:\n  "
            + "\n  ".join(leftovers)
        )

    changed = [rel for rel in TEXT_FILES if files[rel] != originals[rel]]
    print("Cleanup preflight passed.")
    print("Files to change:")
    for rel in changed:
        print(f"  - {rel}")

    if args.check:
        print("\n--check requested; no files were written.")
        return

    for rel in changed:
        (root / rel).write_text(files[rel])

    print("\nDiagnostic cleanup applied successfully.")
    print("Retained: profiling, 4M certificate admission, 4N TaskManager pin,")
    print("witness baseline, and the core correctness-preserving recovery path.")
    print("Rebuild Ray before running tests.")


if __name__ == "__main__":
    try:
        main()
    except CleanupError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
