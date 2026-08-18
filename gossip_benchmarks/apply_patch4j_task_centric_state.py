#!/usr/bin/env python3
'''
Patch 4J: task-centric Recovery Succession state + on-demand owner lineage.

Applies on top of Patch 4I.

Goals
-----
1. First-borrow activation no longer duplicates the producer TaskSpec inside
   RecoverySuccessionManager. TaskManager remains the owner-side source of truth.
2. Ordinary owned/borrowed object metadata is reconstructed from:
       TaskID -> TaskRecoveryState.manifest
   plus a compact per-object (TaskID, return_index) record for borrowed objects.
   The old object_recovery_metadata_ map remains only as a compatibility fallback.
3. Full TaskSpec is fetched from TaskManager only when actually needed:
   - H1 4F/4I piggyback
   - no-piggyback / H2+ holder install
   - owner-side recovery replay
4. Recovery semantics are unchanged: holder commitment, witnesses, rollback,
   tombstones, and provisional-holder rules remain intact.

Files modified
--------------
- src/ray/core_worker/recovery_succession_manager.h
- src/ray/core_worker/recovery_succession_manager.cc
- src/ray/core_worker/core_worker.cc
- gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py

No protobuf changes are required.
'''

from __future__ import annotations

import datetime as _dt
import shutil
import subprocess
import sys
from pathlib import Path

VERSION = "4J-v1-task-centric-state"
print(f"Patch 4J patcher version: {VERSION}")

ROOT = Path.cwd()
FILES = {
    "h": ROOT / "src/ray/core_worker/recovery_succession_manager.h",
    "cc": ROOT / "src/ray/core_worker/recovery_succession_manager.cc",
    "core": ROOT / "src/ray/core_worker/core_worker.cc",
    "bench": ROOT / "gossip_benchmarks/16_patch4g_b1_hotpath_ablation.py",
}


def die(msg: str) -> None:
    raise SystemExit(f"Patch 4J: {msg}")


for key, path in FILES.items():
    if not path.exists():
        die(f"missing required file: {path}")


texts = {k: p.read_text() for k, p in FILES.items()}

required = {
    "cc": [
        "Patch 4I: TaskSpec-level recovery argument sidecar.",
        "ExpandTaskSidecarRecoveryMetadata",
        "recovery_argument_metadata",
    ],
    "h": ["Patch 4H: compact task-argument recovery metadata."],
    "core": ["RecoverySuccessionManager::CarriesRecoveryMetadata"],
    "bench": ['Case("full", "Full4I", True, "full")'],
}
for key, needles in required.items():
    for needle in needles:
        if needle not in texts[key]:
            die(f"{FILES[key]} is not the expected post-4I tree; missing {needle!r}")

if "Patch 4J: task-centric recovery state." in texts["cc"]:
    die("Patch 4J already appears to be applied")


def replace_once(text: str, old: str, new: str, desc: str) -> str:
    count = text.count(old)
    if count != 1:
        die(f"{desc}: expected exactly one anchor, found {count}")
    return text.replace(old, new, 1)


def function_span(text: str, marker: str) -> tuple[int, int]:
    start = text.find(marker)
    if start < 0:
        die(f"missing function marker {marker!r}")
    brace = text.find("{", start)
    if brace < 0:
        die(f"missing opening brace after {marker!r}")
    depth = 0
    i = brace
    while i < len(text):
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                while end < len(text) and text[end] in " \t":
                    end += 1
                if end < len(text) and text[end] == "\n":
                    end += 1
                return start, end
        i += 1
    die(f"unbalanced braces for {marker!r}")


def replace_function(text: str, marker: str, new_func: str) -> str:
    start, end = function_span(text, marker)
    return text[:start] + new_func.rstrip() + "\n" + text[end:]


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
h = texts["h"]
h = replace_once(
    h,
    "/// Patch 4H: compact task-argument recovery metadata.\n",
    "/// Patch 4H: compact task-argument recovery metadata.\n"
    "/// Patch 4I: TaskSpec-level recovery argument sidecar.\n"
    "/// Patch 4J: task-centric recovery state and on-demand owner lineage.\n",
    "header patch comments",
)

h = replace_once(
    h,
    "    uint64_t owner_task_spec_copy_count = 0;\n"
    "    uint64_t owner_task_spec_copy_time_ns = 0;\n",
    "    uint64_t owner_task_spec_copy_count = 0;\n"
    "    uint64_t owner_task_spec_copy_time_ns = 0;\n"
    "\n"
    "    // Patch 4J: owner first-borrow activation deliberately does not retain\n"
    "    // another full TaskSpec in RecoverySuccessionManager.\n"
    "    uint64_t owner_lazy_task_spec_copies_avoided = 0;\n"
    "    uint64_t task_centric_metadata_builds = 0;\n",
    "4J profile counters",
)

h = replace_once(
    h,
    "  rpc::ReportRecoveryCandidateReply::Result PrepareHolderAdmission(\n"
    "      const rpc::ReportRecoveryCandidateRequest &request,\n"
    "      HolderAdmissionPlan *plan,\n"
    "      rpc::RecoveryManifest *latest_manifest);\n",
    "  rpc::ReportRecoveryCandidateReply::Result PrepareHolderAdmission(\n"
    "      const rpc::ReportRecoveryCandidateRequest &request,\n"
    "      const rpc::TaskSpec *owner_task_spec,\n"
    "      HolderAdmissionPlan *plan,\n"
    "      rpc::RecoveryManifest *latest_manifest);\n",
    "PrepareHolderAdmission declaration",
)

h = replace_once(
    h,
    "  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec);\n",
    "  void PopulateTaskArgumentMetadata(\n"
    "      rpc::TaskSpec *task_spec,\n"
    "      const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs = nullptr);\n",
    "PopulateTaskArgumentMetadata declaration",
)

h = replace_once(
    h,
    "  ReplayPreparationResult PrepareTaskReplay(const rpc::RecoverTaskOutputRequest &request,\n"
    "                                            rpc::TaskSpec *task_spec,\n"
    "                                            rpc::RecoveryManifest *latest_manifest);\n",
    "  ReplayPreparationResult PrepareTaskReplay(\n"
    "      const rpc::RecoverTaskOutputRequest &request,\n"
    "      const rpc::TaskSpec *owner_task_spec,\n"
    "      rpc::TaskSpec *task_spec,\n"
    "      rpc::RecoveryManifest *latest_manifest);\n",
    "PrepareTaskReplay declaration",
)

h = replace_once(
    h,
    "  void EraseTaskObjectMetadataLocked(const TaskID &task_id)\n"
    "      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);\n",
    "  void EraseTaskObjectMetadataLocked(const TaskID &task_id)\n"
    "      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);\n"
    "\n"
    "  // Patch 4J: reconstruct ordinary RecoveryObjectMetadata from task-level\n"
    "  // state. object_recovery_metadata_ is only a legacy compatibility fallback.\n"
    "  bool BuildRecoveryMetadataLocked(\n"
    "      const ObjectID &object_id,\n"
    "      rpc::RecoveryObjectMetadata *metadata) const\n"
    "      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);\n",
    "BuildRecoveryMetadataLocked declaration",
)

h = replace_once(
    h,
    "    rpc::RecoveryManifest manifest;\n\n"
    "    // Present on the owner, executor, and installed lineage holders.\n",
    "    rpc::RecoveryManifest manifest;\n"
    "\n"
    "    // Patch 4J: static return count lets the owner reconstruct per-object\n"
    "    // metadata from ObjectID::ObjectIndex() without a per-return manifest copy.\n"
    "    uint32_t owned_num_returns = 0;\n"
    "\n"
    "    // Present on executors and installed/piggyback lineage holders. The\n"
    "    // original owner may leave this empty and use TaskManager on demand.\n",
    "TaskRecoveryState owned_num_returns",
)

texts["h"] = h


# ---------------------------------------------------------------------------
# recovery_succession_manager.cc
# ---------------------------------------------------------------------------
cc = texts["cc"]
cc = replace_once(
    cc,
    "// Patch 4I: TaskSpec-level recovery argument sidecar.\n",
    "// Patch 4I: TaskSpec-level recovery argument sidecar.\n"
    "// Patch 4J: task-centric recovery state.\n",
    "cc patch comment",
)

register_owned = r'''
void RecoverySuccessionManager::RegisterOwnedTask(
    const TaskSpecification &task_spec,
    std::vector<rpc::ObjectReference> *returned_refs) {
  if (returned_refs == nullptr) {
    return;
  }

  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (!task_proto.has_recovery_manifest() || task_proto.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(task_proto.recovery_manifest());
  task_state.owned_num_returns =
      static_cast<uint32_t>(task_spec.NumReturns());

  rpc::TaskSpec stored_task_spec;
  stored_task_spec.CopyFrom(task_proto);
  ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(
      task_proto.recovery_manifest());

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = true;

  absl::MutexLock lock(&mutex_);
  task_states_[task_id] = std::move(task_state);

  for (size_t return_index = 0; return_index < returned_refs->size(); ++return_index) {
    rpc::ObjectReference &returned_ref = returned_refs->at(return_index);
    if (returned_ref.object_id().empty()) {
      continue;
    }

    rpc::RecoveryObjectMetadata metadata;
    metadata.set_task_id(task_proto.task_id());
    metadata.set_return_index(static_cast<uint32_t>(return_index));
    metadata.mutable_manifest()->CopyFrom(task_proto.recovery_manifest());
    returned_ref.mutable_recovery_metadata()->CopyFrom(metadata);
  }
}
'''
cc = replace_function(
    cc, "void RecoverySuccessionManager::RegisterOwnedTask(", register_owned
)

register_lazy = r'''
bool RecoverySuccessionManager::RegisterOwnedTaskLazy(
    const TaskSpecification &task_spec,
    const rpc::RecoveryManifest &manifest) {
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (task_proto.task_id().empty() || manifest.task_id().empty() ||
      task_proto.task_id() != manifest.task_id()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());

  absl::MutexLock lock(&mutex_);

  const auto existing_it = task_states_.find(task_id);
  if (existing_it != task_states_.end()) {
    if (existing_it->second.manifest.tombstoned()) {
      return false;
    }
    return false;
  }

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(manifest);
  task_state.owned_num_returns =
      static_cast<uint32_t>(task_spec.NumReturns());
  task_state.manifest_committed = true;

  task_states_[task_id] = std::move(task_state);

  if (profiling_enabled_) {
    ++profile_.owner_lazy_task_spec_copies_avoided;
  }

  return true;
}
'''
cc = replace_function(
    cc, "bool RecoverySuccessionManager::RegisterOwnedTaskLazy(", register_lazy
)

old_block = r'''    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = metadata_task_id;
    borrowed_state.return_index = effective_metadata.return_index();

    borrowed_objects_[object_id] = std::move(borrowed_state);
    object_recovery_metadata_[object_id] = effective_metadata;
    task_object_ids_[metadata_task_id].insert(object_id);

    if (valid_piggyback &&
        piggyback_task_spec.task_id() == effective_metadata.task_id()) {
      const auto existing_task_it = task_states_.find(metadata_task_id);

      if (existing_task_it == task_states_.end()) {
        TaskRecoveryState piggyback_state;
        piggyback_state.manifest.CopyFrom(effective_metadata.manifest());
        piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(
            effective_metadata.manifest());
        piggyback_state.task_spec = std::move(piggyback_task_spec);

        // Critical 4F invariant: possession is provisional only. The local
        // owner-only manifest does not yet make this worker a replayable H1.
        piggyback_state.manifest_committed = false;
        piggyback_state.provisional_piggyback_task_spec = true;

        task_states_[metadata_task_id] = std::move(piggyback_state);
        piggyback_task_ids.insert(metadata_task_id);
      } else if (existing_task_it->second.provisional_piggyback_task_spec &&
                 existing_task_it->second.task_spec.has_value()) {
        // Duplicate delivery of the same downstream TaskSpec.
        piggyback_task_ids.insert(metadata_task_id);
      }
      // Any other pre-existing TaskRecoveryState falls back conservatively to
      // the normal InstallRecoveryHolder path.
    }
'''
new_block = r'''    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = metadata_task_id;
    borrowed_state.return_index = effective_metadata.return_index();
    borrowed_objects_[object_id] = std::move(borrowed_state);

    TaskRecoveryState &dependency_state = task_states_[metadata_task_id];
    if (dependency_state.manifest.task_id().empty() ||
        CompareManifestVersions(effective_metadata.manifest(),
                                dependency_state.manifest) > 0) {
      dependency_state.manifest.CopyFrom(effective_metadata.manifest());
    }

    if (valid_piggyback &&
        piggyback_task_spec.task_id() == effective_metadata.task_id()) {
      if (!dependency_state.task_spec.has_value()) {
        piggyback_task_spec.mutable_recovery_manifest()->CopyFrom(
            dependency_state.manifest);
        dependency_state.task_spec = std::move(piggyback_task_spec);
        dependency_state.manifest_committed = false;
        dependency_state.provisional_piggyback_task_spec = true;
        piggyback_task_ids.insert(metadata_task_id);
      } else if (dependency_state.provisional_piggyback_task_spec) {
        piggyback_task_ids.insert(metadata_task_id);
      }
    }
'''
cc = replace_once(cc, old_block, new_block, "RegisterExecutorTask task-centric block")

register_borrowed = r'''
void RecoverySuccessionManager::RegisterBorrowedObject(
    const ObjectID &object_id, const rpc::RecoveryObjectMetadata &metadata) {
  if (metadata.task_id().empty() || !metadata.has_manifest()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(metadata.task_id());

  absl::MutexLock lock(&mutex_);

  const auto tombstone_it = task_states_.find(task_id);
  if (tombstone_it != task_states_.end() &&
      tombstone_it->second.manifest.tombstoned() &&
      CompareManifestVersions(tombstone_it->second.manifest,
                              metadata.manifest()) >= 0) {
    return;
  }

  BorrowedObjectRecoveryState borrowed_state;
  borrowed_state.task_id = task_id;
  borrowed_state.return_index = metadata.return_index();
  borrowed_objects_[object_id] = std::move(borrowed_state);

  TaskRecoveryState &task_state = task_states_[task_id];
  if (task_state.manifest.task_id().empty() ||
      CompareManifestVersions(metadata.manifest(), task_state.manifest) > 0) {
    task_state.manifest.CopyFrom(metadata.manifest());
  }
}
'''
cc = replace_function(
    cc, "void RecoverySuccessionManager::RegisterBorrowedObject(", register_borrowed
)

builder_func = r'''
bool RecoverySuccessionManager::BuildRecoveryMetadataLocked(
    const ObjectID &object_id,
    rpc::RecoveryObjectMetadata *metadata) const {
  if (object_id.IsNil()) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();
  uint32_t return_index = 0;
  bool known_object = false;

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it != borrowed_objects_.end() &&
      borrowed_it->second.task_id == task_id) {
    return_index = borrowed_it->second.return_index;
    known_object = true;
  }

  const auto task_it = task_states_.find(task_id);
  if (!known_object && task_it != task_states_.end() &&
      task_it->second.owned_num_returns > 0) {
    const auto object_index = object_id.ObjectIndex();
    if (object_index > 0 &&
        static_cast<uint64_t>(object_index) <=
            static_cast<uint64_t>(task_it->second.owned_num_returns)) {
      return_index = static_cast<uint32_t>(object_index - 1);
      known_object = true;
    }
  }

  if (known_object && task_it != task_states_.end() &&
      !task_it->second.manifest.task_id().empty()) {
    if (metadata != nullptr) {
      metadata->Clear();
      metadata->set_task_id(task_id.Binary());
      metadata->set_return_index(return_index);
      metadata->mutable_manifest()->CopyFrom(task_it->second.manifest);
    }
    if (profiling_enabled_) {
      ++profile_.task_centric_metadata_builds;
    }
    return true;
  }

  const auto legacy_it = object_recovery_metadata_.find(object_id);
  if (legacy_it == object_recovery_metadata_.end()) {
    return false;
  }
  if (metadata != nullptr) {
    metadata->CopyFrom(legacy_it->second);
  }
  return true;
}

'''
pos = cc.find("bool RecoverySuccessionManager::HasRecoveryMetadata(")
if pos < 0:
    die("missing HasRecoveryMetadata insertion point")
cc = cc[:pos] + builder_func + cc[pos:]

has_metadata = r'''
bool RecoverySuccessionManager::HasRecoveryMetadata(
    const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  return BuildRecoveryMetadataLocked(object_id, nullptr);
}
'''
cc = replace_function(
    cc, "bool RecoverySuccessionManager::HasRecoveryMetadata(", has_metadata
)

populate_metadata = r'''
bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  const auto patch4g_start = std::chrono::steady_clock::now();
  absl::MutexLock lock(&mutex_);

  const bool hit = BuildRecoveryMetadataLocked(object_id, metadata);

  if (profiling_enabled_) {
    ++profile_.recovery_metadata_lookup_calls;
    if (hit) {
      ++profile_.recovery_metadata_lookup_hits;
    }
    profile_.recovery_metadata_lookup_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
  }

  return hit;
}
'''
cc = replace_function(
    cc, "bool RecoverySuccessionManager::PopulateRecoveryMetadata(", populate_metadata
)

cc = replace_once(
    cc,
    "void RecoverySuccessionManager::PopulateTaskArgumentMetadata(\n"
    "    rpc::TaskSpec *task_spec) {",
    "void RecoverySuccessionManager::PopulateTaskArgumentMetadata(\n"
    "    rpc::TaskSpec *task_spec,\n"
    "    const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs) {",
    "PopulateTaskArgumentMetadata definition signature",
)

old_source = r'''    rpc::RecoveryObjectMetadata legacy_expanded;
    const rpc::RecoveryObjectMetadata *source = nullptr;

    const auto metadata_it = object_recovery_metadata_.find(object_id);
    if (metadata_it != object_recovery_metadata_.end()) {
      source = &metadata_it->second;
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
'''
new_source = r'''    rpc::RecoveryObjectMetadata source_storage;
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
'''
cc = replace_once(cc, old_source, new_source, "PopulateTaskArgumentMetadata source lookup")

old_lineage = r'''    // Claim exactly one full-lineage piggyback while the committed succession
    // is still [A]. Later holders use the ordinary Patch-4E install path.
    if (state.first_holder_piggyback_sent ||
        !state.manifest_committed ||
        !state.task_spec.has_value() ||
        state.manifest.tombstoned() ||
        state.manifest.frozen() ||
        state.manifest.succession_size() != 1 ||
        state.manifest.task_id() != source->task_id()) {
      return;
    }

    const rpc::RecoveryHolder *owner = FindHolderByRank(state.manifest, 0);
    if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
      return;
    }

    if (!IsEligibleTask(state.task_spec.value()) ||
        !state.task_spec->has_recovery_manifest() ||
        state.task_spec->task_id() != state.manifest.task_id()) {
      return;
    }
'''
new_lineage = r'''    // Claim exactly one full-lineage piggyback while the committed succession
    // is still [A]. Later holders use the ordinary Patch-4E install path.
    if (state.first_holder_piggyback_sent ||
        !state.manifest_committed ||
        state.manifest.tombstoned() ||
        state.manifest.frozen() ||
        state.manifest.succession_size() != 1 ||
        state.manifest.task_id() != source->task_id()) {
      return;
    }

    const rpc::RecoveryHolder *owner = FindHolderByRank(state.manifest, 0);
    if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
      return;
    }

    const rpc::TaskSpec *lineage_task_spec = nullptr;
    if (state.task_spec.has_value()) {
      lineage_task_spec = &state.task_spec.value();
    } else if (owner_task_specs != nullptr) {
      const auto lineage_it = owner_task_specs->find(producer_task_id);
      if (lineage_it != owner_task_specs->end()) {
        lineage_task_spec = &lineage_it->second;
      }
    }

    if (lineage_task_spec == nullptr ||
        !IsEligibleTask(*lineage_task_spec) ||
        lineage_task_spec->task_id() != state.manifest.task_id()) {
      return;
    }
'''
cc = replace_once(cc, old_lineage, new_lineage, "4J on-demand piggyback lineage")

cc = replace_once(
    cc,
    "    const bool ok = state.task_spec->SerializeToString(&serialized_task_spec);\n",
    "    rpc::TaskSpec piggyback_lineage;\n"
    "    piggyback_lineage.CopyFrom(*lineage_task_spec);\n"
    "    ClearFirstHolderTaskSpecPiggybacks(&piggyback_lineage);\n"
    "    piggyback_lineage.mutable_recovery_manifest()->CopyFrom(state.manifest);\n"
    "    const bool ok = piggyback_lineage.SerializeToString(&serialized_task_spec);\n",
    "piggyback serialization source",
)

cc = replace_once(
    cc,
    "RecoverySuccessionManager::PrepareHolderAdmission(\n"
    "    const rpc::ReportRecoveryCandidateRequest &request,\n"
    "    HolderAdmissionPlan *plan,\n"
    "    rpc::RecoveryManifest *latest_manifest) {",
    "RecoverySuccessionManager::PrepareHolderAdmission(\n"
    "    const rpc::ReportRecoveryCandidateRequest &request,\n"
    "    const rpc::TaskSpec *owner_task_spec,\n"
    "    HolderAdmissionPlan *plan,\n"
    "    rpc::RecoveryManifest *latest_manifest) {",
    "PrepareHolderAdmission definition signature",
)

cc = replace_once(
    cc,
    "  const auto task_it = task_states_.find(task_id);\n"
    "  if (task_it == task_states_.end() || !task_it->second.task_spec.has_value()) {\n"
    "    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;\n"
    "  }\n",
    "  const auto task_it = task_states_.find(task_id);\n"
    "  if (task_it == task_states_.end()) {\n"
    "    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;\n"
    "  }\n",
    "PrepareHolderAdmission owner state check",
)

old_copy = r'''  if (!plan->candidate_already_stores_task_spec) {
    if (profiling_enabled_) {
      const auto copy_start = std::chrono::steady_clock::now();

      plan->task_spec.CopyFrom(task_it->second.task_spec.value());
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);

      const auto copy_end = std::chrono::steady_clock::now();
      const uint64_t copy_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(copy_end - copy_start)
              .count());

      ++profile_.owner_task_spec_copy_count;
      profile_.owner_task_spec_copy_time_ns += copy_ns;
    } else {
      plan->task_spec.CopyFrom(task_it->second.task_spec.value());
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);
    }
  }
'''
new_copy = r'''  if (!plan->candidate_already_stores_task_spec) {
    const rpc::TaskSpec *lineage_task_spec =
        task_it->second.task_spec.has_value()
            ? &task_it->second.task_spec.value()
            : owner_task_spec;

    if (lineage_task_spec == nullptr ||
        lineage_task_spec->task_id() != task_id.Binary() ||
        !IsEligibleTask(*lineage_task_spec)) {
      EraseHolderReservationLocked(reservation_id);
      return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
    }

    if (profiling_enabled_) {
      const auto copy_start = std::chrono::steady_clock::now();

      plan->task_spec.CopyFrom(*lineage_task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&plan->task_spec);
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);

      const auto copy_end = std::chrono::steady_clock::now();
      const uint64_t copy_ns = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(copy_end - copy_start)
              .count());

      ++profile_.owner_task_spec_copy_count;
      profile_.owner_task_spec_copy_time_ns += copy_ns;
    } else {
      plan->task_spec.CopyFrom(*lineage_task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&plan->task_spec);
      plan->task_spec.mutable_recovery_manifest()->CopyFrom(proposed_manifest);
    }
  }
'''
cc = replace_once(cc, old_copy, new_copy, "PrepareHolderAdmission lineage copy")

get_plan = r'''
bool RecoverySuccessionManager::GetBorrowedObjectRecoveryPlan(
    const ObjectID &object_id, BorrowedObjectRecoveryPlan *plan) const {
  if (plan == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it == borrowed_objects_.end()) {
    return false;
  }

  const auto task_it = task_states_.find(borrowed_it->second.task_id);
  if (task_it == task_states_.end() ||
      task_it->second.manifest.task_id().empty()) {
    return false;
  }

  plan->task_id = borrowed_it->second.task_id;
  plan->return_index = borrowed_it->second.return_index;
  plan->cached_manifest.CopyFrom(task_it->second.manifest);
  return true;
}
'''
cc = replace_function(
    cc, "bool RecoverySuccessionManager::GetBorrowedObjectRecoveryPlan(", get_plan
)

cc = replace_once(
    cc,
    "RecoverySuccessionManager::PrepareTaskReplay(const rpc::RecoverTaskOutputRequest &request,\n"
    "                                             rpc::TaskSpec *task_spec,\n"
    "                                             rpc::RecoveryManifest *latest_manifest) {",
    "RecoverySuccessionManager::PrepareTaskReplay(\n"
    "    const rpc::RecoverTaskOutputRequest &request,\n"
    "    const rpc::TaskSpec *owner_task_spec,\n"
    "    rpc::TaskSpec *task_spec,\n"
    "    rpc::RecoveryManifest *latest_manifest) {",
    "PrepareTaskReplay definition signature",
)

cc = replace_once(
    cc,
    "  if (!state.task_spec.has_value()) {\n"
    "    return ReplayPreparationResult::TASK_NOT_FOUND;\n"
    "  }\n",
    "  const rpc::TaskSpec *lineage_task_spec =\n"
    "      state.task_spec.has_value() ? &state.task_spec.value() : owner_task_spec;\n"
    "  if (lineage_task_spec == nullptr ||\n"
    "      lineage_task_spec->task_id() != task_id.Binary() ||\n"
    "      !IsEligibleTask(*lineage_task_spec)) {\n"
    "    return ReplayPreparationResult::TASK_NOT_FOUND;\n"
    "  }\n",
    "PrepareTaskReplay lineage availability",
)

cc = replace_once(
    cc,
    "  state.task_spec->mutable_recovery_manifest()->CopyFrom(state.manifest);\n"
    "\n"
    "  task_spec->CopyFrom(state.task_spec.value());\n"
    "  latest_manifest->CopyFrom(state.manifest);\n",
    "  task_spec->CopyFrom(*lineage_task_spec);\n"
    "  ClearFirstHolderTaskSpecPiggybacks(task_spec);\n"
    "  task_spec->mutable_recovery_manifest()->CopyFrom(state.manifest);\n"
    "  latest_manifest->CopyFrom(state.manifest);\n",
    "PrepareTaskReplay output copy",
)

update_borrowed = r'''
void RecoverySuccessionManager::UpdateBorrowedObjectManifest(
    const ObjectID &object_id,
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto borrowed_it = borrowed_objects_.find(object_id);
  if (borrowed_it == borrowed_objects_.end() ||
      borrowed_it->second.task_id.Binary() != manifest.task_id()) {
    return;
  }

  TaskRecoveryState &state = task_states_[borrowed_it->second.task_id];
  if (!state.manifest.task_id().empty() &&
      CompareManifestVersions(manifest, state.manifest) < 0) {
    return;
  }

  state.manifest.CopyFrom(manifest);
}
'''
cc = replace_function(
    cc, "void RecoverySuccessionManager::UpdateBorrowedObjectManifest(", update_borrowed
)

cc = replace_once(
    cc,
    "  if (!task_state.manifest_committed || task_state.manifest.tombstoned() ||\n"
    "      !task_state.task_spec.has_value()) {\n"
    "    return std::nullopt;\n"
    "  }\n",
    "  if (!task_state.manifest_committed || task_state.manifest.tombstoned()) {\n"
    "    return std::nullopt;\n"
    "  }\n",
    "BuildTombstoneForTask owner task spec condition",
)

erase_meta = r'''
void RecoverySuccessionManager::EraseTaskObjectMetadataLocked(
    const TaskID &task_id) {
  const auto object_ids_it = task_object_ids_.find(task_id);
  if (object_ids_it != task_object_ids_.end()) {
    for (const ObjectID &object_id : object_ids_it->second) {
      object_recovery_metadata_.erase(object_id);
      borrowed_objects_.erase(object_id);
    }
    task_object_ids_.erase(object_ids_it);
  }

  for (auto it = borrowed_objects_.begin(); it != borrowed_objects_.end();) {
    if (it->second.task_id == task_id) {
      object_recovery_metadata_.erase(it->first);
      it = borrowed_objects_.erase(it);
    } else {
      ++it;
    }
  }
}
'''
cc = replace_function(
    cc, "void RecoverySuccessionManager::EraseTaskObjectMetadataLocked(", erase_meta
)

texts["cc"] = cc


# ---------------------------------------------------------------------------
# core_worker.cc
# ---------------------------------------------------------------------------
core = texts["core"]

old_populate_block = r'''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !args.empty()) {
    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());

    if (recovery_succession_profiling_enabled_) {
      const uint64_t start_ns = RecoveryProfileNowNs();

      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage());

      recovery_succession_manager_
          ->RecordTaskArgumentMetadataLatency(
              RecoveryProfileNowNs() - start_ns);
    } else {
      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage());
    }
  }
'''
new_populate_block = r'''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !args.empty()) {
    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());

    absl::flat_hash_map<TaskID, rpc::TaskSpec> owner_recovery_task_specs;
    const std::string &recovery_mode = RecoveryBenchmarkAblationMode();
    if (!recovery_witness_holder_baseline_enabled_ &&
        (recovery_mode == "full" ||
         recovery_mode == "piggyback_no_candidate")) {
      absl::flat_hash_set<TaskID> seen_producers;

      auto maybe_prefetch = [this, &owner_recovery_task_specs, &seen_producers](
                                const rpc::ObjectReference &ref) {
        if (ref.object_id().size() != ObjectID::Size()) {
          return;
        }
        const TaskID producer_task_id =
            ObjectID::FromBinary(ref.object_id()).TaskId();
        if (!seen_producers.insert(producer_task_id).second) {
          return;
        }

        auto producer_spec = task_manager_->GetTaskSpec(producer_task_id);
        if (!producer_spec.has_value()) {
          return;
        }
        const rpc::TaskSpec &proto = producer_spec->GetMessage();
        if (!RecoverySuccessionManager::IsEligibleTask(proto)) {
          return;
        }
        owner_recovery_task_specs[producer_task_id].CopyFrom(proto);
      };

      for (const rpc::TaskArg &task_arg : builder.MutableMessage()->args()) {
        if (task_arg.has_object_ref()) {
          maybe_prefetch(task_arg.object_ref());
        }
        for (const rpc::ObjectReference &nested_ref :
             task_arg.nested_inlined_refs()) {
          maybe_prefetch(nested_ref);
        }
      }
    }

    if (recovery_succession_profiling_enabled_) {
      const uint64_t start_ns = RecoveryProfileNowNs();

      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage(), &owner_recovery_task_specs);

      recovery_succession_manager_
          ->RecordTaskArgumentMetadataLatency(
              RecoveryProfileNowNs() - start_ns);
    } else {
      recovery_succession_manager_->PopulateTaskArgumentMetadata(
          builder.MutableMessage(), &owner_recovery_task_specs);
    }
  }
'''
core = replace_once(core, old_populate_block, new_populate_block, "BuildCommonTaskSpec 4J block")

old_candidate_call = r'''  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
  rpc::RecoveryManifest latest_manifest;

  const auto result = manager->PrepareHolderAdmission(
      request, &admission_plan, &latest_manifest);
'''
new_candidate_call = r'''  RecoverySuccessionManager::HolderAdmissionPlan admission_plan;
  rpc::RecoveryManifest latest_manifest;

  std::optional<TaskSpecification> owner_task_spec;
  const rpc::TaskSpec *owner_task_proto = nullptr;
  if (!request.already_stores_task_spec() &&
      request.task_id().size() == TaskID::Size()) {
    owner_task_spec =
        task_manager_->GetTaskSpec(TaskID::FromBinary(request.task_id()));
    if (owner_task_spec.has_value()) {
      owner_task_proto = &owner_task_spec->GetMessage();
    }
  }

  const auto result = manager->PrepareHolderAdmission(
      request, owner_task_proto, &admission_plan, &latest_manifest);
'''
core = replace_once(core, old_candidate_call, new_candidate_call, "candidate on-demand lineage")

old_replay_call = r'''  const auto preparation = recovery_succession_manager_->PrepareTaskReplay(
      request, &replay_task_proto, &latest_manifest);
'''
new_replay_call = r'''  std::optional<TaskSpecification> owner_replay_task_spec;
  const rpc::TaskSpec *owner_replay_task_proto = nullptr;
  if (request.task_id().size() == TaskID::Size()) {
    owner_replay_task_spec =
        task_manager_->GetTaskSpec(TaskID::FromBinary(request.task_id()));
    if (owner_replay_task_spec.has_value()) {
      owner_replay_task_proto = &owner_replay_task_spec->GetMessage();
    }
  }

  const auto preparation = recovery_succession_manager_->PrepareTaskReplay(
      request,
      owner_replay_task_proto,
      &replay_task_proto,
      &latest_manifest);
'''
core = replace_once(core, old_replay_call, new_replay_call, "replay on-demand lineage")

profile_anchor = '''  result["owner_task_spec_copy_time_ns"] =
      profile.owner_task_spec_copy_time_ns;
'''
profile_new = profile_anchor + '''  result["owner_lazy_task_spec_copies_avoided"] =
      profile.owner_lazy_task_spec_copies_avoided;
  result["task_centric_metadata_builds"] =
      profile.task_centric_metadata_builds;
'''
core = replace_once(core, profile_anchor, profile_new, "4J JSON profile export")

texts["core"] = core


# ---------------------------------------------------------------------------
# Benchmark 16
# ---------------------------------------------------------------------------
bench = texts["bench"]
bench = bench.replace("Full4I", "Full4J")
bench = replace_once(
    bench,
    '    "owner_task_spec_copy_time_ns",\n',
    '    "owner_task_spec_copy_time_ns",\n'
    '    "owner_lazy_task_spec_copies_avoided",\n'
    '    "task_centric_metadata_builds",\n',
    "benchmark PROFILE_KEYS 4J",
)
texts["bench"] = bench


stamp = _dt.datetime.now().strftime("%Y%m%d-%H%M%S")
backup_root = ROOT / ".patch4j_backups" / stamp
for key, path in FILES.items():
    dest = backup_root / path.relative_to(ROOT)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dest)

for key, path in FILES.items():
    path.write_text(texts[key])

subprocess.run(
    [sys.executable, "-m", "py_compile", str(FILES["bench"])],
    check=True,
)
subprocess.run(["git", "diff", "--check"], check=True)

print("Patch 4J applied successfully.")
print(f"Backups: {backup_root}")
print("Modified:")
for path in FILES.values():
    print(f"  {path.relative_to(ROOT)}")
print()
print("Next:")
print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")
print()
print("Then run Benchmark 16 with 2 repetitions.")
