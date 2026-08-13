#!/usr/bin/env python3
'''Apply Patch 4C: lazy Recovery Succession activation.

Goal:
- zero-borrower tasks do not eagerly initialize Recovery Succession;
- first real export/borrow lazily initializes owner recovery state;
- executor-side recovery registration is skipped for tasks carrying no recovery state;
- witness-as-holder baseline keeps its existing eager behavior.

Run from the Ray repository root:

    python gossip_benchmarks/apply_patch4c_lazy_activation.py --check
    python gossip_benchmarks/apply_patch4c_lazy_activation.py

Then run:
    git diff --check
    # rebuild using the same command used for Patch 4B-3
'''

from __future__ import annotations

import argparse
import sys
from pathlib import Path


FILES = {
    "core_cc": Path("src/ray/core_worker/core_worker.cc"),
    "core_h": Path("src/ray/core_worker/core_worker.h"),
    "mgr_cc": Path("src/ray/core_worker/recovery_succession_manager.cc"),
    "mgr_h": Path("src/ray/core_worker/recovery_succession_manager.h"),
}


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(
            f"{label}: expected exactly one source match, found {count}. "
            "Refusing to edit."
        )
    return text.replace(old, new, 1)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    root = args.repo.resolve()
    paths = {name: root / rel for name, rel in FILES.items()}
    for path in paths.values():
        if not path.is_file():
            raise RuntimeError(f"Missing source file: {path}")

    texts = {name: path.read_text() for name, path in paths.items()}
    combined_original = "\n".join(texts.values())

    markers = [
        "CarriesRecoveryMetadata",
        "RegisterOwnedTaskLazy",
        "TryPopulateRecoveryMetadataForObject",
        "EnsureRecoverySuccessionForTaskArguments",
    ]
    if all(marker in combined_original for marker in markers):
        print("Patch 4C lazy activation already appears to be applied.")
        return 0

    # ------------------------------------------------------------------
    # RecoverySuccessionManager header.
    # ------------------------------------------------------------------
    mgr_h = texts["mgr_h"]

    mgr_h = replace_once(
        mgr_h,
        '''  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
''',
        '''  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Returns true only when a task actually carries Recovery Succession
  /// state: either its own recovery manifest or recovery metadata on one of
  /// its ObjectRef arguments.
  static bool CarriesRecoveryMetadata(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
''',
        "manager header: CarriesRecoveryMetadata declaration",
    )

    mgr_h = replace_once(
        mgr_h,
        '''  /// Records a newly submitted task and attaches metadata to its return refs.
  void RegisterOwnedTask(const TaskSpecification &task_spec,
                         std::vector<rpc::ObjectReference> *returned_refs);

  /// Records a received TaskSpec and returns candidate reports that should be
''',
        '''  /// Records a newly submitted task and attaches metadata to its return refs.
  /// This eager entry point is retained for the witness-as-holder baseline.
  void RegisterOwnedTask(const TaskSpecification &task_spec,
                         std::vector<rpc::ObjectReference> *returned_refs);

  /// Lazily installs owner recovery state after a task return is actually
  /// exported/borrowed. The TaskSpec does not need to already carry a
  /// recovery_manifest; this stores a private replayable copy with the
  /// supplied manifest attached and creates metadata for all static returns.
  /// Returns true only if this call performed the initialization.
  bool RegisterOwnedTaskLazy(const TaskSpecification &task_spec,
                             const rpc::RecoveryManifest &manifest);

  /// Records a received TaskSpec and returns candidate reports that should be
''',
        "manager header: RegisterOwnedTaskLazy declaration",
    )

    # ------------------------------------------------------------------
    # RecoverySuccessionManager implementation.
    # ------------------------------------------------------------------
    mgr_cc = texts["mgr_cc"]

    mgr_cc = replace_once(
        mgr_cc,
        '''bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
}

rpc::RecoveryManifest RecoverySuccessionManager::BuildInitialManifest(
''',
        '''bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
}

bool RecoverySuccessionManager::CarriesRecoveryMetadata(
    const rpc::TaskSpec &task_spec) {
  if (task_spec.has_recovery_manifest()) {
    return true;
  }

  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      return true;
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        return true;
      }
    }
  }

  return false;
}

rpc::RecoveryManifest RecoverySuccessionManager::BuildInitialManifest(
''',
        "manager cc: CarriesRecoveryMetadata implementation",
    )

    lazy_impl = r'''
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

    // Another serialization thread already activated this task.
    if (existing_it->second.task_spec.has_value()) {
      return false;
    }

    // Avoid overwriting any unexpected partially-created state.
    return false;
  }

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(manifest);

  rpc::TaskSpec stored_task_spec;
  stored_task_spec.CopyFrom(task_proto);
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(manifest);

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = true;

  task_states_[task_id] = std::move(task_state);

  // Static return IDs are deterministic. Initialize metadata for every return
  // so the first exported return activates protection for the whole task.
  for (size_t return_index = 0; return_index < task_spec.NumReturns();
       ++return_index) {
    const ObjectID object_id = task_spec.ReturnId(return_index);

    if (object_id.IsNil()) {
      continue;
    }

    rpc::RecoveryObjectMetadata metadata;
    metadata.set_task_id(task_proto.task_id());
    metadata.set_return_index(static_cast<uint32_t>(return_index));
    metadata.mutable_manifest()->CopyFrom(manifest);

    object_recovery_metadata_[object_id] = metadata;
    task_object_ids_[task_id].insert(object_id);
  }

  return true;
}

'''

    reg_exec_marker = '''std::vector<RecoverySuccessionManager::CandidateReport>
RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
'''
    if mgr_cc.count(reg_exec_marker) != 1:
        raise RuntimeError(
            "manager cc: could not uniquely locate RegisterExecutorTask insertion point"
        )
    mgr_cc = mgr_cc.replace(reg_exec_marker, lazy_impl + reg_exec_marker, 1)

    # ------------------------------------------------------------------
    # CoreWorker header helpers.
    # ------------------------------------------------------------------
    core_h = texts["core_h"]

    core_h = replace_once(
        core_h,
        '''  /// Adds selected witnesses to a newly created owner manifest.
  void PopulateRecoveryWitnesses(rpc::RecoveryManifest *manifest) const;

  using RecoveryWitnessPublishCallback =
''',
        '''  /// Adds selected witnesses to a newly created owner manifest.
  void PopulateRecoveryWitnesses(rpc::RecoveryManifest *manifest) const;

  /// Populates metadata if already active; otherwise lazily initializes
  /// Recovery Succession for an eligible task return owned by this CoreWorker.
  bool TryPopulateRecoveryMetadataForObject(
      const ObjectID &object_id,
      rpc::RecoveryObjectMetadata *metadata) const;

  /// Lazily activate eligible owned returns that are actually being exported
  /// as arguments of another task.
  void EnsureRecoverySuccessionForTaskArguments(
      rpc::TaskSpec *task_spec) const;

  using RecoveryWitnessPublishCallback =
''',
        "core header: lazy helper declarations",
    )

    # ------------------------------------------------------------------
    # CoreWorker implementation helpers.
    # ------------------------------------------------------------------
    core_cc = texts["core_cc"]

    helper_impl = r'''
bool CoreWorker::TryPopulateRecoveryMetadataForObject(
    const ObjectID &object_id,
    rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr || !recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr) {
    return false;
  }

  // Fast path after the first export/borrow.
  if (recovery_succession_manager_->PopulateRecoveryMetadata(
          object_id, metadata)) {
    return true;
  }

  // Preserve witness-as-holder baseline semantics exactly.
  if (recovery_witness_holder_baseline_enabled_) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();
  auto task_spec_opt = task_manager_->GetTaskSpec(task_id);
  if (!task_spec_opt.has_value()) {
    return false;
  }

  const TaskSpecification &task_spec = task_spec_opt.value();
  const rpc::TaskSpec &task_proto = task_spec.GetMessage();

  if (!RecoverySuccessionManager::IsEligibleTask(task_proto) ||
      task_proto.task_id().empty()) {
    return false;
  }

  // Protect only static task returns, never ray.put() objects or actor handles.
  bool is_static_return = false;
  for (size_t return_index = 0; return_index < task_spec.NumReturns();
       ++return_index) {
    if (task_spec.ReturnId(return_index) == object_id) {
      is_static_return = true;
      break;
    }
  }

  if (!is_static_return) {
    return false;
  }

  uint64_t manifest_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    manifest_start_ns = RecoveryProfileNowNs();
  }

  rpc::RecoveryManifest manifest =
      recovery_succession_manager_->BuildInitialManifest(
          task_id, task_spec.JobId(), task_proto.max_retries());

  if (manifest_start_ns != 0) {
    recovery_succession_manager_->RecordInitialManifestBuild(
        RecoveryProfileNowNs() - manifest_start_ns,
        static_cast<uint64_t>(manifest.ByteSizeLong()));
  }

  uint64_t witness_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    witness_start_ns = RecoveryProfileNowNs();
  }

  PopulateRecoveryWitnesses(&manifest);

  if (witness_start_ns != 0) {
    recovery_succession_manager_->RecordWitnessSelectionLatency(
        RecoveryProfileNowNs() - witness_start_ns);
  }

  uint64_t register_start_ns = 0;
  if (recovery_succession_profiling_enabled_) {
    register_start_ns = RecoveryProfileNowNs();
  }

  const bool initialized_now =
      recovery_succession_manager_->RegisterOwnedTaskLazy(task_spec, manifest);

  if (register_start_ns != 0 && initialized_now) {
    recovery_succession_manager_->RecordRegisterOwnedTaskLatency(
        RecoveryProfileNowNs() - register_start_ns);
  }

  // If another thread won the initialization race, its metadata is visible
  // here once RegisterOwnedTaskLazy returns.
  return recovery_succession_manager_->PopulateRecoveryMetadata(
      object_id, metadata);
}

void CoreWorker::EnsureRecoverySuccessionForTaskArguments(
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr || !recovery_succession_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      recovery_witness_holder_baseline_enabled_) {
    return;
  }

  rpc::RecoveryObjectMetadata ignored_metadata;

  for (const rpc::TaskArg &arg : task_spec->args()) {
    if (arg.has_object_ref() && !arg.object_ref().object_id().empty()) {
      const ObjectID object_id =
          ObjectID::FromBinary(arg.object_ref().object_id());
      ignored_metadata.Clear();
      TryPopulateRecoveryMetadataForObject(object_id, &ignored_metadata);
    }

    for (const rpc::ObjectReference &nested_ref :
         arg.nested_inlined_refs()) {
      if (nested_ref.object_id().empty()) {
        continue;
      }

      const ObjectID nested_id =
          ObjectID::FromBinary(nested_ref.object_id());
      ignored_metadata.Clear();
      TryPopulateRecoveryMetadataForObject(nested_id, &ignored_metadata);
    }
  }
}

'''

    get_refs_marker = '''std::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs(
    const std::vector<ObjectID> &object_ids) const {
'''
    if core_cc.count(get_refs_marker) != 1:
        raise RuntimeError("core cc: could not locate GetObjectRefs insertion point")
    core_cc = core_cc.replace(get_refs_marker, helper_impl + get_refs_marker, 1)

    # Replace the four pre-existing metadata-emission calls with the lazy helper.
    # Protect the two direct calls inside the helper we just inserted.
    helper_fast = '''recovery_succession_manager_->PopulateRecoveryMetadata(
          object_id, metadata)'''
    helper_final = '''recovery_succession_manager_->PopulateRecoveryMetadata(
      object_id, metadata)'''

    if core_cc.count(helper_fast) != 1 or core_cc.count(helper_final) != 1:
        raise RuntimeError(
            "core cc: helper PopulateRecoveryMetadata call shapes changed unexpectedly"
        )

    token_fast = "__PATCH4C_HELPER_FAST__"
    token_final = "__PATCH4C_HELPER_FINAL__"
    core_cc = core_cc.replace(helper_fast, token_fast, 1)
    core_cc = core_cc.replace(helper_final, token_final, 1)

    old_populate = "recovery_succession_manager_->PopulateRecoveryMetadata("
    count_existing = core_cc.count(old_populate)
    if count_existing != 4:
        raise RuntimeError(
            "core cc: expected exactly four pre-existing metadata emission calls, "
            f"found {count_existing}"
        )

    core_cc = core_cc.replace(
        old_populate,
        "TryPopulateRecoveryMetadataForObject(",
    )

    core_cc = core_cc.replace(token_fast, helper_fast, 1)
    core_cc = core_cc.replace(token_final, helper_final, 1)

    # ------------------------------------------------------------------
    # Zero-argument tasks: avoid recovery metadata mutex work entirely.
    # For tasks with args, initialize owned refs before metadata propagation.
    # ------------------------------------------------------------------
    old_arg_block = '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr) {
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

    new_arg_block = '''  if (recovery_succession_enabled_ &&
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

    core_cc = replace_once(
        core_cc,
        old_arg_block,
        new_arg_block,
        "core cc: BuildCommonTaskSpec recovery argument block",
    )

    # ------------------------------------------------------------------
    # Only baseline keeps eager initial manifest creation.
    # ------------------------------------------------------------------
    old_eager_condition = '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      RecoverySuccessionManager::IsEligibleTask(
          builder.GetMessage())) {
'''

    new_eager_condition = '''  if (recovery_succession_enabled_ &&
      recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      RecoverySuccessionManager::IsEligibleTask(
          builder.GetMessage())) {
'''

    core_cc = replace_once(
        core_cc,
        old_eager_condition,
        new_eager_condition,
        "core cc: eager manifest condition",
    )

    # ------------------------------------------------------------------
    # Executor fast path: no recovery work when the incoming task contains
    # neither its own recovery manifest nor recovery-bearing ObjectRefs.
    # ------------------------------------------------------------------
    old_executor = '''  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {
    auto candidate_reports =
        recovery_succession_manager_->RegisterExecutorTask(request.task_spec());
'''

    new_executor = '''  if (recovery_succession_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      RecoverySuccessionManager::CarriesRecoveryMetadata(
          request.task_spec())) {
    auto candidate_reports =
        recovery_succession_manager_->RegisterExecutorTask(request.task_spec());
'''

    core_cc = replace_once(
        core_cc,
        old_executor,
        new_executor,
        "core cc: executor recovery fast path",
    )

    updated = {
        "core_cc": core_cc,
        "core_h": core_h,
        "mgr_cc": mgr_cc,
        "mgr_h": mgr_h,
    }

    combined = "\n".join(updated.values())
    for marker in markers:
        if marker not in combined:
            raise RuntimeError(f"Postcondition failed: missing {marker}")

    if new_eager_condition not in core_cc:
        raise RuntimeError("Postcondition failed: baseline-only eager path missing")

    if args.check:
        print("Patch 4C lazy activation applicability: OK")
        print("Will modify:")
        for rel in FILES.values():
            print(f"  {rel}")
        print()
        print("Expected behavior:")
        print("  - b0 ordinary Succession performs no eager per-task recovery setup")
        print("  - first real export/borrow lazily creates manifest + owner state")
        print("  - tasks carrying no recovery state skip executor recovery registration")
        print("  - witness-holder baseline remains eager")
        print("No files written (--check).")
        return 0

    for name, text in updated.items():
        paths[name].write_text(text)
        print(f"Updated: {paths[name]}")

    print()
    print("Patch 4C applied.")
    print("Next: git diff --check, rebuild, then run B14, B16 sequential_failover, and B17.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
