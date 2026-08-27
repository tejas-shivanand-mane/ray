#!/usr/bin/env python3
from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    if new in text:
        return
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one match, found {count}")
    p.write_text(text.replace(old, new, 1))


# Link the dedicated publication implementation into CoreWorker.
replace_once(
    "src/ray/core_worker/BUILD.bazel",
    '        "core_worker.cc",\n        "core_worker_process.cc",',
    '        "core_worker.cc",\n        "core_worker_process.cc",\n        "recovery_frontier_publication.cc",',
)

# CoreWorker declaration for the backend-neutral fixed-R frontier publisher.
replace_once(
    "src/ray/core_worker/core_worker.h",
    '''  void PublishRecoveryManifestToWitnesses(\n    const rpc::RecoveryManifest &manifest,\n    RecoveryWitnessPublishCallback callback,\n    const rpc::TaskSpec *task_spec = nullptr,\n    const std::string *serialized_task_spec = nullptr) const;\n\n\n  // Patch 4M-CERT delta publication.''',
    '''  void PublishRecoveryManifestToWitnesses(\n    const rpc::RecoveryManifest &manifest,\n    RecoveryWitnessPublishCallback callback,\n    const rpc::TaskSpec *task_spec = nullptr,\n    const std::string *serialized_task_spec = nullptr) const;\n\n  /// Publish every currently uncommitted replay recipe for one Recovery\n  /// Frontier group to its fixed-R holder topology. The owner-side committed\n  /// prefix advances only from the all-holder completion callback.\n  void PublishRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n\n\n  // Patch 4M-CERT delta publication.''',
)

# Manager API/cache: one immutable fixed-R topology per frontier group.
replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''  std::optional<RecoveryFrontierMembership> GetRecoveryFrontierMembership(\n      const TaskID &task_id) const;\n\n  /// Stage/commit/abort the next contiguous group append.''',
    '''  std::optional<RecoveryFrontierMembership> GetRecoveryFrontierMembership(\n      const TaskID &task_id) const;\n\n  /// Return the immutable protection manifest selected for a frontier group.\n  /// The group leader TaskID is the manifest TaskID.\n  bool GetRecoveryFrontierProtectionManifest(\n      const TaskID &group_id, rpc::RecoveryManifest *manifest) const;\n\n  /// Cache the first protection manifest selected for a group and return the\n  /// authoritative cached value. Concurrent first activations therefore cannot\n  /// split later members across different fixed-R holder sets.\n  bool CacheRecoveryFrontierProtectionManifest(\n      const rpc::RecoveryManifest &candidate,\n      rpc::RecoveryManifest *authoritative_manifest);\n\n  /// Stage/commit/abort the next contiguous group append.''',
)

replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''  std::unique_ptr<RecoveryFrontierPlanner> recovery_frontier_planner_\n      ABSL_GUARDED_BY(mutex_);\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);''',
    '''  std::unique_ptr<RecoveryFrontierPlanner> recovery_frontier_planner_\n      ABSL_GUARDED_BY(mutex_);\n\n  /// Immutable backend topology for each activated frontier group. The replay\n  /// capsule grows, but its fixed-R witness/holder set never changes.\n  absl::flat_hash_map<TaskID, rpc::RecoveryManifest>\n      recovery_frontier_protection_manifests_ ABSL_GUARDED_BY(mutex_);\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);''',
)

replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''std::optional<RecoveryFrontierMembership>\nRecoverySuccessionManager::GetRecoveryFrontierMembership(\n    const TaskID &task_id) const {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return std::nullopt;\n  }\n  return recovery_frontier_planner_->FindTask(task_id);\n}\n\nstd::optional<RecoveryFrontierAppendBatch>''',
    '''std::optional<RecoveryFrontierMembership>\nRecoverySuccessionManager::GetRecoveryFrontierMembership(\n    const TaskID &task_id) const {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return std::nullopt;\n  }\n  return recovery_frontier_planner_->FindTask(task_id);\n}\n\nbool RecoverySuccessionManager::GetRecoveryFrontierProtectionManifest(\n    const TaskID &group_id, rpc::RecoveryManifest *manifest) const {\n  if (group_id.IsNil() || manifest == nullptr) {\n    return false;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  const auto it = recovery_frontier_protection_manifests_.find(group_id);\n  if (it == recovery_frontier_protection_manifests_.end()) {\n    return false;\n  }\n  manifest->CopyFrom(it->second);\n  return true;\n}\n\nbool RecoverySuccessionManager::CacheRecoveryFrontierProtectionManifest(\n    const rpc::RecoveryManifest &candidate,\n    rpc::RecoveryManifest *authoritative_manifest) {\n  if (authoritative_manifest == nullptr ||\n      candidate.task_id().size() != TaskID::Size()) {\n    return false;\n  }\n\n  const TaskID group_id = TaskID::FromBinary(candidate.task_id());\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr ||\n      recovery_frontier_planner_->GetGroup(group_id) == nullptr) {\n    return false;\n  }\n\n  auto [it, inserted] =\n      recovery_frontier_protection_manifests_.try_emplace(group_id);\n  if (inserted) {\n    it->second.CopyFrom(candidate);\n  }\n  authoritative_manifest->CopyFrom(it->second);\n  return true;\n}\n\nstd::optional<RecoveryFrontierAppendBatch>''',
)

# Real frontiers are correctness mode and must never inherit the old perf-only
# TaskID density filter, even if that experimental knob is accidentally >1.
replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''  // Register every eligible live owner task with the shared frontier planner\n  // before any backend-specific activation/filtering. This is owner-local only:\n  // no holder, witness, manifest, or candidate RPC is emitted here.\n  if (RecoveryFrontierEnabled() && !returned_refs.empty()) {\n    static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));\n  }\n\n  const bool baseline_enabled =\n      RayConfig::instance().enable_recovery_witness_holder_baseline();\n\n  // PERF-ONLY frontier-density owner-state selector.''',
    '''  // Register every eligible live owner task with the shared frontier planner\n  // before any backend-specific activation/filtering. This is owner-local only:\n  // no holder, witness, manifest, or candidate RPC is emitted here.\n  const bool frontier_enabled = RecoveryFrontierEnabled();\n  if (frontier_enabled && !returned_refs.empty()) {\n    static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));\n  }\n\n  const bool baseline_enabled =\n      RayConfig::instance().enable_recovery_witness_holder_baseline();\n\n  // PERF-ONLY frontier-density owner-state selector.''',
)
replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''  if (baseline_enabled) {\n    const uint32_t protect_every_n =\n        RayConfig::instance().recovery_baseline_perf_protect_every_n();''',
    '''  if (baseline_enabled && !frontier_enabled) {\n    const uint32_t protect_every_n =\n        RayConfig::instance().recovery_baseline_perf_protect_every_n();''',
)

# Live CoreWorker activation. Correctness-capable frontier mode bypasses the old
# perf-only selector, derives one immutable group topology, keeps task metadata
# addressed by the original TaskID, and publishes grouped replay recipes.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  const TaskID task_id = object_id.TaskId();\n\n  // PERF-ONLY frontier-density selector.''',
    '''  const TaskID task_id = object_id.TaskId();\n\n  const bool recovery_frontier_enabled =\n      recovery_witness_holder_baseline_enabled_ &&\n      recovery_succession_manager_->RecoveryFrontierEnabled();\n\n  // PERF-ONLY frontier-density selector.''',
)
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  if (recovery_witness_holder_baseline_enabled_) {\n    const uint32_t protect_every_n =\n        RayConfig::instance().recovery_baseline_perf_protect_every_n();''',
    '''  if (recovery_witness_holder_baseline_enabled_ &&\n      !recovery_frontier_enabled) {\n    const uint32_t protect_every_n =\n        RayConfig::instance().recovery_baseline_perf_protect_every_n();''',
)

old_manifest_block = '''  uint64_t manifest_start_ns = 0;\n  if (recovery_succession_profiling_enabled_) {\n    manifest_start_ns = RecoveryProfileNowNs();\n  }\n\n  rpc::RecoveryManifest manifest =\n      recovery_succession_manager_->BuildInitialManifest(\n          task_id, task_spec.JobId(), task_proto.max_retries());\n\n  if (manifest_start_ns != 0) {\n    recovery_succession_manager_->RecordInitialManifestBuild(\n        RecoveryProfileNowNs() - manifest_start_ns,\n        static_cast<uint64_t>(manifest.ByteSizeLong()));\n  }\n\n  uint64_t witness_start_ns = 0;\n  if (recovery_succession_profiling_enabled_) {\n    witness_start_ns = RecoveryProfileNowNs();\n  }\n\n  PopulateRecoveryWitnesses(&manifest);\n\n  if (witness_start_ns != 0) {\n    recovery_succession_manager_->RecordWitnessSelectionLatency(\n        RecoveryProfileNowNs() - witness_start_ns);\n  }\n'''
new_manifest_block = '''  const bool recovery_frontier_grouping_enabled =\n      recovery_frontier_enabled &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n\n  std::optional<RecoveryFrontierMembership> frontier_membership;\n  rpc::RecoveryManifest frontier_protection_manifest;\n  rpc::RecoveryManifest manifest;\n\n  if (recovery_frontier_grouping_enabled) {\n    frontier_membership =\n        recovery_succession_manager_->GetRecoveryFrontierMembership(task_id);\n    if (!frontier_membership.has_value()) {\n      frontier_membership =\n          recovery_succession_manager_->RegisterOwnerTaskWithRecoveryFrontier(\n              task_spec);\n    }\n    if (!frontier_membership.has_value()) {\n      return false;\n    }\n\n    if (!recovery_succession_manager_->GetRecoveryFrontierProtectionManifest(\n            frontier_membership->group_id, &frontier_protection_manifest)) {\n      const uint64_t manifest_start_ns =\n          recovery_succession_profiling_enabled_\n              ? RecoveryProfileNowNs()\n              : 0;\n\n      rpc::RecoveryManifest candidate =\n          recovery_succession_manager_->BuildInitialManifest(\n              frontier_membership->group_id,\n              task_spec.JobId(),\n              task_proto.max_retries());\n\n      if (manifest_start_ns != 0) {\n        recovery_succession_manager_->RecordInitialManifestBuild(\n            RecoveryProfileNowNs() - manifest_start_ns,\n            static_cast<uint64_t>(candidate.ByteSizeLong()));\n      }\n\n      const uint64_t witness_start_ns =\n          recovery_succession_profiling_enabled_\n              ? RecoveryProfileNowNs()\n              : 0;\n      PopulateRecoveryWitnesses(&candidate);\n      if (witness_start_ns != 0) {\n        recovery_succession_manager_->RecordWitnessSelectionLatency(\n            RecoveryProfileNowNs() - witness_start_ns);\n      }\n\n      RAY_CHECK(\n          recovery_succession_manager_->CacheRecoveryFrontierProtectionManifest(\n              candidate, &frontier_protection_manifest))\n          << "Failed to cache Recovery Frontier protection topology for group "\n          << frontier_membership->group_id;\n    }\n\n    // Manager/object metadata remains task-centric so borrowers continue to\n    // request the original deterministic TaskID. Only holder storage/publication\n    // is grouped under the frontier leader.\n    manifest.CopyFrom(frontier_protection_manifest);\n    manifest.set_task_id(task_id.Binary());\n    manifest.set_job_id(task_spec.JobId().Binary());\n    manifest.set_max_recovery_attempts(task_proto.max_retries());\n  } else {\n    uint64_t manifest_start_ns = 0;\n    if (recovery_succession_profiling_enabled_) {\n      manifest_start_ns = RecoveryProfileNowNs();\n    }\n\n    manifest = recovery_succession_manager_->BuildInitialManifest(\n        task_id, task_spec.JobId(), task_proto.max_retries());\n\n    if (manifest_start_ns != 0) {\n      recovery_succession_manager_->RecordInitialManifestBuild(\n          RecoveryProfileNowNs() - manifest_start_ns,\n          static_cast<uint64_t>(manifest.ByteSizeLong()));\n    }\n\n    uint64_t witness_start_ns = 0;\n    if (recovery_succession_profiling_enabled_) {\n      witness_start_ns = RecoveryProfileNowNs();\n    }\n\n    PopulateRecoveryWitnesses(&manifest);\n\n    if (witness_start_ns != 0) {\n      recovery_succession_manager_->RecordWitnessSelectionLatency(\n          RecoveryProfileNowNs() - witness_start_ns);\n    }\n  }\n'''
replace_once("src/ray/core_worker/core_worker.cc", old_manifest_block, new_manifest_block)

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  if (initialized_now && recovery_witness_holder_baseline_enabled_) {\n    const uint32_t target_holder_count =''',
    '''  if (initialized_now && recovery_witness_holder_baseline_enabled_) {\n    if (recovery_frontier_grouping_enabled) {\n      RAY_CHECK(frontier_membership.has_value());\n      PublishRecoveryFrontierGroup(\n          frontier_membership->group_id, frontier_protection_manifest);\n    } else {\n    const uint32_t target_holder_count =''',
)
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''        publish_task_spec,\n        publish_serialized_task_spec);\n  }\n\n  // If another thread won the initialization race,''',
    '''        publish_task_spec,\n        publish_serialized_task_spec);\n    }\n  }\n\n  // If another thread won the initialization race,''',
)

print("Applied live Recovery Frontier owner publication patch")
