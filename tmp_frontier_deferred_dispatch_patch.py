from pathlib import Path


def replace_once(path, old, new):
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))


# RecoverySuccessionManager declarations.
path = "src/ray/core_worker/recovery_succession_manager.h"
replace_once(
    path,
    """  /// Adds recovery metadata to direct and nested ObjectRef arguments.\n  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec);\n""",
    """  /// Adds recovery metadata to direct and nested ObjectRef arguments.\n  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec);\n\n  /// Builds the same compact argument sidecar for a task that is still local\n  /// and whose remote dispatch is gated on Recovery Frontier durability. The\n  /// sidecar may describe a staged/uncommitted frontier member, but it must\n  /// never leave this CoreWorker until the corresponding all-R ACK completes.\n  void PopulateTaskArgumentMetadataForDeferredFrontierDispatch(\n      rpc::TaskSpec *task_spec);\n""",
)
replace_once(
    path,
    """  bool BuildRecoveryMetadataLocked(\n      const ObjectID &object_id,\n      rpc::RecoveryObjectMetadata *metadata) const\n      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);\n""",
    """  bool BuildRecoveryMetadataLocked(\n      const ObjectID &object_id,\n      rpc::RecoveryObjectMetadata *metadata,\n      bool require_frontier_commit) const\n      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);\n\n  void PopulateTaskArgumentMetadataInternal(\n      rpc::TaskSpec *task_spec, bool require_frontier_commit);\n""",
)

# RecoverySuccessionManager implementation.
path = "src/ray/core_worker/recovery_succession_manager.cc"
replace_once(
    path,
    """bool RecoverySuccessionManager::BuildRecoveryMetadataLocked(\n    const ObjectID &object_id,\n    rpc::RecoveryObjectMetadata *metadata) const {\n""",
    """bool RecoverySuccessionManager::BuildRecoveryMetadataLocked(\n    const ObjectID &object_id,\n    rpc::RecoveryObjectMetadata *metadata,\n    bool require_frontier_commit) const {\n""",
)
p = Path(path)
text = p.read_text()
fn_start = text.index("bool RecoverySuccessionManager::BuildRecoveryMetadataLocked(")
fn_end = text.index("\n\nbool RecoverySuccessionManager::HasRecoveryMetadata", fn_start)
block = text[fn_start:fn_end]
old_gate = """  if (recovery_frontier_planner_ != nullptr &&\n      recovery_frontier_planner_->GroupSize() > 1) {\n"""
new_gate = """  if (require_frontier_commit &&\n      recovery_frontier_planner_ != nullptr &&\n      recovery_frontier_planner_->GroupSize() > 1) {\n"""
if block.count(old_gate) != 1:
    raise SystemExit("BuildRecoveryMetadataLocked: frontier gate not unique")
block = block.replace(old_gate, new_gate, 1)
text = text[:fn_start] + block + text[fn_end:]
p.write_text(text)
replace_once(
    path,
    "return BuildRecoveryMetadataLocked(object_id, nullptr);",
    "return BuildRecoveryMetadataLocked(object_id, nullptr, /*require_frontier_commit=*/true);",
)
replace_once(
    path,
    "const bool hit = BuildRecoveryMetadataLocked(object_id, metadata);",
    "const bool hit = BuildRecoveryMetadataLocked(\n      object_id, metadata, /*require_frontier_commit=*/true);",
)
replace_once(
    path,
    """void RecoverySuccessionManager::PopulateTaskArgumentMetadata(\n    rpc::TaskSpec *task_spec) {\n  if (task_spec == nullptr) {\n""",
    """void RecoverySuccessionManager::PopulateTaskArgumentMetadata(\n    rpc::TaskSpec *task_spec) {\n  PopulateTaskArgumentMetadataInternal(\n      task_spec, /*require_frontier_commit=*/true);\n}\n\nvoid RecoverySuccessionManager::PopulateTaskArgumentMetadataForDeferredFrontierDispatch(\n    rpc::TaskSpec *task_spec) {\n  PopulateTaskArgumentMetadataInternal(\n      task_spec, /*require_frontier_commit=*/false);\n}\n\nvoid RecoverySuccessionManager::PopulateTaskArgumentMetadataInternal(\n    rpc::TaskSpec *task_spec, bool require_frontier_commit) {\n  if (task_spec == nullptr) {\n""",
)
replace_once(
    path,
    "if (BuildRecoveryMetadataLocked(object_id, &source_storage)) {",
    "if (BuildRecoveryMetadataLocked(\n            object_id, &source_storage, require_frontier_commit)) {",
)

# CoreWorker declarations/state.
path = "src/ray/core_worker/core_worker.h"
p = Path(path)
text = p.read_text()
if "#include <atomic>" not in text:
    text = text.replace("#include <gtest/gtest_prod.h>\n\n", "#include <gtest/gtest_prod.h>\n\n#include <atomic>\n", 1)
    p.write_text(text)
replace_once(
    path,
    """  /// Populates metadata if already active; otherwise lazily initializes\n  /// Recovery Succession or the fixed witness-holder baseline for an eligible\n  /// task return owned by this CoreWorker.\n  bool TryPopulateRecoveryMetadataForObject(\n      const ObjectID &object_id,\n      rpc::RecoveryObjectMetadata *metadata) const;\n\n  /// Lazily activate eligible owned returns that are actually being exported\n  /// as arguments of another task.\n  void EnsureRecoverySuccessionForTaskArguments(\n      rpc::TaskSpec *task_spec) const;\n""",
    """  struct DeferredRecoveryFrontierGroup {\n    TaskID group_id = TaskID::Nil();\n    rpc::RecoveryManifest protection_manifest;\n  };\n\n  /// Populates metadata if already active; otherwise lazily initializes\n  /// Recovery Succession or the fixed witness-holder baseline for an eligible\n  /// task return owned by this CoreWorker. When deferred_groups is non-null,\n  /// Fixed-R Frontier K>1 prepares local state without synchronously waiting\n  /// for the all-R publication barrier.\n  bool TryPopulateRecoveryMetadataForObject(\n      const ObjectID &object_id,\n      rpc::RecoveryObjectMetadata *metadata,\n      std::vector<DeferredRecoveryFrontierGroup> *deferred_groups = nullptr) const;\n\n  /// Lazily activate eligible owned returns that are actually being exported\n  /// as arguments of another task.\n  void EnsureRecoverySuccessionForTaskArguments(\n      rpc::TaskSpec *task_spec,\n      std::vector<DeferredRecoveryFrontierGroup> *deferred_groups = nullptr) const;\n\n  /// A chained task can be locally visible before its own upstream groups have\n  /// crossed the all-R barrier. Wait before allowing that task's output to\n  /// become an externally recoverable dependency.\n  void WaitForDeferredRecoveryTaskDependencies(const TaskID &task_id) const;\n""",
)
replace_once(
    path,
    """  /// Publish every currently uncommitted replay recipe for one Recovery\n  /// Frontier group to its fixed-R holder topology. The owner-side committed\n  /// prefix advances only from the all-holder completion callback.\n  void PublishRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n""",
    """  using RecoveryFrontierPublicationCallback = std::function<void()>;\n\n  /// Asynchronously publish every currently uncommitted replay recipe for one\n  /// Recovery Frontier group. callback runs only after the required prefix is\n  /// durable on all fixed-R holders.\n  void PublishRecoveryFrontierGroupAsync(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest,\n      RecoveryFrontierPublicationCallback callback) const;\n\n  /// Synchronous wrapper retained for explicit ObjectRef serialization/status\n  /// paths. Normal-task dispatch uses the asynchronous form instead.\n  void PublishRecoveryFrontierGroup(\n      const TaskID &group_id,\n      const rpc::RecoveryManifest &protection_manifest) const;\n""",
)
replace_once(
    path,
    """      const std::unordered_map<std::string, std::string> &labels = {},\n      const LabelSelector &label_selector = {},\n      const std::vector<FallbackOption> &fallback_strategy = {},\n      int64_t num_objects_per_yield = 1);\n""",
    """      const std::unordered_map<std::string, std::string> &labels = {},\n      const LabelSelector &label_selector = {},\n      const std::vector<FallbackOption> &fallback_strategy = {},\n      int64_t num_objects_per_yield = 1,\n      std::vector<DeferredRecoveryFrontierGroup> *deferred_groups = nullptr);\n""",
)
replace_once(
    path,
    """  absl::flat_hash_map<TaskID, RecoveryHolderAdmissionTaskState>\n      recovery_holder_admission_states_\n          ABSL_GUARDED_BY(recovery_holder_admission_mutex_);\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n""",
    """  absl::flat_hash_map<TaskID, RecoveryHolderAdmissionTaskState>\n      recovery_holder_admission_states_\n          ABSL_GUARDED_BY(recovery_holder_admission_mutex_);\n\n  struct DeferredRecoveryTaskState {\n    std::mutex mutex;\n    std::condition_variable cv;\n    bool ready = false;\n  };\n\n  // Locally returned task refs can precede upstream Frontier durability, but\n  // chained activation must never cross that boundary.\n  mutable std::mutex recovery_frontier_deferred_task_mutex_;\n  mutable absl::flat_hash_map<TaskID, std::shared_ptr<DeferredRecoveryTaskState>>\n      recovery_frontier_deferred_tasks_;\n\n  absl::flat_hash_set<TaskID> recovery_tombstones_in_flight_;\n""",
)

# CoreWorker implementation.
path = "src/ray/core_worker/core_worker.cc"
replace_once(
    path,
    """bool CoreWorker::TryPopulateRecoveryMetadataForObject(\n    const ObjectID &object_id,\n    rpc::RecoveryObjectMetadata *metadata) const {\n""",
    """bool CoreWorker::TryPopulateRecoveryMetadataForObject(\n    const ObjectID &object_id,\n    rpc::RecoveryObjectMetadata *metadata,\n    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) const {\n""",
)
replace_once(
    path,
    """  const TaskID task_id = object_id.TaskId();\n\n  const bool recovery_frontier_enabled =\n""",
    """  const TaskID task_id = object_id.TaskId();\n\n  // Preserve transitive replay correctness for immediate task chains.\n  WaitForDeferredRecoveryTaskDependencies(task_id);\n\n  const bool recovery_frontier_enabled =\n""",
)
replace_once(
    path,
    """      PublishRecoveryFrontierGroup(\n          frontier_membership->group_id, frontier_protection_manifest);\n\n      if (metadata == nullptr) {\n        return true;\n      }\n      return recovery_succession_manager_->PopulateRecoveryMetadata(\n          object_id, metadata);\n""",
    """      if (deferred_groups != nullptr) {\n        RAY_CHECK(metadata == nullptr)\n            << \"Deferred Recovery Frontier preparation must remain owner-local\";\n\n        const bool already_pending = std::any_of(\n            deferred_groups->begin(),\n            deferred_groups->end(),\n            [&frontier_membership](const DeferredRecoveryFrontierGroup &group) {\n              return group.group_id == frontier_membership->group_id;\n            });\n        if (!already_pending) {\n          DeferredRecoveryFrontierGroup group;\n          group.group_id = frontier_membership->group_id;\n          group.protection_manifest.CopyFrom(frontier_protection_manifest);\n          deferred_groups->push_back(std::move(group));\n        }\n        return true;\n      }\n\n      PublishRecoveryFrontierGroup(\n          frontier_membership->group_id, frontier_protection_manifest);\n\n      if (metadata == nullptr) {\n        return true;\n      }\n      return recovery_succession_manager_->PopulateRecoveryMetadata(\n          object_id, metadata);\n""",
)
replace_once(
    path,
    """void CoreWorker::EnsureRecoverySuccessionForTaskArguments(\n    rpc::TaskSpec *task_spec) const {\n""",
    """void CoreWorker::EnsureRecoverySuccessionForTaskArguments(\n    rpc::TaskSpec *task_spec,\n    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) const {\n""",
)
p = Path(path)
text = p.read_text()
ensure_start = text.index("void CoreWorker::EnsureRecoverySuccessionForTaskArguments(")
ensure_end = text.index("\n\nstd::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs", ensure_start)
block = text[ensure_start:ensure_end]
if block.count("TryPopulateRecoveryMetadataForObject(") != 2:
    raise SystemExit("EnsureRecoverySuccessionForTaskArguments: unexpected TryPopulate count")
block = block.replace(
    "TryPopulateRecoveryMetadataForObject(object_id, nullptr);",
    "TryPopulateRecoveryMetadataForObject(\n          object_id, nullptr, deferred_groups);",
    1,
)
block = block.replace(
    "TryPopulateRecoveryMetadataForObject(nested_id, nullptr);",
    "TryPopulateRecoveryMetadataForObject(\n          nested_id, nullptr, deferred_groups);",
    1,
)
text = text[:ensure_start] + block + text[ensure_end:]
p.write_text(text)
p = Path(path)
text = p.read_text()
insert_at = text.index("\n\nstd::vector<rpc::ObjectReference> CoreWorker::GetObjectRefs")
waiter = """

void CoreWorker::WaitForDeferredRecoveryTaskDependencies(
    const TaskID &task_id) const {
  std::shared_ptr<DeferredRecoveryTaskState> state;
  {
    std::lock_guard<std::mutex> lock(recovery_frontier_deferred_task_mutex_);
    const auto it = recovery_frontier_deferred_tasks_.find(task_id);
    if (it == recovery_frontier_deferred_tasks_.end()) {
      return;
    }
    state = it->second;
  }

  std::unique_lock<std::mutex> lock(state->mutex);
  state->cv.wait(lock, [&state] { return state->ready; });
}
"""
text = text[:insert_at] + waiter + text[insert_at:]
p.write_text(text)
replace_once(
    path,
    """    const std::vector<FallbackOption> &fallback_strategy,\n    int64_t num_objects_per_yield) {\n""",
    """    const std::vector<FallbackOption> &fallback_strategy,\n    int64_t num_objects_per_yield,\n    std::vector<DeferredRecoveryFrontierGroup> *deferred_groups) {\n""",
)
replace_once(
    path,
    """    EnsureRecoverySuccessionForTaskArguments(builder.MutableMessage());\n\n    if (recovery_succession_profiling_enabled_) {\n      const uint64_t start_ns = RecoveryProfileNowNs();\n\n      recovery_succession_manager_->PopulateTaskArgumentMetadata(\n          builder.MutableMessage());\n\n      recovery_succession_manager_->RecordTaskArgumentMetadataLatency(\n          RecoveryProfileNowNs() - start_ns);\n    } else {\n      recovery_succession_manager_->PopulateTaskArgumentMetadata(\n          builder.MutableMessage());\n    }\n""",
    """    EnsureRecoverySuccessionForTaskArguments(\n        builder.MutableMessage(), deferred_groups);\n\n    auto populate_argument_metadata = [this, deferred_groups](rpc::TaskSpec *message) {\n      if (deferred_groups != nullptr) {\n        recovery_succession_manager_\n            ->PopulateTaskArgumentMetadataForDeferredFrontierDispatch(message);\n      } else {\n        recovery_succession_manager_->PopulateTaskArgumentMetadata(message);\n      }\n    };\n\n    if (recovery_succession_profiling_enabled_) {\n      const uint64_t start_ns = RecoveryProfileNowNs();\n      populate_argument_metadata(builder.MutableMessage());\n      recovery_succession_manager_->RecordTaskArgumentMetadataLatency(\n          RecoveryProfileNowNs() - start_ns);\n    } else {\n      populate_argument_metadata(builder.MutableMessage());\n    }\n""",
)
replace_once(
    path,
    """  int64_t depth = worker_context_->GetTaskDepth() + 1;\n  // TODO(ekl) offload task building onto a thread pool for performance\n\n  BuildCommonTaskSpec(builder,\n""",
    """  int64_t depth = worker_context_->GetTaskDepth() + 1;\n\n  const bool defer_recovery_frontier_dispatch =\n      recovery_succession_enabled_ &&\n      recovery_witness_holder_baseline_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      recovery_succession_manager_->RecoveryFrontierEnabled() &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n  std::vector<DeferredRecoveryFrontierGroup> deferred_recovery_frontier_groups;\n\n  // TODO(ekl) offload task building onto a thread pool for performance\n\n  BuildCommonTaskSpec(builder,\n""",
)
replace_once(
    path,
    """                      task_options.label_selector,\n                      task_options.fallback_strategy,\n                      task_options.num_objects_per_yield);\n""",
    """                      task_options.label_selector,\n                      task_options.fallback_strategy,\n                      task_options.num_objects_per_yield,\n                      defer_recovery_frontier_dispatch\n                          ? &deferred_recovery_frontier_groups\n                          : nullptr);\n""",
)
replace_once(
    path,
    """  io_service_.post(\n      [this, task_spec = std::move(task_spec)]() mutable {\n        normal_task_submitter_->SubmitTask(std::move(task_spec));\n      },\n      \"CoreWorker.SubmitTask\");\n  return returned_refs;\n}\n""",
    """  if (defer_recovery_frontier_dispatch &&\n      !deferred_recovery_frontier_groups.empty()) {\n    const TaskID deferred_task_id = task_spec.TaskId();\n    auto readiness = std::make_shared<DeferredRecoveryTaskState>();\n    {\n      std::lock_guard<std::mutex> lock(recovery_frontier_deferred_task_mutex_);\n      const auto inserted = recovery_frontier_deferred_tasks_.emplace(\n          deferred_task_id, readiness);\n      RAY_CHECK(inserted.second)\n          << \"Duplicate deferred Recovery Frontier task state for \"\n          << deferred_task_id;\n    }\n\n    auto remaining = std::make_shared<std::atomic<size_t>>(\n        deferred_recovery_frontier_groups.size());\n    auto task_to_dispatch =\n        std::make_shared<TaskSpecification>(std::move(task_spec));\n\n    for (const DeferredRecoveryFrontierGroup &group :\n         deferred_recovery_frontier_groups) {\n      PublishRecoveryFrontierGroupAsync(\n          group.group_id,\n          group.protection_manifest,\n          [this, deferred_task_id, readiness, remaining, task_to_dispatch]() mutable {\n            if (remaining->fetch_sub(1) != 1) {\n              return;\n            }\n\n            {\n              std::lock_guard<std::mutex> ready_lock(readiness->mutex);\n              readiness->ready = true;\n            }\n            readiness->cv.notify_all();\n\n            {\n              std::lock_guard<std::mutex> map_lock(\n                  recovery_frontier_deferred_task_mutex_);\n              const auto it =\n                  recovery_frontier_deferred_tasks_.find(deferred_task_id);\n              if (it != recovery_frontier_deferred_tasks_.end() &&\n                  it->second == readiness) {\n                recovery_frontier_deferred_tasks_.erase(it);\n              }\n            }\n\n            io_service_.post(\n                [this, task_to_dispatch]() mutable {\n                  normal_task_submitter_->SubmitTask(\n                      std::move(*task_to_dispatch));\n                },\n                \"CoreWorker.SubmitTaskAfterRecoveryFrontierCommit\");\n          });\n    }\n  } else {\n    io_service_.post(\n        [this, task_spec = std::move(task_spec)]() mutable {\n          normal_task_submitter_->SubmitTask(std::move(task_spec));\n        },\n        \"CoreWorker.SubmitTask\");\n  }\n  return returned_refs;\n}\n""",
)

# Frontier publisher: asynchronous primitive plus synchronous compatibility wrapper.
path = "src/ray/core_worker/recovery_frontier_publication.cc"
p = Path(path)
text = p.read_text()
start = text.index("void CoreWorker::PublishRecoveryFrontierGroup(")
end = text.rindex("\n}  // namespace ray::core")
replacement = r'''void CoreWorker::PublishRecoveryFrontierGroupAsync(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest,
    RecoveryFrontierPublicationCallback callback) const {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !recovery_succession_manager_->RecoveryFrontierEnabled() ||
      group_id.IsNil() ||
      protection_manifest.task_id() != group_id.Binary()) {
    callback();
    return;
  }

  const uint32_t target_holder_count =
      RayConfig::instance().recovery_succession_target_holder_count();
  RAY_CHECK_EQ(
      static_cast<uint32_t>(protection_manifest.witness_raylets_size()),
      target_holder_count)
      << "Recovery Frontier fixed-R publication requires exactly "
      << target_holder_count << " holder raylets for group " << group_id;
  RAY_CHECK_EQ(protection_manifest.witness_count(), target_holder_count);

  if (!recovery_succession_manager_
           ->RecoveryFrontierGroupHasUncommittedMembers(group_id)) {
    callback();
    return;
  }

  auto staged = recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
  if (!staged.has_value()) {
    io_service_.post(
        [this,
         group_id,
         protection_manifest,
         callback = std::move(callback)]() mutable {
          PublishRecoveryFrontierGroupAsync(
              group_id, protection_manifest, std::move(callback));
        },
        "CoreWorker.RetryRecoveryFrontierPublication",
        /*delay_us=*/50);
    return;
  }

  auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
      std::move(staged.value()));
  const std::string serialized_append = BuildRecoveryFrontierAppendEnvelope(*batch);
  const uint64_t publish_start_ns =
      recovery_succession_profiling_enabled_ ? RecoveryProfileNowNs() : 0;

  PublishRecoveryManifestToWitnesses(
      protection_manifest,
      [this,
       manager = recovery_succession_manager_,
       group_id,
       protection_manifest,
       batch,
       publish_start_ns,
       callback = std::move(callback)](
          bool stored,
          std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (publish_start_ns != 0) {
          manager->RecordWitnessPublishLatency(
              RecoveryProfileNowNs() - publish_start_ns);
        }

        if (!stored) {
          const bool aborted = manager->AbortRecoveryFrontierAppend(*batch);
          RAY_CHECK(aborted)
              << "Failed to abort Recovery Frontier append generation "
              << batch->generation << " for group " << group_id;
          RAY_LOG(FATAL)
              .WithField(group_id)
              << "Recovery Frontier failed to install append generation "
              << batch->generation << " on every fixed-R holder."
              << (newer_manifest.has_value()
                      ? " A newer holder manifest was observed."
                      : "");
          return;
        }

        const bool committed = manager->CommitRecoveryFrontierAppend(*batch);
        RAY_CHECK(committed)
            << "Stale or mismatched Recovery Frontier ACK for generation "
            << batch->generation << " group " << group_id;

        RAY_LOG(INFO)
            .WithField(group_id)
            << "Committed Recovery Frontier append generation "
            << batch->generation << " members=["
            << batch->begin_member_index << ","
            << batch->end_member_index << ") on all fixed-R holders";

        PublishRecoveryFrontierGroupAsync(
            group_id, protection_manifest, std::move(callback));
      },
      /*task_spec=*/nullptr,
      &serialized_append);
}

void CoreWorker::PublishRecoveryFrontierGroup(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest) const {
  auto completion = std::make_shared<std::promise<void>>();
  std::future<void> completion_future = completion->get_future();

  PublishRecoveryFrontierGroupAsync(
      group_id,
      protection_manifest,
      [completion]() { completion->set_value(); });

  completion_future.get();
}
'''
text = text[:start] + replacement + text[end:]
text = text.replace("#include <thread>\n", "")
p.write_text(text)

print("Deferred Recovery Frontier dispatch patch applied")
