from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# CoreWorker ownership serialization: task-argument serialization may defer
# lazy K>1 Frontier activation to BuildCommonTaskSpec, which sees nested refs.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/core_worker.h",
    '''  Status GetOwnershipInfo(const ObjectID &object_id,\n                          rpc::Address *owner_address,\n                          std::string *serialized_object_status);\n''',
    '''  Status GetOwnershipInfo(const ObjectID &object_id,\n                          rpc::Address *owner_address,\n                          std::string *serialized_object_status,\n                          bool task_argument_serialization = false);\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''Status CoreWorker::GetOwnershipInfo(const ObjectID &object_id,\n                                    rpc::Address *owner_address,\n                                    std::string *serialized_object_status) {\n''',
    '''Status CoreWorker::GetOwnershipInfo(const ObjectID &object_id,\n                                    rpc::Address *owner_address,\n                                    std::string *serialized_object_status,\n                                    bool task_argument_serialization) {\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {\n    rpc::RecoveryObjectMetadata metadata;\n\n    if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {\n      object_status.mutable_recovery_metadata()->CopyFrom(metadata);\n    }\n  }\n''',
    '''  if (recovery_succession_enabled_ && recovery_succession_manager_ != nullptr) {\n    rpc::RecoveryObjectMetadata metadata;\n\n    // Nested ObjectRefs serialized as task arguments are seen again by\n    // BuildCommonTaskSpec via nested_inlined_refs(). For Fixed-R Frontier K>1,\n    // do not synchronously cross the all-R durability barrier here; the task\n    // builder attaches the owner-local recovery sidecar and gates remote dispatch\n    // on the same Frontier ACK. Explicit/out-of-band serialization, Fixed-R K1,\n    // and Succession retain the original blocking visibility contract.\n    const bool defer_frontier_task_argument_activation =\n        task_argument_serialization &&\n        recovery_witness_holder_baseline_enabled_ &&\n        recovery_succession_manager_->RecoveryFrontierEnabled() &&\n        RayConfig::instance().recovery_frontier_group_size() > 1;\n\n    if (defer_frontier_task_argument_activation) {\n      // Preserve already-committed metadata without activating new protection.\n      // PopulateRecoveryMetadata exposes only committed recovery state.\n      if (recovery_succession_manager_->PopulateRecoveryMetadata(object_id, &metadata)) {\n        object_status.mutable_recovery_metadata()->CopyFrom(metadata);\n      }\n    } else if (TryPopulateRecoveryMetadataForObject(object_id, &metadata)) {\n      object_status.mutable_recovery_metadata()->CopyFrom(metadata);\n    }\n  }\n''')

# ---------------------------------------------------------------------------
# Cython bridge: mark only normal/actor TASK argument serialization. ray.put,
# explicit pickle/cloudpickle and actor creation keep the existing behavior.
# ---------------------------------------------------------------------------
replace_once(
    "python/ray/includes/libcoreworker.pxd",
    '''        CRayStatus GetOwnershipInfo(const CObjectID &object_id,\n                                    CAddress *owner_address,\n                                    c_string *object_status)\n''',
    '''        CRayStatus GetOwnershipInfo(const CObjectID &object_id,\n                                    CAddress *owner_address,\n                                    c_string *object_status,\n                                    c_bool task_argument_serialization)\n''')

replace_once(
    "python/ray/_raylet.pyx",
    '''async_task_function_name = contextvars.ContextVar('async_task_function_name',                                                  default=None)\n\n\n# Update the type names of the extension type so they are\n''',
    '''async_task_function_name = contextvars.ContextVar('async_task_function_name',                                                  default=None)\n\n# True only while a normal or actor task's by-value arguments are being\n# serialized. Nested ObjectRefs encountered in this scope may defer lazy K>1\n# Recovery Frontier activation to BuildCommonTaskSpec. Generic serialization\n# (ray.put, cloudpickle, explicit ObjectRef serialization) remains synchronous.\ntask_argument_serialization = contextvars.ContextVar(\n    'ray_task_argument_serialization', default=False)\n\n\n# Update the type names of the extension type so they are\n''')

replace_once(
    "python/ray/_raylet.pyx",
    '''    def serialize_object_ref(self, ObjectRef object_ref):\n        cdef:\n            CObjectID c_object_id = object_ref.native()\n            CAddress c_owner_address = CAddress()\n            c_string serialized_object_status\n        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(\n''',
    '''    def serialize_object_ref(self, ObjectRef object_ref):\n        cdef:\n            CObjectID c_object_id = object_ref.native()\n            CAddress c_owner_address = CAddress()\n            c_string serialized_object_status\n            c_bool task_arg_mode = bool(task_argument_serialization.get())\n        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(\n''')

replace_once(
    "python/ray/_raylet.pyx",
    '''        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(\n                c_object_id, &c_owner_address, &serialized_object_status)\n''',
    '''        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(\n                c_object_id, &c_owner_address, &serialized_object_status,\n                task_arg_mode)\n''')

# There are exactly two task submission paths that should set the marker:
# normal tasks and actor tasks. Actor creation intentionally remains unchanged.
needle = '''            prepare_args_and_increment_put_refs(\n                language, args, &args_vector, function_descriptor,\n                &incremented_put_arg_ids)\n'''
p = Path("python/ray/_raylet.pyx")
text = p.read_text()
occurrences = [i for i in range(len(text)) if text.startswith(needle, i)]
if len(occurrences) < 3:
    raise SystemExit(f"expected normal/actor/create-actor arg preparation occurrences, found {len(occurrences)}")
# submit_task occurs before create_actor; submit_actor_task occurs later. Patch by
# function-local spans so create_actor stays synchronous.
for fn_name in ["    def submit_task(self,", "    def submit_actor_task(self,"]:
    start = text.index(fn_name)
    next_def = text.find("\n    def ", start + len(fn_name))
    if next_def == -1:
        next_def = len(text)
    segment = text[start:next_def]
    if segment.count(needle) != 1:
        raise SystemExit(f"{fn_name.strip()}: expected one prepare_args call, found {segment.count(needle)}")
    replacement = '''            task_arg_token = task_argument_serialization.set(True)\n            try:\n                prepare_args_and_increment_put_refs(\n                    language, args, &args_vector, function_descriptor,\n                    &incremented_put_arg_ids)\n            finally:\n                task_argument_serialization.reset(task_arg_token)\n'''
    segment = segment.replace(needle, replacement, 1)
    text = text[:start] + segment + text[next_def:]
p.write_text(text)

# ---------------------------------------------------------------------------
# ActorTaskSubmitter: reserve actor sequence order immediately but optionally
# postpone dependency resolution until Recovery Frontier durability completes.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/task_submission/actor_task_submitter.h",
    '''  /// Submit a task to an actor for execution.\n  void SubmitTask(TaskSpecification task_spec);\n''',
    '''  /// Submit a task to an actor for execution.\n  /// When defer_dependency_resolution is true, the actor sequence position is\n  /// reserved immediately but dependency resolution starts only after\n  /// ResumeDeferredTask().\n  void SubmitTask(TaskSpecification task_spec,\n                  bool defer_dependency_resolution = false);\n\n  /// Resume an actor task whose dependency resolution was deferred behind a\n  /// Recovery Frontier durability barrier. Safe to call after cancellation or\n  /// actor death; stale resumes are ignored.\n  void ResumeDeferredTask(const TaskID &task_id);\n''')

replace_once(
    "src/ray/core_worker/task_submission/actor_task_submitter.h",
    '''  void CancelDependencyResolution(const TaskID &task_id)\n      ABSL_LOCKS_EXCLUDED(resolver_mu_);\n''',
    '''  void StartDependencyResolution(TaskSpecification task_spec)\n      ABSL_LOCKS_EXCLUDED(resolver_mu_);\n\n  void CancelDependencyResolution(const TaskID &task_id)\n      ABSL_LOCKS_EXCLUDED(resolver_mu_);\n''')

replace_once(
    "src/ray/core_worker/task_submission/actor_task_submitter.h",
    '''  absl::flat_hash_set<TaskID> pending_dependency_resolution_\n      ABSL_GUARDED_BY(resolver_mu_);\n''',
    '''  absl::flat_hash_set<TaskID> pending_dependency_resolution_\n      ABSL_GUARDED_BY(resolver_mu_);\n  // Subset of pending_dependency_resolution_ whose actor sequence positions are\n  // already reserved but whose dependency resolver has not yet been started.\n  absl::flat_hash_set<TaskID> deferred_dependency_resolution_\n      ABSL_GUARDED_BY(resolver_mu_);\n''')

actor_cc = Path("src/ray/core_worker/task_submission/actor_task_submitter.cc")
text = actor_cc.read_text()
start = text.index("void ActorTaskSubmitter::SubmitTask(TaskSpecification task_spec) {")
end = text.index("void ActorTaskSubmitter::CancelDependencyResolution", start)
old_block = text[start:end]
new_block = r'''void ActorTaskSubmitter::StartDependencyResolution(TaskSpecification task_spec) {
  const auto task_id = task_spec.TaskId();
  const auto actor_id = task_spec.ActorId();
  const auto send_pos = task_spec.ConcurrencyGroupSequenceNumber();
  const auto concurrency_group = task_spec.ConcurrencyGroupName();

  io_service_.post(
      [task_spec, task_id, actor_id, send_pos, concurrency_group, this]() mutable {
        {
          absl::MutexLock resolver_lock(&resolver_mu_);
          if (pending_dependency_resolution_.erase(task_id) == 0) {
            return;
          }
          deferred_dependency_resolution_.erase(task_id);
        }
        resolver_.ResolveDependencies(
            task_spec,
            [this, send_pos, concurrency_group, actor_id, task_id](Status status) {
              task_manager_.MarkDependenciesResolved(task_id);
              bool fail_or_retry_task = false;
              {
                absl::MutexLock lock(&mu_);
                auto queue = client_queues_.find(actor_id);
                RAY_CHECK(queue != client_queues_.end());
                auto &actor_submit_queue = queue->second.actor_submit_queue_;
                // Only dispatch tasks if the submitted task is still queued. The task
                // may have been dequeued if the actor has since failed.
                if (actor_submit_queue->Contains(concurrency_group, send_pos)) {
                  if (status.ok()) {
                    actor_submit_queue->MarkDependencyResolved(concurrency_group,
                                                               send_pos);
                    SendPendingTasks(actor_id);
                  } else {
                    fail_or_retry_task = true;
                    actor_submit_queue->MarkDependencyFailed(concurrency_group,
                                                             send_pos);
                  }
                }
              }

              if (fail_or_retry_task) {
                task_manager_.FailOrRetryPendingTask(
                    task_id, rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status);
              }
            });
      },
      "ActorTaskSubmitter::StartDependencyResolution");
}

void ActorTaskSubmitter::SubmitTask(TaskSpecification task_spec,
                                    bool defer_dependency_resolution) {
  auto task_id = task_spec.TaskId();
  auto actor_id = task_spec.ActorId();
  RAY_LOG(DEBUG).WithField(task_id) << "Submitting task";
  RAY_CHECK(task_spec.IsActorTask());

  bool task_queued = false;
  {
    // Reserve actor send order before dependency resolution. This remains true
    // even when Recovery Frontier durability temporarily defers resolution.
    absl::MutexLock lock(&mu_);
    auto queue = client_queues_.find(actor_id);
    RAY_CHECK(queue != client_queues_.end());
    if (queue->second.state_ == rpc::ActorTableData::DEAD &&
        queue->second.is_restartable_ && queue->second.owned_) {
      RestartActorForLineageReconstruction(actor_id);
    }
    if (queue->second.state_ != rpc::ActorTableData::DEAD) {
      const uint64_t send_pos = task_spec.ConcurrencyGroupSequenceNumber();
      const auto concurrency_group = task_spec.ConcurrencyGroupName();
      queue->second.actor_submit_queue_->Emplace(concurrency_group, send_pos, task_spec);
      queue->second.cur_pending_calls_++;
      task_queued = true;
    }
  }

  if (task_queued) {
    {
      absl::MutexLock resolver_lock(&resolver_mu_);
      pending_dependency_resolution_.insert(task_id);
      if (defer_dependency_resolution) {
        deferred_dependency_resolution_.insert(task_id);
      }
    }
    if (!defer_dependency_resolution) {
      StartDependencyResolution(std::move(task_spec));
    }
  } else {
    // Do not hold the lock while calling into task_manager_.
    task_manager_.MarkTaskNoRetry(task_id);
    rpc::ErrorType error_type;
    rpc::RayErrorInfo error_info;
    {
      absl::MutexLock lock(&mu_);
      const auto queue_it = client_queues_.find(task_spec.ActorId());
      const auto &death_cause = queue_it->second.death_cause_;
      error_info = gcs::GetErrorInfoFromActorDeathCause(death_cause);
      error_type = error_info.error_type();
    }
    auto status = Status::IOError("cancelling task of dead actor");
    // No need to increment the number of completed tasks since the actor is dead.
    bool fail_immediately =
        error_info.has_actor_died_error() &&
        error_info.actor_died_error().has_oom_context() &&
        error_info.actor_died_error().oom_context().fail_immediately();
    task_manager_.FailOrRetryPendingTask(task_id,
                                         error_type,
                                         &status,
                                         &error_info,
                                         /*mark_task_object_failed*/ true,
                                         fail_immediately);
  }
}

void ActorTaskSubmitter::ResumeDeferredTask(const TaskID &task_id) {
  {
    absl::MutexLock resolver_lock(&resolver_mu_);
    if (deferred_dependency_resolution_.erase(task_id) == 0) {
      // Cancellation, actor death, or a duplicate callback may have removed it.
      return;
    }
  }

  auto task_spec = task_manager_.GetTaskSpec(task_id);
  if (!task_spec.has_value()) {
    absl::MutexLock resolver_lock(&resolver_mu_);
    pending_dependency_resolution_.erase(task_id);
    return;
  }
  StartDependencyResolution(std::move(task_spec.value()));
}

'''
text = text[:start] + new_block + text[end:]
actor_cc.write_text(text)

replace_once(
    "src/ray/core_worker/task_submission/actor_task_submitter.cc",
    '''void ActorTaskSubmitter::CancelDependencyResolution(const TaskID &task_id) {\n  absl::MutexLock resolver_lock(&resolver_mu_);\n  pending_dependency_resolution_.erase(task_id);\n  RAY_UNUSED(resolver_.CancelDependencyResolution(task_id));\n}\n''',
    '''void ActorTaskSubmitter::CancelDependencyResolution(const TaskID &task_id) {\n  absl::MutexLock resolver_lock(&resolver_mu_);\n  deferred_dependency_resolution_.erase(task_id);\n  pending_dependency_resolution_.erase(task_id);\n  RAY_UNUSED(resolver_.CancelDependencyResolution(task_id));\n}\n''')

# ---------------------------------------------------------------------------
# Actor CoreWorker path: BuildCommonTaskSpec prepares deferred Frontier groups,
# actor submitter reserves sequence immediately, ACK callbacks resume resolution.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  const auto task_name = task_options.name.empty()\n                             ? function.GetFunctionDescriptor()->DefaultTaskName()\n                             : task_options.name;\n\n  // The depth of the actor task is depth of the caller + 1\n''',
    '''  const auto task_name = task_options.name.empty()\n                             ? function.GetFunctionDescriptor()->DefaultTaskName()\n                             : task_options.name;\n\n  const bool defer_recovery_frontier_dispatch =\n      recovery_succession_enabled_ &&\n      recovery_witness_holder_baseline_enabled_ &&\n      recovery_succession_manager_ != nullptr &&\n      recovery_succession_manager_->RecoveryFrontierEnabled() &&\n      RayConfig::instance().recovery_frontier_group_size() > 1;\n  std::vector<DeferredRecoveryFrontierGroup> deferred_recovery_frontier_groups;\n\n  // The depth of the actor task is depth of the caller + 1\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''                      /*label_selector=*/{},\n                      /*fallback_strategy=*/{},\n                      task_options.num_objects_per_yield);\n''',
    '''                      /*label_selector=*/{},\n                      /*fallback_strategy=*/{},\n                      task_options.num_objects_per_yield,\n                      defer_recovery_frontier_dispatch\n                          ? &deferred_recovery_frontier_groups\n                          : nullptr);\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  task_returns = task_manager_->AddPendingTask(\n      rpc_address_, task_spec, CurrentCallSite(), max_retries);\n  actor_task_submitter_->SubmitTask(task_spec);\n  return Status::OK();\n}\n''',
    '''  task_returns = task_manager_->AddPendingTask(\n      rpc_address_, task_spec, CurrentCallSite(), max_retries);\n\n  const bool defer_actor_dependency_resolution =\n      defer_recovery_frontier_dispatch &&\n      !deferred_recovery_frontier_groups.empty();\n  // Reserve the actor sequence position immediately. When deferred, the\n  // ActorTaskSubmitter holds dependency resolution until all Frontier groups\n  // protecting this task's arguments have committed.\n  actor_task_submitter_->SubmitTask(task_spec, defer_actor_dependency_resolution);\n\n  if (defer_actor_dependency_resolution) {\n    auto remaining_groups = std::make_shared<std::atomic<size_t>>(\n        deferred_recovery_frontier_groups.size());\n    for (const DeferredRecoveryFrontierGroup &group :\n         deferred_recovery_frontier_groups) {\n      PublishRecoveryFrontierGroupAsync(\n          group.group_id,\n          group.protection_manifest,\n          [this, actor_task_id, remaining_groups]() {\n            if (remaining_groups->fetch_sub(1, std::memory_order_acq_rel) == 1) {\n              actor_task_submitter_->ResumeDeferredTask(actor_task_id);\n            }\n          });\n    }\n  }\n\n  return Status::OK();\n}\n''')

print("Recovery Frontier task-argument fast-path patch applied")
