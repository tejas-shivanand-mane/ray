from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    p.write_text(text.replace(old, new, 1))


# CoreWorker-local normal-task submission profiling. Keep this independent of
# RecoverySuccessionManager so the disabled control can expose identical stage
# counters when recovery itself is off.
replace_once(
    "src/ray/core_worker/core_worker.h",
    '''  const bool recovery_succession_profiling_enabled_;\n\n  /// Distributed recovery succession state. Null when the feature is disabled.\n''',
    '''  const bool recovery_succession_profiling_enabled_;\n\n  // Diagnostic-only normal-task submit-stage timers. These are CoreWorker-local\n  // so the disabled control can be measured with exactly the same instrumentation.\n  mutable std::atomic<uint64_t> normal_submit_profile_calls_{0};\n  mutable std::atomic<uint64_t> normal_submit_prebuild_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_build_common_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_finalize_spec_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_add_pending_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_owner_setup_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_dispatch_setup_time_ns_{0};\n  mutable std::atomic<uint64_t> normal_submit_total_time_ns_{0};\n\n  /// Distributed recovery succession state. Null when the feature is disabled.\n''')

# Export CoreWorker-local timers before the early return so recovery-disabled
# runs still expose them.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  result["profiling_enabled"] =\n      recovery_succession_profiling_enabled_;\n\n  if (!recovery_succession_profiling_enabled_ ||\n      recovery_succession_manager_ == nullptr) {\n    return result.dump();\n  }\n''',
    '''  result["profiling_enabled"] =\n      recovery_succession_profiling_enabled_;\n\n  result["normal_submit_profile_calls"] =\n      normal_submit_profile_calls_.load(std::memory_order_relaxed);\n  result["normal_submit_prebuild_time_ns"] =\n      normal_submit_prebuild_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_build_common_time_ns"] =\n      normal_submit_build_common_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_finalize_spec_time_ns"] =\n      normal_submit_finalize_spec_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_add_pending_time_ns"] =\n      normal_submit_add_pending_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_owner_setup_time_ns"] =\n      normal_submit_owner_setup_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_dispatch_setup_time_ns"] =\n      normal_submit_dispatch_setup_time_ns_.load(std::memory_order_relaxed);\n  result["normal_submit_total_time_ns"] =\n      normal_submit_total_time_ns_.load(std::memory_order_relaxed);\n\n  if (!recovery_succession_profiling_enabled_ ||\n      recovery_succession_manager_ == nullptr) {\n    return result.dump();\n  }\n''')

replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''void CoreWorker::ResetRecoverySuccessionProfile() {\n  if (recovery_succession_manager_ != nullptr) {\n    recovery_succession_manager_->ResetProfile();\n  }\n}\n''',
    '''void CoreWorker::ResetRecoverySuccessionProfile() {\n  normal_submit_profile_calls_.store(0, std::memory_order_relaxed);\n  normal_submit_prebuild_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_build_common_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_finalize_spec_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_add_pending_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_owner_setup_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_dispatch_setup_time_ns_.store(0, std::memory_order_relaxed);\n  normal_submit_total_time_ns_.store(0, std::memory_order_relaxed);\n\n  if (recovery_succession_manager_ != nullptr) {\n    recovery_succession_manager_->ResetProfile();\n  }\n}\n''')

# Start total timer before any normal-task submission work.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''    const std::string &call_site,\n    const TaskID current_task_id) {\n  SubscribeToNodeChanges();\n''',
    '''    const std::string &call_site,\n    const TaskID current_task_id) {\n  const bool profile_normal_submit = recovery_succession_profiling_enabled_;\n  const uint64_t normal_submit_start_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  SubscribeToNodeChanges();\n''')

# Timestamp immediately before BuildCommonTaskSpec.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  // TODO(ekl) offload task building onto a thread pool for performance\n\n  BuildCommonTaskSpec(builder,\n''',
    '''  // TODO(ekl) offload task building onto a thread pool for performance\n\n  const uint64_t normal_submit_prebuild_done_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  BuildCommonTaskSpec(builder,\n''')

# Timestamp BuildCommonTaskSpec completion before normal-task-specific finalization.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''                      defer_recovery_frontier_dispatch\n                          ? &deferred_recovery_frontier_groups\n                          : nullptr);\n  ActorID root_detached_actor_id;\n''',
    '''                      defer_recovery_frontier_dispatch\n                          ? &deferred_recovery_frontier_groups\n                          : nullptr);\n  const uint64_t normal_submit_build_common_done_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  ActorID root_detached_actor_id;\n''')

# Timestamp completed TaskSpecification before TaskManager insertion.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();\n  RAY_LOG(DEBUG) << "Submitting normal task " << task_spec.DebugString();\n  std::vector<rpc::ObjectReference> returned_refs;\n  returned_refs = task_manager_->AddPendingTask(\n''',
    '''  TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();\n  RAY_LOG(DEBUG) << "Submitting normal task " << task_spec.DebugString();\n  const uint64_t normal_submit_finalize_done_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  std::vector<rpc::ObjectReference> returned_refs;\n  returned_refs = task_manager_->AddPendingTask(\n''')

# Timestamp TaskManager insertion completion.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  returned_refs = task_manager_->AddPendingTask(\n      task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);\n\n  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for\n''',
    '''  returned_refs = task_manager_->AddPendingTask(\n      task_spec.CallerAddress(), task_spec, CurrentCallSite(), max_retries);\n  const uint64_t normal_submit_add_pending_done_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  // Patch 4L: retain one correctness-preserving owner TaskSpec copy for\n''')

# Timestamp end of owner recovery setup just before dispatch/deferred-gate setup.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  if (defer_recovery_frontier_dispatch &&\n      !deferred_recovery_frontier_groups.empty()) {\n''',
    '''  const uint64_t normal_submit_owner_setup_done_ns =\n      profile_normal_submit ? RecoveryProfileNowNs() : 0;\n\n  if (defer_recovery_frontier_dispatch &&\n      !deferred_recovery_frontier_groups.empty()) {\n''')

# Record all stage deltas after dispatch/deferred setup and before returning refs.
replace_once(
    "src/ray/core_worker/core_worker.cc",
    '''  } else {\n    io_service_.post(\n        [this, task_spec = std::move(task_spec)]() mutable {\n          normal_task_submitter_->SubmitTask(std::move(task_spec));\n        },\n        "CoreWorker.SubmitTask");\n  }\n  return returned_refs;\n}\n\nStatus CoreWorker::CreateActor''',
    '''  } else {\n    io_service_.post(\n        [this, task_spec = std::move(task_spec)]() mutable {\n          normal_task_submitter_->SubmitTask(std::move(task_spec));\n        },\n        "CoreWorker.SubmitTask");\n  }\n\n  if (profile_normal_submit) {\n    const uint64_t normal_submit_end_ns = RecoveryProfileNowNs();\n    normal_submit_profile_calls_.fetch_add(1, std::memory_order_relaxed);\n    normal_submit_prebuild_time_ns_.fetch_add(\n        normal_submit_prebuild_done_ns - normal_submit_start_ns,\n        std::memory_order_relaxed);\n    normal_submit_build_common_time_ns_.fetch_add(\n        normal_submit_build_common_done_ns - normal_submit_prebuild_done_ns,\n        std::memory_order_relaxed);\n    normal_submit_finalize_spec_time_ns_.fetch_add(\n        normal_submit_finalize_done_ns - normal_submit_build_common_done_ns,\n        std::memory_order_relaxed);\n    normal_submit_add_pending_time_ns_.fetch_add(\n        normal_submit_add_pending_done_ns - normal_submit_finalize_done_ns,\n        std::memory_order_relaxed);\n    normal_submit_owner_setup_time_ns_.fetch_add(\n        normal_submit_owner_setup_done_ns - normal_submit_add_pending_done_ns,\n        std::memory_order_relaxed);\n    normal_submit_dispatch_setup_time_ns_.fetch_add(\n        normal_submit_end_ns - normal_submit_owner_setup_done_ns,\n        std::memory_order_relaxed);\n    normal_submit_total_time_ns_.fetch_add(\n        normal_submit_end_ns - normal_submit_start_ns,\n        std::memory_order_relaxed);\n  }\n\n  return returned_refs;\n}\n\nStatus CoreWorker::CreateActor''')

print("normal-task submit-stage profiler patch applied")
