from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one anchor, found {count}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# RayConfig: add correctness-capable frontier controls. Keep the old K-density
# knob explicitly performance-only and independent.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/common/ray_config_def.h",
    '''/// failure/correctness testing.\nRAY_CONFIG(uint32_t, recovery_baseline_perf_protect_every_n, 1)\n\nRAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)\n''',
    '''/// failure/correctness testing.\nRAY_CONFIG(uint32_t, recovery_baseline_perf_protect_every_n, 1)\n\n/// Enables correctness-capable Recovery Frontier grouping above the selected\n/// recovery backend. When enabled, eligible owner tasks are assigned to\n/// append-only frontier groups. The first task in each group is the protected\n/// leader; later members share that protection topology once their replay\n/// recipes are durably appended.\n///\n/// This is independent of recovery_baseline_perf_protect_every_n, which remains\n/// a performance-only proxy and must stay at 1 for correctness experiments.\nRAY_CONFIG(bool, enable_recovery_frontier, false)\n\n/// Maximum number of task replay recipes in one Recovery Frontier group.\n/// K=1 degenerates to ordinary per-task protection.\nRAY_CONFIG(uint32_t, recovery_frontier_group_size, 16)\n\nRAY_CONFIG(uint32_t, recovery_succession_target_holder_count, 2)\n''')


# ---------------------------------------------------------------------------
# Build graph: the recovery manager now owns the backend-neutral planner.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/BUILD.bazel",
    '''    visibility = [":__subpackages__"],\n    deps = [\n        "//src/ray/common:id",\n        "//src/ray/common:task_common",\n''',
    '''    visibility = [":__subpackages__"],\n    deps = [\n        ":recovery_frontier",\n        "//src/ray/common:id",\n        "//src/ray/common:task_common",\n''')


# ---------------------------------------------------------------------------
# RecoverySuccessionManager API/state.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''#include <functional>\n#include <map>\n#include <optional>\n''',
    '''#include <functional>\n#include <map>\n#include <memory>\n#include <optional>\n''')

replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''#include "ray/common/id.h"\n#include "ray/common/task/task_spec.h"\n#include "src/ray/protobuf/common.pb.h"\n''',
    '''#include "ray/common/id.h"\n#include "ray/common/task/task_spec.h"\n#include "ray/core_worker/recovery_frontier.h"\n#include "src/ray/protobuf/common.pb.h"\n''')

replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''  /// Returns true when recovery succession supports the task.\n  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);\n\n  /// Returns true only when a task actually carries Recovery Succession\n''',
    '''  /// Returns true when recovery succession supports the task.\n  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);\n\n  /// Returns whether owner-side correctness-capable Recovery Frontier\n  /// grouping is enabled for this manager.\n  bool RecoveryFrontierEnabled() const;\n\n  /// Assign an eligible owner task to its append-only frontier group. The\n  /// first member becomes the immediately protectable group leader.\n  std::optional<RecoveryFrontierMembership> RegisterOwnerTaskWithRecoveryFrontier(\n      const TaskSpecification &task_spec);\n\n  /// Return stable group coordinates for a previously registered owner task.\n  std::optional<RecoveryFrontierMembership> GetRecoveryFrontierMembership(\n      const TaskID &task_id) const;\n\n  /// Stage/commit/abort the next contiguous group append. These methods expose\n  /// the frontier acknowledged-prefix state machine to either protection\n  /// backend without coupling the planner to Baseline or Succession RPCs.\n  std::optional<RecoveryFrontierAppendBatch> StageRecoveryFrontierAppend(\n      const TaskID &group_id, uint32_t max_batch_members = 0);\n  bool CommitRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n  bool AbortRecoveryFrontierAppend(const RecoveryFrontierAppendBatch &batch);\n\n  /// Resolve a committed group-global return index back to the original task\n  /// replay recipe. Uncommitted members deliberately return false.\n  bool ExtractRecoveryFrontierTaskForReturn(const TaskID &group_id,\n                                            uint32_t group_return_index,\n                                            rpc::TaskSpec *task_spec,\n                                            uint32_t *task_return_index) const;\n\n  /// Returns true only when a task actually carries Recovery Succession\n''')

replace_once(
    "src/ray/core_worker/recovery_succession_manager.h",
    '''  mutable absl::Mutex mutex_;\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n''',
    '''  mutable absl::Mutex mutex_;\n\n  /// Backend-neutral owner-side grouping state. Null when Recovery Frontiers\n  /// are disabled. All access is serialized by the manager mutex so the\n  /// planner itself remains deliberately lock-free.\n  std::unique_ptr<RecoveryFrontierPlanner> recovery_frontier_planner_\n      ABSL_GUARDED_BY(mutex_);\n\n  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);\n''')


# ---------------------------------------------------------------------------
# Manager implementation: instantiate planner and expose its acknowledged-prefix
# state machine behind the existing manager mutex.
# ---------------------------------------------------------------------------
replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)\n    : self_address_(std::move(self_address)),\n      profiling_enabled_(\n          RayConfig::instance().enable_recovery_succession_profiling()) {}\n\nbool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {\n  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&\n         !task_spec.streaming_generator() && task_spec.max_retries() != 0;\n}\n''',
    '''RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)\n    : self_address_(std::move(self_address)),\n      profiling_enabled_(\n          RayConfig::instance().enable_recovery_succession_profiling()) {\n  if (RayConfig::instance().enable_recovery_frontier()) {\n    const uint32_t group_size =\n        RayConfig::instance().recovery_frontier_group_size();\n    RAY_CHECK_GT(group_size, 0U)\n        << "recovery_frontier_group_size must be positive";\n    recovery_frontier_planner_ =\n        std::make_unique<RecoveryFrontierPlanner>(group_size);\n  }\n}\n\nbool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {\n  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&\n         !task_spec.streaming_generator() && task_spec.max_retries() != 0;\n}\n\nbool RecoverySuccessionManager::RecoveryFrontierEnabled() const {\n  absl::MutexLock lock(&mutex_);\n  return recovery_frontier_planner_ != nullptr;\n}\n\nstd::optional<RecoveryFrontierMembership>\nRecoverySuccessionManager::RegisterOwnerTaskWithRecoveryFrontier(\n    const TaskSpecification &task_spec) {\n  const rpc::TaskSpec &task_proto = task_spec.GetMessage();\n  if (!IsEligibleTask(task_proto) || task_proto.task_id().empty() ||\n      task_spec.NumReturns() == 0) {\n    return std::nullopt;\n  }\n\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return std::nullopt;\n  }\n  return recovery_frontier_planner_->RegisterTask(task_proto);\n}\n\nstd::optional<RecoveryFrontierMembership>\nRecoverySuccessionManager::GetRecoveryFrontierMembership(\n    const TaskID &task_id) const {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return std::nullopt;\n  }\n  return recovery_frontier_planner_->FindTask(task_id);\n}\n\nstd::optional<RecoveryFrontierAppendBatch>\nRecoverySuccessionManager::StageRecoveryFrontierAppend(\n    const TaskID &group_id, uint32_t max_batch_members) {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return std::nullopt;\n  }\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(group_id);\n  return group == nullptr ? std::nullopt : group->StageAppend(max_batch_members);\n}\n\nbool RecoverySuccessionManager::CommitRecoveryFrontierAppend(\n    const RecoveryFrontierAppendBatch &batch) {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return false;\n  }\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(batch.group_id);\n  return group != nullptr && group->CommitAppend(batch);\n}\n\nbool RecoverySuccessionManager::AbortRecoveryFrontierAppend(\n    const RecoveryFrontierAppendBatch &batch) {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return false;\n  }\n  RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetMutableGroup(batch.group_id);\n  return group != nullptr && group->AbortAppend(batch);\n}\n\nbool RecoverySuccessionManager::ExtractRecoveryFrontierTaskForReturn(\n    const TaskID &group_id,\n    uint32_t group_return_index,\n    rpc::TaskSpec *task_spec,\n    uint32_t *task_return_index) const {\n  absl::MutexLock lock(&mutex_);\n  if (recovery_frontier_planner_ == nullptr) {\n    return false;\n  }\n  const RecoveryFrontierGroup *group =\n      recovery_frontier_planner_->GetGroup(group_id);\n  return group != nullptr &&\n         group->ExtractTaskForReturn(\n             group_return_index, task_spec, task_return_index);\n}\n''')

replace_once(
    "src/ray/core_worker/recovery_succession_manager.cc",
    '''  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());\n\n  const bool baseline_enabled =\n      RayConfig::instance().enable_recovery_witness_holder_baseline();\n''',
    '''  const TaskID task_id = TaskID::FromBinary(task_proto.task_id());\n\n  // Register every eligible live owner task with the shared frontier planner\n  // before any backend-specific activation/filtering. This is owner-local only:\n  // no holder, witness, manifest, or candidate RPC is emitted here.\n  if (RecoveryFrontierEnabled() && !returned_refs.empty()) {\n    static_cast<void>(RegisterOwnerTaskWithRecoveryFrontier(task_spec));\n  }\n\n  const bool baseline_enabled =\n      RayConfig::instance().enable_recovery_witness_holder_baseline();\n''')

print("Recovery Frontier manager integration applied successfully.")
