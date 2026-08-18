// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/recovery_succession_manager.h"
#include "ray/common/ray_config.h"
#include "absl/cleanup/cleanup.h"
#include <cstddef>
#include <utility>
#include <chrono>


namespace ray::core {

// Patch 4D: pipelined holder admission.
// Patch 4F: first-holder TaskSpec piggyback.
// Patch 4G: hot-path profiling and B1 ablations.
// Patch 4H: compact task-argument recovery metadata.
// Patch 4I: TaskSpec-level recovery argument sidecar.
// Patch 4J: task-centric recovery state.
// Patch 4K: full mode uses async holder install; no H1 TaskSpec piggyback.

namespace {


const rpc::RecoveryHolder *FindHolderByRank(const rpc::RecoveryManifest &manifest,
                                            uint32_t rank) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == rank) {
      return &holder;
    }
  }

  return nullptr;
}

bool SameWorker(const rpc::Address &left, const rpc::Address &right) {
  return !left.worker_id().empty() && left.worker_id() == right.worker_id();
}

bool ContainsWorker(const rpc::RecoveryManifest &manifest, const rpc::Address &address) {
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (SameWorker(holder.address(), address)) {
      return true;
    }
  }

  return false;
}

int CompareManifestVersions(const rpc::RecoveryManifest &left,
                            const rpc::RecoveryManifest &right) {
  if (left.version().generation() < right.version().generation()) {
    return -1;
  }

  if (left.version().generation() > right.version().generation()) {
    return 1;
  }

  return 0;
}


// Patch 4F transport sidecars must never become part of retained/replayed
// lineage, otherwise a downstream TaskSpec could recursively contain upstream
// full TaskSpecs.
void ClearFirstHolderTaskSpecPiggybacks(rpc::TaskSpec *task_spec) {
  if (task_spec == nullptr) {
    return;
  }

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && arg.object_ref().has_recovery_metadata()) {
      arg.mutable_object_ref()
          ->mutable_recovery_metadata()
          ->clear_first_holder_task_spec();
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.has_recovery_metadata()) {
        nested_ref.mutable_recovery_metadata()->clear_first_holder_task_spec();
      }
    }
  }

  // Patch 4I sidecars are part of the downstream TaskSpec's dependency
  // recovery description and therefore must survive replay. Only the nested
  // full-lineage piggyback is transport-only and must be stripped.
  for (rpc::RecoveryTaskArgumentMetadata &entry :
       *task_spec->mutable_recovery_argument_metadata()) {
    if (entry.has_recovery_metadata()) {
      entry.mutable_recovery_metadata()->clear_first_holder_task_spec();
    }
  }
}


// Patch 4H sender-side compact encoding. This is intentionally restricted to
// TaskSpec argument transport; internal recovery state still stores a complete
// RecoveryManifest. If the standard ObjectReference owner cannot reproduce the
// manifest's rank-0 owner exactly enough for recovery, return false and the
// caller falls back to the old full metadata representation.
bool WriteCompactTaskArgumentRecoveryMetadata(
    const rpc::RecoveryObjectMetadata &source,
    const rpc::RecoveryManifest &manifest,
    const rpc::Address &object_owner,
    rpc::RecoveryObjectMetadata *out) {
  if (out == nullptr || source.task_id().empty() || manifest.task_id().empty() ||
      source.task_id() != manifest.task_id() || !manifest.has_version()) {
    return false;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(manifest, 0);
  if (owner == nullptr || object_owner.worker_id().empty() ||
      !SameWorker(owner->address(), object_owner)) {
    return false;
  }

  out->Clear();
  out->set_return_index(source.return_index());

  rpc::RecoveryObjectTransportManifest *compact = out->mutable_compact_manifest();
  compact->set_target_holder_count(manifest.target_holder_count());
  compact->set_witness_count(manifest.witness_count());
  compact->set_generation(manifest.version().generation());
  compact->set_frozen(manifest.frozen());
  compact->set_tombstoned(manifest.tombstoned());
  compact->set_recovery_attempt(manifest.recovery_attempt());
  compact->set_max_recovery_attempts(manifest.max_recovery_attempts());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    compact->add_witness_raylets()->CopyFrom(witness);
  }

  // Ranks are fixed and contiguous in Recovery Succession. Rank 0 is already
  // carried by ObjectReference.owner_address, so only H1..HR are transported.
  for (const rpc::RecoveryHolder &holder : manifest.succession()) {
    if (holder.rank() == 0) {
      continue;
    }
    if (holder.rank() !=
        static_cast<uint32_t>(compact->non_owner_holders_size() + 1)) {
      out->Clear();
      return false;
    }
    compact->add_non_owner_holders()->CopyFrom(holder.address());
  }

  return true;
}

// Patch 4H receiver-side expansion. Existing admission, witness confirmation,
// replay, tombstone, and rollback code continues to consume the ordinary full
// RecoveryManifest, so the compact representation never escapes this boundary.
bool ExpandTaskArgumentRecoveryMetadata(
    const rpc::ObjectReference &object_ref,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || object_ref.object_id().empty() ||
      !object_ref.has_recovery_metadata()) {
    return false;
  }

  const rpc::RecoveryObjectMetadata &transport = object_ref.recovery_metadata();

  // Backward-compatible/fallback path.
  if (!transport.task_id().empty() && transport.has_manifest()) {
    expanded->CopyFrom(transport);
    expanded->clear_compact_manifest();
    return true;
  }

  if (!transport.has_compact_manifest() || !object_ref.has_owner_address() ||
      object_ref.owner_address().worker_id().empty() ||
      object_ref.object_id().size() != ObjectID::Size()) {
    return false;
  }

  const rpc::RecoveryObjectTransportManifest &compact =
      transport.compact_manifest();
  if (compact.generation() == 0) {
    return false;
  }

  const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
  const TaskID task_id = object_id.TaskId();

  expanded->Clear();
  expanded->set_task_id(task_id.Binary());
  expanded->set_return_index(transport.return_index());
  if (!transport.first_holder_task_spec().empty()) {
    expanded->set_first_holder_task_spec(transport.first_holder_task_spec());
  }

  rpc::RecoveryManifest *manifest = expanded->mutable_manifest();
  manifest->set_task_id(task_id.Binary());
  manifest->set_job_id(task_id.JobId().Binary());
  manifest->set_target_holder_count(compact.target_holder_count());
  manifest->set_witness_count(compact.witness_count());
  manifest->mutable_version()->set_generation(compact.generation());
  manifest->set_frozen(compact.frozen());
  manifest->set_tombstoned(compact.tombstoned());
  manifest->set_recovery_attempt(compact.recovery_attempt());
  manifest->set_max_recovery_attempts(compact.max_recovery_attempts());

  for (const rpc::Address &witness : compact.witness_raylets()) {
    manifest->add_witness_raylets()->CopyFrom(witness);
  }

  rpc::RecoveryHolder *owner = manifest->add_succession();
  owner->mutable_address()->CopyFrom(object_ref.owner_address());
  owner->set_rank(0);
  owner->set_failure_domain_id(object_ref.owner_address().node_id());

  for (int i = 0; i < compact.non_owner_holders_size(); ++i) {
    const rpc::Address &address = compact.non_owner_holders(i);
    if (address.worker_id().empty()) {
      expanded->Clear();
      return false;
    }
    rpc::RecoveryHolder *holder = manifest->add_succession();
    holder->mutable_address()->CopyFrom(address);
    holder->set_rank(static_cast<uint32_t>(i + 1));
    holder->set_failure_domain_id(address.node_id());
  }

  return true;
}


// Patch 4I TaskSpec-level sidecar expansion. Reuse the Patch-4H expansion
// logic through a local synthetic ObjectReference so all downstream manager
// state continues to see the exact ordinary RecoveryObjectMetadata shape.
bool ExpandTaskSidecarRecoveryMetadata(
    const rpc::RecoveryTaskArgumentMetadata &entry,
    rpc::RecoveryObjectMetadata *expanded) {
  if (expanded == nullptr || entry.object_id().empty() ||
      !entry.has_recovery_metadata()) {
    return false;
  }

  rpc::ObjectReference synthetic_ref;
  synthetic_ref.set_object_id(entry.object_id());
  if (entry.has_owner_address()) {
    synthetic_ref.mutable_owner_address()->CopyFrom(entry.owner_address());
  }
  synthetic_ref.mutable_recovery_metadata()->CopyFrom(entry.recovery_metadata());
  return ExpandTaskArgumentRecoveryMetadata(synthetic_ref, expanded);
}

const std::string &RecoveryBenchmarkAblationMode() {
  static const std::string mode =
      RayConfig::instance().recovery_succession_benchmark_ablation_mode();
  RAY_CHECK(mode == "full" || mode == "no_piggyback" ||
            mode == "metadata_only" || mode == "piggyback_no_candidate" ||
            mode == "candidate_rpc_no_admit")
      << "Unknown recovery_succession_benchmark_ablation_mode=" << mode;
  return mode;
}

}  // namespace

RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)
    : self_address_(std::move(self_address)),
      profiling_enabled_(
          RayConfig::instance().enable_recovery_succession_profiling()) {}

bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
}

bool RecoverySuccessionManager::CarriesRecoveryMetadata(
    const rpc::TaskSpec &task_spec) {
  if (task_spec.has_recovery_manifest() ||
      task_spec.recovery_argument_metadata_size() > 0) {
    return true;
  }

  // Backward compatibility for TaskSpecs created by pre-4I workers/tests.
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
    const TaskID &task_id, const JobID &job_id, int32_t max_retries) const {
  rpc::RecoveryManifest manifest;

  manifest.set_task_id(task_id.Binary());
  manifest.set_job_id(job_id.Binary());
  const uint32_t target_holder_count =
      RayConfig::instance().recovery_succession_target_holder_count();

  RAY_CHECK_GT(target_holder_count, 0U);

  manifest.set_target_holder_count(target_holder_count);

  manifest.set_witness_count(0);

  rpc::RecoveryManifestVersion *version = manifest.mutable_version();
  version->set_generation(1);

  manifest.set_frozen(false);
  manifest.set_tombstoned(false);
  manifest.set_recovery_attempt(0);
  manifest.set_max_recovery_attempts(max_retries);

  rpc::RecoveryHolder *owner = manifest.add_succession();
  owner->mutable_address()->CopyFrom(self_address_);
  owner->set_rank(0);
  owner->set_failure_domain_id(self_address_.node_id());

  return manifest;
}


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

std::vector<RecoverySuccessionManager::CandidateReport>
RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
  const auto patch4g_start = std::chrono::steady_clock::now();
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;
  absl::flat_hash_set<ObjectID> received_object_ids;

  auto append_metadata = [&received_metadata, &received_object_ids](
                             const ObjectID &object_id,
                             rpc::RecoveryObjectMetadata metadata) {
    if (!received_object_ids.insert(object_id).second) {
      return;
    }
    received_metadata.emplace_back(object_id, std::move(metadata));
  };

  // Patch 4I primary path: one TaskSpec-level sidecar per unique dependency.
  for (const rpc::RecoveryTaskArgumentMetadata &entry :
       task_spec.recovery_argument_metadata()) {
    if (entry.object_id().size() != ObjectID::Size()) {
      continue;
    }

    rpc::RecoveryObjectMetadata metadata;
    if (!ExpandTaskSidecarRecoveryMetadata(entry, &metadata)) {
      continue;
    }

    append_metadata(ObjectID::FromBinary(entry.object_id()), std::move(metadata));
  }

  // Backward-compatible path for pre-4I TaskSpecs. A TaskSpec-level entry wins
  // if both representations are present for the same ObjectID.
  auto collect_legacy_metadata =
      [&received_object_ids, &append_metadata](const rpc::ObjectReference &object_ref) {
        if (object_ref.object_id().empty() || !object_ref.has_recovery_metadata() ||
            object_ref.object_id().size() != ObjectID::Size()) {
          return;
        }

        const ObjectID object_id = ObjectID::FromBinary(object_ref.object_id());
        if (received_object_ids.contains(object_id)) {
          return;
        }

        rpc::RecoveryObjectMetadata metadata;
        if (!ExpandTaskArgumentRecoveryMetadata(object_ref, &metadata)) {
          return;
        }

        append_metadata(object_id, std::move(metadata));
      };

  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref()) {
      collect_legacy_metadata(arg.object_ref());
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      collect_legacy_metadata(nested_ref);
    }
  }

  const bool should_store_task = IsEligibleTask(task_spec) &&
                                 task_spec.has_recovery_manifest() &&
                                 !task_spec.task_id().empty();

  std::vector<CandidateReport> reports;
  absl::flat_hash_set<TaskID> piggyback_task_ids;

  absl::MutexLock lock(&mutex_);

  for (const auto &[object_id, metadata] : received_metadata) {
    // Parse the Patch-4F transport sidecar before normal metadata selection.
    // The sidecar itself is never retained in object_recovery_metadata_.
    rpc::TaskSpec piggyback_task_spec;
    bool valid_piggyback = false;

    if (!metadata.first_holder_task_spec().empty()) {
      valid_piggyback =
          piggyback_task_spec.ParseFromString(metadata.first_holder_task_spec()) &&
          !piggyback_task_spec.task_id().empty() &&
          piggyback_task_spec.task_id() == metadata.task_id() &&
          IsEligibleTask(piggyback_task_spec);

      if (valid_piggyback) {
        ClearFirstHolderTaskSpecPiggybacks(&piggyback_task_spec);
      } else {
        RAY_LOG(DEBUG)
            << "Ignoring invalid Patch 4F first-holder TaskSpec piggyback";
      }
    }

    rpc::RecoveryObjectMetadata effective_metadata;
    effective_metadata.CopyFrom(metadata);

    const auto existing_metadata = object_recovery_metadata_.find(object_id);

    if (existing_metadata != object_recovery_metadata_.end() &&
        CompareManifestVersions(existing_metadata->second.manifest(),
                                metadata.manifest()) > 0) {
      effective_metadata.CopyFrom(existing_metadata->second);
    }

    // Transport only: do not recursively forward full TaskSpecs.
    effective_metadata.clear_first_holder_task_spec();

    const TaskID metadata_task_id =
        TaskID::FromBinary(effective_metadata.task_id());

    const auto tombstone_it = task_states_.find(metadata_task_id);
    if (tombstone_it != task_states_.end() &&
        tombstone_it->second.manifest.tombstoned() &&
        CompareManifestVersions(tombstone_it->second.manifest,
                                effective_metadata.manifest()) >= 0) {
      continue;
    }

    BorrowedObjectRecoveryState borrowed_state;
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
  }

  if (should_store_task) {
    const TaskID task_id = TaskID::FromBinary(task_spec.task_id());

    const auto existing_task_it = task_states_.find(task_id);

    const bool stale_after_tombstone =
        existing_task_it != task_states_.end() &&
        existing_task_it->second.manifest.tombstoned() &&
        CompareManifestVersions(existing_task_it->second.manifest,
                                task_spec.recovery_manifest()) >= 0;

    if (!stale_after_tombstone) {
      TaskRecoveryState &task_state = task_states_[task_id];

      if (task_state.manifest.task_id().empty() ||
          CompareManifestVersions(task_spec.recovery_manifest(),
                                  task_state.manifest) > 0) {
        task_state.manifest.CopyFrom(task_spec.recovery_manifest());
      }

      rpc::TaskSpec stored_task_spec;
      stored_task_spec.CopyFrom(task_spec);
      ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
      stored_task_spec.mutable_recovery_manifest()->CopyFrom(task_state.manifest);

      task_state.task_spec = std::move(stored_task_spec);

      // IMPORTANT: the original executor is not admitted as a durable recovery
      // holder. Holder admission still comes only from downstream borrowers.
    }
  }

  for (const auto &[object_id, metadata] : received_metadata) {
    static_cast<void>(object_id);

    const TaskID metadata_task_id = TaskID::FromBinary(metadata.task_id());
    MaybeAddCandidateReportLocked(
        metadata.manifest(),
        piggyback_task_ids.contains(metadata_task_id),
        &reports);
  }

  if (profiling_enabled_) {
    ++profile_.register_executor_task_calls;
    profile_.register_executor_task_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    profile_.register_executor_metadata_refs_seen +=
        static_cast<uint64_t>(received_metadata.size());
    profile_.register_executor_candidate_reports_built +=
        static_cast<uint64_t>(reports.size());
  }

  return reports;
}


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

rpc::ReportRecoveryCandidateReply::Result
RecoverySuccessionManager::PrepareHolderAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
    const rpc::TaskSpec *owner_task_spec,
    HolderAdmissionPlan *plan,
    rpc::RecoveryManifest *latest_manifest) {
  if (plan == nullptr || latest_manifest == nullptr || request.task_id().empty() ||
      !request.has_candidate_address() || !request.has_cached_manifest()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  *plan = HolderAdmissionPlan();
  latest_manifest->Clear();

  const rpc::Address &candidate_address = request.candidate_address();

  if (candidate_address.worker_id().empty() || candidate_address.node_id().empty() ||
      candidate_address.ip_address().empty() || candidate_address.port() <= 0) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);
  if (task_it == task_states_.end()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  const TaskRecoveryState &task_state = task_it->second;
  latest_manifest->CopyFrom(task_state.manifest);

  if (task_state.manifest.tombstoned()) {
    return rpc::ReportRecoveryCandidateReply::TOMBSTONED;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(task_state.manifest, 0);
  if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
    return rpc::ReportRecoveryCandidateReply::WRONG_COORDINATOR;
  }

  if (request.cached_manifest().version().generation() >
      task_state.manifest.version().generation()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  if (ContainsWorker(task_state.manifest, candidate_address)) {
    return rpc::ReportRecoveryCandidateReply::ACCEPTED;
  }

  if (task_state.manifest.frozen()) {
    return rpc::ReportRecoveryCandidateReply::FROZEN;
  }

  const uint32_t confirmed_non_owner_holders =
      task_state.manifest.succession_size() > 0
          ? static_cast<uint32_t>(task_state.manifest.succession_size() - 1)
          : 0;

  const auto per_task_it = holder_reservation_by_task_.find(task_id);
  const size_t pending_count =
      per_task_it == holder_reservation_by_task_.end()
          ? 0
          : per_task_it->second.size();

  if (confirmed_non_owner_holders + pending_count >=
      task_state.manifest.target_holder_count()) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  // Patch 4D: a task may have several provisional reservations. Reject only
  // duplicate/failure-domain candidates; do not reject merely because another
  // rank is currently being installed.
  for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
    if (!holder.failure_domain_id().empty() &&
        holder.failure_domain_id() == candidate_address.node_id()) {
      return rpc::ReportRecoveryCandidateReply::NO_SLOT;
    }
  }

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      static_cast<void>(rank);
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      if (SameWorker(pending.candidate_address, candidate_address)) {
        // The original report RPC for this candidate is still responsible for
        // completing the admission. Treat duplicate reports as already accepted.
        return rpc::ReportRecoveryCandidateReply::ACCEPTED;
      }

      if (!pending.candidate_address.node_id().empty() &&
          pending.candidate_address.node_id() == candidate_address.node_id()) {
        return rpc::ReportRecoveryCandidateReply::NO_SLOT;
      }
    }
  }

  // Construct the speculative prefix from the committed manifest plus all
  // earlier reservations. Every proposed manifest is therefore contiguous:
  // [A,H1], [A,H1,H2], ... even while H1..HR installations overlap.
  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(task_state.manifest);

  if (per_task_it != holder_reservation_by_task_.end()) {
    for (const auto &[rank, existing_reservation_id] : per_task_it->second) {
      const auto reservation_it = holder_reservations_.find(existing_reservation_id);
      if (reservation_it == holder_reservations_.end()) {
        continue;
      }

      const HolderReservation &pending = reservation_it->second;
      rpc::RecoveryHolder *holder = proposed_manifest.add_succession();
      holder->mutable_address()->CopyFrom(pending.candidate_address);
      holder->set_rank(rank);
      holder->set_failure_domain_id(pending.candidate_address.node_id());
    }
  }

  const uint32_t proposed_rank =
      static_cast<uint32_t>(proposed_manifest.succession_size());

  rpc::RecoveryHolder *new_holder = proposed_manifest.add_succession();
  new_holder->mutable_address()->CopyFrom(candidate_address);
  new_holder->set_rank(proposed_rank);
  new_holder->set_failure_domain_id(candidate_address.node_id());

  proposed_manifest.mutable_version()->set_generation(
      task_state.manifest.version().generation() + pending_count + 1);

  const uint32_t holders_after_admission =
      static_cast<uint32_t>(proposed_manifest.succession_size() - 1);
  if (holders_after_admission >= proposed_manifest.target_holder_count()) {
    proposed_manifest.set_frozen(true);
  }

  const std::string reservation_id = UniqueID::FromRandom().Binary();

  HolderReservation reservation;
  reservation.task_id = task_id;
  reservation.candidate_address.CopyFrom(candidate_address);
  reservation.proposed_manifest.CopyFrom(proposed_manifest);
  reservation.proposed_rank = proposed_rank;

  holder_reservations_[reservation_id] = std::move(reservation);
  holder_reservation_by_task_[task_id][proposed_rank] = reservation_id;

  plan->reservation_id = reservation_id;
  plan->candidate_address.CopyFrom(candidate_address);
  plan->candidate_already_stores_task_spec = request.already_stores_task_spec();

  if (!plan->candidate_already_stores_task_spec) {
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

  plan->proposed_manifest.CopyFrom(proposed_manifest);
  return rpc::ReportRecoveryCandidateReply::ACCEPTED;
}

bool RecoverySuccessionManager::InstallRecoveryHolder(
    const rpc::InstallRecoveryHolderRequest &request) {
  if (request.task_id().empty() || request.reservation_id().empty() ||
      !request.has_task_spec() || !request.has_proposed_manifest() ||
      request.task_spec().task_id() != request.task_id() ||
      request.proposed_manifest().task_id() != request.task_id() ||
      !IsEligibleTask(request.task_spec())) {
    return false;
  }

  const rpc::RecoveryHolder *proposed_holder =
      FindHolderByRank(request.proposed_manifest(), request.proposed_rank());

  if (proposed_holder == nullptr ||
      !SameWorker(proposed_holder->address(), self_address_) ||
      proposed_holder->failure_domain_id() != self_address_.node_id()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  TaskRecoveryState &task_state = task_states_[task_id];

  if (!task_state.manifest.task_id().empty()) {
    const int comparison =
        CompareManifestVersions(task_state.manifest, request.proposed_manifest());

    if (comparison > 0) {
      return false;
    }

    if (comparison == 0 && task_state.manifest_committed &&
        ContainsWorker(task_state.manifest, self_address_)) {
      return true;
    }
  }

  task_state.manifest.CopyFrom(request.proposed_manifest());

  rpc::TaskSpec stored_task_spec;
  stored_task_spec.CopyFrom(request.task_spec());
  ClearFirstHolderTaskSpecPiggybacks(&stored_task_spec);
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(request.proposed_manifest());

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = false;
  task_state.provisional_reservation_id = request.reservation_id();
  task_state.provisional_piggyback_task_spec = false;

  candidate_reports_sent_.insert(task_id);

  return true;
}

bool RecoverySuccessionManager::CommitHolderAdmission(
    const std::string &reservation_id, rpc::RecoveryManifest *committed_manifest) {
  if (reservation_id.empty() || committed_manifest == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return false;
  }

  const HolderReservation &reservation = reservation_it->second;
  const TaskID task_id = reservation.task_id;
  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    EraseHolderReservationLocked(reservation_id);
    return false;
  }

  const rpc::RecoveryManifest &current = task_it->second.manifest;
  const rpc::RecoveryManifest &proposed = reservation.proposed_manifest;

  // Patch 4D: only the next contiguous rank may become durable. Install RPCs
  // may complete in any order, but commits must remain H1,H2,...
  if (reservation.proposed_rank !=
          static_cast<uint32_t>(current.succession_size()) ||
      proposed.succession_size() != current.succession_size() + 1 ||
      proposed.version().generation() != current.version().generation() + 1) {
    return false;
  }

  for (int index = 0; index < current.succession_size(); ++index) {
    if (current.succession(index).SerializeAsString() !=
        proposed.succession(index).SerializeAsString()) {
      return false;
    }
  }

  UpdateManifestForTaskLocked(task_id, proposed, true);

  if (profiling_enabled_) {
    ++profile_.holder_admissions_committed;
    ++profile_.manifest_generations_committed;

    if (proposed.version().generation() > profile_.max_generation) {
      profile_.max_generation = proposed.version().generation();
    }

    const uint64_t non_owner_holders =
        proposed.succession_size() > 0
            ? static_cast<uint64_t>(proposed.succession_size() - 1)
            : 0;

    if (non_owner_holders > profile_.max_non_owner_holders) {
      profile_.max_non_owner_holders = non_owner_holders;
    }

    if (proposed.frozen()) {
      ++profile_.frozen_commits;
    }
  }

  committed_manifest->CopyFrom(proposed);
  EraseHolderReservationLocked(reservation_id);
  return true;
}

void RecoverySuccessionManager::AbortHolderAdmission(
    const std::string &reservation_id) {
  if (reservation_id.empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t failed_rank = reservation_it->second.proposed_rank;

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it == holder_reservation_by_task_.end()) {
    holder_reservations_.erase(reservation_it);
    return;
  }

  // Patch 4D conservative failure rule: a missing lower rank invalidates every
  // speculative suffix reservation because their proposed manifests include it.
  std::vector<std::string> suffix;
  for (auto it = task_index_it->second.lower_bound(failed_rank);
       it != task_index_it->second.end(); ++it) {
    suffix.push_back(it->second);
  }

  for (const std::string &id : suffix) {
    EraseHolderReservationLocked(id);
  }
}

void RecoverySuccessionManager::AllowCandidateReportRetry(
    const TaskID &task_id) {
  if (task_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  candidate_reports_sent_.erase(task_id);
}


void RecoverySuccessionManager::EraseHolderReservationLocked(
    const std::string &reservation_id) {
  const auto reservation_it = holder_reservations_.find(reservation_id);
  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;
  const uint32_t rank = reservation_it->second.proposed_rank;

  const auto task_index_it = holder_reservation_by_task_.find(task_id);
  if (task_index_it != holder_reservation_by_task_.end()) {
    auto rank_it = task_index_it->second.find(rank);
    if (rank_it != task_index_it->second.end() && rank_it->second == reservation_id) {
      task_index_it->second.erase(rank_it);
    }
    if (task_index_it->second.empty()) {
      holder_reservation_by_task_.erase(task_index_it);
    }
  }

  holder_reservations_.erase(reservation_it);
}



bool RecoverySuccessionManager::ApplyCommittedManifest(
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(manifest.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty()) {
    const int comparison = CompareManifestVersions(manifest, task_it->second.manifest);

    if (comparison < 0) {
      // Patch 4D failure-only rollback. A speculative higher-rank holder may
      // have installed a future manifest before a lower rank failed. The
      // coordinator cleans that candidate up by sending the last committed
      // prefix through the existing CommitRecoveryManifest RPC. Accept this
      // older prefix only for an uncommitted provisional holder that is NOT a
      // member of the committed prefix.
      if (!task_it->second.manifest_committed &&
          !ContainsWorker(manifest, self_address_)) {
        UpdateManifestForTaskLocked(task_id, manifest, true);
        candidate_reports_sent_.erase(task_id);
        return true;
      }
      return false;
    }

    if (comparison == 0 &&
        manifest.SerializeAsString() != task_it->second.manifest.SerializeAsString()) {
      return false;
    }
  }

  UpdateManifestForTaskLocked(task_id, manifest, true);

  if (ContainsWorker(manifest, self_address_)) {
    candidate_reports_sent_.insert(task_id);
  }

  return true;
}


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


bool RecoverySuccessionManager::HasRecoveryMetadata(
    const ObjectID &object_id) const {
  absl::MutexLock lock(&mutex_);
  return BuildRecoveryMetadataLocked(object_id, nullptr);
}


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

void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec,
    const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs) {
  if (task_spec == nullptr) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  // This field is transport-only. Rebuilding it from manager state also makes
  // this method idempotent if task construction revisits the same TaskSpec.
  task_spec->clear_recovery_argument_metadata();
  absl::flat_hash_set<ObjectID> attached_object_ids;

  auto populate_one = [this, task_spec, &attached_object_ids, owner_task_specs](
                          const ObjectID &object_id,
                          rpc::ObjectReference *object_ref) {
    if (object_ref == nullptr || object_id.IsNil()) {
      return;
    }

    // A legacy/pre-4I ObjectRef may already carry recovery metadata. Save it
    // as a compatibility fallback, then make the ObjectReference ordinary on
    // the TaskSpec wire path.
    rpc::RecoveryObjectMetadata legacy_transport;
    const bool had_legacy_transport = object_ref->has_recovery_metadata();
    if (had_legacy_transport) {
      legacy_transport.CopyFrom(object_ref->recovery_metadata());
    }
    object_ref->clear_recovery_metadata();

    // One sidecar per unique dependency even if the same ObjectRef appears in
    // multiple direct/nested argument positions.
    if (attached_object_ids.contains(object_id)) {
      return;
    }

    rpc::RecoveryObjectMetadata source_storage;
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

    if (source == nullptr || source->task_id().empty() || !source->has_manifest()) {
      return;
    }

    rpc::RecoveryTaskArgumentMetadata *entry =
        task_spec->add_recovery_argument_metadata();
    entry->set_object_id(object_id.Binary());
    if (object_ref->has_owner_address()) {
      entry->mutable_owner_address()->CopyFrom(object_ref->owner_address());
    }

    rpc::RecoveryObjectMetadata *out = entry->mutable_recovery_metadata();
    bool compact_transport = false;

    // Keep witness-as-holder baseline semantics and representation unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    } else if (entry->has_owner_address()) {
      compact_transport = WriteCompactTaskArgumentRecoveryMetadata(
          *source, source->manifest(), entry->owner_address(), out);
      if (!compact_transport) {
        out->CopyFrom(*source);
        out->clear_first_holder_task_spec();
        out->clear_compact_manifest();
      }
    } else {
      // Safety fallback: a full manifest does not need owner reconstruction.
      out->CopyFrom(*source);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }

    attached_object_ids.insert(object_id);

    if (profiling_enabled_) {
      ++profile_.task_argument_metadata_refs_attached;
      profile_.task_argument_metadata_full_bytes_equivalent +=
          static_cast<uint64_t>(source->ByteSizeLong());
      profile_.task_argument_metadata_transport_bytes +=
          static_cast<uint64_t>(out->ByteSizeLong());
      if (compact_transport) {
        ++profile_.task_argument_metadata_compact_refs;
      } else if (!RayConfig::instance().enable_recovery_witness_holder_baseline()) {
        ++profile_.task_argument_metadata_compact_fallbacks;
      }
    }

    // Keep the witness-as-holder baseline unchanged.
    if (RayConfig::instance().enable_recovery_witness_holder_baseline()) {
      return;
    }

    // Patch 4G benchmark ablations that isolate compact metadata and/or the
    // candidate RPC must not put the full TaskSpec on PushTask. no_piggyback
    // recreates the pre-4F H1 transport while keeping full admission semantics.
    const std::string &patch4g_mode = RecoveryBenchmarkAblationMode();
    if (patch4g_mode == "metadata_only" ||
        patch4g_mode == "candidate_rpc_no_admit" ||
        patch4g_mode == "no_piggyback" ||
        patch4g_mode == "full") {
      return;
    }

    const TaskID producer_task_id = TaskID::FromBinary(source->task_id());
    const auto task_it = task_states_.find(producer_task_id);
    if (task_it == task_states_.end()) {
      return;
    }

    TaskRecoveryState &state = task_it->second;

    // Claim exactly one full-lineage piggyback while the committed succession
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

    // Pair the piggybacked lineage with the manager's exact current manifest.
    // If compact encoding cannot faithfully reproduce rank 0, retain the old
    // full-manifest fallback rather than weakening recovery semantics.
    if (!entry->has_owner_address() ||
        !WriteCompactTaskArgumentRecoveryMetadata(
            *source, state.manifest, entry->owner_address(), out)) {
      out->CopyFrom(*source);
      out->mutable_manifest()->CopyFrom(state.manifest);
      out->clear_first_holder_task_spec();
      out->clear_compact_manifest();
    }

    const auto serialize_start = std::chrono::steady_clock::now();
    std::string serialized_task_spec;
    rpc::TaskSpec piggyback_lineage;
    piggyback_lineage.CopyFrom(*lineage_task_spec);
    ClearFirstHolderTaskSpecPiggybacks(&piggyback_lineage);
    piggyback_lineage.mutable_recovery_manifest()->CopyFrom(state.manifest);
    const bool ok = piggyback_lineage.SerializeToString(&serialized_task_spec);
    const auto serialize_end = std::chrono::steady_clock::now();

    if (!ok || serialized_task_spec.empty()) {
      return;
    }

    out->set_first_holder_task_spec(serialized_task_spec);
    state.first_holder_piggyback_sent = true;

    if (profiling_enabled_) {
      ++profile_.first_holder_piggyback_copies_sent;
      profile_.first_holder_piggyback_bytes_sent +=
          static_cast<uint64_t>(serialized_task_spec.size());
      profile_.task_spec_bytes_sent +=
          static_cast<uint64_t>(serialized_task_spec.size());
      profile_.first_holder_piggyback_serialize_time_ns +=
          static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  serialize_end - serialize_start)
                  .count());
    }
  };

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref() && !arg.object_ref().object_id().empty() &&
        arg.object_ref().object_id().size() == ObjectID::Size()) {
      populate_one(ObjectID::FromBinary(arg.object_ref().object_id()),
                   arg.mutable_object_ref());
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.object_id().empty() ||
          nested_ref.object_id().size() != ObjectID::Size()) {
        continue;
      }
      populate_one(ObjectID::FromBinary(nested_ref.object_id()), &nested_ref);
    }
  }
}

void RecoverySuccessionManager::MaybeAddCandidateReportLocked(
    const rpc::RecoveryManifest &manifest,
    bool already_stores_task_spec,
    std::vector<CandidateReport> *reports) {
  const auto patch4g_start = std::chrono::steady_clock::now();
  const size_t patch4g_reports_before = reports == nullptr ? 0 : reports->size();
  absl::Cleanup patch4g_profile = [this, patch4g_start, patch4g_reports_before, reports] {
    if (!profiling_enabled_) {
      return;
    }
    ++profile_.candidate_report_build_calls;
    profile_.candidate_report_build_time_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - patch4g_start)
            .count());
    if (reports != nullptr && reports->size() > patch4g_reports_before) {
      profile_.candidate_reports_built +=
          static_cast<uint64_t>(reports->size() - patch4g_reports_before);
    }
  };

  const std::string &patch4g_mode = RecoveryBenchmarkAblationMode();
  if (patch4g_mode == "metadata_only" ||
      patch4g_mode == "piggyback_no_candidate") {
    return;
  }

  if (RayConfig::instance().enable_recovery_succession() &&
      RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    return;
  }

  if (reports == nullptr || manifest.task_id().empty()) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(manifest.task_id());

  if (candidate_reports_sent_.contains(task_id)) {
    return;
  }

  rpc::RecoveryManifest effective_manifest;
  effective_manifest.CopyFrom(manifest);

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty() &&
      CompareManifestVersions(task_it->second.manifest, effective_manifest) > 0) {
    effective_manifest.CopyFrom(task_it->second.manifest);
  }

  if (effective_manifest.frozen() || effective_manifest.tombstoned() ||
      ContainsWorker(effective_manifest, self_address_)) {
    return;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(effective_manifest, 0);

  if (owner == nullptr || SameWorker(owner->address(), self_address_)) {
    return;
  }

  CandidateReport report;
  report.coordinator_address.CopyFrom(owner->address());

  report.request.set_task_id(effective_manifest.task_id());
  report.request.mutable_candidate_address()->CopyFrom(self_address_);
  report.request.mutable_cached_manifest()->CopyFrom(effective_manifest);
  report.request.set_already_stores_task_spec(already_stores_task_spec);

  reports->push_back(std::move(report));

  candidate_reports_sent_.insert(task_id);
}

void RecoverySuccessionManager::UpdateManifestForTaskLocked(
    const TaskID &task_id,
    const rpc::RecoveryManifest &manifest,
    bool committed) {
  TaskRecoveryState &task_state = task_states_[task_id];

  const bool discard_unadmitted_piggyback =
      committed &&
      task_state.provisional_piggyback_task_spec &&
      !ContainsWorker(manifest, self_address_);

  task_state.manifest.CopyFrom(manifest);
  task_state.manifest_committed = committed;

  if (committed) {
    task_state.provisional_reservation_id.clear();

    if (task_state.provisional_piggyback_task_spec) {
      task_state.provisional_piggyback_task_spec = false;

      if (discard_unadmitted_piggyback) {
        // Rollback/rejection must not leave an orphaned full TaskSpec that can
        // later be mistaken for committed recovery lineage.
        task_state.task_spec.reset();
        candidate_reports_sent_.erase(task_id);
      }
    }
  }

  // Do not copy the new manifest into the stored TaskSpec here.
  // PrepareHolderAdmission and PrepareTaskReplay attach the current manifest
  // immediately before the TaskSpec is transferred or replayed.

  const auto object_ids_it = task_object_ids_.find(task_id);

  if (object_ids_it == task_object_ids_.end()) {
    return;
  }

  for (const ObjectID &object_id : object_ids_it->second) {
    const auto metadata_it = object_recovery_metadata_.find(object_id);

    if (metadata_it != object_recovery_metadata_.end()) {
      metadata_it->second.mutable_manifest()->CopyFrom(manifest);
      metadata_it->second.clear_first_holder_task_spec();
    }
  }
}


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

RecoverySuccessionManager::ReplayPreparationResult
RecoverySuccessionManager::PrepareTaskReplay(
    const rpc::RecoverTaskOutputRequest &request,
    const rpc::TaskSpec *owner_task_spec,
    rpc::TaskSpec *task_spec,
    rpc::RecoveryManifest *latest_manifest) {
  if (task_spec == nullptr ||
    latest_manifest == nullptr ||
    request.task_id().size() != TaskID::Size() ||
    !request.has_requester_manifest() ||
    request.requester_manifest().task_id() !=
        request.task_id()) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  TaskRecoveryState &state = task_it->second;
  latest_manifest->CopyFrom(state.manifest);

  if (state.manifest.tombstoned()) {
    return ReplayPreparationResult::TOMBSTONED;
  }

  const rpc::TaskSpec *lineage_task_spec =
      state.task_spec.has_value() ? &state.task_spec.value() : owner_task_spec;
  if (lineage_task_spec == nullptr ||
      lineage_task_spec->task_id() != task_id.Binary() ||
      !IsEligibleTask(*lineage_task_spec)) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
  }

  if (!state.manifest_committed) {
    // Both an installed holder and a Patch-4F piggyback holder remain
    // non-replayable until this worker independently verifies witnesses.
    const bool installed_provisional =
        !state.provisional_reservation_id.empty() &&
        ContainsWorker(state.manifest, self_address_);
    const bool piggyback_provisional =
        state.provisional_piggyback_task_spec;

    if (!installed_provisional && !piggyback_provisional) {
      return ReplayPreparationResult::TASK_NOT_FOUND;
    }

    return ReplayPreparationResult::WITNESS_CONFIRMATION_REQUIRED;
  }

    const int requester_comparison =
      CompareManifestVersions(
          request.requester_manifest(),
          state.manifest);

  if (requester_comparison < 0) {
    return ReplayPreparationResult::MANIFEST_STALE;
  }

  if (requester_comparison == 0 &&
      request.requester_manifest().SerializeAsString() !=
          state.manifest.SerializeAsString()) {
    return ReplayPreparationResult::MANIFEST_STALE;
  }

  if (requester_comparison > 0) {
    if (request.requester_manifest().tombstoned()) {
      latest_manifest->CopyFrom(
          request.requester_manifest());

      return ReplayPreparationResult::TOMBSTONED;
    }

    // A newer manifest may be accepted only if this worker remains a
    // member of the committed succession list.
    if (!ContainsWorker(
            request.requester_manifest(),
            self_address_)) {
      return ReplayPreparationResult::WRONG_HOLDER;
    }

    state.manifest.CopyFrom(
        request.requester_manifest());

    state.manifest_committed = true;

    latest_manifest->CopyFrom(
        state.manifest);
  }

  const int32_t max_recovery_attempts = state.manifest.max_recovery_attempts();

  if (max_recovery_attempts >= 0 &&
      state.manifest.recovery_attempt() >= static_cast<uint32_t>(max_recovery_attempts)) {
    return ReplayPreparationResult::RETRY_LIMIT_EXCEEDED;
  }

  bool self_is_holder = false;

  for (const rpc::RecoveryHolder &holder : state.manifest.succession()) {
    if (SameWorker(holder.address(), self_address_)) {
      self_is_holder = true;
      break;
    }
  }

  if (!self_is_holder) {
    return ReplayPreparationResult::WRONG_HOLDER;
  }

  state.manifest.set_recovery_attempt(state.manifest.recovery_attempt() + 1);

  task_spec->CopyFrom(*lineage_task_spec);
  ClearFirstHolderTaskSpecPiggybacks(task_spec);
  task_spec->mutable_recovery_manifest()->CopyFrom(state.manifest);
  latest_manifest->CopyFrom(state.manifest);

  return ReplayPreparationResult::READY;
}



bool RecoverySuccessionManager::ConfirmProvisionalHolderFromWitness(
    const rpc::RecoveryManifest &witness_manifest,
    rpc::RecoveryManifest *confirmed_manifest) {
  if (confirmed_manifest == nullptr ||
      witness_manifest.task_id().size() != TaskID::Size() ||
      !witness_manifest.has_version() ||
      witness_manifest.tombstoned()) {
    return false;
  }

  const TaskID task_id =
      TaskID::FromBinary(witness_manifest.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end() ||
      !task_it->second.task_spec.has_value()) {
    return false;
  }

  TaskRecoveryState &state = task_it->second;

  if (state.manifest.task_id() != witness_manifest.task_id() ||
      state.manifest.tombstoned() ||
      !ContainsWorker(witness_manifest, self_address_)) {
    return false;
  }

  const bool installed_provisional =
      !state.provisional_reservation_id.empty() &&
      ContainsWorker(state.manifest, self_address_);
  const bool piggyback_provisional =
      state.provisional_piggyback_task_spec;

  // A normal committed holder must already appear in its local manifest.
  // Patch 4F is intentionally different only while provisional: H1 initially
  // has [A] locally, and may promote only if a directly fetched witness
  // manifest contains this worker.
  if (state.manifest_committed &&
      !ContainsWorker(state.manifest, self_address_)) {
    return false;
  }

  if (!state.manifest_committed &&
      !installed_provisional &&
      !piggyback_provisional) {
    return false;
  }

  const int comparison =
      CompareManifestVersions(witness_manifest, state.manifest);

  if (comparison < 0) {
    if (!state.manifest_committed) {
      return false;
    }

    confirmed_manifest->CopyFrom(state.manifest);
    return true;
  }

  if (comparison == 0 &&
      witness_manifest.SerializeAsString() != state.manifest.SerializeAsString()) {
    return false;
  }

  if (comparison > 0 || !state.manifest_committed) {
    UpdateManifestForTaskLocked(task_id, witness_manifest, true);
  }

  candidate_reports_sent_.insert(task_id);
  confirmed_manifest->CopyFrom(state.manifest);
  return true;
}


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

std::optional<rpc::RecoveryManifest> RecoverySuccessionManager::BuildTombstoneForTask(
    const TaskID &task_id) const {
  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    return std::nullopt;
  }

  const TaskRecoveryState &task_state = task_it->second;

  if (!task_state.manifest_committed || task_state.manifest.tombstoned()) {
    return std::nullopt;
  }

  const rpc::RecoveryHolder *owner = FindHolderByRank(task_state.manifest, 0);

  if (owner == nullptr || !SameWorker(owner->address(), self_address_)) {
    return std::nullopt;
  }

  rpc::RecoveryManifest tombstone;
  tombstone.CopyFrom(task_state.manifest);

  tombstone.set_tombstoned(true);
  tombstone.set_frozen(true);

  rpc::RecoveryManifestVersion *version = tombstone.mutable_version();

  version->set_generation(task_state.manifest.version().generation() + 1);

  return tombstone;
}

bool RecoverySuccessionManager::ApplyRecoveryTombstone(
    const rpc::RecoveryManifest &tombstone) {
  if (tombstone.task_id().size() != TaskID::Size() || !tombstone.has_version() ||
      !tombstone.tombstoned()) {
    return false;
  }

  const TaskID task_id = TaskID::FromBinary(tombstone.task_id());

  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it != task_states_.end() && !task_it->second.manifest.task_id().empty()) {
    const int comparison = CompareManifestVersions(tombstone, task_it->second.manifest);

    if (comparison < 0) {
      return false;
    }

    if (comparison == 0 &&
        tombstone.SerializeAsString() != task_it->second.manifest.SerializeAsString()) {
      return false;
    }
  }

  TaskRecoveryState &state = task_states_[task_id];
  state.manifest.CopyFrom(tombstone);
  state.manifest_committed = true;
  state.task_spec.reset();
  state.provisional_reservation_id.clear();
  state.provisional_piggyback_task_spec = false;

  EraseTaskObjectMetadataLocked(task_id);

  const auto reservation_it = holder_reservation_by_task_.find(task_id);
  if (reservation_it != holder_reservation_by_task_.end()) {
    std::vector<std::string> reservation_ids;
    reservation_ids.reserve(reservation_it->second.size());
    for (const auto &[rank, reservation_id] : reservation_it->second) {
      static_cast<void>(rank);
      reservation_ids.push_back(reservation_id);
    }
    for (const std::string &reservation_id : reservation_ids) {
      EraseHolderReservationLocked(reservation_id);
    }
  }

  candidate_reports_sent_.erase(task_id);

  return true;
}


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
      const auto erase_it = it++;
      borrowed_objects_.erase(erase_it);
    } else {
      ++it;
    }
  }
}


void RecoverySuccessionManager::HandleWorkerFailure(const WorkerID &worker_id) {
  if (worker_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  failed_workers_.insert(worker_id);
}

void RecoverySuccessionManager::HandleNodeFailure(const NodeID &node_id) {
  if (node_id.IsNil()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  failed_nodes_.insert(node_id);
}

bool RecoverySuccessionManager::IsRecoveryHolderKnownFailed(
    const rpc::RecoveryHolder &holder) const {
  const rpc::Address &address = holder.address();

  absl::MutexLock lock(&mutex_);

  if (address.worker_id().size() == WorkerID::Size() &&
      failed_workers_.contains(WorkerID::FromBinary(address.worker_id()))) {
    return true;
  }

  if (address.node_id().size() == NodeID::Size() &&
      failed_nodes_.contains(NodeID::FromBinary(address.node_id()))) {
    return true;
  }

  return false;
}

bool RecoverySuccessionManager::HasConfirmedHolderResponsibilities() const {
  absl::MutexLock lock(&mutex_);

  for (const auto &[task_id, task_state] : task_states_) {
    static_cast<void>(task_id);

    if (!task_state.manifest_committed || !task_state.task_spec.has_value()) {
      continue;
    }

    for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
      if (holder.rank() == 0) {
        continue;
      }

      if (SameWorker(holder.address(), self_address_)) {
        return true;
      }
    }
  }

  return false;
}

RecoverySuccessionManager::RecoverySuccessionProfile
RecoverySuccessionManager::GetProfileSnapshot() const {
  absl::MutexLock lock(&mutex_);
  return profile_;
}

void RecoverySuccessionManager::ResetProfile() {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  profile_ = RecoverySuccessionProfile{};
}

void RecoverySuccessionManager::RecordCandidateReport(bool accepted) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.candidate_reports_received;

  if (accepted) {
    ++profile_.candidate_reports_accepted;
  }
}

void RecoverySuccessionManager::RecordHolderInstallRpcSent(
    uint64_t task_spec_bytes,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.holder_install_rpcs_sent;
  profile_.task_spec_bytes_sent += task_spec_bytes;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordHolderInstallRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.holder_install_rpcs_completed;
  profile_.holder_install_rpc_time_ns += latency_ns;
}


void RecoverySuccessionManager::RecordOwnerTaskSpecCopyLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.owner_task_spec_copy_count;
  profile_.owner_task_spec_copy_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessUpdateRpcSent(
    uint64_t task_spec_bytes,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.witness_update_rpcs_sent;
  profile_.task_spec_bytes_sent += task_spec_bytes;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordWitnessUpdateRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_update_rpcs_completed;
  profile_.witness_update_rpc_time_ns += latency_ns;
}


void RecoverySuccessionManager::RecordWitnessPublishLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_publish_count;
  profile_.witness_publish_time_ns += latency_ns;

  if (latency_ns > profile_.witness_publish_max_time_ns) {
    profile_.witness_publish_max_time_ns = latency_ns;
  }
}




void RecoverySuccessionManager::RecordHolderCommitRpcSent(
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  ++profile_.holder_commit_rpcs_sent;
  profile_.manifest_bytes_sent += manifest_bytes;
}

void RecoverySuccessionManager::RecordHolderCommitRpcLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.holder_commit_rpcs_completed;
  profile_.holder_commit_rpc_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordHolderAdmissionLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  profile_.holder_admission_time_ns += latency_ns;

  if (latency_ns > profile_.holder_admission_max_time_ns) {
    profile_.holder_admission_max_time_ns = latency_ns;
  }
}


void RecoverySuccessionManager::RecordTaskArgumentMetadataLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.task_argument_metadata_calls;
  profile_.task_argument_metadata_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordInitialManifestBuild(
    uint64_t latency_ns,
    uint64_t manifest_bytes) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.initial_manifest_build_count;
  profile_.initial_manifest_build_time_ns += latency_ns;
  profile_.initial_manifest_bytes += manifest_bytes;
}

void RecoverySuccessionManager::RecordWitnessSelectionLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_selection_count;
  profile_.witness_selection_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordWitnessGcsQueryLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.witness_gcs_query_count;
  profile_.witness_gcs_query_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordTaskSpecManifestAttachLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.task_spec_manifest_attach_count;
  profile_.task_spec_manifest_attach_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordRegisterOwnedTaskLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  ++profile_.register_owned_task_count;
  profile_.register_owned_task_time_ns += latency_ns;
}


void RecoverySuccessionManager::RecordEnsureTaskArgumentsLatency(
    uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.ensure_task_arguments_calls;
  profile_.ensure_task_arguments_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateQueueLatency(uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  ++profile_.candidate_queue_calls;
  profile_.candidate_queue_time_ns += latency_ns;
}

void RecoverySuccessionManager::RecordCandidateRpcSent(
    uint64_t logical_reports, uint64_t request_bytes) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_sent += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_sent;
  profile_.candidate_rpc_request_bytes_sent += request_bytes;
}

void RecoverySuccessionManager::RecordCandidateRpcLatency(
    uint64_t logical_reports, uint64_t latency_ns) {
  if (!profiling_enabled_) {
    return;
  }
  absl::MutexLock lock(&mutex_);
  profile_.candidate_rpc_logical_reports_completed += logical_reports;
  ++profile_.candidate_rpc_physical_rpcs_completed;
  profile_.candidate_rpc_time_ns += latency_ns;
}













}  // namespace ray::core