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
#include <cstddef>
#include <utility>
#include <chrono>


namespace ray::core {

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

}  // namespace

RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)
    : self_address_(std::move(self_address)),
      profiling_enabled_(
          RayConfig::instance().enable_recovery_succession_profiling()) {}

bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
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
  task_state.task_spec = task_proto;
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

    const ObjectID object_id = ObjectID::FromBinary(returned_ref.object_id());

    object_recovery_metadata_[object_id] = metadata;
    task_object_ids_[task_id].insert(object_id);

    returned_ref.mutable_recovery_metadata()->CopyFrom(metadata);
  }
}

std::vector<RecoverySuccessionManager::CandidateReport>
RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
  std::vector<std::pair<ObjectID, rpc::RecoveryObjectMetadata>> received_metadata;

  auto collect_metadata = [&received_metadata](const rpc::ObjectReference &object_ref) {
    if (object_ref.object_id().empty() || !object_ref.has_recovery_metadata()) {
      return;
    }

    const rpc::RecoveryObjectMetadata &metadata = object_ref.recovery_metadata();

    if (metadata.task_id().empty() || !metadata.has_manifest()) {
      return;
    }

    received_metadata.emplace_back(ObjectID::FromBinary(object_ref.object_id()),
                                   metadata);
  };

  for (const rpc::TaskArg &arg : task_spec.args()) {
    if (arg.has_object_ref()) {
      collect_metadata(arg.object_ref());
    }

    for (const rpc::ObjectReference &nested_ref : arg.nested_inlined_refs()) {
      collect_metadata(nested_ref);
    }
  }

  const bool should_store_task = IsEligibleTask(task_spec) &&
                                 task_spec.has_recovery_manifest() &&
                                 !task_spec.task_id().empty();

  std::vector<CandidateReport> reports;

  absl::MutexLock lock(&mutex_);

  for (const auto &[object_id, metadata] : received_metadata) {
    rpc::RecoveryObjectMetadata effective_metadata;
    effective_metadata.CopyFrom(metadata);

    const auto existing_metadata = object_recovery_metadata_.find(object_id);

    if (existing_metadata != object_recovery_metadata_.end() &&
        CompareManifestVersions(existing_metadata->second.manifest(),
                                metadata.manifest()) > 0) {
      effective_metadata.CopyFrom(existing_metadata->second);
    }

    const TaskID metadata_task_id =
        TaskID::FromBinary(effective_metadata.task_id());

    // Do not recreate metadata for a task whose equal-or-newer tombstone
    // has already been applied locally.
    const auto tombstone_it = task_states_.find(metadata_task_id);

    if (tombstone_it != task_states_.end() &&
        tombstone_it->second.manifest.tombstoned() &&
        CompareManifestVersions(
            tombstone_it->second.manifest,
            effective_metadata.manifest()) >= 0) {
      continue;
    }

    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = metadata_task_id;
    borrowed_state.return_index = effective_metadata.return_index();

    borrowed_objects_[object_id] = std::move(borrowed_state);
    object_recovery_metadata_[object_id] = effective_metadata;
    task_object_ids_[metadata_task_id].insert(object_id);

  }

  if (should_store_task) {
    const TaskID task_id = TaskID::FromBinary(task_spec.task_id());

    const auto existing_task_it = task_states_.find(task_id);

    const bool stale_after_tombstone =
        existing_task_it != task_states_.end() &&
        existing_task_it->second.manifest.tombstoned() &&
        CompareManifestVersions(
            existing_task_it->second.manifest,
            task_spec.recovery_manifest()) >= 0;

    if (!stale_after_tombstone) {
      TaskRecoveryState &task_state = task_states_[task_id];

      if (task_state.manifest.task_id().empty() ||
          CompareManifestVersions(
              task_spec.recovery_manifest(),
              task_state.manifest) > 0) {
        task_state.manifest.CopyFrom(
            task_spec.recovery_manifest());
      }

      rpc::TaskSpec stored_task_spec;
      stored_task_spec.CopyFrom(task_spec);
      stored_task_spec.mutable_recovery_manifest()->CopyFrom(
          task_state.manifest);

      task_state.task_spec = std::move(stored_task_spec);

      // IMPORTANT:
      //
      // Do not admit the original task executor as a recovery
      // succession holder.
      //
      // Although the executor may run on a node distinct from the
      // task owner, Ray's worker lease is owned by the task submitter.
      // If that owner's node dies, NodeManager::NodeRemoved() kills
      // this leased executor as part of normal Ray cleanup.
      //
      // Therefore the executor is not failure-independent from the
      // original owner and cannot provide owner-failure tolerance.
      //
      // The TaskSpec may still be retained locally for normal task
      // bookkeeping, but holder admission must come from independent
      // downstream borrowers.
    }
  }


  for (const auto &[object_id, metadata] : received_metadata) {
    static_cast<void>(object_id);

    MaybeAddCandidateReportLocked(metadata.manifest(), false, &reports);
  }

  return reports;
}

void RecoverySuccessionManager::RegisterBorrowedObject(
    const ObjectID &object_id, const rpc::RecoveryObjectMetadata &metadata) {
  if (metadata.task_id().empty() || !metadata.has_manifest()) {
    return;
  }

  rpc::RecoveryObjectMetadata effective_metadata;
  effective_metadata.CopyFrom(metadata);

  absl::MutexLock lock(&mutex_);

  const auto existing_metadata = object_recovery_metadata_.find(object_id);

  if (existing_metadata != object_recovery_metadata_.end() &&
      CompareManifestVersions(existing_metadata->second.manifest(), metadata.manifest()) >
          0) {
    effective_metadata.CopyFrom(existing_metadata->second);
  }

  const TaskID task_id =
    TaskID::FromBinary(effective_metadata.task_id());

  const auto tombstone_it = task_states_.find(task_id);

  if (tombstone_it != task_states_.end() &&
      tombstone_it->second.manifest.tombstoned() &&
      CompareManifestVersions(
          tombstone_it->second.manifest,
          effective_metadata.manifest()) >= 0) {
    return;
  }

  BorrowedObjectRecoveryState borrowed_state;
  borrowed_state.task_id = task_id;
  borrowed_state.return_index = effective_metadata.return_index();

  borrowed_objects_[object_id] = std::move(borrowed_state);
  object_recovery_metadata_[object_id] = effective_metadata;
  task_object_ids_[task_id].insert(object_id);

}

rpc::ReportRecoveryCandidateReply::Result
RecoverySuccessionManager::PrepareHolderAdmission(
    const rpc::ReportRecoveryCandidateRequest &request,
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

  if (task_it == task_states_.end() || !task_it->second.task_spec.has_value()) {
    return rpc::ReportRecoveryCandidateReply::STALE_MANIFEST;
  }

  const TaskRecoveryState &task_state = task_it->second;

  latest_manifest->CopyFrom(task_state.manifest);

  if (task_state.manifest.tombstoned()) {
    return rpc::ReportRecoveryCandidateReply::TOMBSTONED;
  }

  // Only the original owner forms the frozen succession list.
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

  if (confirmed_non_owner_holders >= task_state.manifest.target_holder_count()) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  if (holder_reservation_by_task_.contains(task_id)) {
    return rpc::ReportRecoveryCandidateReply::NO_SLOT;
  }

  for (const rpc::RecoveryHolder &holder : task_state.manifest.succession()) {
    if (!holder.failure_domain_id().empty() &&
        holder.failure_domain_id() == candidate_address.node_id()) {
      return rpc::ReportRecoveryCandidateReply::NO_SLOT;
    }
  }

  rpc::RecoveryManifest proposed_manifest;
  proposed_manifest.CopyFrom(task_state.manifest);

  const uint32_t proposed_rank =
      static_cast<uint32_t>(proposed_manifest.succession_size());

  rpc::RecoveryHolder *new_holder = proposed_manifest.add_succession();
  new_holder->mutable_address()->CopyFrom(candidate_address);
  new_holder->set_rank(proposed_rank);
  new_holder->set_failure_domain_id(candidate_address.node_id());

  proposed_manifest.mutable_version()->set_generation(
      task_state.manifest.version().generation() + 1);

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

  holder_reservations_[reservation_id] = std::move(reservation);
  holder_reservation_by_task_[task_id] = reservation_id;

  plan->reservation_id = reservation_id;
  plan->candidate_address.CopyFrom(candidate_address);
  plan->candidate_already_stores_task_spec =
      request.already_stores_task_spec();

  if (!plan->candidate_already_stores_task_spec) {
  if (profiling_enabled_) {
    const auto copy_start =
        std::chrono::steady_clock::now();

    plan->task_spec.CopyFrom(
        task_it->second.task_spec.value());

    plan->task_spec.mutable_recovery_manifest()->CopyFrom(
        proposed_manifest);

    const auto copy_end =
        std::chrono::steady_clock::now();

    const uint64_t copy_ns =
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                copy_end - copy_start)
                .count());

    ++profile_.owner_task_spec_copy_count;
    profile_.owner_task_spec_copy_time_ns += copy_ns;
  } else {
    plan->task_spec.CopyFrom(
        task_it->second.task_spec.value());

    plan->task_spec.mutable_recovery_manifest()->CopyFrom(
        proposed_manifest);
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
  stored_task_spec.mutable_recovery_manifest()->CopyFrom(request.proposed_manifest());

  task_state.task_spec = std::move(stored_task_spec);
  task_state.manifest_committed = false;
  task_state.provisional_reservation_id = request.reservation_id();

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

  const TaskID task_id = reservation_it->second.task_id;

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    EraseHolderReservationLocked(reservation_id);
    return false;
  }

  UpdateManifestForTaskLocked(task_id, reservation_it->second.proposed_manifest, true);


  if (profiling_enabled_) {
    const rpc::RecoveryManifest &manifest =
        reservation_it->second.proposed_manifest;

    ++profile_.holder_admissions_committed;
    ++profile_.manifest_generations_committed;

    if (manifest.version().generation() >
        profile_.max_generation) {
      profile_.max_generation =
          manifest.version().generation();
    }

    const uint64_t non_owner_holders =
        manifest.succession_size() > 0
            ? static_cast<uint64_t>(
                  manifest.succession_size() - 1)
            : 0;

    if (non_owner_holders >
        profile_.max_non_owner_holders) {
      profile_.max_non_owner_holders =
          non_owner_holders;
    }

    if (manifest.frozen()) {
      ++profile_.frozen_commits;
    }
  }

  committed_manifest->CopyFrom(reservation_it->second.proposed_manifest);

  EraseHolderReservationLocked(reservation_id);

  return true;
}

void RecoverySuccessionManager::AbortHolderAdmission(const std::string &reservation_id) {
  if (reservation_id.empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);
  EraseHolderReservationLocked(reservation_id);

}


void RecoverySuccessionManager::EraseHolderReservationLocked(
    const std::string &reservation_id) {
  const auto reservation_it =
      holder_reservations_.find(reservation_id);

  if (reservation_it == holder_reservations_.end()) {
    return;
  }

  const TaskID task_id = reservation_it->second.task_id;

  const auto task_index_it =
      holder_reservation_by_task_.find(task_id);

  if (task_index_it != holder_reservation_by_task_.end() &&
      task_index_it->second == reservation_id) {
    holder_reservation_by_task_.erase(task_index_it);
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

bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto metadata_it = object_recovery_metadata_.find(object_id);

  if (metadata_it == object_recovery_metadata_.end()) {
    return false;
  }

  metadata->CopyFrom(metadata_it->second);

  return true;
}

void RecoverySuccessionManager::PopulateTaskArgumentMetadata(
    rpc::TaskSpec *task_spec) const {
  if (task_spec == nullptr) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  for (rpc::TaskArg &arg : *task_spec->mutable_args()) {
    if (arg.has_object_ref()) {
      rpc::ObjectReference *object_ref = arg.mutable_object_ref();

      if (!object_ref->object_id().empty()) {
        const ObjectID object_id = ObjectID::FromBinary(object_ref->object_id());

        const auto metadata_it = object_recovery_metadata_.find(object_id);

        if (metadata_it != object_recovery_metadata_.end()) {
          object_ref->mutable_recovery_metadata()->CopyFrom(metadata_it->second);
        }
      }
    }

    for (rpc::ObjectReference &nested_ref : *arg.mutable_nested_inlined_refs()) {
      if (nested_ref.object_id().empty()) {
        continue;
      }

      const ObjectID nested_id = ObjectID::FromBinary(nested_ref.object_id());

      const auto metadata_it = object_recovery_metadata_.find(nested_id);

      if (metadata_it != object_recovery_metadata_.end()) {
        nested_ref.mutable_recovery_metadata()->CopyFrom(metadata_it->second);
      }
    }
  }
}

void RecoverySuccessionManager::MaybeAddCandidateReportLocked(
    const rpc::RecoveryManifest &manifest,
    bool already_stores_task_spec,
    std::vector<CandidateReport> *reports) {

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

  task_state.manifest.CopyFrom(manifest);
  task_state.manifest_committed = committed;

  if (committed) {
    task_state.provisional_reservation_id.clear();
  }

  // Do not copy the new manifest into the stored TaskSpec here.
  // PrepareHolderAdmission and PrepareTaskReplay attach the current manifest
  // immediately before the TaskSpec is transferred or replayed.

  const auto object_ids_it = task_object_ids_.find(task_id);

  if (object_ids_it == task_object_ids_.end()) {
    return;
  }

  for (const ObjectID &object_id : object_ids_it->second) {
    const auto metadata_it =
        object_recovery_metadata_.find(object_id);

    if (metadata_it != object_recovery_metadata_.end()) {
      metadata_it->second.mutable_manifest()->CopyFrom(manifest);
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

  const auto metadata_it =
    object_recovery_metadata_.find(object_id);

  if (metadata_it == object_recovery_metadata_.end() ||
      !metadata_it->second.has_manifest()) {
    return false;
  }

  plan->task_id = borrowed_it->second.task_id;
  plan->return_index = borrowed_it->second.return_index;
  plan->cached_manifest.CopyFrom(
      metadata_it->second.manifest());

  return true;

}

RecoverySuccessionManager::ReplayPreparationResult
RecoverySuccessionManager::PrepareTaskReplay(const rpc::RecoverTaskOutputRequest &request,
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

  if (!state.task_spec.has_value() || !state.manifest_committed) {
    return ReplayPreparationResult::TASK_NOT_FOUND;
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

  state.task_spec->mutable_recovery_manifest()->CopyFrom(state.manifest);

  task_spec->CopyFrom(state.task_spec.value());
  latest_manifest->CopyFrom(state.manifest);

  return ReplayPreparationResult::READY;
}

void RecoverySuccessionManager::UpdateBorrowedObjectManifest(
    const ObjectID &object_id,
    const rpc::RecoveryManifest &manifest) {
  if (manifest.task_id().empty()) {
    return;
  }

  absl::MutexLock lock(&mutex_);

  const auto borrowed_it = borrowed_objects_.find(object_id);

  if (borrowed_it == borrowed_objects_.end()) {
    return;
  }

  auto metadata_it = object_recovery_metadata_.find(object_id);

  if (metadata_it == object_recovery_metadata_.end() ||
      !metadata_it->second.has_manifest() ||
      metadata_it->second.task_id() != manifest.task_id()) {
    return;
  }

  if (CompareManifestVersions(
          manifest,
          metadata_it->second.manifest()) < 0) {
    return;
  }

  metadata_it->second.mutable_manifest()->CopyFrom(manifest);
}

std::optional<rpc::RecoveryManifest> RecoverySuccessionManager::BuildTombstoneForTask(
    const TaskID &task_id) const {
  absl::MutexLock lock(&mutex_);

  const auto task_it = task_states_.find(task_id);

  if (task_it == task_states_.end()) {
    return std::nullopt;
  }

  const TaskRecoveryState &task_state = task_it->second;

  if (!task_state.manifest_committed || task_state.manifest.tombstoned() ||
      !task_state.task_spec.has_value()) {
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

  EraseTaskObjectMetadataLocked(task_id);

  const auto reservation_it =
      holder_reservation_by_task_.find(task_id);

  if (reservation_it != holder_reservation_by_task_.end()) {
    const std::string reservation_id = reservation_it->second;
    EraseHolderReservationLocked(reservation_id);
  }

  candidate_reports_sent_.erase(task_id);

  return true;
}

void RecoverySuccessionManager::EraseTaskObjectMetadataLocked(
    const TaskID &task_id) {
  const auto object_ids_it = task_object_ids_.find(task_id);

  if (object_ids_it == task_object_ids_.end()) {
    return;
  }

  for (const ObjectID &object_id : object_ids_it->second) {
    object_recovery_metadata_.erase(object_id);
    borrowed_objects_.erase(object_id);
  }

  task_object_ids_.erase(object_ids_it);
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

}  // namespace ray::core