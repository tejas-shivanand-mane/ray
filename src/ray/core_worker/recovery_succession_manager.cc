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

#include <cstddef>
#include <utility>

namespace ray::core {

namespace {

/// Number of non-owner lineage holders requested by the initial implementation.
///
/// This becomes a Ray configuration option in a later phase.
constexpr uint32_t kDefaultTargetHolderCount = 2;

}  // namespace

RecoverySuccessionManager::RecoverySuccessionManager(rpc::Address self_address)
    : self_address_(std::move(self_address)) {}

bool RecoverySuccessionManager::IsEligibleTask(const rpc::TaskSpec &task_spec) {
  return task_spec.type() == rpc::TaskType::NORMAL_TASK && !task_spec.returns_dynamic() &&
         !task_spec.streaming_generator() && task_spec.max_retries() != 0;
}

rpc::RecoveryManifest RecoverySuccessionManager::BuildInitialManifest(
    const TaskID &task_id, const JobID &job_id, int32_t max_retries) const {
  rpc::RecoveryManifest manifest;

  manifest.set_task_id(task_id.Binary());
  manifest.set_job_id(job_id.Binary());
  manifest.set_target_holder_count(kDefaultTargetHolderCount);

  // Witnesses are selected in the witness phase.
  manifest.set_witness_quorum(0);

  // Generation 1 is the initial owner-created version.
  auto *version = manifest.mutable_version();
  version->set_generation(1);
  version->set_coordinator_rank(0);

  manifest.set_frozen(false);
  manifest.set_tombstoned(false);
  manifest.set_recovery_attempt(0);
  manifest.set_max_recovery_attempts(max_retries);

  // Rank 0 is always the original owner.
  auto *owner = manifest.add_succession();
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

    returned_ref.mutable_recovery_metadata()->CopyFrom(metadata);
  }
}

void RecoverySuccessionManager::RegisterExecutorTask(const rpc::TaskSpec &task_spec) {
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

  absl::MutexLock lock(&mutex_);

  for (const auto &[object_id, metadata] : received_metadata) {
    BorrowedObjectRecoveryState borrowed_state;
    borrowed_state.task_id = TaskID::FromBinary(metadata.task_id());
    borrowed_state.return_index = metadata.return_index();
    borrowed_state.cached_manifest.CopyFrom(metadata.manifest());

    borrowed_objects_[object_id] = std::move(borrowed_state);

    object_recovery_metadata_[object_id] = metadata;
  }

  if (!should_store_task) {
    return;
  }

  const TaskID task_id = TaskID::FromBinary(task_spec.task_id());

  TaskRecoveryState task_state;
  task_state.manifest.CopyFrom(task_spec.recovery_manifest());
  task_state.task_spec = task_spec;

  task_states_[task_id] = std::move(task_state);
}

void RecoverySuccessionManager::RegisterBorrowedObject(
    const ObjectID &object_id, const rpc::RecoveryObjectMetadata &metadata) {
  if (metadata.task_id().empty() || !metadata.has_manifest()) {
    return;
  }

  BorrowedObjectRecoveryState borrowed_state;
  borrowed_state.task_id = TaskID::FromBinary(metadata.task_id());
  borrowed_state.return_index = metadata.return_index();
  borrowed_state.cached_manifest.CopyFrom(metadata.manifest());

  absl::MutexLock lock(&mutex_);

  borrowed_objects_[object_id] = std::move(borrowed_state);

  object_recovery_metadata_[object_id] = metadata;
}

bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) const {
  if (metadata == nullptr) {
    return false;
  }

  absl::MutexLock lock(&mutex_);

  const auto it = object_recovery_metadata_.find(object_id);

  if (it == object_recovery_metadata_.end()) {
    return false;
  }

  metadata->CopyFrom(it->second);
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

bool RecoverySuccessionManager::TryRecoverObject(const ObjectID &object_id) {
  // Actual holder traversal and replay are added later.
  static_cast<void>(object_id);
  return false;
}

void RecoverySuccessionManager::HandleWorkerFailure(const WorkerID &worker_id) {
  // Failure-based coordinator takeover is added later.
  static_cast<void>(worker_id);
}

void RecoverySuccessionManager::HandleNodeFailure(const NodeID &node_id) {
  // Holder failure detection and repair are added later.
  static_cast<void>(node_id);
}

bool RecoverySuccessionManager::HasConfirmedHolderResponsibilities() const {
  absl::MutexLock lock(&mutex_);

  for (const auto &entry : task_states_) {
    const rpc::RecoveryManifest &manifest = entry.second.manifest;

    for (const rpc::RecoveryHolder &holder : manifest.succession()) {
      if (holder.rank() == 0) {
        continue;
      }

      if (holder.address().worker_id() == self_address_.worker_id()) {
        return true;
      }
    }
  }

  return false;
}

}  // namespace ray::core