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

#pragma once

#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray::core {

/// Stores the lineage and succession metadata used by the experimental
/// recovery-succession implementation.
///
/// Phase 1 only stores and propagates metadata. Actual remote holder
/// admission and recovery are added in later phases.
class RecoverySuccessionManager {
 public:
  explicit RecoverySuccessionManager(rpc::Address self_address);

  RecoverySuccessionManager(const RecoverySuccessionManager &) = delete;
  RecoverySuccessionManager &operator=(
      const RecoverySuccessionManager &) = delete;

  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
  rpc::RecoveryManifest BuildInitialManifest(
      const TaskID &task_id,
      const JobID &job_id,
      int32_t max_retries) const;

  /// Records a newly submitted task and attaches metadata to its return refs.
  void RegisterOwnedTask(
      const TaskSpecification &task_spec,
      std::vector<rpc::ObjectReference> *returned_refs);

  /// Records a task whose TaskSpec was received by this executor.
  void RegisterExecutorTask(const rpc::TaskSpec &task_spec);

  /// Records metadata for an object borrowed by this worker.
  void RegisterBorrowedObject(
      const ObjectID &object_id,
      const rpc::RecoveryObjectMetadata &metadata);

  /// Copies known recovery metadata into metadata.
  ///
  /// Returns false when this worker has no metadata for object_id.
  bool PopulateRecoveryMetadata(
      const ObjectID &object_id,
      rpc::RecoveryObjectMetadata *metadata) const;

  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec) const;

  /// Phase 1 placeholder. Actual recovery is implemented later.
  bool TryRecoverObject(const ObjectID &object_id);

  /// Phase 1 placeholders for failure processing.
  void HandleWorkerFailure(const WorkerID &worker_id);
  void HandleNodeFailure(const NodeID &node_id);

  /// Returns whether this worker is a confirmed non-owner lineage holder.
  bool HasConfirmedHolderResponsibilities() const;

 private:
  struct TaskRecoveryState {
    rpc::RecoveryManifest manifest;

    // Present on the owner and on workers that store the task lineage.
    std::optional<rpc::TaskSpec> task_spec;
  };

  struct BorrowedObjectRecoveryState {
    TaskID task_id;
    uint32_t return_index = 0;
    rpc::RecoveryManifest cached_manifest;
  };

  /// Address of this CoreWorker.
  rpc::Address self_address_;

  mutable absl::Mutex mutex_;

  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_
      ABSL_GUARDED_BY(mutex_);

  /// Recovery information for borrowed objects.
  absl::flat_hash_map<ObjectID, BorrowedObjectRecoveryState>
      borrowed_objects_ ABSL_GUARDED_BY(mutex_);

  /// Metadata that should be attached whenever an ObjectRef is serialized.
  absl::flat_hash_map<ObjectID, rpc::RecoveryObjectMetadata>
      object_recovery_metadata_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray::core