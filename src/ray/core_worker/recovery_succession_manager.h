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
#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray::core {

/// Stores lineage, succession, and holder-admission state for the
/// experimental recovery-succession implementation.
class RecoverySuccessionManager {
 public:
  struct CandidateReport {
    rpc::Address coordinator_address;
    rpc::ReportRecoveryCandidateRequest request;
  };

  struct HolderAdmissionPlan {
    std::string reservation_id;
    rpc::Address candidate_address;
    rpc::TaskSpec task_spec;
    rpc::RecoveryManifest proposed_manifest;
  };

  explicit RecoverySuccessionManager(rpc::Address self_address);

  RecoverySuccessionManager(const RecoverySuccessionManager &) = delete;
  RecoverySuccessionManager &operator=(const RecoverySuccessionManager &) = delete;

  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
  rpc::RecoveryManifest BuildInitialManifest(const TaskID &task_id,
                                             const JobID &job_id,
                                             int32_t max_retries) const;

  /// Records a newly submitted task and attaches metadata to its return refs.
  void RegisterOwnedTask(const TaskSpecification &task_spec,
                         std::vector<rpc::ObjectReference> *returned_refs);

  /// Records a received TaskSpec and returns candidate reports that should be
  /// sent to the coordinators of the received task and its dependencies.
  std::vector<CandidateReport> RegisterExecutorTask(const rpc::TaskSpec &task_spec);

  /// Records metadata for an object borrowed by this worker.
  void RegisterBorrowedObject(const ObjectID &object_id,
                              const rpc::RecoveryObjectMetadata &metadata);

  /// Prepares a provisional holder admission on the current coordinator.
  rpc::ReportRecoveryCandidateReply::Result PrepareHolderAdmission(
      const rpc::ReportRecoveryCandidateRequest &request,
      HolderAdmissionPlan *plan,
      rpc::RecoveryManifest *latest_manifest);

  /// Stores lineage provisionally on a candidate holder.
  bool InstallRecoveryHolder(const rpc::InstallRecoveryHolderRequest &request);

  /// Commits a previously reserved holder admission on the coordinator.
  bool CommitHolderAdmission(const std::string &reservation_id,
                             rpc::RecoveryManifest *committed_manifest);

  /// Removes a failed provisional reservation.
  void AbortHolderAdmission(const std::string &reservation_id);

  /// Applies a committed manifest received from the coordinator.
  bool ApplyCommittedManifest(const rpc::RecoveryManifest &manifest);

  /// Copies known recovery metadata into metadata.
  bool PopulateRecoveryMetadata(const ObjectID &object_id,
                                rpc::RecoveryObjectMetadata *metadata) const;

  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  void PopulateTaskArgumentMetadata(rpc::TaskSpec *task_spec) const;

  /// Actual recovery is implemented in Phase 5.
  bool TryRecoverObject(const ObjectID &object_id);

  /// Failure-based takeover and repair are implemented later.
  void HandleWorkerFailure(const WorkerID &worker_id);
  void HandleNodeFailure(const NodeID &node_id);

  /// Returns whether this worker is a committed non-owner lineage holder.
  bool HasConfirmedHolderResponsibilities() const;

 private:
  struct TaskRecoveryState {
    rpc::RecoveryManifest manifest;

    // Present on the owner, executor, and installed lineage holders.
    std::optional<rpc::TaskSpec> task_spec;

    // An installed holder is not usable until CommitRecoveryManifest arrives.
    bool manifest_committed = true;
    std::string provisional_reservation_id;
  };

  struct BorrowedObjectRecoveryState {
    TaskID task_id;
    uint32_t return_index = 0;
    rpc::RecoveryManifest cached_manifest;
  };

  struct HolderReservation {
    TaskID task_id;
    rpc::Address candidate_address;
    rpc::RecoveryManifest proposed_manifest;
  };

  void MaybeAddCandidateReportLocked(const rpc::RecoveryManifest &manifest,
                                     bool already_stores_task_spec,
                                     std::vector<CandidateReport> *reports)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void UpdateManifestForTaskLocked(const TaskID &task_id,
                                   const rpc::RecoveryManifest &manifest,
                                   bool committed) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /// Address of this CoreWorker.
  rpc::Address self_address_;

  mutable absl::Mutex mutex_;

  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_ ABSL_GUARDED_BY(mutex_);

  /// Recovery information for borrowed objects.
  absl::flat_hash_map<ObjectID, BorrowedObjectRecoveryState> borrowed_objects_
      ABSL_GUARDED_BY(mutex_);

  /// Metadata attached whenever an ObjectRef is serialized.
  absl::flat_hash_map<ObjectID, rpc::RecoveryObjectMetadata> object_recovery_metadata_
      ABSL_GUARDED_BY(mutex_);

  /// Provisional owner-side holder reservations.
  absl::flat_hash_map<std::string, HolderReservation> holder_reservations_
      ABSL_GUARDED_BY(mutex_);

  /// Prevents repeated reports for the same producer task.
  absl::flat_hash_set<TaskID> candidate_reports_sent_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray::core