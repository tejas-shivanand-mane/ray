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
#include <functional>
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

    // True when the candidate is the executor of the original task and
    // RegisterExecutorTask has already retained the complete TaskSpec.
    bool candidate_already_stores_task_spec = false;
  };


  struct RecoverySuccessionProfile {
    uint64_t candidate_reports_received = 0;
    uint64_t candidate_reports_accepted = 0;

    uint64_t holder_install_rpcs_sent = 0;
    uint64_t holder_install_rpcs_completed = 0;

    uint64_t holder_commit_rpcs_sent = 0;
    uint64_t holder_commit_rpcs_completed = 0;

    uint64_t witness_update_rpcs_sent = 0;
    uint64_t witness_update_rpcs_completed = 0;

    // Wall-clock latency of the whole witness-publication stage.
    // This is different from witness_update_rpc_time_ns, which sums
    // the RTTs of individual witness RPCs.
    uint64_t witness_publish_count = 0;
    uint64_t witness_publish_time_ns = 0;
    uint64_t witness_publish_max_time_ns = 0;

    uint64_t task_spec_bytes_sent = 0;
    uint64_t manifest_bytes_sent = 0;

    uint64_t owner_task_spec_copy_count = 0;
    uint64_t owner_task_spec_copy_time_ns = 0;

    uint64_t holder_install_rpc_time_ns = 0;
    uint64_t holder_commit_rpc_time_ns = 0;
    uint64_t witness_update_rpc_time_ns = 0;

    uint64_t holder_admissions_committed = 0;
    uint64_t holder_admission_time_ns = 0;
    uint64_t holder_admission_max_time_ns = 0;

    uint64_t manifest_generations_committed = 0;
    uint64_t max_generation = 0;
    uint64_t max_non_owner_holders = 0;
    uint64_t frozen_commits = 0;
  };


  RecoverySuccessionProfile GetProfileSnapshot() const;

  void ResetProfile();

  void RecordCandidateReport(bool accepted);

  void RecordHolderInstallRpcSent(uint64_t task_spec_bytes,
                                  uint64_t manifest_bytes);

  void RecordHolderInstallRpcLatency(uint64_t latency_ns);

  void RecordOwnerTaskSpecCopyLatency(uint64_t latency_ns);

  void RecordWitnessUpdateRpcSent(uint64_t task_spec_bytes,
                                  uint64_t manifest_bytes);

  void RecordWitnessUpdateRpcLatency(uint64_t latency_ns);

  void RecordWitnessPublishLatency(uint64_t latency_ns);

  void RecordHolderCommitRpcSent(uint64_t manifest_bytes);

  void RecordHolderCommitRpcLatency(uint64_t latency_ns);

  void RecordHolderAdmissionLatency(uint64_t latency_ns);






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

  struct BorrowedObjectRecoveryPlan {
    TaskID task_id;
    uint32_t return_index = 0;
    rpc::RecoveryManifest cached_manifest;
  };

  enum class ReplayPreparationResult {
    READY,
    TASK_NOT_FOUND,
    MANIFEST_STALE,
    TOMBSTONED,
    RETRY_LIMIT_EXCEEDED,
    WRONG_HOLDER,
  };

  bool GetBorrowedObjectRecoveryPlan(const ObjectID &object_id,
                                     BorrowedObjectRecoveryPlan *plan) const;

  ReplayPreparationResult PrepareTaskReplay(const rpc::RecoverTaskOutputRequest &request,
                                            rpc::TaskSpec *task_spec,
                                            rpc::RecoveryManifest *latest_manifest);

  void UpdateBorrowedObjectManifest(const ObjectID &object_id,
                                    const rpc::RecoveryManifest &manifest);

  std::optional<rpc::RecoveryManifest> BuildTombstoneForTask(const TaskID &task_id) const;

  /// Applies a tombstone and removes retained lineage and object metadata.
  bool ApplyRecoveryTombstone(const rpc::RecoveryManifest &tombstone);

  /// Records definitive Ray failure notifications.
  void HandleWorkerFailure(const WorkerID &worker_id);
  void HandleNodeFailure(const NodeID &node_id);

  /// Returns true when the holder's worker or node is known to be dead.
  bool IsRecoveryHolderKnownFailed(const rpc::RecoveryHolder &holder) const;

  /// Returns whether this worker is a committed non-owner lineage holder.
  bool HasConfirmedHolderResponsibilities() const;

 private:

  void EraseHolderReservationLocked(const std::string &reservation_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void EraseTaskObjectMetadataLocked(const TaskID &task_id)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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


  const bool profiling_enabled_;

  mutable absl::Mutex mutex_;

  RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);

  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_ ABSL_GUARDED_BY(mutex_);

  /// Recovery information for borrowed objects.
  absl::flat_hash_map<ObjectID, BorrowedObjectRecoveryState> borrowed_objects_
      ABSL_GUARDED_BY(mutex_);

  /// Metadata attached whenever an ObjectRef is serialized.
  absl::flat_hash_map<ObjectID, rpc::RecoveryObjectMetadata> object_recovery_metadata_
      ABSL_GUARDED_BY(mutex_);

  /// Object IDs carrying recovery metadata, indexed by producer task.
  /// This avoids scanning every tracked object when one task's manifest changes
  /// or its tombstone is applied.
  absl::flat_hash_map<TaskID, absl::flat_hash_set<ObjectID>> task_object_ids_
      ABSL_GUARDED_BY(mutex_);


  /// Provisional owner-side holder reservations.
  absl::flat_hash_map<std::string, HolderReservation> holder_reservations_
      ABSL_GUARDED_BY(mutex_);

  /// At most one provisional holder reservation is permitted per task.
  absl::flat_hash_map<TaskID, std::string> holder_reservation_by_task_
      ABSL_GUARDED_BY(mutex_);

  /// Prevents repeated reports for the same producer task.
  absl::flat_hash_set<TaskID> candidate_reports_sent_ ABSL_GUARDED_BY(mutex_);

  /// Workers definitively reported dead by the GCS.
  absl::flat_hash_set<WorkerID> failed_workers_ ABSL_GUARDED_BY(mutex_);

  /// Nodes definitively reported dead by the GCS.
  absl::flat_hash_set<NodeID> failed_nodes_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray::core