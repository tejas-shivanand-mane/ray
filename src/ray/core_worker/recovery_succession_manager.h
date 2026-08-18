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
#include <map>
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

/// Patch 4D: pipelined holder admission.
/// Patch 4F: first-holder TaskSpec piggyback.
/// Patch 4G: hot-path profiling and B1 ablations.
/// Patch 4H: compact task-argument recovery metadata.
/// Patch 4I: TaskSpec-level recovery argument sidecar.
/// Patch 4J: task-centric recovery state and on-demand owner lineage.
/// Patch 4L: correctness-preserving retained owner lineage for late borrow.
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

    // True when the candidate already has the complete producer TaskSpec.
    // Patch 4F uses this for a downstream borrower that consumed the
    // transport-only TaskSpec sidecar. This does not imply commitment.
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

    // Legacy Patch-4J metric. Patch 4L deliberately retains one dormant
    // owner TaskSpec, so this remains zero under the 4L design.
    uint64_t owner_lazy_task_spec_copies_avoided = 0;

    // Patch 4L owner-retained lineage accounting. "current" and "peak" are
    // gauges/state high-water marks; created/released are cumulative events
    // since the last profile reset.
    uint64_t owner_retained_task_specs_current = 0;
    uint64_t owner_retained_task_specs_peak = 0;
    uint64_t owner_retained_task_spec_bytes_current = 0;
    uint64_t owner_retained_task_spec_bytes_peak = 0;
    uint64_t owner_retained_task_specs_created = 0;
    uint64_t owner_retained_task_specs_released = 0;
    uint64_t owner_retained_task_spec_copy_time_ns = 0;

    uint64_t task_centric_metadata_builds = 0;

    // Patch 4F full-lineage copies moved through normal downstream PushTask
    // transport instead of InstallRecoveryHolder. task_spec_bytes_sent also
    // includes these bytes.
    uint64_t first_holder_piggyback_copies_sent = 0;
    uint64_t first_holder_piggyback_bytes_sent = 0;
    uint64_t first_holder_piggyback_serialize_time_ns = 0;

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

    // Recovery work performed before any downstream holder admission.
    uint64_t task_argument_metadata_calls = 0;
    uint64_t task_argument_metadata_time_ns = 0;

    // Patch 4H: base recovery metadata bytes attached to normal TaskSpec
    // arguments, excluding the optional first-holder TaskSpec sidecar.
    uint64_t task_argument_metadata_refs_attached = 0;
    uint64_t task_argument_metadata_compact_refs = 0;
    uint64_t task_argument_metadata_compact_fallbacks = 0;
    uint64_t task_argument_metadata_full_bytes_equivalent = 0;
    uint64_t task_argument_metadata_transport_bytes = 0;

    uint64_t initial_manifest_build_count = 0;
    uint64_t initial_manifest_build_time_ns = 0;
    uint64_t initial_manifest_bytes = 0;

    uint64_t witness_selection_count = 0;
    uint64_t witness_selection_time_ns = 0;

    uint64_t witness_gcs_query_count = 0;
    uint64_t witness_gcs_query_time_ns = 0;

    uint64_t task_spec_manifest_attach_count = 0;
    uint64_t task_spec_manifest_attach_time_ns = 0;

    uint64_t register_owned_task_count = 0;
    uint64_t register_owned_task_time_ns = 0;


    // Patch 4G: synchronous hot-path costs. These are CPU/wall-clock durations
    // spent inside the calling thread, not asynchronous control-RPC latency.
    uint64_t recovery_metadata_lookup_calls = 0;
    uint64_t recovery_metadata_lookup_hits = 0;
    uint64_t recovery_metadata_lookup_time_ns = 0;

    uint64_t ensure_task_arguments_calls = 0;
    uint64_t ensure_task_arguments_time_ns = 0;

    uint64_t register_executor_task_calls = 0;
    uint64_t register_executor_task_time_ns = 0;
    uint64_t register_executor_metadata_refs_seen = 0;
    uint64_t register_executor_candidate_reports_built = 0;

    uint64_t candidate_report_build_calls = 0;
    uint64_t candidate_reports_built = 0;
    uint64_t candidate_report_build_time_ns = 0;

    uint64_t candidate_queue_calls = 0;
    uint64_t candidate_queue_time_ns = 0;

    // Candidate-report transport. logical_reports counts individual tasks;
    // physical_rpcs counts actual single/batched gRPCs.
    uint64_t candidate_rpc_logical_reports_sent = 0;
    uint64_t candidate_rpc_logical_reports_completed = 0;
    uint64_t candidate_rpc_physical_rpcs_sent = 0;
    uint64_t candidate_rpc_physical_rpcs_completed = 0;
    uint64_t candidate_rpc_request_bytes_sent = 0;
    uint64_t candidate_rpc_time_ns = 0;



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


  void RecordTaskArgumentMetadataLatency(uint64_t latency_ns);

  void RecordInitialManifestBuild(
      uint64_t latency_ns,
      uint64_t manifest_bytes);

  void RecordWitnessSelectionLatency(uint64_t latency_ns);

  void RecordWitnessGcsQueryLatency(uint64_t latency_ns);

  void RecordTaskSpecManifestAttachLatency(uint64_t latency_ns);

  void RecordRegisterOwnedTaskLatency(uint64_t latency_ns);


  // Patch 4G hot-path profiling.
  void RecordEnsureTaskArgumentsLatency(uint64_t latency_ns);
  void RecordCandidateQueueLatency(uint64_t latency_ns);
  void RecordCandidateRpcSent(uint64_t logical_reports, uint64_t request_bytes);
  void RecordCandidateRpcLatency(uint64_t logical_reports, uint64_t latency_ns);



  explicit RecoverySuccessionManager(rpc::Address self_address);

  RecoverySuccessionManager(const RecoverySuccessionManager &) = delete;
  RecoverySuccessionManager &operator=(const RecoverySuccessionManager &) = delete;

  /// Returns true when recovery succession supports the task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);

  /// Returns true only when a task actually carries Recovery Succession
  /// state: either its own recovery manifest or recovery metadata on one of
  /// its ObjectRef arguments.
  static bool CarriesRecoveryMetadata(const rpc::TaskSpec &task_spec);

  /// Creates the initial manifest owned by this CoreWorker.
  rpc::RecoveryManifest BuildInitialManifest(const TaskID &task_id,
                                             const JobID &job_id,
                                             int32_t max_retries) const;

  /// Records a task whose TaskSpec already carries a recovery manifest and
  /// attaches metadata to its return refs. This path is also used by recovery replay.
  void RegisterOwnedTask(const TaskSpecification &task_spec,
                         std::vector<rpc::ObjectReference> *returned_refs);

  /// Lazily installs owner recovery state after a task return is actually
  /// exported/borrowed. The TaskSpec does not need to already carry a
  /// recovery_manifest; this stores a private replayable copy with the
  /// supplied manifest attached and creates metadata for all static returns.
  /// Returns true only if this call performed the initialization.
  bool RegisterOwnedTaskLazy(const TaskSpecification &task_spec,
                             const rpc::RecoveryManifest &manifest);

  /// Patch 4L: retain one dormant owner TaskSpec copy while at least one static
  /// return ObjectRef is truly in scope. This does not activate recovery or
  /// construct a manifest.
  void RetainOwnerTaskSpecForLazyRecovery(
      const TaskSpecification &task_spec,
      const std::vector<rpc::ObjectReference> &returned_refs);

  /// Copies the retained owner TaskSpec if one is still live.
  bool GetRetainedOwnerTaskSpec(const TaskID &task_id,
                                rpc::TaskSpec *task_spec) const;

  /// True while this owner task still has at least one live returned ObjectRef.
  bool OwnerTaskHasLiveReturns(const TaskID &task_id) const;

  /// Records actual ObjectRef deletion. Returns true iff this was the final
  /// owner return and an activated recovery task should now be tombstoned.
  bool HandleOwnerReturnRefDeleted(const ObjectID &object_id);

  /// Records a received TaskSpec and returns candidate reports that should be
  /// sent to the coordinators of the received task and its dependencies.
  std::vector<CandidateReport> RegisterExecutorTask(const rpc::TaskSpec &task_spec);

  /// Records metadata for an object borrowed by this worker.
  void RegisterBorrowedObject(const ObjectID &object_id,
                              const rpc::RecoveryObjectMetadata &metadata);

  /// Prepares a provisional holder admission on the current coordinator.
  rpc::ReportRecoveryCandidateReply::Result PrepareHolderAdmission(
      const rpc::ReportRecoveryCandidateRequest &request,
      const rpc::TaskSpec *owner_task_spec,
      HolderAdmissionPlan *plan,
      rpc::RecoveryManifest *latest_manifest);

  /// Stores lineage provisionally on a candidate holder.
  bool InstallRecoveryHolder(const rpc::InstallRecoveryHolderRequest &request);

  /// Commits a previously reserved holder admission on the coordinator.
  bool CommitHolderAdmission(const std::string &reservation_id,
                             rpc::RecoveryManifest *committed_manifest);

  /// Patch 4D: removes a failed provisional reservation and every
  /// speculative reservation at a higher rank for the same task.
  void AbortHolderAdmission(const std::string &reservation_id);

  /// Allows a borrower whose candidate report was rejected/aborted to
  /// report itself again on a later ObjectRef delivery.
  void AllowCandidateReportRetry(const TaskID &task_id);

  /// Applies a committed manifest received from the coordinator.
  bool ApplyCommittedManifest(const rpc::RecoveryManifest &manifest);

  /// Copies known recovery metadata into metadata.
  bool PopulateRecoveryMetadata(const ObjectID &object_id,
                                rpc::RecoveryObjectMetadata *metadata) const;

  /// Patch 4H no-copy fast path used by task-argument activation. This checks
  /// whether metadata already exists without copying the full metadata proto.
  bool HasRecoveryMetadata(const ObjectID &object_id) const;

  /// Adds recovery metadata to direct and nested ObjectRef arguments.
  /// Patch 4F may atomically claim the one-shot H1 TaskSpec piggyback, so this
  /// method intentionally mutates manager state.
  void PopulateTaskArgumentMetadata(
      rpc::TaskSpec *task_spec,
      const absl::flat_hash_map<TaskID, rpc::TaskSpec> *owner_task_specs = nullptr);

  struct BorrowedObjectRecoveryPlan {
    TaskID task_id;
    uint32_t return_index = 0;
    rpc::RecoveryManifest cached_manifest;
  };

  enum class ReplayPreparationResult {
    READY,
    WITNESS_CONFIRMATION_REQUIRED,
    TASK_NOT_FOUND,
    MANIFEST_STALE,
    TOMBSTONED,
    RETRY_LIMIT_EXCEEDED,
    WRONG_HOLDER,
  };



  bool GetBorrowedObjectRecoveryPlan(const ObjectID &object_id,
                                     BorrowedObjectRecoveryPlan *plan) const;

  ReplayPreparationResult PrepareTaskReplay(
      const rpc::RecoverTaskOutputRequest &request,
      const rpc::TaskSpec *owner_task_spec,
      rpc::TaskSpec *task_spec,
      rpc::RecoveryManifest *latest_manifest);

  /// Promotes a provisional holder only from a manifest obtained directly
  /// from one of the task's compact witnesses.
  ///
  /// A newer witness-backed generation may also be adopted if this worker
  /// remains in the succession list.
  bool ConfirmProvisionalHolderFromWitness(
      const rpc::RecoveryManifest &witness_manifest,
      rpc::RecoveryManifest *confirmed_manifest);



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

  // Patch 4J: reconstruct ordinary RecoveryObjectMetadata from task-level
  // state. object_recovery_metadata_ is only a legacy compatibility fallback.
  bool BuildRecoveryMetadataLocked(
      const ObjectID &object_id,
      rpc::RecoveryObjectMetadata *metadata) const
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  struct TaskRecoveryState {
    rpc::RecoveryManifest manifest;

    // Patch 4J: static return count lets the owner reconstruct per-object
    // metadata from ObjectID::ObjectIndex() without a per-return manifest copy.
    uint32_t owned_num_returns = 0;

    // Present on executors and installed/piggyback lineage holders. The
    // original owner may leave this empty and use TaskManager on demand.
    std::optional<rpc::TaskSpec> task_spec;

    // An installed holder is not usable until either CommitRecoveryManifest
    // arrives or the holder independently confirms the manifest from a compact
    // witness during owner-failure recovery.
    bool manifest_committed = true;
    std::string provisional_reservation_id;

    // Owner-side one-shot transport claim. After the first full TaskSpec has
    // been attached to a downstream PushTask, later holders use the ordinary
    // Patch-4E install path when necessary.
    bool first_holder_piggyback_sent = false;

    // Borrower-side state: full TaskSpec is present, but replay remains blocked
    // until a witness-confirmed manifest explicitly contains this worker.
    bool provisional_piggyback_task_spec = false;
  };

  struct OwnerRetainedTaskState {
    rpc::TaskSpec task_spec;
    uint64_t task_spec_bytes = 0;
    absl::flat_hash_set<ObjectID> live_return_ids;
  };

  struct BorrowedObjectRecoveryState {
    TaskID task_id;
    uint32_t return_index = 0;
  };

  struct HolderReservation {
    TaskID task_id;
    rpc::Address candidate_address;
    rpc::RecoveryManifest proposed_manifest;
    uint32_t proposed_rank = 0;  // Patch 4D: speculative contiguous rank.
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

  mutable RecoverySuccessionProfile profile_ ABSL_GUARDED_BY(mutex_);

  /// Recovery state indexed by the original task ID.
  absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_ ABSL_GUARDED_BY(mutex_);

  /// Patch 4L: one correctness-preserving owner TaskSpec copy retained
  /// independently of TaskManager's ordinary lineage lifetime. Presence here
  /// does not mean recovery has been activated.
  absl::flat_hash_map<TaskID, OwnerRetainedTaskState> owner_retained_tasks_
      ABSL_GUARDED_BY(mutex_);

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

  /// Patch 4D: multiple provisional reservations may coexist per task.
  /// The ordered rank map is the speculative prefix H1..HR.
  absl::flat_hash_map<TaskID, std::map<uint32_t, std::string>>
      holder_reservation_by_task_ ABSL_GUARDED_BY(mutex_);

  /// Prevents repeated reports for the same producer task.
  absl::flat_hash_set<TaskID> candidate_reports_sent_ ABSL_GUARDED_BY(mutex_);

  /// Workers definitively reported dead by the GCS.
  absl::flat_hash_set<WorkerID> failed_workers_ ABSL_GUARDED_BY(mutex_);

  /// Nodes definitively reported dead by the GCS.
  absl::flat_hash_set<NodeID> failed_nodes_ ABSL_GUARDED_BY(mutex_);
};

}  // namespace ray::core