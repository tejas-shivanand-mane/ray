#include "src/ray/protobuf/common.pb.h"




struct PendingHolderInstallation {
  rpc::Address candidate_address;
  std::string reservation_id;
  uint32_t proposed_rank = 0;
  bool task_spec_stored = false;
};

struct RecoveryWaiter {
  uint32_t return_index;
  rpc::RecoverTaskOutputReply *reply;
  rpc::SendReplyCallback send_reply_callback;
};

struct TaskRecoveryState {
  rpc::RecoveryManifest manifest;

  // Present only on owner and lineage holders.
  std::optional<rpc::TaskSpec> task_spec;

  absl::flat_hash_map<WorkerID, PendingHolderInstallation>
      pending_installations;

  bool replay_in_progress = false;

  // All replacement returns from one replay.
  std::vector<rpc::ObjectReference> replacement_returns;

  std::vector<RecoveryWaiter> waiters;
};

struct BorrowedObjectRecoveryState {
  TaskID task_id;
  uint32_t return_index = 0;
  rpc::RecoveryManifest cached_manifest;

  bool recovery_in_progress = false;
};



absl::flat_hash_map<TaskID, TaskRecoveryState> task_states_;
absl::flat_hash_map<ObjectID, BorrowedObjectRecoveryState> borrowed_objects_;





class RecoverySuccessionManager {
 public:
  rpc::RecoveryManifest BuildInitialManifest(
      const TaskID &task_id,
      const JobID &job_id,
      int32_t max_retries);

  /// Returns whether recovery succession supports this task.
  static bool IsEligibleTask(const rpc::TaskSpec &task_spec);


  void RegisterOwnedTask(
      const TaskSpecification &task_spec,
      std::vector<rpc::ObjectReference> *returned_refs);

  void RegisterExecutorTask(const rpc::TaskSpec &task_spec);

  void RegisterBorrowedObject(
      const ObjectID &object_id,
      const rpc::RecoveryObjectMetadata &metadata);

  void PopulateRecoveryMetadata(
      const ObjectID &object_id,
      rpc::RecoveryObjectMetadata *metadata) const;

  bool TryRecoverObject(const ObjectID &object_id);

  void HandleWorkerFailure(const WorkerID &worker_id);
  void HandleNodeFailure(const NodeID &node_id);

  void HandleReportRecoveryCandidate(...);
  void HandleInstallRecoveryHolder(...);
  void HandleCommitRecoveryManifest(...);
  void HandleRecoverTaskOutput(...);
  void HandleApplyRecoveryTombstone(...);

  bool HasConfirmedHolderResponsibilities() const;
};