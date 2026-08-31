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
#include <memory>
#include <optional>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/frontier/recovery_frontier.pb.h"

namespace ray::core {

/// Describes one task inside a recovery-frontier capsule.
///
/// A frontier capsule is deliberately not a DAG checkpoint. The first task is
/// the protected frontier leader and later independent or dependent tasks may
/// append their replay recipes to the same capsule. This lets one protection
/// topology amortize control-plane work across a window of fine-grained tasks.
struct RecoveryFrontierMember {
  TaskID task_id = TaskID::Nil();
  // The replay recipe is immutable after registration. Production owner tasks
  // share the TaskManager-owned protobuf instead of deep-copying it here;
  // TaskSpecification detaches on mutation. Staged append batches share the
  // same immutable recipe safely while asynchronous publication is in flight.
  std::shared_ptr<const rpc::TaskSpec> task_spec;
  uint32_t member_index = 0;
  uint32_t first_group_return_index = 0;
  uint32_t num_returns = 0;

  // Owner-local lifetime bit. It is intentionally not serialized: holders
  // store replay recipes, while only the producer owner decides group cleanup.
  bool owner_returns_live = true;
};

struct RecoveryFrontierMembership {
  TaskID group_id = TaskID::Nil();
  uint32_t member_index = 0;
  uint32_t first_group_return_index = 0;
  uint32_t num_returns = 0;
  bool is_leader = false;
  bool closes_group = false;
};

/// One staged append to a protected frontier group.
///
/// The backend (fixed-R or Succession) publishes every member in `members` to
/// the group's already-selected holders. The append becomes visible to
/// borrowers only after the backend ACKs the whole batch and CommitAppend()
/// advances the committed prefix.
struct RecoveryFrontierAppendBatch {
  TaskID group_id = TaskID::Nil();
  uint64_t base_generation = 0;
  uint64_t generation = 0;
  uint32_t begin_member_index = 0;
  uint32_t end_member_index = 0;  // exclusive
  std::vector<RecoveryFrontierMember> members;
};

/// Owner-local append-only recovery-frontier capsule.
///
/// The group ID is the first member's TaskID, so the leader can be protected
/// immediately even if no second task ever arrives. K therefore controls only
/// how many later replay recipes may share that protection unit; it never
/// delays protection of a single-task workload.
///
/// Durability is prefix-based. At most one append is in flight per group. An
/// object may advertise frontier recovery only if its member index is below
/// CommittedMemberCount(). This gives a simple crash invariant: after owner
/// failure, every advertised member exists at all holders required by the
/// selected protection backend.
class RecoveryFrontierGroup {
 public:
  RecoveryFrontierGroup(TaskID group_id, uint32_t max_members);

  const TaskID &GroupId() const { return group_id_; }
  uint32_t MaxMembers() const { return max_members_; }
  uint32_t MemberCount() const { return static_cast<uint32_t>(members_.size()); }
  uint32_t TotalReturns() const { return next_group_return_index_; }
  uint32_t CommittedMemberCount() const { return committed_member_count_; }
  uint64_t Generation() const { return generation_; }
  bool Full() const { return MemberCount() >= max_members_; }
  bool AppendInFlight() const { return append_in_flight_; }
  bool HasUncommittedMembers() const { return committed_member_count_ < MemberCount(); }

  /// Append a replayable TaskSpec with shared immutable ownership. Returns its
  /// stable membership coordinates. Duplicate TaskIDs are idempotent and
  /// return the original membership.
  std::optional<RecoveryFrontierMembership> AddTask(
      std::shared_ptr<const rpc::TaskSpec> task_spec);

  /// Stage the next contiguous append. Only one append may be in flight.
  /// max_batch_members=0 means stage every currently uncommitted member.
  /// After AbortAppend(), the exact aborted generation/boundary is retried
  /// before any members that joined while the failed publication was in flight.
  std::optional<RecoveryFrontierAppendBatch> StageAppend(uint32_t max_batch_members = 0);

  /// Commit exactly the currently staged append after backend durability ACKs.
  /// Returns false for stale/out-of-order ACKs and leaves state unchanged.
  bool CommitAppend(const RecoveryFrontierAppendBatch &batch);

  /// Abort exactly the currently staged append. Members remain pending, but the
  /// failed generation's exact [begin,end) boundary is retained. The next
  /// StageAppend() therefore retries the same recipes under the same generation
  /// before later members can advance the Frontier.
  bool AbortAppend(const RecoveryFrontierAppendBatch &batch);

  /// Import an append that another worker has already made durable. This is the
  /// holder-side counterpart to StageAppend()+CommitAppend(): it validates the
  /// same contiguous generation/member invariants, installs the exact replay
  /// recipes, and advances only the acknowledged prefix. Duplicate delivery of
  /// the already-committed append is idempotent when the stored records match.
  ///
  /// This primitive is backend-neutral. Fixed-R holders and adaptive
  /// Succession holders can therefore store the same Frontier wire record while
  /// keeping protection topology decisions outside the planner.
  bool ApplyCommittedAppend(const rpc::RecoveryFrontierAppend &append);

  bool IsTaskCommitted(const TaskID &task_id) const;

  /// Mark this owner's TaskManager lineage for one member as released.
  /// Idempotent. Returns true iff no registered owner member remains live.
  bool MarkOwnerTaskReleased(const TaskID &task_id);

  /// Look up a member by its producer TaskID.
  std::optional<RecoveryFrontierMembership> FindTask(const TaskID &task_id) const;

  /// Resolve a group-global return index to the original TaskSpec and local
  /// return index. Only committed members are eligible for recovery.
  bool ExtractTaskForReturn(uint32_t group_return_index,
                            rpc::TaskSpec *task_spec,
                            uint32_t *task_return_index) const;

  const std::vector<RecoveryFrontierMember> &Members() const { return members_; }

 private:
  bool MatchesInFlight(const RecoveryFrontierAppendBatch &batch) const;

  TaskID group_id_;
  uint32_t max_members_;
  uint32_t next_group_return_index_ = 0;
  uint32_t committed_member_count_ = 0;
  uint32_t live_owner_members_ = 0;
  uint64_t generation_ = 0;
  bool append_in_flight_ = false;
  uint64_t in_flight_generation_ = 0;
  uint32_t in_flight_begin_ = 0;
  uint32_t in_flight_end_ = 0;
  std::vector<RecoveryFrontierMember> members_;
  absl::flat_hash_map<TaskID, uint32_t> task_to_member_index_;
};

/// Assigns tasks submitted by one owner to append-only frontier groups.
///
/// Grouping is submission-order based rather than DAG-depth based. This is
/// intentional: it works for both independent map-style tasks and connected
/// task DAGs. The first task opens a group and becomes its immediately
/// protectable leader. Up to K-1 later tasks reuse the same protection unit.
class RecoveryFrontierPlanner {
 public:
  explicit RecoveryFrontierPlanner(uint32_t group_size);

  uint32_t GroupSize() const { return group_size_; }

  /// Compatibility/test path: make an owned immutable recipe from a protobuf
  /// reference. Production owner registration should use the shared_ptr
  /// overload below to avoid a second full TaskSpec copy.
  RecoveryFrontierMembership RegisterTask(const rpc::TaskSpec &task_spec);

  RecoveryFrontierMembership RegisterTask(
      std::shared_ptr<const rpc::TaskSpec> task_spec);

  /// Holder-side import for an append that is already durable according to the
  /// selected protection backend. Unlike RegisterTask(), this never opens or
  /// extends the owner's submission-order group; the transmitted group ID and
  /// membership coordinates are authoritative.
  bool ApplyCommittedAppend(const rpc::RecoveryFrontierAppend &append);

  std::optional<RecoveryFrontierMembership> FindTask(const TaskID &task_id) const;

  const RecoveryFrontierGroup *GetGroup(const TaskID &group_id) const;
  RecoveryFrontierGroup *GetMutableGroup(const TaskID &group_id);

  /// Permanently close a partially filled group so future tasks open a
  /// fresh group instead of appending to a terminal/tombstoned capsule.
  bool SealGroup(const TaskID &group_id);

  /// Remove a terminal group and all task-to-group membership aliases.
  /// The caller must ensure no live owner return can activate it again.
  bool EraseGroup(const TaskID &group_id);

 private:
  uint32_t group_size_;
  TaskID open_group_id_ = TaskID::Nil();
  absl::flat_hash_map<TaskID, RecoveryFrontierGroup> groups_;
  absl::flat_hash_map<TaskID, RecoveryFrontierMembership> membership_by_task_;
};

}  // namespace ray::core