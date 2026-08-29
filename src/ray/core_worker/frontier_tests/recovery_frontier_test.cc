// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier.h"

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "ray/common/task/task_spec.h"

namespace ray::core {
namespace {

rpc::TaskSpec MakeTask(char byte, int64_t num_returns = 1) {
  rpc::TaskSpec spec;
  spec.set_task_id(std::string(TaskID::Size(), byte));
  spec.set_num_returns(num_returns);
  spec.set_max_retries(2);
  return spec;
}

rpc::RecoveryFrontierAppend MakeCommittedAppend(
    const rpc::TaskSpec &first,
    const rpc::TaskSpec &second) {
  rpc::RecoveryFrontierAppend append;
  append.set_group_id(first.task_id());
  append.set_base_generation(0);
  append.set_generation(1);
  append.set_begin_member_index(0);
  append.set_end_member_index(2);

  auto *first_record = append.add_members();
  first_record->set_task_id(first.task_id());
  first_record->set_member_index(0);
  first_record->set_first_group_return_index(0);
  first_record->set_num_returns(static_cast<uint32_t>(first.num_returns()));
  first_record->mutable_task_spec()->CopyFrom(first);

  auto *second_record = append.add_members();
  second_record->set_task_id(second.task_id());
  second_record->set_member_index(1);
  second_record->set_first_group_return_index(
      static_cast<uint32_t>(first.num_returns()));
  second_record->set_num_returns(static_cast<uint32_t>(second.num_returns()));
  second_record->mutable_task_spec()->CopyFrom(second);
  return append;
}

TEST(RecoveryFrontierTest, SharedRegistrationReusesCleanImmutableTaskSpec) {
  RecoveryFrontierPlanner planner(/*group_size=*/32);
  auto task = std::make_shared<rpc::TaskSpec>(MakeTask('s'));
  std::shared_ptr<const rpc::TaskSpec> immutable_task = task;

  const auto membership = planner.RegisterTask(immutable_task);
  const RecoveryFrontierGroup *group = planner.GetGroup(membership.group_id);
  ASSERT_NE(group, nullptr);
  ASSERT_EQ(group->Members().size(), 1U);
  ASSERT_NE(group->Members()[0].task_spec, nullptr);

  // The production shared_ptr overload must not deep-copy a clean owner
  // TaskSpec. This is the hot path isolated by Benchmarks 49/50.
  EXPECT_EQ(group->Members()[0].task_spec.get(), task.get());
  EXPECT_EQ(group->Members()[0].task_spec->SerializeAsString(),
            task->SerializeAsString());
}

TEST(RecoveryFrontierTest, SharedRegistrationSanitizesTransportPiggybackPrivately) {
  RecoveryFrontierPlanner planner(/*group_size=*/32);
  auto task = std::make_shared<rpc::TaskSpec>(MakeTask('p'));
  auto *entry = task->add_recovery_argument_metadata();
  entry->mutable_recovery_metadata()->set_first_holder_task_spec("transport-only");
  std::shared_ptr<const rpc::TaskSpec> immutable_task = task;

  const auto membership = planner.RegisterTask(immutable_task);
  const RecoveryFrontierGroup *group = planner.GetGroup(membership.group_id);
  ASSERT_NE(group, nullptr);
  ASSERT_EQ(group->Members().size(), 1U);
  const auto &stored = group->Members()[0].task_spec;
  ASSERT_NE(stored, nullptr);

  // A Patch-4F full-lineage sidecar is transport-only. Even without a recovery
  // manifest it must force a private sanitized recipe instead of being shared.
  EXPECT_NE(stored.get(), task.get());
  ASSERT_EQ(stored->recovery_argument_metadata_size(), 1);
  EXPECT_TRUE(stored->recovery_argument_metadata(0)
                  .recovery_metadata()
                  .first_holder_task_spec()
                  .empty());

  // Sanitization must never mutate the caller/TaskManager-owned TaskSpec.
  EXPECT_EQ(task->recovery_argument_metadata(0)
                .recovery_metadata()
                .first_holder_task_spec(),
            "transport-only");
}

TEST(RecoveryFrontierTest, SharedRegistrationRemovesRecoveryManifestPrivately) {
  RecoveryFrontierPlanner planner(/*group_size=*/32);
  auto task = std::make_shared<rpc::TaskSpec>(MakeTask('m'));
  task->mutable_recovery_manifest()->set_task_id(task->task_id());
  std::shared_ptr<const rpc::TaskSpec> immutable_task = task;

  const auto membership = planner.RegisterTask(immutable_task);
  const RecoveryFrontierGroup *group = planner.GetGroup(membership.group_id);
  ASSERT_NE(group, nullptr);
  ASSERT_EQ(group->Members().size(), 1U);
  const auto &stored = group->Members()[0].task_spec;
  ASSERT_NE(stored, nullptr);

  EXPECT_NE(stored.get(), task.get());
  EXPECT_FALSE(stored->has_recovery_manifest());
  EXPECT_TRUE(task->has_recovery_manifest());
}

TEST(RecoveryFrontierTest, TaskSpecificationMutationDetachesSharedReplaySnapshot) {
  // Actor tasks skip scheduling-class construction, which keeps this fixture
  // intentionally minimal. The COW property itself is TaskSpecification-wide.
  auto proto = std::make_shared<rpc::TaskSpec>();
  proto->set_type(rpc::TaskType::ACTOR_TASK);
  proto->set_attempt_number(3);
  TaskSpecification task_spec(proto);

  const auto replay_snapshot = task_spec.GetSharedMessage();
  ASSERT_EQ(replay_snapshot.get(), proto.get());
  EXPECT_EQ(replay_snapshot->attempt_number(), 3);

  // Normal task retry logic mutates attempt_number through GetMutableMessage().
  // A shared Frontier replay snapshot must remain at the original attempt.
  task_spec.GetMutableMessage().set_attempt_number(4);

  EXPECT_NE(task_spec.GetSharedMessage().get(), replay_snapshot.get());
  EXPECT_EQ(replay_snapshot->attempt_number(), 3);
  EXPECT_EQ(task_spec.GetMessage().attempt_number(), 4);
}

TEST(RecoveryFrontierTest, SingleTaskIsImmediateLeaderForAnyK) {
  RecoveryFrontierPlanner planner(/*group_size=*/32);
  const rpc::TaskSpec task = MakeTask('a');

  const auto membership = planner.RegisterTask(task);

  EXPECT_TRUE(membership.is_leader);
  EXPECT_EQ(membership.member_index, 0U);
  EXPECT_EQ(membership.first_group_return_index, 0U);
  EXPECT_EQ(membership.num_returns, 1U);
  EXPECT_EQ(membership.group_id, TaskID::FromBinary(task.task_id()));
  EXPECT_FALSE(membership.closes_group);

  // The leader is immediately stageable even though the group is not full.
  RecoveryFrontierGroup *group = planner.GetMutableGroup(membership.group_id);
  ASSERT_NE(group, nullptr);
  auto batch = group->StageAppend();
  ASSERT_TRUE(batch.has_value());
  EXPECT_EQ(batch->begin_member_index, 0U);
  EXPECT_EQ(batch->end_member_index, 1U);

  // Staging must share the immutable canonical replay recipe instead of
  // deep-copying the TaskSpec into every append batch.
  ASSERT_EQ(group->Members().size(), 1U);
  ASSERT_EQ(batch->members.size(), 1U);
  ASSERT_TRUE(group->Members()[0].task_spec != nullptr);
  ASSERT_TRUE(batch->members[0].task_spec != nullptr);
  EXPECT_EQ(batch->members[0].task_spec.get(),
            group->Members()[0].task_spec.get());
  EXPECT_EQ(batch->members[0].task_spec->task_id(), task.task_id());
}

TEST(RecoveryFrontierTest, IndependentTasksShareOneGroup) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);

  const auto a = planner.RegisterTask(MakeTask('a'));
  const auto b = planner.RegisterTask(MakeTask('b'));
  const auto c = planner.RegisterTask(MakeTask('c'));
  const auto d = planner.RegisterTask(MakeTask('d'));

  EXPECT_EQ(a.group_id, b.group_id);
  EXPECT_EQ(a.group_id, c.group_id);
  EXPECT_EQ(a.group_id, d.group_id);
  EXPECT_TRUE(a.is_leader);
  EXPECT_FALSE(b.is_leader);
  EXPECT_FALSE(c.is_leader);
  EXPECT_FALSE(d.is_leader);
  EXPECT_TRUE(d.closes_group);

  const RecoveryFrontierGroup *group = planner.GetGroup(a.group_id);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->MemberCount(), 4U);
  EXPECT_TRUE(group->Full());
}

TEST(RecoveryFrontierTest, AckedPrefixControlsRecoverability) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', /*num_returns=*/2);
  const rpc::TaskSpec second = MakeTask('b', /*num_returns=*/3);

  const auto first_membership = planner.RegisterTask(first);
  planner.RegisterTask(second);
  RecoveryFrontierGroup *group = planner.GetMutableGroup(first_membership.group_id);
  ASSERT_NE(group, nullptr);

  // Before a backend ACK, no member may be advertised as recoverable.
  EXPECT_FALSE(group->IsTaskCommitted(TaskID::FromBinary(first.task_id())));
  EXPECT_FALSE(group->IsTaskCommitted(TaskID::FromBinary(second.task_id())));

  // Protect only the leader first. This is the single-task fast safety path.
  auto leader_batch = group->StageAppend(/*max_batch_members=*/1);
  ASSERT_TRUE(leader_batch.has_value());
  EXPECT_EQ(leader_batch->begin_member_index, 0U);
  EXPECT_EQ(leader_batch->end_member_index, 1U);
  ASSERT_TRUE(group->CommitAppend(*leader_batch));
  EXPECT_EQ(group->CommittedMemberCount(), 1U);
  EXPECT_EQ(group->Generation(), 1U);
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(first.task_id())));
  EXPECT_FALSE(group->IsTaskCommitted(TaskID::FromBinary(second.task_id())));

  // Append the second member under the same protection topology.
  auto member_batch = group->StageAppend();
  ASSERT_TRUE(member_batch.has_value());
  EXPECT_EQ(member_batch->base_generation, 1U);
  EXPECT_EQ(member_batch->generation, 2U);
  ASSERT_TRUE(group->CommitAppend(*member_batch));
  EXPECT_EQ(group->CommittedMemberCount(), 2U);
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(second.task_id())));
}

TEST(RecoveryFrontierTest, InFlightAppendKeepsGroupPendingUntilAck) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const auto first = planner.RegisterTask(MakeTask('a'));
  planner.RegisterTask(MakeTask('b'));

  RecoveryFrontierGroup *group = planner.GetMutableGroup(first.group_id);
  ASSERT_NE(group, nullptr);
  EXPECT_TRUE(group->HasUncommittedMembers());

  // Model one exporter publishing only the leader while another exporter
  // reaches the same group. A second physical append must not be stageable
  // until the first ACK either commits or aborts its prefix.
  auto leader_batch = group->StageAppend(/*max_batch_members=*/1);
  ASSERT_TRUE(leader_batch.has_value());
  EXPECT_TRUE(group->HasUncommittedMembers());
  EXPECT_TRUE(group->AppendInFlight());
  EXPECT_FALSE(group->StageAppend().has_value());

  ASSERT_TRUE(group->CommitAppend(*leader_batch));
  EXPECT_FALSE(group->AppendInFlight());
  EXPECT_TRUE(group->HasUncommittedMembers());

  // The waiting exporter can now stage the remaining contiguous suffix. Only
  // after its ACK is the whole group outside the publication barrier.
  auto suffix_batch = group->StageAppend();
  ASSERT_TRUE(suffix_batch.has_value());
  EXPECT_EQ(suffix_batch->begin_member_index, 1U);
  EXPECT_EQ(suffix_batch->end_member_index, 2U);
  ASSERT_TRUE(group->CommitAppend(*suffix_batch));
  EXPECT_FALSE(group->HasUncommittedMembers());
}

TEST(RecoveryFrontierTest, GroupReturnIndexSelectsCommittedOriginalTaskAndReturn) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', /*num_returns=*/2);
  const rpc::TaskSpec second = MakeTask('b', /*num_returns=*/3);

  const auto first_membership = planner.RegisterTask(first);
  const auto second_membership = planner.RegisterTask(second);

  EXPECT_EQ(first_membership.first_group_return_index, 0U);
  EXPECT_EQ(second_membership.first_group_return_index, 2U);

  RecoveryFrontierGroup *group = planner.GetMutableGroup(first_membership.group_id);
  ASSERT_NE(group, nullptr);

  // Extraction must fail before the corresponding append is durable.
  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  EXPECT_FALSE(group->ExtractTaskForReturn(/*group_return_index=*/3,
                                           &replay,
                                           &local_return));

  auto batch = group->StageAppend();
  ASSERT_TRUE(batch.has_value());
  ASSERT_TRUE(group->CommitAppend(*batch));

  ASSERT_TRUE(group->ExtractTaskForReturn(/*group_return_index=*/3,
                                          &replay,
                                          &local_return));
  EXPECT_EQ(replay.task_id(), second.task_id());
  EXPECT_EQ(local_return, 1U);
}

TEST(RecoveryFrontierTest, HolderImportPreservesCommittedGroupReplayMapping) {
  RecoveryFrontierPlanner holder(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', /*num_returns=*/2);
  const rpc::TaskSpec second = MakeTask('b', /*num_returns=*/3);
  const rpc::RecoveryFrontierAppend append = MakeCommittedAppend(first, second);

  ASSERT_TRUE(holder.ApplyCommittedAppend(append));
  const TaskID group_id = TaskID::FromBinary(first.task_id());
  const RecoveryFrontierGroup *group = holder.GetGroup(group_id);
  ASSERT_NE(group, nullptr);
  EXPECT_EQ(group->Generation(), 1U);
  EXPECT_EQ(group->CommittedMemberCount(), 2U);
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(first.task_id())));
  EXPECT_TRUE(group->IsTaskCommitted(TaskID::FromBinary(second.task_id())));

  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(group->ExtractTaskForReturn(/*group_return_index=*/3,
                                          &replay,
                                          &local_return));
  EXPECT_EQ(replay.task_id(), second.task_id());
  EXPECT_EQ(local_return, 1U);

  // Retry of the exact already-durable append is idempotent.
  EXPECT_TRUE(holder.ApplyCommittedAppend(append));
  EXPECT_EQ(group->Generation(), 1U);
  EXPECT_EQ(group->CommittedMemberCount(), 2U);
}

TEST(RecoveryFrontierTest, HolderImportRejectsConflictingDuplicateAndGenerationGap) {
  RecoveryFrontierPlanner holder(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a');
  const rpc::TaskSpec second = MakeTask('b');
  rpc::RecoveryFrontierAppend append = MakeCommittedAppend(first, second);
  ASSERT_TRUE(holder.ApplyCommittedAppend(append));

  rpc::RecoveryFrontierAppend conflicting;
  conflicting.CopyFrom(append);
  conflicting.mutable_members(1)->mutable_task_spec()->set_max_retries(7);
  EXPECT_FALSE(holder.ApplyCommittedAppend(conflicting));

  RecoveryFrontierPlanner fresh_holder(/*group_size=*/4);
  rpc::RecoveryFrontierAppend gap;
  gap.CopyFrom(append);
  gap.set_base_generation(1);
  gap.set_generation(2);
  EXPECT_FALSE(fresh_holder.ApplyCommittedAppend(gap));
  EXPECT_EQ(fresh_holder.GetGroup(TaskID::FromBinary(first.task_id())), nullptr);
}

TEST(RecoveryFrontierTest, StaleOrAbortedAppendCannotAdvancePrefix) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const auto membership = planner.RegisterTask(MakeTask('a'));
  RecoveryFrontierGroup *group = planner.GetMutableGroup(membership.group_id);
  ASSERT_NE(group, nullptr);

  auto first_attempt = group->StageAppend();
  ASSERT_TRUE(first_attempt.has_value());
  ASSERT_TRUE(group->AbortAppend(*first_attempt));
  EXPECT_EQ(group->CommittedMemberCount(), 0U);
  EXPECT_EQ(group->Generation(), 0U);

  auto retry = group->StageAppend();
  ASSERT_TRUE(retry.has_value());
  ASSERT_TRUE(group->CommitAppend(*retry));
  EXPECT_EQ(group->CommittedMemberCount(), 1U);
  EXPECT_EQ(group->Generation(), 1U);

  // A duplicate/stale ACK cannot advance state a second time.
  EXPECT_FALSE(group->CommitAppend(*retry));
  EXPECT_EQ(group->CommittedMemberCount(), 1U);
}

TEST(RecoveryFrontierTest, FullGroupRollsOverToNewLeader) {
  RecoveryFrontierPlanner planner(/*group_size=*/2);

  const auto first = planner.RegisterTask(MakeTask('a'));
  const auto second = planner.RegisterTask(MakeTask('b'));
  const auto third = planner.RegisterTask(MakeTask('c'));

  EXPECT_EQ(first.group_id, second.group_id);
  EXPECT_NE(second.group_id, third.group_id);
  EXPECT_TRUE(first.is_leader);
  EXPECT_FALSE(second.is_leader);
  EXPECT_TRUE(second.closes_group);
  EXPECT_TRUE(third.is_leader);
  EXPECT_EQ(third.member_index, 0U);
}

TEST(RecoveryFrontierTest, SealedOpenGroupForcesFreshLeader) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const auto first = planner.RegisterTask(MakeTask('a'));
  const auto second = planner.RegisterTask(MakeTask('b'));
  ASSERT_EQ(first.group_id, second.group_id);

  ASSERT_TRUE(planner.SealGroup(first.group_id));
  const auto third = planner.RegisterTask(MakeTask('c'));
  EXPECT_NE(third.group_id, first.group_id);
  EXPECT_TRUE(third.is_leader);
  EXPECT_EQ(third.member_index, 0U);
}

TEST(RecoveryFrontierTest, ErasedTerminalGroupRemovesMembershipAliases) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first_task = MakeTask('a');
  const rpc::TaskSpec second_task = MakeTask('b');
  const auto first = planner.RegisterTask(first_task);
  planner.RegisterTask(second_task);

  ASSERT_TRUE(planner.SealGroup(first.group_id));
  ASSERT_TRUE(planner.EraseGroup(first.group_id));
  EXPECT_EQ(planner.GetGroup(first.group_id), nullptr);
  EXPECT_FALSE(planner.FindTask(TaskID::FromBinary(first_task.task_id())).has_value());
  EXPECT_FALSE(planner.FindTask(TaskID::FromBinary(second_task.task_id())).has_value());

  const auto replacement = planner.RegisterTask(MakeTask('c'));
  EXPECT_TRUE(replacement.is_leader);
  EXPECT_NE(replacement.group_id, first.group_id);
}
TEST(RecoveryFrontierTest, KOneDegeneratesToPerTaskProtection) {
  RecoveryFrontierPlanner planner(/*group_size=*/1);

  const auto first = planner.RegisterTask(MakeTask('a'));
  const auto second = planner.RegisterTask(MakeTask('b'));

  EXPECT_TRUE(first.is_leader);
  EXPECT_TRUE(first.closes_group);
  EXPECT_TRUE(second.is_leader);
  EXPECT_TRUE(second.closes_group);
  EXPECT_NE(first.group_id, second.group_id);
}

}  // namespace
}  // namespace ray::core
