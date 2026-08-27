// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/recovery_frontier.h"

#include <string>

#include <gtest/gtest.h>

namespace ray::core {
namespace {

rpc::TaskSpec MakeTask(char byte, int64_t num_returns = 1) {
  rpc::TaskSpec spec;
  spec.set_task_id(std::string(TaskID::Size(), byte));
  spec.set_num_returns(num_returns);
  spec.set_max_retries(2);
  return spec;
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
