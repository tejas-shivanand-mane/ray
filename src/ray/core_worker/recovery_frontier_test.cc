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

TEST(RecoveryFrontierTest, GroupReturnIndexSelectsOriginalTaskAndReturn) {
  RecoveryFrontierPlanner planner(/*group_size=*/4);
  const rpc::TaskSpec first = MakeTask('a', /*num_returns=*/2);
  const rpc::TaskSpec second = MakeTask('b', /*num_returns=*/3);

  const auto first_membership = planner.RegisterTask(first);
  const auto second_membership = planner.RegisterTask(second);

  EXPECT_EQ(first_membership.first_group_return_index, 0U);
  EXPECT_EQ(second_membership.first_group_return_index, 2U);

  const RecoveryFrontierGroup *group = planner.GetGroup(first_membership.group_id);
  ASSERT_NE(group, nullptr);

  rpc::TaskSpec replay;
  uint32_t local_return = 99;

  ASSERT_TRUE(group->ExtractTaskForReturn(/*group_return_index=*/3,
                                          &replay,
                                          &local_return));
  EXPECT_EQ(replay.task_id(), second.task_id());
  EXPECT_EQ(local_return, 1U);
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
