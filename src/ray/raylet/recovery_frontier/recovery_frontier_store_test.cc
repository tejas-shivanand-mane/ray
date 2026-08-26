// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/raylet/recovery_frontier/recovery_frontier_store.h"

#include <string>

#include <gtest/gtest.h>

namespace ray::raylet {
namespace {

rpc::TaskSpec MakeTask(char byte, uint32_t num_returns) {
  rpc::TaskSpec task;
  task.set_task_id(std::string(TaskID::Size(), byte));
  task.set_num_returns(num_returns);
  task.set_max_retries(2);
  return task;
}

void AddMember(rpc::RecoveryFrontierAppend *append,
               const rpc::TaskSpec &task,
               uint32_t member_index,
               uint32_t first_group_return_index) {
  auto *member = append->add_members();
  member->set_task_id(task.task_id());
  member->set_member_index(member_index);
  member->set_first_group_return_index(first_group_return_index);
  member->set_num_returns(static_cast<uint32_t>(task.num_returns()));
  member->mutable_task_spec()->CopyFrom(task);
}

rpc::RecoveryFrontierAppend MakeInitialAppend() {
  const rpc::TaskSpec first = MakeTask('a', 2);
  const rpc::TaskSpec second = MakeTask('b', 1);

  rpc::RecoveryFrontierAppend append;
  append.set_group_id(first.task_id());
  append.set_base_generation(0);
  append.set_generation(1);
  append.set_begin_member_index(0);
  append.set_end_member_index(2);
  AddMember(&append, first, 0, 0);
  AddMember(&append, second, 1, 2);
  return append;
}

TEST(RecoveryFrontierStoreTest, AppliesInitialPrefixAndExtractsIndependentTask) {
  RecoveryFrontierStore store;
  const auto append = MakeInitialAppend();
  const TaskID group_id = TaskID::FromBinary(append.group_id());

  EXPECT_EQ(store.ApplyAppend(append),
            RecoveryFrontierStore::ApplyResult::APPLIED);
  ASSERT_TRUE(store.Generation(group_id).has_value());
  EXPECT_EQ(*store.Generation(group_id), 1U);
  ASSERT_TRUE(store.CommittedMemberCount(group_id).has_value());
  EXPECT_EQ(*store.CommittedMemberCount(group_id), 2U);

  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(store.ExtractTaskForReturn(group_id, 2, &replay, &local_return));
  EXPECT_EQ(replay.task_id(), append.members(1).task_id());
  EXPECT_EQ(local_return, 0U);
}

TEST(RecoveryFrontierStoreTest, AppliesOnlyContiguousNextGeneration) {
  RecoveryFrontierStore store;
  const auto first_append = MakeInitialAppend();
  const TaskID group_id = TaskID::FromBinary(first_append.group_id());
  ASSERT_EQ(store.ApplyAppend(first_append),
            RecoveryFrontierStore::ApplyResult::APPLIED);

  const rpc::TaskSpec third = MakeTask('c', 3);
  rpc::RecoveryFrontierAppend second_append;
  second_append.set_group_id(first_append.group_id());
  second_append.set_base_generation(1);
  second_append.set_generation(2);
  second_append.set_begin_member_index(2);
  second_append.set_end_member_index(3);
  AddMember(&second_append, third, 2, 3);

  EXPECT_EQ(store.ApplyAppend(second_append),
            RecoveryFrontierStore::ApplyResult::APPLIED);
  EXPECT_EQ(store.ApplyAppend(second_append),
            RecoveryFrontierStore::ApplyResult::IDEMPOTENT);
  EXPECT_EQ(*store.Generation(group_id), 2U);
  EXPECT_EQ(*store.CommittedMemberCount(group_id), 3U);

  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(store.ExtractTaskForReturn(group_id, 4, &replay, &local_return));
  EXPECT_EQ(replay.task_id(), third.task_id());
  EXPECT_EQ(local_return, 1U);
}

TEST(RecoveryFrontierStoreTest, RejectsStaleAndOutOfOrderAppends) {
  RecoveryFrontierStore store;
  const auto first_append = MakeInitialAppend();
  ASSERT_EQ(store.ApplyAppend(first_append),
            RecoveryFrontierStore::ApplyResult::APPLIED);

  rpc::RecoveryFrontierAppend future = first_append;
  future.set_base_generation(2);
  future.set_generation(3);
  future.set_begin_member_index(2);
  future.set_end_member_index(3);
  future.clear_members();
  const rpc::TaskSpec third = MakeTask('c', 1);
  AddMember(&future, third, 2, 3);
  EXPECT_EQ(store.ApplyAppend(future),
            RecoveryFrontierStore::ApplyResult::STALE);

  rpc::RecoveryFrontierAppend stale = first_append;
  stale.mutable_members(1)->mutable_task_spec()->set_name("different");
  EXPECT_EQ(store.ApplyAppend(stale),
            RecoveryFrontierStore::ApplyResult::STALE);
}

TEST(RecoveryFrontierStoreTest, RejectsMalformedPrefixCoordinates) {
  RecoveryFrontierStore store;
  auto append = MakeInitialAppend();
  append.mutable_members(1)->set_first_group_return_index(7);
  EXPECT_EQ(store.ApplyAppend(append),
            RecoveryFrontierStore::ApplyResult::INVALID);

  auto wrong_leader = MakeInitialAppend();
  wrong_leader.set_group_id(std::string(TaskID::Size(), 'z'));
  EXPECT_EQ(store.ApplyAppend(wrong_leader),
            RecoveryFrontierStore::ApplyResult::INVALID);
}

TEST(RecoveryFrontierStoreTest, KOneShapeIsOrdinarySingleTaskCapsule) {
  RecoveryFrontierStore store;
  const rpc::TaskSpec task = MakeTask('q', 1);

  rpc::RecoveryFrontierAppend append;
  append.set_group_id(task.task_id());
  append.set_base_generation(0);
  append.set_generation(1);
  append.set_begin_member_index(0);
  append.set_end_member_index(1);
  AddMember(&append, task, 0, 0);

  EXPECT_EQ(store.ApplyAppend(append),
            RecoveryFrontierStore::ApplyResult::APPLIED);
  rpc::TaskSpec replay;
  uint32_t local_return = 99;
  ASSERT_TRUE(store.ExtractTaskForReturn(TaskID::FromBinary(task.task_id()),
                                         0,
                                         &replay,
                                         &local_return));
  EXPECT_EQ(replay.task_id(), task.task_id());
  EXPECT_EQ(local_return, 0U);
}

}  // namespace
}  // namespace ray::raylet
