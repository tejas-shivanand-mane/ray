// Copyright 2026 The Ray Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#include "ray/common/streaming_recovery/streaming_recovery.h"

#include <functional>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace {

rpc::Address Worker() {
  rpc::Address address;
  address.set_worker_id(WorkerID::FromRandom().Binary());
  address.set_node_id(NodeID::FromRandom().Binary());
  address.set_ip_address("127.0.0.1");
  address.set_port(10001);
  return address;
}

class StreamingRecoveryProtocolTest : public ::testing::Test {
 public:
  void SetUp() override {
    const auto job_id = JobID::FromInt(1);
    const auto task_id = TaskID::FromRandom(job_id);
    descriptor.set_version(1);
    descriptor.set_task_id(task_id.Binary());
    descriptor.set_generator_id(ObjectID::FromIndex(task_id, 1).Binary());
    descriptor.set_expected_returns(3);
    descriptor.mutable_consumer_address()->CopyFrom(Worker());
    auto *manifest = descriptor.mutable_manifest();
    manifest->set_task_id(task_id.Binary());
    manifest->set_job_id(job_id.Binary());
    manifest->set_target_holder_count(2);
    manifest->set_witness_count(2);
    manifest->set_max_recovery_attempts(2);
    manifest->mutable_version()->set_generation(1);
    auto *owner = manifest->add_succession();
    owner->mutable_address()->CopyFrom(Worker());
    owner->set_failure_domain_id(owner->address().node_id());
    for (int i = 0; i < 2; ++i) {
      auto *witness = manifest->add_witness_raylets();
      witness->CopyFrom(Worker());
      witness->clear_worker_id();
    }
    recipe.set_task_id(task_id.Binary());
    recipe.set_job_id(job_id.Binary());
    recipe.set_type(rpc::TaskType::NORMAL_TASK);
    recipe.set_streaming_generator(true);
    recipe.set_returns_dynamic(true);
    recipe.set_num_returns(1);
    recipe.set_num_objects_per_yield(1);
    recipe.set_max_retries(2);
    recipe.mutable_caller_address()->CopyFrom(owner->address());
    recipe.mutable_recovery_stream_descriptor()->CopyFrom(descriptor);
    recipe.mutable_recovery_manifest()->CopyFrom(*manifest);
    ASSERT_TRUE(ValidateRecoveryStreamRecipe(recipe).ok());
  }

  rpc::GetRecoveryWitnessReply Grant() const {
    rpc::GetRecoveryWitnessReply reply;
    reply.set_found(true);
    reply.set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_GRANTED);
    reply.mutable_acting_owner()->CopyFrom(descriptor.consumer_address());
    reply.mutable_manifest()->CopyFrom(descriptor.manifest());
    reply.mutable_manifest()->set_recovery_attempt(1);
    reply.mutable_task_spec()->CopyFrom(recipe);
    reply.mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(reply.manifest());
    return reply;
  }

  rpc::RecoveryStreamDescriptor descriptor;
  rpc::TaskSpec recipe;
};

TEST_F(StreamingRecoveryProtocolTest, RequiresEveryDistinctAckAndConsumerReceipt) {
  RecoveryStreamInstallation install;
  ASSERT_TRUE(install.Initialize(descriptor).ok());
  rpc::UpdateRecoveryWitnessReply ack;
  ack.set_stored(true);
  ASSERT_TRUE(install.RecordConsumerReceipt(descriptor.consumer_address(), descriptor).ok());
  ASSERT_TRUE(install.RecordWitnessReply(
      descriptor.manifest().witness_raylets(0), Status::OK(), ack).ok());
  // Repeated ACKs from holder 0 cannot substitute for a withheld holder 1 ACK.
  ASSERT_TRUE(install.RecordWitnessReply(
      descriptor.manifest().witness_raylets(0), Status::OK(), ack).ok());
  EXPECT_FALSE(install.IsReady());
  EXPECT_FALSE(install.ReadyDescriptor().has_value());
  EXPECT_FALSE(install.RecordWitnessReply(Worker(), Status::OK(), ack).ok());
  ASSERT_TRUE(install.RecordWitnessReply(
      descriptor.manifest().witness_raylets(1), Status::OK(), ack).ok());
  ASSERT_TRUE(install.ReadyDescriptor().has_value());
  EXPECT_TRUE(SameRecoveryStreamDescriptor(*install.ReadyDescriptor(), descriptor));
  install.Cancel();
  EXPECT_FALSE(install.IsReady());
  EXPECT_FALSE(install.Initialize(descriptor).ok());
  EXPECT_FALSE(install.RecordConsumerReceipt(descriptor.consumer_address(), descriptor).ok());
}

TEST_F(StreamingRecoveryProtocolTest, AllAcksAloneDoNotProveConsumerReceipt) {
  RecoveryStreamInstallation install;
  ASSERT_TRUE(install.Initialize(descriptor).ok());
  rpc::UpdateRecoveryWitnessReply ack;
  ack.set_stored(true);
  for (const auto &witness : descriptor.manifest().witness_raylets()) {
    ASSERT_TRUE(install.RecordWitnessReply(witness, Status::OK(), ack).ok());
  }
  EXPECT_FALSE(install.IsReady());
  EXPECT_FALSE(install.RecordConsumerReceipt(Worker(), descriptor).ok());
  auto wrong_descriptor = descriptor;
  wrong_descriptor.set_expected_returns(4);
  EXPECT_FALSE(install.RecordConsumerReceipt(
      descriptor.consumer_address(), wrong_descriptor).ok());
  EXPECT_FALSE(install.IsReady());
  ASSERT_TRUE(install.RecordConsumerReceipt(descriptor.consumer_address(), descriptor).ok());
  EXPECT_TRUE(install.IsReady());
}

TEST_F(StreamingRecoveryProtocolTest, FailedOrSupersededInstallCannotBecomeReady) {
  for (int failure = 0; failure < 3; ++failure) {
    RecoveryStreamInstallation install;
    ASSERT_TRUE(install.Initialize(descriptor).ok());
    rpc::UpdateRecoveryWitnessReply ack;
    ack.set_stored(failure != 0);
    Status status = failure == 1 ? Status::IOError("holder lost") : Status::OK();
    if (failure == 2) {
      ack.mutable_latest_manifest()->CopyFrom(descriptor.manifest());
      ack.mutable_latest_manifest()->set_tombstoned(true);
    }
    EXPECT_FALSE(install.RecordWitnessReply(
        descriptor.manifest().witness_raylets(0), status, ack).ok());
    ack.Clear();
    ack.set_stored(true);
    EXPECT_FALSE(install.RecordWitnessReply(
        descriptor.manifest().witness_raylets(0), Status::OK(), ack).ok());
    EXPECT_FALSE(install.IsReady());
  }
}

TEST_F(StreamingRecoveryProtocolTest, RejectsInvalidIdentityCountAndTopology) {
  const std::vector<std::function<void(rpc::RecoveryStreamDescriptor &)>> changes = {
      [](auto &d) { d.set_version(2); },
      [](auto &d) { d.set_task_id("short"); },
      [](auto &d) { d.set_generator_id("short"); },
      [](auto &d) { d.set_expected_returns(-1); },
      [](auto &d) { d.set_expected_returns(RayConfig::instance().max_num_generator_returns()); },
      [](auto &d) { d.mutable_manifest()->set_witness_count(1); },
      [](auto &d) { d.mutable_manifest()->set_tombstoned(true); },
      [](auto &d) { d.mutable_manifest()->set_recovery_attempt(1); },
      [](auto &d) { d.mutable_manifest()->set_job_id(JobID::FromInt(2).Binary()); },
      [](auto &d) { d.mutable_manifest()->mutable_witness_raylets(1)->CopyFrom(
          d.manifest().witness_raylets(0)); },
      [](auto &d) { d.mutable_manifest()->mutable_witness_raylets(0)->set_node_id(
          d.manifest().succession(0).address().node_id()); },
      [](auto &d) { d.mutable_consumer_address()->CopyFrom(
          d.manifest().succession(0).address()); },
  };
  for (const auto &change : changes) {
    auto invalid = descriptor;
    change(invalid);
    EXPECT_FALSE(ValidateRecoveryStreamDescriptor(invalid).ok());
  }
  descriptor.set_expected_returns(0);
  EXPECT_TRUE(ValidateRecoveryStreamDescriptor(descriptor).ok());
}

TEST_F(StreamingRecoveryProtocolTest, RejectsUnsupportedRecipes) {
  const std::vector<std::function<void(rpc::TaskSpec &)>> changes = {
      [](auto &r) { r.set_type(rpc::TaskType::ACTOR_TASK); },
      [](auto &r) { r.set_streaming_generator(false); },
      [](auto &r) { r.set_returns_dynamic(false); },
      [](auto &r) { r.set_num_objects_per_yield(2); },
      [](auto &r) { r.set_attempt_number(1); },
      [](auto &r) { r.set_max_retries(0); },
      [](auto &r) { r.set_num_streaming_generator_returns(0); },
      [](auto &r) { r.add_args()->mutable_object_ref(); },
      [](auto &r) { r.add_args()->add_nested_inlined_refs(); },
      [](auto &r) { r.add_recovery_argument_metadata(); },
      [](auto &r) { r.set_tensor_transport("nixl"); },
      [](auto &r) { r.mutable_caller_address()->CopyFrom(Worker()); },
  };
  for (const auto &change : changes) {
    auto invalid = recipe;
    change(invalid);
    EXPECT_FALSE(ValidateRecoveryStreamRecipe(invalid).ok());
  }
}

TEST_F(StreamingRecoveryProtocolTest, InstalledRecipeCannotChangeOrDropDescriptor) {
  auto retained = recipe;
  retained.clear_recovery_manifest();
  EXPECT_TRUE(SameRecoveryStreamRecipe(recipe, retained));
  auto conflicting = recipe;
  conflicting.add_args()->set_data("changed input");
  EXPECT_FALSE(SameRecoveryStreamRecipe(conflicting, retained));
  conflicting = recipe;
  conflicting.mutable_recovery_stream_descriptor()->set_expected_returns(4);
  EXPECT_FALSE(SameRecoveryStreamRecipe(conflicting, retained));
  conflicting = recipe;
  conflicting.clear_recovery_stream_descriptor();
  EXPECT_FALSE(SameRecoveryStreamRecipe(conflicting, retained));
}

TEST_F(StreamingRecoveryProtocolTest, ValidGrantPreservesIdsAndSetsReplayCallerAndAttempt) {
  auto grant = Grant();
  auto *affinity = grant.mutable_task_spec()->mutable_scheduling_strategy()
                       ->mutable_node_affinity_scheduling_strategy();
  affinity->set_node_id(descriptor.manifest().succession(0).address().node_id());
  affinity->set_soft(true);
  rpc::TaskSpec replay;
  ASSERT_TRUE(PrepareRecoveryStreamReplay(
      descriptor, descriptor.consumer_address(), grant, &replay).ok());
  EXPECT_EQ(replay.task_id(), recipe.task_id());
  EXPECT_EQ(replay.attempt_number(), 1);
  EXPECT_EQ(replay.num_streaming_generator_returns(), 3);
  EXPECT_EQ(replay.caller_address().worker_id(), descriptor.consumer_address().worker_id());
  EXPECT_TRUE(replay.scheduling_strategy().has_default_scheduling_strategy());
  EXPECT_EQ(grant.task_spec().attempt_number(), 0);
  EXPECT_TRUE(RecoveryStreamClaimantMatches(recipe, descriptor.consumer_address()));
  EXPECT_FALSE(RecoveryStreamClaimantMatches(recipe, Worker()));
}

TEST_F(StreamingRecoveryProtocolTest, InvalidGrantLeavesReplayOutputUnchanged) {
  const std::vector<std::function<void(rpc::GetRecoveryWitnessReply &)>> changes = {
      [](auto &r) { r.set_found(false); },
      [](auto &r) { r.set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_ALREADY_GRANTED); },
      [](auto &r) { r.set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_TOMBSTONED); },
      [](auto &r) { r.mutable_acting_owner()->CopyFrom(Worker()); },
      [](auto &r) { r.clear_task_spec(); },
      [](auto &r) { r.mutable_task_spec()->mutable_recovery_stream_descriptor()
                       ->set_expected_returns(4); },
      [](auto &r) { r.mutable_manifest()->set_recovery_attempt(2); },
      [](auto &r) {
        r.mutable_manifest()->set_tombstoned(true);
        r.mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(r.manifest());
      },
      [](auto &r) {
        r.mutable_manifest()->mutable_witness_raylets(0)->CopyFrom(Worker());
        r.mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(r.manifest());
      },
  };
  for (const auto &change : changes) {
    auto grant = Grant();
    change(grant);
    rpc::TaskSpec replay;
    replay.set_task_id("unchanged");
    EXPECT_FALSE(PrepareRecoveryStreamReplay(
        descriptor, descriptor.consumer_address(), grant, &replay).ok());
    EXPECT_EQ(replay.task_id(), "unchanged");
  }
}

}  // namespace
}  // namespace ray
