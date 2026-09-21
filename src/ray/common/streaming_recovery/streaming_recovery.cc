// Copyright 2026 The Ray Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#include "ray/common/streaming_recovery/streaming_recovery.h"

#include <google/protobuf/util/message_differencer.h>

#include "ray/common/id.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace {

bool SameAddress(const rpc::Address &a, const rpc::Address &b) {
  return google::protobuf::util::MessageDifferencer::Equals(a, b);
}

bool ValidWorker(const rpc::Address &address) {
  return address.worker_id().size() == WorkerID::Size() &&
         !WorkerID::FromBinary(address.worker_id()).IsNil() &&
         address.node_id().size() == NodeID::Size() &&
         !NodeID::FromBinary(address.node_id()).IsNil() &&
         !address.ip_address().empty() && address.port() > 0 && address.port() <= 65535;
}

bool SameManifest(const rpc::RecoveryManifest &a, const rpc::RecoveryManifest &b) {
  return google::protobuf::util::MessageDifferencer::Equals(a, b);
}

}  // namespace

Status ValidateRecoveryStreamDescriptor(const rpc::RecoveryStreamDescriptor &d) {
  if (d.version() != 1 || d.task_id().size() != TaskID::Size() ||
      TaskID::FromBinary(d.task_id()).IsNil() ||
      d.generator_id().size() != ObjectID::Size() || d.expected_returns() < 0 ||
      static_cast<uint64_t>(d.expected_returns()) >=
          RayConfig::instance().max_num_generator_returns() ||
      !ValidWorker(d.consumer_address()) || !d.has_manifest()) {
    return Status::Invalid("Invalid streaming descriptor identity, count, or consumer");
  }
  const auto task_id = TaskID::FromBinary(d.task_id());
  if (d.generator_id() != ObjectID::FromIndex(task_id, 1).Binary()) {
    return Status::Invalid("Streaming completion ID does not match original task");
  }
  const auto &m = d.manifest();
  if (m.task_id() != d.task_id() || m.job_id() != task_id.JobId().Binary() ||
      m.tombstoned() || m.frozen() || m.recovery_attempt() != 0 ||
      m.version().generation() != 1 || m.max_recovery_attempts() == 0 ||
      m.max_recovery_attempts() < -1 || m.succession_size() != 1 ||
      m.succession(0).rank() != 0 || !ValidWorker(m.succession(0).address()) ||
      m.succession(0).failure_domain_id() != m.succession(0).address().node_id() ||
      m.target_holder_count() == 0 || m.witness_count() != m.target_holder_count() ||
      m.witness_count() != static_cast<uint32_t>(m.witness_raylets_size())) {
    return Status::Invalid("Streaming descriptor requires an initial K=1 Fixed-R manifest");
  }
  const auto &owner = m.succession(0).address();
  if (owner.node_id() == d.consumer_address().node_id() ||
      owner.worker_id() == d.consumer_address().worker_id()) {
    return Status::Invalid("Streaming consumer must survive the owner node");
  }
  std::unordered_set<std::string> nodes;
  for (const auto &witness : m.witness_raylets()) {
    if (witness.node_id().size() != NodeID::Size() ||
        NodeID::FromBinary(witness.node_id()).IsNil() ||
        witness.node_id() == owner.node_id() || witness.ip_address().empty() ||
        witness.port() <= 0 || witness.port() > 65535 ||
        !nodes.insert(witness.node_id()).second) {
      return Status::Invalid("Streaming holders must occupy distinct non-owner nodes");
    }
  }
  return Status::OK();
}

bool SameRecoveryStreamDescriptor(const rpc::RecoveryStreamDescriptor &a,
                                  const rpc::RecoveryStreamDescriptor &b) {
  return google::protobuf::util::MessageDifferencer::Equals(a, b);
}

Status ValidateRecoveryStreamRecipe(const rpc::TaskSpec &recipe) {
  if (!recipe.has_recovery_stream_descriptor()) {
    return Status::Invalid("Missing streaming recovery descriptor");
  }
  const auto &d = recipe.recovery_stream_descriptor();
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamDescriptor(d));
  if (recipe.task_id() != d.task_id() || recipe.job_id() != d.manifest().job_id() ||
      recipe.type() != rpc::TaskType::NORMAL_TASK || !recipe.streaming_generator() ||
      !recipe.returns_dynamic() || recipe.num_returns() != 1 ||
      recipe.num_objects_per_yield() != 1 || recipe.attempt_number() != 0 ||
      recipe.max_retries() != d.manifest().max_recovery_attempts() ||
      recipe.has_tensor_transport() ||
      !SameAddress(recipe.caller_address(), d.manifest().succession(0).address()) ||
      (recipe.has_num_streaming_generator_returns() &&
       recipe.num_streaming_generator_returns() !=
           static_cast<uint64_t>(d.expected_returns()))) {
    return Status::Invalid("Recipe does not satisfy the bounded streaming contract");
  }
  for (const auto &arg : recipe.args()) {
    if (arg.has_object_ref() || !arg.nested_inlined_refs().empty()) {
      return Status::Invalid("Streaming recovery requires by-value inputs without refs");
    }
  }
  if (!recipe.recovery_argument_metadata().empty()) {
    return Status::Invalid("Streaming recovery does not support dependency sidecars");
  }
  return Status::OK();
}

bool RecoveryStreamClaimantMatches(const rpc::TaskSpec &recipe,
                                   const rpc::Address &claimant) {
  return recipe.has_recovery_stream_descriptor() &&
         SameAddress(recipe.recovery_stream_descriptor().consumer_address(), claimant);
}

bool SameRecoveryStreamRecipe(const rpc::TaskSpec &left, const rpc::TaskSpec &right) {
  rpc::TaskSpec a(left);
  rpc::TaskSpec b(right);
  a.clear_recovery_manifest();
  b.clear_recovery_manifest();
  return google::protobuf::util::MessageDifferencer::Equals(a, b);
}

Status PrepareRecoveryStreamReplay(const rpc::RecoveryStreamDescriptor &d,
                                   const rpc::Address &consumer,
                                   const rpc::GetRecoveryWitnessReply &reply,
                                   rpc::TaskSpec *replay) {
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamDescriptor(d));
  if (replay == nullptr || !SameAddress(d.consumer_address(), consumer) ||
      !reply.found() || reply.claim_result() != rpc::GetRecoveryWitnessReply::CLAIM_GRANTED ||
      !reply.has_task_spec() || !reply.has_manifest() || !reply.has_acting_owner() ||
      !SameAddress(reply.acting_owner(), consumer)) {
    return Status::Invalid("Streaming replay requires a grant to the designated consumer");
  }
  const auto &recipe = reply.task_spec();
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamRecipe(recipe));
  if (!SameRecoveryStreamDescriptor(d, recipe.recovery_stream_descriptor()) ||
      !recipe.has_recovery_manifest() ||
      !SameManifest(recipe.recovery_manifest(), reply.manifest()) ||
      reply.manifest().recovery_attempt() != 1) {
    return Status::Invalid("Streaming claim does not match the installed recipe");
  }
  rpc::RecoveryManifest initial(reply.manifest());
  initial.set_recovery_attempt(0);
  if (!SameManifest(initial, d.manifest())) {
    return Status::Invalid("Streaming claim changed the installation topology or lifetime");
  }
  rpc::TaskSpec prepared(recipe);
  prepared.mutable_caller_address()->CopyFrom(consumer);
  prepared.set_attempt_number(1);
  prepared.set_num_streaming_generator_returns(d.expected_returns());
  auto *strategy = prepared.mutable_scheduling_strategy();
  if (strategy->has_node_affinity_scheduling_strategy() &&
      strategy->node_affinity_scheduling_strategy().soft()) {
    strategy->clear_scheduling_strategy();
    strategy->mutable_default_scheduling_strategy();
  }
  replay->Swap(&prepared);
  return Status::OK();
}

Status RecoveryStreamInstallation::Initialize(const rpc::RecoveryStreamDescriptor &d) {
  if (descriptor_.has_value() || terminal_) {
    return Status::Invalid("Streaming installation cannot be reinitialized");
  }
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamDescriptor(d));
  descriptor_ = d;
  return Status::OK();
}

Status RecoveryStreamInstallation::RecordWitnessReply(
    const rpc::Address &witness,
    const Status &status,
    const rpc::UpdateRecoveryWitnessReply &reply) {
  if (!descriptor_.has_value() || terminal_) {
    return Status::Invalid("Streaming installation is inactive");
  }
  bool selected = false;
  for (const auto &candidate : descriptor_->manifest().witness_raylets()) {
    selected |= SameAddress(witness, candidate);
  }
  if (!selected) {
    return Status::Invalid("Streaming installation ACK is from an unselected holder");
  }
  if (!status.ok() || !reply.stored() || reply.has_latest_recovery_claim() ||
      (reply.has_latest_manifest() &&
       !SameManifest(reply.latest_manifest(), descriptor_->manifest()))) {
    terminal_ = true;
    return Status::Invalid("Streaming full-recipe installation failed or was superseded");
  }
  // Set insertion makes duplicate successful callbacks idempotent.
  acknowledged_nodes_.insert(witness.node_id());
  return Status::OK();
}

Status RecoveryStreamInstallation::RecordConsumerReceipt(
    const rpc::Address &consumer, const rpc::RecoveryStreamDescriptor &d) {
  if (!descriptor_.has_value() || terminal_ ||
      !SameAddress(consumer, descriptor_->consumer_address()) ||
      !SameRecoveryStreamDescriptor(d, *descriptor_)) {
    return Status::Invalid("Streaming receipt does not match the designated enrollment");
  }
  consumer_received_ = true;
  return Status::OK();
}

void RecoveryStreamInstallation::Cancel() { terminal_ = true; }

bool RecoveryStreamInstallation::IsReady() const {
  return descriptor_.has_value() && !terminal_ && consumer_received_ &&
         acknowledged_nodes_.size() == descriptor_->manifest().witness_count();
}

std::optional<rpc::RecoveryStreamDescriptor>
RecoveryStreamInstallation::ReadyDescriptor() const {
  return IsReady() ? descriptor_ : std::nullopt;
}

}  // namespace ray
