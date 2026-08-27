// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/raylet/recovery_frontier/frontier_aware_node_manager.h"

#include <functional>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray::raylet {

void FrontierAwareNodeManager::HandleUpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest request,
    rpc::UpdateRecoveryWitnessReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const bool has_frontier_envelope =
      IsRecoveryFrontierAppendEnvelope(request.serialized_task_spec());
  const bool tombstone_request =
      request.has_manifest() && request.manifest().tombstoned();
  const TaskID tombstoned_group_id =
      tombstone_request ? TaskID::FromBinary(request.manifest().task_id()) : TaskID::Nil();

  // Keep the existing fixed-R and Succession hot paths exactly unchanged.
  if (!has_frontier_envelope && !tombstone_request) {
    NodeManager::HandleUpdateRecoveryWitness(
        std::move(request), reply, std::move(send_reply_callback));
    return;
  }

  rpc::RecoveryFrontierAppend frontier_append;
  if (has_frontier_envelope) {
    // Frontier appends and the legacy full-TaskSpec payload are mutually
    // exclusive. A frontier update carries its leader/group manifest plus the
    // versioned append envelope in serialized_task_spec.
    if (request.has_task_spec() || !request.has_manifest() ||
        request.manifest().tombstoned() ||
        !ParseRecoveryFrontierAppendEnvelope(request.serialized_task_spec(),
                                             &frontier_append) ||
        frontier_append.group_id() != request.manifest().task_id()) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
  }

  // Let the existing handler remain authoritative for manifest versioning,
  // certificate/tombstone semantics, and the enabled-backend checks. Strip the
  // frontier envelope first so the legacy handler sees a normal manifest-only
  // update rather than trying to parse it as a serialized TaskSpec.
  rpc::UpdateRecoveryWitnessRequest legacy_request;
  legacy_request.Swap(&request);
  if (has_frontier_envelope) {
    legacy_request.clear_serialized_task_spec();
  }

  bool legacy_replied = false;
  Status legacy_status = Status::OK();
  std::function<void()> legacy_success;
  std::function<void()> legacy_failure;

  NodeManager::HandleUpdateRecoveryWitness(
      std::move(legacy_request),
      reply,
      [&legacy_replied, &legacy_status, &legacy_success, &legacy_failure](
          Status status,
          std::function<void()> success,
          std::function<void()> failure) {
        legacy_replied = true;
        legacy_status = std::move(status);
        legacy_success = std::move(success);
        legacy_failure = std::move(failure);
      });

  // The existing NodeManager handler is synchronous. Preserve that invariant
  // explicitly because the frontier commit must happen before its ACK is sent.
  RAY_CHECK(legacy_replied);

  if (legacy_status.ok() && reply->stored()) {
    absl::MutexLock lock(&recovery_frontier_mutex_);

    if (tombstone_request) {
      // Claims are per original member task even though storage is grouped.
      // Remove every claim whose member belongs to the terminal group before
      // deleting the member aliases themselves.
      for (auto it = recovery_frontier_claims_.begin();
           it != recovery_frontier_claims_.end();) {
        RecoveryFrontierStore::CommittedMember member;
        if (recovery_frontier_store_.LookupCommittedMember(it->first, &member) &&
            member.group_id == tombstoned_group_id) {
          const auto erase_it = it++;
          recovery_frontier_claims_.erase(erase_it);
        } else {
          ++it;
        }
      }
      recovery_frontier_store_.EraseGroup(tombstoned_group_id);
    } else if (has_frontier_envelope) {
      const RecoveryFrontierStore::ApplyResult result =
          recovery_frontier_store_.ApplyAppend(frontier_append);
      if (result != RecoveryFrontierStore::ApplyResult::APPLIED &&
          result != RecoveryFrontierStore::ApplyResult::IDEMPOTENT) {
        // The manifest may already be materialized by the legacy handler, but
        // the caller does not receive an ACK for this append. The owner must
        // therefore not advance/advertise the frontier committed prefix and
        // may safely retry a valid contiguous append.
        reply->set_stored(false);
      }
    }
  }

  send_reply_callback(std::move(legacy_status),
                      std::move(legacy_success),
                      std::move(legacy_failure));
}

void FrontierAwareNodeManager::HandleUpdateRecoveryWitnessBatch(
    rpc::UpdateRecoveryWitnessBatchRequest request,
    rpc::UpdateRecoveryWitnessBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Route every logical update through the frontier-aware single-item handler
  // so transport batching cannot bypass append validation or commit ordering.
  for (int i = 0; i < request.updates_size(); ++i) {
    rpc::UpdateRecoveryWitnessRequest item_request;
    item_request.Swap(request.mutable_updates(i));

    auto *item_reply = reply->add_replies();
    HandleUpdateRecoveryWitness(
        std::move(item_request),
        item_reply,
        [](Status, std::function<void()>, std::function<void()>) {});
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void FrontierAwareNodeManager::HandleGetRecoveryWitness(
    rpc::GetRecoveryWitnessRequest request,
    rpc::GetRecoveryWitnessReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (request.task_id().size() != TaskID::Size()) {
    NodeManager::HandleGetRecoveryWitness(
        std::move(request), reply, std::move(send_reply_callback));
    return;
  }

  const TaskID task_id = TaskID::FromBinary(request.task_id());
  RecoveryFrontierStore::CommittedMember member;
  {
    absl::MutexLock lock(&recovery_frontier_mutex_);
    if (!recovery_frontier_store_.LookupCommittedMember(task_id, &member)) {
      NodeManager::HandleGetRecoveryWitness(
          std::move(request), reply, std::move(send_reply_callback));
      return;
    }
  }

  // Reuse the base NodeManager as the authority for the group's mutable
  // manifest. Frontier storage contains replay recipes only. A compact lookup
  // is synchronous in NodeManager, so no lock is held while delegating.
  rpc::GetRecoveryWitnessRequest group_request;
  group_request.set_task_id(member.group_id.Binary());
  rpc::GetRecoveryWitnessReply group_reply;
  bool group_replied = false;
  Status group_status = Status::OK();
  std::function<void()> group_success;
  std::function<void()> group_failure;

  NodeManager::HandleGetRecoveryWitness(
      std::move(group_request),
      &group_reply,
      [&group_replied, &group_status, &group_success, &group_failure](
          Status status,
          std::function<void()> success,
          std::function<void()> failure) {
        group_replied = true;
        group_status = std::move(status);
        group_success = std::move(success);
        group_failure = std::move(failure);
      });
  RAY_CHECK(group_replied);

  if (!group_status.ok()) {
    send_reply_callback(std::move(group_status),
                        std::move(group_success),
                        std::move(group_failure));
    return;
  }
  if (!group_reply.found() || !group_reply.has_manifest() ||
      group_reply.manifest().task_id() != member.group_id.Binary()) {
    reply->set_found(false);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  rpc::RecoveryManifest member_manifest;
  member_manifest.CopyFrom(group_reply.manifest());
  member_manifest.set_task_id(task_id.Binary());
  member_manifest.set_max_recovery_attempts(member.task_spec.max_retries());

  reply->set_found(true);

  if (!request.claim_recovery()) {
    reply->mutable_manifest()->CopyFrom(member_manifest);
    reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_NOT_REQUESTED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const bool baseline_enabled =
      RayConfig::instance().enable_recovery_succession() &&
      RayConfig::instance().enable_recovery_witness_holder_baseline();
  if (!baseline_enabled || !request.has_claimant_address() ||
      request.claimant_address().worker_id().size() != WorkerID::Size() ||
      request.claimant_address().node_id().size() != NodeID::Size()) {
    reply->set_found(false);
    reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_INVALID);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  if (member_manifest.tombstoned()) {
    reply->mutable_manifest()->CopyFrom(member_manifest);
    reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_TOMBSTONED);
    send_reply_callback(Status::OK(), nullptr, nullptr);
    return;
  }

  const auto same_worker = [](const rpc::Address &left, const rpc::Address &right) {
    return left.worker_id() == right.worker_id() &&
           left.node_id() == right.node_id();
  };

  {
    absl::MutexLock lock(&recovery_frontier_mutex_);

    // Revalidate the alias after the group-manifest lookup. A concurrent
    // tombstone may have erased the group while no frontier lock was held.
    RecoveryFrontierStore::CommittedMember current_member;
    if (!recovery_frontier_store_.LookupCommittedMember(task_id, &current_member) ||
        current_member.group_id != member.group_id) {
      reply->set_found(false);
      reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_INVALID);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }

    auto claim_it = recovery_frontier_claims_.find(task_id);
    if (claim_it != recovery_frontier_claims_.end()) {
      member_manifest.set_recovery_attempt(claim_it->second.recovery_attempt);
      reply->mutable_acting_owner()->CopyFrom(claim_it->second.acting_owner);

      if (same_worker(claim_it->second.acting_owner, request.claimant_address())) {
        // Idempotent retry by the already-selected acting owner.
        reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_GRANTED);
        reply->mutable_task_spec()->CopyFrom(current_member.task_spec);
        reply->mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(
            member_manifest);
      } else {
        reply->set_claim_result(
            rpc::GetRecoveryWitnessReply::CLAIM_ALREADY_GRANTED);
      }
      reply->mutable_manifest()->CopyFrom(member_manifest);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }

    const int32_t max_recovery_attempts = current_member.task_spec.max_retries();
    const uint32_t current_attempt = 0;
    if (max_recovery_attempts >= 0 &&
        current_attempt >= static_cast<uint32_t>(max_recovery_attempts)) {
      member_manifest.set_recovery_attempt(current_attempt);
      reply->mutable_manifest()->CopyFrom(member_manifest);
      reply->set_claim_result(
          rpc::GetRecoveryWitnessReply::CLAIM_RETRY_LIMIT_EXCEEDED);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }

    FrontierMemberClaimState claim_state;
    claim_state.acting_owner.CopyFrom(request.claimant_address());
    claim_state.recovery_attempt = current_attempt + 1;
    member_manifest.set_recovery_attempt(claim_state.recovery_attempt);
    recovery_frontier_claims_[task_id] = claim_state;

    reply->set_claim_result(rpc::GetRecoveryWitnessReply::CLAIM_GRANTED);
    reply->mutable_acting_owner()->CopyFrom(request.claimant_address());
    reply->mutable_manifest()->CopyFrom(member_manifest);
    reply->mutable_task_spec()->CopyFrom(current_member.task_spec);
    reply->mutable_task_spec()->mutable_recovery_manifest()->CopyFrom(member_manifest);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

}  // namespace ray::raylet
