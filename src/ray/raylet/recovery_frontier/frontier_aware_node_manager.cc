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
      recovery_frontier_store_.EraseGroup(
          TaskID::FromBinary(request.manifest().task_id()));
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

}  // namespace ray::raylet
