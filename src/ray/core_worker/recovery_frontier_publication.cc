// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/core_worker.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/core_worker/recovery_frontier_wire/recovery_frontier_wire.h"
#include "ray/core_worker/recovery_succession_manager.h"
#include "ray/util/logging.h"

namespace ray::core {

void CoreWorker::PublishRecoveryFrontierGroup(
    const TaskID &group_id,
    const rpc::RecoveryManifest &protection_manifest) const {
  if (!recovery_succession_enabled_ ||
      !recovery_witness_holder_baseline_enabled_ ||
      recovery_succession_manager_ == nullptr ||
      !recovery_succession_manager_->RecoveryFrontierEnabled() ||
      group_id.IsNil() ||
      protection_manifest.task_id() != group_id.Binary()) {
    return;
  }

  // Stage every member that is currently pending. If tasks were submitted
  // before the first exported ObjectRef activates the group, this turns them
  // into one physical fixed-R append. At most one append may be in flight per
  // group; a completion callback below drains any suffix that arrived while
  // this publication was outstanding.
  auto staged =
      recovery_succession_manager_->StageRecoveryFrontierAppend(group_id);
  if (!staged.has_value()) {
    return;
  }

  auto batch = std::make_shared<RecoveryFrontierAppendBatch>(
      std::move(staged.value()));
  const std::string serialized_append =
      BuildRecoveryFrontierAppendEnvelope(*batch);

  const uint32_t target_holder_count =
      RayConfig::instance().recovery_succession_target_holder_count();
  RAY_CHECK_EQ(
      static_cast<uint32_t>(protection_manifest.witness_raylets_size()),
      target_holder_count)
      << "Recovery Frontier fixed-R publication requires exactly "
      << target_holder_count << " holder raylets for group " << group_id;
  RAY_CHECK_EQ(protection_manifest.witness_count(), target_holder_count);

  // Passing a serialized payload makes PublishRecoveryManifestToWitnesses use
  // the fixed-R all-witness durability rule. The raylet frontier wrapper
  // recognizes the versioned envelope, applies the group manifest through the
  // legacy witness path, then atomically advances its local capsule prefix.
  PublishRecoveryManifestToWitnesses(
      protection_manifest,
      [this,
       manager = recovery_succession_manager_,
       group_id,
       protection_manifest,
       batch](bool stored,
              std::optional<rpc::RecoveryManifest> newer_manifest) mutable {
        if (!stored) {
          const bool aborted =
              manager->AbortRecoveryFrontierAppend(*batch);
          RAY_CHECK(aborted)
              << "Failed to abort Recovery Frontier append generation "
              << batch->generation << " for group " << group_id;

          // Keep fixed-R semantics strict. A partial append must never advance
          // the owner committed prefix. Unlike ordinary per-task cleanup, group
          // tombstone/liveness is handled separately and is not yet allowed to
          // silently supersede a live frontier append.
          RAY_LOG(FATAL)
              .WithField(group_id)
              << "Recovery Frontier failed to install append generation "
              << batch->generation << " on every fixed-R holder."
              << (newer_manifest.has_value()
                      ? " A newer holder manifest was observed."
                      : "");
          return;
        }

        const bool committed =
            manager->CommitRecoveryFrontierAppend(*batch);
        RAY_CHECK(committed)
            << "Stale or mismatched Recovery Frontier ACK for generation "
            << batch->generation << " group " << group_id;

        RAY_LOG(INFO)
            .WithField(group_id)
            << "Committed Recovery Frontier append generation "
            << batch->generation << " members=["
            << batch->begin_member_index << ","
            << batch->end_member_index << ") on all fixed-R holders";

        // Tasks may have joined the open group while this append was in flight.
        // Stage and publish that contiguous suffix now. If there is no suffix,
        // this is a cheap no-op.
        PublishRecoveryFrontierGroup(group_id, protection_manifest);
      },
      /*task_spec=*/nullptr,
      &serialized_append);
}

}  // namespace ray::core
