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

#include "ray/core_worker/recovery_succession_manager.h"

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

namespace ray::core {
namespace {

std::mutex &AdaptiveFrontierPublisherMutex() {
  static std::mutex mutex;
  return mutex;
}

RecoverySuccessionManager::AdaptiveFrontierPublisher &AdaptiveFrontierPublisherSlot() {
  static RecoverySuccessionManager::AdaptiveFrontierPublisher publisher;
  return publisher;
}

RecoverySuccessionManager::AdaptiveFrontierPublisher GetAdaptiveFrontierPublisher() {
  std::lock_guard<std::mutex> lock(AdaptiveFrontierPublisherMutex());
  return AdaptiveFrontierPublisherSlot();
}

}  // namespace

void RecoverySuccessionManager::RegisterAdaptiveFrontierPublisher(
    AdaptiveFrontierPublisher publisher) {
  std::lock_guard<std::mutex> lock(AdaptiveFrontierPublisherMutex());
  AdaptiveFrontierPublisherSlot() = std::move(publisher);
}

bool RecoverySuccessionManager::PublishAdaptiveRecoveryFrontierForObjectIfNeeded(
    const ObjectID &object_id) {
  if (object_id.IsNil() ||
      RayConfig::instance().enable_recovery_witness_holder_baseline()) {
    return false;
  }

  const TaskID task_id = object_id.TaskId();

  for (;;) {
    RecoveryFrontierAppendBatch batch;
    rpc::RecoveryManifest protection_manifest;
    bool staged = false;
    bool wait_for_existing_publication = false;

    {
      absl::MutexLock lock(&mutex_);

      if (recovery_frontier_planner_ == nullptr ||
          recovery_frontier_planner_->GroupSize() <= 1) {
        return false;
      }

      const auto membership = recovery_frontier_planner_->FindTask(task_id);
      if (!membership.has_value()) {
        return false;
      }

      RecoveryFrontierGroup *group =
          recovery_frontier_planner_->GetMutableGroup(membership->group_id);
      if (group == nullptr) {
        return false;
      }

      // Initial candidate formation is deliberately allowed before any
      // non-owner holder exists. Dynamic publication begins only after the
      // shared adaptive topology is fully committed/frozen.
      const auto protection_it =
          recovery_frontier_protection_manifests_.find(membership->group_id);
      if (protection_it == recovery_frontier_protection_manifests_.end() ||
          !protection_it->second.frozen()) {
        return false;
      }

      if (group->IsTaskCommitted(task_id)) {
        return true;
      }

      if (group->AppendInFlight()) {
        wait_for_existing_publication = true;
      } else {
        auto next = group->StageAppend();
        if (!next.has_value()) {
          return false;
        }
        batch = std::move(next.value());
        protection_manifest.CopyFrom(protection_it->second);
        staged = true;
      }
    }

    if (wait_for_existing_publication) {
      // A concurrent exporter owns the one legal append generation. It cannot
      // publish metadata for this member until that generation commits, so wait
      // behind the same owner-side prefix barrier rather than letting an
      // unprotected ObjectRef escape.
      std::this_thread::sleep_for(std::chrono::microseconds(50));
      continue;
    }

    RAY_CHECK(staged);

    rpc::RecoveryFrontierAppend append;
    if (!BuildRecoveryFrontierAppendProto(batch, &append)) {
      const bool aborted = AbortRecoveryFrontierAppend(batch);
      RAY_CHECK(aborted);
      return false;
    }

    const AdaptiveFrontierPublisher publisher = GetAdaptiveFrontierPublisher();
    if (!publisher) {
      // Standalone manager tests do not construct CoreWorker's publication
      // translation unit. Preserve the old state-only behavior there.
      const bool aborted = AbortRecoveryFrontierAppend(batch);
      RAY_CHECK(aborted);
      return false;
    }

    if (!publisher(protection_manifest, append)) {
      const bool aborted = AbortRecoveryFrontierAppend(batch);
      RAY_CHECK(aborted)
          << "Failed to abort adaptive Recovery Frontier append generation "
          << batch.generation << " for group " << batch.group_id;
      RAY_LOG(FATAL)
          .WithField(batch.group_id)
          << "Adaptive Recovery Frontier failed to install recipe append generation "
          << batch.generation << " on every admitted Succession holder";
      return false;
    }

    const bool committed =
        CommitAdaptiveRecoveryFrontierAppend(batch, protection_manifest);
    RAY_CHECK(committed)
        << "Stale or mismatched adaptive Recovery Frontier append ACK for generation "
        << batch.generation << " group " << batch.group_id;

    // CommitAdaptiveRecoveryFrontierAppend installs the authoritative member
    // manifests, but dynamically appended members may not yet have gone through
    // RegisterOwnedTaskLazy. BuildRecoveryMetadataLocked deliberately requires
    // owned_num_returns > 0 before treating an ObjectID as an owner return.
    // Populate that owner-local identity from the already-committed Frontier
    // recipe before allowing the export-side metadata lookup to resume.
    //
    // This does not make the member visible early: we reach this block only
    // after every existing H1..HR ACKed the exact suffix and CommitAppend()
    // advanced the durable prefix.
    {
      absl::MutexLock lock(&mutex_);
      for (const RecoveryFrontierMember &member : batch.members) {
        RAY_CHECK_GT(member.num_returns, 0U);

        auto state_it = task_states_.find(member.task_id);
        RAY_CHECK(state_it != task_states_.end())
            << "Committed adaptive Frontier member is missing task recovery state: "
            << member.task_id;
        RAY_CHECK_EQ(state_it->second.manifest.task_id(), member.task_id.Binary());

        if (state_it->second.owned_num_returns == 0) {
          state_it->second.owned_num_returns = member.num_returns;
        } else {
          RAY_CHECK_EQ(state_it->second.owned_num_returns, member.num_returns)
              << "Adaptive Frontier return-count mismatch for task "
              << member.task_id;
        }
      }
    }

    RAY_LOG(INFO)
        .WithField(batch.group_id)
        << "Committed adaptive Recovery Frontier recipe append generation "
        << batch.generation << " members=[" << batch.begin_member_index << ","
        << batch.end_member_index << ") on existing Succession holders";

    return true;
  }
}

bool RecoverySuccessionManager::HasRecoveryMetadata(const ObjectID &object_id) {
  const auto *const_view = static_cast<const RecoverySuccessionManager *>(this);
  if (const_view->HasRecoveryMetadata(object_id)) {
    return true;
  }

  if (!PublishAdaptiveRecoveryFrontierForObjectIfNeeded(object_id)) {
    return false;
  }

  return const_view->HasRecoveryMetadata(object_id);
}

bool RecoverySuccessionManager::PopulateRecoveryMetadata(
    const ObjectID &object_id, rpc::RecoveryObjectMetadata *metadata) {
  const auto *const_view = static_cast<const RecoverySuccessionManager *>(this);
  if (const_view->PopulateRecoveryMetadata(object_id, metadata)) {
    return true;
  }

  if (!PublishAdaptiveRecoveryFrontierForObjectIfNeeded(object_id)) {
    return false;
  }

  return const_view->PopulateRecoveryMetadata(object_id, metadata);
}

}  // namespace ray::core