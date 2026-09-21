// Copyright 2026 The Ray Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

#include "ray/core_worker/core_worker.h"

#include <chrono>
#include <future>
#include <thread>

#include "ray/common/ray_config.h"
#include "ray/core_worker/task_manager.h"

namespace ray::core {
namespace {
using Deadline = std::chrono::steady_clock::time_point;

// Callbacks only fill a private promise. A late response after a frontend
// timeout cannot adopt refs, submit a task, or access a destroyed stack frame.
template <typename Reply, typename Send>
Status AwaitStreamRpc(Send send, Deadline deadline, Reply *reply) {
  auto promise = std::make_shared<std::promise<std::pair<Status, Reply>>>();
  auto future = promise->get_future();
  send([promise](const Status &status, const Reply &response) {
    promise->set_value({status, response});
  });
  if (future.wait_until(deadline) != std::future_status::ready) {
    return Status::TimedOut("Streaming recovery RPC deadline expired");
  }
  auto result = future.get();
  reply->Swap(&result.second);
  return result.first;
}
}  // namespace

bool CoreWorker::TryReleaseStreamingRecoveryReturn(const ObjectID &object_id) {
  std::vector<ObjectID> deleted;
  const bool released = reference_counter_->TryReleaseStreamingRecoveryReturn(object_id, &deleted);
  memory_store_->Delete(deleted);
  return released;
}

Status CoreWorker::ValidateStreamingRecoveryInputs(
    const std::vector<ObjectID> &object_ids) const {
  return reference_counter_->ValidateStreamingRecoveryInputs(object_ids);
}

Status CoreWorker::RecoverStreamingTask(
    const std::string &serialized_descriptor,
    int64_t next_index,
    const std::vector<ObjectID> &live_consumed_returns,
    int64_t timeout_ms,
    rpc::ObjectReference *generator_ref) {
  rpc::RecoveryStreamDescriptor descriptor;
  if (!RayConfig::instance().enable_recovery_streaming_fixed_r() ||
      !recovery_succession_enabled_ || !recovery_witness_holder_baseline_enabled_ ||
      generator_ref == nullptr || timeout_ms <= 0 ||
      !descriptor.ParseFromString(serialized_descriptor)) {
    return Status::Invalid("Invalid streaming recovery request or disabled feature");
  }
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamDescriptor(descriptor));
  if (descriptor.consumer_address().SerializeAsString() != rpc_address_.SerializeAsString() ||
      descriptor.manifest().job_id() != GetCurrentJobId().Binary() ||
      next_index < 0 || next_index > RecoveryStreamReturnLimit(descriptor)) {
    return Status::Invalid("Streaming recovery requires the designated consumer and cursor");
  }
  const auto task_id = TaskID::FromBinary(descriptor.task_id());
  const auto generator_id = ObjectID::FromBinary(descriptor.generator_id());
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    if (recovery_stream_submissions_.contains(task_id)) {
      return Status::Invalid("Streaming recovery already exists locally");
    }
  }
  SubscribeToNodeChanges();
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  const auto owner_node = NodeID::FromBinary(
      descriptor.manifest().succession(0).address().node_id());
  // An actor/worker RPC failure is not node-failure authority. Wait for GCS;
  // the selected witness independently enforces its own owner-node death view.
  while (!gcs_client_->Nodes().IsNodeDead(owner_node)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return Status::TimedOut("Original streaming owner node is not known dead");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  rpc::GetRecoveryWitnessReply grant;
  bool granted = false;
  while (!granted && std::chrono::steady_clock::now() < deadline) {
    for (const auto &witness : descriptor.manifest().witness_raylets()) {
      if (gcs_client_->Nodes().IsNodeDead(NodeID::FromBinary(witness.node_id()))) {
        continue;
      }
      rpc::GetRecoveryWitnessRequest request;
      request.set_task_id(descriptor.task_id());
      request.set_claim_recovery(true);
      request.mutable_claimant_address()->CopyFrom(rpc_address_);
      request.mutable_stream_descriptor()->CopyFrom(descriptor);
      auto client = raylet_client_pool_->GetOrConnectByAddress(witness);
      const auto status = AwaitStreamRpc<rpc::GetRecoveryWitnessReply>(
          [client, request](auto callback) mutable {
            client->GetRecoveryWitness(std::move(request), callback);
          },
          deadline,
          &grant);
      if (status.IsTimedOut()) {
        return status;
      }
      if (!status.ok()) {
        continue;
      }
      if (grant.claim_result() == rpc::GetRecoveryWitnessReply::CLAIM_TOMBSTONED ||
          grant.claim_result() == rpc::GetRecoveryWitnessReply::CLAIM_RETRY_LIMIT_EXCEEDED ||
          grant.claim_result() == rpc::GetRecoveryWitnessReply::CLAIM_ALREADY_GRANTED) {
        return Status::Invalid("Streaming recovery claim is terminal or belongs elsewhere");
      }
      if (grant.claim_result() == rpc::GetRecoveryWitnessReply::CLAIM_GRANTED) {
        granted = true;
        break;
      }
      // CLAIM_INVALID may mean this witness has not yet observed GCS death.
    }
    if (!granted) {
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
  }
  if (!granted) {
    return Status::TimedOut("No streaming witness grant before deadline");
  }

  rpc::TaskSpec replay;
  rpc::ObjectReference completion;
  const auto adopted = future_resolver_->AdoptStreamingRecovery([&] {
    return task_manager_->AddPendingStreamingTaskFromWitness(rpc_address_,
                                                            descriptor,
                                                            grant,
                                                            next_index,
                                                            live_consumed_returns,
                                                            &completion,
                                                            &replay);
  });
  RAY_RETURN_NOT_OK(adopted);
  auto state = std::make_shared<RecoveryStreamSubmission>();
  state->recipe.CopyFrom(replay);
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    RAY_CHECK(recovery_stream_submissions_.emplace(task_id, state).second);
  }
  auto fail_after_adoption = [&](const Status &status) {
    // No frontend owns the acquired completion ref yet. Cancel the held task,
    // request stream deletion, and release that one reference on every error.
    AsyncDelObjectRefStream(generator_id);
    RemoveLocalReference(generator_id);
    return status;
  };
  std::vector<ObjectID> adopted_ids(live_consumed_returns);
  adopted_ids.push_back(generator_id);
  for (const auto &id : adopted_ids) {
    future_resolver_->ResolveFutureAsync(id, rpc_address_);
  }
  auto local_raylet = GetRayletRpcClient(GetCurrentNodeId());
  if (local_raylet == nullptr) {
    return fail_after_adoption(Status::Invalid("Local streaming raylet is unavailable"));
  }
  rpc::PrepareStreamingRecoveryRequest barrier;
  barrier.mutable_stream_descriptor()->CopyFrom(descriptor);
  for (const auto &id : adopted_ids) {
    barrier.add_object_ids(id.Binary());
  }
  rpc::PrepareStreamingRecoveryReply barrier_reply;
  Status status;
  do {
    status = AwaitStreamRpc<rpc::PrepareStreamingRecoveryReply>(
        [local_raylet, barrier](auto callback) {
          local_raylet->PrepareStreamingRecovery(barrier, callback);
        },
        deadline,
        &barrier_reply);
    if (status.IsTimedOut() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  } while (status.IsTimedOut() && std::chrono::steady_clock::now() < deadline);
  if (!status.ok()) {
    return fail_after_adoption(status);
  }

  // The acknowledged raylet barrier fences old location callbacks. Delete
  // only stale errors, preserving healthy locally available consumed outputs.
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> local_values;
  status = plasma_store_provider_->GetIfLocal(adopted_ids, &local_values);
  if (!status.ok()) {
    return fail_after_adoption(status);
  }
  absl::flat_hash_set<ObjectID> stale;
  for (const auto &[id, value] : local_values) {
    rpc::ErrorType error;
    if (value && value->IsException(&error) && error == rpc::ErrorType::OWNER_DIED) {
      stale.insert(id);
    }
  }
  local_values.clear();  // Release Plasma buffers before requesting deletion.
  if (!stale.empty()) {
    status = plasma_store_provider_->Delete(stale, /*local_only=*/true);
    if (!status.ok()) {
      return fail_after_adoption(status);
    }
    for (const auto &id : stale) {
      bool present = true;
      while (present) {
        status = plasma_store_provider_->Contains(id, &present);
        if (!status.ok()) {
          return fail_after_adoption(status);
        }
        if (present && std::chrono::steady_clock::now() >= deadline) {
          return fail_after_adoption(Status::TimedOut("Stale Plasma error remains pinned"));
        }
        if (present) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      }
    }
  }
  if (std::chrono::steady_clock::now() >= deadline) {
    return fail_after_adoption(Status::TimedOut("Streaming preparation deadline expired"));
  }
  {
    std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
    if (state->cancelled) {
      // Cleanup below cannot be invoked while holding the map mutex.
      status = Status::Invalid("Streaming recovery cancelled during preparation");
    } else {
      state->dispatch_scheduled = true;
    }
  }
  if (!status.ok()) {
    return fail_after_adoption(status);
  }
  io_service_.post(
      [this, state] {
        {
          std::lock_guard<std::mutex> lock(recovery_stream_submission_mutex_);
          if (state->cancelled) {
            return;
          }
          state->submitted = true;
        }
        normal_task_submitter_->SubmitTask(TaskSpecification(state->recipe));
      },
      "CoreWorker.DispatchStreamingReplay");
  generator_ref->Swap(&completion);
  return Status::OK();
}

Status CoreWorker::CloseStreamingRecovery(const std::string &serialized_descriptor,
                                         int64_t timeout_ms) {
  rpc::RecoveryStreamDescriptor descriptor;
  if (!RayConfig::instance().enable_recovery_streaming_fixed_r() ||
      !recovery_succession_enabled_ || !recovery_witness_holder_baseline_enabled_ ||
      timeout_ms <= 0 || !descriptor.ParseFromString(serialized_descriptor)) {
    return Status::Invalid("Invalid streaming close request");
  }
  RAY_RETURN_NOT_OK(ValidateRecoveryStreamDescriptor(descriptor));
  if (descriptor.consumer_address().SerializeAsString() != rpc_address_.SerializeAsString()) {
    return Status::Invalid("Only the designated consumer may close this stream");
  }
  CancelRecoveryStreamSubmission(ObjectID::FromBinary(descriptor.generator_id()));
  rpc::RecoveryManifest tombstone(descriptor.manifest());
  tombstone.set_tombstoned(true);
  tombstone.mutable_version()->set_generation(tombstone.version().generation() + 1);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
  // Explicit close is an all-surviving-holder ACK barrier. GC remains a best
  // effort fallback. No new replay is dispatched by any callback in this API.
  for (const auto &witness : descriptor.manifest().witness_raylets()) {
    if (gcs_client_->Nodes().IsNodeDead(NodeID::FromBinary(witness.node_id()))) {
      continue;
    }
    rpc::UpdateRecoveryWitnessRequest request;
    request.mutable_manifest()->CopyFrom(tombstone);
    auto client = raylet_client_pool_->GetOrConnectByAddress(witness);
    rpc::UpdateRecoveryWitnessReply reply;
    RAY_RETURN_NOT_OK(AwaitStreamRpc<rpc::UpdateRecoveryWitnessReply>(
        [client, request](auto callback) mutable {
          client->UpdateRecoveryWitness(std::move(request), callback);
        },
        deadline,
        &reply));
    if (!reply.stored()) {
      return Status::Invalid("Streaming tombstone was not acknowledged");
    }
  }
  return Status::OK();
}
}  // namespace ray::core
