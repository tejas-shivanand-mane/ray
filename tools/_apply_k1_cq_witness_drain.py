#!/usr/bin/env python3
from pathlib import Path


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one occurrence, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# Generic gRPC client: allow one optional CQ-thread hook to run after the
# completed RPC's logical callback has been posted to the main event loop.
# Existing callers leave the hook empty, so their behavior is unchanged.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/rpc/client_call.h",
    '''#include <cstdint>\n#include <memory>\n''',
    '''#include <cstdint>\n#include <functional>\n#include <memory>\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''  uint64_t RecoveryWitnessMainLoopStartTimeNs() const {\n    return recovery_witness_main_loop_start_time_ns_;\n  }\n\n  virtual ~ClientCall() = default;\n''',
    '''  uint64_t RecoveryWitnessMainLoopStartTimeNs() const {\n    return recovery_witness_main_loop_start_time_ns_;\n  }\n\n  void SetCompletionQueueHook(std::function<void()> hook) {\n    completion_queue_hook_ = std::move(hook);\n  }\n\n  void RunCompletionQueueHook() {\n    if (completion_queue_hook_) {\n      auto hook = std::move(completion_queue_hook_);\n      hook();\n    }\n  }\n\n  virtual ~ClientCall() = default;\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''  uint64_t recovery_witness_submit_time_ns_ = 0;\n  uint64_t recovery_witness_cq_receive_time_ns_ = 0;\n  uint64_t recovery_witness_main_loop_start_time_ns_ = 0;\n};\n''',
    '''  uint64_t recovery_witness_submit_time_ns_ = 0;\n  uint64_t recovery_witness_cq_receive_time_ns_ = 0;\n  uint64_t recovery_witness_main_loop_start_time_ns_ = 0;\n  std::function<void()> completion_queue_hook_;\n};\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''  explicit ClientCallImpl(const ClientCallback<Reply> &callback,\n                          const ClusterID &cluster_id,\n                          std::shared_ptr<StatsHandle> stats_handle,\n                          bool record_stats,\n                          int64_t timeout_ms = -1)\n''',
    '''  explicit ClientCallImpl(const ClientCallback<Reply> &callback,\n                          const ClusterID &cluster_id,\n                          std::shared_ptr<StatsHandle> stats_handle,\n                          bool record_stats,\n                          int64_t timeout_ms = -1,\n                          std::function<void()> completion_queue_hook = nullptr)\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n''',
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n    SetCompletionQueueHook(std::move(completion_queue_hook));\n    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name,\n      int64_t method_timeout_ms = -1) {\n''',
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name,\n      int64_t method_timeout_ms = -1,\n      std::function<void()> completion_queue_hook = nullptr) {\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''    auto call = std::make_shared<ClientCallImpl<Reply>>(\n        callback, cluster_id_, std::move(stats_handle), record_stats_, method_timeout_ms);\n''',
    '''    auto call = std::make_shared<ClientCallImpl<Reply>>(\n        callback,\n        cluster_id_,\n        std::move(stats_handle),\n        record_stats_,\n        method_timeout_ms,\n        std::move(completion_queue_hook));\n''',
)
replace_one(
    "src/ray/rpc/client_call.h",
    '''          main_service_.post(\n              [tag]() {\n                tag->GetCall()->MarkRecoveryWitnessMainLoopCallbackStarted();\n                tag->GetCall()->OnReplyReceived();\n                // The call is finished, and we can delete this tag now.\n                delete tag;\n              },\n              stats_handle->event_name + ".OnReplyReceived",\n              // Implement the delay of the rpc client call as the\n              // delay of OnReplyReceived().\n              ray::asio::testing::GetDelayUs(stats_handle->event_name));\n          main_service_.stats()->RecordEnd(std::move(stats_handle));\n''',
    '''          main_service_.post(\n              [tag]() {\n                tag->GetCall()->MarkRecoveryWitnessMainLoopCallbackStarted();\n                tag->GetCall()->OnReplyReceived();\n                // The call is finished, and we can delete this tag now.\n                delete tag;\n              },\n              stats_handle->event_name + ".OnReplyReceived",\n              // Implement the delay of the rpc client call as the\n              // delay of OnReplyReceived().\n              ray::asio::testing::GetDelayUs(stats_handle->event_name));\n\n          // CQ-driven transport hooks run only after the completed call's\n          // logical callback has been posted. This preserves main-loop callback\n          // ordering while allowing a transport lane to launch its next physical\n          // RPC without waiting for that posted callback to execute.\n          tag->GetCall()->RunCompletionQueueHook();\n          main_service_.stats()->RecordEnd(std::move(stats_handle));\n''',
)

# ---------------------------------------------------------------------------
# GrpcClient: thread the optional CQ hook through normal and chaos paths.
# Existing call sites compile unchanged because the new argument is optional.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''#include <boost/asio.hpp>\n#include <memory>\n''',
    '''#include <boost/asio.hpp>\n#include <functional>\n#include <memory>\n''',
)
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name = "UNKNOWN_RPC",\n      int64_t method_timeout_ms = -1) {\n''',
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name = "UNKNOWN_RPC",\n      int64_t method_timeout_ms = -1,\n      std::function<void()> completion_queue_hook = nullptr) {\n''',
)
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''      client_call_manager_.GetMainService().post(\n          [callback]() {\n            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),\n                     Reply());\n          },\n          "RpcChaos");\n''',
    '''      client_call_manager_.GetMainService().post(\n          [callback]() {\n            callback(Status::RpcError("Unavailable", grpc::StatusCode::UNAVAILABLE),\n                     Reply());\n          },\n          "RpcChaos");\n      if (completion_queue_hook) {\n        completion_queue_hook();\n      }\n''',
)
# Response failure CreateCall.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          },\n          std::move(call_name),\n          method_timeout_ms);\n    } else if (failure == testing::RpcFailure::InFlight) {\n''',
    '''          },\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook));\n    } else if (failure == testing::RpcFailure::InFlight) {\n''',
)
# In-flight failure CreateCall.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          },\n          std::move(call_name),\n          method_timeout_ms);\n      client_call_manager_.GetMainService().post(\n''',
    '''          },\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook));\n      client_call_manager_.GetMainService().post(\n''',
)
# Normal CreateCall.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          callback,\n          std::move(call_name),\n          method_timeout_ms);\n      RAY_CHECK(call != nullptr);\n''',
    '''          callback,\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook));\n      RAY_CHECK(call != nullptr);\n''',
)

# ---------------------------------------------------------------------------
# RayletClient: centralize lane advancement and, for ordinary K=1 only, run it
# from the CQ hook. Fixed-R/Frontier/cert/tombstone/claim paths keep the legacy
# main-loop advancement behavior.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.h",
    '''  static void DispatchRecoveryWitnessBatch(\n      std::shared_ptr<RecoveryWitnessBatchState> state,\n      std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client,\n      std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch);\n''',
    '''  static void AdvanceRecoveryWitnessBatchTransport(\n      std::shared_ptr<RecoveryWitnessBatchState> state,\n      std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client);\n\n  static void DispatchRecoveryWitnessBatch(\n      std::shared_ptr<RecoveryWitnessBatchState> state,\n      std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client,\n      std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch);\n''',
)
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''  DispatchRecoveryWitnessBatch(state, grpc_client_, std::move(batch));\n}\n\nvoid RayletClient::DispatchRecoveryWitnessBatch(\n''',
    '''  DispatchRecoveryWitnessBatch(state, grpc_client_, std::move(batch));\n}\n\nvoid RayletClient::AdvanceRecoveryWitnessBatchTransport(\n    std::shared_ptr<RecoveryWitnessBatchState> state,\n    std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client) {\n  auto next_batch =\n      std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();\n  {\n    std::lock_guard<std::mutex> lock(state->mutex);\n    const size_t count = std::min(\n        RayletClient::kRecoveryWitnessBatchMaxSize, state->pending.size());\n\n    if (count == 0) {\n      state->in_flight = false;\n      return;\n    }\n\n    next_batch->reserve(count);\n    for (size_t i = 0; i < count; ++i) {\n      next_batch->emplace_back(std::move(state->pending.front()));\n      state->pending.pop_front();\n    }\n  }\n\n  RayletClient::DispatchRecoveryWitnessBatch(\n      std::move(state), std::move(grpc_client), std::move(next_batch));\n}\n\nvoid RayletClient::DispatchRecoveryWitnessBatch(\n''',
)
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''  rpc::UpdateRecoveryWitnessBatchRequest request;\n  bool profile_batch = false;\n  for (const auto &item : *batch) {\n    profile_batch = profile_batch || item.profiling;\n  }\n''',
    '''  rpc::UpdateRecoveryWitnessBatchRequest request;\n  bool profile_batch = false;\n  bool use_cq_transport_drain =\n      ::RayConfig::instance().enable_recovery_succession() &&\n      !::RayConfig::instance().enable_recovery_witness_holder_baseline() &&\n      !::RayConfig::instance().enable_recovery_frontier() &&\n      !::RayConfig::instance().enable_recovery_succession_certificate_admission();\n\n  for (const auto &item : *batch) {\n    profile_batch = profile_batch || item.profiling;\n    const auto &update = item.request;\n    use_cq_transport_drain =\n        use_cq_transport_drain && update.has_manifest() &&\n        !update.manifest().tombstoned() &&\n        update.manifest().target_holder_count() == 2 &&\n        update.manifest().witness_count() == 2 &&\n        update.manifest().witness_raylets_size() == 2 &&\n        !update.has_task_spec() && update.serialized_task_spec().empty() &&\n        !update.has_holder_certificate() && !update.has_recovery_claim();\n  }\n''',
)
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''  auto batch_callback =\n      [state, grpc_client, batch](\n''',
    '''  auto batch_callback =\n      [state, grpc_client, batch, use_cq_transport_drain](\n''',
)
replace_one(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''        auto next_batch =\n            std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();\n        {\n          std::lock_guard<std::mutex> lock(state->mutex);\n          const size_t count = std::min(\n              RayletClient::kRecoveryWitnessBatchMaxSize,\n              state->pending.size());\n\n          if (count == 0) {\n            state->in_flight = false;\n            return;\n          }\n\n          next_batch->reserve(count);\n          for (size_t i = 0; i < count; ++i) {\n            next_batch->emplace_back(std::move(state->pending.front()));\n            state->pending.pop_front();\n          }\n        }\n\n        RayletClient::DispatchRecoveryWitnessBatch(\n            state, grpc_client, std::move(next_batch));\n      };\n\n  INVOKE_RPC_CALL(NodeManagerService,\n                  UpdateRecoveryWitnessBatch,\n                  request,\n                  batch_callback,\n                  grpc_client,\n                  /*method_timeout_ms=*/-1);\n''',
    '''        if (!use_cq_transport_drain) {\n          // Legacy path: physical transport advances only after logical\n          // callbacks have executed on the CoreWorker main event loop.\n          RayletClient::AdvanceRecoveryWitnessBatchTransport(\n              state, grpc_client);\n        }\n      };\n\n  std::function<void()> completion_queue_hook;\n  if (use_cq_transport_drain) {\n    completion_queue_hook = [state, grpc_client]() {\n      // Ordinary K=1 only: once gRPC has completed the current physical\n      // batch, launch the next queued physical batch directly from the CQ\n      // polling thread. The completed batch's logical callbacks have already\n      // been posted to the main event loop by ClientCallManager and still run\n      // there exactly as before.\n      RayletClient::AdvanceRecoveryWitnessBatchTransport(state, grpc_client);\n    };\n  }\n\n  grpc_client->CallMethod<UpdateRecoveryWitnessBatchRequest,\n                          UpdateRecoveryWitnessBatchReply>(\n      &NodeManagerService::Stub::PrepareAsyncUpdateRecoveryWitnessBatch,\n      request,\n      batch_callback,\n      "NodeManagerService.grpc_client.UpdateRecoveryWitnessBatch",\n      /*method_timeout_ms=*/-1,\n      std::move(completion_queue_hook));\n''',
)

print("Applied ordinary-K1 CQ-driven witness transport draining.")
