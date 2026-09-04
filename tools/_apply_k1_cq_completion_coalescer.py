#!/usr/bin/env python3
from pathlib import Path


def replace_one(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected 1 occurrence, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


# This applicator runs after the workflow restores the four transport files to
# profiling baseline 28e486413b5318965f3b5b5089d9770fdfbeec20.

# ---------------------------------------------------------------------------
# ClientCallManager: add an optional CQ hook plus a single-flight main-loop
# completion coalescer. Only callers that opt in are coalesced; all ordinary
# Ray RPC callbacks keep the existing per-call post behavior.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/rpc/client_call.h",
    '''#include <chrono>\n#include <cstdint>\n#include <memory>\n''',
    '''#include <chrono>\n#include <cstdint>\n#include <deque>\n#include <functional>\n#include <memory>\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''  uint64_t RecoveryWitnessMainLoopStartTimeNs() const {\n    return recovery_witness_main_loop_start_time_ns_;\n  }\n\n  virtual ~ClientCall() = default;\n\n private:\n''',
    '''  uint64_t RecoveryWitnessMainLoopStartTimeNs() const {\n    return recovery_witness_main_loop_start_time_ns_;\n  }\n\n  void SetCompletionQueueHook(std::function<void()> hook) {\n    completion_queue_hook_ = std::move(hook);\n  }\n\n  void RunCompletionQueueHook() {\n    if (completion_queue_hook_) {\n      auto hook = std::move(completion_queue_hook_);\n      hook();\n    }\n  }\n\n  void SetCoalesceMainLoopCallback(bool enabled) {\n    coalesce_main_loop_callback_ = enabled;\n  }\n\n  bool CoalesceMainLoopCallback() const {\n    return coalesce_main_loop_callback_;\n  }\n\n  virtual ~ClientCall() = default;\n\n private:\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''  uint64_t recovery_witness_submit_time_ns_ = 0;\n  uint64_t recovery_witness_cq_receive_time_ns_ = 0;\n  uint64_t recovery_witness_main_loop_start_time_ns_ = 0;\n};\n\nclass ClientCallManager;\n''',
    '''  uint64_t recovery_witness_submit_time_ns_ = 0;\n  uint64_t recovery_witness_cq_receive_time_ns_ = 0;\n  uint64_t recovery_witness_main_loop_start_time_ns_ = 0;\n  std::function<void()> completion_queue_hook_;\n  bool coalesce_main_loop_callback_ = false;\n};\n\nclass ClientCallManager;\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''                          std::shared_ptr<StatsHandle> stats_handle,\n                          bool record_stats,\n                          int64_t timeout_ms = -1)\n''',
    '''                          std::shared_ptr<StatsHandle> stats_handle,\n                          bool record_stats,\n                          int64_t timeout_ms = -1,\n                          std::function<void()> completion_queue_hook = nullptr,\n                          bool coalesce_main_loop_callback = false)\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n''',
    '''    if (!cluster_id.IsNil()) {\n      context_.AddMetadata(kClusterIdKey, cluster_id.Hex());\n    }\n    SetCompletionQueueHook(std::move(completion_queue_hook));\n    SetCoalesceMainLoopCallback(coalesce_main_loop_callback);\n    if constexpr (HasRecoveryWitnessClientTimingFields<Reply>::value) {\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''class ClientCallTag {\n public:\n''',
    '''class ClientCallTag {\n public:\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of\n/// `ClientCall` objects.\n''',
    '''struct CoalescedClientCallbackState {\n  absl::Mutex mutex;\n  std::deque<std::shared_ptr<ClientCall>> pending ABSL_GUARDED_BY(mutex);\n  bool drain_scheduled ABSL_GUARDED_BY(mutex) = false;\n};\n\n/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of\n/// `ClientCall` objects.\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name,\n      int64_t method_timeout_ms = -1) {\n''',
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name,\n      int64_t method_timeout_ms = -1,\n      std::function<void()> completion_queue_hook = nullptr,\n      bool coalesce_main_loop_callback = false) {\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''    auto call = std::make_shared<ClientCallImpl<Reply>>(\n        callback, cluster_id_, std::move(stats_handle), record_stats_, method_timeout_ms);\n''',
    '''    auto call = std::make_shared<ClientCallImpl<Reply>>(\n        callback,\n        cluster_id_,\n        std::move(stats_handle),\n        record_stats_,\n        method_timeout_ms,\n        std::move(completion_queue_hook),\n        coalesce_main_loop_callback);\n''',
)

# Insert the single-flight helper immediately before PollEventsFromCompletionQueue.
replace_one(
    "src/ray/rpc/client_call.h",
    ''' private:\n  /// This function runs in a background thread. It keeps polling events from the\n  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`\n  /// objects.\n  void PollEventsFromCompletionQueue(int index) {\n''',
    ''' private:\n  void EnqueueCoalescedMainLoopCallback(\n      const std::shared_ptr<ClientCall> &call) {\n    auto state = coalesced_callback_state_;\n    bool schedule_drain = false;\n    {\n      absl::MutexLock lock(&state->mutex);\n      state->pending.push_back(call);\n      if (!state->drain_scheduled) {\n        state->drain_scheduled = true;\n        schedule_drain = true;\n      }\n    }\n\n    if (!schedule_drain) {\n      return;\n    }\n\n    main_service_.post(\n        [state]() {\n          // One main-loop event drains every completion that has accumulated.\n          // If more completions arrive while callbacks are running, continue\n          // draining them in this same event before releasing single-flight.\n          while (true) {\n            std::deque<std::shared_ptr<ClientCall>> ready;\n            {\n              absl::MutexLock lock(&state->mutex);\n              if (state->pending.empty()) {\n                state->drain_scheduled = false;\n                return;\n              }\n              ready.swap(state->pending);\n            }\n\n            for (auto &ready_call : ready) {\n              ready_call->MarkRecoveryWitnessMainLoopCallbackStarted();\n              ready_call->OnReplyReceived();\n            }\n          }\n        },\n        "ClientCallManager.CoalescedOnReplyReceived");\n  }\n\n  /// This function runs in a background thread. It keeps polling events from the\n  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`\n  /// objects.\n  void PollEventsFromCompletionQueue(int index) {\n''',
)

# Replace the successful completion dispatch block. A stable shared_ptr is kept
# before any main-loop post so there is no tag lifetime race.
replace_one(
    "src/ray/rpc/client_call.h",
    '''        auto tag = static_cast<ClientCallTag *>(got_tag);\n        // Refresh the tag.\n        got_tag = nullptr;\n        tag->GetCall()->MarkRecoveryWitnessCompletionQueueReceived();\n        tag->GetCall()->SetReturnStatus();\n        std::shared_ptr<StatsHandle> stats_handle = tag->GetCall()->GetStatsHandle();\n        RAY_CHECK_NE(stats_handle, nullptr);\n        if (ok && !main_service_.stopped() && !shutdown_) {\n          if (record_stats_ && !tag->GetCall()->GetStatus().ok()) {\n            client_metrics_.req_failed.Record(1.0,\n                                              {{"Method", stats_handle->event_name}});\n          }\n          // Post the callback to the main event loop.\n          main_service_.post(\n              [tag]() {\n                tag->GetCall()->MarkRecoveryWitnessMainLoopCallbackStarted();\n                tag->GetCall()->OnReplyReceived();\n                // The call is finished, and we can delete this tag now.\n                delete tag;\n              },\n              stats_handle->event_name + ".OnReplyReceived",\n              // Implement the delay of the rpc client call as the\n              // delay of OnReplyReceived().\n              ray::asio::testing::GetDelayUs(stats_handle->event_name));\n          main_service_.stats()->RecordEnd(std::move(stats_handle));\n        } else {\n          delete tag;\n        }\n''',
    '''        auto tag = static_cast<ClientCallTag *>(got_tag);\n        // Refresh the tag.\n        got_tag = nullptr;\n        auto call = tag->GetCall();\n        call->MarkRecoveryWitnessCompletionQueueReceived();\n        call->SetReturnStatus();\n        std::shared_ptr<StatsHandle> stats_handle = call->GetStatsHandle();\n        RAY_CHECK_NE(stats_handle, nullptr);\n        if (ok && !main_service_.stopped() && !shutdown_) {\n          if (record_stats_ && !call->GetStatus().ok()) {\n            client_metrics_.req_failed.Record(1.0,\n                                              {{"Method", stats_handle->event_name}});\n          }\n\n          if (call->CoalesceMainLoopCallback()) {\n            // The tag is only a wrapper around this stable shared_ptr. K1 witness\n            // completions opt into one single-flight main-loop drain instead of\n            // posting one event per physical batch.\n            delete tag;\n            EnqueueCoalescedMainLoopCallback(call);\n          } else {\n            // Existing behavior for every other RPC.\n            main_service_.post(\n                [tag]() {\n                  tag->GetCall()->MarkRecoveryWitnessMainLoopCallbackStarted();\n                  tag->GetCall()->OnReplyReceived();\n                  // The call is finished, and we can delete this tag now.\n                  delete tag;\n                },\n                stats_handle->event_name + ".OnReplyReceived",\n                // Implement the delay of the rpc client call as the\n                // delay of OnReplyReceived().\n                ray::asio::testing::GetDelayUs(stats_handle->event_name));\n          }\n\n          // Physical witness transport may advance immediately at CQ completion.\n          // Use the retained shared_ptr, never the raw tag, because the main loop\n          // may already have consumed and deleted the tag.\n          call->RunCompletionQueueHook();\n          main_service_.stats()->RecordEnd(std::move(stats_handle));\n        } else {\n          delete tag;\n        }\n''',
)

replace_one(
    "src/ray/rpc/client_call.h",
    '''  /// The index to send RPCs in a round-robin fashion\n  std::atomic<uint64_t> rr_index_ = 0;\n\n  /// The gRPC `CompletionQueue` object used to poll events.\n''',
    '''  /// The index to send RPCs in a round-robin fashion\n  std::atomic<uint64_t> rr_index_ = 0;\n\n  // Only explicitly opted-in calls use this queue. It is independent of the\n  // physical gRPC completion queues and exists solely to reduce main-loop post\n  // amplification for ordinary K1 witness batch completions.\n  std::shared_ptr<CoalescedClientCallbackState> coalesced_callback_state_ =\n      std::make_shared<CoalescedClientCallbackState>();\n\n  /// The gRPC `CompletionQueue` object used to poll events.\n''',
)

# ---------------------------------------------------------------------------
# GrpcClient: thread the two optional controls through to ClientCallManager.
# Existing call sites use defaults and preserve behavior.
# ---------------------------------------------------------------------------
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''#include <boost/asio.hpp>\n#include <memory>\n''',
    '''#include <boost/asio.hpp>\n#include <functional>\n#include <memory>\n''',
)
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name = "UNKNOWN_RPC",\n      int64_t method_timeout_ms = -1) {\n''',
    '''      const ClientCallback<Reply> &callback,\n      std::string call_name = "UNKNOWN_RPC",\n      int64_t method_timeout_ms = -1,\n      std::function<void()> completion_queue_hook = nullptr,\n      bool coalesce_main_loop_callback = false) {\n''',
)

# Response-failure path.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          },\n          std::move(call_name),\n          method_timeout_ms);\n    } else if (failure == testing::RpcFailure::InFlight) {\n''',
    '''          },\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook),\n          coalesce_main_loop_callback);\n    } else if (failure == testing::RpcFailure::InFlight) {\n''',
)

# In-flight-failure transport call. The synthetic failure callback remains the
# existing main-loop path; ordinary K1 benchmark runs do not enable chaos.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          },\n          std::move(call_name),\n          method_timeout_ms);\n      client_call_manager_.GetMainService().post(\n''',
    '''          },\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook),\n          coalesce_main_loop_callback);\n      client_call_manager_.GetMainService().post(\n''',
)

# Normal path.
replace_one(
    "src/ray/rpc/grpc_client.h",
    '''          callback,\n          std::move(call_name),\n          method_timeout_ms);\n      RAY_CHECK(call != nullptr);\n''',
    '''          callback,\n          std::move(call_name),\n          method_timeout_ms,\n          std::move(completion_queue_hook),\n          coalesce_main_loop_callback);\n      RAY_CHECK(call != nullptr);\n''',
)

# ---------------------------------------------------------------------------
# RayletClient: CQ-driven physical draining only for ordinary adaptive K1, plus
# opt those same physical batch completions into the single-flight main-loop
# coalescer. Fixed-R/Frontier/certificate/tombstone/claim stay legacy.
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
    '''        if (!use_cq_transport_drain) {\n          RayletClient::AdvanceRecoveryWitnessBatchTransport(\n              state, grpc_client);\n        }\n      };\n\n  std::function<void()> completion_queue_hook;\n  if (use_cq_transport_drain) {\n    completion_queue_hook = [state, grpc_client]() {\n      RayletClient::AdvanceRecoveryWitnessBatchTransport(state, grpc_client);\n    };\n  }\n\n  grpc_client->CallMethod<UpdateRecoveryWitnessBatchRequest,\n                          UpdateRecoveryWitnessBatchReply>(\n      &NodeManagerService::Stub::PrepareAsyncUpdateRecoveryWitnessBatch,\n      request,\n      batch_callback,\n      "NodeManagerService.grpc_client.UpdateRecoveryWitnessBatch",\n      /*method_timeout_ms=*/-1,\n      std::move(completion_queue_hook),\n      /*coalesce_main_loop_callback=*/use_cq_transport_drain);\n''',
)

print("Applied ordinary-K1 CQ transport drain with single-flight completion coalescing.")
