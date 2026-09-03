#!/usr/bin/env python3
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text()


def write(path: str, text: str) -> None:
    (ROOT / path).write_text(text)


def replace_exact(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one exact match, found {count}")
    write(path, text.replace(old, new, 1))


def replace_regex(path: str, pattern: str, replacement: str) -> None:
    text = read(path)
    new_text, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise RuntimeError(f"{path}: expected exactly one regex match, found {count}")
    write(path, new_text)


# ---------------------------------------------------------------------------
# RayletClient state: keep one zero-delay posted flush for an idle ordinary-K1
# witness lane. Existing in-flight backlog still drains immediately on RPC ACK.
# ---------------------------------------------------------------------------
replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.h",
    '''#include <vector>\n\n#include "ray/raylet_rpc_client/raylet_client_interface.h"\n''',
    '''#include <vector>\n\n#include "ray/asio/instrumented_io_context.h"\n#include "ray/raylet_rpc_client/raylet_client_interface.h"\n''',
)

replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.h",
    '''  struct RecoveryWitnessBatchState {\n    std::mutex mutex;\n    std::deque<PendingRecoveryWitnessUpdate> pending;\n    bool in_flight = false;\n  };\n\n  // At inflight=64 this cap is large enough to collapse queue pressure while\n  // keeping individual batch messages modest. There is intentionally no\n  // timer: an idle connection sends its first update immediately.\n  static constexpr size_t kRecoveryWitnessBatchMaxSize = 32;\n\n  static void DispatchRecoveryWitnessBatch(\n''',
    '''  struct RecoveryWitnessBatchState {\n    std::mutex mutex;\n    std::deque<PendingRecoveryWitnessUpdate> pending;\n    bool in_flight = false;\n    bool flush_scheduled = false;\n  };\n\n  // At inflight=64 this cap is large enough to collapse queue pressure while\n  // keeping individual batch messages modest. Ordinary adaptive K=1 uses one\n  // zero-delay event-loop turn when an idle lane becomes active; no timer or\n  // fixed microsecond delay is introduced. Backlogged lanes still drain\n  // immediately after each physical RPC completes.\n  static constexpr size_t kRecoveryWitnessBatchMaxSize = 32;\n\n  static void FlushRecoveryWitnessMicrobatch(\n      std::shared_ptr<RecoveryWitnessBatchState> state,\n      std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client);\n\n  static void DispatchRecoveryWitnessBatch(\n''',
)

replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.h",
    '''  std::shared_ptr<RecoveryWitnessBatchState> recovery_witness_batch_state_ =\n      std::make_shared<RecoveryWitnessBatchState>();\n\n protected:\n''',
    '''  instrumented_io_context &main_service_;\n\n  std::shared_ptr<RecoveryWitnessBatchState> recovery_witness_batch_state_ =\n      std::make_shared<RecoveryWitnessBatchState>();\n\n protected:\n''',
)

replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''    : grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(\n''',
    '''    : main_service_(client_call_manager.GetMainService()),\n      grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(\n''',
)

new_update_and_flush = r'''void RayletClient::UpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest &&request,
    const rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> &callback) {
  // Ordinary adaptive K=1 only: when an idle witness lane receives its first
  // normal generation update, defer the physical send by exactly one ordinary
  // event-loop post so peer updates arriving in this same turn can join it.
  // There is no timer/fixed delay. Fixed-R, Frontier, certificate mode,
  // tombstones, and failure-path claims retain the legacy immediate-idle path.
  const bool use_k1_zero_turn_microbatch =
      ::RayConfig::instance().enable_recovery_succession() &&
      !::RayConfig::instance().enable_recovery_witness_holder_baseline() &&
      !::RayConfig::instance().enable_recovery_frontier() &&
      !::RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      request.has_manifest() && !request.manifest().tombstoned() &&
      request.manifest().target_holder_count() == 2 &&
      request.manifest().witness_count() == 2 &&
      request.manifest().witness_raylets_size() == 2 &&
      !request.has_task_spec() && request.serialized_task_spec().empty() &&
      !request.has_holder_certificate() && !request.has_recovery_claim();

  PendingRecoveryWitnessUpdate item{std::move(request), callback};
  item.profiling =
      ::RayConfig::instance().enable_recovery_succession_profiling();
  if (item.profiling) {
    item.enqueue_time_ns = RecoveryWitnessClientProfileNowNs();
  }

  auto state = recovery_witness_batch_state_;
  std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch;
  bool schedule_flush = false;

  {
    std::lock_guard<std::mutex> lock(state->mutex);

    if (use_k1_zero_turn_microbatch) {
      state->pending.emplace_back(std::move(item));
      if (!state->in_flight && !state->flush_scheduled) {
        state->flush_scheduled = true;
        schedule_flush = true;
      }
    } else {
      // A legacy/tombstone update must not overtake an already-posted ordinary
      // K1 flush. If such a flush exists, preserve FIFO order by joining the
      // same pending deque; otherwise retain the old immediate-idle behavior.
      if (state->in_flight || state->flush_scheduled) {
        state->pending.emplace_back(std::move(item));
        return;
      }

      state->in_flight = true;
      batch = std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
      batch->reserve(1);
      batch->emplace_back(std::move(item));
    }
  }

  if (schedule_flush) {
    auto state_for_flush = state;
    auto grpc_client_for_flush = grpc_client_;
    main_service_.post(
        [state_for_flush, grpc_client_for_flush]() mutable {
          RayletClient::FlushRecoveryWitnessMicrobatch(
              std::move(state_for_flush), std::move(grpc_client_for_flush));
        },
        "RayletClient.FlushRecoveryWitnessMicrobatch");
    return;
  }

  RAY_CHECK(batch != nullptr && !batch->empty());
  DispatchRecoveryWitnessBatch(state, grpc_client_, std::move(batch));
}

void RayletClient::FlushRecoveryWitnessMicrobatch(
    std::shared_ptr<RecoveryWitnessBatchState> state,
    std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client) {
  std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch;

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->flush_scheduled = false;

    // A defensive guard: a legacy request that raced with the posted callback
    // is required to queue while flush_scheduled=true, so normally in_flight
    // cannot be true here. If it is, its completion will drain pending.
    if (state->in_flight || state->pending.empty()) {
      return;
    }

    state->in_flight = true;
    const size_t count = std::min(
        RayletClient::kRecoveryWitnessBatchMaxSize, state->pending.size());
    batch = std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
    batch->reserve(count);
    for (size_t i = 0; i < count; ++i) {
      batch->emplace_back(std::move(state->pending.front()));
      state->pending.pop_front();
    }
  }

  DispatchRecoveryWitnessBatch(state, grpc_client, std::move(batch));
}

void RayletClient::DispatchRecoveryWitnessBatch('''

replace_regex(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    r'''void RayletClient::UpdateRecoveryWitness\(\n    rpc::UpdateRecoveryWitnessRequest &&request,\n    const rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> &callback\) \{.*?\n\}\n\nvoid RayletClient::DispatchRecoveryWitnessBatch\(''',
    new_update_and_flush,
)

# Use the batch-level server wall clock as the robust handler measurement.
replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''        const bool reply_shape_ok =\n            !status.ok() ||\n            static_cast<size_t>(reply.replies_size()) == batch->size();\n\n        if (status.ok() && !reply_shape_ok) {\n''',
    '''        const bool reply_shape_ok =\n            !status.ok() ||\n            static_cast<size_t>(reply.replies_size()) == batch->size();\n        const uint64_t amortized_witness_handler_ns =\n            status.ok() && !batch->empty() &&\n                    reply.witness_batch_handler_time_ns() != 0\n                ? reply.witness_batch_handler_time_ns() / batch->size()\n                : 0;\n\n        if (status.ok() && !reply_shape_ok) {\n''',
)

replace_exact(
    "src/ray/raylet_rpc_client/raylet_client.cc",
    '''          if ((*batch)[i].profiling) {\n            item_reply.set_client_queue_time_ns((*batch)[i].client_queue_time_ns);\n            item_reply.set_client_batch_size(\n                static_cast<uint32_t>(batch->size()));\n            item_reply.set_client_batch_leader(i == 0);\n          }\n''',
    '''          if ((*batch)[i].profiling) {\n            item_reply.set_client_queue_time_ns((*batch)[i].client_queue_time_ns);\n            if (amortized_witness_handler_ns != 0) {\n              item_reply.set_witness_handler_time_ns(\n                  amortized_witness_handler_ns);\n            }\n            item_reply.set_client_batch_size(\n                static_cast<uint32_t>(batch->size()));\n            item_reply.set_client_batch_leader(i == 0);\n          }\n''',
)

# ---------------------------------------------------------------------------
# Profiling: expose one physical-batch wall-clock value on the outer reply.
# ---------------------------------------------------------------------------
replace_exact(
    "src/ray/protobuf/node_manager.proto",
    '''message UpdateRecoveryWitnessBatchReply {\n  repeated UpdateRecoveryWitnessReply replies = 1;\n}\n''',
    '''message UpdateRecoveryWitnessBatchReply {\n  repeated UpdateRecoveryWitnessReply replies = 1;\n\n  // Profiling-only physical-batch server wall time for Benchmark 70.\n  // Zero when recovery succession profiling is disabled.\n  uint64 witness_batch_handler_time_ns = 2;\n}\n''',
)

new_batch_handler = r'''void NodeManager::HandleUpdateRecoveryWitnessBatch(
    rpc::UpdateRecoveryWitnessBatchRequest request,
    rpc::UpdateRecoveryWitnessBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Reuse the single-update implementation so batching cannot diverge from
  // the existing validation/versioning/baseline semantics. The single-item
  // handler is synchronous; its send callback only marks that logical item
  // complete, so a no-op callback is sufficient inside this outer RPC.
  const bool profile_witness =
      RayConfig::instance().enable_recovery_succession_profiling();
  const uint64_t batch_start_ns =
      profile_witness ? RecoveryWitnessProfileNowNs() : 0;

  for (int i = 0; i < request.updates_size(); ++i) {
    rpc::UpdateRecoveryWitnessRequest item_request;
    item_request.Swap(request.mutable_updates(i));

    const uint64_t item_start_ns =
        profile_witness ? RecoveryWitnessProfileNowNs() : 0;
    auto *item_reply = reply->add_replies();
    HandleUpdateRecoveryWitness(
        std::move(item_request),
        item_reply,
        [](Status, std::function<void()>, std::function<void()>) {});
    if (item_start_ns != 0) {
      const uint64_t item_end_ns = RecoveryWitnessProfileNowNs();
      item_reply->set_witness_handler_time_ns(item_end_ns - item_start_ns);
      item_reply->set_witness_batch_queue_time_ns(
          item_start_ns - batch_start_ns);
    }
  }

  if (batch_start_ns != 0) {
    reply->set_witness_batch_handler_time_ns(
        RecoveryWitnessProfileNowNs() - batch_start_ns);
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void NodeManager::ReplicateFixedRRecoveryClaim('''

replace_regex(
    "src/ray/raylet/node_manager.cc",
    r'''void NodeManager::HandleUpdateRecoveryWitnessBatch\(.*?\n\}\n\nvoid NodeManager::ReplicateFixedRRecoveryClaim\(''',
    new_batch_handler,
)

replace_exact(
    "gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py",
    '            "  witness handler total                 = "\n',
    '            "  witness handler total (amortized)     = "\n',
)

# Final structural guards.
checks = {
    "src/ray/raylet_rpc_client/raylet_client.cc": [
        "use_k1_zero_turn_microbatch",
        "RayletClient.FlushRecoveryWitnessMicrobatch",
        "witness_batch_handler_time_ns",
    ],
    "src/ray/raylet_rpc_client/raylet_client.h": [
        "flush_scheduled = false",
        "instrumented_io_context &main_service_",
    ],
    "src/ray/protobuf/node_manager.proto": [
        "witness_batch_handler_time_ns = 2",
    ],
    "src/ray/raylet/node_manager.cc": [
        "set_witness_batch_handler_time_ns",
    ],
}
for path, needles in checks.items():
    text = read(path)
    for needle in needles:
        if needle not in text:
            raise RuntimeError(f"{path}: missing expected marker {needle!r}")

print("Applied ordinary-K1 zero-event-loop-turn witness microbatch + robust handler profiling")
