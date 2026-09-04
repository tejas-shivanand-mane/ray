#!/usr/bin/env python3
from pathlib import Path

# Restore the validated immediate-idle RayletClient witness batching policy while
# retaining the newer Benchmark-70 profiling fields and server batch timing.

h_path = Path("src/ray/raylet_rpc_client/raylet_client.h")
h = h_path.read_text()
start = h.index("  struct RecoveryWitnessBatchState {")
end = h.index(" protected:\n", start)
replacement = '''  struct RecoveryWitnessBatchState {
    std::mutex mutex;
    std::deque<PendingRecoveryWitnessUpdate> pending;
    bool in_flight = false;
  };

  // At inflight=64 this cap is large enough to collapse queue pressure while
  // keeping individual batch messages modest. There is intentionally no
  // timer: an idle connection sends its first update immediately.
  static constexpr size_t kRecoveryWitnessBatchMaxSize = 32;

  static void DispatchRecoveryWitnessBatch(
      std::shared_ptr<RecoveryWitnessBatchState> state,
      std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client,
      std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch);

  std::shared_ptr<RecoveryWitnessBatchState> recovery_witness_batch_state_ =
      std::make_shared<RecoveryWitnessBatchState>();

'''
h = h[:start] + replacement + h[end:]
h_path.write_text(h)

cc_path = Path("src/ray/raylet_rpc_client/raylet_client.cc")
cc = cc_path.read_text()
old_ctor = '''    : main_service_(client_call_manager.GetMainService()),
      grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(
'''
new_ctor = '''    : grpc_client_(std::make_shared<rpc::GrpcClient<rpc::NodeManagerService>>(
'''
if cc.count(old_ctor) != 1:
    raise RuntimeError(f"expected one microbatch constructor init, found {cc.count(old_ctor)}")
cc = cc.replace(old_ctor, new_ctor, 1)

start = cc.index("void RayletClient::UpdateRecoveryWitness(")
end = cc.index("void RayletClient::DispatchRecoveryWitnessBatch(", start)
update_impl = '''void RayletClient::UpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest &&request,
    const rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> &callback) {
  PendingRecoveryWitnessUpdate item{std::move(request), callback};
  item.profiling =
      ::RayConfig::instance().enable_recovery_succession_profiling();
  if (item.profiling) {
    item.enqueue_time_ns = RecoveryWitnessClientProfileNowNs();
  }
  auto state = recovery_witness_batch_state_;
  std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch;

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->in_flight) {
      state->pending.emplace_back(std::move(item));
      return;
    }

    state->in_flight = true;
    batch = std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
    batch->reserve(1);
    batch->emplace_back(std::move(item));
  }

  DispatchRecoveryWitnessBatch(state, grpc_client_, std::move(batch));
}

'''
cc = cc[:start] + update_impl + cc[end:]

# Guard that the improved profiler survives this behavioral rollback.
required = [
    "reply.witness_batch_handler_time_ns()",
    "amortized_witness_handler_ns",
    "item_reply.set_witness_handler_time_ns(",
]
for needle in required:
    if needle not in cc:
        raise RuntimeError(f"profiling regression: missing {needle!r}")

cc_path.write_text(cc)

# Cross-file guards: retain the newer physical-batch server handler timing.
proto = Path("src/ray/protobuf/node_manager.proto").read_text()
if "witness_batch_handler_time_ns" not in proto:
    raise RuntimeError("missing witness_batch_handler_time_ns protobuf field")
node = Path("src/ray/raylet/node_manager.cc").read_text()
if "set_witness_batch_handler_time_ns" not in node:
    raise RuntimeError("missing server batch-handler timing stamp")
bench = Path("gossip_benchmarks/70_recovery_succession_k1_quick_control_profile.py").read_text()
if "witness handler total (amortized)" not in bench:
    raise RuntimeError("missing corrected B70 handler label")

print("Restored immediate-idle K1 witness batching; preserved barrier profiling.")
