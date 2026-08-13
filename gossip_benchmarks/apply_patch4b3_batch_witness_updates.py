#!/usr/bin/env python3
'''Apply Patch 4B-3: adaptive per-witness batching for Recovery Succession.

Design:
- CoreWorker keeps issuing the existing logical UpdateRecoveryWitness RPC API.
- Each concrete RayletClient transparently batches normal (compact-manifest)
  witness updates destined for the same raylet connection.
- The first update is sent immediately (no batching timer / no intentional
  low-load delay).
- While one batch is in flight, later updates queue FIFO. On completion, up
  to 32 queued logical updates are sent in one physical batch RPC.
- Per-update replies/callbacks are preserved exactly and in request order.
- The witness-as-holder baseline (request.has_task_spec()) stays on the old
  single-update RPC path so this optimization does not change that baseline.

Run from the Ray repository root:
    python apply_patch4b3_batch_witness_updates.py

Use --check to validate applicability without writing files.
'''

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FILES = {
    "proto": Path("src/ray/protobuf/node_manager.proto"),
    "node_h": Path("src/ray/raylet/node_manager.h"),
    "node_cc": Path("src/ray/raylet/node_manager.cc"),
    "client_h": Path("src/ray/raylet_rpc_client/raylet_client.h"),
    "client_cc": Path("src/ray/raylet_rpc_client/raylet_client.cc"),
}


def die(msg: str) -> None:
    raise RuntimeError(msg)


def require_once(text: str, needle: str, label: str) -> int:
    count = text.count(needle)
    if count != 1:
        die(f"{label}: expected exactly one match, found {count}")
    return text.index(needle)


def insert_before_once(text: str, marker: str, insertion: str, label: str) -> str:
    idx = require_once(text, marker, label)
    return text[:idx] + insertion + text[idx:]


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        die(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch_proto(text: str) -> str:
    if "message UpdateRecoveryWitnessBatchRequest" in text:
        die("node_manager.proto already contains the 4B-3 batch messages")

    marker = "message GetRecoveryWitnessRequest {"
    messages = r'''// Batches independent logical witness updates that target the same raylet.
// Each element has exactly the same semantics as UpdateRecoveryWitnessRequest.
message UpdateRecoveryWitnessBatchRequest {
  repeated UpdateRecoveryWitnessRequest updates = 1;
}

// Replies are returned in the same order as UpdateRecoveryWitnessBatchRequest.updates.
message UpdateRecoveryWitnessBatchReply {
  repeated UpdateRecoveryWitnessReply replies = 1;
}

'''
    text = insert_before_once(
        text, marker, messages, "node_manager.proto batch-message insertion"
    )

    old_rpc = '''  rpc UpdateRecoveryWitness(UpdateRecoveryWitnessRequest)
      returns (UpdateRecoveryWitnessReply);
'''
    new_rpc = old_rpc + '''
  // Transport-only batching for compact recovery witness updates.
  // No voting, locking, or holder-admission decision is performed here.
  rpc UpdateRecoveryWitnessBatch(UpdateRecoveryWitnessBatchRequest)
      returns (UpdateRecoveryWitnessBatchReply);
'''
    return replace_once(text, old_rpc, new_rpc, "node_manager.proto batch RPC")


def patch_node_h(text: str) -> str:
    if "HandleUpdateRecoveryWitnessBatch" in text:
        die("node_manager.h already contains HandleUpdateRecoveryWitnessBatch")

    old = '''  void HandleUpdateRecoveryWitness(
      rpc::UpdateRecoveryWitnessRequest request,
      rpc::UpdateRecoveryWitnessReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;
'''
    new = old + '''
  /// Applies a transport batch of independent compact witness updates.
  void HandleUpdateRecoveryWitnessBatch(
      rpc::UpdateRecoveryWitnessBatchRequest request,
      rpc::UpdateRecoveryWitnessBatchReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;
'''
    return replace_once(text, old, new, "node_manager.h handler declaration")


def patch_node_cc(text: str) -> str:
    if "NodeManager::HandleUpdateRecoveryWitnessBatch" in text:
        die("node_manager.cc already contains HandleUpdateRecoveryWitnessBatch")

    marker = "void NodeManager::HandleGetRecoveryWitness("
    handler = r'''void NodeManager::HandleUpdateRecoveryWitnessBatch(
    rpc::UpdateRecoveryWitnessBatchRequest request,
    rpc::UpdateRecoveryWitnessBatchReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // Reuse the single-update implementation so batching cannot diverge from
  // the existing validation/versioning/baseline semantics. The single-item
  // handler is synchronous; its send callback only marks that logical item
  // complete, so a no-op callback is sufficient inside this outer RPC.
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

'''
    return insert_before_once(
        text, marker, handler, "node_manager.cc batch-handler insertion"
    )


def patch_client_h(text: str) -> str:
    if "RecoveryWitnessBatchState" in text:
        die("raylet_client.h already contains the 4B-3 batching state")

    if "#include <deque>" not in text:
        text = replace_once(
            text,
            "#include <memory>\n",
            "#include <deque>\n#include <memory>\n",
            "raylet_client.h deque include",
        )
    if "#include <mutex>" not in text:
        text = replace_once(
            text,
            "#include <memory>\n",
            "#include <memory>\n#include <mutex>\n",
            "raylet_client.h mutex include",
        )

    marker = " protected:\n  /// gRPC client to the NodeManagerService."
    private_block = r''' private:
  struct PendingRecoveryWitnessUpdate {
    rpc::UpdateRecoveryWitnessRequest request;
    rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> callback;
  };

  struct RecoveryWitnessBatchState {
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
    return insert_before_once(
        text, marker, private_block, "raylet_client.h batching state"
    )


def patch_client_cc(text: str) -> str:
    if "RayletClient::DispatchRecoveryWitnessBatch" in text:
        die("raylet_client.cc already contains 4B-3 batching")

    if "#include <algorithm>" not in text:
        text = replace_once(
            text,
            '#include "ray/raylet_rpc_client/raylet_client.h"\n\n',
            '#include "ray/raylet_rpc_client/raylet_client.h"\n\n#include <algorithm>\n',
            "raylet_client.cc algorithm include",
        )

    pattern = re.compile(
        r"void RayletClient::UpdateRecoveryWitness\(\n"
        r".*?\n\}\n\nvoid RayletClient::GetRecoveryWitness\(",
        re.DOTALL,
    )
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        die(
            "raylet_client.cc: expected exactly one UpdateRecoveryWitness function "
            f"followed by GetRecoveryWitness, found {len(matches)}"
        )

    replacement = r'''void RayletClient::UpdateRecoveryWitness(
    rpc::UpdateRecoveryWitnessRequest &&request,
    const rpc::ClientCallback<rpc::UpdateRecoveryWitnessReply> &callback) {
  // Keep the witness-as-holder baseline on the original one-request RPC path.
  // Those requests can contain a full TaskSpec and are not the compact normal
  // Recovery Succession traffic targeted by Patch 4B-3.
  if (request.has_task_spec()) {
    INVOKE_RPC_CALL(NodeManagerService,
                    UpdateRecoveryWitness,
                    request,
                    callback,
                    grpc_client_,
                    /*method_timeout_ms=*/-1);
    return;
  }

  PendingRecoveryWitnessUpdate item{std::move(request), callback};
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

void RayletClient::DispatchRecoveryWitnessBatch(
    std::shared_ptr<RecoveryWitnessBatchState> state,
    std::shared_ptr<rpc::GrpcClient<rpc::NodeManagerService>> grpc_client,
    std::shared_ptr<std::vector<PendingRecoveryWitnessUpdate>> batch) {
  RAY_CHECK(batch != nullptr && !batch->empty());

  rpc::UpdateRecoveryWitnessBatchRequest request;
  for (const auto &item : *batch) {
    request.add_updates()->CopyFrom(item.request);
  }

  INVOKE_RPC_CALL(
      NodeManagerService,
      UpdateRecoveryWitnessBatch,
      request,
      [state, grpc_client, batch](
          const Status &status,
          rpc::UpdateRecoveryWitnessBatchReply &&reply) mutable {
        const bool reply_shape_ok =
            !status.ok() ||
            static_cast<size_t>(reply.replies_size()) == batch->size();

        if (status.ok() && !reply_shape_ok) {
          RAY_LOG(ERROR)
              << "Recovery witness batch reply size mismatch: sent="
              << batch->size() << " received=" << reply.replies_size();
        }

        for (size_t i = 0; i < batch->size(); ++i) {
          rpc::UpdateRecoveryWitnessReply item_reply;
          if (status.ok() && reply_shape_ok) {
            item_reply.Swap(reply.mutable_replies(static_cast<int>(i)));
          }
          // Transport failures retain their non-OK status. A malformed
          // successful batch reply yields the default stored=false item reply,
          // which safely fails that logical witness update.
          (*batch)[i].callback(status, std::move(item_reply));
        }

        auto next_batch =
            std::make_shared<std::vector<PendingRecoveryWitnessUpdate>>();
        {
          std::lock_guard<std::mutex> lock(state->mutex);
          const size_t count = std::min(
              RayletClient::kRecoveryWitnessBatchMaxSize,
              state->pending.size());

          if (count == 0) {
            state->in_flight = false;
            return;
          }

          next_batch->reserve(count);
          for (size_t i = 0; i < count; ++i) {
            next_batch->emplace_back(std::move(state->pending.front()));
            state->pending.pop_front();
          }
        }

        RayletClient::DispatchRecoveryWitnessBatch(
            state, grpc_client, std::move(next_batch));
      },
      grpc_client,
      /*method_timeout_ms=*/-1);
}

void RayletClient::GetRecoveryWitness('''

    match = matches[0]
    return text[: match.start()] + replacement + text[match.end() :]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo", type=Path, default=Path.cwd(), help="Ray repository root"
    )
    parser.add_argument(
        "--check", action="store_true", help="validate patch applicability only"
    )
    args = parser.parse_args()

    repo = args.repo.resolve()
    missing = [str(p) for p in FILES.values() if not (repo / p).is_file()]
    if missing:
        die("Not a Ray repo root or files missing: " + ", ".join(missing))

    patchers = {
        "proto": patch_proto,
        "node_h": patch_node_h,
        "node_cc": patch_node_cc,
        "client_h": patch_client_h,
        "client_cc": patch_client_cc,
    }

    originals: dict[str, str] = {}
    patched: dict[str, str] = {}
    for key, rel in FILES.items():
        original = (repo / rel).read_text()
        originals[key] = original
        patched[key] = patchers[key](original)
        if patched[key] == original:
            die(f"{rel}: patch unexpectedly produced no change")

    print("Patch 4B-3 applicability: OK")
    for key, rel in FILES.items():
        before = originals[key].count("\n") + 1
        after = patched[key].count("\n") + 1
        print(f"  {rel}: {before} -> {after} lines")

    if args.check:
        print("--check requested; no files written.")
        return 0

    # Validate every source shape before the first write so a mismatch cannot
    # leave a half-applied source tree.
    for key, rel in FILES.items():
        (repo / rel).write_text(patched[key])

    print("\nApplied Patch 4B-3 successfully.")
    print("Next: run `git diff --check` and inspect `git diff` before rebuilding.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
