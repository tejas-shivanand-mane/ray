#!/usr/bin/env python3
'''Fix Patch 4B-3 build error caused by an inline lambda inside INVOKE_RPC_CALL.

Run from the Ray repository root after apply_patch4b3_batch_witness_updates.py:

    python gossip_benchmarks/fix_patch4b3_macro_build.py
'''

from __future__ import annotations

import argparse
import sys
from pathlib import Path


OLD = r'''  INVOKE_RPC_CALL(
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
'''

NEW = r'''  auto batch_callback =
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
      };

  INVOKE_RPC_CALL(NodeManagerService,
                  UpdateRecoveryWitnessBatch,
                  request,
                  batch_callback,
                  grpc_client,
                  /*method_timeout_ms=*/-1);
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    path = args.repo.resolve() / "src/ray/raylet_rpc_client/raylet_client.cc"
    if not path.is_file():
        raise RuntimeError(f"File not found: {path}")

    text = path.read_text()

    if "auto batch_callback =" in text and OLD not in text:
        print("Patch 4B-3 macro fix already appears to be applied.")
        return 0

    count = text.count(OLD)
    if count != 1:
        raise RuntimeError(
            "Expected exactly one buggy Patch 4B-3 INVOKE_RPC_CALL block, "
            f"found {count}. Refusing to edit."
        )

    if args.check:
        print("Patch 4B-3 macro fix applicability: OK")
        print("No files written (--check).")
        return 0

    path.write_text(text.replace(OLD, NEW, 1))
    print(f"Fixed: {path}")
    print("Next: git diff --check, then rebuild.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
