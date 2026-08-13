#!/usr/bin/env python3
'''Fix Patch 4B-3 NodeManager server-handler registration.

Run from the Ray repository root:

    python gossip_benchmarks/fix_patch4b3_server_handler.py --check
    python gossip_benchmarks/fix_patch4b3_server_handler.py
'''

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    path = args.repo.resolve() / "src/ray/rpc/node_manager/node_manager_server.h"
    if not path.is_file():
        raise RuntimeError(f"File not found: {path}")

    text = path.read_text()

    macro_marker = "RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(UpdateRecoveryWitnessBatch)"
    virtual_marker = "virtual void HandleUpdateRecoveryWitnessBatch("

    if macro_marker in text and virtual_marker in text:
        print("Patch 4B-3 NodeManager server-handler fix already applied.")
        return 0

    old_macro = r'''    RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(UpdateRecoveryWitness)          \
    RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetRecoveryWitness)'''

    new_macro = r'''    RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(UpdateRecoveryWitness)          \
    RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(UpdateRecoveryWitnessBatch)     \
    RAY_NODE_MANAGER_RPC_SERVICE_HANDLER(GetRecoveryWitness)'''

    old_virtual = r'''    virtual void HandleUpdateRecoveryWitness(
        UpdateRecoveryWitnessRequest request,
        UpdateRecoveryWitnessReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleGetRecoveryWitness(
        GetRecoveryWitnessRequest request,
        GetRecoveryWitnessReply *reply,
        SendReplyCallback send_reply_callback) = 0;'''

    new_virtual = r'''    virtual void HandleUpdateRecoveryWitness(
        UpdateRecoveryWitnessRequest request,
        UpdateRecoveryWitnessReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleUpdateRecoveryWitnessBatch(
        UpdateRecoveryWitnessBatchRequest request,
        UpdateRecoveryWitnessBatchReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleGetRecoveryWitness(
        GetRecoveryWitnessRequest request,
        GetRecoveryWitnessReply *reply,
        SendReplyCallback send_reply_callback) = 0;'''

    updated = text

    if macro_marker not in updated:
        count = updated.count(old_macro)
        if count != 1:
            raise RuntimeError(
                "Could not uniquely locate the recovery RPC registration block "
                f"(found {count}). Refusing to edit."
            )
        updated = updated.replace(old_macro, new_macro, 1)

    if virtual_marker not in updated:
        count = updated.count(old_virtual)
        if count != 1:
            raise RuntimeError(
                "Could not uniquely locate the recovery handler interface block "
                f"(found {count}). Refusing to edit."
            )
        updated = updated.replace(old_virtual, new_virtual, 1)

    if updated.count(macro_marker) != 1:
        raise RuntimeError("Unexpected batch RPC registration count after patch.")
    if updated.count(virtual_marker) != 1:
        raise RuntimeError("Unexpected batch handler declaration count after patch.")

    if args.check:
        print("Patch 4B-3 NodeManager server-handler fix applicability: OK")
        print("Will add:")
        print("  1. UpdateRecoveryWitnessBatch to RAY_NODE_MANAGER_RPC_HANDLERS")
        print("  2. HandleUpdateRecoveryWitnessBatch to NodeManagerServiceHandler")
        print("No files written (--check).")
        return 0

    path.write_text(updated)
    print(f"Fixed: {path}")
    print("Registered UpdateRecoveryWitnessBatch with the NodeManager gRPC server")
    print("and added it to NodeManagerServiceHandler.")
    print("Next: git diff --check, then rebuild.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
