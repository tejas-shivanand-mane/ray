#!/usr/bin/env python3
'''
Hotfix for Patch 4E: wire the new batched recovery RPCs into Ray's
CoreWorker gRPC server handler interface and server call factories.

Apply from the Ray repository root:

    python gossip_benchmarks/fix_patch4e_grpc_service_wiring.py

This patch expects Patch 4E to already be applied (the proto must contain
ReportRecoveryCandidateBatch and InstallRecoveryHolderBatch).

Modified files:
  src/ray/core_worker/grpc_service.h
  src/ray/core_worker/grpc_service.cc

Backups:
  <file>.pre4e_grpc_fix
'''

from __future__ import annotations

import shutil
from pathlib import Path


ROOT = Path.cwd()

GRPC_H = ROOT / "src/ray/core_worker/grpc_service.h"
GRPC_CC = ROOT / "src/ray/core_worker/grpc_service.cc"
PROTO = ROOT / "src/ray/protobuf/core_worker.proto"


def require_file(path: Path) -> str:
    if not path.is_file():
        raise SystemExit(f"ERROR: expected file not found: {path}")
    return path.read_text()


def backup(path: Path) -> None:
    dst = path.with_name(path.name + ".pre4e_grpc_fix")
    if not dst.exists():
        shutil.copy2(path, dst)
        print(f"backup: {dst}")


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count == 0:
        raise SystemExit(
            f"ERROR: could not find expected insertion point for {label}.\n"
            "Your tree may differ from the post-4E tree this hotfix targets."
        )
    if count != 1:
        raise SystemExit(
            f"ERROR: expected exactly one insertion point for {label}, found {count}."
        )
    return text.replace(old, new, 1)


proto = require_file(PROTO)
if "rpc ReportRecoveryCandidateBatch(" not in proto:
    raise SystemExit(
        "ERROR: Patch 4E does not appear to be applied: "
        "core_worker.proto has no ReportRecoveryCandidateBatch RPC."
    )
if "rpc InstallRecoveryHolderBatch(" not in proto:
    raise SystemExit(
        "ERROR: Patch 4E does not appear to be applied: "
        "core_worker.proto has no InstallRecoveryHolderBatch RPC."
    )

h = require_file(GRPC_H)
cc = require_file(GRPC_CC)

# ------------------------------------------------------------
# grpc_service.h
# ------------------------------------------------------------

if "HandleReportRecoveryCandidateBatch(" not in h:
    old = '''    virtual void HandleReportRecoveryCandidate(
        ReportRecoveryCandidateRequest request,
        ReportRecoveryCandidateReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleInstallRecoveryHolder(
'''
    new = '''    virtual void HandleReportRecoveryCandidate(
        ReportRecoveryCandidateRequest request,
        ReportRecoveryCandidateReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleReportRecoveryCandidateBatch(
        ReportRecoveryCandidateBatchRequest request,
        ReportRecoveryCandidateBatchReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleInstallRecoveryHolder(
'''
    h = replace_once(
        h, old, new, "HandleReportRecoveryCandidateBatch in grpc_service.h"
    )
else:
    print("already present: HandleReportRecoveryCandidateBatch")

if "HandleInstallRecoveryHolderBatch(" not in h:
    old = '''    virtual void HandleInstallRecoveryHolder(
        InstallRecoveryHolderRequest request,
        InstallRecoveryHolderReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleCommitRecoveryManifest(
'''
    new = '''    virtual void HandleInstallRecoveryHolder(
        InstallRecoveryHolderRequest request,
        InstallRecoveryHolderReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleInstallRecoveryHolderBatch(
        InstallRecoveryHolderBatchRequest request,
        InstallRecoveryHolderBatchReply *reply,
        SendReplyCallback send_reply_callback) = 0;

    virtual void HandleCommitRecoveryManifest(
'''
    h = replace_once(
        h, old, new, "HandleInstallRecoveryHolderBatch in grpc_service.h"
    )
else:
    print("already present: HandleInstallRecoveryHolderBatch")

# ------------------------------------------------------------
# grpc_service.cc
# ------------------------------------------------------------

if "ReportRecoveryCandidateBatch" not in cc:
    old = '''  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        ReportRecoveryCandidate,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        InstallRecoveryHolder,
'''
    new = '''  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        ReportRecoveryCandidate,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        ReportRecoveryCandidateBatch,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        InstallRecoveryHolder,
'''
    cc = replace_once(
        cc, old, new, "ReportRecoveryCandidateBatch factory in grpc_service.cc"
    )
else:
    print("already present: ReportRecoveryCandidateBatch factory")

if "InstallRecoveryHolderBatch" not in cc:
    old = '''  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        InstallRecoveryHolder,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        CommitRecoveryManifest,
'''
    new = '''  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        InstallRecoveryHolder,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        InstallRecoveryHolderBatch,
        max_active_rpcs_per_handler_,
        ClusterIdAuthType::NO_AUTH);

  RPC_SERVICE_HANDLER_CUSTOM_AUTH_SERVER_METRICS_DISABLED(
        CoreWorkerService,
        CommitRecoveryManifest,
'''
    cc = replace_once(
        cc, old, new, "InstallRecoveryHolderBatch factory in grpc_service.cc"
    )
else:
    print("already present: InstallRecoveryHolderBatch factory")

backup(GRPC_H)
backup(GRPC_CC)

GRPC_H.write_text(h)
GRPC_CC.write_text(cc)

print()
print("Patch 4E gRPC service wiring hotfix applied.")
print("Modified:")
print(f"  {GRPC_H}")
print(f"  {GRPC_CC}")
print()
print("Next:")
print("  git diff --check")
print("  git diff -- src/ray/core_worker/grpc_service.h src/ray/core_worker/grpc_service.cc")
print("  nice -n 10 python -m pip install -e python/ --verbose 2>&1 | tee ray-build.log")
