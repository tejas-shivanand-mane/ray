#!/usr/bin/env python3
from pathlib import Path
import subprocess

CHECKPOINT = "1ce790c5b20620b961ad62ff75cbad964d7ac287"
CORE = Path("src/ray/core_worker/core_worker.cc")
NODE = Path("src/ray/raylet/node_manager.cc")
PROTO = Path("src/ray/protobuf/node_manager.proto")


def restore(path: Path) -> None:
    data = subprocess.check_output(["git", "show", f"{CHECKPOINT}:{path.as_posix()}"])
    path.write_bytes(data)


for p in (CORE, NODE, PROTO):
    restore(p)

# ---------------------------------------------------------------------------
# CoreWorker: ordinary K=1 owner sends each holder-admission generation only
# to the deterministic primary witness. The primary raylet is responsible for
# launching the second witness write before acknowledging the owner.
# ---------------------------------------------------------------------------
core = CORE.read_text()
old = '''  const size_t witness_count = static_cast<size_t>(manifest.witness_raylets_size());

  for (const rpc::Address &witness : manifest.witness_raylets()) {
    rpc::UpdateRecoveryWitnessRequest request;
    request.mutable_manifest()->CopyFrom(manifest);
'''
new = '''  const size_t configured_witness_count =
      static_cast<size_t>(manifest.witness_raylets_size());

  // Patch 4N-WCHAIN: move ordinary K=1 W=2 fan-out off the owner hot path.
  // The owner sends one update to the deterministic primary witness. That
  // witness stores locally, launches the same generation to the secondary
  // witness, and only then returns its ordinary local-store ACK. This preserves
  // the existing Succession durability boundary (one compact-witness ACK) while
  // keeping the second real witness write in flight before the owner can commit.
  // Fixed-R, Frontier, certificates, tombstones, and non-R=2 configurations
  // retain their existing direct fan-out paths.
  const bool use_k1_witness_coordinator =
      !require_all_witnesses &&
      !recovery_witness_holder_baseline_enabled_ &&
      recovery_succession_manager_ != nullptr &&
      !recovery_succession_manager_->RecoveryFrontierEnabled() &&
      !RayConfig::instance().enable_recovery_succession_certificate_admission() &&
      !manifest.tombstoned() && manifest.target_holder_count() == 2 &&
      manifest.witness_count() == 2 && configured_witness_count == 2 &&
      manifest.succession_size() >= 2;

  const size_t witness_count =
      use_k1_witness_coordinator ? 1 : configured_witness_count;

  for (size_t witness_index = 0; witness_index < witness_count; ++witness_index) {
    const rpc::Address &witness =
        manifest.witness_raylets(static_cast<int>(witness_index));
    rpc::UpdateRecoveryWitnessRequest request;
    request.mutable_manifest()->CopyFrom(manifest);
'''
if core.count(old) != 1:
    raise RuntimeError(f"core witness loop anchor count={core.count(old)}")
core = core.replace(old, new, 1)
CORE.write_text(core)

# ---------------------------------------------------------------------------
# NodeManager: after a primary witness stores an ordinary K=1 generation, launch
# the same full manifest to the other configured witness before sending the
# primary's local ACK back. The forwarded request is marked so the secondary
# never forwards back and creates a loop.
# ---------------------------------------------------------------------------
node = NODE.read_text()
anchor = '''  const rpc::RecoveryManifest &incoming = request.manifest();
  const TaskID task_id = TaskID::FromBinary(incoming.task_id());

  const bool has_serialized_task_spec =
      !request.serialized_task_spec().empty();
'''
replacement = '''  const rpc::RecoveryManifest &incoming = request.manifest();
  const TaskID task_id = TaskID::FromBinary(incoming.task_id());

  // Patch 4N-WCHAIN: ordinary adaptive K=1 uses witness 0 as a tiny fan-out
  // coordinator. The coordinator must itself be one of the two configured
  // witnesses and must have exactly one distinct peer to forward to.
  const bool use_k1_witness_coordinator =
      !baseline_enabled && !certificate_mode && !request.witness_forwarded() &&
      !RayConfig::instance().enable_recovery_frontier() && !incoming.tombstoned() &&
      incoming.target_holder_count() == 2 && incoming.witness_count() == 2 &&
      incoming.witness_raylets_size() == 2 && incoming.succession_size() >= 2;

  std::optional<rpc::Address> witness_forward_target;
  if (use_k1_witness_coordinator) {
    bool self_is_configured_witness = false;
    for (const rpc::Address &witness : incoming.witness_raylets()) {
      if (witness.node_id() == self_node_id_.Binary()) {
        self_is_configured_witness = true;
      } else if (!witness_forward_target.has_value()) {
        witness_forward_target = witness;
      } else {
        witness_forward_target.reset();
        break;
      }
    }

    if (!self_is_configured_witness || !witness_forward_target.has_value() ||
        witness_forward_target->node_id().size() != NodeID::Size() ||
        witness_forward_target->ip_address().empty() ||
        witness_forward_target->port() <= 0) {
      reply->set_stored(false);
      send_reply_callback(Status::OK(), nullptr, nullptr);
      return;
    }
  }

  const bool has_serialized_task_spec =
      !request.serialized_task_spec().empty();
'''
if node.count(anchor) != 1:
    raise RuntimeError(f"node incoming anchor count={node.count(anchor)}")
node = node.replace(anchor, replacement, 1)

end_anchor = '''  send_reply_callback(Status::OK(), nullptr, nullptr);
}


void NodeManager::HandleUpdateRecoveryWitnessBatch(
'''
end_replacement = '''  if (reply->stored() && witness_forward_target.has_value()) {
    rpc::UpdateRecoveryWitnessRequest forwarded_request;
    forwarded_request.mutable_manifest()->CopyFrom(incoming);
    forwarded_request.set_witness_forwarded(true);

    auto forward_client =
        raylet_client_pool_.GetOrConnectByAddress(witness_forward_target.value());
    const uint64_t generation = incoming.version().generation();
    forward_client->UpdateRecoveryWitness(
        std::move(forwarded_request),
        [task_id, generation](const Status &status,
                              rpc::UpdateRecoveryWitnessReply &&forward_reply) {
          if (!status.ok() || !forward_reply.stored()) {
            RAY_LOG(WARNING).WithField(task_id)
                << "Patch 4N-WCHAIN secondary witness forward failed for generation "
                << generation << ": " << status;
          }
        });
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}


void NodeManager::HandleUpdateRecoveryWitnessBatch(
'''
if node.count(end_anchor) != 1:
    raise RuntimeError(f"node handler end anchor count={node.count(end_anchor)}")
node = node.replace(end_anchor, end_replacement, 1)
NODE.write_text(node)

# ---------------------------------------------------------------------------
# Wire marker used only on the raylet->raylet forwarded copy.
# ---------------------------------------------------------------------------
proto = PROTO.read_text()
proto_anchor = '''  // Fixed-R failure-path reservation. Mutually exclusive with the normal
  // manifest/TaskSpec/certificate publication payloads above.
  optional RecoveryWitnessClaim recovery_claim = 5;
}'''
proto_replacement = '''  // Fixed-R failure-path reservation. Mutually exclusive with the normal
  // manifest/TaskSpec/certificate publication payloads above.
  optional RecoveryWitnessClaim recovery_claim = 5;

  // Patch 4N-WCHAIN: marks the raylet-to-raylet secondary copy so a forwarded
  // witness update is stored normally and never recursively forwarded again.
  bool witness_forwarded = 6;
}'''
if proto.count(proto_anchor) != 1:
    raise RuntimeError(f"proto anchor count={proto.count(proto_anchor)}")
proto = proto.replace(proto_anchor, proto_replacement, 1)
PROTO.write_text(proto)

subprocess.check_call(["git", "diff", "--check"])
print("Applied ordinary K=1 witness-coordinator fan-out patch")
