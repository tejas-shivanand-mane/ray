// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/future_resolver.h"

#include <memory>
#include <utility>

#include "ray/common/ray_config.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/core_worker_process.h"

namespace ray {
namespace core {

bool FutureResolver::RecordAndIsRecoverySuccessor(
    const ObjectID &object_id, const rpc::Address &owner_address) {
  if (owner_address.worker_id().size() != WorkerID::Size()) {
    return false;
  }

  const WorkerID owner_id = WorkerID::FromBinary(owner_address.worker_id());
  absl::MutexLock lock(&recovery_owner_mutex_);
  auto [it, inserted] = initial_owner_by_object_.try_emplace(object_id, owner_id);
  return !inserted && it->second != owner_id;
}

void FutureResolver::ClearRecoveryOwnerTracking(const ObjectID &object_id) {
  absl::MutexLock lock(&recovery_owner_mutex_);
  initial_owner_by_object_.erase(object_id);
  recovery_reentry_in_flight_.erase(object_id);
}

void FutureResolver::ResolveFutureAsync(const ObjectID &object_id,
                                        const rpc::Address &owner_address) {
  if (rpc_address_.worker_id() == owner_address.worker_id()) {
    // We do not need to resolve objects that we own. This can happen if a task
    // with a borrowed reference executes on the object's owning worker. It also
    // happens when this worker becomes the Fixed-R acting owner; no future
    // owner-failure handoff is needed after that promotion.
    ClearRecoveryOwnerTracking(object_id);
    return;
  }

  // Preserve the first owner for this unresolved ObjectID. A later call using
  // a different owner means recovery has already handed ownership to an acting
  // successor, which is the only case where FutureResolver itself should
  // transparently re-enter recovery after an owner RPC failure.
  static_cast<void>(RecordAndIsRecoverySuccessor(object_id, owner_address));

  auto conn = owner_clients_->GetOrConnect(owner_address);

  rpc::GetObjectStatusRequest request;
  request.set_object_id(object_id.Binary());
  request.set_owner_worker_id(owner_address.worker_id());
  conn->GetObjectStatus(
      std::move(request),
      [this, object_id, owner_address](const Status &status,
                                       const rpc::GetObjectStatusReply &reply) {
        ProcessResolvedObject(object_id, owner_address, status, reply);
      });
}

void FutureResolver::ProcessResolvedObject(const ObjectID &object_id,
                                           const rpc::Address &owner_address,
                                           const Status &status,
                                           const rpc::GetObjectStatusReply &reply) {

  // A recovery successor may have taken ownership while an older
  // GetObjectStatus RPC was still in flight. Ignore responses from
  // an owner that is no longer the current owner of this ObjectID.
  rpc::Address current_owner;

  if (reference_counter_->GetOwner(object_id, &current_owner) &&
      current_owner.worker_id() != owner_address.worker_id()) {
    RAY_LOG(DEBUG).WithField(object_id)
        << "Ignoring stale GetObjectStatus response from previous owner";
    return;
  }

  if (status.ok() && reply.has_recovery_metadata() && recovery_metadata_callback_) {
    recovery_metadata_callback_(object_id, reply.recovery_metadata());
  }

  if (!status.ok()) {
    RAY_LOG(WARNING).WithField(object_id)
        << "Failed to retrieve deserialized object value: " << status;

    const bool fixed_r_enabled =
        RayConfig::instance().enable_recovery_succession() &&
        RayConfig::instance().enable_recovery_witness_holder_baseline();
    const bool recovery_successor =
        RecordAndIsRecoverySuccessor(object_id, owner_address);

    if (fixed_r_enabled && recovery_successor && CoreWorkerProcess::IsInitialized()) {
      bool start_reentry = false;
      {
        absl::MutexLock lock(&recovery_owner_mutex_);
        start_reentry = recovery_reentry_in_flight_.insert(object_id).second;
      }

      if (!start_reentry) {
        // Another failed status RPC for the same acting owner arrived while a
        // recovery re-entry is already in flight. Do not publish OWNER_DIED and
        // wake the blocked getter prematurely.
        return;
      }

      RAY_LOG(INFO).WithField(object_id)
          << "Acting recovery owner became unreachable; re-entering Fixed-R recovery";

      CoreWorkerProcess::GetCoreWorker().TryRecoverTaskDependency(
          object_id,
          [this, object_id, owner_address](bool started) {
            {
              absl::MutexLock lock(&recovery_owner_mutex_);
              recovery_reentry_in_flight_.erase(object_id);
            }

            rpc::Address latest_owner;
            const bool owner_changed =
                reference_counter_->GetOwner(object_id, &latest_owner) &&
                latest_owner.worker_id() != owner_address.worker_id();

            if (started || owner_changed) {
              RAY_LOG(INFO).WithField(object_id)
                  << "Transparent recovery-owner handoff kept blocked future alive";
              return;
            }

            // Recovery could not be re-entered and the failed acting owner is
            // still authoritative locally. Preserve the ordinary terminal
            // behavior so callers receive OWNER_DIED rather than hanging.
            in_memory_store_->Put(
                RayObject(rpc::ErrorType::OWNER_DIED),
                object_id,
                reference_counter_->HasReference(object_id));
          });
      return;
    }

    // Original-owner failure keeps the existing behavior. The ordinary get /
    // dependency-recovery path consumes this OWNER_DIED signal and initiates
    // the first Fixed-R attempt; only later acting-owner failures are handled
    // transparently above.
    in_memory_store_->Put(RayObject(rpc::ErrorType::OWNER_DIED),
                          object_id,
                          reference_counter_->HasReference(object_id));
  } else if (reply.status() == rpc::GetObjectStatusReply::OUT_OF_SCOPE) {
    // The owner replied that the object has gone out of scope (this is an edge
    // case in the distributed ref counting protocol where a borrower dies
    // before it can notify the owner of another borrower). Store an error so
    // that an exception will be thrown immediately when the worker tries to
    // get the value.
    ClearRecoveryOwnerTracking(object_id);
    in_memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_DELETED),
                          object_id,
                          reference_counter_->HasReference(object_id));
  } else if (reply.status() == rpc::GetObjectStatusReply::CREATED) {
    // The object is either an indicator that the object is in Plasma, or
    // the object has been returned directly in the reply. In either
    // case, we put the corresponding RayObject into the in-memory store.
    // If the owner later fails or the object is released, the raylet
    // will eventually store an error in Plasma on our behalf.

    ClearRecoveryOwnerTracking(object_id);

    // We save the returned locality data first in order to ensure that it
    // is available for any tasks whose submission is triggered by the in-memory
    // store Put().
    absl::flat_hash_set<NodeID> locations;
    for (const auto &node_id : reply.node_ids()) {
      locations.emplace(NodeID::FromBinary(node_id));
    }
    report_locality_data_callback_(object_id, locations, reply.object_size());

    // Put the RayObject into the in-memory store.
    const auto &data = reply.object().data();
    std::shared_ptr<LocalMemoryBuffer> data_buffer;
    if (!data.empty()) {
      RAY_LOG(DEBUG).WithField(object_id)
          << "Object returned directly in GetObjectStatus reply, "
          << "putting it in memory store";
      data_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(data.data())),
          data.size());
    } else {
      RAY_LOG(DEBUG).WithField(object_id)
          << "Object not returned directly in GetObjectStatus reply, "
          << "fetching it from Plasma";
    }
    const auto &metadata = reply.object().metadata();
    std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
    if (!metadata.empty()) {
      metadata_buffer = std::make_shared<LocalMemoryBuffer>(
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(metadata.data())),
          metadata.size());
    }
    auto inlined_refs =
        VectorFromProtobuf<rpc::ObjectReference>(reply.object().nested_inlined_refs());
    for (const auto &inlined_ref : inlined_refs) {
      const ObjectID nested_object_id = ObjectID::FromBinary(inlined_ref.object_id());

      reference_counter_->AddBorrowedObject(
          nested_object_id, object_id, inlined_ref.owner_address());

      if (inlined_ref.has_recovery_metadata() && recovery_metadata_callback_) {
        recovery_metadata_callback_(nested_object_id, inlined_ref.recovery_metadata());
      }
    }
    in_memory_store_->Put(RayObject(data_buffer, metadata_buffer, inlined_refs),
                          object_id,
                          reference_counter_->HasReference(object_id));
  }
}

}  // namespace core
}  // namespace ray
