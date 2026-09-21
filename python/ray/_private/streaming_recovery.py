"""Consumer delivery state for the bounded Fixed-R streaming experiment.

Internal integration component, not an owner-loss API. A trusted transport must
complete the all-R enrollment gate, forward owner reads, obtain witness claims,
and adopt/dispatch the replay before attaching its local ObjectRefGenerator.
State lives on the designated surviving consumer and cannot be serialized.
"""

from dataclasses import dataclass
from threading import RLock
from typing import Tuple

import ray
from ray._raylet import (
    _inspect_recovery_stream_descriptor,
    _recovery_stream_return_id,
)
from ray.core.generated.common_pb2 import Address


class StreamingRecoveryStateError(RuntimeError):
    """An internal transport operation violated the consumer state contract."""


class StreamingRecoveryCountError(StreamingRecoveryStateError):
    """The producer ended early or yielded beyond its declared count."""


@dataclass(frozen=True, eq=False)
class _OwnerRead:
    index: int

    def __reduce__(self):
        raise TypeError("Owner read tickets are local to the surviving consumer")


@dataclass(frozen=True, eq=False)
class _RecoverySnapshot:
    descriptor: bytes
    next_index: int
    live_consumed_refs: Tuple["ray.ObjectRef", ...]

    def __reduce__(self):
        raise TypeError("Recovery snapshots are local to the surviving consumer")


class StreamingRecoveryConsumer:
    """Serialize delivery progress, live refs, and one recovery transition.

    ``accept_owner_item`` records its ref and cursor before exposing the ref to
    the application. Retained refs remain strongly live until ``release`` or
    ``close``. Call ``release`` only once the application has stopped using ALL
    aliases of that output, including submitted/exported refs. Native adoption
    rejects an unlisted ref if this contract is violated; it cannot silently
    promote only part of the reference table.

    The transport must settle its one outstanding owner read before taking a
    recovery snapshot, and must discard late responses using the original read
    ticket. No transport or native ownership mutation occurs in this class.
    """

    def __init__(self, descriptor: bytes, consumer_address: bytes):
        info = _inspect_recovery_stream_descriptor(descriptor)
        expected = Address.FromString(info["consumer_address"])
        if Address.FromString(consumer_address) != expected:
            raise ValueError("This stream belongs to a different consumer")
        self._descriptor = descriptor
        self._generator_id = info["generator_id"]
        self._expected_returns = info["expected_returns"]
        self._next_index = 0
        self._retained = {}
        self._phase = "offered"
        self._owner_read = None
        self._snapshot = None
        self._generator = None
        self._reading_replay = False
        self._recovery_started = False
        self._lock = RLock()

    @property
    def next_index(self) -> int:
        with self._lock:
            return self._next_index

    @property
    def phase(self) -> str:
        with self._lock:
            return self._phase

    def mark_ready(self, descriptor: bytes) -> None:
        """Accept the transport's completed all-R + consumer-receipt barrier.

        Matching bytes alone are not readiness evidence. Only the trusted
        enrollment transport may call this after its gate succeeds.
        """
        with self._lock:
            if descriptor != self._descriptor or self._phase != "offered":
                raise StreamingRecoveryStateError("Unexpected readiness notification")
            self._phase = "forwarding"

    def begin_owner_read(self) -> _OwnerRead:
        """Reserve one read, including the EOF check when the cursor equals N."""
        with self._lock:
            if self._phase != "forwarding" or self._owner_read is not None:
                raise StreamingRecoveryStateError("An owner read cannot start now")
            ticket = _OwnerRead(self._next_index)
            self._owner_read = ticket
            return ticket

    def _check_ticket(self, ticket: _OwnerRead) -> None:
        if self._phase != "forwarding" or self._owner_read is not ticket:
            raise StreamingRecoveryStateError("Stale or unknown owner read response")

    def _record_item(self, ref: "ray.ObjectRef") -> "ray.ObjectRef":
        if not isinstance(ref, ray.ObjectRef) or ref.is_nil():
            raise StreamingRecoveryStateError("Delivery requires a non-nil ObjectRef")
        if ref.binary() == self._generator_id:
            raise StreamingRecoveryStateError("Completion/error ref is not a yielded item")
        if self._next_index >= self._expected_returns:
            raise StreamingRecoveryCountError("Producer exceeded the declared count")
        expected_id = _recovery_stream_return_id(self._descriptor, self._next_index)
        if ref.binary() != expected_id:
            raise StreamingRecoveryStateError("Yield ObjectID does not match the next index")
        self._retained[self._next_index] = ref
        self._next_index += 1
        return ref

    def accept_owner_item(self, ticket: _OwnerRead, ref: "ray.ObjectRef"):
        with self._lock:
            self._check_ticket(ticket)
            try:
                result = self._record_item(ref)
            except Exception:
                self._phase = "failed"
                raise
            finally:
                self._owner_read = None
            return result

    def accept_owner_eof(self, ticket: _OwnerRead) -> None:
        """Transport must first resolve the original completion ref successfully."""
        with self._lock:
            self._check_ticket(ticket)
            self._owner_read = None
            if self._next_index != self._expected_returns:
                self._phase = "failed"
                raise StreamingRecoveryCountError("Producer ended before the declared count")
            self._phase = "completed"

    def settle_failed_owner_read(self, ticket: _OwnerRead) -> None:
        """Transport has settled/cancelled the read without accepting an item.

        This is not failure detection. The subsequent claim path must establish
        owner-node death; a transient read failure alone does not authorize it.
        """
        with self._lock:
            self._check_ticket(ticket)
            self._owner_read = None

    def begin_recovery(self) -> _RecoverySnapshot:
        with self._lock:
            if (
                self._phase not in ("forwarding", "completed")
                or self._owner_read is not None
                or self._recovery_started
            ):
                raise StreamingRecoveryStateError(
                    "Recovery requires a settled owner read and no previous recovery"
                )
            snapshot = _RecoverySnapshot(
                self._descriptor,
                self._next_index,
                tuple(self._retained[i] for i in sorted(self._retained)),
            )
            self._snapshot = snapshot
            self._recovery_started = True
            self._phase = "recovering"
            return snapshot

    def attach_replay(self, snapshot: _RecoverySnapshot, generator) -> None:
        """Attach the local generator after checked native adoption and dispatch.

        The transport transfers exactly one native-acquired completion ref into
        this handle with skip_adding_local_ref=True. Drop the transport's snapshot
        after attachment so its temporary strong refs do not extend lifetimes.
        On rejection the transport remains responsible for cancelling/deleting
        the untransferred native stream.
        """
        with self._lock:
            if self._phase != "recovering" or snapshot is not self._snapshot:
                raise StreamingRecoveryStateError("Stale or duplicate replay attachment")
            if generator.completed().binary() != self._generator_id:
                raise StreamingRecoveryStateError("Replay changed the completion ObjectID")
            self._generator = generator
            self._snapshot = None
            self._phase = "replaying"

    def read_replay(self, timeout_s=0):
        """Return the next ref, None on timeout, or raise StopIteration at EOF.

        Count overflow, wrong IDs and the generator's error/completion ref are
        never exposed as ordinary yielded values. The underlying native reader
        supplies backpressure credit and suppresses the consumed replay prefix.
        """
        with self._lock:
            if self._phase == "completed":
                raise StopIteration
            if self._phase != "replaying" or self._reading_replay:
                raise StreamingRecoveryStateError("A replay read cannot start now")
            generator = self._generator
            self._reading_replay = True
        try:
            try:
                ref = generator._next_sync(timeout_s=timeout_s)
            except StopIteration:
                with self._lock:
                    if self._phase != "replaying":
                        raise StreamingRecoveryStateError("Replay state changed during a read")
                    if self._next_index != self._expected_returns:
                        raise StreamingRecoveryCountError(
                            "Replay ended before the declared count"
                        )
                    self._phase = "completed"
                raise
            with self._lock:
                if self._phase != "replaying":
                    raise StreamingRecoveryStateError("Replay state changed during a read")
                if ref.is_nil():
                    return None
                if ref.binary() == self._generator_id:
                    # ObjectRefGenerator returns this only to surface a task
                    # error. Re-raise the original Ray exception to the caller.
                    ray.get(ref)
                    raise StreamingRecoveryStateError("Unexpected successful completion ref")
                return self._record_item(ref)
        except StopIteration:
            raise
        except BaseException:
            with self._lock:
                if self._phase != "closed":
                    self._phase = "failed"
            raise
        finally:
            with self._lock:
                self._reading_replay = False

    def release(self, index: int) -> None:
        """Drop the adapter's hold after all application uses of this item end."""
        with self._lock:
            if self._phase in ("recovering", "closed") or self._reading_replay:
                raise StreamingRecoveryStateError("References cannot be released now")
            if index not in self._retained:
                raise StreamingRecoveryStateError("No retained item at this index")
            del self._retained[index]

    def fail(self) -> None:
        """Transport reports a terminal producer/enrollment/recovery failure."""
        with self._lock:
            if self._phase != "closed":
                self._phase = "failed"
                self._owner_read = None
                self._snapshot = None

    def close(self) -> None:
        """Drop local holds after transport cancellation/tombstone handling.

        Idempotent. Does not send RPCs or revoke exported refs. Dropping an
        attached ObjectRefGenerator uses its existing native deletion path.
        """
        with self._lock:
            self._phase = "closed"
            self._owner_read = None
            self._snapshot = None
            self._generator = None
            self._retained.clear()

    def __reduce__(self):
        raise TypeError("Streaming recovery consumer state must remain on its consumer")
