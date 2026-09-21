"""Private, bounded Fixed-R streaming enrollment and single-owner-node recovery.

Use StreamingRecoveryReader with a StreamingRecoveryOwnerActor on another node.
The designated consumer, driver/job, and runtime must survive. Inputs are by
value, output count is finite/known, and there must be no earlier executor retry.
"""

import math
import time
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


def _timeout_ms(timeout_s):
    if not isinstance(timeout_s, (int, float)) or not math.isfinite(timeout_s):
        raise ValueError("Recovery timeout must be finite and positive")
    result = int(timeout_s * 1000)
    if not 0 < result < 2**63:
        raise ValueError("Recovery timeout must be finite and positive")
    return result


def streaming_recovery_address() -> bytes:
    """Address of this worker, for the explicit designated-consumer handshake."""
    worker = ray._private.worker.global_worker
    worker.check_connected()
    return worker.core_worker.get_streaming_recovery_address()


class StreamingRecoveryOwner:
    """Hold the original generator through explicit Fixed-R enrollment and EOF.

    The consumer must construct/retain its state before acknowledging receipt.
    Keep this owner handle alive until the consumer releases the stream; dropping
    it cancels the native task/enrollment and publishes cancellation tombstones.
    It cannot be passed to another worker. Transfer only its offered descriptor.
    """

    @classmethod
    def submit(
        cls,
        producer,
        *,
        expected_returns,
        consumer_address,
        args=(),
        kwargs=None,
        **task_options,
    ):
        generator = producer.options(
            **task_options,
            _streaming_recovery={
                "expected_returns": expected_returns,
                "consumer_address": consumer_address,
            },
        ).remote(*args, **(kwargs or {}))
        return cls(generator)

    def __init__(self, generator):
        self._generator = generator
        self._worker = ray._private.worker.global_worker
        self._worker.check_connected()
        # Confirms this is a locally enrolled generator, not an ordinary stream.
        self.submission()

    def submission(self):
        """Return (offered descriptor bytes, all-R/receipt readiness)."""
        if self._generator is None:
            raise StreamingRecoveryStateError("The owner handle is closed")
        return self._worker.core_worker.get_streaming_recovery_submission(
            self._generator.completed()
        )

    def confirm_receipt(self, descriptor: bytes, consumer_address: bytes) -> None:
        if self._generator is None:
            raise StreamingRecoveryStateError("The owner handle is closed")
        self._worker.core_worker.confirm_streaming_recovery_receipt(
            self._generator.completed(), descriptor, consumer_address
        )

    def next_ref(self):
        """Read one original item only after the enrollment barrier succeeds."""
        _, ready = self.submission()
        if not ready:
            raise StreamingRecoveryStateError("Streaming enrollment is not ready")
        return next(self._generator)

    def close(self) -> None:
        if self._generator is not None:
            try:
                ray.cancel(self._generator, force=False, recursive=True)
            finally:
                self._generator = None

    def __reduce__(self):
        raise TypeError("The original streaming owner handle cannot be transferred")


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

    def recover(self, timeout_s=60) -> None:
        """Claim, adopt, repair and dispatch one replay on this consumer.

        Settle the owner read and pause application gets/exports of retained
        refs before calling. No other worker may still use those outputs during
        this bounded handoff. Existing application aliases remain the same IDs.
        """
        timeout_ms = _timeout_ms(timeout_s)
        worker = ray._private.worker.global_worker
        worker.check_connected()
        with self._lock:
            snapshot = self.begin_recovery()
            completion = None
            generator = None
            try:
                completion = worker.core_worker.recover_streaming_task(
                    snapshot.descriptor,
                    snapshot.next_index,
                    snapshot.live_consumed_refs,
                    timeout_ms,
                )
                from ray._private.object_ref_generator import ObjectRefGenerator

                generator = ObjectRefGenerator(completion, worker)
                self.attach_replay(snapshot, generator)
            except BaseException:
                self.fail()
                if completion is not None:
                    try:
                        ray.cancel(completion, force=False, recursive=True)
                    finally:
                        if generator is None:
                            worker.core_worker.async_delete_object_ref_stream(completion)
                raise

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


class StreamingRecoveryOwnerActor:
    """Wrap with ray.remote(num_cpus=0), and place on the protected owner node.

    This actor holds stream state but is not itself recovered. Its normal
    producer task is replayed on the surviving consumer's behalf.
    """

    def __init__(self):
        self.stream = None

    def begin(self, producer, expected_returns, consumer_address, args, kwargs, options):
        if self.stream is not None:
            raise StreamingRecoveryStateError("This owner already holds a stream")
        self.stream = StreamingRecoveryOwner.submit(
            producer,
            expected_returns=expected_returns,
            consumer_address=consumer_address,
            args=args,
            kwargs=kwargs,
            **options,
        )
        return self.stream.submission()

    def offer(self):
        return self.stream.submission()

    def confirm(self, descriptor, consumer_address):
        self.stream.confirm_receipt(descriptor, consumer_address)

    def pull(self):
        try:
            ref = self.stream.next_ref()
        except StopIteration:
            ray.get(self.stream._generator.completed())
            return {"eof": True}
        if ref == self.stream._generator.completed():
            ray.get(ref)  # Surface producer errors, never forward completion as a yield.
        return {"ref": ref}

    def close(self):
        if self.stream is not None:
            self.stream.close()


class StreamingRecoveryReader:
    """One consumer's original delivery and owner-node-loss replay transport.

    Keep this reader and consumed ObjectRefs on the same surviving worker.
    Serialize reader operations and pause other gets/exports during recovery.
    release(index) requires all application uses/aliases of that output to end.
    close() acknowledges tombstones at every surviving selected holder.
    """

    @classmethod
    def submit(
        cls,
        owner,
        producer,
        *,
        expected_returns,
        args=(),
        kwargs=None,
        timeout_s=60,
        **options,
    ):
        timeout_ms = _timeout_ms(timeout_s)
        address = streaming_recovery_address()
        try:
            descriptor, _ = ray.get(
                owner.begin.remote(
                    producer, expected_returns, address, args, kwargs or {}, options
                ),
                timeout=timeout_ms / 1000,
            )
        except BaseException:
            # A queued begin can still finish after a caller timeout. Queue
            # close behind it so an abandoned offer cannot remain held forever.
            owner.close.remote()
            raise
        reader = cls(owner, descriptor, address, timeout_s)
        try:
            ray.get(owner.confirm.remote(descriptor, address), timeout=timeout_s)
            deadline = time.monotonic() + timeout_s
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Streaming enrollment did not become ready")
                offered, ready = ray.get(owner.offer.remote(), timeout=remaining)
                if offered != descriptor:
                    raise StreamingRecoveryStateError("Owner changed its descriptor")
                if ready:
                    break
                time.sleep(0.01)
            reader.consumer.mark_ready(descriptor)
            return reader
        except BaseException:
            reader.close()
            raise

    def __init__(self, owner, descriptor, address, timeout_s=60):
        self.owner = owner
        self.descriptor = descriptor
        self.consumer = StreamingRecoveryConsumer(descriptor, address)
        self.timeout_s = timeout_s
        self._closed = False

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __next__(self):
        if self._closed:
            raise StreamingRecoveryStateError("Streaming reader is closed")
        if self.consumer.phase == "completed":
            raise StopIteration
        if self.consumer.phase == "forwarding":
            ticket = self.consumer.begin_owner_read()
            try:
                # A timeout alone would leave this read outstanding; wait for
                # a settled actor result or failure before taking a snapshot.
                response = ray.get(self.owner.pull.remote())
            except ray.exceptions.RayActorError:
                self.consumer.settle_failed_owner_read(ticket)
                self.recover()
            except BaseException:
                self.consumer.fail()
                raise
            else:
                if response.get("eof"):
                    self.consumer.accept_owner_eof(ticket)
                    raise StopIteration
                return self.consumer.accept_owner_item(ticket, response["ref"])
        return self.consumer.read_replay(timeout_s=None)

    def recover(self):
        """Explicit recovery also supports retained outputs after original EOF."""
        self.consumer.recover(self.timeout_s)

    def release(self, index):
        self.consumer.release(index)

    def close(self):
        if self._closed:
            return
        worker = ray._private.worker.global_worker
        worker.check_connected()
        try:
            try:
                ray.get(self.owner.close.remote(), timeout=self.timeout_s)
            except ray.exceptions.RayActorError:
                pass
            worker.core_worker.close_streaming_recovery(
                self.descriptor, _timeout_ms(self.timeout_s)
            )
        finally:
            self.consumer.close()
        # A timed-out close can be retried; it must not report durable success.
        self._closed = True

    def __reduce__(self):
        raise TypeError("Streaming reader must remain on its designated consumer")
