"""Local adapter tests; no Ray cluster or distributed failure is simulated."""

import pickle
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import ray
from ray._private.streaming_recovery import (
    StreamingRecoveryConsumer,
    StreamingRecoveryCountError,
    StreamingRecoveryReader,
    StreamingRecoveryRequired,
    StreamingRecoveryStateError,
)
from ray._raylet import (
    JobID,
    NodeID,
    TaskID,
    WorkerID,
    _inspect_recovery_stream_descriptor,
    _recovery_stream_generator_id,
    _recovery_stream_return_id,
)
from ray.core.generated.common_pb2 import Address, RecoveryStreamDescriptor


def address():
    return Address(
        worker_id=WorkerID.from_random().binary(),
        node_id=NodeID.from_random().binary(),
        ip_address="127.0.0.1",
        port=10001,
    )


@pytest.fixture
def make_consumer():
    consumers = []

    def make(count=3, ready=True):
        job_id = JobID.from_int(1)
        task_id = TaskID.for_fake_task(job_id)
        consumer = address()
        owner = address()
        descriptor = RecoveryStreamDescriptor(
            version=1,
            task_id=task_id.binary(),
            generator_id=_recovery_stream_generator_id(task_id),
            expected_returns=count,
            consumer_address=consumer,
        )
        manifest = descriptor.manifest
        manifest.task_id = task_id.binary()
        manifest.job_id = job_id.binary()
        manifest.target_holder_count = 2
        manifest.witness_count = 2
        manifest.max_recovery_attempts = 2
        manifest.version.generation = 1
        manifest.succession.add(address=owner, rank=0, failure_domain_id=owner.node_id)
        manifest.witness_raylets.add().CopyFrom(address())
        manifest.witness_raylets.add().CopyFrom(address())
        serialized = descriptor.SerializeToString()
        state = StreamingRecoveryConsumer(serialized, consumer.SerializeToString())
        consumers.append(state)
        if ready:
            state.mark_ready(serialized)
        return state, serialized

    yield make
    for consumer in consumers:
        consumer.close()


def output(descriptor, index):
    return ray.ObjectRef(_recovery_stream_return_id(descriptor, index))


def deliver(state, descriptor, index):
    ticket = state.begin_owner_read()
    ref = output(descriptor, index)
    assert state.accept_owner_item(ticket, ref) is ref
    assert state.next_index == index + 1
    return ticket, ref


class ReplayReader:
    def __init__(self, descriptor, events=()):
        self.completion = ray.ObjectRef(
            _inspect_recovery_stream_descriptor(descriptor)["generator_id"]
        )
        self.events = iter(events)
        self.timeouts = []
        self.exhausted = False

    def _stream_exhausted(self):
        return self.exhausted

    def completed(self):
        return self.completion

    def _next_sync(self, timeout_s):
        self.timeouts.append(timeout_s)
        event = next(self.events)
        if isinstance(event, BaseException):
            raise event
        if callable(event):
            return event()
        return ray.ObjectRef.nil() if event is None else event


def test_native_descriptor_helpers_reject_malformed_inputs(make_consumer):
    _, descriptor = make_consumer()
    info = _inspect_recovery_stream_descriptor(descriptor)
    assert info["expected_returns"] == 3
    assert output(descriptor, 0).task_id().binary() == info["task_id"]
    assert output(descriptor, 0).binary() != info["generator_id"]
    with pytest.raises(ValueError):
        _inspect_recovery_stream_descriptor(b"not a protobuf")
    with pytest.raises(ValueError):
        _recovery_stream_generator_id(TaskID.nil())
    for index in (-1, 3):
        with pytest.raises(ValueError):
            _recovery_stream_return_id(descriptor, index)
    with pytest.raises(ValueError):
        StreamingRecoveryConsumer(descriptor, address().SerializeToString())


def test_no_delivery_or_recovery_before_readiness(make_consumer):
    state, descriptor = make_consumer(ready=False)
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_owner_read()
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_recovery()
    with pytest.raises(StreamingRecoveryStateError):
        state.mark_ready(b"different enrollment")
    state.mark_ready(descriptor)
    deliver(state, descriptor, 0)
    with pytest.raises(StreamingRecoveryStateError):
        state.mark_ready(descriptor)
    assert state.next_index == 1


def test_records_live_prefix_before_return_and_never_rewinds(make_consumer):
    state, descriptor = make_consumer()
    _, first = deliver(state, descriptor, 0)
    _, second = deliver(state, descriptor, 1)
    snapshot = state.begin_recovery()
    assert snapshot.next_index == 2
    assert snapshot.live_consumed_refs == (first, second)
    with pytest.raises(StreamingRecoveryStateError):
        state.release(0)
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_recovery()
    third = output(descriptor, 2)
    reader = ReplayReader(descriptor, [None, third])
    state.attach_replay(snapshot, reader)
    with pytest.raises(StreamingRecoveryStateError):
        state.attach_replay(snapshot, reader)
    assert state.read_replay(timeout_s=0.25) is None
    assert state.next_index == 2
    assert state.read_replay() is third
    with pytest.raises(StopIteration):
        state.read_replay()
    assert state.phase == "completed"
    assert state.next_index == 3
    assert reader.timeouts == [0.25, 0, 0]
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_recovery()  # Version 1 does not admit a second owner failure.


def test_release_changes_retention_without_changing_cursor(make_consumer):
    state, descriptor = make_consumer()
    deliver(state, descriptor, 0)
    _, live = deliver(state, descriptor, 1)
    state.release(0)
    assert state.next_index == 2
    snapshot = state.begin_recovery()
    assert snapshot.next_index == 2
    assert snapshot.live_consumed_refs == (live,)


def test_settle_outstanding_read_and_reject_late_or_duplicate_responses(make_consumer):
    state, descriptor = make_consumer()
    ticket = state.begin_owner_read()
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_owner_read()
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_recovery()
    state.settle_failed_owner_read(ticket)
    new_ticket = state.begin_owner_read()
    with pytest.raises(StreamingRecoveryStateError):
        state.accept_owner_item(ticket, output(descriptor, 0))
    state.accept_owner_item(new_ticket, output(descriptor, 0))
    with pytest.raises(StreamingRecoveryStateError):
        state.accept_owner_item(new_ticket, output(descriptor, 0))
    snapshot = state.begin_recovery()
    with pytest.raises(StreamingRecoveryStateError):
        state.accept_owner_item(ticket, output(descriptor, 0))
    assert state.phase == "recovering"
    assert snapshot.next_index == state.next_index == 1


@pytest.mark.parametrize("count", [0, 3])
def test_recovery_after_original_eof_retains_live_refs(make_consumer, count):
    state, descriptor = make_consumer(count=count)
    refs = [deliver(state, descriptor, i)[1] for i in range(count)]
    state.accept_owner_eof(state.begin_owner_read())
    assert state.phase == "completed"
    snapshot = state.begin_recovery()
    assert snapshot.next_index == count
    assert snapshot.live_consumed_refs == tuple(refs)
    state.attach_replay(snapshot, ReplayReader(descriptor))
    with pytest.raises(StopIteration):
        state.read_replay()
    assert state.phase == "completed"


@pytest.mark.parametrize("phase", ["owner", "replay"])
def test_early_eof_fails_without_fabricating_delivery(make_consumer, phase):
    state, descriptor = make_consumer()
    deliver(state, descriptor, 0)
    with pytest.raises(StreamingRecoveryCountError):
        if phase == "owner":
            state.accept_owner_eof(state.begin_owner_read())
        else:
            state.attach_replay(state.begin_recovery(), ReplayReader(descriptor))
            state.read_replay()
    assert state.next_index == 1
    assert state.phase == "failed"


@pytest.mark.parametrize("kind", ["wrong_index", "other_task", "completion", "nil"])
def test_invalid_forwarded_ref_does_not_advance_cursor(make_consumer, kind):
    state, descriptor = make_consumer()
    _, other = make_consumer()
    refs = {
        "wrong_index": output(descriptor, 1),
        "other_task": output(other, 0),
        "completion": ReplayReader(descriptor).completed(),
        "nil": ray.ObjectRef.nil(),
    }
    with pytest.raises(StreamingRecoveryStateError):
        state.accept_owner_item(state.begin_owner_read(), refs[kind])
    assert state.next_index == 0
    assert state.phase == "failed"


def test_replay_does_not_deliver_consumed_prefix_or_excess_items(make_consumer):
    state, descriptor = make_consumer(count=1)
    deliver(state, descriptor, 0)
    state.attach_replay(
        state.begin_recovery(), ReplayReader(descriptor, [output(descriptor, 0)])
    )
    with pytest.raises(StreamingRecoveryCountError):
        state.read_replay()
    assert state.next_index == 1
    assert state.phase == "failed"


def test_completion_error_is_raised_and_not_delivered(make_consumer, monkeypatch):
    state, descriptor = make_consumer()
    reader = ReplayReader(descriptor)
    reader.events = iter([reader.completed()])
    state.attach_replay(state.begin_recovery(), reader)
    error = RuntimeError("producer failed")

    def resolve(ref):
        assert ref is reader.completed()
        raise error

    monkeypatch.setattr(ray, "get", resolve)
    with pytest.raises(RuntimeError, match="producer failed") as raised:
        state.read_replay()
    assert raised.value is error
    assert state.next_index == 0
    assert state.phase == "failed"


def test_rejects_foreign_snapshot_and_wrong_replay_handle(make_consumer):
    state, descriptor = make_consumer()
    other, other_descriptor = make_consumer()
    snapshot = state.begin_recovery()
    with pytest.raises(StreamingRecoveryStateError):
        state.attach_replay(other.begin_recovery(), ReplayReader(descriptor))
    with pytest.raises(StreamingRecoveryStateError):
        state.attach_replay(snapshot, ReplayReader(other_descriptor))
    assert state.phase == "recovering"
    state.attach_replay(snapshot, ReplayReader(descriptor))


def test_closed_or_failed_state_cannot_restart(make_consumer):
    state, descriptor = make_consumer()
    ticket = state.begin_owner_read()
    state.fail()
    state.close()
    state.close()
    with pytest.raises(StreamingRecoveryStateError):
        state.mark_ready(descriptor)
    with pytest.raises(StreamingRecoveryStateError):
        state.begin_recovery()
    with pytest.raises(StreamingRecoveryStateError):
        state.accept_owner_item(ticket, output(descriptor, 0))
    assert state.phase == "closed"


def test_close_during_replay_read_discards_late_item(make_consumer):
    state, descriptor = make_consumer()

    def late_item():
        state.close()
        return output(descriptor, 0)

    state.attach_replay(state.begin_recovery(), ReplayReader(descriptor, [late_item]))
    with pytest.raises(StreamingRecoveryStateError):
        state.read_replay()
    assert state.next_index == 0
    assert state.phase == "closed"


def test_only_descriptor_is_transferable(make_consumer):
    state, descriptor = make_consumer()
    ticket = state.begin_owner_read()
    state.settle_failed_owner_read(ticket)
    snapshot = state.begin_recovery()
    assert pickle.loads(pickle.dumps(descriptor)) == descriptor
    for local_state in (state, ticket, snapshot):
        with pytest.raises(TypeError):
            pickle.dumps(local_state)


@pytest.fixture
def reader_transport(make_consumer, monkeypatch):
    state, descriptor = make_consumer(count=1)
    owner = SimpleNamespace(pull=SimpleNamespace(remote=Mock(return_value=object())))
    consumer_address = RecoveryStreamDescriptor.FromString(descriptor).consumer_address
    reader = StreamingRecoveryReader(
        owner, descriptor, consumer_address.SerializeToString(),
    )
    reader.consumer = state
    wait = Mock(side_effect=lambda refs, **kwargs: (refs, []))
    get = Mock(return_value={"ref": output(descriptor, 0)})
    monkeypatch.setattr(ray, "wait", wait)
    monkeypatch.setattr(ray, "get", get)
    return reader, owner.pull.remote, wait, get


def test_poll_timeout_keeps_one_read_and_does_not_advance(reader_transport):
    reader, pull, wait, get = reader_transport
    wait.side_effect = lambda refs, **kwargs: ([], refs)
    pending = reader.get_waitable()
    ticket = reader.consumer._owner_read
    for _ in range(3):
        assert reader.poll_next() is None
        assert reader.get_waitable() is pending
        assert reader.consumer._owner_read is ticket
    assert pull.call_count == 1
    assert reader.consumer.next_index == 0
    assert reader.consumer.phase == "forwarding"
    get.assert_not_called()
    with pytest.raises(StreamingRecoveryStateError, match="outstanding owner read"):
        reader.recover()
    assert not reader.consumer._recovery_started


def test_poll_local_fetch_race_keeps_ticket(reader_transport):
    reader, pull, _, get = reader_transport
    get.side_effect = ray.exceptions.GetTimeoutError("not local yet")
    pending = reader.get_waitable()
    assert reader.poll_next() is None
    assert reader.get_waitable() is pending
    get.side_effect = None
    ref = reader.poll_next()
    assert ref == output(reader.descriptor, 0)
    assert reader.consumer.next_index == 1
    assert pull.call_count == 1  # No prefetch after accepting this item.
    assert reader._pending_read is None


def test_owner_pending_response_releases_ticket_without_consuming(reader_transport):
    reader, pull, _, get = reader_transport
    get.return_value = {"pending": True}
    assert reader.poll_next() is None
    assert reader.consumer._owner_read is None
    assert reader.consumer.next_index == 0
    assert reader._pending_read is None
    get.return_value = {"ref": output(reader.descriptor, 0)}
    assert reader.poll_next() == output(reader.descriptor, 0)
    assert pull.call_count == 2


def test_poll_owner_loss_requires_explicit_recovery(reader_transport, monkeypatch):
    reader, pull, _, get = reader_transport
    get.side_effect = ray.exceptions.RayActorError()
    recover = Mock()
    monkeypatch.setattr(reader.consumer, "recover", recover)
    for _ in range(2):
        with pytest.raises(StreamingRecoveryRequired, match="quiesce"):
            reader.poll_next()
    assert pull.call_count == 1
    assert reader.consumer.next_index == 0
    assert reader.consumer._owner_read is None
    recover.assert_not_called()
    assert not reader.consumer._recovery_started
    reader.recover()
    recover.assert_called_once_with(reader.timeout_s)
    assert not reader._recovery_required


def test_poll_producer_error_is_terminal(reader_transport):
    reader, pull, _, get = reader_transport
    get.side_effect = ValueError("producer failed")
    with pytest.raises(ValueError, match="producer failed"):
        reader.poll_next()
    assert reader.consumer.phase == "failed"
    assert reader._pending_read is None
    with pytest.raises(StreamingRecoveryStateError, match="cannot read"):
        reader.poll_next()
    assert pull.call_count == 1
    assert not reader._recovery_required


def test_blocking_iterator_still_recovers_on_owner_loss(reader_transport, monkeypatch):
    reader, _, _, get = reader_transport
    get.side_effect = ray.exceptions.RayActorError()
    ref = output(reader.descriptor, 0)
    replay = ReplayReader(reader.descriptor, [ref])

    def recover(timeout_s):
        reader.consumer.attach_replay(reader.consumer.begin_recovery(), replay)

    monkeypatch.setattr(reader.consumer, "recover", recover)
    assert next(reader) is ref
    assert reader.consumer.phase == "replaying"
    assert replay.timeouts == [None]
    assert not reader._recovery_required


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan"), "0"])
def test_poll_invalid_timeout_does_not_start_read(reader_transport, timeout):
    reader, pull, _, _ = reader_transport
    with pytest.raises(ValueError, match="Read timeout"):
        reader.poll_next(timeout)
    pull.assert_not_called()


def test_poll_early_eof_is_a_count_error(reader_transport):
    reader, _, _, get = reader_transport
    get.return_value = {"eof": True}
    with pytest.raises(StreamingRecoveryCountError, match="ended before"):
        reader.poll_next()
    assert reader.consumer.phase == "failed"


def test_poll_replay_preserves_timeout_and_fetches_only_completion(reader_transport):
    reader, pull, wait, get = reader_transport
    ref = output(reader.descriptor, 0)
    replay = ReplayReader(reader.descriptor, [None, ref])
    reader.consumer.attach_replay(reader.consumer.begin_recovery(), replay)
    assert reader.get_waitable() is replay
    assert reader.poll_next() is None
    assert reader.consumer.next_index == 0
    assert reader.poll_next() is ref
    assert replay.timeouts == [0, 0]
    wait.assert_not_called()
    get.assert_not_called()
    pull.assert_not_called()

    replay.exhausted = True
    assert reader.get_waitable() is replay.completed()
    wait.side_effect = lambda refs, **kwargs: ([], refs)
    assert reader.poll_next() is None
    assert reader.consumer.phase == "replaying"
    assert replay.timeouts == [0, 0]
    wait.side_effect = lambda refs, **kwargs: (refs, [])
    with pytest.raises(StopIteration):
        reader.poll_next()
    wait.assert_called_with([replay.completed()], timeout=0, fetch_local=True)
    assert reader.consumer.phase == "completed"


def test_close_discards_pending_response_and_is_retryable(reader_transport, monkeypatch):
    reader, pull, _, get = reader_transport
    pending = reader.get_waitable()
    dependency = object()
    reader._input_refs = (dependency,)
    reader.owner.close = SimpleNamespace(remote=Mock(return_value=object()))
    close = Mock(side_effect=TimeoutError("tombstone barrier"))
    worker = SimpleNamespace(
        check_connected=Mock(),
        core_worker=SimpleNamespace(close_streaming_recovery=close),
    )
    monkeypatch.setattr(ray._private.worker, "global_worker", worker)
    with pytest.raises(TimeoutError, match="tombstone barrier"):
        reader.close()
    assert not reader._closed
    assert reader._input_refs == (dependency,)
    assert reader._pending_read is None
    assert reader.consumer.phase == "closed"
    with pytest.raises(StreamingRecoveryStateError, match="closed"):
        reader.poll_next()
    assert all(call.args[0] is not pending for call in get.call_args_list)
    close.side_effect = None
    reader.close()
    assert reader._closed
    assert reader._input_refs == ()
    assert pull.call_count == 1
