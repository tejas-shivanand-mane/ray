"""Original enrollment/dispatch on three local raylets; no owner-loss replay."""

import os
from pathlib import Path

import psutil
import pytest

import ray
from ray._common.ray_option_utils import task_options
from ray._common.test_utils import wait_for_condition
from ray._private.streaming_recovery import (
    StreamingRecoveryConsumer,
    StreamingRecoveryOwner,
    streaming_recovery_address,
)
from ray.cluster_utils import Cluster
from ray.core.generated.common_pb2 import Address, RecoveryStreamDescriptor
from ray.exceptions import RayActorError, RayTaskError, TaskCancelledError


@ray.remote(num_returns="streaming", max_retries=1)
def finite_stream(marker, count, extra=None):
    # Shared filesystem is only a local-cluster observation, not replay state.
    with Path(marker).open("a") as output:
        output.write("started\n")
    for index in range(count):
        yield index


@ray.remote(num_cpus=0)
class Owner:
    def __init__(self):
        self.stream = None

    def begin(self, consumer_address, marker, count, extra=None, options=None):
        self.stream = StreamingRecoveryOwner.submit(
            finite_stream,
            expected_returns=count,
            consumer_address=consumer_address,
            args=(marker, count, extra[0] if extra else None),
            resources={"stream_executor": 0.01},
            _generator_backpressure_num_objects=1,
            **(options or {}),
        )
        return self.stream.submission()

    def offer(self):
        return self.stream.submission()

    def confirm(self, descriptor, consumer_address):
        self.stream.confirm_receipt(descriptor, consumer_address)

    def pull(self):
        try:
            return {"ref": self.stream.next_ref()}
        except StopIteration:
            # EOF is accepted only after completion has resolved successfully.
            ray.get(self.stream._generator.completed())
            return {"eof": True}

    def cancel(self):
        completion = self.stream._generator.completed()
        self.stream.close()
        return [completion]

    def close(self):
        if self.stream is not None:
            self.stream.close()

    def exit_while_held(self):
        ray.actor.exit_actor()

    def pid(self):
        return os.getpid()


@pytest.fixture(scope="module")
def recovery_cluster():
    cluster = Cluster()
    try:
        cluster.add_node(
            num_cpus=1,
            _system_config={
                "enable_recovery_succession": True,
                "enable_recovery_witness_holder_baseline": True,
                "enable_recovery_streaming_fixed_r": True,
                "recovery_succession_target_holder_count": 2,
                "recovery_succession_witness_count": 2,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
            },
        )
        cluster.add_node(num_cpus=1, resources={"stream_owner": 1})
        cluster.add_node(num_cpus=1, resources={"stream_executor": 1})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.fixture
def owner(recovery_cluster):
    actor = Owner.options(resources={"stream_owner": 0.01}).remote()
    try:
        yield actor
    finally:
        try:
            ray.get(actor.close.remote(), timeout=10)
        except RayActorError:
            pass
        finally:
            ray.kill(actor)


@pytest.mark.parametrize("count", [0, 3])
def test_receipt_releases_original_dispatch(owner, tmp_path, count):
    address = streaming_recovery_address()
    marker = tmp_path / "producer"
    descriptor, ready = ray.get(owner.begin.remote(address, str(marker), count))
    consumer = StreamingRecoveryConsumer(descriptor, address)
    try:
        assert not ready
        assert not marker.exists()
        with pytest.raises(RayTaskError, match="enrollment is not ready"):
            ray.get(owner.pull.remote(), timeout=10)
        assert not ray.get(owner.offer.remote())[1]

        parsed = RecoveryStreamDescriptor.FromString(descriptor)
        owner_node = parsed.manifest.succession[0].address.node_id
        holder_nodes = {w.node_id for w in parsed.manifest.witness_raylets}
        assert len(holder_nodes) == 2
        assert owner_node not in holder_nodes
        assert Address.FromString(address).node_id != owner_node

        ray.get(owner.confirm.remote(descriptor, address))
        wait_for_condition(lambda: ray.get(owner.offer.remote())[1], timeout=30)
        # Duplicate receipt must not schedule a second producer.
        ray.get(owner.confirm.remote(descriptor, address))
        consumer.mark_ready(descriptor)
        for index in range(count):
            ticket = consumer.begin_owner_read()
            message = ray.get(owner.pull.remote(), timeout=30)
            ref = consumer.accept_owner_item(ticket, message["ref"])
            assert ray.get(ref, timeout=30) == index
            del ref, message
            consumer.release(index)
        ticket = consumer.begin_owner_read()
        assert ray.get(owner.pull.remote(), timeout=30) == {"eof": True}
        consumer.accept_owner_eof(ticket)
        assert consumer.next_index == count
        assert consumer.phase == "completed"
        assert marker.read_text() == "started\n"
        # Retaining the owner handle keeps the immutable offer after EOF.
        assert ray.get(owner.offer.remote()) == (descriptor, True)
    finally:
        ray.get(owner.close.remote(), timeout=10)
        consumer.close()


def test_bad_receipt_does_not_release_or_poison_offer(owner, tmp_path):
    address = streaming_recovery_address()
    marker = tmp_path / "producer"
    descriptor, _ = ray.get(owner.begin.remote(address, str(marker), 0))
    changed = RecoveryStreamDescriptor.FromString(descriptor)
    changed.expected_returns = 1
    with pytest.raises(RayTaskError, match="receipt does not match"):
        ray.get(owner.confirm.remote(changed.SerializeToString(), address))
    wrong_consumer = Address.FromString(address)
    wrong_consumer.port += 1
    with pytest.raises(RayTaskError, match="receipt does not match"):
        ray.get(owner.confirm.remote(descriptor, wrong_consumer.SerializeToString()))
    assert ray.get(owner.offer.remote()) == (descriptor, False)
    assert not marker.exists()
    consumer = StreamingRecoveryConsumer(descriptor, address)
    try:
        ray.get(owner.confirm.remote(descriptor, address))
        wait_for_condition(lambda: ray.get(owner.offer.remote())[1], timeout=30)
        consumer.mark_ready(descriptor)
        ticket = consumer.begin_owner_read()
        assert ray.get(owner.pull.remote(), timeout=30) == {"eof": True}
        consumer.accept_owner_eof(ticket)
    finally:
        ray.get(owner.close.remote(), timeout=10)
        consumer.close()


def test_cancel_before_receipt_settles_pending_completion(owner, tmp_path):
    address = streaming_recovery_address()
    marker = tmp_path / "producer"
    descriptor, ready = ray.get(owner.begin.remote(address, str(marker), 1))
    assert not ready
    completion = ray.get(owner.cancel.remote(), timeout=10)[0]
    with pytest.raises(TaskCancelledError):
        ray.get(completion, timeout=30)
    with pytest.raises(RayTaskError, match="owner handle is closed"):
        ray.get(owner.confirm.remote(descriptor, address))
    assert not marker.exists()


def test_owner_exit_does_not_wait_forever_for_receipt(owner, tmp_path):
    marker = tmp_path / "producer"
    ray.get(owner.begin.remote(streaming_recovery_address(), str(marker), 1))
    pid = ray.get(owner.pid.remote())
    with pytest.raises(RayActorError):
        ray.get(owner.exit_while_held.remote(), timeout=30)
    wait_for_condition(lambda: not psutil.pid_exists(pid), timeout=30)
    assert not marker.exists()


def test_cancel_after_dispatch_uses_normal_submitter(owner, tmp_path):
    address = streaming_recovery_address()
    marker = tmp_path / "producer"
    descriptor, _ = ray.get(owner.begin.remote(address, str(marker), 3))
    consumer = StreamingRecoveryConsumer(descriptor, address)
    try:
        ray.get(owner.confirm.remote(descriptor, address))
        wait_for_condition(marker.exists, timeout=30)
        # The producer has started and is blocked by backpressure, so this
        # cancellation must go through the normal submitter rather than only
        # settling a held TaskManager entry.
        completion = ray.get(owner.cancel.remote(), timeout=10)[0]
        with pytest.raises(TaskCancelledError):
            ray.get(completion, timeout=30)
    finally:
        consumer.close()


@pytest.mark.parametrize(
    "invalid", ["no_retry", "by_ref", "nested_ref", "not_stream", "malformed_address"]
)
def test_invalid_recipe_fails_submission_without_killing_worker(owner, tmp_path, invalid):
    marker = tmp_path / "producer"
    address = streaming_recovery_address()
    extra = None
    options = {}
    if invalid == "no_retry":
        options["max_retries"] = 0
    elif invalid == "not_stream":
        options["num_returns"] = 1
    elif invalid == "malformed_address":
        address += b"\x80"
    else:
        ref = ray.put(42)
        # Nest once for the actor call so the owner receives an ObjectRef.
        extra = [ref] if invalid == "by_ref" else [{"nested": ref}]
    with pytest.raises(RayTaskError):
        ray.get(owner.begin.remote(address, str(marker), 1, extra, options), timeout=30)
    assert not marker.exists()
    # The same worker can enroll another stream after synchronous rejection.
    descriptor, ready = ray.get(
        owner.begin.remote(streaming_recovery_address(), str(marker), 0), timeout=30
    )
    assert descriptor and not ready


@pytest.mark.parametrize("count", [-1, True, 2**63, 1.5])
def test_option_rejects_invalid_count(count):
    with pytest.raises(ValueError, match="nonnegative int64"):
        task_options["_streaming_recovery"].validate(
            "_streaming_recovery", {"expected_returns": count, "consumer_address": b"x"}
        )


@pytest.mark.parametrize(
    "value",
    [{}, {"expected_returns": 1}, {"expected_returns": 1, "consumer_address": "text"}],
)
def test_option_requires_complete_offer_parameters(value):
    with pytest.raises(ValueError):
        task_options["_streaming_recovery"].validate("_streaming_recovery", value)
