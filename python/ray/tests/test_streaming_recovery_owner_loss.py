"""Real local-raylet owner-node crashes through the private streaming reader."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pickle
import time

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.streaming_recovery import (
    StreamingRecoveryOwnerActor,
    StreamingRecoveryReader,
    StreamingRecoveryRequired,
    StreamingRecoveryStateError,
)
from ray.cluster_utils import Cluster
from ray.core.generated.common_pb2 import RecoveryStreamDescriptor
from ray.exceptions import GetTimeoutError, OwnerDiedError, RayActorError, RaySystemError


@ray.remote(num_returns="streaming", max_retries=1)
def producer(count, size, attempts_path, gate=None):
    worker = ray._private.worker.global_worker.core_worker
    attempt = worker.get_current_task_attempt_number()
    # Local test observations/control affect timing only; output bytes depend
    # exclusively on the by-value count/size arguments.
    with Path(attempts_path).open("a") as log:
        log.write(f"{attempt}\n")
    for index in range(count):
        if gate and index == 1:
            Path(gate + ".blocked").touch()
            while not Path(gate).exists():
                time.sleep(0.01)
        yield bytes([index]) * size


@pytest.fixture(scope="module")
def surviving_cluster():
    cluster = Cluster()
    try:
        cluster.add_node(
            num_cpus=1,
            include_dashboard=False,
            _system_config={
                "enable_recovery_succession": True,
                "enable_recovery_witness_holder_baseline": True,
                "enable_recovery_streaming_fixed_r": True,
                "recovery_succession_target_holder_count": 2,
                "recovery_succession_witness_count": 2,
                "recovery_frontier_group_size": 1,
                "recovery_baseline_perf_protect_every_n": 1,
                "health_check_initial_delay_ms": 1000,
                "health_check_period_ms": 1000,
                "health_check_timeout_ms": 3000,
                "health_check_failure_threshold": 3,
            },
        )
        cluster.add_node(num_cpus=1, resources={"replay_executor": 1})
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.fixture
def stream(surviving_cluster, tmp_path):
    cluster = surviving_cluster
    node = cluster.add_node(num_cpus=1, resources={"protected_stream_owner": 1})
    cluster.wait_for_nodes()
    owner = ray.remote(num_cpus=0)(StreamingRecoveryOwnerActor).options(
        resources={"protected_stream_owner": 0.01}
    ).remote()
    readers = []
    crashed = False

    def start(count=3, size=200_000, gate=None, task=producer, task_args=None):
        reader = StreamingRecoveryReader.submit(
            owner, task, expected_returns=count,
            args=(count, size, str(tmp_path / "attempts"), gate)
            if task_args is None else task_args,
            resources={"replay_executor": 0.01},
            _generator_backpressure_num_objects=1,
            timeout_s=60,
        )
        readers.append(reader)
        return reader

    def crash():
        nonlocal crashed
        cluster.remove_node(node, allow_graceful=False)
        crashed = True
        # Removing the raylet does not synchronously stop its actor workers.
        # A surviving owner can otherwise drain the short stream before its
        # parent-death check runs, so no recovery is exercised at c=0 or c=1.
        def owner_stopped():
            try:
                ray.get(owner.__ray_ready__.remote(), timeout=1)
            except RayActorError:
                return True
            except GetTimeoutError:
                return False
            return False

        wait_for_condition(owner_stopped, timeout=60, raise_exceptions=True)

    try:
        yield start, crash, tmp_path
    finally:
        try:
            for reader in readers:
                reader.close()
        finally:
            if not crashed:
                cluster.remove_node(node, allow_graceful=False)


@pytest.mark.parametrize("consumed", [0, 1, 3])
@pytest.mark.parametrize("size", [16, 200_000])
def test_replay_preserves_ids_cursor_and_retained_outputs(stream, consumed, size):
    start, crash, directory = stream
    reader = start(size=size)
    retained = [next(reader) for _ in range(consumed)]
    original_ids = [ref.binary() for ref in retained]
    if consumed == 3:
        with pytest.raises(StopIteration):
            next(reader)
    wait_for_condition(
        lambda: (directory / "attempts").exists()
        and (directory / "attempts").read_text().splitlines() == ["0"],
        timeout=30,
    )
    crash()
    if consumed == 3:
        # There is no further owner read to fail after original EOF. Explicitly
        # recover retained refs, then verify c=N produces only replay EOF.
        reader.recover()
    remaining = list(reader)
    assert len(remaining) == 3 - consumed
    assert [ref.binary() for ref in retained] == original_ids
    all_refs = retained + remaining
    assert len({ref.binary() for ref in all_refs}) == 3
    assert ray.get(all_refs, timeout=60) == [bytes([i]) * size for i in range(3)]
    assert reader.consumer.next_index == 3
    assert (directory / "attempts").read_text().splitlines() == ["0", "1"]
    with pytest.raises(StreamingRecoveryStateError, match="no previous recovery"):
        reader.recover()
    del all_refs, retained, remaining
    for index in range(3):
        reader.release(index)
    reader.close()


def test_recovery_settles_an_outstanding_owner_read(stream):
    start, crash, directory = stream
    gate = directory / "continue"
    reader = start(gate=str(gate))
    first = next(reader)
    wait_for_condition(lambda: Path(str(gate) + ".blocked").exists(), timeout=30)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(next, reader)
        try:
            wait_for_condition(lambda: reader.consumer._owner_read is not None, timeout=10)
            crash()
            wait_for_condition(lambda: reader.consumer.phase == "replaying", timeout=60)
        finally:
            gate.touch()
        second = pending.result(timeout=60)
    rest = list(reader)
    assert ray.get([first, second] + rest, timeout=60) == [
        bytes([i]) * 200_000 for i in range(3)
    ]
    assert (directory / "attempts").read_text().splitlines() == ["0", "1"]


def test_empty_stream_recovery_after_original_eof(stream):
    start, crash, directory = stream
    reader = start(count=0)
    assert list(reader) == []
    crash()
    reader.recover()
    assert list(reader) == []
    assert reader.consumer.next_index == 0
    assert (directory / "attempts").read_text().splitlines() == ["0", "1"]


def test_recovery_replaces_observed_owner_died_value(stream):
    start, crash, _ = stream
    reader = start()
    retained = next(reader)
    original_id = retained.binary()
    crash()
    owner_node = RecoveryStreamDescriptor.FromString(
        reader.descriptor
    ).manifest.succession[0].address.node_id.hex()
    wait_for_condition(
        lambda: any(n["NodeID"] == owner_node and not n["Alive"] for n in ray.nodes()),
        timeout=60,
    )
    with pytest.raises(OwnerDiedError):
        ray.get(retained, timeout=60)
    reader.recover()
    remaining = list(reader)
    assert retained.binary() == original_id
    assert ray.get([retained] + remaining, timeout=60) == [
        bytes([i]) * 200_000 for i in range(3)
    ]


def test_live_owner_and_foreign_consumer_cannot_claim(stream):
    start, _, directory = stream
    reader = start(count=1)
    worker = ray._private.worker.global_worker.core_worker
    with pytest.raises(GetTimeoutError, match="owner node is not known dead"):
        worker.recover_streaming_task(reader.descriptor, 0, [], 100)
    changed = RecoveryStreamDescriptor.FromString(reader.descriptor)
    changed.consumer_address.port += 1
    with pytest.raises(RaySystemError, match="designated consumer"):
        worker.recover_streaming_task(changed.SerializeToString(), 0, [], 100)
    ref = next(reader)
    assert ray.get(ref, timeout=30) == bytes([0]) * 200_000
    assert list(reader) == []
    assert (directory / "attempts").read_text().splitlines() == ["0"]


@ray.remote(num_returns="streaming", max_retries=1)
def data_map_stream(
    transformer, data_context, task_context, block, attempts_path, metadata_gate=None
):
    from ray.data._internal.execution.operators.map_operator import _map_task

    attempt = ray._private.worker.global_worker.core_worker.get_current_task_attempt_number()
    with Path(attempts_path).open("a") as log:
        log.write(f"{attempt}\n")
    mapped = _map_task(transformer, data_context, task_context, block)
    if metadata_gate is None:
        yield from mapped
        return
    # Delay metadata on attempt 0 while preserving Ray's serialization-stat
    # feedback into _map_task. Never collect the stream to determine its size.
    feedback = None
    index = 0
    try:
        while True:
            try:
                value = mapped.send(feedback)
            except StopIteration:
                return
            if attempt == 0 and index == 1:
                Path(metadata_gate + ".blocked").touch()
                while not Path(metadata_gate).exists():
                    time.sleep(0.01)
            feedback = yield value
            index += 1
    finally:
        mapped.close()


@pytest.mark.parametrize("consumed", [0, 1, 2])
def test_ray_data_map_task_with_retained_input_refs(stream, consumed):
    import pyarrow as pa
    from ray.data._internal.execution.interfaces.task_context import TaskContext
    from ray.data._internal.execution.operators.map_transformer import (
        BlockMapTransformFn,
        MapTransformer,
    )
    from ray.data.context import DataContext

    # Serialize the transform by value: workers cannot import the driver's
    # pytest module by its unqualified test_streaming_recovery_owner_loss name.
    def identity_blocks(blocks, ctx):
        yield from blocks

    start, crash, directory = stream
    block = pa.table({"value": list(range(50_000))})
    transformer = MapTransformer([
        BlockMapTransformFn(identity_blocks, disable_block_shaping=True)
    ])
    inputs = (
        ray.put(transformer), ray.put(DataContext.get_current()),
        TaskContext(task_idx=0, op_name="FixedRDataMap"), ray.put(block),
        str(directory / "attempts"),
    )
    reader = start(count=2, task=data_map_stream, task_args=inputs)
    del inputs  # The reader, rather than this test variable, retains the inputs.
    retained = [next(reader) for _ in range(consumed)]
    ids = [ref.binary() for ref in retained]
    if consumed == 2:
        with pytest.raises(StopIteration):
            next(reader)
    wait_for_condition(
        lambda: (directory / "attempts").exists()
        and (directory / "attempts").read_text().splitlines() == ["0"],
        timeout=30,
    )
    crash()
    if consumed == 2:
        reader.recover()
    outputs = retained + list(reader)
    assert [ref.binary() for ref in retained] == ids
    assert len(outputs) == 2
    recovered_block, serialized_metadata = ray.get(outputs, timeout=60)
    assert recovered_block.equals(block)
    metadata = pickle.loads(serialized_metadata)
    assert metadata.num_rows == block.num_rows
    assert metadata.schema.equals(block.schema)
    assert (directory / "attempts").read_text().splitlines() == ["0", "1"]


@pytest.mark.parametrize("action", ["resume", "owner_loss", "close"])
def test_poll_ray_data_metadata_gap(stream, action):
    import pyarrow as pa
    from ray.data._internal.execution.interfaces.task_context import TaskContext
    from ray.data._internal.execution.operators.map_transformer import (
        BlockMapTransformFn,
        MapTransformer,
    )
    from ray.data.context import DataContext

    def identity_blocks(blocks, ctx):
        yield from blocks

    start, crash, directory = stream
    gate = directory / "metadata"
    block = pa.table({"value": list(range(50_000))})
    inputs = (
        ray.put(MapTransformer([
            BlockMapTransformFn(identity_blocks, disable_block_shaping=True)
        ])),
        ray.put(DataContext.get_current()),
        TaskContext(task_idx=0, op_name="FixedRDataPoll"), ray.put(block),
        str(directory / "attempts"), str(gate),
    )
    reader = start(count=2, task=data_map_stream, task_args=inputs)
    del inputs
    try:
        block_ref = next(reader)
        block_id = block_ref.binary()
        wait_for_condition(lambda: Path(str(gate) + ".blocked").exists(), timeout=30)
        for _ in range(3):
            pending = reader.get_waitable()
            assert reader.get_waitable() == pending
            assert reader.poll_next(timeout_s=0) is None
            assert reader.consumer.next_index == 1

        if action == "close":
            # close must run behind the outstanding actor read without opening
            # the producer gate. A blocking owner.pull would prevent this.
            reader.close()
            assert not gate.exists()
            assert reader.consumer.phase == "closed"
            with pytest.raises(StreamingRecoveryStateError, match="closed"):
                reader.poll_next()
            return

        if action == "owner_loss":
            crash()

            def recovery_required():
                try:
                    assert reader.poll_next() is None
                except StreamingRecoveryRequired:
                    return True
                return False

            wait_for_condition(recovery_required, timeout=60)
            assert reader.consumer.phase == "forwarding"
            assert not reader.consumer._recovery_started
            assert reader.consumer.next_index == 1
            assert (directory / "attempts").read_text().splitlines() == ["0"]
            # Only this consumer holds block_ref; no downstream task or get is
            # using it during the explicit ownership transition.
            gate.touch()
            reader.recover()
        else:
            gate.touch()

        metadata_refs = []

        def metadata_ready():
            ref = reader.poll_next()
            if ref is None:
                return False
            metadata_refs.append(ref)
            return True

        wait_for_condition(metadata_ready, timeout=60)

        def eof_ready():
            try:
                assert reader.poll_next() is None
            except StopIteration:
                return True
            return False

        wait_for_condition(eof_ready, timeout=60)
        assert reader.consumer.next_index == 2
        assert block_ref.binary() == block_id
        recovered, serialized_metadata = ray.get([block_ref, metadata_refs[0]], timeout=60)
        assert recovered.equals(block)
        metadata = pickle.loads(serialized_metadata)
        assert metadata.num_rows == block.num_rows
        assert metadata.schema.equals(block.schema)
        expected_attempts = ["0", "1"] if action == "owner_loss" else ["0"]
        assert (directory / "attempts").read_text().splitlines() == expected_attempts
    finally:
        gate.touch()


def test_rejects_nested_payload_in_consumer_owned_input(stream):
    start, _, _ = stream
    nested = ray.put([ray.put(42)])
    with pytest.raises(RaySystemError, match="consumer-owned objects without refs"):
        start(count=0, task_args=(0, 16, "unused", nested))


def test_acknowledged_close_prevents_replay(stream):
    start, crash, directory = stream
    reader = start(count=1)
    wait_for_condition(
        lambda: (directory / "attempts").exists()
        and (directory / "attempts").read_text().splitlines() == ["0"],
        timeout=30,
    )
    descriptor = reader.descriptor
    reader.close()
    crash()
    worker = ray._private.worker.global_worker.core_worker
    with pytest.raises(RaySystemError, match="claim is terminal"):
        worker.recover_streaming_task(descriptor, 0, [], 60_000)
    assert (directory / "attempts").read_text().splitlines() == ["0"]
