"""Sequential helper reuse must retain per-task identity and retirement gates."""

from pathlib import Path
from types import SimpleNamespace
import time
from unittest.mock import Mock

import pyarrow as pa
import pytest
import ray

from ray._private import streaming_recovery as protocol
from ray._common.test_utils import wait_for_condition
from ray.cluster_utils import Cluster
from ray.data import DataContext
from ray.data._internal.execution import streaming_recovery as runtime
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.tests.test_streaming_recovery_data import submission_race


@pytest.fixture(scope="module")
def surviving_data_cluster():
    # Use bounded object stores on this one-machine test cluster.
    cluster = Cluster()
    try:
        cluster.add_node(
            num_cpus=1, include_dashboard=False, object_store_memory=256 * 1024**2,
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
        executor = cluster.add_node(num_cpus=3, object_store_memory=256 * 1024**2)
        cluster.add_node(num_cpus=3, object_store_memory=256 * 1024**2)
        cluster.wait_for_nodes()
        ray.init(address=cluster.address)
        yield cluster, executor.node_id
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.fixture
def data_cluster(surviving_data_cluster):
    cluster, executor_id = surviving_data_cluster
    node = cluster.add_node(num_cpus=0, object_store_memory=256 * 1024**2)
    cluster.wait_for_nodes()
    removed = False

    def crash(owners):
        nonlocal removed
        cluster.remove_node(node, allow_graceful=False)
        removed = True

        def stopped():
            for owner in owners:
                try:
                    ray.get(owner.__ray_ready__.remote(), timeout=1)
                except ray.exceptions.RayActorError:
                    continue
                except ray.exceptions.GetTimeoutError:
                    return False
                return False
            return any(n["NodeID"] == node.node_id and not n["Alive"] for n in ray.nodes())

        wait_for_condition(stopped, timeout=60)

    try:
        yield node.node_id, executor_id, crash
    finally:
        if not removed:
            cluster.remove_node(node, allow_graceful=False)


def test_generation_fences_stale_calls_and_never_multiplexes(monkeypatch):
    streams = []

    def submit(*args, **kwargs):
        stream = Mock()
        stream.submission.return_value = (b"descriptor", True)
        streams.append(stream)
        return stream

    monkeypatch.setattr(protocol.StreamingRecoveryOwner, "submit", submit)
    owner = protocol.ReusableStreamingRecoveryOwnerActor()
    arguments = ("producer", -1, b"address", (), {}, {})
    owner.begin(1, *arguments)
    with pytest.raises(protocol.StreamingRecoveryStateError, match="already holds"):
        owner.begin(2, *arguments)
    owner.close(1)
    owner.begin(2, *arguments)
    owner.close(1)  # Delayed duplicate close must not cancel generation 2.
    streams[0].close.assert_called_once()
    streams[1].close.assert_not_called()
    assert owner.offer(2) == (b"descriptor", True)
    for call in (lambda: owner.offer(1), lambda: owner.pull(1, 0),
                 lambda: owner.confirm(1, b"descriptor", b"address"),
                 lambda: owner.begin(1, *arguments)):
        with pytest.raises(protocol.StreamingRecoveryStateError):
            call()
    assert len(streams) == 2
    owner.close(2)
    owner.close(2)
    streams[1].close.assert_called_once()
    owner.close(3)  # Cancellation beats a delayed begin.
    with pytest.raises(protocol.StreamingRecoveryStateError):
        owner.begin(3, *arguments)
    owner.begin(4, *arguments)
    owner.close(4)


@pytest.fixture
def pool_environment(monkeypatch):
    actors = [Mock(), Mock(), Mock()]
    actor_class = Mock()
    actor_class.options.return_value.remote.side_effect = actors
    monkeypatch.setattr(runtime.ray, "remote", lambda **kw: lambda cls: actor_class)
    killed = Mock()
    monkeypatch.setattr(runtime.ray, "kill", killed)
    config = runtime.FixedRDataConfig(
        ray.NodeID.from_random().hex(), ray.NodeID.from_random().hex(), {},
        dynamic_task_outputs=True,
    )
    stats = runtime.new_metrics()
    pool = runtime.OwnerHelperPool(config, stats, max_idle=1)
    return SimpleNamespace(pool=pool, actors=actors, killed=killed, stats=stats)


def retired(lease):
    return SimpleNamespace(owner=lease, _closed=True)


def test_pool_reuses_only_retired_helpers_and_bounds_idle_processes(pool_environment):
    env = pool_environment
    first, second = env.pool.acquire(), env.pool.acquire()
    assert first.actor is not second.actor  # Concurrent tasks get separate owners.
    with pytest.raises(RuntimeError, match="retirement"):
        env.pool.recycle(first, SimpleNamespace(owner=first, _closed=False))
    env.pool.recycle(first, retired(first))
    third = env.pool.acquire()
    assert third.actor is first.actor
    assert (first.generation, third.generation) == (1, 2)
    third.pull.remote(0.1)
    third.actor.pull.remote.assert_called_once_with(2, 0.1)
    with pytest.raises(RuntimeError, match="not active"):
        env.pool.recycle(first, retired(first))
    env.pool.recycle(third, retired(third))
    env.pool.recycle(second, retired(second))  # Idle capacity is one.
    env.killed.assert_called_once_with(second.actor, no_restart=True)
    env.pool.close()
    assert env.killed.call_count == 2
    assert env.stats["fixed_r_helper_creations"] == 2
    assert env.stats["fixed_r_helper_reuses"] == 1
    assert env.stats["fixed_r_helper_kill_requests"] == 2
    with pytest.raises(RuntimeError, match="closed"):
        env.pool.acquire()


def test_failed_retirement_is_not_recycled_or_killed_by_pool_shutdown(pool_environment, monkeypatch):
    env = pool_environment
    lease = env.pool.acquire()
    reader = SimpleNamespace(owner=lease, descriptor=b"descriptor", _closed=False)
    error = RuntimeError("tombstone acknowledgement timed out")

    def close():
        if not reader._closed:
            raise error

    reader.close = close
    monkeypatch.setattr(runtime, "_inspect_recovery_stream_descriptor",
                        lambda _: {"task_id": ray.TaskID.from_random().binary()})
    stream = runtime._DataStream(-1, env.stats, reader=reader, owner=lease, owner_pool=env.pool)
    with pytest.raises(RuntimeError) as raised:
        stream.close()
    assert raised.value is error
    assert not stream.closed and not env.pool._idle
    env.pool.close()
    env.killed.assert_not_called()
    # A later successful barrier can still retire the checked-out helper.
    reader._closed = True
    stream.close()
    stream.close()
    env.killed.assert_called_once_with(lease.actor, no_restart=True)
    assert env.stats["fixed_r_closed_streams"] == 1


@pytest.mark.parametrize("point", ["startup", "begin"])
def test_failed_pooled_submission_discards_helper_without_resubmitting(
    monkeypatch, submission_race, point,
):
    env = submission_race
    pool = runtime.OwnerHelperPool(env.config, env.stats)
    error = ray.exceptions.RayActorError()
    if point == "startup":
        env.ray.get.side_effect = error
        monkeypatch.setattr(runtime, "_owner_alive", lambda config: True)
        monkeypatch.setattr(runtime, "time", SimpleNamespace(
            monotonic=Mock(side_effect=[0, 2]), sleep=Mock()))
    else:
        env.ray.get.side_effect = [None, ray.exceptions.RayActorError()]
        env.reader_submit.side_effect = error
        monkeypatch.setattr(runtime, "_owner_alive", lambda config: True)
    with pytest.raises(ray.exceptions.RayActorError) as raised:
        runtime.submit_stream(env.config, env.producer, (), {}, {}, 1, env.stats,
                              owner_pool=pool)
    assert raised.value is error
    env.producer.options.assert_not_called()
    assert not pool._active and not pool._idle
    env.ray.kill.assert_called_once_with(env.owner, no_restart=True)


@pytest.mark.parametrize("reuse", [False, True])
def test_dynamic_map_operator_reuses_and_cleans_up_helpers(data_cluster, reuse):
    owner_id, executor_id, _ = data_cluster
    context = DataContext.get_current().copy()
    context.eager_free = False
    context.enable_progress_bars = False
    context.execution_options.preserve_order = True
    context.set_config(runtime.CONFIG_KEY, runtime.FixedRDataConfig(
        owner_id, executor_id, {}, dynamic_task_outputs=True, reuse_owner_helpers=reuse,
    ))

    def increment(batch):
        return pa.table({"value": [v + 1 for v in batch["value"].to_pylist()]})

    with DataContext.current(context):
        ds = ray.data.from_blocks([pa.table({"value": [i]}) for i in range(8)])
        ds = ds.map_batches(increment, batch_size=1, batch_format="pyarrow", concurrency=1)
        iterator, _, executor = ds._execute_to_iterator()
        try:
            values = [v for bundle in iterator for ref in bundle.block_refs
                      for v in ray.get(ref)["value"].to_pylist()]
        finally:
            executor.shutdown(force=False)
    assert values == list(range(1, 9))
    metrics = [op.metrics.extra_metrics for op in executor._topology if isinstance(op, MapOperator)]
    assert sum(m["fixed_r_enrolled_tasks"] for m in metrics) == 8
    reuses = sum(m["fixed_r_helper_reuses"] for m in metrics)
    if reuse:
        assert reuses > 0
    else:
        assert reuses == 0
    assert sum(m["fixed_r_closed_streams"] for m in metrics) == 8
    assert all(m["fixed_r_helper_creations"] == m["fixed_r_helper_kill_requests"] for m in metrics)


@ray.remote(num_returns="streaming", max_retries=1)
def reuse_values(marker, gate):
    for index in range(3):
        if gate and index == 1:
            while not Path(gate).exists():
                time.sleep(0.01)
        yield bytes([marker, index]) * 100_000


def next_stream_ref(stream):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            ref = stream.poll()
        except protocol.StreamingRecoveryRequired:
            stream.recover()
            continue
        if ref is not None:
            return ref
        time.sleep(0.01)
    raise TimeoutError("Reused stream made no progress")


def test_owner_loss_after_reuse_preserves_new_task_and_retained_output(data_cluster, tmp_path):
    owner_id, executor_id, crash = data_cluster
    config = runtime.FixedRDataConfig(owner_id, executor_id, {}, dynamic_task_outputs=True)
    stats = runtime.new_metrics()
    pool = runtime.OwnerHelperPool(config, stats)
    streams = []
    gate = tmp_path / "release"

    def submit(marker, gate_path):
        stream = runtime.submit_stream(
            config, reuse_values, (marker, gate_path), {},
            {"_generator_backpressure_num_objects": 1}, 0, stats, owner_pool=pool,
        )
        streams.append(stream)
        return stream

    try:
        first = submit(7, None)
        for index in range(3):
            assert ray.get(next_stream_ref(first)) == bytes([7, index]) * 100_000
        with pytest.raises(StopIteration):
            next_stream_ref(first)
        first.close()
        second = submit(9, str(gate))
        assert first.owner.actor._actor_id == second.owner.actor._actor_id
        assert first.task_id != second.task_id
        assert second.owner.generation == first.owner.generation + 1
        # Even an explicitly delayed RPC from the old lease must be harmless.
        ray.get(first.owner.close.remote(), timeout=10)
        assert ray.get(second.owner.offer.remote(), timeout=10)[0] == second.reader.descriptor
        retained = next_stream_ref(second)
        original_id = retained.binary()
        assert ray.get(retained) == bytes([9, 0]) * 100_000
        second.waitable()  # Exercise an outstanding owner read at failure.
        crash([second.owner])
        gate.touch()
        for index in (1, 2):
            assert ray.get(next_stream_ref(second)) == bytes([9, index]) * 100_000
        with pytest.raises(StopIteration):
            next_stream_ref(second)
        assert retained.binary() == original_id
        assert ray.get(retained) == bytes([9, 0]) * 100_000
        assert stats["fixed_r_recovered_tasks"] == 1
        assert stats["fixed_r_helper_reuses"] == 1
        assert stats["fixed_r_helper_creations"] == 1
        second.close()
        with pytest.raises(protocol.StreamingRecoveryStateError):
            first.reader.recover()  # Retired task must never become recoverable again.
    finally:
        gate.touch()
        try:
            for stream in streams:
                stream.close()
        finally:
            pool.close()
