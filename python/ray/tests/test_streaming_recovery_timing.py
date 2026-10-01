"""Timing must preserve errors, poll credit and disabled-path behavior."""

from types import SimpleNamespace

import pytest

from ray._private import streaming_recovery as recovery


def test_timing_records_failed_call_without_swallowing_error(monkeypatch):
    ticks = iter((1.0, 3.0, 4.0, 7.0))
    monkeypatch.setattr(recovery.time, "perf_counter", lambda: next(ticks))
    stats = {"fixed_r_timing_enabled": True}
    error = RuntimeError("original failure")
    with pytest.raises(RuntimeError) as raised:
        with recovery._record_duration(stats, "close"):
            raise error
    assert raised.value is error
    with recovery._record_duration(stats, "close"):
        pass
    assert stats["fixed_r_timing_close_s"] == 5
    assert stats["fixed_r_timing_close_count"] == 2
    assert stats["fixed_r_timing_close_max_s"] == 3


def test_disabled_timing_does_not_read_clock_or_add_metrics(monkeypatch):
    def unexpected_clock():
        raise AssertionError("disabled timing read the clock")

    monkeypatch.setattr(recovery.time, "perf_counter", unexpected_clock)
    stats = {}
    for value in (stats, None):
        with recovery._record_duration(value, "copy"):
            pass
    assert stats == {}


def test_pending_polls_keep_one_rpc_and_one_latency_sample(monkeypatch):
    requests = []
    waitable, output = object(), object()
    consumer = SimpleNamespace(
        phase="forwarding", begin_owner_read=lambda: "ticket",
        accept_owner_item=lambda ticket, ref: ref,
    )
    monkeypatch.setattr(recovery, "StreamingRecoveryConsumer", lambda *args: consumer)
    owner = SimpleNamespace(pull=SimpleNamespace(
        remote=lambda timeout: requests.append(timeout) or waitable))
    reader = recovery.StreamingRecoveryReader(owner, b"descriptor", b"address")
    reader._timing_stats = {"fixed_r_timing_enabled": True}
    ticks = iter((10.0, 13.0, 13.1, 13.2))
    monkeypatch.setattr(recovery.time, "perf_counter", lambda: next(ticks))
    waits = iter((([], [waitable]), ([waitable], [])))
    monkeypatch.setattr(recovery.ray, "wait", lambda *a, **kw: next(waits))
    monkeypatch.setattr(recovery.ray, "get", lambda *a, **kw: {"ref": output})
    assert reader.poll_next() is None
    assert reader.get_waitable() is waitable
    assert "fixed_r_timing_owner_pull_result_count" not in reader._timing_stats
    # The accept call has its own nested timer after the pull settles.
    assert reader.poll_next() is output
    assert len(requests) == 1
    assert reader._timing_stats["fixed_r_timing_owner_pull_result_count"] == 1
    assert reader._timing_stats["fixed_r_timing_owner_pull_result_s"] == 3
    assert reader._pending_read is None
    assert reader._pending_read_started is None
