"""Local fault injection and evidence for the active XGBoost collective probe."""

from concurrent.futures import Future
import json
import threading
import time

import ray
import xgboost as xgb

from ray.experimental.recovery._xgboost_active import validate_cleanup
from ray.experimental.recovery._xgboost_boundary import tree_digest


def interrupt_collective(actors, identities, checkpoint, start, generation,
                         directory, crash, policy, timeout_s, evidence):
    directory.mkdir()
    deadline = time.monotonic() + timeout_s

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("Active collective probe exceeded its phase deadline")
        return left

    tracker = xgb.RabitTracker(n_workers=2, host_ip=ray.util.get_node_ip_address(),
                              sortby="task", timeout=10)
    tracker.start()
    finished = Future()

    def wait_tracker():
        try:
            tracker.wait_for(timeout=30)
            finished.set_result(None)
        except BaseException as exc:
            finished.set_exception(exc)

    thread = threading.Thread(target=wait_tracker, daemon=True)
    thread.start()
    try:
        args = tracker.worker_args()
        refs = [a.interrupt_segment.remote(args, checkpoint, start, generation, str(directory))
                for a in actors]
        paths = [directory / "rank-0-gated.json", directory / "rank-1-allreduce-enter.json"]
        while not all(p.exists() for p in paths):
            ready, _ = ray.wait(refs, num_returns=1, timeout=min(.05, remaining()))
            if ready:
                ray.get(ready[0])  # Preserve the actual startup/worker error.
                raise ValueError("Worker finished before the active fault gate")
        gates = [json.loads(p.read_text()) for p in paths]
        evidence["gates"] = gates
        for rank, gate in enumerate(gates):
            if (gate["rank"] != rank or gate["round"] != start + 1
                    or gate["generation"] != generation or gate["identity"] != identities[rank]):
                raise ValueError("Fault gate does not match this worker generation")
        # The healthy allreduce must remain pending while the peer is gated.
        ready, _ = ray.wait(refs, num_returns=1, timeout=min(.2, remaining()))
        if ready or (directory / "rank-1-allreduce-error.json").exists():
            raise ValueError("Collective failed or completed before node injection")
        evidence.update(request_ns=time.monotonic_ns(), checkpoint_round=start,
                        interrupted_round=start + 1, pending_before_fault=True)
        failure = crash(identities[0]["node_id"], identities[0]["pid"])
        evidence["worker_node_failure"] = failure
        if policy == "full":
            ray.kill(actors[1], no_restart=True)
        try:
            ray.get(refs[0], timeout=remaining())
        except ray.exceptions.RayActorError as exc:
            evidence["failed_worker_error"] = str(exc)
        else:
            raise ValueError("Injected node loss did not fail its worker RPC")
        if policy == "selective":
            result = ray.get(refs[1], timeout=remaining())
            model = xgb.Booster(model_file=bytearray(checkpoint))
            validate_cleanup(result, identities[1], start, tree_digest(model), generation)
            evidence["healthy_cleanup"] = result
            error_event = json.loads((directory / "rank-1-allreduce-error.json").read_text())
            if error_event["time_ns"] < evidence["request_ns"]:
                raise ValueError("Healthy allreduce failed before the injected fault")
            evidence["failure_to_healthy_cleanup_s"] = (
                result["cleanup_finished_ns"] - evidence["request_ns"]) / 1e9
        return evidence["request_ns"], failure
    finally:
        # A dead rank cannot send tracker shutdown. Stop this generation's
        # tracker explicitly, after survivor cleanup, before creating another.
        try:
            tracker.free()
        except xgb.core.XGBoostError as exc:
            evidence["tracker_cleanup_error"] = str(exc)
        thread.join(timeout=2)
        evidence["tracker_thread_stopped"] = not thread.is_alive()
        if thread.is_alive():
            raise TimeoutError("Interrupted collective left its tracker thread running")
        if finished.done() and finished.exception() is not None:
            evidence["tracker_wait_error"] = str(finished.exception())
        evidence["worker_events"] = [json.loads(p.read_text()) for p in sorted(directory.glob("*.json"))]
