"""Runs on the surviving head; only the external notebook host controls GCP."""

import argparse
import functools
import hashlib
import json
import traceback
from pathlib import Path

from gcp.backend import RayWorkers, VMFaultInjector
from replay_runtime import replay_once
from run_study import enumerate_study, profile
from workload import write_json


def compact_checkpoints(folder, keep):
    """After validation, retain final/restored states and each attempt's latest.

    This applies only to newly generated epoch checkpoints in this observation.
    The manifest records hashes of removed intermediate states.
    """
    folder = Path(folder)
    retained = {str(Path(p).resolve()) for p in keep if p}
    files = list(folder.glob("*/epoch-*.pt"))
    for parent in {p.parent for p in files}:
        latest = max(parent.glob("epoch-*.pt"), key=lambda p: int(p.stem.split("-")[1]))
        retained.add(str(latest.resolve()))
    removed = {}
    for path in files:
        if str(path.resolve()) not in retained:
            removed[str(path.relative_to(folder))] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    write_json(
        folder / "checkpoint-retention.json",
        dict(retained=sorted(retained), removed_sha256=removed),
    )
    for relative in removed:
        (folder / relative).unlink()


def main(request):
    import ray

    folder = Path(request["folder"])
    ray.init(
        address="auto",
        runtime_env={
            "working_dir": str(Path(__file__).resolve().parents[1]),
            "env_vars": {
                "OMP_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
            },
        },
    )
    try:
        if request["action"] == "profile":
            measured = profile(request["config"], folder, workers_factory=RayWorkers)
            enumerate_study(folder)
            if not request["config"].get("keep_all_checkpoints", True):
                compact_checkpoints(folder / "profile", measured["references"].values())
        else:
            measured = json.loads((folder / "profile.json").read_text())
            row = replay_once(
                measured,
                request["case"],
                request["plan"],
                request["scenario"],
                request["output"],
                workers_factory=RayWorkers,
                injector_factory=functools.partial(
                    VMFaultInjector,
                    folder=request["control"],
                    failure_mode=request.get("failure_mode", "vm"),
                ),
                kind="measured_gcp_worker_"
                + request.get("failure_mode", "vm")
                + "_loss",
            )
            # A stopped VM is not restored until after validation and driver exit.
            write_json(Path(request["control"]) / "result.json", row)
            if row["correct"] and not measured["config"].get(
                "keep_all_checkpoints", True
            ):
                events = json.loads(
                    (Path(request["output"]) / "events.json").read_text()
                )
                keep = [e.get("path") for e in events if e["kind"] == "result"]
                keep += [e.get("checkpoint") for e in events if e["kind"] == "dispatch"]
                compact_checkpoints(request["output"], keep)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("request")
    args = parser.parse_args()
    request = json.loads(Path(args.request).read_text())
    status = Path(request["control"]) / "status.json"
    try:
        main(request)
    except Exception:
        write_json(status, dict(ok=False, traceback=traceback.format_exc()))
        raise
    else:
        write_json(status, dict(ok=True))
