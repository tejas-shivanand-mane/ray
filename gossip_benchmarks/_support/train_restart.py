"""Application restart baseline on the surviving cluster; recovery stays in Ray."""

import argparse
import copy
import os
import time
import traceback

from train_workload import run_case as run_workload, write_record


def verify_owner_loss(sample):
    from run_fashion_owner_comparison import verify_progress

    verify_progress(sample)
    fault, owner = sample["data_owner_fault"], sample["shuffle_owner"]
    loss, head = sample["ordinary_owner_loss"], fault["head_replacement"]
    if (sample["mode"] != "off" or sample["status"] != "failed"
            or sample.get("timeout") or sample.get("error_type") == "TimeoutError"
            or sample.get("workload_completed") is not False or sample.get("reports")
            or not fault.get("completed") or not fault.get("submission_batch_settled")
            or fault.get("stage") != "RandomShuffle.map"
            or not head.get("original_head_processes_exited")
            or head.get("failure_scope") != "all_head_processes_with_surviving_gcs_storage"
            or fault.get("ownership") != owner or owner["task_id"] != fault["target"]["task_id"]
            or owner["owner_node_id"] != head["original_head_node_id"]
            or owner["owner_node_id"] != sample["selected_owner_node_id"]
            or owner["recorded_ns"] > fault["request_ns"]
            or loss.get("error_type") != "OwnerDiedError" or loss.get("source") != "shuffle_metadata_fetch"
            or any(loss.get(key) != owner.get(key) for key in ("object_ref_hex", "owner_worker_id", "owner_node_id"))
            or not owner.get("object_ref_hex") or loss["observed_ns"] <= fault["replacement_ready_ns"]):
        raise ValueError("Application restart requires verified, completed head-owner loss before training")


def run_case(options, directory, diagnostics):
    import ray
    from ray.experimental.recovery._local import local_head_failure_cluster

    args = argparse.Namespace(local_executor_nodes=4, local_object_store_mb=512,
                              owner_node_id=None, executor_node_ids=None,
                              producer_concurrency=None, recovery_timeout_s=30)
    attempts = diagnostics["attempts"] = []
    diagnostics["restart_policy"] = "one_same_cluster_application_restart" if options["mode"] == "off" else "fixed_r_replay"

    def save():
        write_record(directory / "restart-attempts.json", {"attempts": attempts,
                     "restart_policy": diagnostics["restart_policy"]})

    with local_head_failure_cluster(args, coordinator_cpus=0, recovery_enabled=options["mode"] == "on",
                                    allow_head_failure=options["scenario"] == "data-owner",
                                    include_worker_failure=True) as cluster:
        runtime = ray.get_runtime_context()
        driver = {"pid": os.getpid(), "node_id": runtime.get_node_id(), "job_id": runtime.get_job_id()}
        current = copy.deepcopy(options)
        for index in range(2):
            observed_driver = {"pid": os.getpid(), "node_id": runtime.get_node_id(), "job_id": runtime.get_job_id()}
            if observed_driver != driver:
                raise ValueError("Application attempt changed the surviving driver")
            attempt_directory = directory / f"attempt-{index}"
            attempt_directory.mkdir()
            attempt = {"mode": current["mode"], "scenario": current["scenario"],
                       "failure_point": current["failure_point"], "directory": str(attempt_directory),
                       "status": "running", "provenance": diagnostics["provenance"],
                       "driver_identity": observed_driver, "attempt_started_ns": time.monotonic_ns()}
            attempts.append(attempt)
            save()
            try:
                result = run_workload(current, attempt_directory, attempt, existing_cluster=cluster)
                attempt.update(result, status="passed")
            except Exception as exc:
                attempt.update(status="failed", validation_status="failed", error_type=type(exc).__name__,
                               error=str(exc), traceback=traceback.format_exc())
            attempt["attempt_finished_ns"] = time.monotonic_ns()
            write_record(attempt_directory / "case.json", attempt)
            save()
            if attempt["status"] == "passed":
                # The final attempt carries normal workload validation fields.
                # Earlier failures remain embedded; no sum of isolated runs is substituted.
                diagnostics.update({k: v for k, v in attempt.items() if k not in ("scenario", "mode", "failure_point", "directory", "status")})
                diagnostics["whole_workload_restarts"] = index
                diagnostics["predictions_directory"] = str(attempt_directory)
                return {"validation_status": "passed", "training_s": attempt["training_s"]}
            if index or options["mode"] != "off" or options["scenario"] != "data-owner":
                raise RuntimeError(attempt["error"])
            verify_owner_loss(attempt)
            attempt["expected_owner_loss"] = True
            # Fresh application state and output paths, but the same repaired
            # Ray cluster, driver, inputs, OS cache and standard retry budget.
            if driver != {"pid": os.getpid(), "node_id": runtime.get_node_id(), "job_id": runtime.get_job_id()}:
                raise ValueError("Application restart changed the surviving driver")
            case_args, crash_head, crash_worker = cluster
            replacement = argparse.Namespace(**vars(case_args))
            replacement.owner_node_id = attempt["data_owner_fault"]["head_replacement"]["replacement_head_node_id"]
            cluster = (replacement, crash_head, crash_worker)
            current.update(scenario="none", failure_point="none",
                           owner_progress_plan={"map_count": 4, "target_index": 0, "inject": False})
            save()
