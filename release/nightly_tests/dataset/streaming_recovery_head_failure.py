"""Local full-head-process failure for the controlled Data backpressure workload.

The driver and task executors survive on separate logical nodes. All processes
of the protected head, including GCS, are killed. A replacement head opens the
same RocksDB directory at the same GCS endpoint. This simulates head replacement
with surviving storage; it does not simulate loss of the host or its disk.
"""

import argparse
import time
import traceback

import ray

from streaming_recovery_benchmark import (
    HEAD_FAILURE_FRACTIONS,
    head_failure_target,
    run_controlled,
)


def run_head_failure_cases(benchmark, args):
    point = getattr(args, "head_failure_point", "gated")
    points = tuple(HEAD_FAILURE_FRACTIONS) if point == "suite" else (point,)
    if point != "gated":
        if args.recovery_plan != "dataset":
            raise ValueError("Progress-triggered head failure requires --recovery-plan dataset")
        for name in points:
            if getattr(args, "recovery_workload", "instrumented") != "original":
                head_failure_target(
                    name, args.num_input_blocks * args.output_batches_per_input_batch
                )
    failed = []
    for name in points:
        selected = argparse.Namespace(**vars(args))
        selected.head_failure_point = name
        key = f"{args.case}/fixed_r_head_failure"
        if name != "gated":
            key += f"/{name}"
        diagnostics = {}
        start = time.monotonic()
        try:
            # Every point gets fresh Ray processes and a separate GCS database.
            with local_head_failure_cluster(selected) as (case_args, crash_head):
                diagnostics.update(vars(case_args))
                if getattr(case_args, "recovery_workload", "instrumented") == "original":
                    from streaming_recovery_original_workload import prepare_original_workload

                    # Outside Benchmark.run_fn: calibration is not timed workload.
                    prepare_original_workload(case_args)
                # Also retain calibration when timed execution raises.
                diagnostics.update(vars(case_args))
                benchmark.run_fn(
                    key, run_controlled, case_args, crash_owner=crash_head,
                    diagnostics=diagnostics,
                )
            benchmark.result[key]["validation_status"] = "passed"
        except Exception as exc:
            failed.append(name)
            # Report the original failure before attempting to encode diagnostics.
            # A reporting error must not be the only traceback visible to users.
            traceback.print_exc()
            # Never replace a failure with partial-output success. Preserve its
            # phase, task observations and traceback, and exercise later points.
            benchmark.result[key] = {
                **benchmark.result.get(key, {}), **vars(selected), **diagnostics,
                "validation_status": "failed", "error_type": type(exc).__name__,
                "error": str(exc), "traceback": traceback.format_exc(),
                "case_wall_time_s": time.monotonic() - start,
            }
        finally:
            benchmark.write_result()
    if failed:
        raise RuntimeError(
            f"Head-failure cases failed: {', '.join(failed)}; inspect the saved JSON"
        )


# Compatibility import for the earlier evaluation harnesses.
from ray.experimental.recovery._local import local_head_failure_cluster
