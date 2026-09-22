"""Compatibility CLI for the earlier benchmark-specific recovery experiments.

Normal workloads need none of these options. Use the shared
``python -m ray.experimental.recovery`` launcher for new integrations.
"""

import argparse

import ray


def add_backpressure_arguments(parser):
    parser.add_argument(
        "--recovery-mode",
        choices=[
            "original", "ordinary", "copy", "fixed_r", "fixed_r_failure",
            "fixed_r_head_failure", "suite",
        ],
        default="original",
        help="Opt-in controlled physical benchmark; suite runs four fresh local clusters",
    )
    parser.add_argument("--owner-node-id")
    parser.add_argument("--executor-node-ids", help="Comma-separated surviving worker node IDs")
    parser.add_argument("--producer-concurrency", type=int)
    parser.add_argument("--recovery-timeout-s", type=float, default=120)
    parser.add_argument(
        "--recovery-head-timing", choices=["paused", "early", "middle", "late", "suite"],
        default="paused",
        help="Runtime plan: asynchronous failure after 10/50/90 percent producer rows",
    )
    parser.add_argument(
        "--runtime-failure-point",
        choices=["producer_before_output", "producer_after_output", "consumer"],
        default="producer_after_output",
        help="Failure point for a single runtime fixed_r_head_failure case",
    )
    parser.add_argument(
        "--local-executor-nodes", type=int, default=0,
        help="Create a local test cluster with this many two-CPU executor nodes",
    )
    parser.add_argument("--local-object-store-mb", type=int, default=256)
    parser.add_argument(
        "--recovery-plan", choices=["physical", "dataset", "runtime"], default="physical",
        help="dataset uses declared counts; runtime uses automatic streaming recovery",
    )
    parser.add_argument(
        "--recovery-workload", choices=["instrumented", "original"],
        default="instrumented",
        help="Original UDFs with normal block shaping; requires Dataset progress-triggered head failure",
    )
    parser.add_argument(
        "--head-failure-point", choices=["gated", "early", "middle", "late", "suite"],
        default="gated",
        help="Head failure at 10/50/90 percent output progress; suite runs all three",
    )


def add_worker_arguments(parser):
    parser.add_argument(
        "--recovery-mode", default="original",
        choices=["original", "copy", "fixed_r", "fixed_r_head_failure", "suite"],
        help="Opt-in task recovery / surviving-actor coverage; suite uses fresh local clusters",
    )
    parser.add_argument("--local-executor-nodes", type=int, default=2)
    parser.add_argument("--local-object-store-mb", type=int, default=512)
    parser.add_argument("--recovery-timeout-s", type=float, default=180)
    parser.add_argument(
        "--recovery-head-timing", choices=["paused", "early", "middle", "late", "suite"],
        default="paused",
        help="Dataset plan: asynchronous failure after 10/50/90 percent target-stage rows",
    )
    parser.add_argument("--recovery-output-mode", choices=["streaming", "buffered"], default="streaming")
    parser.add_argument(
        "--recovery-plan", choices=["controlled", "dataset"], default="controlled",
        help="dataset uses the original range/map_batches/materialize pipeline with runtime recovery",
    )
    parser.add_argument(
        "--recovery-failure-stage", choices=["read", "map"], default="map",
        help="For the dataset recovery plan, select ReadRange or a map stage for head failure",
    )
    parser.add_argument(
        "--recovery-failure-operator", type=int, default=0,
        help="Zero-based map stage whose first enrolled task gates head failure",
    )


def validate_worker_arguments(parser, args):
    if args.recovery_head_timing != "paused" and args.recovery_plan != "dataset":
        parser.error("--recovery-head-timing requires --recovery-plan dataset")
    if args.recovery_mode != "original":
        from streaming_recovery_worker_scaling import validate_recovery_args

        try:
            validate_recovery_args(args)
        except ValueError as exc:
            parser.error(str(exc))


def dispatch_worker(args):
    if args.recovery_mode == "original":
        return False
    if args.recovery_plan == "dataset":
        from streaming_recovery_worker_dataset import run_recovery_cases
    else:
        from streaming_recovery_worker_scaling import run_recovery_cases
    run_recovery_cases(args)
    return True


def dispatch_backpressure(args):
    if args.recovery_head_timing != "paused" and args.recovery_plan != "runtime":
        raise ValueError("--recovery-head-timing requires --recovery-plan runtime")
    if args.recovery_plan == "runtime":
        from streaming_recovery_backpressure_dataset import run_recovery_cases
        run_recovery_cases(args)
        return True
    if args.recovery_workload == "original" and (
        args.recovery_mode != "fixed_r_head_failure"
        or args.recovery_plan != "dataset" or args.head_failure_point == "gated"
    ):
        raise ValueError("Original-workload experiment requires dataset head failure at a progress point")
    if args.head_failure_point != "gated" and args.recovery_mode != "fixed_r_head_failure":
        raise ValueError("--head-failure-point requires --recovery-mode fixed_r_head_failure")
    if args.recovery_mode == "original":
        return False
    from benchmark import Benchmark
    benchmark = Benchmark()
    run_recovery_cases(benchmark, args)
    benchmark.write_result()
    return True


def run_recovery_cases(benchmark, args):
    from streaming_recovery_benchmark import MODES, recovery_system_config, run_controlled

    if args.case != "fast-producer-slow-consumer":
        raise ValueError("Controlled recovery modes support fast-producer-slow-consumer only")
    if args.recovery_plan == "dataset" and args.recovery_mode in ("ordinary", "suite"):
        raise ValueError("Dataset recovery supports copy, fixed_r, and both failure modes")
    if args.local_executor_nodes < 0 or args.local_object_store_mb < 80:
        raise ValueError("Local nodes must be nonnegative and object stores at least 80 MiB")
    if args.recovery_mode == "suite" and not args.local_executor_nodes:
        raise ValueError("suite requires --local-executor-nodes; external failures need a fresh owner")
    if args.local_executor_nodes and (args.owner_node_id or args.executor_node_ids):
        raise ValueError("Choose local nodes or explicit cluster node IDs")
    if args.recovery_mode == "fixed_r_head_failure":
        from streaming_recovery_head_failure import run_head_failure_cases

        run_head_failure_cases(benchmark, args)
        return
    modes = MODES if args.recovery_mode == "suite" else (args.recovery_mode,)
    for mode in modes:
        case_args = argparse.Namespace(**vars(args))
        case_args.recovery_mode = mode
        cluster = None
        try:
            if args.local_executor_nodes:
                from ray.cluster_utils import Cluster

                cluster = Cluster()
                node_options = dict(object_store_memory=args.local_object_store_mb * 1024**2)
                cluster.add_node(
                    num_cpus=1, include_dashboard=False,
                    _system_config=recovery_system_config(), **node_options,
                )
                executors = [cluster.add_node(num_cpus=2, **node_options)
                             for _ in range(args.local_executor_nodes)]
                owner = cluster.add_node(num_cpus=0, **node_options)
                cluster.wait_for_nodes()
                ray.init(address=cluster.address)
                case_args.owner_node_id = owner.node_id
                case_args.executor_node_ids = tuple(node.node_id for node in executors)

                def crash_owner():
                    cluster.remove_node(owner, allow_graceful=False)

            else:
                if not args.owner_node_id or not args.executor_node_ids:
                    raise ValueError("External runs require --owner-node-id and --executor-node-ids")
                ray.init(address="auto")
                case_args.executor_node_ids = tuple(args.executor_node_ids.split(","))
                crash_owner = None
            if case_args.producer_concurrency is None:
                case_args.producer_concurrency = len(case_args.executor_node_ids)
            benchmark.run_fn(
                f"{args.case}/{mode}", run_controlled, case_args, crash_owner=crash_owner,
            )
            # Preserve completed cases if a later mode fails.
            benchmark.write_result()
        finally:
            ray.shutdown()
            if cluster is not None:
                cluster.shutdown()

