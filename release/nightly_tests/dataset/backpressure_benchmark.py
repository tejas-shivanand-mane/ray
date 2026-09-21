import argparse
import functools
import time

import numpy as np
import pyarrow as pa
import ray

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backpressure benchmark")
    parser.add_argument(
        "--case",
        choices=["fast-producer-slow-consumer", "training-prefetch"],
        required=True,
    )
    parser.add_argument("--num-input-blocks", type=int, default=128)
    parser.add_argument("--output-batches-per-input-batch", type=int, default=8)
    parser.add_argument("--output-batch-rows", type=int, default=128)
    parser.add_argument("--output-row-bytes", type=int, default=1024**2)
    parser.add_argument("--consumer-sleep-s", type=float, default=1.0)
    parser.add_argument("--num-trainers", type=int, default=8)
    parser.add_argument("--prefetch-batches", type=int, default=8)
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
        "--local-executor-nodes", type=int, default=0,
        help="Create a local test cluster with this many two-CPU executor nodes",
    )
    parser.add_argument("--local-object-store-mb", type=int, default=256)
    parser.add_argument(
        "--disable-locality-hints",
        action="store_true",
        default=False,
        help="Disable locality hints for streaming_split",
    )
    return parser.parse_args()


def make_inputs(num_input_blocks: int):
    return [
        pa.Table.from_pydict({"id": [input_id]}) for input_id in range(num_input_blocks)
    ]


def produce(
    batch,
    *,
    output_batches_per_input_batch: int,
    output_batch_rows: int,
    output_row_bytes: int,
):
    for _ in range(output_batches_per_input_batch):
        yield {
            "data": np.zeros((output_batch_rows, output_row_bytes), dtype=np.uint8),
        }


def consume_slow(batch, *, sleep_s: float):
    time.sleep(sleep_s)
    return {"status": ["ok"]}


def run_fast_producer_slow_consumer(args: argparse.Namespace):
    producer = functools.partial(
        produce,
        output_batches_per_input_batch=args.output_batches_per_input_batch,
        output_batch_rows=args.output_batch_rows,
        output_row_bytes=args.output_row_bytes,
    )
    consumer = functools.partial(consume_slow, sleep_s=args.consumer_sleep_s)

    ds = (
        ray.data.from_blocks(make_inputs(args.num_input_blocks))
        .map_batches(producer)
        .map_batches(consumer, compute=ray.data.TaskPoolStrategy(size=1))
    )
    for _ in ds.iter_internal_ref_bundles():
        pass

    return vars(args)


def run_training_prefetch(args: argparse.Namespace):
    producer = functools.partial(
        produce,
        output_batches_per_input_batch=args.output_batches_per_input_batch,
        output_batch_rows=args.output_batch_rows,
        output_row_bytes=args.output_row_bytes,
    )

    trainers = [
        Trainer.options(scheduling_strategy="SPREAD").remote(
            consumer_sleep_s=args.consumer_sleep_s,
            prefetch_batches=args.prefetch_batches,
        )
        for _ in range(args.num_trainers)
    ]

    trainer_node_ids = ray.get([trainer.get_node_id.remote() for trainer in trainers])

    iterators = (
        ray.data.from_blocks(make_inputs(args.num_input_blocks))
        .map_batches(producer)
        .streaming_split(
            args.num_trainers,
            equal=True,
            locality_hints=trainer_node_ids
            if not args.disable_locality_hints
            else None,
        )
    )

    ray.get(
        [
            trainers[i].train.remote(iterators[i], batch_size=args.output_batch_rows)
            for i in range(args.num_trainers)
        ]
    )

    return vars(args)


@ray.remote(num_cpus=1)
class Trainer:
    def __init__(self, consumer_sleep_s: float, prefetch_batches: int):
        self._consumer_sleep_s = consumer_sleep_s
        self._prefetch_batches = prefetch_batches

    def train(self, data_iterator, batch_size: int):
        for _ in data_iterator.iter_batches(
            batch_size=batch_size,
            prefetch_batches=self._prefetch_batches,
        ):
            time.sleep(self._consumer_sleep_s)

    def get_node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    if args.recovery_mode != "original":
        run_recovery_cases(benchmark, args)
    elif args.case == "fast-producer-slow-consumer":
        benchmark.run_fn(args.case, run_fast_producer_slow_consumer, args)
    elif args.case == "training-prefetch":
        benchmark.run_fn(args.case, run_training_prefetch, args)
    else:
        raise ValueError(f"Unexpected benchmark case: {args.case}")

    benchmark.write_result()


def run_recovery_cases(benchmark, args):
    from streaming_recovery_benchmark import MODES, recovery_system_config, run_controlled

    if args.case != "fast-producer-slow-consumer":
        raise ValueError("Controlled recovery modes support fast-producer-slow-consumer only")
    if args.local_executor_nodes < 0 or args.local_object_store_mb < 80:
        raise ValueError("Local nodes must be nonnegative and object stores at least 80 MiB")
    if args.recovery_mode == "suite" and not args.local_executor_nodes:
        raise ValueError("suite requires --local-executor-nodes; external failures need a fresh owner")
    if args.local_executor_nodes and (args.owner_node_id or args.executor_node_ids):
        raise ValueError("Choose local nodes or explicit cluster node IDs")
    if args.recovery_mode == "fixed_r_head_failure":
        from streaming_recovery_head_failure import local_head_failure_cluster

        with local_head_failure_cluster(args) as (case_args, crash_head):
            benchmark.run_fn(
                f"{args.case}/fixed_r_head_failure", run_controlled, case_args,
                crash_owner=crash_head,
            )
            benchmark.write_result()
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


if __name__ == "__main__":
    main(parse_args())
