import json
import numpy as np
import os
import pandas as pd
import time
from typing import Dict

import xgboost as xgb

import ray
from ray import data
try:
    import lightgbm as lgb
    from ray.train.lightgbm import (
        LightGBMTrainer,
        RayTrainReportCallback as LightGBMReportCallback,
        normalize_pandas_for_lightgbm,
    )
except ImportError:
    # XGBoost runs need not install the unused LightGBM framework.
    lgb = LightGBMTrainer = LightGBMReportCallback = normalize_pandas_for_lightgbm = None
from ray.train.xgboost import (
    RayTrainReportCallback as XGBoostReportCallback,
    XGBoostTrainer,
)
from ray.train import FailureConfig, RunConfig, ScalingConfig

_TRAINING_TIME_THRESHOLD = 600
_PREDICTION_TIME_THRESHOLD = 450

_EXPERIMENT_PARAMS = {
    "smoke_test": {
        "data": (
            "https://air-example-data-2.s3.us-west-2.amazonaws.com/"
            "10G-xgboost-data.parquet/8034b2644a1d426d9be3bbfa78673dfa_000000.parquet"
        ),
        "num_workers": 1,
        "cpus_per_worker": 1,
    },
    "10G": {
        "data": "s3://air-example-data-2/10G-xgboost-data.parquet/",
        "num_workers": 1,
        "cpus_per_worker": 12,
    },
    "100G": {
        "data": "s3://air-example-data-2/100G-xgboost-data.parquet/",
        "num_workers": 10,
        "cpus_per_worker": 12,
    },
}


class BasePredictor:
    def __init__(self, report_callback_cls, result: ray.train.Result):
        self.model = report_callback_cls.get_model(result.checkpoint)

    def __call__(self, data):
        raise NotImplementedError


class XGBoostPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        dmatrix = xgb.DMatrix(data)
        return {"predictions": self.model.predict(dmatrix)}


class LightGBMPredictor(BasePredictor):
    def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        return {"predictions": self.model.predict(normalize_pandas_for_lightgbm(data))}


class ResumableXGBoostReportCallback(XGBoostReportCallback):
    """Checkpoint on absolute model rounds, including after a worker restart."""

    def __init__(self, frequency, restored_rounds):
        super().__init__(frequency=frequency)
        self.restored_rounds = restored_rounds
        self.completed_rounds = restored_rounds

    def _with_progress(self, report_dict):
        return {
            **report_dict,
            "boosting_rounds": self.completed_rounds,
            "restored_checkpoint_rounds": self.restored_rounds,
        }

    def _report_metrics(self, report_dict):
        return super()._report_metrics(self._with_progress(report_dict))

    def _save_and_report_checkpoint(self, report_dict, model):
        return super()._save_and_report_checkpoint(self._with_progress(report_dict), model)

    def after_iteration(self, model, epoch, evals_log):
        self.completed_rounds = model.num_boosted_rounds()
        # XGBoost restarts `epoch` at zero when appending to a loaded model.
        # Use absolute rounds for both checkpoint frequency and final deduplication.
        return super().after_iteration(model, self.completed_rounds - 1, evals_log)


def xgboost_train_loop_function(config: Dict):
    report_callback = config["report_callback_cls"]
    checkpoint = ray.train.get_checkpoint()
    starting_model = report_callback.get_model(checkpoint) if checkpoint else None
    restored_rounds = starting_model.num_boosted_rounds() if starting_model is not None else 0
    remaining_rounds = config.get("num_boost_round", 10) - restored_rounds
    if remaining_rounds < 0:
        raise ValueError("Checkpoint contains more rounds than the requested training target")
    if remaining_rounds == 0:
        # A failure can occur after the final checkpoint but before Train finishes.
        # Re-report that model without consuming data or adding extra trees.
        ray.train.report(
            {"boosting_rounds": restored_rounds, "restored_checkpoint_rounds": restored_rounds},
            checkpoint=checkpoint if ray.train.get_context().get_world_rank() == 0 else None,
        )
        return

    train_ds_iter = ray.train.get_dataset_shard("train")
    train_df = train_ds_iter.materialize().to_pandas()

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]

    dtrain = xgb.DMatrix(train_X, label=train_y)

    frequency = config.get("checkpoint_frequency", 0)
    callback = (
        ResumableXGBoostReportCallback(frequency, restored_rounds)
        if frequency or restored_rounds else report_callback()
    )
    xgb.train(
        params,
        dtrain=dtrain,
        num_boost_round=remaining_rounds,
        xgb_model=starting_model,
        callbacks=[callback],
    )


def lightgbm_train_loop_function(config: Dict):
    train_ds_iter = ray.train.get_dataset_shard("train")
    train_df = normalize_pandas_for_lightgbm(train_ds_iter.materialize().to_pandas())

    label_column, params = config["label_column"], config["params"]
    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
    train_set = lgb.Dataset(train_X, label=train_y)

    report_callback = config["report_callback_cls"]
    network_params = ray.train.lightgbm.get_network_params()
    params.update(network_params)

    lgb.train(
        params,
        train_set=train_set,
        num_boost_round=config.get("num_boost_round", 10),
        callbacks=[report_callback()],
    )


_FRAMEWORK_PARAMS = {
    "xgboost": {
        "trainer_cls": XGBoostTrainer,
        "predictor_cls": XGBoostPredictor,
        "train_loop_function": xgboost_train_loop_function,
        "train_loop_config": {
            "params": {
                "objective": "binary:logistic",
                "eval_metric": ["logloss", "error"],
            },
            "label_column": "labels",
            "report_callback_cls": XGBoostReportCallback,
        },
    },
    "lightgbm": {
        "trainer_cls": LightGBMTrainer,
        "predictor_cls": LightGBMPredictor,
        "train_loop_function": lightgbm_train_loop_function,
        "train_loop_config": {
            "params": {
                "objective": "binary",
                "metric": ["binary_logloss", "binary_error"],
            },
            "label_column": "labels",
            "report_callback_cls": LightGBMReportCallback,
        },
    },
}


def train(
    framework: str, data_path: str, num_workers: int, cpus_per_worker: int,
    *, run_config=None, read_kwargs=None, placement_strategy="PACK", num_boost_round=10,
    checkpoint_frequency=0,
) -> ray.train.Result:
    if num_boost_round < 1:
        raise ValueError("num_boost_round must be positive")
    if checkpoint_frequency < 0 or (checkpoint_frequency and framework != "xgboost"):
        raise ValueError("Periodic checkpoint recovery requires XGBoost and a nonnegative frequency")
    ds = data.read_parquet(data_path, **(read_kwargs or {}))
    framework_params = _FRAMEWORK_PARAMS[framework]
    if framework_params["trainer_cls"] is None:
        raise ImportError("Install lightgbm to run the LightGBM benchmark")

    trainer_cls = framework_params["trainer_cls"]
    framework_train_loop_fn = framework_params["train_loop_function"]

    trainer = trainer_cls(
        train_loop_per_worker=framework_train_loop_fn,
        train_loop_config={
            **framework_params["train_loop_config"], "num_boost_round": num_boost_round,
            "checkpoint_frequency": checkpoint_frequency,
        },
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": cpus_per_worker},
            placement_strategy=placement_strategy,
        ),
        datasets={"train": ds},
        run_config=run_config or RunConfig(
            storage_path="/mnt/cluster_storage", name=f"{framework}_benchmark"
        ),
    )
    result = trainer.fit()
    return result


def predict(
    framework: str, result: ray.train.Result, data_path: str,
    *, output_path="/mnt/cluster_storage/predictions", read_kwargs=None,
):
    framework_params = _FRAMEWORK_PARAMS[framework]

    predictor_cls = framework_params["predictor_cls"]

    ds = data.read_parquet(data_path, **(read_kwargs or {}))
    ds = ds.drop_columns(["labels"])

    concurrency = int(ray.cluster_resources()["CPU"] // 2)
    ds.map_batches(
        predictor_cls,
        # Improve prediction throughput with larger batch size than default 4096
        batch_size=8192,
        concurrency=concurrency,
        fn_constructor_kwargs={
            "report_callback_cls": framework_params["train_loop_config"][
                "report_callback_cls"
            ],
            "result": result,
        },
        batch_format="pandas",
    ).write_parquet(output_path)


def main(args):
    framework = args.framework

    experiment = args.size if not args.smoke_test else "smoke_test"
    experiment_params = _EXPERIMENT_PARAMS[experiment]

    data_path, num_workers, cpus_per_worker = (
        experiment_params["data"],
        experiment_params["num_workers"],
        experiment_params["cpus_per_worker"],
    )
    data_path = getattr(args, "data_path", None) or data_path
    num_workers = getattr(args, "num_workers", None) or num_workers
    cpus_per_worker = getattr(args, "cpus_per_worker", None) or cpus_per_worker
    read_blocks = getattr(args, "read_blocks", None)
    read_kwargs = {"override_num_blocks": read_blocks} if read_blocks else None
    if getattr(args, "small_blocks", False):
        data.DataContext.get_current().target_min_block_size = 0
    storage_path = getattr(args, "storage_path", None)
    num_boost_round = getattr(args, "num_boost_round", 10)

    print(f"Running {framework} training benchmark...")
    training_start = time.perf_counter()
    result = train(
        framework, data_path, num_workers, cpus_per_worker,
        read_kwargs=read_kwargs,
        run_config=RunConfig(
            storage_path=storage_path or "/mnt/cluster_storage", name=f"{framework}_benchmark",
            failure_config=FailureConfig(max_failures=args.max_failures),
        ),
        placement_strategy=getattr(args, "placement_strategy", "PACK"),
        num_boost_round=num_boost_round,
        checkpoint_frequency=args.checkpoint_frequency,
    )
    training_time = time.perf_counter() - training_start

    print(f"Running {framework} prediction benchmark...")
    prediction_start = time.perf_counter()
    predict(
        framework, result, data_path, read_kwargs=read_kwargs,
        output_path=getattr(args, "prediction_output_path", "/mnt/cluster_storage/predictions"),
    )
    prediction_time = time.perf_counter() - prediction_start

    times = {
        "training_time": training_time, "prediction_time": prediction_time,
        "num_boost_round": num_boost_round,
        "checkpoint_frequency": args.checkpoint_frequency, "max_failures": args.max_failures,
    }
    print("Training result:\n", result)
    print("Training/prediction times:", times)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(times, f)

    if not args.disable_check:
        if training_time > _TRAINING_TIME_THRESHOLD:
            raise RuntimeError(
                f"Training is taking {training_time} seconds, "
                f"which is longer than expected ({_TRAINING_TIME_THRESHOLD} seconds)."
            )

        if prediction_time > _PREDICTION_TIME_THRESHOLD:
            raise RuntimeError(
                f"Batch prediction is taking {prediction_time} seconds, "
                f"which is longer than expected ({_PREDICTION_TIME_THRESHOLD} seconds)."
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "framework", type=str, choices=["xgboost", "lightgbm"], default="xgboost"
    )
    parser.add_argument("--size", type=str, choices=["10G", "100G"], default="100G")
    # Add a flag for disabling the timeout error.
    # Use case: running the benchmark as a documented example, in infra settings
    # different from the formal benchmark's EC2 setup.
    parser.add_argument(
        "--disable-check",
        action="store_true",
        help="disable runtime error on benchmark timeout",
    )
    parser.add_argument("--smoke-test", action="store_true")
    # Ordinary workload/resource options also make this entry point usable on a
    # development machine. Recovery is enabled externally by the shared launcher.
    parser.add_argument("--data-path")
    parser.add_argument("--num-workers", type=int)
    parser.add_argument("--num-boost-round", type=int, default=10,
                        help="Boosting rounds; increase to measure longer training on the same data")
    parser.add_argument("--checkpoint-frequency", type=int, default=0,
                        help="XGBoost: save every N model rounds; 0 retains final-only checkpoints")
    parser.add_argument("--max-failures", type=int, default=0,
                        help="Worker failure retry budget; XGBoost resumes from its latest checkpoint")
    parser.add_argument("--cpus-per-worker", type=int)
    parser.add_argument("--storage-path")
    parser.add_argument("--prediction-output-path", default="/mnt/cluster_storage/predictions")
    parser.add_argument("--read-blocks", type=int)
    parser.add_argument("--small-blocks", action="store_true")
    parser.add_argument("--placement-strategy", default="PACK", choices=("PACK", "SPREAD", "STRICT_SPREAD"))
    args = parser.parse_args()
    if args.num_boost_round < 1:
        parser.error("--num-boost-round must be positive")
    if args.checkpoint_frequency < 0 or (args.checkpoint_frequency and args.framework != "xgboost"):
        parser.error("--checkpoint-frequency must be nonnegative and requires XGBoost")
    if args.max_failures < 0 or (args.max_failures and args.framework != "xgboost"):
        parser.error("--max-failures must be nonnegative and requires XGBoost")
    main(args)
