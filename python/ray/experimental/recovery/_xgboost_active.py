"""Experimental rollback after an interrupted CPU Rabit collective.

The local test gates rank zero in a training callback while rank one calls a
real allreduce on the training communicator. It does not model arbitrary
failures in XGBoost's native tree-building operations.
"""

import json
from pathlib import Path
import time

import numpy as np
import xgboost as xgb

from ray.experimental.recovery._xgboost_boundary import BoundaryWorker, tree_digest


def write_event(directory, rank, phase, **fields):
    path = Path(directory) / f"rank-{rank}-{phase}.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps({"rank": rank, "phase": phase,
                                     "time_ns": time.monotonic_ns(), **fields}))
    temporary.replace(path)


def validate_cleanup(result, identity, checkpoint_round, checkpoint_digest, generation):
    if (result["identity"] != identity or result["generation"] != generation
            or result["checkpoint_round"] != checkpoint_round
            or result["discarded_rounds"] != 1
            or result["restored_tree_sha256"] != checkpoint_digest
            or not result["allreduce_error"] or not result["communicator_cleared"]):
        raise ValueError("Healthy worker did not cleanly roll back in the same process")


class ActiveWorker(BoundaryWorker):
    def interrupt_segment(self, tracker_args, checkpoint, start, generation, event_directory):
        if (self.frame is None or generation <= self.generation or start <= 0
                or checkpoint is None or xgb.collective.is_distributed()):
            raise ValueError("Invalid interrupted generation or communicator state")
        saved = xgb.Booster(model_file=bytearray(checkpoint))
        if (saved.num_boosted_rounds() != start or self.model is None
                or tree_digest(self.model) != tree_digest(saved)):
            raise ValueError("Interrupted segment requires the committed checkpoint")
        before = self.identity()
        actor = self
        allreduce_error = []
        speculative_rounds = []

        class Interrupt(xgb.callback.TrainingCallback):
            def after_iteration(self, model, epoch, evals_log):
                rounds = model.num_boosted_rounds()
                if rounds != start + 1:
                    raise ValueError("Fault gate missed the first uncommitted round")
                actor.model = model
                speculative_rounds.append(rounds)
                # Both ranks finish an actual new boosting round before the
                # asymmetric gate; no checkpoint is published for this round.
                xgb.collective.allreduce(np.ones(2, dtype=np.float32), xgb.collective.Op.SUM)
                phase = "gated" if actor.rank == 0 else "allreduce-enter"
                write_event(event_directory, actor.rank, phase, round=rounds,
                            generation=generation, identity=before)
                if actor.rank == 0:
                    # The controller kills this logical node. A missed fault
                    # raises an error; it never releases the gate as success.
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        time.sleep(.05)
                    raise TimeoutError("Controller did not inject the gated node failure")
                try:
                    xgb.collective.allreduce(np.ones(1024, dtype=np.float32), xgb.collective.Op.SUM)
                except xgb.core.XGBoostError as exc:
                    allreduce_error.append(str(exc))
                    write_event(event_directory, actor.rank, "allreduce-error", error=str(exc))
                    raise
                raise ValueError("Faulted allreduce unexpectedly succeeded")

        args = {**tracker_args, "dmlc_task_id": f"rank-{self.rank:08}",
                "dmlc_timeout": 10, "dmlc_retry": 1}
        context = xgb.collective.CommunicatorContext(**args)
        context.__enter__()
        training_error = None
        finalize_error = None
        try:
            if xgb.collective.get_rank() != self.rank or xgb.collective.get_world_size() != 2:
                raise ValueError("Unexpected collective membership")
            self.matrix = xgb.DMatrix(self.frame.drop("labels", axis=1), label=self.frame["labels"], nthread=1)
            self.matrix_builds += 1
            xgb.train({"objective": "binary:logistic", "eval_metric": ["logloss", "error"],
                       "tree_method": "hist", "nthread": 1, "seed": 0},
                      self.matrix, num_boost_round=2, xgb_model=saved, callbacks=[Interrupt()])
        except xgb.core.XGBoostError as exc:
            training_error = str(exc)
        finally:
            try:
                # A failed shutdown may still clear the thread-local native
                # communicator. Verify that explicitly before permitting reuse.
                context.__exit__(None, None, None)
            except xgb.core.XGBoostError as exc:
                finalize_error = str(exc)
                write_event(event_directory, self.rank, "finalize-error", error=str(exc))
        if (not allreduce_error or not training_error or speculative_rounds != [start + 1]
                or xgb.collective.is_distributed()):
            raise ValueError("Interrupted communicator could not be safely reused")
        # Always deserialize the durable checkpoint; discard the speculative
        # booster and its matrix even when the healthy process stays alive.
        self.model = xgb.Booster(model_file=bytearray(checkpoint))
        self.matrix = None
        self.generation = generation
        result = {"identity": self.identity(), "generation": generation,
                  "checkpoint_round": start, "discarded_rounds": speculative_rounds[0] - start,
                  "restored_tree_sha256": tree_digest(self.model),
                  "allreduce_error": allreduce_error[0], "finalize_error": finalize_error,
                  "communicator_cleared": not bool(xgb.collective.is_distributed()),
                  "cleanup_finished_ns": time.monotonic_ns()}
        validate_cleanup(result, before, start, tree_digest(saved), generation)
        write_event(event_directory, self.rank, "rolled-back", result=result)
        return result
