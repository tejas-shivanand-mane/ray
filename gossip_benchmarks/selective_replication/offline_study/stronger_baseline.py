"""Risk-aware sequential baseline used as an additional diagnostic.

First optimize single-copy placement under the same failure model. If no plan
meets the target, use maximum success probability, then the same busy-time
objective. Freeze those primaries, but allow any backups, order and checkpoint
choices in phase two. This is stronger than healthy-only placement_first.
"""
import itertools

import sensitivity
from reference import planner


def solve(spec, case, choices, delay=8, scale=1.25):
    seed = (
        case["policies"]["checkpoint_retry"]
        or case["unconstrained_policies"]["checkpoint_retry"]
    )
    fixed = seed["plan"]
    rows = []
    for backups in itertools.product(
        *[
            [(), *((m,) for m in spec["machines"] if m != fixed["copies"][t][0])]
            for t in spec["tasks"]
        ]
    ):
        for order in itertools.permutations(spec["tasks"]):
            for cps in itertools.product(choices, repeat=len(spec["tasks"])):
                plan = dict(
                    copies={
                        t: [fixed["copies"][t][0], *b]
                        for t, b in zip(spec["tasks"], backups)
                    },
                    order=order,
                    checkpoint_every=dict(zip(spec["tasks"], cps)),
                )
                rows.append(
                    sensitivity.evaluate(
                        plan,
                        spec["tasks"],
                        spec["machines"],
                        spec["profile"],
                        case["scenarios"],
                        case["deadline_s"],
                        case["horizon_s"],
                        detection_s=delay,
                        epoch_scale=scale,
                        dispatch_s=1,
                    )
                )
    return dict(
        single_copy_seed=seed,
        result=planner.choose(rows, case["target"], "expected_worker_busy_s"),
    )
