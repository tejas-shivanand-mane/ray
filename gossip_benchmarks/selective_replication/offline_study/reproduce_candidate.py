"""Reoptimize the selected synthetic candidate with every original baseline."""

import argparse
import hashlib
import json
from pathlib import Path

import sensitivity
from dense_timing_sweep import scenarios
from offline_sweep import dump
from reference import planner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=False)
    spec = json.loads((Path(__file__).parent / "inputs/candidate.json").read_text())
    choices = [1, 2, 5, 10, 20, 25, 33, 50, 75, 100]
    fractions = [(i + 0.5) / 20 for i in range(20)]
    dump(
        out / "design.json",
        dict(
            spec=spec,
            checkpoint_choices=choices,
            fractions=fractions,
            safe_probability=0.005,
            correlated_probability=0.005,
            deadline_s=185,
            target=0.99,
            detection_s=8,
            epoch_scale=1.25,
            dispatch_s=1,
            failure_window_s=157,
            planner_sha256=hashlib.sha256(
                Path(planner.__file__).read_bytes()
            ).hexdigest(),
            limitation="Synthetic discovery model; not measured training or cloud reliability.",
        ),
    )
    original = planner.simulate
    planner.simulate = lambda *args: sensitivity.simulate(
        *args, detection_s=8, epoch_scale=1.25, dispatch_s=1
    )
    try:
        case = planner.search(
            spec["tasks"],
            spec["machines"],
            spec["profile"],
            scenarios(0.005, 0.005, fractions),
            185,
            target=0.99,
            checkpoint_choices={t: choices for t in spec["tasks"]},
            objective="expected_worker_busy_s",
        )
    finally:
        planner.simulate = original
    dump(out / "case-030.json", case)
    print("Synthetic candidate: expected busy / elapsed seconds / success probability")
    for name, row in case["policies"].items():
        if row is None:
            print(f"{name}: infeasible")
        else:
            print(
                f"{name}: {row['expected_worker_busy_s']:.2f} / "
                f"{row['expected_elapsed_s']:.2f} / {row['success_probability']:.5f}"
            )


if __name__ == "__main__":
    main()
