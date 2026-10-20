"""Re-evaluate every selected policy without the sweep's simulation cache."""
import argparse
import json
import math
from pathlib import Path

import sensitivity
from reference import planner


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True)
    args = parser.parse_args()
    root = Path(args.results)
    count = 0
    sensitivity.verify()
    for directory in ["results", "heterogeneous_results", "reliability_results"]:
        paths = (root / directory).glob(
            "*__*.json" if directory == "results" else "profile-*.json"
        )
        for path in paths:
            model = json.loads(path.read_text())
            for case in model["cases"]:
                for family in ["policies", "unconstrained_policies"]:
                    for row in case[family].values():
                        if row is None:
                            continue
                        if directory == "results":
                            checked = planner.evaluate(
                                row["plan"],
                                model["tasks"],
                                case["machines"],
                                model["estimates"],
                                case["scenarios"],
                                case["deadline_s"],
                                case["horizon_s"],
                            )
                        else:
                            checked = sensitivity.evaluate(
                                row["plan"],
                                model["tasks"],
                                case["machines"],
                                model["profile"],
                                case["scenarios"],
                                case["deadline_s"],
                                case["horizon_s"],
                                detection_s=case["detection_s"],
                                epoch_scale=case["epoch_scale"],
                                dispatch_s=case["dispatch_s"],
                            )
                        for key in [
                            "success_probability",
                            "expected_worker_busy_s",
                            "expected_elapsed_s",
                            "expected_cost",
                        ]:
                            assert math.isclose(
                                row[key], checked[key], rel_tol=1e-10, abs_tol=1e-9
                            ), (path, key, row[key], checked[key])
                        count += 1
    dense = root / "dense_results"
    if (dense / "design.json").exists():
        spec = json.loads((dense / "design.json").read_text())["spec"]
        for path in dense.glob("case-[0-9][0-9][0-9].json"):
            case = json.loads(path.read_text())
            for family in ["policies", "unconstrained_policies"]:
                for row in case[family].values():
                    if row is None:
                        continue
                    checked = sensitivity.evaluate(
                        row["plan"],
                        spec["tasks"],
                        case["machines"],
                        spec["profile"],
                        case["scenarios"],
                        case["deadline_s"],
                        case["horizon_s"],
                        detection_s=8,
                        epoch_scale=1.25,
                        dispatch_s=1,
                    )
                    for key in [
                        "success_probability",
                        "expected_worker_busy_s",
                        "expected_elapsed_s",
                        "expected_cost",
                    ]:
                        assert math.isclose(
                            row[key], checked[key], rel_tol=1e-10, abs_tol=1e-9
                        ), (path, key, row[key], checked[key])
                    count += 1
    assert count > 0, "No policies found to verify"
    result = dict(
        selected_policy_recalculations=count,
        passed=True,
        zero_delay_equivalence_cases=200,
        delayed_loss_analytic_checks=2,
    )
    (root / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    print(result)


if __name__ == "__main__":
    main()
