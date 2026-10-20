# Offline selective recovery study

For the new comparison of **joint checkpoint planning against strong sequential
and multistart alternating optimization**, use [JOINT_PLANNING.md](JOINT_PLANNING.md).
It has a separate resumable entry point, saves all wins/ties/losses, and requires
no training run. The older commands below concern the replication study.

Run from the repository root using Python 3.10 or newer. The study uses only the
standard library and the existing parent `planner.py`; it needs no Ray cluster,
GCP credentials, or new cloud resources.

```bash
python3 -m venv .venv-offline
source .venv-offline/bin/activate
python gossip_benchmarks/selective_replication/offline_study/run_offline.py
```

The default command reoptimizes the selected synthetic candidate against all
existing planner baselines, evaluates an additional risk-aware sequential
baseline, checks 12 frozen-plan sensitivity settings, and recalculates the
selected policies without the sweep cache. It first runs 200 simulator parity
checks and two analytic delayed-loss checks.

To repeat the entire discovery search, add `--full-sweep` (Linux or macOS):

```bash
python gossip_benchmarks/selective_replication/offline_study/run_offline.py --full-sweep
```

The full search covers 1,968 configurations: 216 transformations of the included
measured timing profile, 720 seeded heterogeneous configurations, 960 with
nonzero reliable-worker loss, and 72 dense-timing configurations. It also repeats
the earlier candidate's sensitivity audit. Every case is saved, including ties
and infeasible policies. The original six baselines are retained throughout;
the additional risk-aware sequential baseline is evaluated on the final
candidate only, so sweep counts do not establish superiority over that baseline.

Outputs go to a unique `ray_gcp_results/offline-selective/` directory. Use
`--output /path/to/new-directory` to choose a location. Existing directories are
not overwritten. Each stage has a log; `status.json` records completed stages
and any failure. `--max-minutes` can lower the default 28-minute total execution
budget. Timeout or interruption preserves completed output and returns nonzero;
partial results must not be reported as a completed sweep. A fresh invocation
starts a new run. The individual scripts accept input/output paths if you want
to rerun a particular unfinished stage.

## Candidate and interpretation

Two independent training trials share three workers. One worker is fast but
unreliable; two are slower, with one assigned a lower failure probability.
Checkpoints cost five seconds on every worker. The model includes eight seconds
of failure detection delay, a 1.25 compute-time multiplier, and one second of
additional dispatch time. It assumes a surviving controller and shared
checkpoint storage. These are synthetic assumptions, not measured cloud failure
rates or an actual training speedup.

Candidate selection uses 20 midpoint failure times within 157 seconds, a
185-second deadline, and a 99% modeled success target. Healthy probability is
50%; reliable-worker loss and correlated spot-worker loss each have probability
0.5%; individual spot-worker losses each have probability 24.5%. The checkpoint
menu is 1, 2, 5, 10, 20, 25, 33, 50, 75, and 100 epochs. All original policy
families use the same scenarios and menu.

Expected results for the included planner:

| Policy | Worker busy seconds | Elapsed seconds | Deadline success |
| --- | ---: | ---: | ---: |
| Optimized checkpoint retry | 313.56 | 162.92 | 99.225% |
| Fixed placement replication | 364.55 | 157.64 | 99.525% |
| Placement first | 331.51 | 151.55 | 99.675% |
| Joint selective plan | 276.24 | 157.50 | 99.550% |
| Risk-aware sequential | 276.24 | 157.50 | 99.550% |

The selective plan replicates trial A across the two spot workers, runs trial B
on the reliable worker, and saves only at completion. It reduces modeled worker
busy time, but is not the fastest policy in expected elapsed time. The stronger
sequential baseline finds the same execution plan. This is evidence of a useful
selective protection pattern in this model, not a unique advantage of joint
optimization. The original GCP experiment still has no demonstrated joint win.

The 50- and 100-point failure grids and timing perturbations are frozen-plan
sensitivity checks, not independent empirical validation. Discovery was adaptive:
the earlier sparse-timing candidate failed finer timing checks, motivating the
dense-timing search. Testing a real workload is still necessary.

`inputs/measured_profile.json` retains the pilot's task descriptions and timing
estimates, with machine network metadata removed. Its transformed workloads and
all seeded profiles are exploratory. `inputs/candidate.json` contains the final
synthetic two-trial profile. The simulator loads the repository planner directly
so future planner changes may change these results; design files record its hash.
