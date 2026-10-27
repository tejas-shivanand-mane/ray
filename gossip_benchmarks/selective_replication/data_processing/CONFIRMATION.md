# Does Z3 deliver a practical advantage?

This is a prespecified follow-up to the exploratory document-deduplication
counterexample. It can support, contradict, or leave the hypothesis unresolved
for these cases. It cannot establish universal superiority across applications.

## Run

From the repository root, in the existing `ray-dev` environment:

```bash
git pull --ff-only origin main
python -m pip install -r gossip_benchmarks/selective_replication/data_processing/requirements-comprehensive.txt
python gossip_benchmarks/selective_replication/data_processing/comprehensive.py --confirm-z3
```

The assistant prepares code only. No assistant-run benchmark, tests, import
checks, compilation, lint or hooks are implied. The owner runs this command.

The controller has the same **28-minute limit per invocation**, including data
preparation, profiling, planning, checks and replays. Package installation and
final report writing are outside this timer. Completion within the cap depends
on the machine; partial work is retained. Share `analysis.zip` from the printed
`ray_gcp_results/data-processing-confirmation/...` directory.

```bash
python gossip_benchmarks/selective_replication/data_processing/comprehensive.py \
  --resume ray_gcp_results/data-processing-confirmation/YOUR_RUN_DIRECTORY
```

Resume uses frozen settings and completed replays. Do not combine it with
`--confirm-z3`, change sources/dependencies, reuse an old exploratory run, or
stop early because an interim interval looks favorable. The primary verdict
requires all seven blocks and the complete protocol.

## Fixed design

The application is the existing seven-source MinHash/LSH document deduplication
pipeline, with serial independent producers and one fused final reducer. No
workload time or output size is padded. Input/shard hashes, document IDs and
actual checkpoint bytes are recorded.

Two disjoint samples each contain 1,500 records from the same fixed shuffled
20 Newsgroups corpus, capped at 512 normalized tokens per document:

- **Replication:** first 1,500 records, matching the exploratory data selection,
  but with fresh profiles, planning and execution timings.
- **Heldout:** next 1,500 records, never used by the earlier case discovery.
  Disjoint records need not have distinct text; natural duplicates remain.

This tests a second sample of the same application, not a second application.
Each sample is profiled twice. All methods receive the same measured coefficients
for that sample, and those coefficients are fixed throughout measured execution.
No old plan, profiling measurement or execution observation is reused.

Five cases are fixed in advance for **each** sample:

| Case | Failure phase | Checkpoint budget | Assumed failure probability |
|---|---|---|---:|
| Target | Final reducer | 579970/901133 of total checkpoint bytes | 30% |
| Lower budget | Final reducer | 60% of total checkpoint bytes | 30% |
| Lower risk | Final reducer | Same target ratio | 10% |
| Progress control | Across useful computation | Same target ratio | 30% |
| Unrestricted control | Final reducer | All optional checkpoint bytes | 30% |

The target ratio is frozen from the previous discovery. There is no search for
a new favorable budget, no choosing cases based on fresh results, and no silent
replacement of a case where greedy ties or wins. Budgets are rounded once from
profiled bytes. The protocol is saved in `manifest.json` before execution.

Planning failure positions remain 15%, 35%, 55%, 75%, 95%; evaluation uses
20%, 60%, 90%. In reduce cases these are positions within the reducer; in the
progress control they span all useful compute including the final task.
These are assumed scenarios, not production failure traces. There is at most
one failure; its three evaluation positions have equal conditional weight.
Durable inputs, checkpoints and the controller survive process loss. Any timer
fallback is reported. All recovered outputs must match the profile references.

## Methods and exactness

- Recompute; checkpoint every producer when feasible.
- Existing multi-seed greedy and greedy with one-for-one swaps, unchanged.
- **Exact knapsack dynamic programming**, using a sparse Pareto frontier.
- Z3, with explicit strict-improvement checks and lexicographic optimization.

For this particular independent-producer graph, the modeled expected cost is
additive over checkpoint choices. The DP evaluates the empty choice and each
singleton, then selects the minimum-cost combination under the byte budget.
It does not consume the exhaustive table or Z3's answer. An independent audit
checks additivity over every subset and verifies both exact methods against
exhaustive enumeration. If that structure is violated or a solve is unresolved,
the run stops and retains diagnostics instead of silently dropping Z3.

The exact methods use the same tie-break: expected cost, then bytes, then mask.
They should therefore return the same plan. **If so, Z3 has no execution-time
advantage over DP; the remaining distinction is planning cost.** This is an
intentional strong baseline, not a reason to omit it. Results cannot establish
that SMT is necessary for a problem already solved by this specialized DP.

Each planner is invoked three times on the frozen profile in shuffled order.
The report includes all planning times and uses their median. Choices must be
consistent. The exhaustive/additivity audit is experimental overhead, outside
these planning timers; it is still inside the overall controller time limit.

## Measurements and decision rules

There are **seven complete timing blocks**. Every block covers all ten cases;
unique checkpoint sets are executed in seeded shuffled order under healthy and
three interrupted conditions. Identical plans share observations and are exact
execution ties. They are not counted as independent runs for different methods.

The report provides:

1. Healthy and with-failure completion times, including worker restart.
2. Expected execution time using the case's assumed probability.
3. Expected execution plus one planning decision.
4. Expected time per job if one plan serves 20 comparable jobs, estimated as
   execution + planning/20. This is not an executed 20-job batch.
5. One-job totals including the measured profiling cost for methods that need
   a profile. Recompute and checkpoint-all do not pay profiling cost.

The **primary metric** is the equal-case average percentage improvement of Z3
in expected execution **plus planning**. There are two primary comparisons:
Z3 versus greedy with swaps, and Z3 versus exact DP. Within each block, combine
all ten cases before computing uncertainty; shared risk mixtures and repeated
uses of the same timing are not treated as independent samples.

For these two primary comparisons, paired-t intervals use Bonferroni correction
for a 5% family error rate (97.5% interval for each comparison). A positive lower
bound supports Z3 being faster for this fixed suite; a negative upper bound
supports the baseline; an interval spanning zero is inconclusive. A separate
predeclared **1% practical margin** is met only if the corrected lower bound is
above 1%. Lack of significance is not evidence of equivalence.

Per-case, execution-only and amortized comparisons are **descriptive** and use
uncorrected intervals. Their results must not replace the primary metric after
the run. In particular, faster execution with higher end-to-end time means a
better checkpoint set may exist, but does not establish a one-job Z3 advantage.

Seven repetitions give limited precision. Intervals are conditional on these
fixed input samples, profiles, selected plans and observed planning medians;
they do not cover uncertainty in real failure rates, new profiling sessions,
or other datasets/machines. The experiment may honestly remain inconclusive.

Optional owner-run DP correctness checks:

```bash
python gossip_benchmarks/selective_replication/data_processing/test_confirmation.py
```

## Outputs

`analysis.zip` contains the frozen manifest, profiles, per-case plans and all
planning observations, Z3 queries/results, raw replay events, input hashes/IDs,
completion status, aggregate CSV, full comparison JSON, summary and source
snapshots. It excludes raw corpus files and checkpoint payloads. Both losses and
ties remain visible. An interrupted run receives no final primary verdict.
