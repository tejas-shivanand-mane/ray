# Selective replication feasibility

Run `gossip_benchmarks/Selective_Replication_Feasibility.ipynb` in a Python
environment with torch, numpy, scikit-learn, and matplotlib. It uses scikit-learn's
bundled digits dataset (no download), three independent MLP hyperparameter trials,
and three local CPU worker processes. All settings are visible in the notebook.

The question is whether choosing task placement and duplication together offers
enough benefit to justify an SMT implementation. No SMT solver is used yet.
Replicas execute the same trial with private model/optimizer state. The first
validated result commits once; other copies are cancelled. This is task
replication, not adding DDP ranks. All policies can retry from shared checkpoints.

The enumerator covers one/two copies per trial, ordered placements on distinct
machines, and all task priority orders. Originals have dispatch priority over
backups. Failed assignments relocate to the cheapest free live machine; a busy
live assigned machine is waited for. This restricted search does not optimize
arbitrary start times, adaptive priorities, or an arbitrary DAG. Shared input and
the final selection run outside the failure domain on the controller.

Profiles measure one-thread CPU training, initialization, and checkpoint writes.
The model averages per-epoch/checkpoint times; it does not model contention or
variable restoration costs. Local replay tests these modeling limitations.
Worker startup and dataset preparation are outside the warm-pool timing interval.
Controller/dataset/checkpoint storage survive injected process loss. Nothing here
establishes head-VM resilience or a TorchFT/Fixed-R contribution.

Costs and scenario probabilities are assumptions, not current cloud prices or
estimated failure rates. Billing starts on first use and includes idle time until
job completion or machine loss. Provisioning, standby-pool, storage and network
charges are excluded. The two failure-prone workers can fail together; the third
is assumed to survive the enumerated scenarios. No independence assumption is
used to multiply node survival probabilities.

Run the notebook's profile, search, replay, and report cells in order. Use a new
output directory for each study; existing results are never overwritten by a new
profile/search/replay. Replay defaults select cases using modeled results and
include an unseen fault time; edit case indices for broader coverage. Identical
plans share measurements. Report tiny effects and negative results honestly.

Tests: `python -m pytest -q -o addopts='' gossip_benchmarks/selective_replication/test_study.py`.
