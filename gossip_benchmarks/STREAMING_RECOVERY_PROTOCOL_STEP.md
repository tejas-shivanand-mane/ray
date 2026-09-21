# Fixed-R streaming recovery: descriptor and witness handoff

Starting checkpoint: `565816c09fb8fe897641c238b8c888e7d44a0345`, latest `main`
fetched on 2026-09-21. The user reported the focused native streaming tests and
the full task-manager/reference-counter targets passed at that checkpoint.

## Implemented in this step

- `RecoveryStreamDescriptor` version 1 binds the original TaskID, deterministic
  completion ID, declared object count N, designated consumer address, and
  initial K=1 Fixed-R manifest. It is retained inside the full TaskSpec and sent
  on a streaming witness claim. Consumer cursor/live-ref state is not frozen
  into the installation descriptor: the surviving consumer supplies current c.
- `RecoveryStreamInstallation` is a serialized owner-local gate. It exposes a
  ready descriptor only after every distinct selected holder has ACKed the exact
  full-recipe install and the designated consumer has acknowledged retaining the
  offered descriptor. Duplicate ACKs cannot substitute for a missing holder.
  Failure, supersession, or explicit cancellation prevents readiness. This is
  state machinery for the future adapter, not yet a CoreWorker publication call.
- The existing raylet witness RPC accepts enrolled streams only with the new
  disabled-by-default `enable_recovery_streaming_fixed_r` configuration and the
  existing Fixed-R configuration. Ordinary streaming TaskSpecs without an
  explicit valid descriptor are rejected. Static publication is unchanged.
- Installation validates the count, retry-enabled normal streaming mode, single
  object per yield, by-value inputs without nested refs/dependency sidecars,
  original owner, and independent non-owner holder nodes. Later installs cannot
  change or remove the descriptor or replace the stream's retained recipe.
- The witness coordinator requires the exact installed descriptor, the
  designated consumer, and authoritative knowledge that the original owner node
  failed. The consumer must still be live. Replicated reservations must name that
  same consumer and recovery attempt 1. Existing ordered Fixed-R claim
  replication and tombstone deletion remain in use.
- `PrepareRecoveryStreamReplay` validates a granted reply against the descriptor
  and initial manifest before any ownership mutation. It preserves TaskID,
  changes the caller to the consumer, uses replay attempt 1, sets N, and removes
  soft node affinity. Redirects, mismatched consumers/topologies/counts,
  tombstones, and subsequent recovery attempts are rejected.
- `TaskManager::AddPendingStreamingTaskFromWitness` feeds a validated grant into
  the native adoption primitive and returns the exact registered TaskSpec for
  later dispatch. Rejection leaves output arguments and ownership unchanged.
  The ordinary CoreWorker static replay entry now explicitly rejects streams.

## Bounded contract and remaining integration

Version 1 is deliberately limited to one original owner-node failure, an initial
attempt-0 recipe, and a surviving designated consumer. The experiment must not
include an earlier executor retry or consumer/acting-owner failure. Earlier
executor retries need an additional attempt-fencing design; the installation
recipe alone cannot reveal the latest attempt on a subsequently failed owner.
The code validates the installed attempt, not the absence of later executor
failures. Deterministic values/finite N remain application assumptions.

The descriptor is internal protocol state, not a cryptographic capability.
Possession of its bytes is not proof of all-R installation. The future adapter
must associate every callback with its exact outgoing installation, retain refs
and cursor before acknowledging consumer receipt, announce ready only through
the gate, and call the handoff only with an actual selected-witness RPC reply.
Gate calls require serialization, and a cancelled/failed gate cannot be reused.
All participants must run the same rebuilt fork; mixed-version operation is not
part of this experiment.

This change does **not** enroll or dispatch ordinary streaming tasks, expose a
Python API, or implement an owner-loss benchmark. The new flag enables only the
witness protocol boundary. Automatic streaming eligibility remains disabled for
Fixed-R and Succession. The gate and checked native handoff have no production
CoreWorker adapter caller yet.

Next integration work is the before-first-yield enrollment/dispatch barrier,
transferable single-consumer frontend adapter and pre-failure ref forwarding,
claim RPC orchestration, resolver/plasma owner rebinding, recipe lifetime and
cancellation/tombstone propagation, replay dispatch, and handle cleanup. A
cancelled gate alone does not send tombstones or release retained witness state.
Count validation still happens at native completion; an adapter must bound
delivery and handle producer errors. Ray Data coordinator and dependency
survival remain a separate gate after the bounded owner-loss experiment.

## Validation

Eight new `StreamingRecoveryProtocolTest` cases cover the all-R/receipt gate,
duplicate and missing ACKs, failure/cancellation, invalid identities/topologies,
unsupported recipes, immutable recipe matching, replay preparation, and bad
grants. Two additional `StreamingRecoveryTest` cases use the real reference
counter to check the witness-to-native handoff, retained prefix, duplicate
adoption, cleanup, and atomic rejection. Witness RPC/GCS behavior has been
reviewed in source; these tests do not instantiate a distributed raylet cluster.

No builds, tests, lint, benchmarks, or rendering were run by the agent. Run:

```bash
bazel test //src/ray/common/streaming_recovery:streaming_recovery_test \
  //src/ray/core_worker/tests:task_manager_test \
  //src/ray/core_worker/tests:reference_counter_test \
  --test_output=errors
```

The task-manager tests do not compile the modified NodeManager/CoreWorker
implementations. Also build those integration targets:

```bash
bazel build //src/ray/raylet:raylet \
  //src/ray/core_worker:core_worker_lib
```

Use your usual additional Bazel flags. These protocol changes require rebuilding
the protobuf/native components. A passing build and unit tests will still not
establish owner-loss recovery or Ray Data benchmark support. README is unchanged.

## User validation reported after publication

The user reported all three targets above passed at
`71f9f7c5a7b538e1dafc0ded14ba2ea9dcb4ed3f`, then confirmed that the entire Ray
build had already completed successfully. These reports cover this protocol
step, not subsequent consumer-adapter changes or distributed owner-loss behavior.
