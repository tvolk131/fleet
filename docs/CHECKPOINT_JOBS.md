# Checkpoint-resumable VM jobs

Fleet can suspend a Tart job at a verified application checkpoint, delete its VM, and later run a new
attempt in a fresh VM under the same logical job ID. This is explicit application resume, not a VM memory
snapshot. Work performed after the selected checkpoint is discarded and will run again.

## Model and control-plane boundary

`State.jobs` is the durable logical job registry. A job owns its original specification, scheduling
requirements, revision, lifecycle, ordered attempt IDs, and selected resume checkpoint. Each attempt is
one immutable worker assignment with its own backend execution ID, attempt number, and source checkpoint.
Logs, metrics, and outputs belong to that attempt. The original specification never changes on resume.

Persistent VMs use the same placement envelope but have no job attempt provenance. Job intent is
Running/Suspended/Cancelled; VM intent is Running/Stopped/Deleted. Their observation types are distinct.
The worker and state machine reject a VM phase on a job, superseded observations cannot free resources,
and an old attempt cannot complete the current attempt. The old prototype `Submit`, `Cancel`, `Observe`
commands and simulated job registry are gone. The simulator lives only in tests.

The checkpoint additions to the closed consensus command enum are `CheckpointReady`, `SuspendJob`, and
`ResumeJob`. Applying them changes metadata deterministically; it never runs guest code or transfers files.
The normal scheduler commits placement, and workers reconcile committed assignment intent idempotently.

1. The application atomically publishes an immutable manifest after finishing all named files.
2. The worker reads files through the existing bounded guest protocol, verifies sizes and SHA-256,
   syncs a private host copy, imports content-addressed blobs under durable tags, and commits its journal.
3. The authenticated worker proposes `CheckpointReady`. Only then can the control plane advertise the
   checkpoint as a selectable resume point. File bytes stay outside Raft.
4. Suspend commits the exact selected checkpoint and a new assignment revision. The worker stops the
   guest workload and deletes its disposable VM. Capacity remains reserved until cleanup is confirmed.
5. Resume requires that confirmation. The scheduler creates a new attempt, stages the original inputs
   plus verified checkpoint files, and starts the declared resume command in a new VM.

Leader loss and client/worker restart preserve this flow. Emergency recovery imports checkpoint metadata
alongside immutable attempt history, while retaining the existing authority rebinding, reconciliation,
and explicit resume safeguards. A missing worker or missing files never triggers speculative rerun.

## Application contract

A job JSON can contain:

```json
{
  "name": "Resumable trainer",
  "argv": ["bin/trainer", "--output", "training"],
  "inputs": {"bin/trainer": "/absolute/trainer"},
  "publish": ["training/metrics.jsonl"],
  "vm": {"image":"macos","cpus":2,"memory_mb":8192,"disk_gb":50,"network":"offline","gpu":"exclusive"},
  "checkpoint": {
    "manifests": "training/checkpoints/*/fleet.json",
    "resume_argv": ["bin/trainer", "--resume-from", "{checkpoint}", "--output", "training"]
  }
}
```

The command is an argv array, not a shell expression. `{checkpoint}` expands to the relative directory
`__fleet_checkpoint`; `{attempt}` expands to the new one-based attempt number. That input directory is
reserved. Initial inputs are retained for subsequent attempts. Checkpoint policies are supported only
for VM jobs; native process pause/legacy resume are not part of this contract.

For a file containing the single byte `7`, a complete `training/checkpoints/one/fleet.json` is:

```json
{
  "version": 1,
  "position": 7,
  "files": {
    "state": {"size": 1, "sha256": "7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451"}
  }
}
```

Paths are relative to the manifest's directory. `position` is optional application-defined progress for
display; Fleet does not infer consistency from it. Include every file needed to restore the intended
application state: model weights alone may omit optimizer, random state, opponent pools, or counters.
The application owns consistency. Write files into a new checkpoint directory, close them, then write
and atomically rename the manifest. Never modify a published directory or reuse a captured manifest path.
Fleet verifies the exact declared bytes; it cannot determine whether the application saved enough state.

Each manifest permits 1–128 files, at most 1 GiB per file and 4 GiB total, with a 64 KiB manifest limit.
Opt in to [rolling retention](ROLLING_STORAGE.md) for renewable capture budgets, automatic retirement of older checkpoints, pins and storage visibility.

By default, capture is bounded to 16 checkpoints and 16 GiB of cumulative transfer per attempt. Failed captures also
consume transfer budget; publish only complete points, at a useful cadence. Invalid/incomplete manifests
are not advertised and do not abort training or block a later valid manifest. Capture errors appear in
the worker storage report (`fleet storage status`). Without a rolling policy, retained captures stop growing at the limit.
The combined original and checkpoint inputs must also fit the existing VM job input limits.

## Client workflow

```sh
fleet submit /absolute/job.json --id training-1 --request-id training-submit
fleet checkpoints training-1
fleet suspend training-1 --checkpoint latest --request-id stop-at-checkpoint
fleet job training-1                 # wait for phase suspended, not merely accepted
fleet resume training-1 --request-id continue-training
fleet attempts training-1
fleet logs training-1 --attempt 1
fleet outputs training-1 --attempt 2
fleet fetch training-1 /absolute/new-output --attempt 2
fleet fetch-checkpoint CHECKPOINT_ID /absolute/new-checkpoint-export
```

`latest` resolves once when preparing the durable suspend request and selects only among the current
attempt's registered checkpoints. Retry with `fleet retry REQUEST_ID`; it preserves that exact selection.
Suspend does not ask the application to create a new checkpoint and is rejected if none is registered.
The selected checkpoint remains explicit even if another point is captured during shutdown.

`logs`, `outputs`, and `fetch` default to the current/latest attempt. The export command verifies all bytes
and writes `files/` plus `checkpoint.json` only after verification. It requires a fresh destination.
Failed exports remove their partial destination. Configured backups allow export with the worker offline.

The [JSON CLI interface](NORMAL_FLEET.md#json-automation-interface) exposes `checkpoints`, `suspend`,
`resume`, `attempts`, and `fetch_checkpoint`, plus `attempt` on `logs`, `outputs`, and `fetch`. Submit
checkpoint policy inside the request’s `job` object. Suspend/resume require explicit request IDs and
expected job revisions. Poll `job` for `succeeded`, `failed`, or `cancelled` when waiting for completion;
suspension is intentionally nonterminal at the logical-job level. Poll specifically for `suspended`
when waiting for suspension.

The iced-m3 GUI shows the current attempt, verified checkpoint position/location, and explicit suspend
and resume actions. It resets the log view when the execution changes. Historical attempt inspection
and checkpoint export are available through the CLI/JSON interface.

## Durability and scope

Checkpoints survive VM deletion and worker process restart. Configured [storage agents](ARTIFACT_STORAGE.md)
retain off-worker copies. [Portable Tart jobs](PORTABLE_JOBS.md) also back up executable/input bundles
and can cooperatively resume on another compatible worker; other jobs resume on the original worker.
A replicated control-plane catalog alone is not a checkpoint backup. Physical host-loss protection
requires a copy on another machine; original input dependencies and worker identity/history are separate.
The control plane need not be always online for capture, but quorum is required to publish metadata,
accept suspend/resume, and resolve normal client reads.

There is no automatic retry, lost-worker takeover, user-defined
checkpoint hook, exactly-once external side-effect guarantee, or automatic resume after failure in this
milestone. Cancellation is terminal. Explicit suspension frees VM compute and disk reservations only
after confirmed deletion; host checkpoint storage is retained separately and is bounded per attempt,
with explicit artifact retention/pinning and safe cleanup available through the storage catalog.
Tart networking and VM isolation are unchanged.

## Baseline and existing state

New installations use [baseline family format 1](BASELINE.md), including checkpoints, portable inputs,
history, storage lifecycle and permanent request receipts. The sole existing installation has a
separate offline importer that preserves suspended jobs, checkpoint metadata and execution fences.
Historical format-by-format runtime migrations are removed. Future compatible releases use explicit
[rolling generation upgrades](ROLLING_UPGRADES.md).

## Qualification

`cargo test --all-features --locked` covers metadata transitions, recovery, idempotency, corrupt manifests,
worker restart, leader loss, immutable attempts and resource-release ordering. The separate importer
tests verify preservation and refusal. CI also runs:

```sh
target/debug/fleet-qualify checkpoint --fake --backup-restore --binary "$PWD/target/debug/fleet" \
  --home /tmp/fleet-control-checkpoint-ci
```

Omit `--fake` and provide `--instances /absolute/instances.json --image-cache /absolute/tart/cache` for
a real offline macOS VM qualification. Use a fresh `/tmp/fleet-control-*` home. Real mode checks that
installed work is idle and verifies its records remain unchanged. The script tests export, suspension,
VM deletion, combined-service restart, fresh-VM resume, request retries, and both attempts' logs.
An optional `--job /absolute/qualification-job.json` supplies an ordinary job JSON file with absolute, individual input file paths.
