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
Capture is bounded to 16 checkpoints and 16 GiB of cumulative transfer per attempt. Failed captures also
consume transfer budget; publish only complete points, at a useful cadence. Invalid/incomplete manifests
are not advertised and do not abort training or block a later valid manifest. Capture errors appear in
the assignment observation. Retained captures stop growing at the limit; automatic rotation is future work.
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
A failed export can leave a partial directory, without a completed `checkpoint.json` descriptor.

Python exposes `submit(checkpoint=...)`, `checkpoints(job)`, `suspend(job, checkpoint='latest')`,
`resume(job)`, `attempts(job)`, `fetch_checkpoint(id, destination)`, and `attempt=N` on logs/outputs/fetch.
`wait()` waits for success, failure, or cancellation; suspension is intentionally nonterminal at the
logical-job level. Poll `job()['phase']` when waiting specifically for suspension.

The iced-m3 GUI shows the current attempt, verified checkpoint position/location, and explicit suspend
and resume actions. It resets the log view when the execution changes. Historical attempt inspection
and checkpoint export are available through CLI/Python.

## Durability and scope

Checkpoints survive VM deletion and worker process restart, but their bytes remain on the original
worker. Resume is restricted to that worker. A replicated control-plane catalog is not a checkpoint
backup: disk loss can lose both sealed files and blobs. Export points you need to keep off-machine.
The control plane need not be always online for capture, but quorum is required to publish metadata,
accept suspend/resume, and resolve normal client reads.

There is no automatic retry, cross-worker migration, checkpoint garbage collection, user-defined
checkpoint hook, exactly-once external side-effect guarantee, or automatic resume after failure in this
milestone. Cancellation is terminal. Explicit suspension frees VM compute and disk reservations only
after confirmed deletion; host checkpoint storage is retained separately and is bounded per attempt,
not by a cluster-wide retention policy. Tart networking and VM isolation are unchanged.

## Explicit format-5 upgrade

The model uses control-plane/worker format 6. Mixed-version operation is rejected. Fresh homes need no
migration. For an existing format-5 installation:

1. Finish or cancel queued/active jobs and stop or delete persistent VMs. Do not upgrade under training.
2. If needed, cooperatively shrink to a single voter using the old binary and let it settle.
3. Stop controller, worker, and client-staging services. Preserve old binaries and service definitions.
4. Run the new binary offline, listing all applicable worker and client homes:

   ```sh
   fleet cluster upgrade --controller-home /absolute/controller \
     --worker-home /absolute/worker --client-home /absolute/client
   ```

5. Start the new binary and verify history and idle state before submitting work. Add freshly prepared
   replicas to grow back to three. Never start an old replica against the upgraded cluster.

The command locks and validates every selected database before writing, backs up each changed file with
`.v5-backup` (a unique suffix prevents overwriting an earlier backup), and converts it transactionally.
It preserves cluster identity, applied log boundary, votes/membership, receipts, backend execution IDs,
and completed job history. Each old job's existing assignment becomes its first attempt. A v6 snapshot
replaces the compacted historical log; original raw log bytes remain in the backup. Repeating the command
skips already-converted databases, allowing recovery from interruption between database commits.

It refuses active/uncertain work, unapplied log suffixes, mixed cluster identities, nonempty prototype
job databases, pending membership operations, and format-5 installations bound to emergency recovery
history. Those need a separately reviewed migration, not an override. Historical released receipt
tombstones remain non-executable; a transformed old request may report a fingerprint conflict rather
than an expired receipt. Full retained receipts and saved requests are rehashed for the typed commands.

Rollback before accepting new work requires stopping every upgraded role and restoring all matching
backups and the old binary together. Do not roll back after executing new work: that would discard
accepted history. Backups are operator recovery material, not files to publish or upload to CI.

## Qualification

`cargo test --all-features --locked` covers metadata transitions, recovery, idempotency, corrupt manifests,
worker restart, leader loss, immutable attempts, resource-release ordering, and offline upgrade refusal
and preservation. CI also runs:

```sh
python3 contrib/qualify_checkpoint_tart.py --fake --binary "$PWD/target/debug/fleet" \
  --home /tmp/fleet-control-checkpoint-ci
```

Omit `--fake` and provide `--instances /absolute/instances.json --image-cache /absolute/tart/cache` for
a real offline macOS VM qualification. Use a fresh `/tmp/fleet-control-*` home. Real mode checks that
installed work is idle and verifies its records remain unchanged. The script tests export, suspension,
VM deletion, combined-service restart, fresh-VM resume, request retries, and both attempts' logs.
An optional `--job /absolute/qualification-job.json` supplies application-specific submit arguments.
