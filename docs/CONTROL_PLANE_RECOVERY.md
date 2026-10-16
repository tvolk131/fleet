# Lost quorum recovery

Fleet can recover a permanently unavailable control plane from one surviving replica's applied state.
This is an explicit disaster recovery operation in `fleet-control-lab`, separate from ordinary Raft
membership changes. It creates a new controller identity and wire incarnation, preserves the original
logical Fleet identity in a signed recovery record, and requires explicit client and worker rebinding.
Installed Fleet services and training records remain outside this workflow.

The normal queue and recovery use protocol and storage format 9. See the [checkpoint/upgrade guide](CHECKPOINT_JOBS.md) for the current model. Recovery-bound format-5/6/7/8 state is deliberately refused by that upgrade; earlier lab formats and the SQLite coordinator are not imported. The recovery command
accepts a concrete loopback or LAN address. See [multi-machine deployment](CONTROL_PLANE_NETWORKING.md)
for the network configuration and physical-host qualification.

## What recovery preserves and can lose

The source replica must be stopped. redb's exclusive process lock prevents reading a live source as
an offline recovery image. Fleet preserves a complete `source.redb` copy, exports `source-state.json`,
and signs their hashes and the last applied log position with an existing administrator key. Only the
applied state is used: locally present, unapplied log entries are not promoted into decisions, even if
another replica might have committed them. The new Raft history starts with a single fresh replica.
The original source is not repurposed as the recovered replica.

Jobs, assignments, registrations, resource reservations, receipts and observation cursors in that image
survive. Old membership operations remain in the source backup and historical receipts, but are not
continued in the new membership history. Preserved acceptance receipts describe historical acceptance;
they do not imply that an interrupted membership operation completed.

An acknowledged submission, cancellation, enrollment or receipt can be absent from the survivor. Worker
inventory may recover accepted assignments and newer intentions. It cannot reconstruct a missing client
receipt or prove that an absent request never ran. Artifact bytes remain on workers; recovery does not
replicate files, restore missing worker journals, or guarantee exactly-once external effects.

## Retire the previous authority

Before recovery, stop or isolate **all old controller processes**, including machines that might return
later. A disconnected majority can still operate under its original configuration. A recovery manifest
cannot remotely revoke authority on a machine that has not received it. Existing workloads and detached
supervisors may continue while controllers and worker agents are stopped.

`recover` requires `--accept-data-loss` and `--old-controllers-fenced`. These acknowledge operator actions;
Fleet cannot independently prove that an unreachable machine is powered off. Retain the old directories
for investigation. Returning controller machines must use fresh homes and keys and join the recovered
cluster through normal membership management; never bootstrap their old databases.

## Recover a stopped survivor

Use a new destination directory, the survivor's matching configuration, an existing administrator key,
and an unused port on an explicit local IP address. These example paths are placeholders for an isolated deployment:

```sh
target/debug/fleet-control-lab recover \
  --source-home /tmp/fleet-example/node-1 \
  --config /tmp/fleet-example/node-1/config.json \
  --key /tmp/fleet-example/client/node.key \
  --home /tmp/fleet-recovered \
  --bind 127.0.0.1:28101 \
  --accept-data-loss --old-controllers-fenced
```

Review `/tmp/fleet-recovered/recovery.json`: it contains the source position, source hashes, logical
cluster identity, previous and new wire incarnations, new replica public identity, and administrator
signature. Configuration is published last. Interrupted preparation leaves the original usable and a
partial destination that must not be started; retry into another fresh destination.

Start the recovered replica once with bootstrap:

```sh
target/debug/fleet-control-lab serve \
  --home /tmp/fleet-recovered --config /tmp/fleet-recovered/config.json --bootstrap
```

Subsequent restarts omit `--bootstrap`. New assignments and worker-initiated running effects remain paused after recovery.
Ordinary membership changes can grow this new single-replica cluster to three voters.

## Rebind clients and preserve request origins

For each client configuration, explicitly accept the signed recovery record:

```sh
target/debug/fleet-control-lab accept-recovery \
  --config /tmp/fleet-example/client/config.json \
  --manifest /tmp/fleet-recovered/recovery.json
```

The signer must already be an administrator trusted by that configuration. The manifest must name the
currently trusted previous incarnation. A sibling recovery cannot silently override an accepted choice.
Configuration replacement is atomic, and discovery caches are separated by incarnation. Stop old
client processes before rebinding; an already-running client retains its old in-memory configuration.
Clients that missed several recoveries accept the signed chain in order.

Every mutation file includes an immutable `origin` containing the wire cluster ID at creation:

```json
{"origin":"CLUSTER_ID_AT_CREATION","id":"cancel-example","mutation":{"Desire":{"id":"example","expected_revision":1,"desired":"Cancelled"}}}
```

Preserve the complete request file across retries. Never rewrite its origin after recovery.
`submit-job` keeps that origin in its durable prepared request; its historical retry checks the receipt
without uploading inputs or requiring the original worker. `membership` and `resume-recovery` require
an explicit `--origin`, which must also remain unchanged on retry.

A surviving matching receipt returns its original response or `Expired`. A conflicting use of the same
request ID is rejected. An old-origin request with no surviving receipt returns `RecoveryUnknown` and
is never executed or recorded as a fresh request. Recovery inventory can establish that an execution
exists without turning this unknown submission receipt into a fabricated acceptance. Deliberately new
operations use fresh IDs and the newly accepted origin.

## Rebind and reconcile workers

Stop each worker agent, keeping its journal, key and backend directory intact. Detached supervisors
continue to own their workloads. Then run:

```sh
target/debug/fleet-control-lab rebind-worker \
  --home /tmp/fleet-example/worker \
  --manifest /tmp/fleet-recovered/recovery.json

target/debug/fleet-control-lab worker --home /tmp/fleet-example/worker
```

Rebinding commits the new authority and a frozen recovery inventory in the worker journal before
publishing the new configuration. The old configuration then fails startup validation. Repeating the
same rebind repairs a crash between those two writes. A running worker holds the journal lock and
cannot be rebound concurrently. No worker key, execution ID, backend ownership, or attempt marker is
reset. A pending inventory prevents the agent from executing stale intentions while disconnected.
After inventory commits, the worker persists the pause until it observes explicit administrator resume;
this also holds accepted work that has not started yet. Backend observers in replicated-worker mode do
not independently relaunch VM or guest supervisors. Already detached supervisors continue their accepted
work, and explicit cancellation/deletion remains available while paused.

The worker submits its own authenticated durable inventory. The replicated state machine stages and
validates the entire merge before committing it and its sequence cursor atomically:

- Matching assignments retain their identities and adopt newer accepted intentions and observations.
- Assignments missing from the recovered controller are imported from the worker journal.
- Restored assignments absent from the worker journal are held as uncertain. They require an explicit
  `Desire` cancellation (jobs) or deletion (instances); Fleet does not automatically start them.
- Definition conflicts, inconsistent revisions or a worker cursor older than the restored cursor refuse
  the merge. Inspect the preserved histories; there is no force-adopt or journal-reset bypass.
- Reconciliation is retryable, including after a lost response. Missing or uncertain backend executions
  keep conservative reservations; the existing backend launch guards still apply.

If the worker's enrollment was also lost, submit a new-origin `Enroll` request containing its original
registration from `worker.json`, then let it reconnect. Do not prepare a replacement worker over its
existing home. A registered worker remains quarantined until its inventory is committed.

Inspect `read` before resuming. `recovery.inventories` identifies reconciled workers, `recovery.held`
identifies uncertain assignments, and `recovery.manifest` preserves the chosen recovery provenance.
Cancellation/deletion of reconciled work remains possible while new work is paused.

## Resume and handle unavailable workers

After inspecting reconciliation, explicitly enable new work:

```sh
target/debug/fleet-control-lab resume-recovery \
  --key /tmp/fleet-example/client/node.key \
  --config /tmp/fleet-example/client/config.json \
  --origin RECOVERED_CLUSTER_ID --request-id resume-after-review
```

If enrolled workers remain unavailable, name **every** unreconciled worker with repeated
`--unavailable-worker WORKER_ID` options. The request fails if that set differs from current state.
Those workers retain their assignments and reservations and cannot receive new work or lifecycle
changes until they are rebound and reconciled. They are not automatically replaced elsewhere. This
allows healthy reconciled workers to operate without assuming that unreachable workloads have stopped.
A later valid inventory permits that worker to participate under the already-resumed recovery.

## Qualification

Run the isolated consensus, membership, worker and recovery tests serially to avoid test-port contention:

```sh
cargo test --features control-plane --locked --test control -- --test-threads=1
```

The recovery tests cover missing committed history, an unapplied Raft suffix, old request retries,
released receipts, signed trust changes, old replica rejection, atomic inventory conflicts, quarantined
workers, native supervisor continuity, persistent Tart fixtures, held VM assignments and normal growth
back to three voters. Before admitting learners, a recovered cluster snapshots its seeded state and
compacts the log prefix. A new replica must receive that snapshot: replaying only the new incarnation's
log would omit the recovered history. Regression coverage cold-restarts the new majority with the
original survivor offline and checks historical jobs, receipts, provenance and unknown-request refusal.
The tests also cover repeated recovery, interrupted worker rebinding, durable launch pause and refusal
of automatic backend repair during recovery. Tart fixture tests do not claim additional Apple hypervisor qualification.

A separate process canary uses the actual CLI, three controller processes, worker SIGKILL, a deliberately
stale recovery source, a tiny native job, explicit client/worker rebinding, cancellation and verified
output capture. It only creates its own new directory and never opens installed Fleet state:

```sh
python3 contrib/qualify_control_recovery.py \
  --binary /absolute/path/to/fleet/target/debug/fleet-control-lab \
  --home /tmp/fleet-recovery-canary-new
```

The canary records its binary hash and results in `qualification.json` and stops its fixture processes.
The qualification record (local hardware qualification record; excluded from the public source package) records the completed run and
its boundaries. The subsequent [multi-host qualification](CONTROL_PLANE_NETWORKING.md) exercises this
workflow across physical machines. Physical power loss, disk corruption and total loss of every usable
controller database remain outside this milestone.

Artifact storage roles also require explicit signed rebinding. Emergency recovery holds payload cleanup
until the recovered catalog is reviewed; see [storage recovery safeguards](ARTIFACT_STORAGE.md#emergency-control-plane-recovery).
