# Artifact storage and lifecycle

Fleet catalogs portable inputs, checkpoints, final outputs and diagnostic history in its replicated control plane and keeps file
contents outside Raft. Independently enrolled storage agents retain verified copies over authenticated
iroh. A storage role can run alone or share a service process with a controller or worker; each role
has a separate identity and non-overlapping state home. Storage placement is independent of replica
membership. Growing to three controllers does not create three checkpoint copies.

## What is retained

An artifact has an immutable content manifest, owning job or source worker/attempt, kind, byte count, retention
revision, optional pin/expiry, and observations for its source and configured backup targets. Kinds are
a portable job input bundle, an application checkpoint, one final output set for a finished attempt, or an
immutable log/metric chunk. A suspended attempt can have final outputs too. [Durable history](DURABLE_HISTORY.md)
combines archived chunks with the worker's newest unarchived output; intermediate application output
reads retain their existing worker path.

`Enqueue` registers a portable input bundle with the job. `CheckpointReady` registers the checkpoint artifact atomically with the resume metadata. Final outputs
are registered after confirmed attempt cleanup. Backup policy applies to existing live artifacts as
well as future ones. Removing a default target does not delete its existing copies. Defaults are empty,
so enroll and configure storage explicitly. Inputs, checkpoints and final outputs do not expire
automatically by default; history has its own rolling byte/count limits and completion-age expiry.

Files are limited to 1 GiB each, 4 GiB per artifact, and 256 files per final-output artifact. Checkpoints
retain their stricter 128-file/64-KiB manifest limits. Oversized or changing final outputs cannot become
an artifact; the worker logs the publication failure and continues other artifacts and workloads.
Application consistency remains the application's responsibility.

## Consensus and reconciliation

The closed command enum adds `EnrollStorage`, `StoragePolicy`, `ArtifactReady`, `ArtifactReport`,
`ArtifactRetention`, `DeleteArtifact`, `ExpireArtifacts`, and `ReviewStorageRecovery`.
`CheckpointReady` also registers the corresponding artifact. History adds `HistoryReport`, `HistoryPolicy`
and `SweepHistory`, reusing the artifact copy/retention commands. Applying these commands changes only
metadata. Authenticated `StorageRead` and `WorkerRead` return quorum-confirmed intent. The storage and
worker reconcilers perform transfers/deletion afterward, then propose versioned observations. Disk IO,
network IO, and wall-clock expiry decisions never run inside the replicated state machine.

## Storage setup

Prepare storage using a trusted cluster configuration and an explicit advertised address. The key is
created locally in the storage home; only its public registration is sent to the administrator client.
`failure-domain` is an operator-declared physical machine/location label, not proof of independent
hardware. Use another machine and disk for protection against worker-host loss.

```sh
# On the storage host; use its actual LAN address, not the example address.
fleet cluster prepare-storage --home /absolute/storage \
  --config /absolute/trusted-cluster-config.json --id backup-1 \
  --bind 192.0.2.20:29420 --failure-domain backup-host --capacity-bytes 107374182400

# On an administrator client, after transferring registration.json (not node.key).
fleet storage enroll /absolute/registration.json --request-id enroll-backup-1
fleet storage status
fleet storage policy --revision 0 --input backup-1 --checkpoint backup-1 --output backup-1 --history backup-1 \
  --request-id configure-backups

# On the storage host; alternatively add --storage-home when configuring a combined service.
fleet service configure --config /absolute/storage-service.json --storage-home /absolute/storage
fleet service run --config /absolute/storage-service.json
```

Policy revisions are explicit. Read `storage status` before changing an existing policy; repeated
`--input`, `--checkpoint`, `--output` and `--history` options select multiple destinations. Supply every
desired category when replacing the default policy. Registration (identity, incarnation,
address, failure domain, quota) is immutable in this milestone; prepare a new role/home to replace one.
Storage agents can read source artifacts and report their assigned copies, but cannot enqueue work,
change policy, or read the administrator-only control-plane state.

The first backend is a private filesystem, not an object-storage provider. Artifact transfers expose
only manifest-listed files; there is no arbitrary path API. Guests receive no storage credentials and
retain the same offline/public-internet-only networking restrictions.

## Verification, quotas, and repair

A storage agent reserves room, downloads into a private staging directory, verifies BLAKE3 and sizes
(and the application's SHA-256 checkpoint manifest), fsyncs files and metadata, then atomically publishes
the complete directory before reporting `Ready`. Interrupted staging is discarded at startup. Ready
copies are audited on restart and every five minutes; every served file is verified again. Downloaders
verify received bytes independently. Corrupt copies are withdrawn and repaired from a worker or another
ready storage target. Failed transfers and quota refusals appear as copy errors; they are never `Ready`.

A per-store quota covers retained/staging file lengths and conservative transfer/metadata reservations.
Admission also checks filesystem free space. Transfers are serialized per storage agent and serving is
bounded to four streams. This is a Fleet admission limit, not an OS filesystem quota against unrelated
host processes. Size and allocated-block accounting includes orphaned payloads and partial staging:

```sh
fleet cluster storage-usage --home /absolute/storage
fleet cluster worker-storage-usage --home /absolute/worker
fleet artifacts
fleet artifacts --attempt JOB-a1
fleet fetch-artifact ARTIFACT_ID /absolute/new-directory
```

`artifacts` includes manifest, source/backup copy phases, revisions, pins, expiry, tombstones, and whether
the artifact is currently protected. Copy phases are `Pending`, `Ready`, `Missing`, `Failed`, `Deleted`.
Source and storage observations are authenticated, compare-and-swap versioned, and authority-bound.
A replica restart or leader change does not reset them. A stale report cannot undo deletion intent.

## Restore and export

`fetch-checkpoint`, `fetch-artifact`, and final `fetch JOB` prefer ready backup copies and fall back to
the source worker. A worker does not have to be online for backup export. Destinations must be fresh;
failed transfers remove their partial download. Exports validate every file before returning success.
The GUI shows checkpoint backup status, and its existing output download uses the same fallback path.

[Portable Tart jobs](PORTABLE_JOBS.md) can resume on a different compatible worker after confirmed
suspension and VM cleanup. The original executable/inputs and selected checkpoint must have verified
storage copies. The destination imports verified bytes before launching; the original worker and client
can be unavailable. Jobs without an explicit portable runtime retain original-worker resume semantics.
Neither path authorizes lost-worker takeover. VM disks are not backed up by this artifact policy.
Logs and metrics use the separate `history` destination set; their newest unarchived tail remains
worker-local. Input bundles are protected while their job is unfinished or its checkpoints
are retained; rolling checkpoint retention never selects the input bundle.

## Retention and cleanup

By default inputs, checkpoints and final outputs are retained until explicitly deleted or given an expiry.
Jobs can opt into [rolling checkpoint retention](ROLLING_STORAGE.md). [History](DURABLE_HISTORY.md#retention-and-pressure)
defaults to 1 GiB combined logs and 128 MiB metrics, with 30/90-day expiry after logical completion,
plus chunk-count limits. A pin protects an
artifact regardless of expiry. The selected resume checkpoint and latest checkpoint of an unfinished
logical job are also protected. Unpinning cannot bypass those dependencies.

```sh
fleet artifact-retention ARTIFACT_ID --revision 1 --pinned --request-id pin-result
# Replace retention intent: omit --pinned to unpin; omit --expires-at to clear expiry.
fleet artifact-retention ARTIFACT_ID --revision 2 --expires-at 1800000000 --request-id expire-result
fleet expire-artifacts --as-of 1800000000 --request-id retention-sweep-1
# Alternatively, delete immediately using the current revision from artifacts.
fleet delete-artifact ARTIFACT_ID --revision CURRENT_REVISION --request-id delete-result
```

Expiry uses an explicit Unix timestamp so the state-machine decision is deterministic. Invoke
`expire-artifacts` for explicit artifact expiries; history's own completion-age policy is swept
automatically. Deletion first commits a tombstone. Agents then remove owned payloads and report `Deleted`; unavailable agents remain
pending until they return. Deletion is irreversible through Fleet. Source reports cannot resurrect an
artifact. Job/attempt summaries, checkpoint descriptors and user request receipts remain. Fully deleted
history chunks can leave the catalogue; stream cursors and compact storage deletion ranges prevent replay.

Cleanup removes retained storage objects, registered source checkpoint/final-output files, restored
checkpoint files, and their named blob tags. The worker then collects only eligible cache hashes from
committed artifact tombstones. Other retained artifacts, active resume inputs, original input reservations
and in-flight imports protect shared hashes. Unknown cache entries are retained. Cleanup does **not**
erase job directories, old loose logs, initial-input spools or VM image caches. History capture buffers
have their own bounded reclamation after verified backup or committed deletion. Artifact deletion is not a promise
to erase every historical copy from the host or external exports. Rolling jobs release capture slots
after local reclamation; the default cumulative per-attempt limit is not reset by deletion.

## Emergency control-plane recovery

Storage agents stay bound to their original cluster authority until explicitly rebound using the same
administrator-signed recovery manifest as clients/workers. Stop the agent, run
`fleet cluster rebind-storage --home /absolute/storage --manifest /absolute/recovery.json`, then restart
it. Existing files and local deletion tombstones survive. A wrong signature or sibling recovery is
refused, and requests carrying the old authority cannot use the rebound artifact endpoint.

Emergency recovery holds all artifact cleanup, including old pending tombstones, while exports and
backups can continue. After worker reconciliation and `resume-recovery`, inspect `artifacts` and
`storage status`. Copies must report under the new authority. Then explicitly acknowledge the recovered
retention inventory (which may have lost recent pins/deletion decisions):

```sh
fleet storage review-recovery --catalog-fingerprint FINGERPRINT_FROM_STORAGE_STATUS \
  --request-id review-recovered-storage
```

The fingerprint binds approval to the inspected catalog; a changed catalog requires a new review.
Unavailable locations must be named with repeated `--unavailable-location storage/ID` or `worker/ID`.
They remain quarantined from cleanup even after review. Once they return and reconcile, inspect again
and submit a new review without those exclusions. No timed lease or automatic authority takeover can
turn a stale controller snapshot into deletion permission.

## CLI JSON interface and qualification

The normal `fleet api` interface exposes `storage`, `artifacts`, `enroll_storage`, `storage_policy`,
`artifact_retention`, `delete_artifact`, `expire_artifacts`, `fetch_artifact`, and
`review_storage_recovery`. Mutations require an explicit request ID and applicable revision/fingerprint.
Use `retry` after an unknown mutation outcome; do not invent a new request ID to bypass uncertainty.

The current protocol/metadata format is 10. Mixed versions are refused. The explicit offline singleton upgrade
accepts idle formats 5–9; formats 6–9 may include suspended jobs. It preserves checkpoint metadata and
receipts, registers existing checkpoints as source artifacts, and creates versioned backups. It refuses
recovery-bound old-format installations pending a separately reviewed migration. See
[the upgrade procedure](CHECKPOINT_JOBS.md#explicit-format-56789-upgrade).

Rust qualification covers three-replica checkpoint backup/export/resume with the original worker payloads
and blob cache unavailable, leader loss, immutable attempt history, pins/expiry/cleanup, quotas,
authentication, interrupted staging, corruption/peer repair, signed recovery/review, and format-6/7 upgrade.
The JSON CLI/service scenario is also run in CI on Linux and macOS:

```sh
python3 contrib/qualify_checkpoint_tart.py --fake --backup-restore \
  --binary "$PWD/target/debug/fleet" --home /tmp/fleet-control-storage-ci
```

Omit `--fake` and supply `--instances` and `--image-cache` to qualify with real offline Tart VMs. The
backup fixture uses the built-in input-free canary. It checks installed work is idle and verifies its
records are unchanged. Separate homes/processes on one M4 qualify the recovery protocol, **not survival
of physical M4 loss**. That final failure domain still requires a second machine.

See [rolling checkpoint storage](ROLLING_STORAGE.md) for opt-in automatic retention, cache reclamation and timestamped storage visibility.
