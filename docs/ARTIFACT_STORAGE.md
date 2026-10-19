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
`ArtifactRetention`, `DeleteArtifact`, `ExpireArtifacts`, and `ReviewStorageRecovery`. Agent lifecycle
uses `ReplaceStorage`, `RetireStorage`, `FinishStorageRetirement`, and `DeclareStorageLost`.
`CheckpointReady` also registers the corresponding artifact. History adds `HistoryReport`, `HistoryPolicy`
and `SweepHistory`, reusing the artifact copy/retention commands. Applying these commands changes only
metadata. Authenticated `StorageRead` and `WorkerRead` return quorum-confirmed intent. The storage and
worker reconcilers perform transfers/deletion afterward, then propose versioned observations.
`AuthorizeArtifact` is a quorum-confirmed read that checks the current identities and assignment before
each new transfer; it does not add a request receipt or a consensus command. Disk IO,
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
address, failure domain, quota) is immutable. Prepare a new role/home and use the replacement workflow below.
Retired and lost IDs and public keys remain reserved and cannot be enrolled again, including as workers.
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
the artifact is currently protected. Copy phases are `Pending`, `Ready`, `Missing`, `Failed`, `Deleted`, and `Lost`. `Lost` records explicit
identity revocation; it never means that an unreachable disk was erased or its data restored.
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

## Replace and retire a storage agent

Storage identities have a replicated lifecycle: **active → draining → cleaning → retired**, with a
separate irreversible **lost** state. Each lifecycle has a revision. The normal workflow is:

1. Prepare and enroll a new storage role with a fresh identity, ID, and empty home. Start its service.
2. Run `storage replace OLD NEW --revision REVISION`. Fleet atomically substitutes the new agent in
   future placement and adds it to every retained artifact assigned to the old agent. The old agent
   stays readable while draining. Existing pins, expiry, manifests, jobs and attempts are unchanged.
3. Inspect `storage plan OLD`. Wait until `awaiting_replacement` is empty, then run `storage retire`
   with that plan's revision and inventory fingerprint. The controller checks the current inventory
   again before committing old-copy cleanup. A stale fingerprint or incomplete replacement is rejected.
4. While **cleaning**, the old agent deletes only its own assigned copies and acknowledges each deletion.
   `storage finish-retirement` succeeds only after all acknowledgments under the current authority.
   It permanently revokes the old identity. Stop its service afterward.

```sh
fleet storage plan old-store
fleet storage replace old-store new-store --revision 0 --request-id replace-old
fleet storage plan old-store
fleet storage retire old-store --revision 1 --inventory FINGERPRINT --request-id retire-old
fleet storage plan old-store
fleet storage finish-retirement old-store --revision 2 --request-id finish-old
```

Every retained artifact requires an independent new copy: choosing an existing backup target as the
replacement is rejected when that would collapse two required copies into one. Other configured copies
remain required, including future defaults when the artifact catalogue is still empty. The replacement
must be active and cannot itself drain while an incoming replacement is unfinished. An empty unused
agent may retire without a replacement. Removing an agent from default
policy alone still neither retires it nor deletes its existing copies.

Transfers verify bytes and sizes before reporting Ready. Admission failures, unavailable sources,
corruption, and restarts keep the operation pending; they do not authorize early cleanup. A Ready
observation records verified durable bytes, not a guarantee of current host uptime or future disk
health. Failure-domain labels remain operator declarations. Choose a different machine/disk when
physical host-loss protection is required.

## Explicit lost-agent recovery

When an agent or its disk cannot be recovered, inspect `storage plan OLD` and enroll a fresh
replacement. Then explicitly declare the old identity lost, using the plan's inventory fingerprint
and acknowledging **exactly** its `unavailable_without_agent` artifact IDs:

```sh
fleet storage plan failed-store
fleet storage declare-lost failed-store replacement-store --revision REVISION \
  --inventory FINGERPRINT --unavailable-artifact ARTIFACT_ID --request-id lost-store
# Repeat --unavailable-artifact for every listed ID; omit it when the set is empty.
```

This immediately fences the old identity, routes future placement to the replacement, removes the
old required target, and records old unconfirmed copies as Lost. It preserves manifests, pins,
resume dependencies and attempt history. Repair proceeds from surviving storage or worker copies;
replacement copies remain Pending/Failed until their bytes are actually verified. Reads of unavailable
data fail explicitly. Portable resume continues to require verified inputs and checkpoints. No
metadata override creates bytes or turns missing data into a successful read.

The acknowledged set means **no other current-authority Ready copy is recorded**. A surviving Ready
observation may still refer to an offline host. The plan distinguishes this inventory assessment from
actual repair completion. The loss record retains the reviewed inventory and artifact IDs even after
history chunks later expire and compact. Cleanup of surviving copies still follows ordinary pin,
retention, resume-dependency and emergency-recovery holds; a loss declaration does not bypass them.

An old agent returning with its original disk cannot report Ready, accept new input uploads, or start
new artifact reads. Storage servers and workers authorize every new artifact transfer through a fresh
quorum read, so cached reader lists cannot restore revoked access. Transfers already authorized may
finish; stale observations cannot restore the old placement. New transfers pause without quorum.
Authorization shares the role's authenticated discovery cache, so it follows replica rotations even
when no artifacts were read during the change, including after a cold restart.
Reusing the old disk/credentials is not a rejoin procedure: retain the disk for manual salvage and
prepare a fresh role/home. This milestone does not add automatic salvage/import from a revoked disk.

The lost path also handles a donor disappearing during drain or cleanup. If a replacement destination
fails while another donor is draining toward it, declaring that destination lost redirects the pending
replacement to the fresh agent. A loss declaration never claims physical deletion of the lost disk.
Compact history cleanup can stop waiting for that revoked location, while the lifecycle loss record
retains the explicit acknowledgement.

The JSON CLI exposes `storage_plan`, `replace_storage`, `retire_storage`,
`finish_storage_retirement`, and `declare_storage_lost`. Mutations use saved request IDs and explicit
revisions; use `retry` after an unknown outcome. `storage` includes lifecycle plans and progress.
The iced-m3 GUI shows these states and offers replacement/retirement actions. Declaring loss has a
review panel listing the exact unavailable artifacts; it submits the displayed fingerprint rather
than silently accepting a newer inventory during refresh.

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
and submit a new review without those exclusions. Permanently revoked locations cannot return; naming
them in the review carries their existing Deleted/Lost observations into the new authority, allowing
fully expired history to compact without claiming that a lost disk was erased. No timed lease or
automatic authority takeover can turn a stale controller snapshot into deletion permission.

## CLI JSON interface and qualification

The normal `fleet api` interface exposes `storage`, `artifacts`, `enroll_storage`, `storage_policy`,
`artifact_retention`, `delete_artifact`, `expire_artifacts`, `fetch_artifact`, and
`review_storage_recovery`. Mutations require an explicit request ID and applicable revision/fingerprint.
Use `retry` after an unknown mutation outcome; do not invent a new request ID to bypass uncertainty.

Storage lifecycle is part of [baseline generation 1](BASELINE.md). The separate offline importer
preserves existing artifacts, receipts and worker execution fences.

Rust qualification covers replacement across leader loss, restart and stale access, explicit lost data,
identity reuse rejection, copy-count preservation, backup-only restore, immutable attempt history,
pins/expiry/cleanup, quotas, authentication, interrupted staging, corruption/peer repair and recovery review.
The JSON CLI/service scenario is also run in CI on Linux and macOS:

```sh
target/debug/fleet-qualify checkpoint --fake --backup-restore \
  --binary "$PWD/target/debug/fleet" --home /tmp/fleet-control-storage-ci
```

Omit `--fake` and supply `--instances` and `--image-cache` to qualify with real offline Tart VMs. The
backup fixture uses the built-in input-free canary. It checks installed work is idle and verifies its
records are unchanged. Separate homes/processes on one M4 qualify the recovery protocol, **not survival
of physical M4 loss**. That final failure domain still requires a second machine.

See [rolling checkpoint storage](ROLLING_STORAGE.md) for opt-in automatic retention, cache reclamation and timestamped storage visibility.

The portable CLI scenario exercises replacement and retirement of inputs, checkpoints, logs, metrics
and outputs, then repair from a surviving backup after explicit agent loss:

```sh
target/debug/fleet-qualify portable --fake --storage-lifecycle \
  --binary "$PWD/target/debug/fleet" --home /tmp/fleet-control-storage-lifecycle
```

All homes, keys, services, workloads and fault injection are isolated. This scenario is part of CI on
Linux and macOS; it uses the fake Tart adapter because the behavior under test is storage orchestration.
