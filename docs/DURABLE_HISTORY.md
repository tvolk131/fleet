# Durable job logs, metrics, and attempt history

Fleet archives native and Tart job stdout, stderr, and metric observations through the existing
storage agents. Each logical job retains its immutable attempts: worker, execution identity,
checkpoint lineage, outcome, and separate stream offsets. A resumed job starts new streams at offset
zero; its earlier attempt remains readable.

The controller commits stream cursors, manifests, retention decisions and copy status using Raft/redb.
It does not store log text or metric samples in its consensus database. Workers capture the bytes;
storage agents verify and retain immutable chunks over the authenticated artifact transport. Clients
read verified storage copies first, then available source copies or the worker's newest unarchived tail.
A storage role can share a machine with a worker, but protecting against physical machine loss requires
a copy on a different failure domain. A ready copy is not a promise that the machine is currently online.

## Enable archival and declare metrics

Select history destinations explicitly. With no history destinations, capture is worker-local and
bounded; no backup is claimed. Storage policy updates replace the desired sets, so include the other
artifact destinations you want to keep:

```sh
fleet storage policy --revision 0 --input backup --checkpoint backup --output backup \
  --history backup --request-id storage-policy-1
```

An example job file (add the usual `vm` and input/runtime declarations for a Tart job):

```json
{
  "name": "Training",
  "argv": ["bin/train"],
  "inputs": {"bin/train": "/absolute/path/to/train"},
  "metric_files": ["metrics.jsonl"],
  "history": {
    "log_bytes": 1073741824,
    "metric_bytes": 134217728,
    "log_days": 30,
    "metric_days": 90
  }
}
```

All `history` fields are optional and default to the values above. `metric_files` defaults to empty.
Existing `probes` continue to work: their changed observations are sampled, normally no more than once
per 15 seconds. They are useful for live progress, but do not promise every training step. Declare
append-only JSONL files when individual records matter. Each complete line must be a JSON object:

```json
{"step": 120, "loss": 0.34, "validation_accuracy": 0.81}
```

Fleet records the source path, file identity, generation, byte range and capture timestamp along with
the object. Source offsets and captured bytes commit together locally, so rereading a batch after a
restart does not duplicate records. File replacement/truncation starts a new generation. JSONL files
should remain append-only within a generation; rewriting already-consumed bytes in place is unsupported.
Rapid rotation before Fleet observes a file can lose records. No stdout protocol or job SDK is required.

Limits are four exact relative paths, 16 KiB per JSON record, and 64 KiB/256 lines per source per poll.
Incomplete lines wait for a newline. Oversized, invalid, or incomplete final records produce gap
records. Missing final files and read errors appear in stream status. Native capture drains final
batches over subsequent worker polls. Tart finalization drains at most 16 batches per file before VM
cleanup; a remaining tail is explicitly reported as incomplete. Cooperative suspension allows up to
30 seconds to start final drain batches, with a 35-second worker stop deadline. Cancellation still
stops the VM immediately; unavailable final tails are reported as incomplete. Applications should flush records
periodically rather than writing a huge file only on exit. Original application files have their own
lifecycle; diagnostic retention does not truncate them.

## Read and manage history

```sh
fleet history JOB
fleet attempts JOB
fleet logs JOB --attempt 1 --from 0
fleet logs JOB --attempt 2 --stderr --follow
fleet metrics JOB --attempt 1 --from 0
fleet history-retention JOB --revision 0 --policy /absolute/history-policy.json \
  --request-id retention-1
```

`history` returns policy/revision, each attempt, stream capture status, chunk ranges, pins and copy
states. `logs` returns a `Logs` object; `metrics` returns the page directly. Pages include `data`, `next`,
`done`, `observed`, and an optional `gap`. Offsets count original bytes, not Unicode characters. Pass
`next` to the next request until `done`; a short page is not necessarily EOF. A chunk can be unavailable
because every copy is offline: that is an error, not an empty successful read or automatic cursor skip.
Expired/discarded ranges instead return an explicit gap and the next available offset.

The JSON CLI supports the same operations, including a page size up to 1 MiB (minimum four bytes):

```json
{"op":"metrics","id":"JOB","attempt":1,"from":0,"max":1048576}
```

Metric page boundaries may split JSON lines. Concatenate consecutive page data before parsing JSONL;
discard an incomplete line at an explicit gap. Probe records have `kind: "probes"`, declared file
records `kind: "jsonl"`, and source changes/gaps `jsonl_source`/`jsonl_gap`.

`logs --follow` binds to one attempt when it starts. It ends at that attempt's EOF and prints gap
notices to stderr. It never silently combines a resumed attempt with offsets from an earlier attempt.

The iced-m3 GUI offers **Logs** and **History & metrics** views, attempt selection, archive/capture
status, recent numeric charts, chunk pin/unpin actions and revision-checked retention edits. Charts
start near the most recent 128 KiB and keep a bounded window, rather than loading an entire training
history into the UI. Use CLI pagination for all retained records. It rejects stale responses after
switching jobs, attempts or log streams. The worker notification feed remains a bounded live view;
it is not the archive.

Individual chunks use the existing artifact retention API. Inspect `history` for the chunk ID and
revision, then use `artifact_retention` with `pinned: true` (or `false`) and the intended `expires_at`.
Pins protect retained bytes; they cannot recover an already expired range.

## Retention and pressure

Defaults apply across all attempts of a logical job:

| Stream | Rolling byte budget | Age expiry |
|---|---:|---|
| stdout + stderr combined | 1 GiB | 30 days after logical completion |
| Metrics | 128 MiB | 90 days after logical completion |

Age expiry starts when the controller first observes terminal logical completion during its periodic
sweep, normally within a minute. Suspension does not start that clock. `null` days disables age expiry;
it does not disable rolling byte/count limits. Budgets can be 1 MiB–64 GiB for logs and 1 MiB–4 GiB for
metrics; day values are 1–36,500 or `null`. Policy changes are revision-checked, durable client requests.

Each category also targets at most 1,024 chunks (pins can keep it above that target); a job can have at most 4,096 catalogued chunks
including deletions awaiting acknowledgment. Small periodic chunks can therefore roll before reaching
the byte budget. Both byte and count limits apply while a job is running or suspended. The oldest
unpinned chunks retire first. Pinned chunks survive automatic retention, but consume the budget. If
pins or offline deletion acknowledgments fill it, archival waits and capture can eventually drop new
bytes. Pinning is not permission for unlimited storage. Lowering a budget below pinned usage preserves
those pins and blocks new publication until the operator resolves the pressure.

Workers seal chunks up to 1 MiB, normally at a full chunk, after a minute, or at stream close. Each
attempt/stream keeps at most 32 MiB/256 local blocks; pending archive payloads are limited to about
96 MiB per attempt. A 1 GiB filesystem headroom floor stops diagnostic payload growth. These are
per-attempt bounds and a filesystem safety floor, not a global worker quota or a reservation against
unrelated disk writers. Storage agents additionally enforce their configured capacity. A storage
outage never turns a successful training process into a failed job solely because logs cannot archive.
Collectors keep draining pipes and count discarded bytes. They cannot promise to record the exact
loss count if the filesystem itself stays unwritable through process exit.

Verified storage acknowledgments allow source-cache release; committed deletion allows reclamation.
Retired controller chunk metadata is removed only after every copy acknowledges deletion under the
current authority. Per-stream publication watermarks and compact storage deletion ranges prevent
retries or controller rollback from resurrecting erased bytes. Emergency-recovery cleanup holds still
apply. Samples/chunks do not leave permanent per-message client receipts; internal monotonic cursors
provide idempotency. User mutations retain normal request receipts. Job/attempt summaries remain after
log expiry; this milestone does not add job-record garbage collection.

## Upgrade and qualification

Durable history introduced format 10. The current physical format is **12**. [Rolling upgrades](ROLLING_UPGRADES.md) support adjacent cluster generations with explicit activation. The explicit offline
[upgrade procedure](CHECKPOINT_JOBS.md#explicit-format-567891011-upgrade) accepts idle format 5–11 singleton
state and preserves identities, receipts, jobs and checkpoints. No installed service is upgraded by
qualification. Existing loose log files remain locally readable when available, but are not
retroactively copied into archives. Historical metric records that were never captured cannot be
reconstructed from live probe snapshots.

Coverage includes bounded capture and UTF-8 boundaries, crash-tail repair, JSONL replay/rotation/gaps,
policy revisions, pin pressure, cross-attempt retention, deletion compaction, notification rollover,
format-9 migration, GUI rendering/stale responses, and a three-replica native worker/storage scenario
that reads through the public client after the source goes offline and storage restarts, then expires
payloads while preserving attempts.

The portable canary additionally exercises 9 MiB stdout, 2,000 declared metric records per attempt,
cooperative cross-worker resume, offline source reads, storage/controller restart and retention edits:

```sh
python3 contrib/qualify_portable_tart.py --fake --history --binary "$PWD/target/debug/fleet" \
  --home /tmp/fleet-control-history-ci
```

Without `--fake`, supply a pinned `--instances` configuration and `--image-cache`. This creates two
separate scratch worker identities and sequential real Tart VMs. It refuses active installed jobs,
checks installed/legacy records before and after, and never imports production credentials into the
scratch cluster. CI runs the fake backend on macOS/Linux; real Tart needs a qualified Apple Silicon
host. Same-host tests demonstrate process/role failures and portability, not physical host-loss survival.
