# Permanent request receipts and indexed metadata

Fleet retains a request's identity indefinitely. There is no expiring request namespace, automatic
receipt TTL, or permission to execute an old request again because its response was discarded.
The key is the existing `(authenticated principal, request ID)` pair. Its record contains the
original authority incarnation, mutation fingerprint, decision, resource reference and applied
Raft index. A conflicting payload or origin under that key is rejected. An explicit `Release`
can discard the command and response bodies; the remaining record still prevents replay and
returns `Expired` with its resource reference. Payload retention is independent: logs, metrics,
checkpoints and VM transfer contents retain their existing policies.

## Where the abstractions live

* **Request receipts** answer whether a particular request was accepted or rejected. They are
  consensus state in each controller's redb database, retained across Raft log compaction.
* **Resources** describe jobs, attempts, instances, checkpoints, artifacts and operations. These
  records change as work progresses; an acceptance receipt is not a live execution status.
* **Worker execution markers** record that an execution may have been attempted. They live in
  the worker's own redb journal and are durable before a backend launch. Missing backend records
  after that point produce an uncertain/interrupted outcome, not a second launch. Only an
  existing, proven pre-publication failure path may clear a native attempt marker.
* **Blobs** hold file contents separately, with their existing hashes, replicas and retention.

## Layout and atomicity

Compatibility generation five activates physical metadata layout 14. The application State format
remains 12: physical layout, wire compatibility and application behavior are separate concepts.
The `meta` table holds identity, version, membership and a resource header. `state_rows` holds keyed
jobs, assignments, checkpoints, workers, membership operations, producer cursors, VM operations,
artifact records and history catalogs. `request_receipts` holds permanent receipts. Changed resource
rows, affected receipts, the header and `last_applied` commit in one immediate-durability transaction.
Multiple commands in one Raft apply batch can see each other's receipts, including releases.

The deterministic resource state machine remains shared with the in-memory model tests. Its
cross-resource scheduling and retention checks still hydrate the resource catalog. This milestone
removes request-history-sized reads and rewrites from that path; it does not claim that scheduler
work is independent of the number of jobs or artifacts. Only changed resource rows are rewritten.
Existing VM metadata admission limits and worker-view size limits still apply.

Workers migrate their native and guest execution markers into `execution_markers`, keyed by
execution domain and ID. Migration and the worker's accepted generation commit together. Subsequent
marker lookups/writes avoid loading marker history. Worker resource reconciliation still uses its
existing journal and checks against regressing assignments, outcomes and acknowledgements.

## Reads

At generation five the normal client/GUI resource view excludes receipt history, and a job lookup
returns that job's view instead of every job. Raw resource/receipt point reads and bounded pages are
available through the JSON CLI. All network queries require administrator authentication and a
quorum read barrier. Inspect only selects the compatible protocol; it does not authorize an action
or substitute for that barrier.

```sh
fleet --home /absolute/client-home api <<'JSON'
{"op":"metadata","query":{"kind":"stats"}}
JSON
fleet --home /absolute/client-home api <<'JSON'
{"op":"metadata","query":{"kind":"get","collection":"jobs","key":"training-1"}}
JSON
fleet --home /absolute/client-home api <<'JSON'
{"op":"metadata","query":{"kind":"receipt","principal":"CLIENT_PUBLIC_KEY","request_id":"submit-training-1"}}
JSON
fleet --home /absolute/client-home api <<'JSON'
{"op":"metadata","query":{"kind":"page","collection":null,"cursor":null,"limit":100}}
JSON
```

For pages, `collection: null` selects receipts; a named resource collection selects resource rows.
Pass the returned `next` object unchanged as `cursor`. Pages allow 1–256 entries and approximately
2 MiB of encoded values/keys. A record that cannot fit produces an explicit error; use its point
query or offline export. The transport's existing 8 MiB frame bound still applies to point reads.
New commands are limited to 7 MiB including their authenticated principal, leaving room for Raft
envelopes. Oversized commands are refused before acceptance. If a replication batch exceeds the
frame bound, the transport asks OpenRaft to retry smaller batches without changing log identities.
Multi-entry batches target at most 1 MiB to avoid spending heartbeat deadlines on large transfers.

These are **live keyset pages**, each with its observed applied index. They do not freeze the entire
database between calls. Concurrent insertion before the cursor may not appear, and an updated
resource or released receipt is observed as of the page that reads it. Cluster/collection mismatch
is rejected, including after emergency authority replacement. Use a snapshot/backup for a consistent
complete image. The legacy full `cluster read` export remains for small diagnostics and qualification;
it is intentionally not the normal request-history access path and remains frame-size bounded.

## Snapshots, recovery and upgrades

Generation-five Raft snapshots stream a header and sorted, length-bounded records through private
unlinked temporary files. The digest covers the entire file. Redb stores snapshot data in 64 KiB
chunks rather than one serialized byte vector. Snapshot capture uses a consistent read transaction;
publication cannot replace a newer installed snapshot. Installation validates identity, generation,
digest, ordering, duplicate keys, resource types and Raft metadata before committing replacement
tables and snapshot metadata together. The current safety bounds are 16 MiB per encoded record and
64 GiB per snapshot. Disk must have room for the temporary file, replacement database pages and
retained snapshot; streaming bounds memory, not disk usage or the duration of an install transaction.
Snapshot RPCs have a separate two-minute installation deadline. Half-open streams and ordinary
server requests retain their five-second bounds; heartbeat/election timing is unchanged.

Install the binary on replicas first, then prepare and activate generation five using the existing
rolling-upgrade commands. Before activation, both the state and saved snapshot remain readable by
the previous 3..4 executable. Activation atomically copies existing receipts/resources into the
indexed tables. After activation, older executables refuse the physical format before writing.
Workers switch marker layout when they accept generation five. The pinned previous-release fixture
qualifies rollback before activation and refusal afterward, including live work and snapshots.

Emergency recovery exports the complete applied image, including permanent receipts, and preserves
the existing authority-transition rules. A known old-origin request returns its previous result;
an absent old-origin request remains unknown and cannot become new work. The current explicit
offline recovery image API materializes that complete image in memory; normal Raft snapshot
transfer and routine reads do not. Large-history recovery operators must provision memory for that
offline export. A future streaming recovery-manifest API can remove this remaining export cost
without changing receipt identity or retention.

## Qualification

`tests/control/indexed.rs` covers activation, atomic abort, duplicate/conflicting/released retries,
pagination, restart, snapshot restore and recovery receipt preservation. A three-replica test
transfers a snapshot larger than 64 MiB over iroh, then loses its leader and retries a request;
large catch-up batches must split instead of retrying an oversized frame forever. Normal CI runs
this scenario and the 10,000-receipt case on both platforms, plus the million-receipt case on Linux.
The explicit large-history run is:

```sh
cargo test --locked --test control indexed::indexed_million_receipts -- --ignored --nocapture --test-threads=1
```

It builds one million distinct decision receipts in temporary databases, reports insertion,
lookup, reopen, snapshot and restore times, and verifies restoration beyond the old 64 MiB snapshot
ceiling. Neither fixture uses a normal Fleet home or starts training workloads.
