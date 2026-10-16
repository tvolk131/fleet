# Rolling checkpoint storage

Checkpoints are independently valuable artifacts: they can support resumption, experiments,
evaluation and reproducibility. Fleet does not assume that only a job's final result matters.
A checkpoint policy is optional. Without rolling retention, Fleet keeps the existing limits
(16 captures and 16 GiB transferred per attempt) and does not automatically delete checkpoints.
Final-output retention remains independent.

An opt-in job declaration:

```json
{
  "checkpoint": {
    "manifests": "checkpoints/*/fleet.json",
    "resume_argv": ["./train", "--resume", "{checkpoint}"],
    "retention": {
      "keep_last": 5,
      "max_live_checkpoints": 8,
      "max_live_bytes": 34359738368,
      "transfer_bytes_per_hour": 68719476736
    }
  }
}
```

`keep_last` retains the newest committed checkpoints of the **logical job**, across attempts.
Ordering is assigned by the replicated state machine, not application progress or machine clocks.
Pins, the latest checkpoint of an unfinished attempt, and selected resume dependencies are protected
in addition to that window. Pin an artifact to preserve it independently. Unpinning can make an old
checkpoint immediately eligible for rolling deletion. Keep or export important research checkpoints
before they fall outside the window. Configure backup destinations independently with `storage policy`.

Every successful state transition reconciles this policy deterministically. Before retiring older
checkpoints, every checkpoint in the replacement window must have Ready copies at its configured
backup targets under the current authority. Backup failure, pin pressure and recovery cleanup holds
can keep more than `keep_last` artifacts. The worker stops capturing if its budget fills; it does not
delete protected files or fail/restart training. Its blocked reason is visible through storage status.
Expiry and explicit deletion remain separate operations with the same pin/resume protections.

Capture admission is per attempt. `max_live_checkpoints` must exceed `keep_last` to provide replacement
headroom. `max_live_bytes` reserves three times the total retained plus next checkpoint payload,
plus 64 KiB for the manifest: source files, content-addressed cache and transfer/staging. It is a
conservative Fleet payload admission budget, not a filesystem quota. Deduplication and APFS clones
may use less physical space. Capture also checks the worker's configured free-disk floor with replacement headroom; unrelated host writers can still consume space. Size the budget for protected checkpoints **and one replacement**.
`transfer_bytes_per_hour` charges manifests, payloads and failed downloads before IO in a durable
one-hour window. Restart cannot reset it; a backward clock step delays renewal. Non-rolling attempts
retain the original lifetime transfer limit.

The controller commits artifact tombstones before any deletion. Workers durably record them, remove
only manifest-owned files and tags, then collect eligible cache hashes. Shared artifacts, active
resume inputs and original input reservations keep their hashes. Unknown cache objects are retained.
New uploads reserve input hashes durably before transfer. Capture slots are released only after
source deletion is acknowledged and the cache entry is gone or held by another reference. A crash
between these steps delays capture safely. Metadata, receipts, lineage and source-path tombstones
remain available; replay cannot resurrect deleted payloads.

`fleet storage status` (JSON API `{"op":"storage"}`) provides the retention decisions, reasons,
worker cache usage, eligible cache bytes, capture usage and blocked reasons. Each worker report has
an observation timestamp; an unavailable worker is explicit. The iced-m3 GUI shows these same reports.
Catalog bytes, logical file lengths and physical allocated bytes are distinct; reports do not imply
the entire machine is under a hard disk quota.

Scope: Fleet-owned host checkpoint copies, final-output artifacts, backup objects and their eligible
cache entries. Logs, metadata, original input spools, Tart images and the application's files inside
the guest have separate lifetimes. The trainer must prune its own guest checkpoint directories; the
guest manifest scan remains bounded to 256 matching files and 100,000 visited entries. A backup on
another directory of the M4 qualifies the protocol but does not protect against loss of that machine.

Rolling storage introduced format 8; [portable jobs](PORTABLE_JOBS.md) introduced format 9; [durable history](DURABLE_HISTORY.md) uses format 10. Upgrade through the explicit offline metadata
migration; mixed-version replicas are refused. Experiments use separate state homes and workloads.
