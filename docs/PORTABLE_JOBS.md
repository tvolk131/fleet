# Portable Tart jobs

Portable jobs keep one logical job identity and immutable attempt history while resuming a suspended
macOS ARM workload on another eligible worker. Executables and declared inputs are job-owned artifacts,
with verified storage copies required before the first attempt starts. Checkpoint bytes must also have
verified storage copies before resume. Payloads travel directly between client, storage and workers;
Raft stores manifests, compatibility requirements, placement, retention and observations.

## Configure compatibility and storage

Workers opt approved image aliases into portable execution in their instance configuration:

```json
{
  "images": {"macos": "registry.example/macos@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
  "platforms": {"macos": {"os": "macos", "arch": "aarch64", "capabilities": ["metal"]}}
}
```

This fragment accompanies the existing Tart paths and resource budgets. Use the real pinned digest of
your approved image. `prepare-worker` advertises these runtimes; the worker checks its local catalog
again at launch. Worker enrollment is immutable: changing the runtime catalog requires a new worker
registration/home. A job requires its image alias on the destination, with the same digest and platform;
required capabilities must be a subset of those declared for that image. Capability names are trusted
operator declarations, not hardware attestation or minimum performance guarantees. The guest helper
also verifies macOS/ARM before executing the application.

Enroll storage as described in [artifact storage](ARTIFACT_STORAGE.md), then set the complete policy:

```sh
fleet storage policy --revision CURRENT_REVISION --input backup-1 --checkpoint backup-1 \
  --output backup-1 --request-id portable-storage
```

Repeated options select multiple targets. All configured targets must report verified copies for the
relevant input bundle or selected checkpoint. Defaults are empty. Submission of a portable job is
rejected until input storage is configured; configure checkpoint storage before trying portable resume.
A storage agent on the same physical machine protects against worker-home loss but not host/disk loss.

## Submit and resume

Add an explicit runtime to an ordinary VM job file:

```json
{
  "name": "Portable training",
  "argv": ["bin/trainer", "start", "dataset"],
  "inputs": {"bin/trainer": "/absolute/trainer", "dataset": "/absolute/dataset"},
  "threads": 2,
  "vm": {
    "image": "macos", "cpus": 2, "memory_mb": 6144, "disk_gb": 50,
    "network": "offline", "gpu": "exclusive",
    "runtime": {
      "platform": {"os": "macos", "arch": "aarch64", "capabilities": ["metal"]},
      "image_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  },
  "checkpoint": {
    "manifests": "checkpoints/*/fleet.json",
    "resume_argv": ["bin/trainer", "resume", "{checkpoint}/state"]
  },
  "publish": ["result.json"]
}
```

Make the source executable executable before submission. Fleet imports immutable bytes and preserves
executable flags in the job specification. Declare every file needed to restart, including dynamic
libraries or other dependencies that are not supplied by the pinned guest image. Inputs use relative,
non-overlapping paths: up to 256 files, 1 GiB each and 4 GiB total. This first implementation does not
provide dataset streaming, directory packaging, VM-image archival or a package manager.
The combined original inputs and selected checkpoint must also fit the 4 GiB VM staging limit;
an oversized combination is refused before a resume attempt is created.

```sh
fleet submit /absolute/job.json --id training --request-id training-submit
fleet stage training
fleet job training
fleet suspend training --request-id training-suspend
# Wait until the logical job reports suspended, confirming old VM cleanup.
fleet resume training --worker another-worker --request-id training-resume
```

`stage --watch` or a client-staging service retries initial upload. Before durability is confirmed, keep
the submitting client's input spool available. Afterwards the source paths and client can be unavailable.
Workers verify downloaded hashes before launching, restore executable flags from the immutable spec,
and retry interrupted transfers without starting a partial job. Empty input bundles are supported for
programs supplied entirely by the pinned image.

Omit `--worker` on resume to let the scheduler choose an eligible worker. An original submit-time worker
pin remains a pin; resume cannot override it. Required tags, resource budgets, drain state, compatibility
and recovery restrictions also remain in force. An explicit target can wait for capacity or a worker
poll; acceptance does not mean execution has started. The JSON `resume` request accepts optional
`worker`, plus the existing explicit `request_id` and `expected_revision`. Request retry returns the
same outcome and never creates another attempt.

`job` JSON exposes `portable`, `input_bundle`, `portable_ready`, `portability_waiting`, `resume_worker`,
and `resume_targets` (worker to blocker, or null if eligible). The iced-m3 GUI shows input/checkpoint
readiness and compatible resume targets. Scheduling still needs a live worker poll; eligibility alone
is not a liveness guarantee.

## Ownership, cleanup and limits

Input manifests belong to the logical job (`origin: {"Job": "training"}`); checkpoint and output
manifests belong to their producing attempt. The immutable job specification and executable flags live
in the replicated database. The checkpoint protocol and [rolling retention](ROLLING_STORAGE.md) remain
application-level: Fleet restarts the declared program with saved files, not a snapshot of VM memory.

Input bundles are protected while the job is unfinished or any of its checkpoints is retained. They
are never swept by rolling checkpoint retention. After those dependencies end, explicit deletion or
expiry can reclaim storage and restored worker payloads. Tombstones remain durable and cleanup waits
for unavailable agents to return. Client spools, logs, job metadata and VM image caches retain their
separate lifecycle; deleting an artifact does not erase every historical copy.

Cooperative resume requires a confirmed suspended attempt and deleted VM. A lost or unreachable worker
is not permission to run a second copy. This milestone adds no forced takeover, automatic failover,
automatic retry of a failed job, cross-platform checkpoint conversion or exactly-once external side
effects. Jobs without `vm.runtime` retain the existing worker-local resume behavior. Native jobs do not
use this portable contract. Compatibility is deliberately conservative: a different image digest is
rejected even when it might happen to work. Approved image availability remains an external dependency.

## Upgrade and qualification

Protocol/storage format is 9. Use the explicit offline [metadata upgrade](CHECKPOINT_JOBS.md#explicit-format-5678-upgrade)
from formats 5–8; mixed binaries are rejected. Migration preserves old artifact IDs, suspended jobs and
retained request receipts. It does not retroactively turn old jobs into portable jobs.

CI runs the Rust protocol/state-machine and headless GUI tests plus the JSON CLI scenario:

```sh
python3 contrib/qualify_portable_tart.py --fake --binary "$PWD/target/debug/fleet" \
  --home /tmp/fleet-control-portable-ci
```

For real Tart, omit `--fake` and supply `--instances /absolute/instances.json` and
`--image-cache /absolute/tart/cache`. The fixture uses separate controller, storage and two worker homes,
clones the approved image cache, and refuses active installed jobs. It suspends and deletes the source
VM, makes the original worker and client unavailable, restarts controller/storage, resumes on a fresh
VM on the second worker, verifies results and idempotency, then checks dependency retention and cleanup.
It never upgrades the installed service. Two workers on one Mac test role separation and transfers;
independent physical-machine failure qualification remains separate.
