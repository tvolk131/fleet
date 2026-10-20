# Replicated control plane: real workers

The replicated worker backend runs native jobs, disposable VM jobs, and persistent
VM instances under the replicated control plane. This is the normal Fleet worker architecture;
[the service guide](NORMAL_FLEET.md) covers installation. The explicit-home examples below are for
isolated qualification and do not adopt archived coordinator workloads.

For permanent controller quorum loss, use [lost-quorum recovery](CONTROL_PLANE_RECOVERY.md). It preserves
worker execution history and requires explicit authority rebinding and inventory reconciliation.

## Authority and recovery

An administrator enrolls a worker's public key, stable worker ID, journal incarnation, address, budgets,
and image aliases. Enrollment is immutable in this milestone. Losing/replacing the worker's journal
requires explicit recovery; it is not equivalent to restarting the worker process.

The normal client enqueues a stable logical job. The controller commits placement and a separate
execution attempt when an eligible worker has capacity. `Assign` also supports explicit placement,
including persistent instances. Job construction in both paths uses the same admission validator;
portable jobs require an immutable input bundle and configured storage targets, so they must use
the queue submission interface that carries that bundle. Direct assignment cannot bypass those rules.
`Desire` changes lifecycle intent with an expected revision.
Acceptance means the intent is durable, not that the process or VM is already running.
[Checkpoint continuation](CHECKPOINT_JOBS.md) creates a new attempt on the original worker; it does
not move a live process or VM between hosts.

The worker initiates an authenticated `WorkerSync`. A replica must establish a linearizable read before
returning a snapshot, change batch, continuation page or head confirmation. Workers follow the same authenticated membership discovery as
clients and retain it on disk. Removed replicas have no push-command channel to the worker. An isolated
old leader cannot supply new work without a quorum. This pull design replaces the earlier proposed
leader-to-worker takeover handshake; there is no additional lease or caller-supplied term to trust.

Before touching a backend, the worker commits the accepted intentions to its own redb journal. It then
reconciles durable backend records using stable execution IDs. Its numbered observation outbox survives
restart. The control plane authenticates the enrolled key/incarnation and applies each contiguous report
and cursor atomically. Confirmed cursor progress allows the worker to discard acknowledged outbox entries.
Control-plane report receipts are retained under the existing receipt policy.

### Bounded synchronization

The worker durably saves a complete accepted image and cursor in `worker.redb`, atomically with the
accepted intent and observation acknowledgments. The cursor contains the committed revision and a
content digest. Polls request changes since that image. Unchanged resource records, including old
assignments and managed VM operations, are not retransmitted. Initial synchronization, or an evicted
base, transfers a complete snapshot. Both snapshots and changes are split into 64 KiB byte pages;
even a single large resource can span pages without exceeding the 8 MiB wire frame bound.

Pages belong to an immutable transfer, scoped to the authenticated worker incarnation. The receiver
checks offsets, transfer hash, resulting image hash and monotonic revision. It assembles changes without
executing intermediate entries, then polls the current head again. Changes committed during pagination,
including cancellation or recovery holds, are incorporated before the worker accepts the resulting image.
Missing pages, controller replacement, worker restart and cache eviction cannot authorize partial effects.
A durable pending-sync marker also survives a crash during transfer: on restart the agent waits for a
complete fresh image rather than initiating effects from its older accepted view while disconnected.
Each tick handles at most eight exchanges. Detached supervisors continue while a replacement image is
being downloaded; the agent defers new reconciliation effects until synchronization completes.

Delivery caches are disposable, not a second replicated log: each replica retains at most three recent
images and two transfer bodies per worker incarnation. Eviction or replica restart falls back to a
snapshot. Workers keep their last accepted image across restart and discard incomplete downloads.
Emergency authority rebinding clears the delivery cursor while preserving execution markers and the
existing explicit recovery inventory/hold protocol. Request receipts do not depend on delivery-cache retention.

Wire pages and the synchronization exchange count per tick are bounded; complete projections and transfer bodies still occupy memory
proportional to resource history. Projection construction, local journal reads and archival scans still
hydrate resource metadata. This is not incremental database hydration or removal of historical records.
The existing backend reconciler short-circuits finished execution once its terminal observation is
durable locally. Checkpoint import must still retry for those attempts; artifact/history publication
and reclamation also continue independently of execution completion.

### Diagnostic delivery

Lifecycle diagnostics are normalized to at most 4,096 UTF-8 bytes before change detection, sequence
assignment and outbox persistence. A pre-fix oversized outbox entry is retried unchanged until the
control plane definitively returns its specific diagnostic-size rejection. Only then may the worker
submit the same sequence, assignment, revision and lifecycle outcome with a shortened diagnostic and
a new deterministic request ID. The original rejection receipt remains immutable. Timeouts retain an
unknown outcome and never authorize correction, skipping a sequence or discarding a lifecycle observation.
The corrected request can be retried after lost replies and worker restart.

Accepted workloads continue during control-plane outages, including local completion and cleanup.
Cancellation accepted during an outage takes effect when the worker receives it. Unreachable or uncertain
executions retain their reservations; Fleet never automatically starts a replacement elsewhere.

Native supervisors use both an OS lock and a durable launch-attempt record. Concurrent retrying launchers
cannot execute the command twice. A published job that has not attempted launch is recoverable. Missing backend records after a recorded
execution attempt produce uncertainty; missing journals and unrecognized backend inventory refuse startup. If the
supervisor disappears after recording an attempt but before a durable exit, Fleet reports `Uncertain` and
holds the reservation. This deliberately does not promise exactly-once external effects: an orphan process
may remain, and an operator must investigate it. There is no automatic PID-based orphan kill or force-release
API in this milestone. VM job supervision retains its existing durable guest operations and cleanup rules.

Raw mutation files require `origin`, the control-plane cluster ID at request creation. Preserve that
origin and request ID across retries, including after disaster recovery.

## Resources and isolation

The enrollment budget applies across all real assignments on that worker. Admission counts accepted,
pending, running, offline, and uncertain assignments. Stopped VM disks continue consuming disk reservation;
CPU/RAM reservations are released only after confirmed stop. Disposable VM jobs remain reserved through
VM deletion, not merely guest-command exit. Stop/cancel/delete remain possible when capacity is exhausted.

| Resource | Guarantee |
| --- | --- |
| CPU | Aggregate declared native threads and VM vCPUs fit the worker's slots; no dedicated-core enforcement |
| Memory | Aggregate declared native memory and VM RAM fit the enrollment budget; native declarations are admission accounting, while the VM driver configures guest RAM |
| VM disk | Sum of virtual disk capacities fits the budget; stopped disks still count; existing free-space guard remains active |
| GPU | Native GPU jobs reserve exclusive use; VM policies are exclusive or best-effort shared; every macOS VM has GPU access |
| VM count | At most two active macOS VM reservations; external hypervisor use can still prevent a launch |
| Networking | `offline` only until Fleet's host network agent exists; no LAN access, inbound exposure, host-directory sharing, or allow-rule API |

Native jobs remain trusted host processes. Their memory, CPU and GPU declarations do not provide VM
isolation or hard per-process quotas. Host processes outside this worker are outside its admission ledger.
Do not give two workers overlapping capacity on the same physical host. A separate home prevents state
collisions, not physical resource contention.

Images remain pinned in the worker's trusted `backend/instances.json`. Inputs are content-addressed and
verified before use. Virtual disk accounting is not a physical-storage quota for image caches, job outputs,
or blob stores. [Artifact storage](ARTIFACT_STORAGE.md) supplies explicit retention, backup targets,
and storage-agent quotas for checkpoints and final outputs; it does not impose a host filesystem quota.

## Data plane

The control plane replicates workload specifications, placement, receipts, intent and compact observations.
It does not replicate bulk log, metric-series or artifact bytes. The authenticated worker data protocol
exposes `Status`, `Jobs`, `Instances`, `Logs`, `Outputs`, `Events`, and `Missing`. It exposes no launch,
guest-exec, cancellation or instance-lifecycle command. Those effects require committed assignments.

Existing backend observations supply stdout/stderr by byte offset, progress probes, VM lifecycle details,
and settled intermediate/final output manifests. Inputs and outputs transfer over authenticated iroh-blobs;
fetch verifies content hashes and writes into a fresh destination directory. Intermediate captures remain
available after job cancellation and disposable-VM deletion. Files are independently captured, not an atomic
checkpoint transaction. Registered application checkpoints and final outputs can have independently
verified backup copies. Unbacked bytes, logs, and metrics can still be lost with a worker disk.

The library can query a known worker directly during a controller outage. The current `data`/`fetch` CLI
first resolves the worker from a strong control-plane read, so those convenience commands require quorum.
The normal CLI, Material 3 GUI and JSON interface expose the replicated queue through [NORMAL_FLEET.md](NORMAL_FLEET.md).
The obsolete public clients and standalone scheduler have been removed. All configured administrators can read worker
outputs; per-user ACLs and reader-key rotation are not implemented.

## Start an isolated worker

First build and initialize replicas as described in [the lab guide](QUALIFICATION.md). Keep its
`lab_dir`, client identity, and running replicas. Prepare a CPU-only worker:

```sh
target/debug/fleet cluster prepare-worker --home "$lab_dir/worker" --config "$lab_dir/client/config.json" --id canary --cpu-slots 2 --memory-mb 1024
target/debug/fleet cluster request --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json" --request "$lab_dir/worker/enroll.json"
target/debug/fleet cluster worker --home "$lab_dir/worker"
```

Preparation refuses a populated home and generates a separate key/incarnation and durable journal. It does not enroll or
launch anything. The worker verifies its budget against host capacity. Its `backend/` holds supervision
records and instance state, including each instance's driver directory. `blobs/` and `worker.redb` are independent of installed Fleet.

For VMs, supply `--instances /absolute/path/to/trusted-instances.json` ([format](VM_INSTANCES.md)) to
`prepare-worker`; its memory budget must match `--memory-mb`. The configuration is copied, and VM disks
live in the new home; image bundles stay where the catalog names them.
Preparation records the currently built lab executable as the supervisor; do not remove or replace that
executable while its worker is in use. Replica processes use `serve`. Combined mode adds
`serve ... --worker-home /absolute/path/to/prepared-worker`; both roles still use their own state and keys.

The original worker qualification used loopback addresses. [Multi-machine deployment](CONTROL_PLANE_NETWORKING.md)
adds explicit direct LAN contacts and cross-host failure tests. There is no implicit service installation.

## Submit and inspect

Save a job file compatible with the existing local job-file schema (local inputs are paths):

```json
{"name":"native canary","argv":["/bin/sh","-c","echo hello; echo result > result.txt"],"publish":["result.txt"],"threads":1,"max_seconds":10,"keep_awake":false}
```

```sh
target/debug/fleet cluster submit-job --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json" --data-home "$lab_dir/client-data" --job /absolute/path/to/job.json --worker canary --id example --request-id example-submit --memory-mb 128
target/debug/fleet cluster read --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json"
target/debug/fleet cluster fetch --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json" --data-home "$lab_dir/client-data" --assignment example --destination "$lab_dir/capture-1"
```

`submit-job` durably saves the prepared request under `client-data/requests/` before transfer/submission.
Retrying its request ID uses that saved specification and input hashes, even if the source job file changed.
Use a fresh request/assignment ID for new work. The example's execution ID is `exec-example`; resource and
execution IDs must be valid identifiers of at most 64 characters. `job-` instance IDs are reserved for
backend-managed disposable VMs.

For a disposable VM job, add `"vm":{"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":50,
"network":"offline","gpu":"exclusive"}` to the job file and use a suitably enrolled VM worker.
The VM's declared RAM supersedes the native `--memory-mb` reservation. Use the normal queue and
[checkpoint contract](CHECKPOINT_JOBS.md) for checkpoint suspension and explicit resume; there is no
automatic preemption or live VM migration.

Pass these JSON files to `request` for lifecycle mutations:

```json
{"origin":"CLUSTER_ID_AT_CREATION","id":"cancel-example","mutation":{"Desire":{"id":"example","expected_revision":1,"desired":"Cancelled"}}}
```

```json
{"origin":"CLUSTER_ID_AT_CREATION","id":"create-vm","mutation":{"Assign":{"id":"persistent","execution":"instance-persistent","worker":"canary","workload":{"Instance":{"spec":{"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":50,"network":"offline","gpu":"shared"}}}}}}
```

An instance starts with desired `Running`. Use `Desire` with `Stopped`, `Running`, or `Deleted` and its
current revision. Deletion automatically waits for a confirmed stop before deleting the disk. IDs are never
reused. Guest application execution is currently exposed through disposable VM jobs, not a direct mutable
instance data-plane API.

For logs, save `{"Logs":{"execution":"exec-example","stderr":false,"from":0,"max":65536}}` and use
`data --key ... --config ... --worker canary --request /absolute/path/to/logs.json`. `"Jobs"`, `"Status"`,
`{"Events":{"since":0,"wait_ms":0}}`, and `{"Outputs":{"execution":"exec-example"}}` are other read requests.

## Qualification

The 2026-10-13 qualification record (local hardware qualification record; excluded from the public source package) records 120 passing
Rust tests, eight Python tests, and the successful real M4/Tart canary. All experimental VMs/services
were cleaned up; 88 installed queue rows and 76 worker job records remained unchanged.

`cargo test --features control-plane --test control real_worker` exercises actual native supervisors and
the `fake` VM driver. Tests cover authenticated worker roles, replay and receipt loss,
worker SIGKILL, replica replacement and shrink, quorum outage, cancellation before inputs, launch uncertainty,
CPU/memory/GPU conflicts, report gaps/stale revisions, two shared VMs, and persistent/disposable cleanup.
The full suite also covers the existing native/VM backends and Raft persistence/membership contracts.

`target/debug/fleet-qualify normal-vm` is a separately invoked real-VM qualification tool. It requires an
explicit `--instances` `vz` catalog, refuses an occupied qualification home or unfinished legacy jobs/VMs,
clones the image bundle into its own instance disks, and compares installed job/instance/database hashes afterward. It runs small native and offline
VM jobs, captures live metrics/files, kills a replica leader and worker, cancels the VM job, and creates/stops/
deletes a persistent instance. It cleans up only its own resources. It does not reserve capacity through a
second coordinator; run it only with the physical machine idle and no legacy queue waiting to start.

The current [baseline family](BASELINE.md) uses `fleet/baseline/control/1` and the read-only data ALPN
`fleet/baseline/worker-data/1`. Future releases use explicit [rolling upgrades](ROLLING_UPGRADES.md).
Physical power-failure qualification remains separate from process-failure tests.
