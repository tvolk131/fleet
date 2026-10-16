# Fleet 0.2: replicated queue, clients, and services

This release uses fresh state and one qualified software version across the cluster. It does not import
SQLite, adopt legacy jobs, or support mixed old/new workers. The separately installed legacy Fleet and
its files can remain available while existing work finishes. Do not assign the same physical capacity
to both schedulers.

## Roles and homes

- A controller home contains its private identity, configuration, and redb database.
- A worker home contains its own identity, immutable enrollment, durable journal, blobs, backend records,
  and optional Tart disks/cache. It receives committed intentions through worker-initiated polling.
- A client home contains its administrator identity, cluster configuration, discovered membership,
  prepared requests, and immutable input copies. The default is `~/.fleet-client`.
- An optional client-staging service retries delivery of saved inputs to eligible workers. It has no
  scheduling authority. Already staged work does not depend on this helper remaining online.

A service can run controller and worker roles in one process. Their identities and state remain separate.
A process failure can interrupt both roles; detached workload supervisors retain their own lifecycle.
Keep release binaries at stable, versioned paths because workers record their supervisor executable.

## Fresh setup

Build on each target OS/architecture:

```sh
cargo build --release --locked --bin fleet                 # headless node
cargo build --release --locked --features gui --bin fleet  # desktop client
```

Prepare a new initial cohort on an administrator machine. Addresses are examples; use stable LAN addresses
and unused UDP ports. Preparation creates keys/configuration, not running services:

```sh
fleet cluster init --home /absolute/new-seed --replicas 3 \
  --address 192.0.2.11:28900 --address 192.0.2.12:28900 --address 192.0.2.13:28900
```

Distribute only `node-N/` to its intended controller over authenticated SSH, retaining directory mode 0700
and key mode 0600. Keep the administrator key on the client machine. A matching cluster ID alone does not
establish trust: public replica identities are pinned. Protect the seed directory, which initially holds
all keys. Do not place durable deployment state in `/tmp`.

```sh
fleet --home /absolute/client-home client import \
  --config /absolute/new-seed/client/config.json --key /absolute/new-seed/client/node.key
fleet cluster prepare-worker --home /absolute/worker-home --config /absolute/public-cluster-config.json \
  --id m4pro --bind 192.0.2.11:28910 --cpu-slots 12 --memory-mb 40960
fleet --home /absolute/client-home enroll /absolute/copied-worker-enroll.json
```

Worker preparation writes `enroll.json`; transfer that public file to the administrator. Pass
`--instances /absolute/trusted-instances.json` to prepare a Tart worker. The pinned image catalog and
memory budget must match enrollment. The worker reports platform/architecture/Metal capability tags.
Use explicit worker selection or required tags for OS-specific binaries. A capability tag is an
eligibility declaration, not hardware isolation.

## Persistent services

Prepare a configuration on each host. Only one initial controller uses `--bootstrap`; on subsequent
service restarts it observes the existing database and does not reinitialize it. Prepared replacement
replicas must join through membership management and must not bootstrap.

```sh
fleet service configure --config /absolute/node-service.json \
  --controller-home /absolute/node-home --worker-home /absolute/worker-home --bootstrap
fleet service run --config /absolute/node-service.json
```

Omit the controller or worker argument for a single-role process. An optional `--client-home` runs the
input-staging helper; it belongs on the machine holding the client input spool. Every home must be absolute; role homes must be separate and non-overlapping.

Render an installation definition, then install it with the repository helper:

```sh
fleet service render --kind launchd --name local.fleet.node --user fleet \
  --user-home /Users/fleet --executable /absolute/release/fleet \
  --config /absolute/node-service.json --output /absolute/local.fleet.node.plist
sudo contrib/install-service.sh launchd local.fleet.node /absolute/local.fleet.node.plist
```

On Linux, use `--kind systemd --name fleet-node` and a `.service` output, then
`sudo contrib/install-service.sh systemd fleet-node /absolute/fleet-node.service`.
These are system services running as the selected regular user. The installer refuses the legacy
agent/coordinator labels and refuses to overwrite a different service definition. It preserves detached
supervisors on service restart. Mac services must be qualified in their actual launch context without
an open SSH session; a successful foreground launch is insufficient evidence of unattended LAN access.

## Queue and client contract

```sh
fleet --home /absolute/client-home submit /absolute/job.json --request-id experiment-1 \
  --id training-1 --memory-mb 8192 --gpu --requires os:macos --requires gpu:metal
fleet --home /absolute/client-home queue
fleet --home /absolute/client-home job training-1
fleet --home /absolute/client-home cancel training-1
fleet --home /absolute/client-home requests
fleet --home /absolute/client-home retry experiment-1
```

The job JSON holds `name`, `argv`, local `inputs`, `publish`, `threads`, time limits, probes and optional
`vm` and `checkpoint`. See [checkpoint-resumable VM jobs](CHECKPOINT_JOBS.md) for the manifest contract, attempt history, and explicit format-5/6 metadata upgrade. CPU threads default to one for native jobs; a VM uses its configured vCPUs. Native memory is an
admission reservation, not a per-process limit. Native GPU use must be declared with `--gpu`; it requires
an advertised compute GPU and reserves exclusive Fleet GPU use. All macOS VMs still have GPU access.
VM CPU/RAM/disk reservations and exclusive/shared GPU policy use the existing backend admission rules.

Higher `--priority` runs first; equal priority uses durable submission order. The scheduler skips work
that does not fit, so an oversized job does not block all smaller jobs. `--after JOB` requires that logical
job to succeed; failed dependencies remain visibly blocked until explicitly cancelled. `--worker ID`
pins eligibility; repeat `--requires TAG` for capability requirements. No preemption or automatic rerun is
implied. Assignment is immutable; missing worker contact never releases its reservation or moves its work.
`fleet drain WORKER` stops new queue placement, and `fleet drain WORKER --resume` permits it again.

The client saves the prepared payload and copied input hashes before first transmission. Reusing an ID
with different arguments fails. Retry uses the saved payload even if original input files changed. The
server's committed receipt is authoritative; a lost response is unknown until inspected or retried.
Cancellation acceptance means committed intent, not confirmed termination. The worker must report
completed cleanup before its capacity becomes available again.

Metadata can be accepted while inputs await delivery. Submission attempts staging immediately;
`fleet stage [JOB]` retries it and `fleet stage --watch` keeps retrying. A configured client-staging service
provides the same behavior after the submitting CLI exits. If that source machine is offline before any
eligible worker has the bytes, the job waits. Controllers do not replicate file bytes. Configure separate [storage agents](ARTIFACT_STORAGE.md)
for verified artifact backups. [Portable Tart jobs](PORTABLE_JOBS.md) require durable executable/input
bundles on storage before execution and can resume on compatible workers after confirmed suspension.
Other jobs stage inputs directly to workers. Retain the client spool until staging is complete.

`fleet logs JOB --follow`, `fleet outputs JOB`, and `fleet fetch JOB /absolute/new-directory` read directly
from the assigned worker after resolving it through the cluster. Fetch verifies hashes and requires a
fresh destination. These convenience commands currently need quorum to resolve the worker. Progress events
and logs stay out of Raft. The GUI polls the shared client API, displays progress/series and stale-view
errors, and never schedules or cancels work simply because the window closes.

## JSON automation interface

`fleet --home /absolute/client-home api` accepts one tagged JSON request on stdin and emits one JSON
response on stdout. Any language can invoke it. The Rust client owns authentication, discovery, saved
request payloads, retries, input staging, and verified output transfers; there is no public Python SDK.
The CLI and GUI use this same Rust implementation.

For example, submit a job, inspect it, and retry a saved request using these stdin payloads:

```json
{"op":"submit","request_id":"experiment-1","id":"training-1","memory_mb":8192,"job":{"name":"training","argv":["bin/train"],"inputs":{"bin/train":"/absolute/train"},"publish":["out/*"],"keep_awake":false}}
```

```json
{"op":"job","id":"training-1"}
```

```json
{"op":"retry","request_id":"experiment-1"}
```

Submission options such as `worker`, `memory_mb`, `gpu`, `requires`, `after`, and `priority` belong to the
outer request. Workload fields such as `argv`, `inputs`, `vm`, and `checkpoint` belong inside `job`.
For JSON requests, use absolute local input paths and list individual files. Persist an explicit job ID
and request ID before submitting; request IDs identify operations, not execution attempts. The complete
request schema is `ApiRequest` in `src/control/client.rs`.

Successful calls return `{"ok":true,"result":...}`. Mutation results contain a `reply`, such as
`{"Accepted":{"job":"training-1","revision":1}}` or `{"Rejected":{"reason":"..."}}`.
An `ok` envelope means the call returned a result; inspect the reply to determine whether the mutation
was accepted. `RecoveryUnknown` means the old request history is unavailable after emergency recovery;
it does not authorize repeating the operation under a new request ID.

Client errors return `{"ok":false,"error":"...","outcome_unknown":true,"request_id":"experiment-1"}`
(with `outcome_unknown` false when appropriate). Unknown outcomes require inspection or retry of the
saved request. A nonzero exit, interrupted process, or incomplete response also cannot prove that a
mutation failed. Invalid JSON/schema input can exit nonzero with diagnostics on stderr, without an envelope.
Do not infer mutation success from the process exit code alone.

Cancel/suspend/resume requests take `expected_revision` from a preceding `job` read and their own explicit
`request_id`. For example:

```json
{"op":"cancel","id":"training-1","expected_revision":1,"request_id":"cancel-training-1"}
```

On a revision conflict, inspect the current state before deciding on a new operation. Retry an unknown
operation with `retry`, preserving its original payload and revision. Poll `job` to observe completion;
`succeeded`, `failed`, and `cancelled` are terminal, while `suspended` remains resumable. Stopping a polling
loop does not cancel the job. Logs, outputs, and fetch accept an optional `attempt` number.

## Instances and cluster administration

```sh
fleet instance create my-vm --worker m4pro --spec /absolute/vm.json
fleet instances
fleet instance stop my-vm
fleet instance start my-vm
fleet instance delete my-vm
fleet workers
fleet status
fleet replicas
fleet replicas --add /absolute/node-4-peer.json --remove 2
fleet operation OPERATION_ID
```

Use `fleet cluster prepare-replica` on a new host to generate its public `peer.json`, and start its service
without bootstrap before admitting it. Add two peers to grow one voter to three; remove two voters to shrink
three to one. Replace a voter with one addition and removal in one request. Membership remains a durable
operation requiring the current quorum. `fleet operation` reports progress after acceptance.

Emergency recovery retains the operator fencing and explicit authority transition from the recovery guide.
Use `fleet cluster recover` with a stopped survivor, administrator key and both explicit acknowledgments;
then `fleet accept-recovery /absolute/recovery.json`, rebind each stopped worker with
`fleet cluster rebind-worker`, restart it, and use `fleet resume-recovery` after inventory reconciliation.
An unavailable worker can be explicitly quarantined with `--unavailable-worker ID`. Missing old receipts
produce `RecoveryUnknown`, not automatic reexecution. Imported worker jobs appear in the normal job view.

## Handoff and limits

Before handing a machine over, verify the old queue and worker have no running, paused, pending or uncertain
work, and preserve its old binary, service definitions and data. Stop its old worker from accepting future
work before starting the new worker's capacity. Do not remove historical job files or Tart disks. A completed
old job remains historical; importing it is not required for new scheduling.

Qualify fresh homes first, then stage each idle machine. Recheck immediately before stopping services because
another client may have submitted work since the earlier audit. A replica-only deployment can proceed while
an old worker remains busy. Reverting to the old installation also requires draining the new worker first.

Cross-worker checkpoint migration, preemption, direct persistent-guest commands, per-user ACLs,
background output fetching to remembered destinations, shared blob-cache vacuum, and mixed-version upgrades
are not in this release. Receipts are retained indefinitely unless explicitly released through the low-level
administrative contract; automatic 90-day pruning remains future work. Metadata/snapshot size limits from
the qualification remain applicable. Existing backend guarantees do not make CPU/GPU scheduling exclusive
against unrelated host processes.

The desktop GUI uses `iced-m3` Material 3 components, follows system light/dark appearance, and uses the same durable request journal and replicated API as the CLI and JSON interface. Closing the window never cancels a job.

## Qualification and rollout status

See the [local scenarios and CI guide](LOCAL_ROLLOUT.md) for reproducible automated checks. Real hardware deployment records are retained locally.
