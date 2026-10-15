# Fleet distributed control plane design

Current implementation: [normal clients/services](NORMAL_FLEET.md) and [logical jobs, attempts, checkpoints](CHECKPOINT_JOBS.md). The target-design history below predates those milestones.

Target design, updated 2026-10-13. The first isolated implementation is described in
[CONTROL_PLANE_LAB.md](CONTROL_PLANE_LAB.md), including its qualification and remaining work.
The isolated [membership milestone](CONTROL_PLANE_MEMBERSHIP.md) implements replica replacement and
single↔three voter transitions. The [real-worker milestone](CONTROL_PLANE_WORKERS.md) adds authenticated
worker polling, native/Tart execution and observations. [Lost-quorum recovery](CONTROL_PLANE_RECOVERY.md)
adds a signed authority transition and conservative worker reconciliation. The
[multi-machine milestone](CONTROL_PLANE_NETWORKING.md) qualifies direct LAN operation, failover,
membership changes and recovery across the M4 Pro, M1 Pro and Pi; production cutover remains separate.

Fleet will provide one logical control plane whose metadata survives individual controller failures.
The selected architecture is embedded Raft over iroh, with redb storing each replica's log and state.
Clients discover and fail over between replicas internally. Workers keep managing accepted workloads
when the control plane is unavailable.

This is the full target design, not a claim that every part is implemented. The first milestone is an
isolated prototype that qualifies persistence, request replay, and worker reconciliation before
production cutover. Existing training runs and their services remain outside that experiment.

## Decisions and scope

- One binary supports worker only, control plane only, or both in the same process. The roles have
  separate modules, durable state, and lifecycle management. Running both together is supported.
- A stable logical cluster ID identifies Fleet independently of its current replica keys. Disaster
  recovery adds a fresh wire incarnation while retaining that logical identity in its signed lineage. Each replica has
  its own iroh identity. Client, worker, and replica authorization are distinct.
- redb is the selected local database. OpenRaft is the intended consensus implementation, subject to
  the storage and transport qualification below. Dependency versions will be pinned during that work.
- This is a breaking release. Old client/worker protocols, SQLite coordinator state, and worker record
  formats impose no compatibility requirement. The initial cluster starts with fresh state and one
  qualified software release. Existing installations and historical data are preserved separately.
- The same protocol supports a single voter or several voters. Qualification starts with one and
  three; the model does not encode a fixed cluster size. Membership changes require the current
  cluster's consensus. A disconnected minority cannot shrink itself into a new authoritative cluster.
- Initially the leader schedules and dispatches. Every global mutation goes through the replicated
  state machine. Applying state never launches a VM, performs network I/O, or writes client files.
- The first integration covers native jobs and Tart instances/jobs through new management contracts.
  Reuse backend mechanics where they fit; historical API shapes and incidental implementation limits
  are not requirements. Tart isolation and resource-admission guarantees remain requirements.
  Separating logical jobs from executions does not add VM checkpoint resume.
- An unreachable worker keeps its assignments and reservations until reconciled. Automatic execution
  elsewhere requires a later design for confirmed termination or enforceable execution authority.
  Recovery requires explicit fencing and authority rebinding; elapsed time is not takeover authority.

## Roles and data flow

The client library connects to a trusted replica and follows authenticated leader hints. The leader
commits requests, applies them locally, and returns durable results. Followers apply the same committed
commands. Workers pull committed intentions from a replica that establishes a linearizable read, and submit
observations for commitment. Placement automation can later produce assignments through the same contract.

| Component | Responsibility |
| --- | --- |
| Client library | Request identity, upload/download, replica discovery, retries, watches |
| Replicated state machine | Resources, reservations, desired state, receipts, observations, membership-related application metadata |
| Control-plane handlers/reconcilers | Serve quorum-confirmed assignments, commit worker events, reconcile membership |
| Worker | Local admission, persistent intent, supervision, VM lifecycle, output capture, observations |
| Blob storage | Input/output/checkpoint bytes, verified independently from metadata |

In combined mode, worker observations still cross the same logical interface; colocation grants no
shortcut around consensus. A shared process failure can interrupt both roles. Existing detached
supervisors remain responsible for the survival of workloads they support.

## Identity and client contract

The main identities are `cluster_id`, `replica_id`, `worker_id`, `client_id`, `request_id`, `job_id`,
`execution_id`, `instance_id`, and `operation_id`. Names are display labels. A logical job may have
several executions over its lifetime; an execution has one fixed worker assignment. IDs of deleted or
completed resources are not reused. Restarting a persistent instance creates a new runtime incarnation.
Dependencies target logical jobs and their outcomes, not individual execution segments. Suspending an
execution does not by itself resolve its logical job's dependent work. The old `parent`/`continued_by`
chain need not be represented as separate public jobs in the new model.

Clients bootstrap with the cluster ID and trusted replica identities/address hints. Subsequent
membership discovery must be authenticated and bound to that cluster; a matching cluster ID alone
does not establish trust. A replica's transport identity does not automatically grant voting rights.

The mutation envelope contains a protocol version, current wire cluster ID, immutable request `origin`
(the wire incarnation at creation), request ID, command, and an
optional expected resource revision. The server derives the client identity from authentication.
The client persists its request ID and prepared payload before first submission so process restart
can retry the same operation. The server fingerprints canonical command contents, including verified
artifact references, rather than unstable transport encoding or client filesystem paths.

| Response | Meaning |
| --- | --- |
| Accepted | The request and its result are committed and applied; resource/operation IDs identify further progress |
| Rejected | A definite semantic decision, such as a revision conflict; committed semantic decisions are replayable |
| Unavailable or redirect | Retry through the client library; this is not a stored rejection of the operation |
| Transport timeout | Outcome unknown; inspect or retry the same request ID |
| Recovery unknown | The request belongs to an earlier incarnation and its receipt is absent; it is not reexecuted |
| Result expired | The request was processed, but its full receipt was pruned; it will not execute again |

Authentication and malformed-message errors can be rejected before consensus. A valid command whose
state-dependent precondition fails records that decision in the state machine. Reusing a request ID
with different contents fails, including after pruning. Replaying a receipt still requires current
authorization to access it.

Acceptance is distinct from completion. A cancellation can be accepted while its job is still running
on a disconnected worker. Operation records track reconciliation separately from immutable request
responses. Cancelling a client wait does not cancel its submitted operation.

Public operation groups are jobs, instances and guest operations, observation, artifacts, and cluster
administration. Design these interfaces around the new resource model; legacy method names, wire
variants, and response shapes are not constraints. All mutations use the same request identity and
receipt contract. Job submission selects its backend in the specification rather than exposing
separate keyed, unkeyed, and VM-observation submission protocols. Backend limitations stay explicit.
Strong reads use a leadership/quorum check and wait for the relevant applied position. Cached views
are explicitly stale. Watches resume using a cluster event cursor; expired history requires a fresh
snapshot and new cursor. A snapshot and its cursor must describe the same state.

Inputs are uploaded and verified before they are usable by an execution. Acceptance of job metadata
does not promise replicated input bytes; unavailable inputs leave an execution waiting for staging.
Downloads and destination overwrite tracking belong on the client. A local client helper can maintain
automatic fetching when desired; control replicas never interpret a client's local destination path.

## Request receipts and retention

Every committed client mutation has a receipt keyed by `(client_id, request_id)`. It stores the
canonical request, fingerprint, durable response, commit position, and retention policy. Large file
contents are represented by references. Status polling, log chunks, and raw metrics do not become
replicated request receipts. This is a mutation journal, not a transcript of every connection attempt.

The proposed default is 90 days for the full receipt from its recorded decision time, with an indefinite
option. A client may explicitly release its full receipt earlier. Both pruning and release are committed
commands. A client release means the full response can be forgotten; it does not undo the request.

A compact record of the request key, fingerprint, outcome summary, and resource/operation references is
retained indefinitely initially. It prevents old requests from becoming executable again. Removing all
such records would require a separate protocol that permanently retires a client request namespace.
The size of these records is observable; they are compact, not bounded in total count.

Outstanding operation state is retained until resolved regardless of receipt retention. Receipt
retention does not set job history or blob retention. Artifact references in old receipts are historical
references, not an automatic promise to keep the referenced bytes forever.

Replicas do not independently prune using their clocks. The leader proposes explicit eligible receipt
IDs with a cutoff, and deterministic application checks the stored policy and preserves compact records.
Uncertain or backward time postpones expiry; clock policy is part of prototype qualification. Indefinite
retention remains available when an elapsed-time retention guarantee is required.

## Raft and redb storage

Each control replica has a redb database with separate logical tables for Raft log/vote state and Fleet
application state. A storage component serializes database writes without blocking asynchronous network
tasks. Native redb multi-table transactions keep primary records and secondary indexes consistent.

Proposed application tables contain cluster configuration and authorization; workers; logical jobs and
executions; instances; operations and desired revisions; resource reservations; full and compact
receipts; worker event cursors; artifact manifests/locations; and a bounded cluster change history.
Indexes support queue order, worker assignments, resource history, and receipt expiry. Encoding and
schema versions are explicit; typed Rust access does not substitute for a durable format specification.

Raft log entries and votes are persisted before the storage adapter reports the corresponding durable
write complete. For committed application commands, one redb transaction updates affected resources,
indexes, pending operations, receipts, and the last applied log position. Membership entries also update
the stored applied membership. A failed transaction leaves none of these changes visible.

Identifiers, timestamps, and scheduling choices are carried in proposed commands. Apply performs
deterministic checks against committed state, including resource reservations and expected revisions.
It does not consult local clocks, hardware, random generators, or network peers. Unknown command or
schema versions stop participation rather than silently skipping a state transition.

Use redb's immediate durability for the required persistence boundaries. Its documented contract makes
a commit persistent before it returns; Raft integration must preserve that contract rather than report
completion on a buffered write. [redb durability](https://docs.rs/redb/latest/redb/enum.Durability.html)

Build a versioned logical snapshot from one consistent read transaction. Include application state,
receipts and compact records, membership, the applied position, and integrity metadata. Stage and
validate received snapshots, then atomically activate the new state generation. Installation preserves
the receiving node's local vote/log state according to Raft's storage protocol. Never overwrite an active
whole database with a peer's database file. Crash recovery must select one complete generation.

Snapshot persistence and installation must be qualified before log compaction is enabled. Application
receipt retention is independent of Raft log retention. OpenRaft defines separate storage responsibilities
for application state and snapshots. [OpenRaft state machine contract](https://docs.rs/openraft/latest/openraft/storage/trait.RaftStateMachine.html)

## Worker reconciliation and controller changes

Placement is a committed decision containing execution ID, fixed worker ID, immutable specification
fingerprint, resource reservation, and desired revision. A durable pending operation records the work
needed to realize it. The worker polls for committed intentions; the replica serves them only after
establishing a linearizable read. It cannot supply a fresh view without quorum. A term number supplied by a caller is not proof
that an operation was committed.

Workers authenticate the cluster's authorized controllers and persist accepted intent before acknowledging
it. Equal IDs and revisions with equal contents are retries; conflicting contents fail. Older revisions
are rejected after a newer one is accepted. Local admission remains authoritative about what can actually
start on the machine; rejection becomes an observation rather than an invented successful launch.

Per-resource reconciliation serializes effects and targets the particular runtime incarnation. Completion,
cancellation, and deletion leave durable terminal records. A cancellation that arrives before a launch
creates a tombstone that prevents the delayed launch. Controller restart or redelivery never by itself
authorizes a new execution. Uncertain guest command side effects are reconciled or reported as uncertain,
not automatically rerun.

The safety argument is based on committed intent, fixed placement, durable worker records, and revisions;
it does not assume only one controller can ever have a message in flight. An old committed start may reach
a worker before a newer cancellation does, briefly starting work that will then stop. Acceptance of the
cancellation is not a claim that this propagation already happened.

Before claiming stop/delete completion or releasing a reservation for reassignment, the control plane
needs worker evidence of durable terminal intent and completed cleanup. An offline worker supplies no
such evidence. Reconnect first reconciles durable worker inventory with current cluster desired state;
unrecognized running work is surfaced, not overwritten or duplicated.

Worker events use `(worker_id, journal_incarnation, sequence)` so a reset log cannot be mistaken for
continuation of an older journal. Restarts with intact storage preserve the incarnation. Applying event
effects and advancing the control-plane cursor is atomic. Event gaps require an inventory snapshot and
matching cursor. Old reports cannot overwrite newer revisions or resurrect a terminal execution.

The real-worker milestone uses worker-initiated polling and persistent authenticated membership
discovery. There is no incoming controller command endpoint or separate connection takeover handshake.
Tests cover removed controllers, reconnects, and stale observations. No lease expiry or automatic worker
replacement is assumed. The trusted-node model does not require cryptographic quorum certificates for
every worker command, but authentication alone is not evidence of present leadership.

## Membership and availability

Membership management is a client-facing administrative operation. Initially an explicit bootstrap
administrator allowlist authorizes it; a future ACL can grant a dedicated membership-management
permission. Ordinary client or worker authentication does not grant that permission. Installing and
starting Fleet on a replacement host is separate from admitting that prepared replica to the cluster.

An authorized client submits a desired voter set with a request ID and expected configuration revision.
It receives a durable operation ID and can watch or retry the operation through leader changes. Only one
membership transition is active at a time. A new leader reconciles the operation against actual committed
Raft membership, including an intermediate joint configuration, instead of blindly repeating every step.

| Requested change | Procedure |
| --- | --- |
| Replace a replica machine | Enroll the replacement with its own identity, catch it up as a learner, commit the new voter set, and retire the removed controller role |
| One voter to three | Keep the original voter available, catch up two learners, then commit a three-voter configuration whose normal quorum is two |
| Three voters to one | Choose an available survivor and commit the reduction with the existing quorum before retiring the other controller roles |

These operations preserve cluster ID, jobs, and request history. Changing a machine's control-plane
membership does not remove its worker role or migrate its workloads. Full replacement of the machines
is possible through successive committed transitions; the first implementation qualifies replacement
and both one-to-three and three-to-one changes. Larger sets can use the same model after qualification.

Bootstrap creates a new cluster once. Additional replicas join through explicit authorization, catch up
as learners, and become voters through the consensus library's membership procedure. Application discovery
metadata must agree with committed membership. Removing a controller also removes its dispatch authority;
workers learn that change through the authenticated refresh protocol qualified above.

OpenRaft provides learner admission and a membership transition through joint and final configurations;
Fleet must durably track the surrounding administrative operation and handle interruption between them.
[OpenRaft membership API](https://docs.rs/openraft/latest/openraft/raft/struct.Raft.html#method.change_membership)

Connected clients refresh and cache authenticated membership. A client offline while every cached
replica is retired needs an updated trusted bootstrap contact; a stable cluster ID alone provides no
network rendezvous. Request IDs and the cluster's retained receipts remain valid after reconnecting.

Loss of quorum prevents new global mutations and strong reads. It does not erase accepted requests,
trigger an independent local coordinator, or stop existing workloads. Cached status remains readable as
stale. Permanent quorum loss uses the explicit [operator recovery procedure](CONTROL_PLANE_RECOVERY.md);
there is no automatic force bootstrap. Replication does not replace backups against accidental deletion or correlated storage loss.
In particular, the only remaining reachable voter of a three-voter cluster cannot authorize a reduction
to one, and a lost sole voter cannot be replaced through this normal membership API.

Replica networking must work directly over the configured LAN without an internet relay or public DNS.
The replicated binary uses explicit direct IP contacts with no public relay, address lookup or automatic
port mapping. See [multi-machine deployment](CONTROL_PLANE_NETWORKING.md) for preparation, diagnostics,
failure qualification and boundaries. The legacy `FLEET_NETWORK` setting does not configure this transport.

## Integration and initial cutover

The normal `fleet` binary always uses an explicitly running replicated control plane, including for
one replica. The retired CLI, GUI, standalone SQLite scheduler, and its installers are removed.
A client-staging helper can deliver saved inputs but cannot schedule work or replace an unavailable
control plane. Native/Tart supervisors, verified transfers, and their backend tests remain shared.

There is no SQLite importer, legacy protocol adapter, or live workload adoption in this milestone.
The new client, control plane, and worker interfaces are versioned independently from the legacy
protocol and reject legacy peers. CLI commands, GUI bindings, and JSON callers are updated together;
existing user scripts may require changes. New enrollment uses fresh cluster and worker state under
separate directories and service configuration, so legacy files cannot be mistaken for new records.

At deployment, drain the legacy work on each machine before transferring that machine to the new
worker. A machine is eligible only after its legacy workloads, including persistent VMs, are stopped
or explicitly retired. Preserve old binaries, databases, logs, and outputs as a separate archive.
Historical jobs are not automatically visible in the new cluster, and persistent instances are not
silently adopted. Input or output files can be explicitly uploaded as new artifacts. Stopping or
retiring real workloads is a deployment step, not part of design or prototype qualification.

Mixed-release operation and rolling upgrades are not first-milestone requirements. Qualify one release
across clients, replicas, and workers; keep explicit protocol, command, and storage format checks so
unsupported versions fail clearly. Recovery of the new format across crashes and restarts remains
mandatory. Future upgrades of the new cluster require a deliberate version/migration procedure; a
breaking initial release is not permission to discard its subsequently committed state.

Reuse the current Tart adapter, guest supervision, native supervision, and verified blob transfer code
where appropriate behind the new interfaces. Reusing code does not require accepting old wire messages
or durable records. Tests are updated around the new contract while preserving backend safety checks.

[Artifact storage](ARTIFACT_STORAGE.md) now manages checkpoint/final-output backups outside Raft. Metadata records verified locations
and distinguishes unavailable bytes from missing resource history. Existing trusted native execution,
Tart isolation, networking policy, and resource admission behavior continue through their current backends.

## Qualification milestones

| Milestone | Work | Evidence required |
| --- | --- | --- |
| Storage | redb adapter, command application, receipts, snapshot format | Crash/reopen, failed writes, atomic indexes, replay, interrupted snapshot creation and installation |
| Replication | One and three replicas over iroh, isolated client, simulated worker | Leader failure, partitions, lost replies, deterministic state convergence, durable duplicate prevention |
| Reconciliation | Complete submit/cancel/observe path | No second execution on redelivery; cancel-before-start; delayed old revisions; worker restart and event gaps |
| Membership and clients | Authorized client operations, learner catch-up, voter replacement, discovery, authority refresh | Replacement and one-to-three/three-to-one changes survive leader failure; unauthorized changes rejected; quorum-loss reduction refused; removed-controller behavior; bootstrap recovery after all cached peers retire |
| Fleet integration | New APIs and worker records, updated clients, reused execution backends | Separate native and Tart test workloads; legacy peers rejected; fresh-state cutover and archive procedure |

The first implementation target ends after isolated storage and replication qualification, including
the submit/cancel/observe path through a simulated worker. All processes use dedicated temporary homes,
keys, ports, and databases. It changes no installed worker/coordinator services and starts no training.

Measure durable submission latency, failover time, log/snapshot growth, and retention costs on this
workload. Process-kill tests demonstrate process-crash behavior; they do not by themselves establish
power-loss durability. Storage contract tests and fault injection must accompany them.

Before real worker integration, the prototype must show that a committed request survives lost replies
and leader failure; repeated delivery cannot create another execution; expired receipts cannot reopen
requests; quorum loss cannot create new decisions; and snapshot recovery preserves these properties.

The real-worker milestone now qualifies the isolated native/Tart integration through the new control
and read-only data protocols, including combined-process operation. See
[CONTROL_PLANE_WORKERS.md](CONTROL_PLANE_WORKERS.md) for the implemented surface, qualification, and limits.
The next integration is implemented in [NORMAL_FLEET.md](NORMAL_FLEET.md): the durable queue, automatic
placement, shared CLI/GUI clients, JSON automation, and persistent service roles. Qualification and idle-worker
handoff are recorded separately from the earlier lab results. Blob lifecycle remains separate work.
