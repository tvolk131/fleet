# Fleet architecture

Fleet has one logical control plane, represented by either one replica or a three-voter Raft quorum.
Trusted physical machines run independently configured controller, worker, storage and tunnel roles.
Roles can share one process, but colocation does not bypass authentication, consensus or worker
reconciliation. Untrusted application code runs inside Tart guests; native jobs remain trusted host
processes. The [baseline contract](BASELINE.md) defines current formats and upgrade boundaries.

## Control plane

OpenRaft orders authenticated commands over iroh. Each replica persists its log and applies committed
commands atomically in redb. `control::model::Mutation` is the explicit command enumeration; its
application is deterministic. Jobs, assignments, worker registrations, VM operations, checkpoint and
artifact catalogs, retention intent, membership operations, recovery state and upgrade state belong
to this replicated state machine. Domain modules implement their invariants; the transport does not
execute workloads. Scheduling proposes assignments through the same committed mutation path.

An accepted request has a permanent `(principal, request ID)` receipt with its original authority,
fingerprint, decision, resource reference and applied index. Matching retries return the recorded
result. Conflicting retries are refused. Explicit response release removes bodies, never the replay
barrier. Receipts are application state and survive Raft log compaction. Producer observations use
separate sequence cursors. See [indexed metadata](INDEXED_METADATA.md).

Redb stores a small header, keyed resource rows and keyed receipts. State changes and applied position
commit together. Raft snapshots stream a consistent header and ordered rows; installation validates
the complete image before committing replacement tables. Large bytes such as logs, inputs and
checkpoints are not placed in the consensus log.

## Worker and backend

A worker pulls a quorum-confirmed view of its assignments. Its own redb journal persists accepted
intent, observed results, report sequences and permanent execution markers. Before launch it records
that execution may have occurred. Missing backend state after that boundary produces uncertainty,
not a fresh execution. Reports are retried and deduplicated by the control plane. The control plane
accepting a request and a worker completing it are separate events.

`backend::Backend` provides local methods for native execution, VM lifecycle, guest operations and
observations. It has no network mutation protocol or coordinator client. The replicated worker owns
authorization and execution fencing. `worker_data` describes replies for the authenticated data plane;
clients can read observations or stage authorized inputs without receiving a direct launch channel.
`inputs::prepare` builds immutable input specifications independently of networking.

Jobs have stable logical IDs and immutable attempts. A portable Tart job can cooperatively suspend
at an application checkpoint and resume with a new attempt on a compatible worker. A persistent VM
has independent lifecycle intent and durable guest operations. See [workers](CONTROL_PLANE_WORKERS.md),
[portable jobs](PORTABLE_JOBS.md) and [managed VMs](MANAGED_VMS.md).

## Storage and networking

Storage roles retain verified, hash-addressed payloads under separate homes and identities. The
control plane owns placement, retention and retirement intent; storage agents reconcile copies and
report inventory. Metadata describes availability and ownership, not an assumption that bytes exist.
See [artifact storage](ARTIFACT_STORAGE.md), [history](DURABLE_HISTORY.md) and [retention](ROLLING_STORAGE.md).

Authenticated iroh service tunnels carry explicitly permitted TCP services, including host-native
Garage and optional VM service attachments. Cached routes enforce the latest locally accepted policy
and expose their freshness; revocation is confirmed through observations. VM networking remains
offline or restricted public-internet access with separately declared private services. See
[service tunnels](SERVICE_TUNNELS.md) and [VM networking](VM_SERVICE_NETWORKING.md).

## Clients, membership and recovery

The CLI, JSON interface and iced-m3 GUI use `AppClient`. Clients normally contact one replica, follow
leader hints and retain membership discovery across retries. They preserve request IDs and authority
origins after an unknown outcome. Quorum reads authorize actions; per-replica diagnostics can be stale.

Cooperative membership changes use learners and joint consensus. Lost-quorum recovery creates a new
authority incarnation with an administrator-signed manifest; clients and workers explicitly accept
that transition. Old-origin requests cannot silently become new work, and workers retain execution
fences while reconciling the surviving inventory. See [membership](CONTROL_PLANE_MEMBERSHIP.md),
[recovery](CONTROL_PLANE_RECOVERY.md) and [rolling upgrades](ROLLING_UPGRADES.md).

## Code and qualification boundaries

The root package contains the product and backend library; `crates/fleet-guest` contains the guest
helper. `crates/fleet-tools` is an unpublished package for release qualification and the one-time
importer. Fake Tart and workload fixture executables require the explicit `qualification` feature.
Failure-injection CLI options are excluded from normal builds. See [qualification](QUALIFICATION.md).
