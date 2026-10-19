# Control plane membership

The replicated control plane supports replacing replicas, expanding one voter to three (normal
quorum: two), and reducing three voters to one. Cluster identity, jobs, assignments and request
history survive these changes. Membership changes neither move workloads nor enroll/remove workers.
The administrator allowlist remains explicit and static on each replica. See [baseline compatibility](BASELINE.md),
[rolling upgrades](ROLLING_UPGRADES.md) and [normal services](NORMAL_FLEET.md) for their separate contracts.

## Contract

`ChangeMembership { expected_revision, voters }` is an ordinary idempotent request, authorized only for
configured administrator identities. `voters` maps numeric replica IDs to their public iroh identity
and direct address. A replica's transport identity does not grant administrative permissions.

Acceptance returns `MembershipAccepted { operation, revision }`. The receipt is immutable; completion
is recorded separately in `membership_operations[operation]`. `read` exposes `configuration.revision`,
the active operation, and the committed Raft membership. The stable revision starts at 1 and advances
once when the final voter set commits. A request for the existing set completes without advancing it.

Only one transition may be active. Stale revisions, conflicting request contents, duplicate identities,
and unsupported voter counts are rejected durably. Within one wire incarnation, retired replica IDs
and keys cannot be reused.
Changing an existing identity/address in place is unsupported: prepare a fresh replica and replace it.

The leader reconciles the durable operation:

1. Admit each new replica as a nonvoting learner.
2. Require acknowledgement through a specific committed log position before promotion. This uses an
   explicit catch-up barrier rather than OpenRaft's default replication-lag tolerance.
3. Commit joint consensus, then the final uniform voter set through OpenRaft.
4. Mark the operation complete in the same redb transaction that applies the final membership entry.

After leader failure, reconciliation resumes from committed membership and operation state, including
an intermediate joint configuration. A timeout does not cancel the operation. Retry with the original
request ID, expected revision, and voter map. Releasing its full receipt preserves the operation
reference and duplicate-prevention record.

An unavailable learner leaves the operation pending. There is currently no transition abort or target
editing operation: bring the prepared replica back with its original identity/state. Do not submit a
different change while one is active. During growth from one voter, that original voter remains required
until the transition completes. Reducing three to one requires the existing quorum; a lone surviving
minority cannot force a reduction. Prepared replicas cannot bootstrap themselves into a new cluster.

## Client discovery and authorization

Authenticated responses include a committed local membership view, ordered by applied log position.
It can be stale and serves only as a routing hint. Strong reads still require the leader's quorum check.
Acceptance includes the prospective peers so clients can learn replacements before old controllers retire.

The CLI caches learned membership beside its config as `config.json.<incarnation-hash>.members.json`, using an atomic file
replacement and a lock to prevent older concurrent clients from overwriting a newer view. Configuration
files and this cache are trusted local files. Cache errors are reported without hiding a successful
committed response. Protect the client directory as you do its bootstrap config.

Clients follow authenticated leader hints and refresh membership internally. A client offline while all
its known peers retire needs a new trusted bootstrap contact in its config. Existing cached contacts do
not override an explicitly provided replacement contact. The cluster ID alone cannot locate or
authenticate a new peer; obtain its public identity through a trusted path.

Replica RPC authorization uses applied membership plus explicitly committed prospective membership,
checks that the authenticated key matches the claimed sender ID, and rechecks each request on existing
connections. Once a survivor applies the final configuration, it rejects a removed replica's Raft
traffic even if that replica still has stale configuration. Removal does not erase that machine's data
or stop its process; shut down the retired controller after completion. Real workers use the same
authenticated membership discovery and keep their assignments through controller replacement.

## Replace a replica and shrink the cluster

Start the three-process lab from [QUALIFICATION.md](QUALIFICATION.md), retaining `lab_dir` and
its process IDs. Set `cluster_origin` to the `cluster` value in the client configuration and preserve it
when retrying the same request. Prepare and start replica 4 in another empty home:

```sh
target/debug/fleet cluster prepare-replica --home "$lab_dir/node-4" --config "$lab_dir/client/config.json" --id 4
target/debug/fleet cluster serve --home "$lab_dir/node-4" --config "$lab_dir/node-4/config.json" >"$lab_dir/node-4.log" 2>&1 &
lab_pid_4=$!
```

Preparation creates a private key, `config.json`, and a public `peer.json` descriptor. It grants no
cluster membership and disables bootstrap for the prepared replica. Initial lab configs enable
bootstrap only for the original cohort. Port selection is provisional until the process starts;
if a port is occupied, recreate that unadmitted lab replica before submitting its descriptor.

Replace replica 1 with replica 4 through the ordinary client. The CLI constructs the voter map:

```sh
target/debug/fleet --home "$lab_dir/client" replicas --add "$lab_dir/node-4/peer.json" --remove 1 --request-id replace-1-with-4
```

The command waits up to 30 seconds for completion and prints the completed operation. `--wait-seconds`
sets the completion wait limit. If it times out, inspect with `read` or retry the exact command; a new
request ID is unnecessary. Once complete, stop the retired replica and reduce the new cluster to 4:

```sh
kill "$lab_pid_1"
wait "$lab_pid_1"
target/debug/fleet --home "$lab_dir/client" replicas --remove 2 --remove 3 --request-id shrink-to-4
```

After that operation completes, replicas 2 and 3 can be stopped. Replica 4 now has the same cluster's
metadata and request history, with configuration revision 3. The CLI continues using its original
config path and learned membership cache.

To grow again, prepare and start fresh replicas 5 and 6. Merge their `peer.json` maps with `shrink.json`
to make `{4,5,6}`, then submit it with a fresh request ID and `--expected-revision 3`. The original
replica 4 stays available until completion. Do not reuse retired IDs 1, 2, or 3. The same procedure
grows a freshly initialized single-replica lab from revision 1. Retain and stop only the PIDs of these
temporary lab processes when finished.

## Qualification boundaries

The qualification record (local hardware qualification record; excluded from the public source package) records 20 passing
control-plane tests, 111 passing Rust tests across all features, formatting, and strict Clippy checks.

`cargo test --features control-plane --test control --locked -- --test-threads=1` covers happy paths, replacement of the
leader, lost acceptance responses, offline learners, interruption during joint consensus for replacement,
growth and shrink, sole-voter restart, refusal of minority reduction, snapshot catch-up with pending
operations and released receipts, stale/concurrent requests, identity reuse, removed-replica traffic,
and persistent discovery after every original seed retires. A CLI test uses separate replica processes;
the deterministic joint-interruption tests stop tasks/replicas within a test process.

Only one- and three-voter configurations are accepted. The original qualification above used loopback;
the subsequent [multi-machine milestone](CONTROL_PLANE_NETWORKING.md) exercises membership across
physical hosts. Permanent quorum loss uses the separate
[disaster recovery workflow](CONTROL_PLANE_RECOVERY.md). Transition abort, administrator rotation and
automatic operation-history pruning remain future work. The existing lab wire, state-size, and snapshot-size limits still apply.
