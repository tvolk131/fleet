# Distributed control plane lab

The `control-plane` feature (enabled by default) implements the
[distributed control plane design](DISTRIBUTED_CONTROL_PLANE.md). It uses OpenRaft 0.9.25,
redb 4.3.0, and real authenticated iroh connections. It retains a simulated worker for consensus tests and now also supports
[real enrolled workers](CONTROL_PLANE_WORKERS.md) for native and Tart workloads in separate homes.

The normal `fleet` binary now uses this control plane through the [queue, clients and services](NORMAL_FLEET.md).
The `fleet-control-lab` wrapper remains for low-level diagnostics and historical qualification scripts;
it requires explicit paths and has no default Fleet home. Installed legacy services change only through
an explicit idle-worker handoff.

## Implemented behavior

- Explicit [lost-quorum recovery](CONTROL_PLANE_RECOVERY.md), signed client/worker rebinding, and inventory reconciliation.
- One or three replicas, explicit initial bootstrap, persistent votes/logs, and leader failover.
- Authorized, durable replica replacement and single↔three voter transitions, with learner catch-up,
  recovery from interrupted joint consensus, and persistent client membership discovery.
- Atomic metadata application including job state, request receipts, worker cursors, and applied position.
- Authenticated administrator mutations and strong reads, with distinct authorization for Raft RPCs.
- Client redirection/retry through configured peers, keeping the original request ID after an unknown outcome.
- Matching request replay, conflicting-content rejection, and explicit full-response release that retains
  the request fingerprint and prevents reexecution.
- Durable snapshot creation, integrity/cluster checks, atomic installation, log compaction, and follower catch-up.
- A persistent simulated worker with one start per execution, revisions, cancellation tombstones, and a
  numbered event journal. `simulate` reconciles once and submits pending journal events through the client.

Submission includes explicit job, execution, and worker IDs. Real assignments have enrolled-worker
authentication and aggregate resource admission. The normal queue adds automatic placement; these
low-level lab commands deliberately expose explicit assignments. Simulated
observations use an administrator identity and stay separate from real assignments.

## Run the automated qualification

From the repository root:

```sh
cargo test --features control-plane --test control --locked -- --test-threads=1
```

The tests require permission to bind loopback UDP sockets. They create temporary homes and clean up their
own replica processes. They cover the OpenRaft storage contract, process kill/restart, response loss after
commitment followed by leader replacement, minority partitions, strong reads, snapshot catch-up after
compaction, receipt release/replay, authentication, and worker reconciliation. Injected failures immediately
before state/snapshot commit verify that reopening selects the complete old state.

Process-kill and transaction-abort tests do not qualify power-loss behavior or actual disk I/O faults.
The simulated effect counter is transactional; its tests do not establish exactly-once external process
launch or guest side effects. The real-worker guide describes the separate backend qualification.

The foundation qualification record (local hardware qualification record; excluded from the public source package) and
[membership guide](CONTROL_PLANE_MEMBERSHIP.md) describe the checks and their limits. Fault tests use
replicas within one test process; an additional CLI test starts separate processes and replaces all
original bootstrap contacts. The forced process-kill test uses a single-replica child process.
[Multi-machine deployment and failure qualification](CONTROL_PLANE_NETWORKING.md) extend this lab to
explicit LAN contacts. Normal client integration is documented in [NORMAL_FLEET.md](NORMAL_FLEET.md); sustained performance qualification remains separate.

The current normal queue uses protocol/storage format 5. Create fresh homes with this version; there
is no importer for earlier lab formats or legacy SQLite state. Historical qualification records retain
the version they actually tested.

## Run an interactive three replica lab

Build the separate executable and prepare an empty temporary directory:

```sh
cargo build --features control-plane --bin fleet-control-lab --locked
lab_dir="$(mktemp -d /tmp/fleet-control-lab.XXXXXX)"
target/debug/fleet-control-lab init --home "$lab_dir" --replicas 3
```

By default, `init` creates private keys, loopback addresses, per-replica configurations, a client configuration, and
`submit.json`. It refuses a populated directory. Keep `lab_dir` available in the shell used below.
The printed configuration contains public identities only. If a selected port becomes occupied before
startup, create a fresh lab rather than changing a running cluster's identity configuration.

Start three processes, retaining their PIDs for cleanup:

```sh
target/debug/fleet-control-lab serve --home "$lab_dir/node-1" --config "$lab_dir/node-1/config.json" --bootstrap >"$lab_dir/node-1.log" 2>&1 &
lab_pid_1=$!
target/debug/fleet-control-lab serve --home "$lab_dir/node-2" --config "$lab_dir/node-2/config.json" >"$lab_dir/node-2.log" 2>&1 &
lab_pid_2=$!
target/debug/fleet-control-lab serve --home "$lab_dir/node-3" --config "$lab_dir/node-3/config.json" >"$lab_dir/node-3.log" 2>&1 &
lab_pid_3=$!
```

Only the first initial startup uses `--bootstrap`. Restarting a replica uses its same home/configuration
without that flag. Bootstrap refuses already initialized state; it is not a recovery override.

Submit the supplied request, reconcile the simulated worker, and inspect cluster state:

```sh
target/debug/fleet-control-lab request --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json" --request "$lab_dir/submit.json"
target/debug/fleet-control-lab simulate --home "$lab_dir/simulation" --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json"
target/debug/fleet-control-lab read --key "$lab_dir/client/node.key" --config "$lab_dir/client/config.json"
```

Repeating the same request returns its original acceptance. Repeating `simulate` reports `starts: 1`.
To cancel the example, save this as a new request file and pass it to `request`, then run `simulate` again:

```json
{"origin":"CLUSTER_ID_AT_CREATION","id":"example-cancel","mutation":{"Cancel":{"job":"example","expected_revision":1}}}
```

Set `origin` to the cluster ID in the client config when creating the request, and retain it on retries.
Cancellation acceptance precedes worker observation. The simulation will record revision 2 as cancelled;
delivering revision 1 afterward cannot start it again. A deliberately new job needs fresh job/execution
IDs and a fresh request ID. Preserve request files after a timeout so retries use the same contents.

Stop only these lab processes when finished:

```sh
kill "$lab_pid_1" "$lab_pid_2" "$lab_pid_3"
wait "$lab_pid_1" "$lab_pid_2" "$lab_pid_3"
```

## Remaining milestones

Replica membership is now managed through the [membership CLI and API](CONTROL_PLANE_MEMBERSHIP.md).
The administrator allowlist remains static. [Real workers](CONTROL_PLANE_WORKERS.md) now authenticate,
pull quorum-confirmed assignments, reconcile native/Tart backends, account for resources, and expose
verified input/output transfer and observations. Removed replicas have no direct worker command channel.

Production metadata tables/indexes, pagination, 90-day timed receipt pruning, automatic placement,
worker replacement/recovery tooling, artifact replication/retention, and production cutover remain ahead.
Full-response release is implemented; automatic age-based pruning is not.

For qualification simplicity the metadata state is one serialized redb value. Snapshot publication is
serialized with writes, and installation replaces state plus saved snapshot in one transaction while
preserving local votes/logs. This is not the final scalable table layout. Wire frames are limited to
8 MiB, snapshots to 64 MiB, and simulated job payloads to 4096 bytes. These are lab limits.
