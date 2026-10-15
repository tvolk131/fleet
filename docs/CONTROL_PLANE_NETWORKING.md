# Multi machine control plane deployment

`fleet-control-lab` can run replicas and workers on separate machines using authenticated iroh QUIC
over explicit IP addresses. The M4 Pro and M1 Pro use the macOS ARM binary; the Pi uses a separately
built Linux ARM binary from the same source and lockfile. The CLI, GUI, JSON automation, and persistent
service roles use this transport; see
[NORMAL_FLEET.md](NORMAL_FLEET.md). The historical qualification below used separate lab homes
and did not change installed legacy services.

## Network contract

- Every replica and worker advertises a concrete unicast IP and nonzero UDP port. It binds that same
  local address. Wildcards, multicast, broadcast and advertised port zero are rejected.
- Clients bind ordinary IPv4/IPv6 interfaces and contact the pinned identities at the configured
  addresses. Direct LAN operation uses no public relay or public address lookup. Automatic router port
  mapping is disabled. SSH is used by the qualification harness for deployment and process control;
  replication, client requests and artifact transfer use direct iroh connections, not SSH tunnels.
- Addresses must be mutually reachable. Use stable addresses or DHCP reservations. An admitted replica's
  address and key are immutable: prepare a fresh replica and rotate membership to change them. Worker
  enrollment is also immutable in this release. Public internet/NAT traversal is not configured here.
- Raft heartbeats run every 250 ms and election timeouts range from 1.5 to 3 seconds. An RPC retains
  its own deadline while a shared connection attempt may continue for up to five seconds. A short
  heartbeat timeout cannot repeatedly cancel the handshake needed by subsequent requests.
- `inspect` is authenticated local diagnostics and may be stale. Only normal quorum-confirmed reads
  and committed mutations authorize scheduling or membership decisions.

The new transport does not change Tart guest networking. Offline and public-internet-only VM policies
remain enforced by the worker's existing Tart/Softnet backend.

## Prepare and distribute initial replicas

Build one qualified release for each OS/architecture:

```sh
cargo build --release --locked --features control-plane --bin fleet-control-lab
```

Choose a fresh staging directory and explicit addresses in replica ID order. Preparation validates
remote addresses without trying to bind those interfaces on the administrator's machine:

```sh
fleet-control-lab init --home /tmp/fleet-control-example --replicas 3 \
  --address 192.0.2.11:28460 \
  --address 192.0.2.12:28460 \
  --address 192.0.2.13:28460
```

Addresses above are examples from the qualification network. Select available ports and reachable
addresses for the actual deployment. Without `--address`, `init` retains its isolated loopback defaults.
Port availability is checked when the destination starts; preparation does not reserve remote ports.

Transfer each `node-N/` directory to its intended host over an authenticated channel, preserving the
private key's mode 0600 and private directory's mode 0700. Keep `client/node.key` on the administrator's
machine; replicas and workers only need its public identity in their configurations. Protect the staging
directory because it initially contains every replica key. Keep future durable deployment homes on
persistent local disks; `/tmp` is for experiments and may be RAM-backed or cleared on reboot.

Start every replica with its own home and configuration:

```sh
fleet-control-lab serve --home /absolute/private/node-2 \
  --config /absolute/private/node-2/config.json
```

On exactly one member of the initial cohort, use the same command with `--bootstrap`. On subsequent
restarts, omit `--bootstrap`. Do not bootstrap prepared joiners or use bootstrap to override quorum.
The foreground process is suitable for an explicitly configured service supervisor; this milestone
does not install or replace system services.

On macOS, the launch context also determines Local Network access. An orphaned process launched by a
short SSH command can bind and accept replies yet receive `EHOSTUNREACH` (`No route to host`) when it
tries to initiate a new LAN connection. In this qualification, retaining the authorized SSH launch
session restored the same failing replica and worker connections, without changing keys, ports,
databases, firewall rules or privacy settings. The harness therefore holds one SSH session per remote
process until that process exits. These sessions carry process control, not replication traffic.
See [Apple's Local Network Privacy guidance](https://developer.apple.com/documentation/technotes/tn3179-understanding-local-network-privacy).
For an unattended deployment, use a supported service launch context and verify outbound connectivity
after logout; detached SSH commands are not the qualified service-installation method. Signed app/user
agent packaging and launchd installation remain part of the later installed-service cutover.

Before running workloads, inspect every replica and verify that all have applied the initial membership:

```sh
fleet-control-lab inspect --key /absolute/client/node.key \
  --config /absolute/client/config.json --replica 3
fleet-control-lab read --key /absolute/client/node.key --config /absolute/client/config.json
```

`inspect` targets exactly the requested pinned replica, including when quorum is unavailable. It reports
its leader hint, term, last log/applied positions and local membership. `read --replica N` instead requires
a strong read from that exact replica and disables automatic leader routing. `request --replica N` has
the same routing restriction; a failed or timed-out mutation still has an unknown outcome. Normal
commands omit `--replica` and handle discovery, leader routing and retries internally.

## Workers, membership and recovery

Copy a public cluster configuration to the worker host, then prepare a separate home there:

```sh
fleet-control-lab prepare-worker --home /absolute/private/worker \
  --config /absolute/public/cluster.json --id example-worker \
  --cpu-slots 1 --memory-mb 256 --bind 192.0.2.12:28480
```

Copy its public `enroll.json` to the administrator and submit it using `request`. Start the worker using
`worker --home /absolute/private/worker`. The optional combined `serve --worker-home ...` mode also
works with separate replica/worker homes and identities. See [real workers](CONTROL_PLANE_WORKERS.md)
for workload, input/output, resource and Tart configuration. Do not enroll overlapping capacity in the
old coordinator and the new cluster. Qualification workloads are tiny, explicitly placed native canaries.

For replica rotation, run this on the new replica's host:

```sh
fleet-control-lab prepare-replica --home /absolute/private/node-4 \
  --config /absolute/public/cluster.json --id 4 --bind 192.0.2.12:28464
```

Start it without bootstrap, then use its `peer.json` in the normal [membership workflow](CONTROL_PLANE_MEMBERSHIP.md).
An initial cluster or a recovered cluster can grow from one to three members. A three-member cluster can
shrink or rotate members while it retains a current majority.

[Lost-quorum recovery](CONTROL_PLANE_RECOVERY.md) now accepts a concrete LAN address for `--bind`.
The authority transition is unchanged: stop/fence the previous controllers, preserve a stopped survivor,
create a new incarnation, explicitly rebind clients and workers, reconcile inventories, then resume.
Distribute the signed public recovery manifest to workers; do not distribute the administrator's private
key. Machines which have not accepted that manifest can still follow their previous authority.

## Isolated failure qualification

`contrib/qualify_control_multihost.py` consumes a private JSON inventory:

```json
{"hosts":[
  {"name":"m4pro","address":"192.0.2.11","ssh":[],"binary":"/absolute/isolated/bin/fleet-control-lab"},
  {"name":"m1pro","address":"192.0.2.12","ssh":["ssh","-o","BatchMode=yes","m1pro"],"binary":"/absolute/isolated/bin/fleet-control-lab"},
  {"name":"pi","address":"192.0.2.13","ssh":["ssh","-o","BatchMode=yes","fleet@192.0.2.13"],"binary":"/absolute/isolated/bin/fleet-control-lab"}
]}
```

Use three distinct physical hosts, with the first host running the harness. Install the experimental
binaries at the specified paths first. Python 3.9 or newer and authenticated SSH access are required.
Choose a **new** fixture home under `/tmp` on all three hosts:

```sh
python3 contrib/qualify_control_multihost.py --inventory /absolute/hosts.json \
  --home /tmp/fleet-control-qualification-new --port 28460
```

The harness records installed Fleet service identities, binary hashes, job/instance record hashes and
queue rows before and after. It creates separate keys, journals, backend homes, blobs and logs, and uses
only its own recorded process identities for cleanup. Native canaries have hard runtime limits and use
one declared CPU thread each; they mostly sleep. There is no training or GPU workload.

The failure sequence covers leader SIGKILL, worker SIGKILL, minority refusal of strong reads/mutations,
healing and idempotent retry, quorum loss/restoration, leader replacement, three-to-one and one-to-three
membership changes, and stale-survivor disaster recovery followed by another three-host expansion.
Running native supervision must retain its original identity and print its start marker exactly once.
Cancellation and hash-verified output retrieval must still work afterward. A final cold restart of
only the M1 and Pi replicas must restore quorum without a controller running on the M4.

That final check exposed a missing recovery-state transfer in the earlier prototype: a new learner
could replay the new incarnation's log without receiving the old state used to seed it. Recovered
clusters now snapshot and compact before admitting learners. The loopback regression checks retained
jobs, receipts and recovery provenance after the new majority restarts; the physical canary checks
cancelled native assignments, worker inventories and recovery provenance in the same scenario.

`serve --qualification-partition FILE` is an explicit test-only facility. The local file contains a JSON
array of public peer keys whose application traffic is blocked. It is reloaded without changing host
firewalls or network interfaces. Malformed/missing updates retain the previous fault. This models a
partition between the experiment's replicas; it does not qualify a physical switch or router failure.

The qualification record (local hardware qualification record; excluded from the public source package) describes the completed run,
binary/source hashes, regression checks and preservation evidence. Physical power loss, disk corruption,
WAN failure, mixed-version upgrades, additional Tart hypervisor behavior and production service cutover
remain outside this qualification. The prior metadata-size and snapshot limits still apply.
