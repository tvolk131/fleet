# VM service attachments

Persistent VMs use the same named TCP services, explicit permissions, iroh
transport, durable policy cache and enforcement reports as host services. The
control-plane catalog authorizes adapter identities; the trusted operator binds
each identity to its local host or VM socket namespace.

## Boundary and data path

Each VM attachment runs on the host with its own adapter key and state directory.
Its immutable local binding names a worker identity and persistent instance and
fingerprints that instance's creation record and specification. One adapter
process may own a VM attachment at a time. Replacement requires stopping the old
process and explicitly preparing the new identity.

An application connects to a configured loopback port **inside its VM**. A Rust
helper carries the stream over Fleet's private guest channel (the guest agent's vsock
port) to the host adapter,
which resolves only a route from its committed policy. The destination checks the
authenticated iroh identity and named-service permission before connecting to a
fixed loopback socket in its own attachment. Same-host VM traffic uses the same
authenticated path and permission checks.

Guests receive socket addresses and bounded route indices, never adapter or
control-plane keys. Guest root can replace its helper or emit hostile protocol
frames; it cannot choose a host TCP destination, claim another VM's identity or
disable host-side authorization. The host, worker and VM driver remain trusted.
Guest-reported readiness is not attestation or application health verification.

There is no LAN bridging, publicly published ports, host shares or guest NIC
allow rules. `offline` denies NIC application traffic; an explicitly attached VM
can additionally use its granted service channels. `public_internet` is
unavailable until Fleet's host network agent exists. For no application
connectivity, use `offline` without a service attachment.

## Worker-managed attachments

Use [managed VM connections](MANAGED_VMS.md) for normal persistent VMs. The worker creates and runs
their adapters automatically. The procedure below remains the low-level manual attachment interface
for operators and transport qualification; do not attach both mechanisms to one VM.

## Prepare and operate

Create a persistent instance normally, then prepare an adapter on its worker.
`--instance` is the backend instance ID (for example `vm-web` for logical instance
`web`), visible in its execution assignment. Use a new adapter home and stable UDP
port in the network configuration:

```sh
fleet tunnels prepare --adapter-home /srv/fleet/adapter-web --adapter web \
  --authority /srv/fleet/client/config.json --network /srv/fleet/web-network.json \
  --worker-home /srv/fleet/worker --instance vm-web
```

Preparation accepts stopped or running persistent instances; it refuses a
different worker authority, deleted instance or disposable job VM. Start the
instance before expecting applied forwarding. Automatic network identities for
job attempts and cross-worker migration are outside this milestone.

Put the returned descriptor in the topology's `adapters` map as for host adapters.
A VM adapter's service targets and route listeners refer to **guest loopback**.
For example, this fragment (plus the two adapter descriptors) grants `web` access
to PostgreSQL inside `database-vm`:

```json
{
  "services": {
    "database": {
      "owner": "database-vm", "target": "127.0.0.1:5432", "allowed": ["web"]
    }
  },
  "routes": [
    {"adapter": "web", "service": "database", "listen": "127.0.0.1:15432"}
  ]
}
```

The web application connects to `127.0.0.1:15432` inside its VM. All processes in
that VM share its permissions. Treat Garage S3, node RPC and administration as
separate services; application VMs normally receive S3 access with scoped Garage
credentials. Garage can remain a separate host service.

Run `fleet tunnels run --adapter-home PATH`, or configure a service with
`fleet service configure --config PATH --tunnel-home PATH`. Each service
configuration runs one adapter; run a service per attachment. Use existing
`tunnels apply`, `status`, `wait`, the JSON API and GUI Service connections panel
for policy and enforcement. Initial preparation is a local operator action,
not a remotely scheduled worker task or GUI operation.

## Lifecycle and upgrades

A policy change tears down and joins old host forwarding tasks, then kills and
reaps the guest channel process before starting its replacement. All guest route
listeners must bind before the attachment reports applied. Bind conflicts,
missing helpers, invalid frames, stopped VMs and changed revisions fail forwarding
closed and appear in enforcement status. Restart retries against the same
immutable VM binding. Failure never substitutes host sockets.

The private guest channel sends heartbeats and closes after ten seconds without
receiving a frame. This releases helper-owned ports if the guest command
channel stays open after the host adapter dies. Reattachment can therefore take a heartbeat
timeout plus a bind retry. This is channel cleanup, independent of the durable
policy cache; it is not a control-plane permission lease.

Removed adapters can acknowledge an empty policy with their VMs stopped. Cached
permissions survive controller loss and adapter restarts; disconnected endpoints
still cannot confirm revocation. Existing freshness and fencing rules apply.
Emergency authority recovery requires explicit adapter and worker rebinding before
recreating routes.

VM homes use `tunnel-vm.json`, distinct from the previous host-only `tunnel.json`.
Older binaries refuse a VM home before they can interpret its sockets as host
sockets. Host homes and the generation-three consensus schema are unchanged.
Fleet installs its matching content-addressed helper through the guest agent, without a guest
interpreter or additional guest network privileges.

## Bounds and qualification

The guest channel admits 64 simultaneous streams, up to 64 route listeners and
256 fixed targets. Frames hold at most 16 KiB; streams have four data credits per
direction, bounded duplex buffers and input queues. Control messages and output
are separately bounded so bidirectional traffic can continue reading while
waiting to send credits. Open requests time out after ten seconds. Stream IDs
have disjoint host/guest ownership and cannot be replayed. FIN preserves TCP
half-close; reset cancels failures. Partial application bytes are never replayed.

This is a service proxy, not an IP VPN: UDP, private IPs, DNS, multicast, discovery
and bandwidth reservations are outside its scope. The shared guest channel adds
copying and can couple stream latency; it is not a native-throughput claim or
isolation from resource contention.

`fleet-qualify vm-services --fake --binary /absolute/path/to/fleet --home
/tmp/fleet-control-vm-services-test` exercises simulated VMs with the real CLI,
control plane and adapters: VM/host services, large transfers, active revocation,
cached-policy restart, VM lifecycle and retired-identity acknowledgment. CI runs
on Linux and macOS. The fake backend shares host namespaces and does not prove
hypervisor or network isolation. It models two small logical workers so the
fixture fits CI host admission limits; fake VMs do not allocate guest RAM.

Omit `--fake` and provide `--instances` with a `vz` catalog for two real offline
VMs. Each gets its own clone of the image bundle; the fixture refuses active installed jobs, checks that
guest listeners do not bind on the host or appear in another VM, and deletes only
fixture VMs. Both real VMs run on one worker with sufficient CPU and RAM budgets.
`--baseline-binary` tests the previous host-only adapter against a
stopped VM adapter home. Publish only the safe `qualification.json`; raw fixture
homes contain test keys.
