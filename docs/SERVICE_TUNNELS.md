# Authenticated service tunnels

Fleet adapters connect registered TCP services over authenticated iroh connections.
The replicated control plane stores service names, owners, contact addresses,
permissions and local routes. Each adapter enforces its committed policy; service
payloads travel directly between adapters, or through an explicitly configured
iroh relay. They never pass through Raft or the control-plane database.

## Scope and trust

An adapter is an optional host role with its own key and state directory. It can
run alone or alongside a controller, compute worker or storage agent in
`fleet service run`. Its identity can read its service policy but cannot submit
administrative changes, read the global job database, or claim another adapter's
identity. The receiving adapter authenticates the iroh peer before resolving a
named service to a fixed local socket.

Targets and listening ports must use loopback. There is no caller-supplied IP
destination, LAN forwarding, public port exposure, DNS interception, or virtual
network interface. A local listening port grants access as that host's adapter:
all processes able to reach the host's loopback are inside this trust boundary.
Do not expose these ports to mutually untrusted VM guests. Per-VM networking and
guest identities require a separate attachment and isolation design.

## Committed policy and outages

`replace_tunnels` replaces the whole topology with an expected revision and an
idempotent request ID. It uses Fleet's normal durable client request journal and
Raft receipt handling. Invalid references, unauthorized routes, duplicate ports,
non-loopback sockets and oversized topologies are rejected before state changes.

Adapters fetch a quorum-confirmed, filtered view, fsync it to `policy.json`, and
apply it locally. Any policy revision closes existing tunnels and rebinds the
configured listeners. Applications reconnect using their normal TCP recovery
logic. A bind conflict fails the adapter's policy closed and retries; Fleet does
not pick a different port silently. The catalog describes desired state, not a
guarantee that every adapter has applied it.

During a controller outage, an adapter keeps its last committed policy. It can
also restart from that policy without a controller. New routes and permission
changes require a quorum. Revocation takes effect when an endpoint receives the
new policy; it cannot instantly revoke a disconnected adapter's saved permission.
For urgent revocation, stop or network-fence the affected adapters. Removing an
adapter retains an identity tombstone, allowing it to fetch an explicit empty
policy after restarting rather than mistake removal for an outage.

Emergency control-plane recovery creates a new authority incarnation and clears
the recovered tunnel topology. An old log could predate a revocation. Fence the
old authority and affected adapters, review the intended permissions, explicitly
rebind each stopped adapter with `tunnels accept-recovery`, and reapply the
reviewed topology. Adapters never choose between competing recovered authorities
automatically. Rebinding discards cached routes; an interrupted rebind fails
closed.

## Protocol and resource bounds

The `fleet/service-tunnel/1` protocol identifies the authority and service name in
a header capped at 1 KiB. The authenticated connection supplies the caller's
identity. One TCP connection maps to one QUIC connection and bidirectional
stream. FIN propagates separately in each direction. A broken connection closes
both ends; Fleet never replays a partial byte stream.

Each adapter admits at most 64 concurrent connections, including handshakes.
Headers and target connections have a ten-second deadline. Stream and connection
receive windows and the send window are 256 KiB, and copies use bounded buffers
with backpressure. Keepalives preserve idle application connections; a dead QUIC
peer times out. The catalog allows 64 adapters, 256 services, 1,024 routes, up to
eight contact addresses per adapter, and at most 256 KiB of topology JSON. It
retains up to 4,096 adapter identities. These are admission limits, not bandwidth
or CPU reservations.

The default has no public relay or address lookup service. `network.json` names a
stable UDP bind address and explicit relay URLs. Setting `bind` to null disables
direct service transport entirely; at least one relay is then required. Control
traffic continues to use Fleet's existing control-plane transport. Relay traffic
remains authenticated and encrypted end to end by iroh.

## Upgrade and CLI

The previous binary supports generations one and two. Activate generation two
with that binary before installing this release, which supports two and three.
Install all replicas, then explicitly prepare and activate generation three.
Tunnel mutations are refused before activation. Fresh clusters also require
explicit activation from two to three. Installing a binary never activates a
new feature or migrates an incompatible database automatically.

Prepare an adapter with `fleet tunnels prepare --adapter-home PATH --adapter ID
--authority CONFIG --network NETWORK`. The new directory receives a private key,
authority binding and public `adapter.json` descriptor. Supply those descriptors
under the topology's `adapters` map. Define `services` as a map from names to
`owner`, `target`, and `allowed` adapter IDs. Define `routes` as a list of
`adapter`, `service`, and `listen` socket addresses.

Use `fleet tunnels apply topology.json --revision N` and `fleet tunnels status`
through an administrator client. The JSON API exposes `replace_tunnels` and
`tunnels`. Run an adapter with `fleet tunnels run --adapter-home PATH`, or add
`--tunnel-home PATH` to `fleet service configure`. Role directories must be
separate. No administrator privilege or host firewall change is required.

## Garage addressing

Garage runs as an unmodified separate process. Assign each Garage node a unique
loopback port whose meaning is identical on every host, such as A at 31001, B at
31002 and C at 31003. On A, Garage binds 31001 and advertises `127.0.0.1:31001`;
Fleet listens on 31002 and 31003 and tunnels to B and C. Each other host has the
corresponding mapping. All Garage RPC traffic stays on loopback or inside iroh.

Garage retains its own node keys, RPC secret, membership, replication and storage
layout. The adapter does not read or rewrite Garage's protocol. Configure both
the advertised RPC addresses and bootstrap addresses consistently, and preserve
the port assignments across restarts. Existing clusters with physical addresses
in persisted peer lists need an explicit migration plan.

This service transport does not yet make Garage a Fleet artifact backend, manage
S3 credentials, or provision buckets. Those are separate integrations after
transport qualification.

## Qualification

`cargo test --test tunnels` exercises direct and forced-relay traffic, authentication,
service permissions, header limits, revocation and large TCP transfers with independent
half-close. The relay test creates a local relay and removes both endpoints' direct
transports. Control-plane integration tests cover leader loss, duplicate requests,
generation gating, adapter restart during controller outage and signed emergency rebinding.

`fleet-qualify garage --linux-binary /absolute/path/to/fleet --home
/tmp/fleet-control-garage-test` runs pinned stock Garage v2.4.1 in three Docker network
namespaces. Use a Linux binary matching the Docker daemon's architecture and compatible
with Ubuntu 24.04's C library. The fixture has an internal bridge, no published ports and
no host state mounts. Its S3 client also runs inside that network. It removes its Docker
resources on exit and retains a safe `result.json`; fixture state files may contain test
keys and must not be published. Linux CI runs this scenario.

The optional `cargo test --release --features gui --test tunnels tunnel_throughput_diagnostic --
--ignored --nocapture` compares local TCP, direct iroh and relay-only iroh with verified
8 MiB payloads in each direction. It reports three timings per path and includes new
connection establishment. These local measurements do not predict WAN performance;
latency, relay placement, flow-control windows and host contention all matter. There
are no timing thresholds in CI. All local scenarios use one physical failure domain.
