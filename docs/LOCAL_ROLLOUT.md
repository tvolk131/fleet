# Local rollout and CI

Local qualification uses three independent replicas; the normal M4 installation runs one replica.
The M1 Pro and Pi remain separate physical deployment targets. Qualification never upgrades the normal
service or adopts existing workloads. A release cutover requires its own idle audit and matching backups.

Three local processes exercise durable storage, elections, membership, client
retries, worker reconciliation, and recovery. They share one hardware failure
domain and do not provide host redundancy. A working single-voter deployment can
later grow cooperatively to three physical replicas.

## Reproducible scenarios

Build the normal binary, then choose a fresh home and unused ports:

```sh
cargo build --workspace --all-features --locked --bins
target/debug/fleet-qualify normal-multihost --local --binary "$PWD/target/debug/fleet" \
  --home /tmp/fleet-control-local-example --port 29300
```

On macOS, add `--launchd` to use temporary services in the current GUI login
session. These use Fleet's normal service renderer and exercise automatic crash
restart. Their labels and log paths are unique to the fixture; cleanup unloads
them. This does not install system boot services. The persistent M4 system daemon
is qualified separately.

The suite uses separate identities, redb databases, worker homes and loopback
ports. It covers CLI/JSON submission, verified inputs and outputs, dependency
cancellation, duplicate retry, leader and combined-service loss, replica rotation,
three-to-one-to-three membership, quorum outage, lost-quorum recovery, client
authority safeguards, worker inventory reconciliation, and cold restart of the
recovered majority. Tiny native canaries use explicit fixture workers; there is
no ML training, host firewall change, or adoption of existing jobs.

## GitHub CI

The workflow runs on Linux and macOS using the pinned Rust toolchain and locked
dependencies. It runs the Rust suite (including fake VM driver contracts
and headless Material 3 GUI tests), Clippy, JSON CLI contracts and fixture safeguards,
then the three-process local scenarios above. Actions are pinned to immutable
commits and receive read-only repository permissions. Artifacts contain only test
logs and the qualification summary, never private keys or complete fixture homes.

Real `vz` VMs/Metal, physical networking/power failure and system-service boot
qualification remain hardware checks. GitHub-hosted tests do not claim to cover
those. No self-hosted runner is installed on a training machine.
