# Rust runtime and qualification tools

Fleet's runtime, guest helpers, test workloads, and qualification harness are Rust. Python is not
required on the worker, in a newly provisioned VM, or by the test suite. User jobs may still choose
their own languages and runtimes.

## Guest executable

Build and package `fleet`, `fleet-guest` and `fleet-runner-vz` together. Install the companions beside
the actual Fleet executable, including when the service uses a symlink to a release directory. The helper must match
the guest platform; the supported macOS-on-Apple-Silicon deployment uses the macOS ARM build. Fleet
ad-hoc signs `fleet-runner-vz` with the virtualization entitlement on first use.

```sh
cargo build --release --locked --bin fleet --bin fleet-guest --bin fleet-runner-vz
```

`crates/fleet-guest` contains the shared request types and the small guest implementation. It does
not link the control plane, network stack, GUI, or worker runtime. Fleet copies the executable through
its guest agent (`fleet guest-exec`, vsock port 1024) using fixed shell code and binary stdin. The
destination includes its SHA-256, so different release helpers can coexist. Installation is serialized
per VM and verified with a protocol readiness response. A guest reboot that clears `/tmp` causes the
next readiness probe to reinstall the helper. Guest workload operations retain their existing durable
identities and are never replayed to repair an ambiguous result.

The helper executes commands, verifies uploads, checks file versions during downloads, inventories
outputs/checkpoints, and samples JSONL metrics. Host validation, byte/time limits, and VM isolation
remain authoritative: the guest and all its replies remain untrusted. This change adds no shared host
directory, network exposure, or administrator requirement.

Existing running supervisors retain their own executable. This is an internal implementation change;
it does not change the replicated state-machine generation or physical metadata format. Deployment
must include the companions before admitting new VM work. The normal installed service is not
upgraded by qualification.

## Qualification commands

```sh
cargo build --workspace --all-features --bins --locked
cargo test --workspace --all-features --locked -- --test-threads=2
target/debug/fleet-qualify --help
```

The runner invokes real Fleet binaries through the normal CLI and JSON interface. It does not have
an alternate client implementation or silently retry failed assertions. Its fixture endpoint confines
writes to fresh `/tmp/fleet-control-*` homes, validates service-role paths, and checks recorded PID
identities before signaling processes. SSH mode uses a matching Rust runner on each host and keeps
the launch session alive until its child exits. Local launchd mode uses disposable user services.

| Contract | Coverage |
| --- | --- |
| Public JSON CLI/service behavior | `tests/cli_contract.rs`: inputs, executable permissions, logs/cursors, output overwrite refusal, queue dependencies/cancellation, durable retries/conflicts/unknown outcomes, staging service, storage status, service rendering and identity permissions |
| Host-fixture safeguards | `tests/cli_contract.rs`: path escapes, foreign service roles, foreign PIDs, owned children and held launch sessions |
| Rolling-reclamation predicates | `contrib/rust/vm.rs` unit tests; unavailable reports cannot confirm cleanup |
| Synthetic baseline/candidate/incompatible builds and rolling upgrades | `fleet-qualify build-upgrade-fixtures` and `rolling-upgrade` |
| Checkpoint retention and backup-only restore | `fleet-qualify checkpoint --fake --rolling --backup-restore` |
| Portable jobs, attempt history and storage replacement/loss | `fleet-qualify portable --fake --storage-lifecycle` (optional `--baseline-binary`) |
| Normal local/SSH services, membership and disaster recovery | `fleet-qualify normal-multihost --local` or `--inventory`; optional local `--launchd` |
| Stock Garage over authenticated Fleet service tunnels | `fleet-qualify garage --linux-binary /absolute/path/to/fleet --home /tmp/fleet-control-garage-test`; three Docker namespaces, pinned Garage/runtime/curl images, internal bridge and no host ports |
| Native/VM coexistence, live outputs/metrics, worker/leader failure and persistent VM lifecycle | `fleet-qualify normal-vm`; real mode requires an explicit `--instances` `vz` catalog |
| Worker and recovery failure scenarios | Consolidated into the normal VM and multihost scenarios above; authenticated partition/minority refusal and old-authority fencing also remain in `tests/control.rs` and `tests/control/recovery.rs` |
| Simulated VMs and workload programs | The `fake` VM driver (fault switches in `vm-faults/` beside the fake image) and the `fleet-test-workload` binary under `tests/fixtures` |
| Systemd validation | `fleet service validate-unit`; the installer takes the absolute Fleet executable as its fourth argument |

The checkpoint scenario now stages a real executable input. Its disk-reclamation assertion accounts
for that executable's measured byte length in addition to the two retained checkpoint payloads. The
backup scenario uses Fleet's existing durable input bundle so both the executable and checkpoint
can be restored from the separate storage home after worker-cache removal. The portable scenario separately proves that executable and
dataset backups survive loss of the original worker, client cache, and input paths.

The consolidated family has no previous release. Rolling qualification therefore builds explicitly
synthetic future releases with ranges 1, 1..2 and 3..4. It tests continuity, activation and incompatible
refusal without claiming historical compatibility. Release qualification must add immutable prior
binaries once a second family release exists. Fake-driver checks exercise current guest protocol
contracts; they do not qualify the real hypervisor.

CI runs on Linux and macOS without a Python setup step. Qualification reports contain hashes and
results; fixture keys, journals, and job data are never uploaded. Actual multi-machine and real-VM
deployment checks remain separate from local simulated-VM qualification.
