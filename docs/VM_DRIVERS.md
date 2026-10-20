# VM drivers

Status: the trait, the `fake` driver and the production `vz` driver are implemented, pass the
conformance suite and are what the worker runs (`vz` by default, `fake` for tests and qualification);
`krun` is still a spike. The host network agent is not built yet, so only `offline` VMs run.

Fleet runs VMs through its own drivers, which replaced Tart and Softnet. A driver turns a Fleet-owned instance
record into a running guest on one host and reports what it observes. Fleet stays the only
authority for what should exist; a driver never invents, renames or garbage-collects instances on
its own.

## Binding constraints

1. **Fleet owns the guest's network card.** The hypervisor gives Fleet raw Ethernet frames and
   nothing else. Addressing, DHCP/RA, DNS, filtering and the iroh transport are Fleet's host agent,
   not the driver's and not a helper like Softnet.
2. **Fleet is the only source of truth.** No second registry of VMs (Tart's `~/.tart`, a daemon's
   database). Driver state on disk is a cache Fleet can rebuild or discard.
3. **Permissive licences only** (Apache-2.0, MIT, BSD) for anything Fleet links or ships.
4. **No root to run VMs.** Installing Fleet may need an administrator once; starting, stopping and
   networking a VM must not.

Not binding: macOS 14 support (macOS 15+ is fine), macOS guests on every backend.

## Backends

| Driver | Host | Guests | Hypervisor | Network card | Notes |
| --- | --- | --- | --- | --- | --- |
| `vz` | macOS, Apple silicon | macOS, Linux | Virtualization.framework via `objc2-virtualization` | `VZFileHandleNetworkDeviceAttachment` (unix datagram, one Ethernet frame per datagram) | Needs the `com.apple.security.virtualization` entitlement. The only way to run macOS guests. Two macOS guests per host at most. |
| `krun` | macOS (HVF), Linux (KVM) | Linux | libkrun (Apache-2.0) | unixgram backend (same frame-per-datagram shape) | Vulkan via Venus for GPU. Same driver on Mac and Linux boxes. |
| `fake` | any | none | none | socketpair | Deterministic test double for the conformance suite and control-plane tests. |

Native processes stay a separate executor (`backend.rs`); they are not a driver. Cloud Hypervisor
is only worth adding if NVIDIA GPU boxes are. Apple's Containerization library is the fallback if
`vz` proves too costly to own, but needs a patch to hand Fleet the network card.

Tart was removed outright, not kept behind a flag: its licence (FSL), its root-owned Softnet helper
and its own VM registry each break a binding constraint.

## Process model

Each VM lives in its own **runner** process, started detached in its own process group, so
restarting the worker never touches guests. The `fake` runner is `fleet driver-runner fake <dir>`;
the `vz` runner is its own executable, `fleet-runner-vz <dir>`, so the virtualization entitlement is
never granted to the rest of Fleet. Both take `--lock-fd` and `--restore` and speak one protocol.

- Virtualization.framework ties a VM to the process that created it, and libkrun's
  `krun_start_enter` takes over the calling process. Either way one process per VM is forced, so
  the driver supervises runners and never links a hypervisor itself.
- The driver takes `runner.lock` before launching and hands the same open file to the runner as
  descriptor 3, so the lock is held continuously from before launch until the runner is gone.
  Lock held means a runner is alive; that is the only liveness test, so a reused pid is never
  signalled.
- The runner creates the network card, keeps the host end for the VM's whole life and serves the
  control protocol (`driver/protocol.rs`): status, stop, network card, guest stream, save. On a
  clean exit it writes `exit.json` with the reason.
- A restarted worker adopts runners simply by observing them: there is nothing to reattach.
- Control sockets live in `/tmp/fleet-<uid>/` (or `$FLEET_RUNTIME_DIR`), named by a hash of the
  instance directory, because macOS limits Unix socket paths to 104 bytes. The directory must be
  owned by the user with mode 0700.
- Descriptors are passed with `SCM_RIGHTS`, and the runner keeps its copy open until the client
  hangs up: on macOS, closing the sender's copy of a socket descriptor still in flight lost about
  one in five of them in the conformance suite.
- A save ends the runner only after the driver has the reply, so a save is never reported as a
  dropped connection.
- `krun` will get a `fleet-runner-krun` executable the same way, so `fleet` never links libkrun.

## Storage and images

- Disks are raw images. Instance disks are `clonefile(2)` copies of a base image, so they are
  instant and copy-on-write on APFS. On Linux hosts, `FICLONE` where the filesystem supports it,
  otherwise a full copy.
- A Fleet image is a flat bundle directory: an `image.json` manifest naming files beside it, either
  a Linux kernel, optional initrd, command line and optional disk, or a macOS hardware model,
  auxiliary storage (NVRAM) and disk. Its identity is `blake3:<hex>`, the iroh-blobs collection
  hash over every regular non-dot file (`driver/bundle.rs`, printed by `fleet image digest`), which
  the trusted catalog pins. `fleet image import-tart` (`vz::import_tart`) turns a Tart VM directory
  into a bundle with clones, so existing images carry over without a download. Still to do:
  transferring bundles between workers with iroh-blobs; today each worker needs its own copy.
- `prepare` clones every file of the bundle into the instance directory and records which bundle it
  came from. A running instance never reads its image again, an instance cannot silently switch
  images, and disks only grow.

## Networking contract

The driver creates a `SOCK_DGRAM` unix socketpair, hands one end to the hypervisor as the guest's
only network card, and returns the other end to Fleet's host network agent. One datagram is one
Ethernet frame. Fleet chooses the MAC address. An `offline` VM still gets the socket, and the agent
simply forwards nothing but the Fleet service channels.

No vmnet, no bridged or shared NAT mode, and no host-side `pf` rules: that is what constraint 1
buys us. The host agent (separate work) turns frames into each workload's stable virtual IPv6
address carried over iroh.

## Guest channel

`fleet-guest --agent` listens on `driver::AGENT_PORT` (vsock 1024) inside the guest; `fleet
guest-exec` and the worker's guest operations use it. The driver exposes `connect_guest(port) -> stream`; the runner bridges it from
`runner.sock` to the hypervisor's vsock device.

A graceful stop presses the virtual power button and also sends `{"kind":"shutdown"}` to the agent
port, retrying while the guest boots. Neither guest tested honours the power button: the Kata test
kernel has no GPIO power-key driver, and macOS ignored it for the whole 90-second deadline. So in
practice graceful stop depends on the agent, and macOS guests without it are forced. For macOS
images, `sudo contrib/install-guest-agent.sh <bundle> <fleet-guest>` installs the agent as a
LaunchAgent. The agent shuts macOS down with `shutdown -h now` and Linux with `poweroff`; a Linux
guest without `poweroff` (its init is the agent) is synced and powered off by the kernel.

## Driver trait

Implemented in `src/driver/`: the `Driver` trait and its types (`mod.rs`), the runner protocol
(`protocol.rs`), the generic runner (`runner.rs`), the `fake` backend (`fake.rs`) and the
conformance suite (`conformance.rs`).

`RunnerDriver<B: Backend>` implements `Driver` for every backend. A `Backend` only declares its
`Capabilities`, lays out an instance directory in `prepare`, and says how to launch its runner;
inside the runner a backend implements `runner::Vm` (start or restore, request stop, connect to a
guest port, save, wait).

An instance directory holds `spec.json` (the recorded spec and generation), `start.json`
(generation and pid of the last launch), `exit.json` (why the last runner ended), the two locks and
`runner.log`. `observe` derives the phase from them:

| Runner lock | Files | Phase |
| --- | --- | --- |
| no `spec.json` | | `Absent` |
| held | socket answers | `Running` or `Stopping` |
| held | socket not answering yet | `Starting` |
| free | `exit.json` says failed | `Failed` |
| free | any other `exit.json`, never started, or launched before the host last booted | `Stopped` |
| free | `start.json` without `exit.json`, same host boot | `Lost` |

Every call is fenced by generation: an older generation than `spec.json` gets `Error::Stale`.
Repeating a generation with a different spec is rejected; `delete` forgets the generation.
`Lost` stays visible until the next `start`, which boots afresh.

`fleet::driver_conformance!(harness)` generates one test per case. A harness provides fresh
drivers over shared state (a new driver stands in for a restarted worker) and two test images that
follow the test-guest contract documented in `conformance.rs`. `tests/driver_conformance.rs` runs
all 20 cases against `fake`, and `tests/driver_conformance_vz.rs` runs them against real
Virtualization.framework guests.

`Running` means the VM started, not that the guest has booted: the test helpers retry guest
connections, and wait for a first frame before sending, as any client of a booting machine must.

## The `vz` driver

`src/driver/vz/`: `VzBackend` (supervising side: bundles, `prepare`, signing) and the
`runner::Vm` implementation used by `fleet-runner-vz`. The runner is ad-hoc signed with
`contrib/vz.entitlements` (`vz::sign_runner`); signing needs no identity.

The real-VM tests are ignored by default, because CI's macOS runners are VMs without nested
virtualization:

    tests/vm-guest/build.sh                                  # test guest bundles in target/vm-guest
    cargo test --test driver_conformance_vz -- --ignored     # 20 cases, about 2 s
    FLEET_VZ_MACOS_TART=<Tart VM dir> cargo test --test driver_vz_macos -- --ignored --nocapture

`tests/vm-guest` is the Linux test guest: a static Rust `/init` on Kata Containers' 4.2.0 guest
kernel (pinned by SHA-256 in `build.sh`). Ten consecutive runs of the suite passed, leaving no
runner processes or sockets behind.

macOS guest, Tahoe base image imported from Tart's cache, 4 vCPUs and 8 GiB, M4 Pro:

| | |
| --- | --- |
| Start (`startWithCompletionHandler`) | 4.4 s |
| First frame from the guest on its card | 5.6 s later (IPv6) |
| Save | 3.5 s, 3.2 GiB |
| Restore in a new runner, new card | 5.6 s; frames within 2 ms |
| Graceful stop with no agent | power button ignored; forced at the deadline |

## Open questions

Answered by the spikes below, except 2 (macOS install).

1. **Adoption and crashes.** Can a restarted worker reliably reattach to a running runner, and
   does a hypervisor-side crash (guest panic, VZ error) show up distinctly from a runner crash?
2. **Long-running prepare.** Is "prepare with progress, fenced by generation" enough for a macOS
   IPSW install, or does it need to be its own resumable operation?
3. **Save/restore.** What does `saveMachineStateTo` actually accept: which devices block it, does
   the file-handle network card survive restore, and can a state file move between hosts? libkrun has
   no equivalent, so checkpoint migration may be `vz`-only.
4. **Network card details.** MTU, maximum datagram size, behaviour when Fleet's end is slow or
   closed, and whether the guest sees a link-down.

## Spikes

Throwaway code, outside the workspace so nothing in the product depended on it, and deleted once
the production driver replaced it; only its findings remain, below.

**vz runner**: one binary that boots an arm64 Linux kernel and initramfs with
`VZLinuxBootLoader`, a serial console to a file, a `VZFileHandleNetworkDeviceAttachment` on a
socketpair, and a virtio-socket device. Pass when it can:

- boot to a shell on the serial console without root, signed ad hoc with the entitlement;
- show frames from the guest arriving on the host end of the socketpair (answers question 4);
- round-trip bytes over vsock from host to a guest listener;
- stop gracefully (`requestStop`) and forcefully, and report the guest stopping on its own;
- pause, `saveMachineStateTo`, exit the process, start a new process and `restoreMachineStateFrom`
  with the same configuration, and find the guest still running (answers question 3).

**krun runner**: the same checks with libkrun, minus save/restore.

The findings get written back into this document, and the trait is changed to fit them before any
production driver is written.

## Findings: `vz` spike, 2026-10-19

Host: Apple silicon MacBook Pro, macOS 26.6.2. Guest: Kata Containers 4.2.0 arm64 kernel (Linux
6.18.35, everything built in) plus a 230 KB initramfs whose only file is a static Rust `/init`.
Runner: debug build, ad-hoc signed with only `com.apple.security.virtualization`, run as a normal
user.

| Check | Result |
| --- | --- |
| No root | Passes. Ad-hoc signature plus the entitlement is enough. |
| Boot | `start` completes in about 80 ms; the kernel reaches `/init` 58 ms later. |
| Network card | Guest frames (our 0x88b5 test frames and the kernel's own IPv6 ND/MLD) arrive one per datagram on Fleet's end of the socketpair. Frames written by the host reach the guest's `AF_PACKET` socket. |
| MTU | Default 1500. `setMaximumTransmissionUnit` up to 65535 is accepted and the guest's `eth0` reports the same MTU (the virtio MTU feature). A host frame larger than MTU + 18 is **truncated**, not dropped, so Fleet must enforce frame size itself. |
| vsock | Host-to-guest connect and round trip in 0.5 to 1.5 ms. Connecting to a port nobody listens on fails immediately with `ECONNRESET`, not a timeout. |
| Save | `validateSaveRestoreSupport` accepts the file-handle network card, vsock, virtio console and entropy devices. Pause plus save takes 123 ms and writes 12.8 MB for a 512 MB guest (only touched memory is stored). |
| Restore | After `kill -9` of the runner, a **new** process with the same configuration (and the same persisted `VZGenericMachineIdentifier`) restores in 153 ms. The guest continues mid-count, and vsock and networking work at once on a **new** socketpair. |
| Graceful stop | `requestStop` is ignored by a guest that does not handle the power button. |
| Guest power-off | Observed as `stopped`; the runner exits 0. |
| Force stop | `stop` completes in 1.4 ms. |
| Runner death | `kill -9` of the runner also ends its `com.apple.Virtualization.VirtualMachine` XPC process: the guest is gone. |

What this settles:

1. **Adoption and crashes.** Runners must outlive the worker, as planned, and a runner crash is a
   lost VM, not a recoverable one. The only recovery is the last saved state, so "how often to save"
   becomes a policy question for checkpointed workloads. `Phase` needs a distinct `Lost` (runner
   gone without a clean stop) next to `Stopped`. Telling a guest power-off from a forced stop needs
   `VZVirtualMachineDelegate` (`guestDidStop` vs `didStopWithError`); polling state cannot.
2. **Save/restore** works with exactly the devices Fleet needs, and the network card is not part of
   the saved state: `restore_state` taking a fresh `NetworkCard` is correct, which is what moving a
   VM between hosts needs. Cross-host restore itself is still untested.
3. **Network card.** The frame-socket contract holds. `InstanceSpec` gains `mtu`, and the host agent
   drops frames over MTU itself, because the hypervisor truncates them.
4. **Graceful stop** goes through the guest agent over vsock, with `requestStop` only for guests that
   handle it (macOS guests do). `StopMode::Graceful` means "ask the agent, then the power button,
   then force after a deadline", decided by the driver, not the caller.

### Network card under stress

| Check | Result |
| --- | --- |
| Throughput | The guest pushed 1514-byte frames at about 1.4 GB/s with Fleet reading; every frame the guest's stack accepted arrived. |
| Fleet stalls | With Fleet not reading, the guest's sends hit back-pressure (`EAGAIN` on a non-blocking send) after about 4 MB, the socket buffer. Nothing already accepted was lost: all of it arrived when Fleet resumed. vsock and the VM were unaffected. |
| Handover | A separate process received a duplicate of the host end over the runner's control socket (`SCM_RIGHTS`), exchanged frames with the guest, closed it and exited; the runner's own copy kept working. |
| Fleet closes its end | The VM keeps running and the guest sees no link change: its sends succeed and vanish. |

So the runner holds the host end for the VM's whole life, and the worker's network agent borrows a
duplicate over `runner.sock` and asks for a new one after it restarts. A worker restart then costs
the guest a pause in traffic (buffered up to the socket size, then back-pressure), not a dead card.
Because a closed end is invisible to the guest, "the card is not connected" has to be Fleet's own
state, reported in `Observed`, never inferred from the guest.

Since done in the production driver: disks (raw images, `clonefile`) and a macOS guest (imported
from Tart, not installed from an IPSW). Still open: the IPSW install (question 2) and restore on a
different host.

## Findings: `krun` spike, 2026-10-19

Same host, and the **same guest**: the Kata kernel also has virtio-mmio, so libkrun boots the
identical kernel and initramfs through `krun_set_kernel`. libkrun 1.19.6 from the `slp/krun`
Homebrew tap, runner ad-hoc signed with only `com.apple.security.hypervisor`, run as a normal user.

| Check | `krun` | `vz` for comparison |
| --- | --- | --- |
| No root | Passes | Passes |
| Boot to `/init` | 24 ms after the kernel starts; the runner enters the VM 0.3 ms after launch | 80 ms start plus 58 ms |
| Processes and memory | One process, 88 MB RSS for a 512 MB guest | Runner 11 MB plus XPC service 88 MB |
| Network card | `krun_add_net_unixgram` with Fleet's socketpair fd: frames both ways, one per datagram | Same |
| Throughput | About 0.96 GB/s of 1514-byte frames | About 1.4 GB/s |
| Fleet stalls | Identical: back-pressure after about 4 MB, everything delivered on resume, vsock unaffected | Same |
| vsock | Host connects to a Unix socket libkrun listens on (`krun_add_vsock_port2(listen=true)`); 0.3 ms round trip | Host calls `connectToPort` on the device; about 1 ms |
| Graceful stop | `krun_get_shutdown_eventfd` written; this guest ignores it | `requestStop`; ignored |
| Guest power-off | libkrun calls `exit(0)`; the runner is gone | `stopped`, runner exits 0 |
| Force stop | Exit the process | `stop`, 1.4 ms |
| Save/restore | None in the API | Works |
| MTU | No API; 1500 | Configurable to 65535 |

Licensing: libkrun (Apache-2.0) links only system frameworks, libepoxy and virglrenderer (both MIT).
It `dlopen`s libkrunfw (GPL/LGPL, the bundled kernel) only when no kernel is supplied; Fleet always
supplies its own. The guest kernel is GPL under either backend: Fleet ships it as a separate
artifact with its source reference, as every VM product does.

## What the two spikes mean for the trait

The hypothesis held. Both backends fit one shape, and the differences are capabilities, not
structure:

- **One runner process per VM, everywhere.** `krun` makes it unavoidable (`krun_start_enter` owns
  the process); `vz` makes it natural (the VM dies with its process). The worker supervises runners
  over `runner.sock` and never links a hypervisor.
- **One runner protocol, two runner implementations.** `status`, `stop`, `kill`, network card
  handover and a guest stream are the same commands for both. `save` exists only where advertised.
  The `Driver` trait in the worker is mostly backend-independent runner supervision, plus per-backend
  `prepare` (image layout) and capabilities.
- **The network card contract is exactly the same**: a unix datagram socketpair, frame per
  datagram, identical back-pressure. One host network agent serves both.
- **The guest channel differs underneath but not above**: `connect_guest(port)` returns a stream
  either way; the runner hides `connectToPort` versus a Unix socket.
- **Graceful stop is the guest agent's job** on both. The backends' own mechanisms (power button,
  shutdown eventfd) are fallbacks for guests that support them.
- **Capabilities that really differ**: save/restore and macOS guests (`vz` only), configurable MTU
  (`vz` only), Linux hosts (`krun` only).

Trait changes from the draft above: `Phase` gains `Lost`; `InstanceSpec` gains `mtu`; `StopMode` is
`Graceful { deadline }` or `Force`, with the driver choosing the escalation; the network card is
created and held by the runner, and the worker borrows it (`network_card(id) -> OwnedFd`) instead of
passing one into `start`.
