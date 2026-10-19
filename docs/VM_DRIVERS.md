# VM drivers

Status: draft. The trait below is a hypothesis to be tested by the spikes in the last section. It is
not frozen until a `vz` and a `krun` guest have both booted under it.

Fleet replaces Tart and Softnet with its own VM drivers. A driver turns a Fleet-owned instance
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

Tart is removed outright, not kept behind a flag: its licence (FSL), its root-owned Softnet helper
and its own VM registry each break a binding constraint.

## Process model

Each running VM lives in its own **runner** process: `fleet instance-runner <home> <id>`.

- Virtualization.framework ties a VM to the process (and dispatch queue) that created it; the VM
  dies with that process. libkrun's `krun_start_enter` takes over the calling process and never
  returns. Both force one process per VM, so the driver is a supervisor of runners, not a library
  that holds VMs in the agent.
- This matches the existing `instance-controller` (one process per instance, holding
  `controller.lock`, detached into its own process group). The runner replaces it.
- The runner is started detached, so restarting the worker does not kill guests. On start the
  driver **adopts** runners it finds by lock file and control socket instead of starting new ones.
- Only the runner binary needs the virtualization entitlement and code signature, not `fleet`.
- The runner exposes a small control socket in the instance directory (`runner.sock`): `status`,
  `stop {graceful|force}`, `save <path>`, and a vsock bridge. Its exit status and the tail of
  `runner.log` explain a crash.

## Storage and images

- Disks are raw images. Instance disks are `clonefile(2)` copies of a base image, so they are
  instant and copy-on-write on APFS. On Linux hosts, `FICLONE` where the filesystem supports it,
  otherwise a full copy.
- Images stop being OCI references pulled by Tart. A Fleet image is a content-addressed bundle
  (disk, plus kernel/initrd or the macOS auxiliary storage and hardware model, plus a small manifest)
  stored and transferred with the existing iroh-blobs store. The trusted image catalog in
  `instances.json` pins bundle hashes instead of OCI digests.

## Networking contract

The driver creates a `SOCK_DGRAM` unix socketpair, hands one end to the hypervisor as the guest's
only network card, and returns the other end to Fleet's host network agent. One datagram is one
Ethernet frame. Fleet chooses the MAC address. An `offline` VM still gets the socket, and the agent
simply forwards nothing but the Fleet service channels.

No vmnet, no bridged or shared NAT mode, and no host-side `pf` rules: that is what constraint 1
buys us. The host agent (separate work) turns frames into each workload's stable virtual IPv6
address carried over iroh.

## Guest channel

`fleet-guest` listens on a fixed vsock port inside the guest, replacing `tart exec`. The driver
exposes `connect_guest(port) -> stream`; the runner bridges it from `runner.sock` to the
hypervisor's vsock device. The guest protocol in `crates/fleet-guest` is unchanged.

## Driver trait (draft)

```rust
pub trait Driver: Send + Sync {
    /// What this host can run, for placement and admission.
    fn capabilities(&self) -> Capabilities;

    /// Every instance this driver finds on the host, including ones Fleet no longer knows about,
    /// so the caller (not the driver) decides what to delete.
    fn inventory(&self) -> Result<Vec<Observed>>;
    fn observe(&self, id: &InstanceId) -> Result<Observed>;

    /// Idempotent for the same (id, generation). May take minutes (macOS install) and reports
    /// progress through `Observed`; a newer generation fences an older in-flight call.
    fn prepare(&self, spec: &InstanceSpec) -> Result<()>;
    fn start(&self, spec: &InstanceSpec, nic: NetworkCard) -> Result<()>;
    fn stop(&self, id: &InstanceId, mode: StopMode) -> Result<()>;
    fn delete(&self, id: &InstanceId) -> Result<()>;

    /// Optional; gated by `Capabilities`.
    fn save_state(&self, id: &InstanceId, to: &Path) -> Result<()>;
    fn restore_state(&self, spec: &InstanceSpec, from: &Path, nic: NetworkCard) -> Result<()>;
    fn snapshot_disks(&self, id: &InstanceId, to: &Path) -> Result<()>;

    fn connect_guest(&self, id: &InstanceId, port: u32) -> Result<UnixStream>;
}

pub struct InstanceSpec {
    pub id: InstanceId,
    /// Bumped by Fleet on every desired-state change; stale calls are rejected.
    pub generation: u64,
    pub image: ImageBundle,
    pub cpus: u32,
    pub memory_mb: u32,
    pub disk_gb: u32,
    pub mac: [u8; 6],
}

pub enum Phase { Absent, Preparing { progress: Option<f32> }, Stopped, Starting, Running, Stopping, Failed }

pub struct Observed {
    pub id: InstanceId,
    pub generation: u64,
    pub phase: Phase,
    pub error: Option<String>,
}

pub struct Capabilities {
    pub guests: BTreeSet<GuestOs>,
    pub max_macos_guests: usize,
    pub save_restore: bool,
    pub gpu: Option<GpuKind>,
}
```

Synchronous on purpose: every call either talks to a runner over a local socket or does local
file work, and the worker already runs instance control on its own threads.

The conformance suite (`tests/driver_conformance.rs`) is the real specification. It runs against
`fake` everywhere, and against `vz` and `krun` where the host can, and covers: idempotent prepare
and start, generation fencing, stop modes, adoption after the supervisor restarts, crash reporting,
inventory finding orphans, frames flowing over the network card, guest channel round trip, and
save/restore where advertised.

## Open questions

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

Throwaway code under `spikes/`, outside the workspace, so nothing in the product depends on it.

**`spikes/vz-runner`**: one binary that boots an arm64 Linux kernel and initramfs with
`VZLinuxBootLoader`, a serial console to a file, a `VZFileHandleNetworkDeviceAttachment` on a
socketpair, and a virtio-socket device. Pass when it can:

- boot to a shell on the serial console without root, signed ad hoc with the entitlement;
- show frames from the guest arriving on the host end of the socketpair (answers question 4);
- round-trip bytes over vsock from host to a guest listener;
- stop gracefully (`requestStop`) and forcefully, and report the guest stopping on its own;
- pause, `saveMachineStateTo`, exit the process, start a new process and `restoreMachineStateFrom`
  with the same configuration, and find the guest still running (answers question 3).

**`spikes/krun-runner`**: the same checks with libkrun, minus save/restore.

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

Still open: disks (raw images, `clonefile`), a
macOS guest and its IPSW install (question 2), restore on a different host, and everything `krun`.
