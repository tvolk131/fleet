# Tart VM instances

A persistent instance is an immutable VM specification and a desired state committed by the replicated
control plane. Its assigned worker owns the VM disk and reconciles Tart through a detached instance
controller. Disposable [VM jobs](VM_JOBS.md) use the same backend and resource accounting.

## Setup and lifecycle

Prepare a worker with `fleet cluster prepare-worker --instances /absolute/instances.json` and enroll it
as described in [normal setup](NORMAL_FLEET.md). The trusted settings select absolute Tart/Softnet paths,
a catalog of digest-pinned images, VM memory/disk budgets, a free-space reserve, and optional additional
blocked networks. The worker rechecks host resources and its configured budgets before boot.

For example, save this as `vm.json`:

```json
{"image":"macos","cpus":2,"memory_mb":6144,"disk_gb":50,"network":"offline","gpu":"exclusive"}
```

```sh
fleet instance create devbox --worker m4pro --spec /absolute/vm.json --request-id create-devbox
fleet instances
fleet instance start devbox --request-id start-devbox
fleet instance stop devbox --request-id stop-devbox
fleet instance delete devbox --request-id delete-devbox
```

Creation requests the running state. Stop retains the disk; delete removes it after confirmed shutdown
and preserves the identity tombstone. A retry must use its original request ID and payload. Inspect
committed intent and observed state separately: acceptance does not mean the hypervisor has finished.
The logical instance ID differs from the worker's backend execution ID.

Normal clients currently expose instance lifecycle and inspection. Arbitrary persistent-guest commands
and file transfers are not exposed through the current client; [guest operations](VM_GUEST_OPERATIONS.md)
remain an internal backend used by VM jobs.

## Resource guarantees

Fleet accounts for native jobs, persistent VMs, and disposable job VMs together. Pending work reserves
capacity, and stopping/unknown workloads retain reservations until cleanup is confirmed. vCPU counts,
RAM size, configured disk capacity, and VM slots must fit worker enrollment and backend admission.
Apple's macOS virtualization limit is represented by the worker's VM-slot budget.

Tart sets guest RAM and virtual disk sizes. vCPUs are scheduled by the host; they are not pinned physical
cores. Disk growth/admission and host free-space guards are distinct from guest filesystem use.
`gpu: "exclusive"` reserves Fleet GPU admission for one workload; `gpu: "shared"` permits best-effort
coexistence. Every macOS guest can access its virtual GPU. Fleet does not partition GPU compute or VRAM,
and unrelated host processes can still contend for hardware. Native GPU jobs reserve exclusive Fleet
GPU use. Keep the Mac dedicated to Fleet for predictable admission accounting.

## Network and isolation

Only `offline` and `public_internet` are accepted. Softnet applies restrictions outside the guest before
boot. Offline mode denies application IP traffic. Public-internet mode blocks the host, LAN, private,
link-local, reserved and peer destinations, plus configured extra deny CIDRs. Neither mode publishes
ports, bridges the home LAN, or grants VM-to-VM access. There are no host-directory shares, clipboard,
audio or USB devices. Fleet management uses Tart's local guest channel independently of application IP.

The backend requires the verified Softnet helper and fails closed when policy enforcement is unavailable.
Loss of network policy or the free-disk guard stops the virtual hardware. For an ordinary stop, the
controller first requests guest shutdown, then uses Tart's forced stop if the guest does not cooperate.
Forced termination can lose unflushed guest writes; capacity remains reserved until termination is verified.

## Recovery and qualification

The detached controller uses an OS lock and durable desired/observed records. Worker/service restarts
reconcile the existing VM; lost replies do not create another identity. Control-plane recovery still
requires explicit worker authority rebinding and inventory reconciliation. Deletion tombstones prevent
late requests from recreating an instance.

`tests/instances.rs` and `tests/instances/guest.rs` test the actual backend against isolated fake Tart
processes, including network arguments, CPU/RAM/GPU budgets, stubborn shutdown, failed launchers,
controller restart, disk ownership and guest-file safety. Current control-plane and CLI qualifications
exercise the same backend through enrolled workers. Real macOS qualification additionally requires Tart,
Softnet and a pinned image; process tests do not establish physical host-loss independence.
