# VM instances

A persistent instance is an immutable VM specification and a desired state committed by the replicated
control plane. Its assigned worker owns the VM disk and reconciles its [VM driver](VM_DRIVERS.md) through a
detached instance controller. Disposable [VM jobs](VM_JOBS.md) use the same backend and resource accounting.

## Setup and lifecycle

Prepare a worker with `fleet cluster prepare-worker --instances /absolute/instances.json` and enroll it
as described in [normal setup](NORMAL_FLEET.md). The trusted settings select a catalog of digest-pinned
image bundles, VM memory/disk budgets, a free-space reserve, optional [portable](PORTABLE_JOBS.md) `platforms`, and optionally the driver
(`vz`, the default, or `fake` for tests):

```json
{
  "images": {"macos": {"bundle": "/absolute/images/macos", "digest": "blake3:<64 hex>"}},
  "memory_budget_mb": 16384, "disk_budget_gb": 200, "min_free_disk_gb": 20
}
```

An image bundle is a flat directory (for `vz`: `image.json`, hardware model, auxiliary storage and
`disk.img`). `fleet image digest BUNDLE` prints its catalog entry; `fleet image import-tart TART_VM_DIR
BUNDLE` converts an existing Tart VM directory with APFS clones. Install the guest agent into macOS
images with `sudo contrib/install-guest-agent.sh BUNDLE FLEET_GUEST`. Bundles are not yet distributed
between workers: place each one on every worker that uses it. The worker verifies the bundle against
its digest before provisioning each instance, caching the result in `image-digests.json` by file
stamps so it re-hashes only changed bundles (about 27 s for a 50 GB image in a release build). It rechecks host resources
and its configured budgets before boot.

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

The driver sets guest RAM and grows the instance's raw disk to `disk_gb`. vCPUs are scheduled by the host; they are not pinned physical
cores. Disk growth/admission and host free-space guards are distinct from guest filesystem use.
`gpu: "exclusive"` reserves Fleet GPU admission for one workload; `gpu: "shared"` permits best-effort
coexistence. Every macOS guest can access its virtual GPU. Fleet does not partition GPU compute or VRAM,
and unrelated host processes can still contend for hardware. Native GPU jobs reserve exclusive Fleet
GPU use. Keep the Mac dedicated to Fleet for predictable admission accounting.

## Network and isolation

Only `offline` works today. The guest's only network card is a socket held by Fleet, and an offline
VM's card forwards nothing, so it has no application IP traffic. `public_internet` is refused at
creation until Fleet's host network agent exists. No mode publishes ports, bridges the home LAN, or
grants VM-to-VM access. There are no host-directory shares, clipboard, audio or USB devices. Fleet
management uses the guest agent over vsock independently of application IP.

Reaching the free-disk guard stops the VM. For an ordinary stop, the controller first requests guest
shutdown through the agent, then forces the VM off if the guest does not cooperate. Forced termination
can lose unflushed guest writes; capacity remains reserved until termination is verified. A VM whose
runner vanished because the host rebooted counts as stopped, and is started again if desired running.

## Recovery and qualification

The detached controller uses an OS lock and durable desired/observed records. Each instance's driver
state (disk, `runner.log`) lives in `instances/<id>/driver/vm/` under the worker's backend home.
Worker/service restarts reconcile the existing VM; lost replies do not create another identity. Control-plane recovery still
requires explicit worker authority rebinding and inventory reconciliation. Deletion tombstones prevent
late requests from recreating an instance.

`tests/instances.rs` and `tests/instances/guest.rs` test the actual backend on the `fake` driver,
whose guests obey fault switches in a `vm-faults/` directory beside the fake image (`fail-prepare`,
`fail-start`, `fail-after-start`, `gate-failure`/`release-failure`, `refuse-stop`, `slow-boot`). They
cover CPU/RAM/GPU budgets, stubborn shutdown, failed starts, controller restart, disk ownership and
guest-file safety. Current control-plane and CLI qualifications exercise the same backend through
enrolled workers. Real macOS qualification additionally requires a `vz` catalog with a pinned bundle;
process tests do not establish physical host-loss independence.
