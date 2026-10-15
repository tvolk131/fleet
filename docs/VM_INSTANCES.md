# Tart VM instances

An instance is a persistent VM on a named Fleet worker. The coordinator and CLI manage its desired state; the
worker owns its disk and a detached instance controller reconciles Tart with that state. Jobs remain independent.
Running Fleet jobs inside a guest is a later milestone.

Protocol 3.5 adds [guest management through Fleet](VM_GUEST_OPERATIONS.md): readiness, commands, retained logs,
regular-file transfers and storage inventory. It uses this same Tart backend and preserves its isolation policy.

## First milestone

- Create, inspect/list, start, stop and delete through the existing coordinator and authenticated agent protocol.
- A worker must explicitly enable the backend and choose trusted images and local Tart/Softnet executables.
- Each instance has an immutable specification and a stable caller-supplied ID. Matching create retries return the
  same instance. Deleted IDs remain tombstones, preventing a delayed request from recreating a deleted VM.
- Creation produces a stopped VM. Starting reserves CPU slots, memory and GPU access. Every macOS guest can use
  its virtual GPU even if its current application claims to be CPU-only. `gpu: "exclusive"` is the default;
  `gpu: "shared"` explicitly accepts best-effort sharing with other shared VMs. Native jobs use the same CPU/GPU
  admission gate and native GPU jobs retain exclusive reservations. Existing jobs remain trusted host processes.
- Stopped VMs retain their private disks. Deleting a stopped instance removes its VM disk and retains its record.
- Ordinary stops ask macOS to shut down through Tart's local guest agent (`sudo -n shutdown -h now`),
  then wait up to 30 seconds after the bounded request before falling back to Tart's forced stop. The trusted
  image must support that command for clean shutdown. A missing or uncooperative agent cannot prevent a stop;
  its VM keeps its reservation until the hypervisor confirms termination. Forced stops are logged in
  `controller.log` and can lose unflushed guest writes. Network-policy and free-disk-guard failures stop the
  virtual hardware immediately without waiting for guest cooperation.
  This distinction follows [Tart's stop implementation](https://github.com/openai/tart/blob/2.40.1/Sources/tart/VM.swift):
  stopping virtual hardware does not itself flush writes still buffered by the guest OS.
- Desired state and observed state are separate durable records. The controller uses a process lock, survives an
  agent restart and inspects Tart after its own restart. A lost response never creates a second VM. A host reboot
  is reconciled, with admission checked again before restarting a VM.
- The worker reports instance changes through its numbered event log. The coordinator retains instance records
  and refreshes them on reconnect. Unknown or stopping VMs continue to reserve resources until confirmed stopped.

## Network contract

Only `offline` and `public_internet` are accepted. Rules are applied outside the guest by Softnet before boot.
Both modes disable port forwarding, clipboard sharing, audio, USB accessories, and host directory sharing.

`offline` denies application IP traffic in both directions. A virtual NIC remains attached: the isolated virtual
link still carries ARP and DHCP housekeeping. This is not a promise that the guest has no virtual network device.

`public_internet` permits guest-initiated connections to globally routable IPv4 destinations and their replies.
It blocks the virtual gateway/host, non-public destinations, directly connected local subnets and unsolicited
inbound connections. IPv6 is dropped by the qualified Softnet implementation. Images must use public DNS
resolvers instead of the blocked virtual gateway. The controller checks directly connected public IPv4 subnets
and the administrator's additional block list during reconciliation (normally once per second); a change or
inspection failure stops the VM. This is polling, not an atomic network-reconfiguration barrier. Keep the host's
network configuration stable while untrusted VMs run. Internet access cannot prevent intentional
communication through public relays or applications exposing themselves using outbound tunnels.

The implementation pins and verifies a compatible Tart/Softnet pair. It must never silently fall back to Tart's
default NAT or host-only networking. Network isolation is verified against a controlled host service, another VM,
the local network and a public endpoint. Production training processes and their agents are not restarted for
these tests.

## Worker setup and CLI

The baseline backend requires protocol **3.3**; shared GPU instances require **3.4** on the coordinator and worker.
Both use Tart **2.40.1** and Softnet **0.24.0** on an Apple Silicon Mac. Enable it
only on the workers intended to host VMs. Upgrading the coordinator alone does not enable instances on an old
agent; old agents continue to accept native jobs through the existing protocol.

Install Tart using its upstream distribution. For Softnet, download the official 0.24.0 release archive, extract
it, and run `sudo contrib/install-vm-networking.sh /absolute/path/to/softnet`. The installer checks the exact binary
SHA-256 before installing a root-owned, setuid helper at
`/Library/PrivilegedHelperTools/com.fleet.softnet/0.24.0/softnet`. Softnet needs elevated privileges to create the
virtual network and drops them after initialization. This installer does not restart Fleet or modify sudoers.

Create `$FLEET_HOME/instances.json` on the worker (replace the Tart path and image digest):

```json
{
  "tart": "/absolute/path/to/tart",
  "softnet": "/Library/PrivilegedHelperTools/com.fleet.softnet/0.24.0/softnet",
  "images": {"macos": "ghcr.io/cirruslabs/macos-tahoe-base@sha256:87f3aa5ce21b5c876268f233bdfecf38b4c2a8116fe9bbb718e714cbae187377"},
  "memory_budget_mb": 12288,
  "disk_budget_gb": 128,
  "min_free_disk_gb": 10,
  "block_cidrs": []
}
```

Only the worker administrator changes this file. Image aliases resolve to immutable OCI digests. A VM retains
the image and executable configuration saved when it was created; changing the catalog does not replace existing disks.
CPU/memory admission uses current worker capacity and memory budget, including after a worker restart.
`block_cidrs` can exclude additional public addresses, such as the home router's public/WAN address when its
hairpin NAT could lead back to locally hosted services. No allow rules or Tart arguments come from the guest or
the instance specification. Block-list changes stop a running public VM; stop/start applies the updated rules.

The instance specification is a JSON file, for example `vm.json`:

```json
{"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":64,"network":"offline","gpu":"exclusive"}
```

```sh
fleet instance create devbox vm.json --on m4pro
fleet instance list --on m4pro
fleet instance inspect devbox --on m4pro --json
fleet instance start devbox --on m4pro
fleet instance stop devbox --on m4pro
# Wait for observed.state=stopped before deleting its persistent disk:
fleet instance delete devbox --on m4pro
```

Creation and state changes are asynchronous; inspect until the observed state and revision match the requested
state. `running` describes the hypervisor, not guest application readiness. A matching create with the same ID
is safe to retry. A different specification under an existing ID is rejected. A deleted ID cannot be reused.
State changes include the inspected revision so delayed requests cannot undo a newer command. A transport error
may mean a request succeeded: inspect the original ID before retrying.

For outbound internet access, choose `"network":"public_internet"` in a new instance's immutable specification.
The trusted guest image must use public DNS (for example 1.1.1.1 or 8.8.8.8), since gateway DNS is deliberately
blocked. Images with the Tart guest agent can be administered through Tart's private host control socket,
including when workload networking is offline. Fleet protocol 3.5 exposes this through its authenticated
guest-management commands; see [the guest workflow](VM_GUEST_OPERATIONS.md). Fleet does not publish a
guest-management TCP port. Preparing trusted base images remains an administrator responsibility.

VM state lives under `$FLEET_HOME/instances/<id>/`; Tart disks and its image cache are under the separate
`$FLEET_HOME/instance-tart/`. `controller.log`, `vm.log`, and `tart-command.stderr` explain provisioning or launch
failures. A failed or unknown VM keeps its reservation until a stop is confirmed. Correct a setup problem, then
issue stop to reconcile before starting again. Missing persistent disks are never silently recreated.

## GPU sharing and resource budgets

The immutable `gpu` field accepts two scheduling policies:

| Policy | Coexistence on a worker |
| --- | --- |
| `exclusive` (default) | No other active macOS VM or native GPU job. Native CPU jobs may use remaining CPU slots. |
| `shared` | Other VMs are allowed only if they also select `shared` and all CPU/memory reservations fit. Native GPU jobs still wait. |

For two shared VMs, create each with a distinct ID and a specification such as:

```json
{"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":64,"network":"offline","gpu":"shared"}
```

The pair reserves four CPU slots and 8192 MiB of the worker's VM memory budget. Starting another VM is rejected
if its allocation would exceed either budget. GPU sharing does not permit CPU or memory overcommit. Admission
counts pending starts and failed, unknown or stopping VMs until their stop is confirmed; controller restart uses
the same aggregate checks and current budgets. Budget reductions block new starts/restarts; they do not resize
or evict already running guests. Fleet caps active macOS VM reservations at two per worker, independently of
CPU and memory budgets; stopped VMs do not consume these slots. Pending starts and unconfirmed stops do.
Apple's platform ceiling also counts macOS VMs launched outside this Fleet home, which Fleet cannot reserve
atomically. If external VMs exhaust that capacity, the launch fails with Tart's error in `inspect` and `vm.log`;
stop the failed instance to release its reservation, then retry after capacity is available. See
[Parallels' documentation of the Apple Silicon macOS VM ceiling](https://kb.parallels.com/en/128867).

These are scheduling policies, not GPU partitions: `shared` supplies no GPU percentage, fairness guarantee or
performance floor. `exclusive` prevents competing Fleet GPU reservations; it does not change Tart's graphics
device. This backend does not offer `gpu: "none"`: every macOS VM has GPU access, and Tart's `--no-graphics` only
hides the window. A guest administrator cannot be trusted to honor an application-level CPU-only setting.
See [Tart's macOS graphics configuration](https://github.com/openai/tart/blob/2.40.1/Sources/tart/Platform/Darwin.swift)
and [launch options](https://github.com/openai/tart/blob/2.40.1/Sources/tart/Commands/Run.swift).

`cpus` sets the virtual CPU count and reserves that many Fleet slots. Tart does not expose physical-core pinning
or a dedicated-core mode. macOS schedules all vCPUs on the host; even without overcommit, shared caches, memory
bandwidth and thermal limits affect performance. Fleet therefore does not advertise a dedicated/shared CPU mode.
The configured guest memory size is enforced by the hypervisor, while the aggregate VM budget is enforced by
Fleet admission. Leave host overhead outside that budget. These controls are described in
[Tart's resource settings](https://github.com/openai/tart/blob/2.40.1/Sources/tart/Commands/Set.swift).

Existing specifications without `gpu` retain exclusive behavior. Serialization omits the default `exclusive`
field for compatibility with 3.3 agents; `shared` requests are refused before sending to an older agent.
Changing modes requires a new instance in this milestone. Multiple stopped VMs are supported under either mode.
CPU jobs declared through protocol 3.3 can share the remaining CPU slots.
Older job records lack GPU declarations and are conservatively treated as GPU jobs. Paused jobs release CPU slots
and release their GPU claim unless `pause_keeps_gpu` is set. Native memory use remains cooperative; leave generous
headroom in `memory_budget_mb`. The disk budget accounts for configured VM capacity, while the free-space guard
checks actual storage before and during cloning and while a VM runs, stopping the download or VM at the guard;
it is not a filesystem quota on Tart's image cache.

## Boundaries and validation

No VM-to-VM networks, LAN bridging, published ports, migration, GUI workflow, or guest job runner in this milestone.
Native jobs have cooperative CPU/GPU declarations, not memory limits; VM memory admission therefore needs an
explicit worker budget that leaves room for native jobs and the host. Disk admission includes a free-space guard.

Automated tests cover validation, path safety, policy construction, admission, duplicate/replayed requests,
controller/agent/coordinator recovery, disk persistence and deletion. A fake Tart executable exercises failure
paths without a hypervisor. Real tests use separate Fleet homes, a separate Tart home, and disposable VMs on the
M4 Pro. Existing training runs and checkpoints are left untouched.

### M4 Pro qualification, 2026-10-12

Qualified with Tart 2.40.1, Softnet 0.24.0, and the pinned Tahoe base image above (macOS 26.6.2),
using 2 vCPUs and 4096 MiB per test VM. Separate Fleet keys, coordinator/worker homes and Tart storage were used.

| Probe | Offline | Public internet |
| --- | --- | --- |
| Public IPv4 TCP connection and replies | Blocked | Allowed |
| Host service via host LAN IP and virtual gateway | Blocked | Blocked |
| LAN gateway service, reachable in a host control test | Blocked | Blocked |
| Service inside a second disposable VM | Blocked | Blocked |
| Unsolicited host connection to a verified guest listener | Blocked | Blocked |
| Public IPv6 connection | Blocked | Blocked |

Public DNS plus an HTTPS request succeeded in public mode. Guest administration used Tart's local guest-agent
channel. VM disks survived stop/start. Restarting the coordinator, worker and instance controller preserved the
guest's boot time. A two-thread native CPU job completed alongside the VM; GPU work and a three-thread job
stayed queued on the four-slot test worker. Terminating that VM's Softnet helper stopped Tart, with no networking
fallback. Both Fleet-managed instances and the peer fixture were deleted after qualification.

The baseline default-feature Rust suite passed all 59 tests, including eight VM tests; the Python client suite passed
all six tests. The GUI-enabled suite and Clippy with all targets/features also passed. A parallel regression run
encountered two timing-sensitive existing process-test failures (progress observation and follower restart timing);
both passed without test changes on rerun and in the final serial suite.

These checks qualify the stated configuration and ordinary IPv4/IPv6 connectivity; they are not a hypervisor
security audit or proof against deliberate public relays, router hairpin NAT, or every possible host network change.

### Shared GPU scheduling validation

The shared-mode follow-up adds automated coverage for exclusive/shared conflicts in both directions, aggregate
CPU and memory admission, physical-memory bounds, conservative unconfirmed reservations, old-spec defaults and
protocol gating. Process tests use a Tart test double to run two instances, recover both after simulated reboot,
and reject a restart when the aggregate memory budget has shrunk. The coordinator/agent test starts two shared
VMs concurrently, rejects incompatible native jobs, and preserves both through agent/coordinator reconnection.
This initial follow-up validated scheduling and lifecycle behavior without simultaneous guest GPU measurements.
Its complete regression suites passed: 63 default-feature tests and 69 GUI-enabled tests, including all 12 VM
tests in each build. Formatting and Clippy with all targets passed in both configurations. Production agents
and training runs were not changed for this follow-up.


### Concurrent VM qualification and local release, 2026-10-12

The final implementation was qualified on the 14-core, 48-GiB M4 Pro with two offline, shared-GPU VMs,
each with 2 vCPUs and 6144 MiB. The experiment used separate Fleet identities, coordinator/worker homes,
Tart storage, copied trainer/checkpoint inputs and output directories. A bounded production placeholder job
reserved four CPU slots and the GPU during performance measurements, then was cancelled after both VMs stopped.
No existing training run was resumed, cancelled or modified.

Real admission checks rejected a third active VM, an exclusive VM alongside shared VMs, insufficient memory,
and incompatible native work. A two-thread native CPU job completed alongside the pair. Both guest boot times
survived coordinator, worker and simultaneous controller restarts. The clean-shutdown change preserved all 11
payload file hashes after copying about 54 MiB and immediately stopping each guest without a separate sync.
An earlier force-stop attempt lost newly buffered files; its incomplete performance results were excluded and
retained separately. Regression coverage now also includes late Tart launch failure and forced-stop fallback.

Both VMs remained booted in every measurement. “Solo” means one active workload and an idle peer; “paired” means
two simultaneous workloads. Public Metal Performance Shaders performed 1024×1024 FP32 matrix multiplications,
128 operations per command buffer, in 20-second windows. All output elements passed verification. There were
two solo windows per guest and three paired windows, with more than 95% overlap in every pair.

| Workload | Solo median, matrices/s | Paired median, matrices/s | Paired / solo |
| --- | ---: | ---: | ---: |
| VM A | 2323 | 1321 | 56.9% |
| VM B | 2327 | 1282 | 55.1% |

The paired aggregate median was 2589 matrices/s versus a pooled solo median of
2327 (1.11× total throughput). This is observed contention,
not an enforced GPU split or a minimum service level.

Every Dominion case completed four iterations of 128 games with finite metrics, using the same archived binary,
copied checkpoint, seed, two rollout shards and a fresh private CubeCL autotuning cache. The later update column
is the median of update time divided by optimizer steps for iterations 2–4; it does not assert that all tuning
has finished. Total wall time includes startup and tuning. The paired trainers were released together.

| Case | Total wall time | Later update per optimizer step | Total optimizer steps |
| --- | ---: | ---: | ---: |
| trainer-solo-a / shared-a | 239.0 s | 348.4 ms | 272 |
| trainer-pair / shared-a | 228.4 s | 358.8 ms | 272 |
| trainer-pair / shared-b | 237.9 s | 331.9 ms | 272 |
| trainer-solo-b / shared-b | 219.5 s | 315.6 ms | 272 |

Normal desktop/development activity remained active. These are small-sample qualification results; trainer
autotuning, early stopping, sample counts and cache history can vary. They are neither a dedicated-host capacity
guarantee nor a repeat of the earlier native-versus-VM comparison, which used larger guests. Keep exclusive GPU
scheduling for predictable Fleet admission; choose shared mode when variable completion time is acceptable.

Final verification passed 66 default-feature Rust tests and 72 GUI-enabled tests (15 VM tests in each), six Python
client tests, formatting, and Clippy with all targets/features and warnings denied. The GUI-enabled release build
passed. A run made while guests were booting hit two existing process-test timing assertions; both complete final
suites passed with the VMs stopped, without changing those tests.

The tested binary is installed at `~/fleet/bin/fleet`; the local coordinator and M4 Pro worker now use protocol
3.4. The M1 Pro remains online on protocol 3.2. The worker has a 12-GiB aggregate VM memory budget, 128-GB configured
disk-capacity budget and 10-GiB free-space guard. A disposable VM created through the installed release booted
macOS, stopped and was deleted. All qualification VMs and fixture services were removed/stopped. All 78 original
coordinator jobs retained their states, execution metadata, iteration values and validation series; all 66 original
local worker job records were unchanged. Reconnecting refreshed three historical progress timestamps and cleared
their cached rates/ETAs because probe origins live in agent memory. No training was restarted. The reservation
is the only new production job record, and it is no longer active.

See the machine-readable qualification report (local hardware qualification record; excluded from the public source package). The private local release
directory `~/fleet/releases/tart-vm-20261012` retains a source archive, checksummed binary, raw results, test logs,
and the previous binary/database/event-log backups. To roll back, first stop all VMs with the new binary and
confirm termination, disable the worker's `instances.json`, restore the previous binary, then restart the local
coordinator and worker. Do not restore the database snapshot over jobs submitted after the snapshot.

The subsequent [guest-management release](VM_GUEST_OPERATIONS.md#qualification-and-local-release-2026-10-12)
upgrades the local coordinator and M4 worker to protocol 3.5. It adds guest operations and storage visibility
without changing Tart, Softnet, these resource policies or the native job backend.

Protocol 3.6 also supports [queued jobs in disposable VMs](VM_JOBS.md). Those instances carry a job ID and are
managed by the job supervisor; use job logs, outputs and cancellation instead of direct instance mutations.
Persistent instances and VM jobs share the same admission budgets and network policies.
