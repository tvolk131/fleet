# Queued jobs in disposable macOS VMs

Protocol **3.7** adds live observations and intermediate results to the opt-in Tart execution introduced in 3.6.
A job with a `vm` specification uses the existing Fleet job queue and gets
one private, disposable macOS VM. Fleet reserves capacity, boots the VM, waits for the guest command channel,
stages verified inputs, executes the command, captures declared outputs during execution and at completion, stops the VM and deletes its disk.
The ordinary queue, logs, outputs, fetch, cancellation, dependencies and submission keys continue to work.
Without `vm`, a job still uses the existing trusted native process backend.

Upgrade the coordinator, submitting client and selected worker. The CLI/Python client uses a new `submit_vm_observed`
request so an older local daemon cannot silently use its older execution/output behavior. Remote submission is gated on protocol
3.7. An older worker remains usable for native jobs and is skipped for VM jobs with an upgrade explanation.

## Submit a job

Enable the worker's existing [Tart backend](VM_INSTANCES.md), including its pinned image catalog and Softnet.
The image needs Tart's guest agent and `/usr/bin/python3`, as for [guest operations](VM_GUEST_OPERATIONS.md).

```json
{
  "name": "isolated training",
  "vm": {
    "image": "macos",
    "cpus": 2,
    "memory_mb": 6144,
    "disk_gb": 50,
    "network": "offline",
    "gpu": "exclusive"
  },
  "argv": ["bin/train", "--output", "training"],
  "inputs": {"bin/train": "./bin/train"},
  "publish": ["training/*"],
  "threads": 2,
  "max_seconds": 600
}
```

```sh
fleet submit job.json --on m4pro --dest runs/isolated --json
fleet queue --json
fleet logs JOB -f
fleet outputs JOB
fleet fetch JOB runs/another-copy
fleet cancel JOB
fleet jobs m4pro --json
```

Alternatively, put only the VM specification in `vm.json` and use `fleet run --vm vm.json --on m4pro
--dest results -- ./bin/tool ...`. `submit`, `run` and `sweep` accept `--vm`; a job file can embed `vm` directly.
Local input paths in a job file are relative to that file. No host directory is mounted in the VM.

The command runs as the image's guest-agent user, in a fresh directory inside the guest. Relative executable
paths such as `bin/train` refer to staged files there; absolute paths refer to the guest filesystem. Fleet sends
argv and environment as data without an implicit shell. Use `/bin/sh -c` explicitly when needed. Thread
environment defaults mirror native jobs; the reservation is the full VM vCPU count even if `threads` is smaller.

```python
from fleet import Fleet
f = Fleet()
job = f.submit(
    "isolated training", ["bin/train", "--output", "training"],
    inputs={"bin/train": "./bin/train", "checkpoint": "./copied-checkpoint"},
    vm=dict(image="macos", cpus=2, memory_mb=6144, disk_gb=50,
            network="offline", gpu="exclusive"),
    threads=2, max_seconds=600, publish=["training/*"],
    on="m4pro", dest="runs/isolated", idempotency_key="experiment/phase-1",
)
result = f.wait(job)
assert result["state"] == "succeeded", result
f.fetch(job, "runs/isolated")
```

`wait` reports job completion; automatic fetching may still be catching up. Explicit `fetch` waits for that
request's verified transfers. Save the submission key before sending and reuse it after an ambiguous response;
the same key/specification returns the original job. A deliberate new attempt needs a new key.

## Placement and coexistence

The coordinator queues VM jobs until the selected worker has the image and enough VM memory, configured disk
capacity, vCPU slots and macOS VM slots. The worker rechecks admission alongside persistent VMs and native jobs.
Pending deliveries reserve capacity. A VM job and its corresponding instance count only once. The VM reservation
remains authoritative until stop is confirmed; job completion waits for disk deletion and staging cleanup.

`gpu: "exclusive"` reserves Fleet GPU admission for that VM. `gpu: "shared"` permits coexistence with another
shared VM within the same CPU/memory budgets and two-VM platform limit. Every macOS VM still has GPU access;
this is a scheduling policy, not GPU partitioning. Native GPU jobs remain exclusive, and native CPU jobs can
coexist within CPU slots. The separate VM memory budget must still leave room for native jobs and the host.
VM jobs do not automatically preempt native training in this milestone.

Networking is unchanged: `offline` or `public_internet`, with no LAN/host/peer access, published ports, clipboard
or host-directory shares. Commands/files travel over Fleet's authenticated transport and Tart's local guest
channel, including when application networking is offline. The VM stays awake while active.

## Live metrics and intermediate results

The same `probes` configuration used by native jobs now works inside a VM. No SDK, network listener, or host
folder mount is required. The program writes JSON-lines metrics and ordinary output files in its working directory:

```json
{
  "probes": [
    {"jsonl_field": {"name": "Iteration", "file": "training/metrics.jsonl", "field": "iteration", "total": 2048}},
    {"jsonl_series": {"name": "Loss", "file": "training/metrics.jsonl", "field": "loss", "points": 60}},
    {"glob_count": {"name": "Checkpoints", "pattern": "training/iter-*/manifest.json"}},
    {"log_regex": {"name": "Phase", "pattern": "phase: (.*)", "stderr": false}}
  ],
  "publish": ["training/metrics.jsonl", "training/iter-*"],
  "settle_seconds": 30
}
```

Add those fields to the complete job definition above (or use the corresponding Python submission arguments).
`jsonl_field` reads the latest numeric value; a `total` enables a progress bar and host-calculated rate/ETA.
`jsonl_series` reads recent numeric values. Dotted fields address nested objects. `*` in a metrics-file path selects
the most recently modified matching regular file. The helper ignores incomplete trailing JSON lines. `glob_count`
counts matching paths at the specified depth. Log regex runs on the host's retained stdout/stderr, using Rust's
bounded regex engine; only the extracted text is displayed.

An observation starts approximately every five seconds after the preceding observation/transfers finish. The
worker checks for changed readings every five seconds and normally emits changed progress at most every fifteen
seconds. Large transfers or a busy guest can delay the next observation; this is periodic monitoring, not a
real-time stream or a full metrics database. Rate origins and latest readings survive worker/supervisor restarts.
The GUI draws the existing fixed metric widgets, not arbitrary guest-supplied HTML or code.

While running, a file must have unchanged modification/change timestamps for `settle_seconds` (default 30) before
capture. A checked download compares identity, size and modification/change timestamps before and after reading.
A changing/disappearing file is deferred to a later live observation. Host publication is an atomic file replacement;
same-size changes also trigger automatic fetching. Unchanged captured file versions are skipped, including after
restarts. Completed observation operations and redundant transfer payloads are reclaimed as batches commit, so
polling does not exhaust the instance operation-history limit. The durable execution operation is never replayed.

Use immutable checkpoint directories or write a temporary file and rename it when complete. Settling and per-file
validation do **not** create an application-consistent snapshot across multiple files, and VM-controlled metadata is
not a security attestation of the results. Captured files are untrusted data. Publish checkpoint manifests only when
their constituent files are complete. Guest deletion does not delete an already captured host copy; publishing a
new version at the same path replaces that job's current copy. Use distinct checkpoint paths to retain versions.

Host-side observation limits are 32 probes / 16 KiB of probe configuration, a 1 MiB accepted snapshot, 500 points per
series (default 60), finite numeric samples with absolute value at most 1e100, and 100,000 scanned paths. JSON-lines
probes read at most the last 512 KiB of each selected file. The 256 retained output paths / 4 GiB current inventory
limit applies across observations, including paths later deleted by the guest. Repeated versions and changed-file
attempts also have a 16 GiB cumulative declared transfer budget per job. The existing 1 GiB per-file bound, bounded
guest command channel, timeouts and host free-space guard remain authoritative. Invalid replies, exceeded limits,
uncertain operations or failed final captures fail the job and trigger VM cleanup, retaining previously captured
results. These bounds do not replace a filesystem quota or general blob-store retention policy.

`fleet logs JOB -f`, `fleet queue`, the GUI, and automatic `--dest` fetching continue to work. `fleet fetch JOB DIR`
waits for verified transfers requested at that point; it does not wait for future guest files. Cancellation still
stops the entire VM. Already captured checkpoints remain retrievable afterward, but cancellation does not promise
a final capture or graceful checkpoint creation. Checkpoint-aware suspension/resumption remains future work.

## Recovery, cancellation and output collection

The worker's detached VM-job supervisor orchestrates stable guest-operation IDs. Worker/coordinator/client
restarts do not restart the training command. A recovered supervisor can reconnect to a still-running operation
or consume a completed result. If an attempted guest operation loses its supervisor, its effects are uncertain:
the job fails and cleans up its VM, without automatically replaying it. No automatic training retry or reboot
failover is promised.

`fleet jobs MACHINE --json` includes each VM job's instance ID, phase, outcome and diagnostic. The queue's JSON
includes its VM specification. Phases are preparing, executing, collecting, stopping, deleting and finished. The coordinator queue JSON includes
`vm_status`; the CLI and GUI show these phases, including cleanup after the command exits.
VMs named `job-...` are owned by their jobs; direct guest commands and lifecycle mutations are refused. Use the
job's logs, outputs and cancellation instead. An orphan reservation left by a crash before job publication is
stopped/deleted without running the workload. Retrying the same unpublished job can reclaim that reservation
only when it has never been provisioned and has no guest-operation history; it rechecks current budgets.

Cancellation is serialized with worker submission and persisted even if it reaches the worker before the job.
A late submission cannot start a job already cancelled there. Cancellation stops the entire VM, after the existing bounded guest OS shutdown attempt, then deletes its disk.
This also terminates guest processes that evade process-group signals. Cancellation is not checkpoint-aware and
does not promise to retrieve files still only inside the VM. Already captured files and logs remain retrievable.
The host independently bounds command runtime and stops the VM if its guest command fails to cooperate. A
failed cleanup retains the job/reservation and is retried; a cleanup deadline is not evidence that a VM stopped.

On normal command completion, including a nonzero exit, Fleet inventories `publish` patterns and captures
regular files individually. It validates every relative output path, count and size on the host, rejects symlinks
and overlapping paths, and never unpacks a guest archive onto the host. Directories are traversed inside the
guest; symlink directories are skipped. Downloads check size/modification time and transport hashes. Have the
application close its files before exiting; this is not an application-consistent filesystem snapshot. Failed
captures fail the job while preserving files already captured. The same capture path runs periodically during execution; the final pass also collects files that have not
yet reached their settling time.

The supervisor deletes job input staging and redundant operation payloads only after confirmed VM deletion.
Job logs, captured outputs, small operation records and the VM tombstone remain for inspection. Shared image
caches and content-addressed blob stores are retained; general retention/garbage collection is a later milestone.

## Current limits

- One disposable VM per job; no reusing a persistent VM for queued jobs.
- Up to 128 input files, 256 retained output paths, 1 GiB per file, 4 GiB total inputs and 4 GiB current output inventory.
  Repeated output versions/attempts have the separate 16 GiB cumulative budget described above.
- Retained command stdout/stderr are limited to 8 MiB each, plus a bounded Fleet failure diagnostic. Excess
  output fails the job and stops its VM. These bounds and the free-space guard are not filesystem quotas.
- Command time limit is 1–86,400 seconds, default 86,400; preparation has a separate one-hour limit. Transfers
  retain the existing ten-minute per-file deadline. A cancellation interrupts job VM image cloning as well.
- No interactive terminal, pause/continue, checkpoint suspension/resumption, migration
  or preemption. Unsupported native-only job options are rejected rather than silently applied differently.
- The existing GUI displays VM job phases, progress bars, rates/ETA, numeric series/charts, counts, text, logs and cancellation; it hides their unsupported pause action.
  Dedicated VM configuration/management screens remain separate work.

These are Fleet orchestration changes. Tart and Softnet remain at their qualified versions, and VM isolation
and the existing native training backend remain unchanged.


## Initial VM-job qualification (protocol 3.6), 2026-10-12

The M4 Pro qualification used separate Fleet identities, coordinator/worker homes and Tart storage, plus copied
archived Dominion inputs. A new bounded placeholder job reserved production CPU/GPU admission; no existing
training was started, stopped or changed. The offline VM used two vCPUs, 6144 MiB and a 50-GiB disk.

Fleet queued and ran two 128-game Dominion training iterations, retained logs, and fetched **14 files**, including
both checkpoint manifests and model files. The command continued through an isolated coordinator and worker
restart. Its VM and input staging were automatically removed. A separate sleeping guest job was cancelled;
Fleet confirmed VM deletion in approximately **17 seconds**. Source copies remained unchanged and all fixture
services, cloned image storage and the qualification reservation were cleaned up.

That initial release passed **80 default Rust tests, 86 GUI Rust tests, eight Python/CLI integration tests**, formatting,
and Clippy with warnings denied in both configurations. Seven VM-job regression tests cover sharing/admission,
restart/retry behavior, uncertain effects, timeouts, malicious output paths, orphan reservations/staging, and
cancellation during image cloning. At that release, the local coordinator and M4 worker used protocol 3.6; the M1 worker was
left at 3.2. Installed-release canaries verified both successful output retrieval and whole-VM cancellation.

See the machine-readable qualification report (local hardware qualification record; excluded from the public source package). The private release directory
`~/fleet/releases/vm-jobs-20261012` holds the committed source, tested binary, checksummed evidence and the previous
binary/database backups. Finish VM jobs/guest operations and stop VMs before rolling back to protocol 3.5; retain
the current database so later job submissions are not lost.


## Live-observation qualification and local release, 2026-10-12

Protocol 3.7 was qualified with a separate offline Dominion training job on the M4 Pro: two vCPUs, 6144 MiB,
50-GiB disk, 128 games/iteration, two rollout shards, and copied archived inputs. Fleet displayed live iteration,
loss and update-time readings and retrieved both six-file checkpoints while the trainer was still running.
The qualification restarted the isolated coordinator, worker, and VM-job orchestration supervisor after the first
checkpoint. The existing guest execution continued and produced the second checkpoint. Cancelling that training
job then deleted its VM in about 14 seconds; the previously retrieved checkpoint files
remained unchanged and accessible. Existing training records and source inputs were preserved.

The release passed **84 default Rust tests, 91 GUI Rust tests, eight Python/CLI integration tests**, formatting, and
Clippy with warnings denied in both configurations. Tests include live probe shapes, incomplete JSON lines, settling,
same-size replacements, unchanged-version transfer deduplication, restart recovery, cancellation retention and
version-checked downloads. GUI phases/charts were rendered and inspected. Installed-release canaries verified
live metrics/output, final same-size replacement, and retained checkpoints after cancellation.

The local coordinator and M4 worker now run protocol 3.7; the M1 worker remains at 3.2. Tart and Softnet were
unchanged. Fixture services, private VM storage and the qualification reservation were cleaned up. See the
qualification report (local hardware qualification record; excluded from the public source package); the private release archive is
`~/fleet/releases/vm-observability-20261012`. Finish VM jobs/guest operations and stop VMs before restoring its
`fleet.before` binary (3.6); retain the current database, including the additive schema column and later submissions.
