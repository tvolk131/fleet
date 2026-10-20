# VM jobs

A job with a `vm` specification runs in a disposable VM. The replicated control plane owns the
logical job and its attempts; the worker stages verified inputs, executes the command, captures outputs,
and confirms VM deletion before releasing the reservation. Native jobs use the trusted process backend.
All roles use the same qualified Fleet version. See [normal setup](NORMAL_FLEET.md).

## Submission

Configure the worker's pinned image catalog as described in [VM instances](VM_INSTANCES.md).
The image needs Fleet's guest agent (`contrib/install-guest-agent.sh`). Install the matching `fleet-guest` executable beside the host Fleet binary; Fleet copies it into each VM through the agent. For Linux images, also install a `fleet-guest` built for the guest as `fleet-guest-linux`:

```sh
cargo build --release --locked -p fleet-guest --target aarch64-unknown-linux-musl
cp target/aarch64-unknown-linux-musl/release/fleet-guest "$(dirname "$(command -v fleet)")/fleet-guest-linux"
```

No guest interpreter is required; installing the helper uses the guest's `/bin/sh`.

```json
{
  "name": "isolated training",
  "vm": {"image":"macos","cpus":2,"memory_mb":6144,"disk_gb":50,"network":"offline","gpu":"exclusive"},
  "argv": ["bin/train", "--output", "training"],
  "inputs": {"bin/train":"./bin/train"},
  "publish": ["training/*"],
  "threads": 2,
  "max_seconds": 600
}
```

```sh
fleet submit job.json --worker m4pro --id training-1 --request-id submit-training-1
fleet queue
fleet logs training-1 --follow
fleet outputs training-1
fleet fetch training-1 /absolute/new-result-directory
fleet cancel training-1 --request-id cancel-training-1
```

CLI job-file input paths are relative to that file. JSON API callers use absolute paths. Inputs are
copied into the client spool before submission; `fleet retry REQUEST_ID` reuses the saved payload even
if the original files change. There is no direct scheduling fallback when the control plane is down.

Arguments and environment are data, with no implicit shell. The guest runs as its image's guest-agent
user in a fresh working directory. Relative executables refer to staged files. No host directories are
mounted. Fleet reserves the full VM vCPU/RAM/disk allocation; GPU exclusivity is Fleet admission policy,
not hardware partitioning. Every macOS VM has GPU access; Linux VMs have none. Jobs share capacity with persistent instances
and native workloads; a job's VM is counted only once. There is no automatic preemption or rerun.

## Metrics and output

Programs can write JSON-lines metrics and ordinary files without an SDK or network listener:

```json
{
  "probes": [
    {"jsonl_field":{"name":"Iteration","file":"training/metrics.jsonl","field":"iteration","total":2048}},
    {"jsonl_series":{"name":"Loss","file":"training/metrics.jsonl","field":"loss","points":60}},
    {"glob_count":{"name":"Checkpoints","pattern":"training/iter-*/manifest.json"}},
    {"log_regex":{"name":"Phase","pattern":"phase: (.*)","stderr":false}}
  ],
  "publish": ["training/metrics.jsonl", "training/iter-*"],
  "settle_seconds": 30
}
```

Add these fields to the job definition. The worker observes bounded guest output manifests and probe
samples through the guest agent's vsock channel. Host-side checks reject unsafe paths, symlinks, oversized
files, and malformed metric data. Same-size output replacements are distinguished by content/version;
unchanged files are not downloaded again. The worker's numbered event log supports reconnection from a
saved cursor. Logs and metrics remain outside Raft; the current GUI displays them through the worker data API.

`fetch` creates a fresh destination and never overwrites an existing export. There is no remembered
background destination or continuous host-directory mirroring. Final outputs and checkpoints can use
[artifact backups and retention](ARTIFACT_STORAGE.md); intermediate logs and metric streams remain on
the worker.

## Completion and recovery

Timeout, cancellation, guest failure, and output validation failure all require VM cleanup before a
terminal observation releases capacity. Cancellation during image verification or cloning does not wait for
it to finish. A supervisor restart reconciles existing guest operations; an uncertain execution is never
replayed. Committed outputs survive cancellation. Checkpoints use the explicit
[checkpoint and resume contract](CHECKPOINT_JOBS.md), with a new attempt after confirmed cleanup.
[Portable VM jobs](PORTABLE_JOBS.md) support cooperative cross-worker resume with durable inputs and
explicit compatibility. Native pause, lost-worker takeover and automatic checkpoint selection/preemption
are not public job operations.

The current control-plane integration tests cover metrics/output updates across worker restart,
timeout versus exit-code failure, unsafe-output cleanup, cancellation during cloning, and checkpoint
backup/restore. Backend tests retain isolation, resource admission, and guest-file validation checks.
