# Legacy Fleet reference

Historical pre-replicated CLI and coordinator documentation. These commands are not the normal
Fleet 0.2 interface. Existing installations and their state remain separate during handoff.
The optional `legacy` Cargo feature retains regression fixtures, not a supported compatibility mode.

# fleet

A small job runner for experiment machines over [iroh](https://iroh.computer): a coordinator on a laptop that moves
between networks, agents on worker Macs that accept only the coordinator's key, a queue, and (later) a GUI. See
`DESIGN.md`.

Milestones 0–4: keys and allowlists; agents (launchd daemons on the workers, `contrib/install-agent.sh`); jobs with
content-addressed inputs and outputs (`iroh-blobs`, BLAKE3-verified) and a detached supervisor each (its own process
group, a sleep assertion, time limits, exit records); each agent's numbered event log; and the coordinator's daemon
(`fleet daemon`, optionally a launchd daemon on the laptop, `contrib/install-coordinator.sh`): a SQLite queue, placement
by machine, tags, GPU and AC power, dependencies, one GPU job per GPU (including GPU work started outside fleet),
keep-awake while work waits, reconnection with exact event replay, and outputs fetched into each job's destination as
they appear; progress probes (a bar with rate and estimated finish, sparklines, counts, log matches); and a desktop
window (`fleet gui`, `--features gui`) with the machines, the queue and live logs, drawn with
[iced-m3](https://github.com/tvolk131/iced-m3)'s Material 3 Expressive components (a wavy bar while a job runs, flat while
it is paused or done, button groups for its actions, a suspend dialog) in the system's light or dark appearance, drawn
on the GPU (on the CPU where no GPU is usable, or with `ICED_BACKEND=tiny-skia`).

```sh
cargo build --release
# on each worker
fleet allow <coordinator-id>                  # allow the laptop's key (fleet id on the laptop prints it)
sudo contrib/install-agent.sh                 # or: fleet agent (foreground)
echo dominion-ai-gpu >> ~/.fleet/gpu-processes   # GPU work started by hand that fleet must not double-book
# on the laptop
fleet machine add m1pro <agent-id>            # fleet id --role agent on the worker prints it
fleet daemon                                  # or: sudo contrib/install-coordinator.sh
fleet machines                                # online, direct or relayed, power, load, GPU, running jobs
fleet submit job.json --gpu --tag gpu:metal --dest runs/my-run [--on m1pro] [--after <job>] [--priority 1]
fleet run --gpu --threads 8 --dest results -f -- ./bin/tool train --out out   # a one-off job; relative files named are shipped
fleet sweep job.json --param seed=521,733 --param lr=3e-4,6e-4 --dest runs/screen   # one job per combination
fleet queue
fleet logs <job> [--stderr] [-f]
fleet outputs <job>
fleet fetch <job> <dest>                      # skips identical files, refuses to overwrite different ones
fleet cancel <job>
fleet pause <job>                             # freeze it where it is; fleet continue <job> resumes it
fleet suspend <job> [--now] [--plan]          # stop at a checkpoint (next one, or now with the loss stated)
fleet resume <job> [--on m1pro]               # queue its next segment
fleet move <job> --to m1pro [--now]           # suspend, then continue on another machine
fleet jobs m1pro                              # the agent's own list
fleet hello m1pro [--watch 3]                 # diagnostics, only while no daemon runs
fleet gui                                     # the window (build with --features gui)
```

Without a running daemon, the same commands work in-process, except that `submit` must find a machine at once.

The historical Python wrapper has been removed. Current automation uses the
[JSON CLI interface](NORMAL_FLEET.md#json-automation-interface).

Persistent macOS VMs are an opt-in worker backend using Tart and Softnet. Protocol 3.3 adds
`fleet instance create/start/stop/inspect/list/delete --on <machine>`, private persistent disks, and offline or
outbound-public-internet-only networking. Protocol 3.4 adds explicit best-effort GPU sharing between up to two
active macOS VMs per worker, with aggregate CPU and VM memory budgets; exclusive GPU scheduling remains the default. See
[`docs/VM_INSTANCES.md`](VM_INSTANCES.md) for worker setup, the image catalog, network policy and recovery.
Protocol 3.5 adds readiness probes, durable guest commands with retained output, regular-file upload/download
and storage inventory through the CLI, including for offline guests. See
[`docs/VM_GUEST_OPERATIONS.md`](VM_GUEST_OPERATIONS.md) for the workflow and recovery limits.

Protocol 3.6 adds queued jobs in disposable VMs: include `vm` in a job file or pass `--vm vm.json` to
`fleet run`. Fleet stages inputs, executes, captures outputs and deletes the VM; native jobs remain the default.
Protocol 3.7 adds live progress probes/charts, settled intermediate output capture, restart-safe capture tracking,
and VM lifecycle phases in the CLI/GUI. See [`docs/VM_JOBS.md`](VM_JOBS.md) for submission, observations,
resource sharing, recovery and current limits.

The proposed next architecture is documented in
[`docs/DISTRIBUTED_CONTROL_PLANE.md`](DISTRIBUTED_CONTROL_PLANE.md): a replicated control plane using
Raft over iroh and redb, with durable request receipts and worker reconciliation. It is a design draft;
the planned release uses fresh state and new protocols. An opt-in
[`fleet-control-lab`](CONTROL_PLANE_LAB.md) now exercises persistence, replication, failover, and
simulated workers in isolation. It also supports
[replica replacement and single↔three membership changes](CONTROL_PLANE_MEMBERSHIP.md) and
[real native/Tart workers](CONTROL_PLANE_WORKERS.md), [lost-quorum recovery](CONTROL_PLANE_RECOVERY.md)
with signed client/worker rebinding, [direct LAN deployment and failure qualification](CONTROL_PLANE_NETWORKING.md),
and optional combined replica/worker
processes. These require fresh explicit homes. Installed training services still use the existing SQLite
coordinator and are not migrated or replaced.

A job file:

```json
{"name": "smoke", "argv": ["bin/tool", "--flag"], "inputs": {"bin/tool": "/local/path/to/tool"},
 "env": {"RAYON_NUM_THREADS": "8"}, "publish": ["out/*", "B/iter-*"], "settle_seconds": 30, "max_seconds": 86400}
```

Every change passes `cargo fmt --check`, `cargo test` and `cargo clippy --all-targets -- -D warnings`, and the same two
with `--features gui`. The GUI's tests draw it headlessly from fixtures; `FLEET_GUI_PREVIEW=<folder> cargo test --features
gui gui::` also saves the drawings there as PNGs, to look at a change without a window.

## Graceful shutdown and recoverable clients

A job can set `"shutdown_grace_seconds": 120` (or pass `--shutdown-grace-seconds 120` to
`fleet run`, `submit`, or `sweep`). The default remains 10 seconds. Cancellation, checkpoint
suspension and runtime expiration send one SIGTERM to the process group, then SIGKILL after
the configured grace. Zero means immediate escalation. Repeated requests do not extend the
clock; paused jobs are continued to handle SIGTERM. The queue reports `stopping.reason` and
`stopping.deadline` while cleanup is in progress. The child must implement its own signal
handler for flushing or checkpointing. Waiting for a suspension checkpoint is separate from
this grace period. Descendants that stay in the job's process group are cleaned up even if
the command's wrapper exits first.

Custom grace requires an updated coordinator and agent protocol **3.2**. Unsupported custom
settings fail at the coordinator or keep the job queued with an agent-upgrade explanation;
they are never silently discarded. Existing jobs retain their saved configuration.

The historical daemon protocol supports a stable submission key.

The key is scoped to this coordinator's database. Repeating the same key with the same prepared
specification returns the same job, including after a lost reply or coordinator restart. Input
contents and executable permissions, placement, destination, dependencies and priority all count
as part of the specification; a changed specification is an error. A deliberate new attempt uses
a new key. Keep the key mapping for as long as clients may replay submissions.

The local protocol exposes `submit_once` and `find_submission`; older daemons reject these
operations explicitly. A lost response means the request may already have taken effect.
Retrying an unkeyed submission is not safe; semantic errors require correction.

If delivery to a worker is uncertain, the coordinator retains that worker assignment and its
resource reservation, reconciles the existing job ID, and retries only on that same worker.
An agent accepts a repeated matching ID without starting a second supervisor. This is recovery
of an existing process submission, not automatic retry of failed training or reboot failover.

The historical Rust backend regression tests use isolated temporary homes:

```sh
cargo test --all-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
```
