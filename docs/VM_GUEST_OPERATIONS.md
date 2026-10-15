# Managing guest software through Fleet

Protocol **3.5** adds guest readiness, durable commands, regular-file transfers and storage inventory to Fleet's
existing Tart lifecycle backend. Upgrade the coordinator and the selected worker. Older workers still accept
native jobs; guest-management requests name the required upgrade rather than silently dropping options.

These are Fleet changes. Tart remains **2.40.1**, Softnet remains **0.24.0**, and the instance's networking and
resource policy remain unchanged. The qualified Tahoe image has Tart's guest agent and `/usr/bin/python3`.
Custom images need both. Commands run as the image's guest-agent user, with that user's existing privileges.
There is no host-directory mount, guest SSH listener, port forwarding, or guest access to Fleet identity keys.
An offline VM can receive an authorized command or file through the worker's local Tart control channel without
obtaining application-network access. See [VM instances](VM_INSTANCES.md) for the isolation contract and setup.

## CLI workflow

Start with a provisioned instance on an enabled worker:

```sh
fleet instance start devbox --on m4pro
fleet instance ready devbox --on m4pro --wait --timeout 180
fleet instance upload devbox ./program /Users/admin/work/program --operation upload-program --on m4pro
fleet instance exec devbox --operation run-program --cwd /Users/admin/work --env THREADS=2 --timeout 600 --on m4pro -- /Users/admin/work/program
fleet instance download devbox /Users/admin/work/result.json ./result.json --operation capture-result --on m4pro
fleet instance storage --on m4pro
```

Guest paths are absolute; local paths refer to the coordinator machine. `ready` probes the guest command channel
and the fixed Python helper. VM `running` still means that the hypervisor is running. Readiness is a timestamped
probe, not a guarantee that a particular application is healthy. `--wait` retries readiness until its deadline.

`exec` sends an argument vector, optional working directory and environment. It does not interpret shell syntax.
To use a shell, explicitly request a guest shell (`-- /bin/sh -c '...'`). Execution is noninteractive: no terminal
or input stream. The foreground CLI streams retained stdout/stderr until completion. `--detach` returns after
recording the operation; disconnecting or closing the CLI does not cancel it. `--json` reports operation metadata
instead of mixing it with raw command output. Failed guest commands make the foreground CLI fail; the exact
Tart/guest exit status is in `exit_code` rather than propagated as the CLI's exit status.

Every command/transfer prints its operation ID before sending. Supply a stable `--operation` when scripting.
IDs are scoped to one VM, start with an ASCII letter or digit, and contain only ASCII letters, digits, `-` and `_`
(up to 64 characters). A new intentional execution needs a new ID.

```sh
fleet instance operations devbox --on m4pro
fleet instance operation devbox run-program --on m4pro
fleet instance logs devbox run-program --on m4pro --follow
fleet instance logs devbox run-program --on m4pro --stderr --from 0
# Fetch an already captured download again, even after stopping or deleting the VM:
fleet instance fetch devbox capture-result ./another-copy.json --on m4pro
```

Log cursors are byte offsets. Each request returns at most 64 KiB; `--follow` reads through completion.
`--json` returns byte arrays so arbitrary binary output and split UTF-8 sequences remain intact. Stdout and stderr
have independent cursors; their relative ordering is not reconstructed. Download payloads use `fetch`, not stdout
logs; download diagnostics are available with `logs --stderr`.

## Recovery and limits

A worker records the full operation before spawning a detached operation supervisor. That supervisor survives
coordinator/agent/client restarts. Matching requests with the same ID, specification and instance revision return
the original operation. A changed request is rejected. The worker recovers records that were durably accepted but
never attempted. Before sending a command into the guest it persists an attempt marker. If the supervisor later
disappears, Fleet marks the outcome `interrupted` and **does not replay it**: guest-side effects may have occurred.
Inspect retained output and application state before deliberately making a new attempt. This is at-most-one
attempt, not exactly-once application semantics or reboot failover.

The VM revision prevents a delayed new command from running after an intervening stop/start. Keep the original
revision when retrying a low-level request. After an ambiguous CLI response, use `operation`, `logs` and `fetch`
to recover the original result; do not generate a fresh operation ID. Historical results remain readable after
stop/delete. An operation's `succeeded` state means the Tart guest channel reported exit code zero, not that an
untrusted guest has proved application correctness.

Limits in this milestone:

- Four active operations and 1024 retained operation records per VM. Reaching the history limit requires a new
  VM ID; explicit retention/pruning is future work. Deleting a VM removes its disk but keeps operation records,
  logs and captured downloads. Storage inventory makes that retained data visible.
- 8 MiB each for command stdout and stderr, enforced by the host. A download has its own payload limit, at most
  1 GiB; its stderr still has the 8 MiB limit. Excess output interrupts the host channel and records the limit.
- Command timeout defaults to 600 seconds, configurable from 1 to 86400 seconds. The guest helper sends TERM to
  the command process group, waits up to five seconds, then sends KILL; timeout reports exit code 124. Host channel
  timeout adds 30 seconds. Transfers have a 600-second channel deadline. A process can escape a guest process
  group, and an untrusted guest can ignore the helper: **stop the VM to enforce termination**. Interrupting the
  host channel alone does not prove that all guest processes stopped.
- Admission and running operations check the worker's existing VM free-space guard. Per-operation byte limits
  and the guard are not filesystem quotas. Blob storage, image cache, operation history and other host writes
  also consume space; automatic cleanup is intentionally absent.

Operation files are owner-private under `$FLEET_HOME/instances/<id>/operations/<operation>/`. They include
`operation.json`, `stdout`, `stderr` and `supervisor.log`; command arguments/environment are retained, so avoid
putting long-lived secrets directly in them. The authenticated coordinator that owns an instance is the only
coordinator allowed to request its guest operations. Fleet's existing worker allowlist and shared content-addressed
blob store remain a trusted-administrator boundary, not independent tenant accounts.

## File semantics and storage

Uploads and downloads handle **one regular file**. Directories, symlinks and devices are refused at the final
file component. Parent paths inside the VM are guest-controlled and can resolve within that VM; they never
select a host path. Package a directory into a file explicitly if needed, then unpack it with a guest command.
Fleet never extracts a guest archive on the coordinator.

Uploads copy the local file into Fleet's content-addressed store and verify its BLAKE3 hash over transport.
The worker verifies the declared size/hash and the fixed guest helper verifies size and SHA-256 before publishing
the file. The helper creates guest parents and atomically creates the final name without overwriting. Identical
existing content and executable status are accepted; conflicting content, permissions or symlinks fail. Uploaded
files receive mode 0700 if executable, otherwise 0600. Do not modify a source file during staging.

Downloads capture bounded bytes on the worker before offering them as a BLAKE3-verified blob. The client chooses
one explicit destination; no guest-supplied filename or archive path can redirect the host write. The coordinator
publishes atomically without overwriting, accepts an identical existing regular file, and refuses conflicting
files or final-component symlinks. New downloads have mode 0600; executable bits are not copied back. The helper
checks size and modification time while reading, but does not offer an application-consistent snapshot: have
writers close/checkpoint a file before capturing it. Completed captures can be fetched without the guest running.

`instance storage` is a read-only, on-demand inventory: free filesystem bytes, configured/reserved VM capacity,
per-VM disk and management bytes, the image cache, and the worker blob store shared with native jobs. Logical size
includes sparse capacity. Allocated bytes come from per-file filesystem block counts. APFS clones can share those
blocks, so summing them is neither unique physical usage nor reclaimable space. The scanner skips symlinks and
has entry/time limits. It performs no deletion, capacity changes, or cache pruning.

The local daemon adds `instance_guest`, `instance_upload` and `instance_download`. The remote authenticated
protocol adds `InstanceGuest`, gated at minor 5. The coordinator stages/fetches blobs, the worker validates
ownership and VM revision, and the detached supervisor drives Tart. Neither the native job scheduler nor Tart
needs a new execution mechanism. Protocol 3.6 adds [queued jobs in disposable VMs](VM_JOBS.md). Interactive terminals, dedicated VM GUI controls,
snapshots, retention, new networking modes and migration remain separate milestones.

## Qualification and local release, 2026-10-12

Qualified the GUI-enabled release on the M4 Pro with Tart 2.40.1, Softnet 0.24.0 and the pinned Tahoe image.
Each disposable VM used two vCPUs, 4096 MiB memory, a 50-GB disk and offline networking. Separate Fleet identities,
coordinator/worker homes and Tart storage kept qualification apart from production. A new, bounded placeholder
job reserved two CPU slots and the GPU while the fixtures ran; it was cancelled after cleanup.

The real VM checks verified readiness, a 10-MiB binary upload/download with matching SHA-256, literal environment
and working-directory handling, retained stdout/stderr, and guest exit code 7. A running command survived both
coordinator and worker restarts; retrying its original ID left exactly one recorded guest-side effect. A completed
download could be fetched after the VM stopped. The offline guest could reach neither a controlled host listener
nor a public TCP endpoint that the host could reach.

A disposable coordinator on the physical M1 Pro then used Fleet over authenticated QUIC to create/start an M4
VM, check readiness, upload and execute a program, verify another 10-MiB round trip, inspect storage, stop the VM,
fetch its captured download and delete it. Guest access used Fleet throughout; no SSH connection into the guest,
network-policy relaxation, Tart modification or host-directory share was needed. The temporary M1 client was
removed, and its production worker stayed on protocol 3.2.

Final checks passed **73 default-feature Rust tests, 79 GUI-enabled Rust tests, seven Python integration tests
against the release binary, formatting and Clippy with warnings denied in both feature configurations**.
The new tests exercise crash/retry semantics, request ownership, time/output bounds, file conflicts and symlinks,
protocol gating, storage accounting and client/agent/coordinator reconnection.

The local coordinator and M4 worker now run protocol 3.5 from `~/fleet/bin/fleet`. An installed-release canary
booted an offline VM, executed a command through Fleet, stopped and deleted it. All 79 original coordinator
records retained their execution fields and measured progress, and all 67 original local worker job records
were preserved. Agent restart refreshed historical probe caches; it did not restart training. The qualification
reservation is the only new production job record, and it is cancelled. All disposable VMs and fixture services
are stopped/deleted and copied fixture image caches have been removed.

See the machine-readable report (local hardware qualification record; excluded from the public source package). The private local release directory
`~/fleet/releases/guest-vm-20261012` contains the source archive, tested binary, checksummed qualification evidence,
and previous binary/database/event-log backups. To roll back to the preceding 3.4 release, first finish guest
operations and stop all VMs, restore `fleet.before` to `~/fleet/bin/fleet`, and restart the local coordinator and
worker using their existing service setup. Do not restore a database snapshot over later job submissions.
