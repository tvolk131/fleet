# Managed persistent VMs

Generation 4 adds worker-managed private service attachments and durable one-off guest operations to
persistent Tart VMs. The CLI, JSON API and Material 3 GUI use the same replicated control plane.
Disposable job VMs retain their existing job/checkpoint lifecycle.

## Ownership and network identity

The control plane commits a logical instance's connection configuration independently of its VM
lifecycle revision. The owning worker prepares an adapter key, immutable attachment and cached routes
under its own home, then reports the public descriptor. It starts and reconciles the adapter inside
the worker service. The guest never receives this key or control-plane credentials.

Stop/start preserves the disk, logical identity, connection configuration and adapter key. Deletion
removes the adapter and grants from the compiled policy; the old key remains a tombstone that can only
receive an empty policy. Deleting a VM does not release its retained operation results. Rebuilding a
VM under the same logical ID and moving persistent VMs between workers are not supported. Create a new
VM and explicitly configure its connections; copying an image does not confer another VM's identity.

Lost-quorum recovery clears connection configuration and bindings, fences unfinished operations as
interrupted, and requires the existing worker recovery procedure. After review, an administrator can
configure connections again; the recovered authority gets new adapter keys. Old cached authority and
keys are never automatically reused. As with all emergency recovery, disconnected old authorities must
be fenced externally.

Configure a destination export, then an allowed source route:

```json
{"exports":{"sql":"127.0.0.1:5432"},"routes":[]}
```

```sh
fleet instance connections database database-connections.json --revision 0
```

```json
{"exports":{},"routes":[
  {"service":{"kind":"vm","instance":"database","service":"sql"},"listen":"127.0.0.1:15432"}
]}
```

```sh
fleet instance connections web web-connections.json --revision 0
fleet instance inspect web
```

The web application connects to its own guest loopback port 15432. Exporting alone grants nobody
access. An administrator's source route explicitly grants that VM access to the named service. For a
host service such as Garage S3, use `{"kind":"host","service":"garage-s3"}` instead. Garage stays a
separate host process with its existing adapter. Garage credentials still authorize S3 operations.
Host-originated connections to automatically managed VM exports are not exposed by this interface yet.

All guest processes share that VM's grants. TCP only; no DNS, arbitrary subnet routing, LAN bridging,
public ingress, port forwarding or cross-worker VM migration. Offline VMs can use explicitly granted
service channels without enabling their NIC. Existing public-internet restrictions remain intact.

Configuration acceptance is not enforcement. `instance inspect` includes the adapter binding and
existing enforcement reports; `tunnels status` and `tunnels wait --revision N` expose fresh application
acknowledgements. Wait for all expected bindings before relying on a global tunnel wait: an unbound VM
has no transport entry yet. Missing destination bindings/exports leave routes unavailable. Offline
adapters may retain their last committed policy; an unconfirmed revocation is not a completed one.
Unrelated catalog changes preserve established streams when an adapter's filtered policy is unchanged.
A relevant policy change closes its existing streams before acknowledging the replacement.
Reports describe a recent observation, not continuous availability. After a known worker restart,
require an increased adapter boot counter before accepting its subsequent applied report.

Manual host topology remains editable through `tunnels apply`. Read `host_topology` and `host_revision`
from `tunnels status` when editing it; `topology` is the combined effective catalog. The reserved `mvm-`
and `mvs-` names are worker-managed and cannot be submitted as host entries. One existing manual adapter
and one managed adapter cannot attach to the same VM simultaneously; retire the manual attachment first.

## Commands and file transfers

```sh
fleet instance exec web --operation inspect-system --request-id inspect-system-1 -- /usr/bin/uname -a
fleet instance logs inspect-system
fleet instance logs inspect-system --stderr
fleet instance upload web /absolute/local/server /Users/admin/server --executable \
  --operation upload-server --request-id upload-server-1
fleet instance stage upload-server
fleet instance download web /Users/admin/report.json --operation capture-report
fleet instance fetch capture-report /absolute/new-result-directory
fleet instance inspect web
```

Every operation has an immutable ID, VM lifecycle revision, argv/file specification, assigned worker,
phase and result manifest. The control plane commits intent first. The worker durably records an attempt
before asking the guest backend to execute. Detached guest supervisors and local records allow recovery
after worker/controller restarts. A pending backend record whose supervisor has not attempted guest
execution can safely resume launching, provided the VM lifecycle revision still matches. An uncertain
attempt is reported as interrupted; Fleet never blindly
reruns it. This is replay protection, not a guarantee that arbitrary guest side effects happen exactly once.
Use `fleet retry REQUEST_ID` for an unknown client outcome; a new operation ID requests new work.

Commands use an argv array, optional working directory and environment, with no implicit shell. Defaults
are a five-minute timeout and seven-day result retention. This is not an interactive terminal, an app
supervisor or a job scheduler. Use the guest OS's service manager for persistent applications. Command
arguments and environment are durable metadata: do not put secrets in them.

Uploads copy a single regular file into a client content store, then transfer its immutable hash to the
assigned worker. A retried request refers to those original bytes even if the source path changes.
If staging was interrupted, `instance stage OPERATION` retries it; acceptance alone does not prove bytes
arrived. The guest backend verifies hashes and refuses to replace a different existing file or executable
mode. Identical existing bytes/mode are accepted. No host share is mounted into the VM.

Downloads capture a single regular guest file. The backend verifies the file's identity/version and
hash before publishing a successful immutable result. Fetch creates a new private local directory,
verifies every file and includes `operation.json`. A successful download is named `download`; command
results contain `stdout`/`stderr`. Live logs are bounded previews, not immutable results until completion.
Directories, symlinks, file synchronization and backups of Garage state are outside this feature.
Use a guest application-aware export/snapshot to capture database data safely.

JSON clients use `vm`, `vm_manage` (actions `connections`, `start`, `release`), `vm_upload`, `vm_stage`,
`vm_logs` and `vm_fetch`. `vm_manage` accepts a durable `request_id`; `start` also requires an operation
`id`, logical `instance`, `expected_revision`, and a guest `spec`. Network updates use the connection
revision; starting a command uses the VM lifecycle revision; releasing results uses the operation revision.
The GUI's **Persistent VMs → Manage** panel uses these interfaces for connections, commands, transfers,
logs and result release. Its command argument field is a JSON array.

## Retention and bounds

Result bytes are durable on the assigned worker, outside the VM disk. They are not currently backed up
to storage agents or Garage. Losing that worker's disk loses its results; control-plane metadata alone
cannot restore them. Fetch important results before deletion of the worker or expiry.

Retention starts at completion: seven days by default, configurable from 60 seconds to 30 days on each
operation. `instance release OP --revision N` can release completed output early. The control plane
commits expiration/release; the worker deletes its guest-operation payloads and blob references, then
reports reclamation. Shared hashes remain protected by other live references. Offline workers reclaim
when they return. Release is not secure erasure: copies already fetched by clients remain theirs.

Limits: four unfinished operations and 128 unreclaimed operations per VM; 4 GiB of logical retained
input/output reservations per VM; 1 GiB per transferred file; 8 MiB per stdout/stderr stream. Host staging/blob copies can require additional physical space. Metadata,
receipts and attempted-operation tombstones remain after payload cleanup. This milestone bounds the
catalog at 4,096 operations, a 2 MiB operation-metadata reservation budget, and 64 configured VM
network identities across its lifetime; automatic
metadata compaction is future work. Stopping/deleting a VM fences pending operations; stopping cannot
undo an application's prior side effects.

## Upgrade and qualification

This executable supports generations 3–4. Upgrade all replicas at generation 3, prepare generation 4,
then explicitly activate it. Workers advertise managed-VM capability before new requests are accepted.
A worker durably accepting generation 4 upgrades its journal marker to format 13, so an older worker
binary refuses to reopen it. Controller redb format remains 12. Never downgrade an activated worker by
editing its marker or restoring an old journal over accepted work.

```sh
cargo build --all-features --bins --locked
fleet-qualify managed-vm --fake --binary /absolute/fleet --home /tmp/fleet-control-managed-vm
```

The qualifier uses public APIs, separate control-plane/worker homes and explicit fixture VMs. It covers
input replay, retained download capture, controller outage and worker restart, command deduplication,
and recovery of a pending launch using a no-op launcher in a private fixture worker configuration,
private connection setup, revocation, deletion and reclamation. The same workflow accepts real Tart
`--instances` and `--image-cache` settings and checks that no guest listener appears on the host.
Real runs require idle installed workloads and never upgrade or stop the normal Fleet service.
