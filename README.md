# Fleet

[![Fleet CI](https://github.com/tvolk131/fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/tvolk131/fleet/actions/workflows/ci.yml)

Fleet manages a durable job queue and native or Tart VM workloads across trusted physical workers.
Its control plane uses Raft over authenticated iroh connections and a redb database on each replica.
One voter or a three-voter majority represents the same logical cluster. Worker supervision continues
when controllers are unavailable; new assignments require quorum.

Fleet 0.2 is a breaking release with fresh cluster and worker homes. The normal `fleet` CLI and desktop GUI
use the replicated API. Automation uses the CLI’s JSON interface. The old SQLite coordinator and protocols are not migrated.
Keep existing work under its old installation until it drains, then transfer that machine's capacity.
See [setup, operations, and handoff](docs/NORMAL_FLEET.md).

```sh
cargo build --release --features gui --bin fleet --bin fleet-guest  # omit gui on headless hosts
fleet --home /absolute/client-home status
fleet --home /absolute/client-home submit job.json --gpu --requires os:macos --requires gpu:metal
fleet --home /absolute/client-home queue
fleet --home /absolute/client-home logs JOB --follow
fleet --home /absolute/client-home fetch JOB /absolute/new-capture
fleet --home /absolute/client-home gui
```

The default client home is `~/.fleet-client`; `FLEET_HOME` or `--home` overrides it. Controller, worker,
storage, and optional client-staging roles use explicit persistent homes and can share one service process.
No role is started implicitly by a client command.

Automation can send a JSON request from any language, without a language-specific SDK:

```sh
fleet --home /absolute/client-home api <<'JSON'
{
  "op": "submit", "request_id": "training-experiment-1", "id": "training-1",
  "memory_mb": 8192, "gpu": true, "requires": ["os:macos", "gpu:metal"],
  "job": {
    "name": "training", "argv": ["bin/trainer"],
    "inputs": {"bin/trainer": "/absolute/trainer"}, "publish": ["out/*"], "threads": 4
  }
}
JSON
```

See the [JSON automation contract](docs/NORMAL_FLEET.md#json-automation-interface) for response envelopes,
retries, and explicit revisions.

Acceptance means the request is durable, not that execution has begun. Request IDs and prepared input
hashes survive client restart. `fleet retry REQUEST_ID` resends the saved operation. A timeout can mean
an unknown outcome; create a fresh request only when you intend new work.

Jobs support worker selection, required capability tags, priority, successful-job dependencies, CPU/RAM
admission, native exclusive GPU reservations, VM GPU policies, logs, progress probes, and verified
intermediate/final outputs. Native jobs are trusted host processes. Tart jobs run inside disposable
macOS VMs and can explicitly suspend at an application checkpoint and resume in a new VM under the same job ID; persistent VMs expose create/start/stop/delete. Tart's CPU/GPU sharing limitations and offline
or public-internet-only networking policies remain unchanged.

- [Artifact backups, restore, retention, and storage roles](docs/ARTIFACT_STORAGE.md)
- [Rolling control-plane upgrades](docs/ROLLING_UPGRADES.md)
- [Checkpoint-resumable VM jobs and metadata upgrade](docs/CHECKPOINT_JOBS.md)
- [Local failure scenarios and CI](docs/LOCAL_ROLLOUT.md)
- [Normal client/service guide](docs/NORMAL_FLEET.md)
- [Distributed architecture](docs/DISTRIBUTED_CONTROL_PLANE.md)
- [Membership design and qualification](docs/CONTROL_PLANE_MEMBERSHIP.md)
- [Emergency recovery and authority transitions](docs/CONTROL_PLANE_RECOVERY.md)
- [Direct LAN deployment qualification](docs/CONTROL_PLANE_NETWORKING.md)
- [Worker execution and Tart guarantees](docs/CONTROL_PLANE_WORKERS.md)
- [Current regression coverage](docs/TEST_COVERAGE.md)

Testing: `cargo test --workspace --all-features --locked`;
`cargo clippy --workspace --all-features --all-targets --locked -- -D warnings`.
The suite includes the public JSON CLI contracts. The guest helper, test workloads, and qualification runner are also Rust binaries.
`fleet-control-lab` is retained for fault qualification. The retired CLI, GUI, standalone daemon,
and their installers are removed. Normal clients always use the replicated control plane.

Rolling checkpoint retention is opt-in; see [rolling storage](docs/ROLLING_STORAGE.md) for budgets, pins, reclamation and storage reports.

[Portable Tart jobs](docs/PORTABLE_JOBS.md) provide durable executable/input bundles, explicit runtime
compatibility and cooperative resume on another worker after confirmed suspension.

Job logs, metrics, attempt selection and their retention are documented in [Durable job history](docs/DURABLE_HISTORY.md). History uses the existing storage role; configure history destinations explicitly for worker-independent reads.

Guest packaging and the Rust qualification commands are described in [Rust runtime and qualification](docs/RUST_RUNTIME_AND_QUALIFICATION.md).
