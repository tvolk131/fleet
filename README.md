# Fleet

[![Fleet CI](https://github.com/tvolk131/fleet/actions/workflows/ci.yml/badge.svg)](https://github.com/tvolk131/fleet/actions/workflows/ci.yml)

Fleet manages a durable job queue and native or Tart VM workloads across trusted physical workers.
Its control plane uses Raft over authenticated iroh connections and a redb database on each replica.
One voter or a three-voter majority represents the same logical cluster. Worker supervision continues
when controllers are unavailable; new assignments require quorum.

Fleet 0.2 is a breaking release with fresh cluster and worker homes. The normal `fleet` CLI, desktop GUI,
and Python adapter use the replicated API. The old SQLite coordinator and protocols are not migrated.
Keep existing work under its old installation until it drains, then transfer that machine's capacity.
See [setup, operations, and handoff](docs/NORMAL_FLEET.md).

```sh
cargo build --release --features gui --bin fleet  # omit gui on headless hosts
fleet --home /absolute/client-home status
fleet --home /absolute/client-home submit job.json --gpu --requires os:macos --requires gpu:metal
fleet --home /absolute/client-home queue
fleet --home /absolute/client-home logs JOB --follow
fleet --home /absolute/client-home fetch JOB /absolute/new-capture
fleet --home /absolute/client-home gui
```

The default client home is `~/.fleet-client`; `FLEET_HOME` or `--home` overrides it. Controller, worker,
and optional client-staging roles use explicit persistent homes and can share one service process.
No role is started implicitly by a client command.

```python
from fleet import Fleet  # contrib/fleet.py; standard library only
f = Fleet(home="/absolute/client-home", binary="/absolute/fleet")
job = f.submit("training", ["bin/trainer"], inputs={"bin/trainer": "/absolute/trainer"},
               publish=["out/*"], gpu=True, requires=["os:macos", "gpu:metal"],
               threads=4, memory_mb=8192, request_id="training-experiment-1")
f.wait(job)
f.fetch(job, "/absolute/new-capture")
```

Acceptance means the request is durable, not that execution has begun. Request IDs and prepared input
hashes survive client restart. `fleet retry REQUEST_ID` resends the saved operation. A timeout can mean
an unknown outcome; create a fresh request only when you intend new work.

Jobs support worker selection, required capability tags, priority, successful-job dependencies, CPU/RAM
admission, native exclusive GPU reservations, VM GPU policies, logs, progress probes, and verified
intermediate/final outputs. Native jobs are trusted host processes. Tart jobs run inside disposable
macOS VMs; persistent VMs expose create/start/stop/delete. Tart's CPU/GPU sharing limitations and offline
or public-internet-only networking policies remain unchanged.

- [Local failure scenarios and CI](docs/LOCAL_ROLLOUT.md)
- [Normal client/service guide](docs/NORMAL_FLEET.md)
- [Distributed architecture](docs/DISTRIBUTED_CONTROL_PLANE.md)
- [Membership design and qualification](docs/CONTROL_PLANE_MEMBERSHIP.md)
- [Emergency recovery and authority transitions](docs/CONTROL_PLANE_RECOVERY.md)
- [Direct LAN deployment qualification](docs/CONTROL_PLANE_NETWORKING.md)
- [Worker execution and Tart guarantees](docs/CONTROL_PLANE_WORKERS.md)
- [Historical CLI reference](docs/LEGACY_FLEET.md)

Testing: `cargo test --all-features --locked`; `cargo clippy --all-features --all-targets --locked -- -D warnings`;
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s contrib -p 'test_*.py'` after building all feature binaries.
`fleet-control-lab` is retained for fault qualification. `fleet-legacy` is built only with `--features legacy`
to exercise historical backend tests; it is not the normal client or a protocol bridge.
