# Current interface regression coverage

Fleet has one normal CLI/GUI and one replicated metadata authority. The obsolete CLI, GUI, standalone
SQLite scheduler, command/sweep composition helpers, and their installers are removed. Backend code
still used by current workers remains, including native supervision, Tart controllers, guest operations,
probes, event logs and verified transfers. The one-time [baseline import](BASELINE.md) remains a separate tool.

The retired daemon/process/UI suites are replaced by the following coverage. Tests for behavior that
is intentionally absent from the current contract are removed rather than recreating that behavior.

| Former coverage | Current coverage or disposition |
| --- | --- |
| Queue placement, CPU/GPU budgets, dependencies, durable submission IDs | `tests/control/queue.rs`, `real_worker.rs`, and `tests/cli_contract.rs` exercise Raft intent, staging, capacity and retries. |
| Native input/executable staging, logs, output capture and overwrite refusal | JSON CLI/service tests, including log-cursor continuity across controller/worker restart. |
| Lost replies, process restart, missed observations and once-only execution | `tests/control.rs`, `real_worker.rs`, and the seven normal three-replica CLI/service scenarios. |
| VM metrics, same-size output replacement, cursor replay, supervisor restart and retained results after cancellation | `tests/control/vm_lifecycle.rs` uses committed queue commands and authenticated worker data reads. |
| VM timeout, exit 124, unsafe outputs, cancellation during cloning, uncertain guest execution | `tests/control/vm_lifecycle.rs` confirms failure/cancellation and VM cleanup through current worker observations. |
| Interrupted VM publication and cancellation before delivery | `tests/control/recovery.rs` keeps the VM reserved until current authority explicitly authorizes cleanup; unrecognized backend inventory is refused. |
| Guest manifest validation and unpublished staging cleanup | `tests/vm_job.rs` directly exercises the current backend invariants. |
| VM shared budgets, admission, controller failure and guest file safety | `tests/instances.rs`, `tests/instances/guest.rs`, and current worker integration tests. |
| Checkpoint suspension/resumption and backup recovery | `tests/control/checkpoints.rs`, `storage.rs`, and `fleet-qualify checkpoint --backup-restore`. |
| GUI rendering, metrics, routed cancellation, checkpoint controls, stale responses | Current iced-m3 tests in `src/gui.rs`; retired dashboard dialogs and clocks are removed. |
| Service tunnel authorization, revocation, controller outages, restarts and emergency rebinding | `tests/control/tunnels.rs` and `tests/tunnels.rs`, including local relay-only transport with direct networking disabled, large transfers, half-close and oversized-header rejection. |
| Visible service-policy enforcement | `src/control/tunnels/status/tests.rs`, `tests/control/tunnels.rs`, `tests/tunnels.rs`, `tests/cli_contract.rs` and GUI tests cover acknowledgment after teardown, partial bind failure, authenticated/monotonic reports, durable evidence with term-local freshness, scoped waits/timeouts/supersession, restarts and stale GUI reports. |
| Unmodified Garage peering over tunnels | `fleet-qualify garage` verifies discovery, six blocked direct RPC paths, S3 integrity, Garage/adapter restart, controller outage, acknowledged enforcement before/after revocation and revocation-induced loss of quorum in separate Docker namespaces. |
| Direct scheduling fallback, native pause, automatic preemption/resume/migration, remembered background fetch destinations, run/sweep convenience commands | Removed with the retired client/scheduler. These are not part of the current public contract. |

CI builds `fleet`, `fleet-guest`, `fleet-qualify`, `fleet-import-baseline`, and the isolated Rust Tart/workload fixtures. It runs Rust tests, headless iced-m3
checks, Clippy, the JSON CLI/service suite, isolated checkpoint backup/restore, and three-replica failure
scenarios on Linux and macOS. None needs an installed Fleet service or existing training data.
Historical installations can remain archived on disk; removing their source/entrypoints does not delete
their state or adopt their workloads. Physical deployment upgrades require separate idle checks and backups.

Managed VM coverage includes `src/control/vm/tests.rs` (capability/revision gates, identity
binding, explicit grants, request replay, retention and recovery), worker journal reopen/rollback tests,
and the managed GUI panel. `fleet-qualify managed-vm` drives public CLI/JSON operations through isolated
workers: immutable uploads, verified downloads, worker/controller restart, no command replay, recovery
of a pending backend launch (fault injected through the private worker's launcher setting), automatic
network attachments, revocation/deletion and retained-result reclamation. CI runs the fake backend on
both operating systems. Future-version fixture binaries verify incompatible startup without DB writes.
`tests/control/tunnels.rs` also verifies that an established stream survives unrelated catalog changes.

Indexed metadata coverage lives in `tests/control/indexed.rs`: atomic application,
permanent receipts and released decisions, conflicting retries, bounded pages, restart, snapshot
replacement and recovery exports. Worker journal tests reopen both native and guest execution markers and refuse missing execution tables.
Importer tests preserve complete state, receipts, execution markers and cursors; they refuse locked,
mismatched and unapplied source images without changing source bytes. A three-replica test transfers a snapshot beyond 64 MiB over iroh, loses its
leader, splits large catch-up batches and verifies idempotent retry and quorum-read refusal.
The explicit million-receipt test reports lookup, reopen, snapshot and restore costs without using
installed service homes.
