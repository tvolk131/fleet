# Current interface regression coverage

Fleet has one normal CLI/GUI and one replicated metadata authority. The obsolete CLI, GUI, standalone
SQLite scheduler, command/sweep composition helpers, and their installers are removed. Backend code
still used by current workers remains, including native supervision, Tart controllers, guest operations,
probes, event logs and verified transfers. The explicit offline format-5/6 migration remains separate.

The retired daemon/process/UI suites are replaced by the following coverage. Tests for behavior that
is intentionally absent from the current contract are removed rather than recreating that behavior.

| Former coverage | Current coverage or disposition |
| --- | --- |
| Queue placement, CPU/GPU budgets, dependencies, durable submission IDs | `tests/control/queue.rs`, `real_worker.rs`, and `tests/cli_contract.rs` exercise Raft intent, staging, capacity and retries. |
| Native input/executable staging, logs, output capture and overwrite refusal | Backend `tests/jobs.rs` plus JSON CLI/service tests, including log-cursor continuity across controller/worker restart. |
| Lost replies, process restart, missed observations and once-only execution | `tests/control.rs`, `real_worker.rs`, and the seven normal three-replica CLI/service scenarios. |
| VM metrics, same-size output replacement, cursor replay, supervisor restart and retained results after cancellation | `tests/control/vm_lifecycle.rs` uses committed queue commands and authenticated worker data reads. |
| VM timeout, exit 124, unsafe outputs, cancellation during cloning, uncertain guest execution | `tests/control/vm_lifecycle.rs` confirms failure/cancellation and VM cleanup through current worker observations. |
| Interrupted VM publication and cancellation before delivery | `tests/control/recovery.rs` keeps the VM reserved until current authority explicitly authorizes cleanup; unrecognized backend inventory is refused. |
| Guest manifest validation and unpublished staging cleanup | `tests/vm_job.rs` directly exercises the current backend invariants. |
| VM shared budgets, admission, controller failure and guest file safety | `tests/instances.rs`, `tests/instances/guest.rs`, and current worker integration tests. |
| Checkpoint suspension/resumption and backup recovery | `tests/control/checkpoints.rs`, `storage.rs`, and `fleet-qualify checkpoint --backup-restore`. |
| GUI rendering, metrics, routed cancellation, checkpoint controls, stale responses | Current iced-m3 tests in `src/gui.rs`; retired dashboard dialogs and clocks are removed. |
| Service tunnel authorization, revocation, controller outages, restarts and emergency rebinding | `tests/control/tunnels.rs` and `tests/tunnels.rs`, including local relay-only transport with direct networking disabled, large transfers, half-close and oversized-header rejection. |
| Unmodified Garage peering over tunnels | `fleet-qualify garage` verifies discovery, six blocked direct RPC paths, S3 integrity, Garage/adapter restart, controller outage and revocation-induced loss of quorum in separate Docker namespaces. |
| Direct scheduling fallback, native pause, automatic preemption/resume/migration, remembered background fetch destinations, run/sweep convenience commands | Removed with the retired client/scheduler. These are not part of the current public contract. |

CI builds `fleet`, `fleet-guest`, `fleet-qualify`, the control-plane lab, and the isolated Rust Tart/workload fixtures. It runs Rust tests, headless iced-m3
checks, Clippy, the JSON CLI/service suite, isolated checkpoint backup/restore, and three-replica failure
scenarios on Linux and macOS. None needs an installed Fleet service or existing training data.
Historical installations can remain archived on disk; removing their source/entrypoints does not delete
their state or adopt their workloads. Physical deployment upgrades require separate idle checks and backups.
