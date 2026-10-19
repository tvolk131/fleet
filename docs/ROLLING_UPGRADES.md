# Rolling control-plane upgrades

Executable releases, cluster behavior generations, wire protocols and database layouts are distinct.
The [consolidated baseline](BASELINE.md) currently supports generation 1 and format 1 in the
`fleet-baseline-202610` family. Patch releases can retain the same compatibility range. A future
release may support two adjacent generations, such as 1..2, while preserving generation-one behavior
until the cluster explicitly activates generation two. Number equality does not imply compatibility
with retired experimental releases.

## Upgrade contract

Every replica must apply a command identically at the active generation. Changes to deterministic
behavior require a new generation even when message schemas are unchanged. New commands declare a
minimum generation and must be refused before Raft admission until activated. Dependency and physical
layout changes require separate compatibility qualification; the generation mechanism does not make
arbitrary Raft or redb formats compatible.

1. Use `fleet versions` to inspect the active generation and each reachable replica's supported range,
   applied position and leadership. Individual replica diagnostics can be stale.
2. Install the compatible next executable on followers one at a time, waiting for catch-up and healthy
   quorum before proceeding. Replace the leader last. A leader election can delay requests; clients
   retry with the original durable IDs. Fleet does not itself install binaries or edit OS services.
3. Once the executables are installed, use `fleet upgrade prepare TARGET --revision REVISION
   --request-id ID`. Preparation commits a pending operation and freezes membership changes. An
   unavailable or incapable member cannot acknowledge readiness; ordinary workload reconciliation
   continues at the old generation.
4. Every current member, including learners, persists its local preparation promise before publishing
   readiness through Raft. The promise prevents an incapable executable reopening that database even
   if its acknowledgement has not reached quorum. Promises survive restart and leadership changes.
5. After all members acknowledge, explicitly use `fleet upgrade activate --revision REVISION
   --request-id ID`. Starting a binary never activates a generation by itself.

Before activation, `fleet upgrade abort` clears preparation. Each prepared replica must apply the
abort using a capable executable before rollback is permitted. After activation, a binary that cannot
support the active generation refuses startup before writable database opening. A dirty database is
repaired only in a private scratch copy during preflight. Worker startup also checks its recorded
observed generation before writing. Restoring old databases is disaster recovery, not binary rollback.

JSON operations are `versions`, `prepare_version`, `activate_version` and `abort_version`. Membership
and recovery safeguards continue to apply. Upgrades of clients, storage agents, workers and guest
helpers must be qualified for the features used by their workloads; compatible controllers alone do
not establish compatibility of every data-plane component.

## Release qualification

`fleet-qualify build-upgrade-fixtures` currently creates three separately compiled executables from
the new baseline: a 1-only release, a synthetic 1..2 candidate and an incompatible 3..4 executable.
They use an accelerated 100-entry snapshot cadence; production keeps 10,000. The report identifies
these as synthetic future releases and records their hashes and cadence. No old-format
release is advertised as rolling-compatible with this family. When a successor is shipped, qualification
must include the immutable previous released executable as well as an incompatible fixture.

`fleet-qualify rolling-upgrade` exercises three local replicas with live native work and storage,
follower replacement, snapshot catch-up, preparation promises, abort and rollback, leader loss during
activation, same-ID retries, cold quorum restart, and incompatible write/startup refusal without
changing database bytes. Final output and supervisor identity verify workload continuity. Unit tests
also cover state-machine readiness and preflight boundaries. Physical host loss and real Tart behavior
are separately qualified; local process tests do not claim those guarantees.

CI runs the rolling scenario separately from workload and networking qualification on Linux and macOS.
The one-time pre-baseline importer has independent tests and deployment rehearsal; it is not a rolling
upgrade and is never invoked by the normal Fleet service.
