# Rolling control-plane upgrades

Fleet separates the executable release, supported cluster behavior generations, stable wire protocol,
and physical database format. The first rolling-capable baseline uses physical format 12, wire 1,
and active generation 1. It requires the existing explicit offline migration from formats 5–11.
An old format-11 executable cannot participate in a format-12 cluster. No installed service is
changed by building, testing, or installing a release candidate.

## Upgrade contract

An executable supports at most two adjacent generations. Patch releases may retain the same range.
Every replica must apply a command identically while the cluster remains at the old generation.
Changes to deterministic command behavior require a new generation even when their schemas are
unchanged. Every mutation explicitly declares its minimum generation; ingress refuses inactive operations before appending them, and application also checks the gate. Old behavior must remain implemented until activation; new commands and snapshot schemas
must not be emitted early. A Raft/redb dependency or physical-layout change requires its own explicit
compatibility qualification; this mechanism is not permission to silently change those contracts.

The first qualified transition is generation 1 to 2. It exercises activation and the downgrade floor; it deliberately keeps existing job/storage semantics and schemas unchanged. The control protocol and worker/storage interfaces are unchanged throughout this first transition.
Old baseline workers, supervisors, storage agents and clients may continue serving during replica
replacement. Future workload features must gate dispatch on the required agent capabilities.

1. Run `fleet versions` to inspect the committed cluster generation and each reachable replica's
   supported range, applied index and leadership. Diagnostics for individual replicas can be stale.
2. Replace one replica executable at a time, restarting that replica's service. Start with followers;
   wait for each replica to catch up and restore a healthy quorum before replacing another. Restart
   the leader last. A leader election may temporarily delay requests; clients retry with saved IDs.
   Fleet does not download binaries, change OS services, or automatically restart other machines.
3. Once the desired binaries are installed, run
   `fleet upgrade prepare 2 --revision 0 --request-id upgrade-prepare`. The actual revision is shown
   by `fleet versions`. This commits a pending operation and freezes membership changes. Preparation
   can wait for an offline/older member; normal jobs, storage transfers and other requests continue.
4. Every current replica, including learners, writes a durable local preparation promise before
   acknowledging readiness through Raft. That promise prevents an older executable from reopening
   its database even if the acknowledgement has not reached quorum yet. Readiness survives crashes,
   snapshots and leader changes. `fleet versions` shows the pending members and ready set.
5. When every member is ready, explicitly run
   `fleet upgrade activate --revision 1 --request-id upgrade-activate`. The replicated transition
   activates the next generation. It never happens merely because a binary starts.

Before activation, `fleet upgrade abort --revision 1 --request-id upgrade-abort` clears preparation.
Let every prepared replica apply the abort using the newer executable before rolling it back. An
offline replica with a preparation promise still refuses the old executable until it catches up.
After activation, an executable which cannot support the active generation is refused. Restoring an
old database backup is disaster recovery, not rollback; it must never discard accepted work.

The JSON CLI operations are `versions`, `prepare_version` (`target`, `expected_revision`, `request_id`),
`activate_version`, and `abort_version` (the latter two take revision and request ID). Normal request
journaling and retry semantics apply. Membership changes and version preparation are serialized.
Lost-quorum recovery preserves the active generation but discards the obsolete cohort's pending
preparation. It remains an explicit authority replacement with the existing safeguards.

## Startup and failure behavior

Startup reads durable compatibility metadata before opening the database for writes. If redb needs
crash repair, Fleet inspects a repaired private scratch copy while holding the source file lock. An
incompatible binary does not repair or migrate the original. Normal startup of a supported generation
does not need quorum, allowing a fully stopped cluster to restart. New generation activation does.

The supported range is also checked on authenticated replica traffic; an incompatible replica cannot
win votes or replicate into a cluster with an unsupported active generation. Unsupported snapshots
are rejected before changing application state. Unsupported jumps return an error and never implicitly
migrate the database. Receipt records for explicitly rejected admin requests are still durable history.

A singleton necessarily pauses control-plane availability when its process restarts. Three replicas
allow one-at-a-time restarts, but three processes on one host do not survive that host failing. In a
combined-role service, detached native and Tart supervisors must survive the worker restart and be
reattached. Artifact streams in a restarted storage process can be interrupted and retried; immutable
payloads and publication verification remain intact. This is not VM live migration or guest reboot.

## Release qualification

Every compatibility release must retain an immutable previous baseline and test the actual previous
and candidate executables together: workload continuity, idempotent requests, snapshots, interrupted
preparation, rollback before activation, refusal after activation, full-cluster restart, and a deliberately
incompatible future executable. The first baseline is a pinned source revision in this PR; subsequent
releases should use the previously shipped revision. The future fixture is explicitly a test build,
not a claim to have tested an unreleased product. Run these scenarios on Linux and macOS in CI.
