# Consolidated Fleet baseline

The `fleet-baseline-202610` format family starts with application/database format 1 and cluster
behavior generation 1. Every new cluster uses indexed redb resource rows, permanent request receipts,
streaming snapshots and indexed worker execution markers. There are no runtime migrations or feature
branches for the earlier experimental releases. Wire protocol 1 uses a new control-plane ALPN,
`fleet/baseline/control/1`; worker data uses `fleet/baseline/worker-data/1`. The required family marker,
snapshot magic and recovery-signature domain distinguish this baseline from historical formats also
numbered one. An older executable cannot safely participate merely because a number matches.

These numbers describe software compatibility. Cluster and worker identities, request IDs, receipt
fingerprints, producer sequences, resource revisions and Raft positions are not reset. The normal
CLI and GUI still share one durable request journal and replicated API. Incremental resource access
is a separate future change: resource invariants still hydrate the catalog inside atomic transactions.

The pre-cutover baseline now uses bounded `WorkerSync` delivery in place of `WorkerRead`. Install
matching controller and worker executables during the pending baseline import; earlier baseline
development builds are not mixed-version participants for this interface change. The sole normal
installation remains on the pre-baseline family until that explicit cutover. The delivery cursor and
pending-transfer marker are additional worker-journal keys, independent of request and execution identity.

## One-time import of the existing installation

The separate, unpublished `fleet-tools` package contains `fleet-import-baseline`. It supports exactly
the installed pre-baseline controller format 12 / generation 4 and worker format 13 / generation 4.
It is an offline release tool, not a command in the Fleet service. Formats 5–11 and earlier prototype
protocols are no longer supported. Historical source and conversion implementations remain in Git.

```sh
cargo build -p fleet-tools --bin fleet-import-baseline --locked
fleet-import-baseline \
  --controller /absolute/stopped-controller/control.redb \
  --worker /absolute/stopped-worker/worker.redb \
  --destination /absolute/new-conversion-bundle
```

The tool exclusively locks both sources, copies them into a new private directory, validates the
identities, sole voter, applied boundary and idle workload state, then converts only those copies.
It refuses unapplied Raft suffixes, in-progress upgrades/membership changes, recovery-bound state,
foreign workers and active/uncertain workloads. Stopped persistent instances and suspended jobs retain
their records. All resource/receipt rows and native/guest execution markers are preserved. The
converted controller gets a streaming snapshot at the original applied boundary; original votes and
identities survive. Client request files, blobs, backend directories and storage-role homes need no
conversion and are not opened by this tool.

`complete.json` is written only after the new controller image has been reopened, checked against the
expected state and snapshotted, and both source hashes have been checked again. It records source and
output hashes plus counts. A failed destination is diagnostic material, not an installable bundle;
rerun into a fresh directory. The importer never installs files or changes an OS service.

For actual cutover, first drain and stop the old roles at an idle boundary. Preserve matching metadata,
binaries and service definitions; run and verify the conversion; replace both databases together while
stopped; install matching `fleet`/`fleet-guest` executables and restart the roles. Keep existing keys,
configuration, backend files, storage and client journals. Verify the preserved inventory before
undraining. Do not roll back metadata after accepting new work: recovery must retain the new decisions
and execution fences. Building this branch and rehearsing the importer do not upgrade the installation.

## Future upgrades

The current executable supports generation 1. The N+1 prepare/acknowledge/activate mechanism remains
implemented. Future baseline-family releases will declare their supported adjacent ranges and retain
old deterministic behavior until activation. See [rolling upgrades](ROLLING_UPGRADES.md).

The qualification runner builds synthetic 1-only, 1..2 and 3..4 executables to test that mechanism now.
Those fixtures are explicitly future-version simulations; they do not establish compatibility with
the retired format family. Once a second release is shipped, add its immutable predecessor to release
qualification. The importer has separate preservation/refusal tests and is rehearsed on private copies
of the installation's saved metadata.
