# Internal guest operations

Tart's guest channel supplies the execution and file-transfer backend for [VM jobs](VM_JOBS.md).
It works for offline guests without granting host shares or application-network access. The current
public Fleet client exposes persistent-instance lifecycle, but does not expose arbitrary persistent-guest
commands, uploads, downloads or interactive shells. The retired CLI commands for those operations are removed.

The internal backend retains durable operation IDs, immutable request specifications, readiness probes,
argv/environment execution without an implicit host shell, regular-file transfers, and bounded log reads.
An uncertain execution attempt is not replayed. Downloads verify file identity/version and content;
symlinks, path escapes, malformed manifests and unsafe guest output are rejected. VM jobs use these
operations to stage inputs, sample metrics, capture outputs and checkpoints, and perform cleanup.

Backend regression coverage remains in `tests/instances/guest.rs`; end-to-end workload coverage uses
the replicated control-plane/worker interfaces. Exposing these operations publicly again would require
a separate control-plane operation/authorization design, rather than restoring the retired direct-agent CLI.
