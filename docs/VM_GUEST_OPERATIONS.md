# Internal guest operations

Fleet's guest agent (`fleet-guest --agent` on vsock port 1024, reached with `fleet guest-exec`) supplies the execution and file-transfer backend for [VM jobs](VM_JOBS.md).
It works for offline guests without granting host shares or application-network access. Persistent instances expose durable commands and regular-file uploads/downloads through the replicated
[managed VM API](MANAGED_VMS.md). Interactive shells are not provided.

The internal backend retains durable operation IDs, immutable request specifications, readiness probes,
argv/environment execution without an implicit host shell, regular-file transfers, and bounded log reads.
An uncertain execution attempt is not replayed. Downloads verify file identity/version and content;
symlinks, path escapes, malformed manifests and unsafe guest output are rejected. VM jobs use these
operations to stage inputs, sample metrics, capture outputs and checkpoints, and perform cleanup.

Backend regression coverage remains in `tests/instances/guest.rs`; end-to-end workload coverage uses
the replicated control-plane/worker interfaces. Public operations use generation-4 committed intent, worker capability checks, immutable target bindings and
bounded worker-local result retention. They do not restore the retired direct-agent CLI.
