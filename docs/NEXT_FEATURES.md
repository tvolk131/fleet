# Next features: decisions to make first (draft, 2026-10-09)

Order agreed with Tommy: (1) `fleet run` and sweeps, (2) CPU slots and spreading evaluations, (3) automatic recovery
with checkpoint mirroring, (4) stall detection and notifications, (5) retention and disk guards. Item 1 needs no
decisions. Each decision below has a recommended default; change any of them.

## 1. `fleet run` and sweeps (built 2026-10-09)

Built as below, with one change found in testing: only *relative* paths in a command are shipped automatically.
Absolute ones (`/bin/sh`) are assumed to exist on the worker, since shipping a copy of a system binary is both
wasteful and unreliable on macOS; `--input /abs/file` ships one explicitly and rewrites the command to use it. Also
fixed: relative input paths in a job file were resolved against the daemon's working directory; they now resolve
against the job file's folder. `--follow` streams a job's output and exits with its exit code.

- `fleet run [placement options] [--name N] [--dest D] [--input PATH]… -- command args…`: a one-off job without a JSON
  file. An argument that is an existing local file is shipped as an input (a relative path keeps its path; an absolute
  one is placed under `inputs/` and the argument rewritten).
- `fleet sweep job.json --param seed=521,733 --param lr=0.0003,0.0006`: one job per combination, with `{seed}` and
  `{lr}` substituted in the name, command, environment and destination.

## 2. CPU slots and spreading evaluations

**Fleet's side built (2026-10-09)** with the defaults below: `cpu_slots` in each machine's `Hello` (cores minus 2,
or `$FLEET_HOME/cpu-slots`); `threads` per job (`--threads`); placement by free slots (a paused job frees its
threads); `RAYON_NUM_THREADS`, `OMP_NUM_THREADS` and `FLEET_THREADS` set to the grant unless the job sets them;
`fleet machines` shows threads used. Not counted: CPU work started outside fleet (only GPU work is detected, by
command line). **Dominion's side** (a helper that builds evaluation jobs and reads their results, and the
cross-machine identity check) waits for the M1 Pro's agent, together with moving the runners onto fleet.

- **A. Capacity.** Each machine offers CPU slots, **default: its cores minus 2** (kept for the system), overridable in
  the agent's configuration. A job asks for `threads` (default 1); fleet sets `RAYON_NUM_THREADS` and
  `OMP_NUM_THREADS` to what it was granted. GPU jobs ask for threads too (our trainer: 8).
- **B. Where the pieces live.** Fleet gains what is generic: jobs that claim threads, sweeps, and gathering many jobs'
  outputs into one folder. What to measure stays in Dominion's runners, as a helper there that builds the evaluation
  jobs and reads their results. The check that results are identical on both Macs is a Dominion test.

> **Items 3, 4 and 5 are shelved (Tommy, 2026-10-09)** until fleet has run real work on the M1 Pro. They are additive
> (new requests, events, background tasks and optional job fields), and after the switch of the agent protocol to
> tolerant JSON they can be rolled out without upgrading every machine at once: each ships as a minor protocol
> version (3.1, …) with its additions listed in `proto::ADDED`, so jobs that use them wait for an upgraded agent.
> Their decisions below stay open.

## 3. Automatic recovery with checkpoint mirroring

- **C. Mirroring.** For jobs that declare `resume`, the daemon copies each settled checkpoint to the laptop as it
  appears, **keeping the last 2 per job** (about 190 MB each for our runs).
- **D. When a machine counts as gone.** **Default: offline for 15 minutes while it was running a job.** Its resumable
  jobs then continue from their mirrored checkpoint on another machine that fits; non-resumable ones are marked lost
  (and retried, if they ask for retries).
- **E. If the machine comes back.** Its original segment may still be running. **Default: the recovered continuation
  wins; the original is cancelled on reconnection, and nothing it wrote after the fork is fetched.**
- **F. Opt-in or default.** **Default: on for every job that declares `resume`**; `"recover": false` opts out.

## 4. Stall detection and notifications

- **G. A stall.** **Default: no change in any progress probe and no log growth for 30 minutes**, or 5 times the job's
  usual interval between updates, whichever is longer. A stalled job is flagged, not stopped.
- **H. Delivery.** macOS notifications on the laptop for: finished, failed, stalled, lost, and waiting on you. A
  launchd-installed daemon has no desktop session to show them in, so **notifications come from `fleet gui` when it
  is open, and otherwise from a small per-user notifier** started at login. Phone pushes would need a third-party
  service: not built unless you want it.

## 5. Retention and disk guards

- **I. Finished jobs.** **Default: a finished job's folder is deleted 3 days after its outputs were fetched**; jobs
  without a destination are kept 7 days. Suspended jobs keep their checkpoint until continued, then 3 days.
- **J. Blobs.** Unused blobs (no job folder or recorded checkpoint refers to them) are removed **daily**, on agents
  and on the laptop.
- **K. Disk guard.** **Default: a machine with under 20 GB free takes no new jobs** (shown in `fleet machines`), with
  a per-machine override.
- **L. Pruning inside a running job** (for example keeping only its last 2 resume points) is **opt-in per job**, never
  automatic: it changes the job's own folder.
