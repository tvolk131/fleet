# Pausing, suspending, moving and preempting jobs (design note)

> Historical backend design. For the normal replicated client, see [checkpoint-resumable VM jobs](CHECKPOINT_JOBS.md) and [normal Fleet](NORMAL_FLEET.md).

Draft of 2026-10-09, for review before the code (milestone 5). Builds on `DESIGN.md`.

## What Tommy asked for

- Pause a job, and switch or swap jobs on a machine.
- Two ways to stop a job that can be continued later: **"whenever you can do so without losing work"** and **"right
  now, even if some work is lost"**, and for the second, **say exactly how much would be lost** before doing it.

## Two mechanisms

| | **Pause** (freeze) | **Suspend** (stop at a checkpoint, continue later) |
|---|---|---|
| How | SIGSTOP to the job's process group; SIGCONT to continue | The job is stopped (SIGTERM, then SIGKILL after a grace period); later a *continuation* starts from its latest checkpoint |
| Needs from the job | nothing | a `resume` declaration (below): where its checkpoints land and how to continue from one |
| Work lost | none | none if suspended at a checkpoint (`--when-safe`); otherwise everything since the last one (`--now`) |
| Memory | kept (including GPU allocations, which on these Macs are shared RAM) | freed |
| Machine | stays on it | the continuation may run anywhere |
| Use | "hold this while I use the Mac"; a short job jumps ahead on the same machine | free a GPU completely; move a run to a better machine; survive a machine going away; preemption |

Jobs without a `resume` declaration can be paused or cancelled, not suspended.

## Pause

**Built (2026-10-09).** `fleet pause`/`fleet continue`, GUI buttons; tested
(a frozen job's time limit waits, its GPU is freed, and it can still be cancelled).

- `fleet pause <job>` and `fleet continue <job>`; GUI buttons; `paused` becomes a job state.
- **Paused time does not count toward `max_seconds`**, and the job's sleep assertion is released while it is paused.
  The job's own clocks keep running (our trainer's `elapsed_seconds` includes the pause).
- **For scheduling, a paused GPU job releases its GPU slot** (it computes nothing); its memory stays allocated.
  Starting another GPU job beside a paused one is fine on these Macs at our sizes; a job can opt out with
  `"pause_keeps_gpu": true`.

## The `resume` declaration

```json
"resume": {
  "points": "B/resume/iter-*",
  "position": "iter-(\\d+)",
  "progress": "iteration",
  "argv": ["bin/dominion-ai-gpu", "train", "...", "--resume-from", "{latest}", "--output", "B{segment}"],
  "carry": ["B/metrics.jsonl"]
}
```

- `points`: where checkpoints appear (files or folders), relative to the working directory.
- `position` (optional): a pattern whose first capture is a checkpoint's position, read from its name (`iter-0768` →
  768).
- `progress` (optional): the name of the job's progress probe that holds its current position, in the same units.
- `argv`: the continuation's command. `{latest}` is replaced by the checkpoint's path in the continuation's working
  directory, `{segment}` by its number (2, 3, …), so each segment writes its own folder.
- `carry` (optional): other files the continuation needs (copied with the checkpoint).

A checkpoint counts only once it has stopped changing for `settle_seconds` (as outputs do), so a half-written one is
never used. (Our trainer also writes resume points to a staging folder and renames them, which is stronger still.)

## Suspend

- **`fleet suspend <job> --when-safe`** (the default): the agent watches for the next checkpoint to settle, then stops
  the job. No work is lost. Before acting, fleet says when that will be: the next position, from the spacing of
  earlier checkpoints, and the time to reach it at the job's current rate, for example "at iteration 1,024, in about
  3.2 h". `--when-safe --within 1h` gives up (and leaves the job running) if no checkpoint comes in time.
- **`fleet suspend <job> --now`**: stops the job at once and rolls back to its latest settled checkpoint. **Before
  acting, fleet states the loss** and asks for confirmation (`--yes` skips the question):

  ```
  The latest checkpoint is iteration 768 (written 42 min ago); the job is at iteration 803.
  Suspending now redoes 35 iterations, about 42 min of this machine's time.
  The next checkpoint, iteration 1,024, is due in about 3.2 h (fleet suspend --when-safe loses nothing).
  Suspend now? [y/N]
  ```

  The loss is measured three ways, whichever the job allows: in its own units (current position minus the
  checkpoint's, when `position` and `progress` are declared), in wall-clock time since the checkpoint was written
  (always), and in compute time to redo it (units divided by the current rate).
- **`fleet suspend --plan <job>`** prints both options without acting; the GUI shows the same two buttons: "Suspend at
  the next checkpoint (iteration 1,024, about 3.2 h)" and "Suspend now (redoes 35 iterations, about 42 min)".
- After stopping, the job is `suspended`, and the agent adds its checkpoint (and `carry` files) to its blob store so
  they can be fetched by hash.

## Continue, move, swap

- **`fleet resume <job> [--on <machine>]`** queues the continuation: the same job with the `resume.argv` command, the
  original inputs plus the checkpoint and carried files, and the same placement and priority. It is the next
  *segment* of the same logical job: the queue and GUI show one job with its segments, and outputs from every segment
  are fetched into the same destination, in separate folders by `{segment}`.
- **Moving data:** on the same machine, nothing moves; the agent already has the checkpoint by hash. On another
  machine, the daemon fetches it from the first agent and pushes it to the second (through the laptop; direct
  agent-to-agent transfer would need agents to trust each other, which v1 does not do).
- **`fleet move <job> --to <machine> [--when-safe | --now]`** is suspend followed by resume there.
- **Swap** is preemption, below, or done by hand: suspend (or pause) one job, submit the other.

## Preemption

- A job may allow itself to be preempted: `"preempt": {"mode": "when_safe"}` or `{"mode": "now", "max_loss_minutes":
  30}` (never, by default).
- When a queued job cannot be placed only because a GPU is held by a **lower-priority** preemptible job, the scheduler
  suspends that job under its own policy (for `now`, only if the stated loss is within `max_loss_minutes`; otherwise
  it waits for the next checkpoint). The preempted job's continuation is queued automatically with its priority, so
  it resumes as soon as a GPU is free, on any machine that fits.
- The queue shows why: "suspending k2 (priority 0) at its next checkpoint, about 40 min, for screen-x (priority 5)".

## Built (2026-10-09), and what changed while building

- **Built:** the `resume` and `preempt` declarations; `fleet suspend` (when safe, `--within-minutes`; `--now` with
  the loss stated and a confirmation; `--plan`), `fleet resume [--on]`, `fleet move --to [--now]`; preemption in the
  scheduler; GUI buttons (Suspend… with both options and their costs, Resume); historical daemon protocol operations.
  Tested end to end: suspend now and resume from the checkpoint; suspend when safe; a move to another machine
  (the checkpoint relayed through the laptop); a higher-priority GPU job preempting a lower one, which then resumes
  by itself.
- **"When safe" loses at most the detection delay:** the agent checks for a settled checkpoint every second, so the
  job may run on for a second or so past it. For our trainer (about a minute per iteration) that is nothing.
- **`every`** (optional, in the job's units) declares the spacing of checkpoints; otherwise fleet infers it from the
  last two, or, from a single checkpoint at position p, assumes every p from the start.
- **A job suspended before its first checkpoint** starts over when continued (its recorded checkpoint is empty).
- **Continuations carry the first segment's inputs plus the latest checkpoint only**, not earlier checkpoints.
- **A race fixed:** a stop signal can reach the job before its supervisor sees the suspend marker; the supervisor
  now counts that as a suspension (as it already did for cancellation), not a failure.

## What it does not do (v1)

- **Transparent checkpointing** of arbitrary processes (CRIU-style). A job continues only if it knows how to resume.
- **Agent-to-agent transfers**: checkpoints between machines go through the laptop, so the laptop must be reachable
  while one moves (about 190 MB per resume point for our runs).
- **Bitwise continuity across machines.** A continuation on another GPU is statistically, not bitwise, the same run;
  the trainer's resume is exact only on the CPU.

## Order of work

1. A process-level Rust test (real `fleet agent` and `fleet daemon` processes, no network needed).
2. Pause and continue.
3. The `resume` declaration, suspend (`--when-safe`, `--now` with the loss stated, `--plan`), resume, move; the GUI's
   buttons; then preemption.
