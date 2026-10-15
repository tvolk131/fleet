# Fleet: a small job runner for experiment machines over iroh (design)

Working name **fleet**. Written 2026-10-08 in the dominion-rs session that motivated it; decisions are below. Reviewed
before any code.

## Why

Experiments now span two Macs (this MacBook Pro and an M1 Pro), with more Macs, a GPU box or cloud machines possible
later.
Today the second machine is driven over SSH on the home network: a dedicated key, shell scripts copied over, `rsync`
for checkpoints, and a hand-rolled polling loop in each runner (`scripts/run_half_bonus_screen_v1.py`). That works,
but:

- **The laptop is the coordinator and it moves.** SSH on the home network stops working as soon as it leaves home.
- **SSH grants a whole shell.** The workers only ever need to do a few things: run a job, report on it, hand back
  files.
- **Every runner reimplements transport.** Each one has its own copying, retrying, hash checks and job supervision
  (`nohup`, `caffeinate`, exit-code files), and there is no view of the machines or of what is queued.

## Goals

1. **A roaming coordinator:** the laptop schedules and collects work from any network, and jobs keep running while it
   sleeps, travels or is offline.
2. **Narrow access on workers:** an agent that accepts only the coordinator's key and only this protocol: no shell.
3. **A queue:** submit jobs that wait for a suitable idle machine; one GPU job per GPU.
4. **Verified, resumable file movement:** binaries and checkpoints are content-addressed, so a binary is sent once,
   every copy is checked, and interrupted transfers resume.
5. **At-a-glance status:** machines (online, direct or relayed, power, load, running job and its progress) and the
   queue, first in the CLI, later in a GUI.
6. **Scriptable:** our Python runners, and Claude, drive it through a CLI with JSON output.

**Not goals (v1):** replacing the runners' experiment logic (pilots, measurements, decisions), several users or
coordinators, containers or sandboxing beyond a job folder, GPU sharing, provisioning cloud machines, general
workflow graphs.

## Architecture

One Rust binary, `fleet`, with three roles:

| Role | Where | What it does |
|---|---|---|
| `fleet agent` | each worker (a launchd service) | Holds a fixed key; accepts connections from allowlisted coordinators; stages inputs, supervises jobs, keeps an event log, serves outputs |
| `fleet daemon` | the laptop (a launchd agent) | The coordinator: the queue and machine registry in SQLite, placement, connections to agents, fetching outputs |
| `fleet <command>` | the laptop | The CLI: talks to the daemon over a local Unix socket; `--json` for scripts |
| `fleet gui` (later) | the laptop | An iced client of the daemon: machines, queue, live logs |

```
 laptop (anywhere)                         worker (M1 Pro, GPU box, ...)
 ┌──────────────────────────┐   iroh/QUIC   ┌──────────────────────────────┐
 │ fleet CLI / GUI ─┐       │  direct, or   │ fleet agent                  │
 │                  ▼       │  via a relay  │  ├─ blob store (BLAKE3)      │
 │ fleet daemon ── SQLite ──┼──────────────►│  ├─ jobs/<id>/ (supervised)   │
 │  └─ blob store           │               │  └─ event log (seq numbers)  │
 └──────────────────────────┘               └──────────────────────────────┘
```

## Identity, trust and pairing

- **Every process has an iroh key**; its public key is its address (the endpoint ID). Keys are files with mode 0600
  (the macOS keychain could come later).
- **Pairing is two-sided.** `fleet agent init` prints the agent's ID; `fleet machine add m1pro <agent-id>` registers it
  on the laptop; `fleet agent allow <coordinator-id>` authorizes the laptop on the worker. A pairing ticket (both IDs
  plus relay hints, shown as text or a QR code) can make this one step.
- **Each connection is checked:** iroh's TLS proves the peer's key, the agent refuses any key not on its allowlist,
  and the coordinator talks only to registered agents. Revoking is deleting a line.
- **What a key on the allowlist can do:** stage files, start jobs from the job specification below, read their logs
  and outputs, cancel them. It cannot open a shell, read files outside the job folders, or change the agent's
  configuration. Because a job runs a command, whoever holds the coordinator key can still run code as the agent's
  user: the same trust as today's SSH key, through a much narrower door. A stricter mode accepts only executables
  whose hashes the worker's owner has pinned in the agent's configuration.
- **The agent runs as an unprivileged user.** Relays see only encrypted traffic.

## Networking

- **iroh endpoints** with discovery so peers find each other by ID (n0's DNS discovery, plus local-network discovery
  at home) and n0's public relays as the fallback; a self-hosted `iroh-relay` is a configuration change.
- **One protocol, its major version named by the ALPN** (`fleet/agent/3`), plus `iroh-blobs` for files.
- **The coordinator dials.** It holds one connection per online agent, opened at start and re-opened on network
  changes or failure. Agents never need to dial, so a worker needs no knowledge of the laptop beyond its allowlisted
  key.
- **The connection's path is reported** (direct or relayed, round-trip time), so slow transfers can be explained.

## Protocol

Request/response messages on QUIC streams, encoded as JSON (protocol 3; it was `postcard` before, see the
compatibility rules in `src/proto.rs`); one stream per request, plus a long-lived event
stream.

**Versions (2026-10-09).** The protocol version is `major.minor`, and the version number stands for the feature set:
no feature lists are exchanged. Both sides state their version in `Hello`, the two use the lower one, and one table in
`src/proto.rs` (`ADDED`) says which minor version added which request or field. The coordinator refuses, before
sending, a request that uses something the agent's version predates, and places a job only on machines whose agents
have everything it uses; `fleet machines` shows each agent's version and what it lacks. Explicit feature lists were
considered and not chosen: they pay off with forks, independent implementations or features that can be switched
off, none of which fleet has. The shorthand stays sound under three rules, written out at the top of `src/proto.rs`:
every release that adds to the protocol bumps the minor version and lists the addition; agents are deployed only
from the main line; and what depends on the machine (GPU, OS, CPU slots) stays a fact the machine reports, not part of
the version. Everything built through protocol 3's introduction is 3.0, the baseline. 3.1 lets a progress probe's file contain `*`, so progress follows a run's continuations (`B*/metrics.jsonl`).

| Request | Answer |
|---|---|
| `Hello { fleet, protocol }` (the coordinator's versions) | agent version, protocol version, hardware (CPU, cores, memory, GPU and backend), OS, tags |
| `Status` | load, memory, power source and battery, disk free, running jobs with progress, the event log's sequence number |
| `Events { since }` | a stream of numbered events since `since`: job state changes, outputs ready, heartbeats every 15 s |
| `Stage { hashes }` | which blobs the agent lacks; the coordinator then sends them through `iroh-blobs` |
| `Submit { job }` | accepted (with the job's folder) or refused, with the reason |
| `Cancel { job, signal }` | sends the signal; the job's state follows on the event stream |
| `Logs { job, stream, from }` | log bytes from an offset; can follow |
| `Outputs { job }` | output folders and files, each with its BLAKE3 hash, ready to fetch through `iroh-blobs` |
| `Release { job }` | the coordinator has the outputs; the agent may delete the job folder after its keep period |

**Reconnecting after time apart is exact.** The agent's event log is append-only and numbered; the daemon stores the
last number it saw per agent and asks for everything after it. Nothing is lost while the laptop is away.

## Jobs

A job is JSON, written by a runner or by hand:

```json
{
  "name": "half-bonus-screen-v1/B",
  "placement": {"tags": ["gpu:metal"], "ac_power": true},
  "resources": {"gpu": 1, "threads": 8},
  "inputs": {"bin/dominion-ai-gpu": "blake3:9f2c…"},
  "argv": ["bin/dominion-ai-gpu", "train", "--output", "B", "--seed", "521", "…"],
  "env": {"RAYON_NUM_THREADS": "8"},
  "outputs": {"publish": ["B/iter-*", "B/resume/iter-*", "B/metrics.jsonl"], "settle_seconds": 30},
  "limits": {"max_seconds": 172800}
}
```

- **Inputs are hashes.** Each input path maps to a blob the agent has, or receives first, so the binary that runs is
  byte-identical to the one named, and a resubmitted binary is not sent again.
- **States:** `queued` (on the laptop) → `assigned` → `staging` → `running` → `succeeded` / `failed` (exit code) /
  `cancelled` / `lost` (the machine disappeared beyond a timeout; the job is resubmitted only by hand).
- **Supervision survives the agent.** The agent starts each job through `fleet supervise <job folder>`, a small
  process that detaches, holds a sleep-prevention assertion (IOKit) for the job's lifetime, records the exit code, and writes logs into the job folder. Restarting or upgrading the agent does not
  touch running jobs; on start the agent re-adopts them. A stopped agent exits within seconds even while jobs run: it
  gives up waiting on blocking work (its wait on each job's supervisor lasts as long as the job) after 2 s. Before that
  fix, found upgrading the M1 Pro, an agent that had been asked to stay awake and was running a job could not exit until
  the job ended, so launchd could not restart it on the new binary.
- **Only the daemon acts for the daemon.** A command that finds no daemon runs in-process under the coordinator's
  key; a command that started against a daemon (`--follow`) waits for it through a restart instead, because acting
  in-process would hold the key the new daemon needs.
- **Outputs are published as they appear.** A file or folder matching `publish` that has not changed for
  `settle_seconds` is hashed and announced, so a checkpoint is fetched while training continues. That replaces our
  wait-30-seconds-after-the-manifest rule. A job given a destination but no `publish` offers everything it writes
  (found on the M1 Pro's first run: it had fetched nothing), and inputs it left unchanged are not fetched back.
- **The laptop never overwrites a run.** Fetched outputs land in a destination named at submission
  (`runs/<experiment>/…`), and the daemon refuses an existing path, matching `runs/`'s append-only rule. Each fetched
  file is verified against its hash. The one exception is the job's own earlier fetch: the daemon records each file it
  writes (path, content, and the job's first segment), and a file still exactly as the same job, or an earlier segment
  of it, wrote it may be replaced by the job's newer version. A log that grew after a quiet spell is brought up to date
  instead of failing the fetch (found on the M1 Pro: the trainer's `critic-warmup.jsonl`). Files are written beside
  the target and renamed into place, so none appears half written.

## Scheduling

- **The queue is FIFO with priorities.** A job is placed on an online machine whose tags match, whose power state
  allows it, and whose GPU (if requested) is free; otherwise it waits.
- **Placement can be pinned** to a named machine, when an arm must run on specific hardware.
- **One GPU job per GPU**, from the agent's own reports of what it is running, including work started outside fleet
  (an agent marks its GPU busy when it sees a known GPU process).
- **Simple dependencies:** `after: [job]`, so a measurement job can wait for a training job's output. Anything richer
  stays in the runners.

## What it changes for our runners

A runner like `scripts/run_half_bonus_screen_v1.py` shrinks to: submit the training job (`fleet submit --json`),
follow its events (`fleet events --job … --json`), fetch each published checkpoint into the run folder
(`fleet fetch`), and run or submit the measurements. The protocol record gains the agent's ID, its hardware report
and every input's hash. Evaluations can also be submitted as CPU jobs, so they spread across machines instead of
competing with the laptop's training.

## Status and the GUI

- **CLI first:** `fleet machines` (online or not, path, power, load, GPU job, progress), `fleet queue`, `fleet logs
  -f`, `fleet events`.
- **Progress probes.** A job may name a file and a JSON field to report as its progress, for example the last
  `iteration` in `B/metrics.jsonl`.
- **The GUI** (an iced app) shows the same machines and queue, with live log tails, submission from a saved job file,
  and cancellation. It is a client of the daemon, so it can be closed without affecting anything.

## Testing

- **Unit tests:** job specifications, placement, the event log's numbering and replay, the output-settling rule.
- **Integration tests:** a daemon and two agents as in-process endpoints connected over localhost. Cases:
  - pairing and refusal of unknown keys;
  - staging and deduplication;
  - a job's whole life, cancellation;
  - a disconnect mid-job with exact event replay;
  - an agent restart with the job re-adopted;
  - fetch verification.
- **A manual test on the two Macs:** the laptop moved between the home network and a phone hotspot while a job runs.

## Milestones

| Milestone | Contents | Rough size |
|---|---|---|
| M0, spike | Endpoints with fixed keys, the allowlist, `Hello`/`Status` between the two Macs across networks; measure direct against relayed | half a day |
| M1, agent and CLI | Staging through `iroh-blobs`, `Submit`, supervision, logs, outputs, fetch; the daemon with one machine and no queue | 2 days |
| M2, queue | SQLite queue and registry, placement, the event log and replay, reconnection, launchd service files | 1–2 days |
| M3, our runners | Port the half-bonus runner's pattern; a small Python helper for submit, follow and fetch | half a day |
| M4, GUI | The iced client | 2 days |

## Milestone 0 findings (2026-10-08, the two Macs on one home network)

- **Dialing by ID works:** the laptop found the M1 Pro's agent by its endpoint ID alone (n0's DNS lookup), and the
  agent refused an unknown key and logged it.
- **Through n0's relay:** connected in about 0.45 s; round trip 110–160 ms (a US-East relay).
- **Direct, forced (`--no-relay --addr <LAN address>`):** connected in 38 ms; round trip 22 ms over Wi-Fi.
- **Hole punching from the relay to the direct path failed, because of macOS's Local Network privacy.** The agent,
  started from an SSH session, got `No route to host` (errno 65) when it tried to send to the laptop's LAN address;
  it could only reply. A process started over SSH has no app to which macOS can attribute the permission, and a bare
  command-line tool cannot show the prompt.
  - **Therefore (M2): the agent is installed as a launchd daemon** (`UserName` set to the dedicated user). As we
    understand Apple's rules (TN3179), launchd daemons are exempt; per-user launch agents and SSH sessions are not.
    Verify this when installing. Metal works without a GUI session: training already runs from SSH-launched
    processes.
  - The laptop side was not blocked, since it inherited the launching app's permission. A launchd-installed
    coordinator needs the same treatment.
  - **Confirmed:** the same agent started from Terminal (so its access counts as Terminal's) upgraded from the relay
    to a direct path 0.48 s after connecting by ID alone, round trip 10.5 ms.
- **mDNS is not needed for this:** the DNS record already carries the LAN addresses. Local discovery (a separate
  crate; iroh 1.3's core has DNS, pkarr and in-memory lookup only) would matter only on a network without internet
  access.
- **Machines sleep between jobs.** The M1 Pro (with a 1-minute sleep timer) went into deep sleep in the gap between
  two jobs, when nothing held it awake. **The agent holds a sleep assertion whenever it has running or queued work**,
  not only while a job runs, and reports the machine's sleep settings in `Hello`.

## Milestones 1 and 2: decisions made while building (2026-10-09)

- **Pushed inputs are confirmed, not assumed.** `iroh-blobs` finishes importing a pushed blob after the pusher's call
  returns, and does not tell the pusher about a refusal. The coordinator keeps the push connection open (closing it
  aborts the import) and asks the agent which inputs it still lacks until none are missing.
- **Inputs are hashed when a job is queued.** A queued job keeps those hashes, so it runs the binaries as they were
  when queued, even if they are rebuilt while it waits.
- **One job ID end to end:** the coordinator names the job, and the agent uses that name for its folder.
- **The daemon is the only user of the coordinator's key.** Two processes with one key confuse discovery, so with a
  daemon running every command goes through its Unix socket. With none running, the CLI runs the same core in-process
  (one implementation, two ways in); then `submit` must place the job at once, since nothing would wait.
- **Scheduling uses one snapshot of the machines per pass**, so the queue's order holds: a machine coming online
  mid-pass cannot let a later job jump an earlier one.
- **GPU work started outside fleet is matched by command line** (`ps -o command`, patterns in
  `$FLEET_HOME/gpu-processes`): process names are unreliable, and `dominion-ai-gpu train …` is unmistakable. Fleet's
  own jobs are left out, by process group (each job runs in its own) and by folder; before that, a fleet job matching a
  pattern was reported as outside work, which would also have stopped preemption.
- **Keep-awake while work waits:** a machine that could take a queued job is sent `KeepAwake` (3 minutes, renewed
  every minute); each running job holds its own assertion through its supervisor.
- **The coordinator's launchd daemon** (`contrib/install-coordinator.sh`) exists for the same reason as the agent's:
  launchd daemons are exempt from Local Network privacy, so direct paths work. Installing it is Tommy's call.

- **One process holds the coordinator's key at a time, enforced by a lock** (`$FLEET_HOME/coordinator.lock`, `flock`):
  the daemon holds it for its life, and a CLI command working in-process takes it first. The process-level test found
  the race this closes: a command run just as the daemon started went in-process with the same key, and the two
  endpoints with one identity left the command waiting forever. Every request now also times out (180 s).
- **Offline mode** (`FLEET_NETWORK=local`: no relays, no DNS; `FLEET_PORT` fixes the port; machines' addresses in the
  machines file) lets the process-level test, and machines without internet access, work.

## Decisions (Tommy, 2026-10-08)

1. **Home:** a separate repository under `~/Documents/GitHub`.
2. **iroh:** the latest release, pinned exactly in `Cargo.toml`.
3. **Relays:** n0's public relays.
4. **Platforms:** macOS only for now (launchd, IOKit sleep assertions, Metal). Linux and systemd come later, so
   nothing should rule them out.
5. **The GUI is in v1, with progress probes.** A job's specification may name probes; the agent evaluates them and
   reports the values on the event stream:
   - `jsonl_field`: the last value of a field in a JSON-lines file, with an optional total, for example
     `{"jsonl_field": {"file": "B/metrics.jsonl", "field": "iteration", "total": 384}}` for a bar, a rate and an
     estimated finish (nested fields with dots: `health.kl`);
   - `jsonl_series`: recent values of a field, for a sparkline (for example `approximate_kl`);
   - `glob_count`: how many paths match, for example `B/iter-*`;
   - `log_regex`: the last match of a pattern in the job's log (its first capture group if it has one).

   The agent logs a progress event only when a value itself changes (not when a rate or estimate drifts), at most
   every 15 s; probes use serde's external tagging (chosen when the wire format was postcard, which cannot carry
   internal tags).

   The GUI shows each machine and job with its bars, estimated finish and sparklines, and the CLI shows the same in
   text.
6. **Executables: open mode in v1.** Any staged binary may run: the same trust as today's SSH key through a
   narrower door. The agent runs as a dedicated standard (non-admin) user on each worker, so a misused key reaches only
   that account's files.
7. **Distributed evaluations: yes, staged into M3.**
   - **For:** training gets the laptop's CPU back, and evaluations need no GPU, so idle machines or cheap CPU-only
     cloud machines can run them. Larger and more frequent measurements become affordable, which narrows the
     single-checkpoint noise and makes plateau-triggered decays practical.
   - **Against:** placement rules are needed so evaluations don't slow training on the same machine, and runners
     depend on fleet sooner and must resubmit evaluations lost to a disconnect (each writes one JSON, so resubmitting
     is harmless).
   - **Plan:** an evaluation helper that splits measurements into jobs placed only on machines without a training job,
     and collects the results. Its first job proves results identical across machines: the same evaluation on both
     Macs, files compared.

## Future work

- **Sandboxing jobs with Seatbelt.** Run each job under `sandbox-exec` with a profile: writes only in its job folder,
  reads only its folder and system libraries, no network (the agent moves files, so jobs need none), and the GPU's
  system services allowed so Metal works. It stacks on the dedicated user. Caveats: Apple marks `sandbox-exec`
  deprecated (it still works and is widely relied on, by browsers, build tools and coding agents), and its profile
  language is thinly documented, so test it with Metal early.
- **Containers only for CPU-only or Linux workers.** Docker or Apple's `container` tool run Linux in a VM with no
  Metal, so they suit evaluation jobs or a Linux GPU box, not Metal training. A Linux build's results may not match
  the macOS build exactly.
- **Signed binaries.** Workers accept only binaries signed by a release key that needs Touch ID on the laptop:
  approval once, on the laptop, rather than pinning hashes on every worker.
- **Linux and systemd workers**, for a GPU box.
