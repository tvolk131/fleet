use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fleet::{
    PathReport,
    agent::{Agent, AgentConfig},
    blobs,
    coordinator::{Coordinator, JobFile, machines},
    daemon::{Core, DaemonRequest, DaemonResponse, Placement, QState, ask_daemon},
    jobs::{self, State},
    keys, paths,
    proto::{self, Hello, Request, Response, Status},
    supervise, system,
};
use iroh::{Endpoint, EndpointId, protocol::Router};
use serde::Serialize;

#[derive(Parser)]
#[command(
    version,
    about = "A small job runner for experiment machines over iroh"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manage persistent macOS VMs on a worker.
    Instance {
        #[arg(long, global = true)]
        on: Option<String>,
        #[arg(long, global = true)]
        json: bool,
        #[command(subcommand)]
        action: InstanceAction,
    },
    #[command(hide = true)]
    InstanceController { home: PathBuf, id: String },
    #[command(hide = true)]
    InstanceOperation {
        home: PathBuf,
        id: String,
        operation: String,
    },
    /// Print this machine's endpoint ID for a role, creating its key if there is none.
    Id {
        #[arg(long, value_enum, default_value_t = Role::Coordinator)]
        role: Role,
    },
    /// Allow a coordinator's endpoint ID to use this machine's agent.
    Allow {
        id: EndpointId,
        /// A note kept beside the ID in the allowlist, for example the machine's name.
        #[arg(long, default_value = "")]
        note: String,
    },
    /// Run the agent in the foreground until interrupted.
    Agent,
    /// Run the coordinator's daemon in the foreground: the queue, connections to machines, automatic fetching.
    Daemon,
    /// Name a machine's agent, so commands can say `m1pro` instead of its endpoint ID.
    Machine {
        #[command(subcommand)]
        action: MachineAction,
    },
    /// Every named machine: online or not, direct or relayed, power, load, GPU, running jobs.
    Machines {
        #[arg(long)]
        json: bool,
    },
    /// Queue a job file (JSON). With a daemon running it waits for a machine that fits; without one it must find a
    /// machine at once.
    Submit {
        job: PathBuf,
        #[command(flatten)]
        queue: QueueArgs,
        #[arg(long)]
        json: bool,
        /// Then print its output until it ends, and exit with its exit code.
        #[arg(long, short)]
        follow: bool,
    },
    /// Run a command as a job, without a job file: `fleet run --gpu -- ./bin/tool train --out results`. Files the
    /// command names are shipped as inputs.
    Run {
        #[command(flatten)]
        queue: QueueArgs,
        /// The job's name (default: the program and its first word).
        #[arg(long)]
        name: Option<String>,
        /// Ship this file or folder as an input too (repeatable).
        #[arg(long = "input")]
        inputs: Vec<PathBuf>,
        /// Offer outputs matching this pattern, for fetching (repeatable).
        #[arg(long = "publish")]
        publish: Vec<String>,
        /// Set an environment variable, NAME=value (repeatable).
        #[arg(long = "env")]
        env: Vec<String>,
        #[arg(long)]
        max_seconds: Option<u64>,
        #[arg(long)]
        json: bool,
        /// Then print its output until it ends, and exit with its exit code.
        #[arg(long, short)]
        follow: bool,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Queue one job per combination of parameters: `fleet sweep job.json --param seed=521,733 --param lr=3e-4,6e-4`.
    /// `{seed}` and `{lr}` are filled in throughout the job file and in --dest; without them in --dest, each job's
    /// outputs go to a folder of its own under it.
    Sweep {
        job: PathBuf,
        #[arg(long = "param", required = true)]
        params: Vec<String>,
        #[command(flatten)]
        queue: QueueArgs,
        #[arg(long)]
        json: bool,
    },
    /// The queue: every job submitted from this machine, its state and where it runs.
    Queue {
        #[arg(long)]
        json: bool,
    },
    /// List the jobs on one machine, as its agent reports them.
    Jobs {
        machine: String,
        #[arg(long)]
        json: bool,
    },
    /// Print a job's output; with --follow, keep printing until the job ends.
    Logs {
        job: String,
        #[arg(long)]
        stderr: bool,
        #[arg(long, short)]
        follow: bool,
    },
    /// Stop a job (or remove it from the queue if it has not started).
    Cancel { job: String },
    /// Freeze a running job where it is; nothing is lost, and its memory stays allocated. Paused time does not count
    /// toward its time limit, and a paused GPU job lets another GPU job start.
    Pause { job: String },
    /// Continue a paused job.
    #[command(name = "continue", alias = "unpause")]
    Unpause { job: String },
    /// Stop a job at a checkpoint, to continue it later (it needs a `resume` declaration). By default when its next
    /// checkpoint has settled, so nothing is lost; with --now at once, redoing the work since its latest checkpoint,
    /// which fleet states first. --plan only shows the two options.
    Suspend {
        job: String,
        #[arg(long)]
        now: bool,
        /// Give up (the job keeps running) if no checkpoint settles within this many minutes.
        #[arg(long)]
        within_minutes: Option<u64>,
        #[arg(long)]
        plan: bool,
        /// Suspend now without asking.
        #[arg(long)]
        yes: bool,
    },
    /// Queue a suspended job's next segment, on a machine or anywhere it fits.
    Resume {
        job: String,
        #[arg(long)]
        on: Option<String>,
    },
    /// Move a running job to another machine: suspend it (at its next checkpoint, or --now) and continue it there.
    Move {
        job: String,
        #[arg(long)]
        to: String,
        #[arg(long)]
        now: bool,
        #[arg(long)]
        yes: bool,
    },
    /// List a job's offered outputs with their sizes and hashes.
    Outputs {
        job: String,
        #[arg(long)]
        json: bool,
    },
    /// Copy a job's offered outputs into a folder; never overwrites, skips identical files.
    Fetch {
        job: String,
        dest: PathBuf,
        #[arg(long)]
        json: bool,
    },
    /// Open the desktop window: machines, the queue, progress, live logs (needs a running daemon).
    #[cfg(feature = "gui")]
    Gui,
    /// Run one job (started by the agent; not for direct use).
    #[command(hide = true)]
    Supervise { dir: PathBuf },
    /// Connect to an agent directly and report what it is, how it is doing, and the connection's paths (diagnostics;
    /// not while a daemon runs).
    Hello {
        machine: String,
        /// Then report the paths and status again every this many seconds, until interrupted.
        #[arg(long)]
        watch: Option<u64>,
        #[arg(long)]
        json: bool,
        /// Also try these addresses of the agent (diagnostics), for example its LAN address.
        #[arg(long = "addr")]
        addrs: Vec<std::net::SocketAddr>,
        /// Disable relays, so only direct paths can be used (diagnostics; pair with --addr).
        #[arg(long)]
        no_relay: bool,
    },
}

#[derive(Subcommand)]
enum InstanceAction {
    /// Probe the local guest command channel and Python helper, even for offline VMs.
    Ready {
        id: String,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 180)]
        timeout: u64,
    },
    /// Execute argv inside the VM. Output is retained; disconnecting does not cancel it.
    Exec {
        id: String,
        #[arg(long)]
        operation: Option<String>,
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long = "env")]
        env: Vec<String>,
        #[arg(long, default_value_t = 600)]
        timeout: u64,
        #[arg(long)]
        detach: bool,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Upload one regular file; creates guest parent directories and refuses conflicting content.
    Upload {
        id: String,
        source: PathBuf,
        path: String,
        #[arg(long)]
        operation: Option<String>,
    },
    /// Download one guest regular file; refuses to overwrite different local content.
    Download {
        id: String,
        path: String,
        dest: PathBuf,
        #[arg(long)]
        operation: Option<String>,
        #[arg(long, default_value_t = 1073741824)]
        max_bytes: u64,
    },
    /// Retrieve a successful download operation again, even after the VM stops.
    Fetch {
        id: String,
        operation: String,
        dest: PathBuf,
    },
    Operations {
        id: String,
    },
    Operation {
        id: String,
        operation: String,
    },
    /// Read retained operation output. Binary downloads use the download command instead.
    Logs {
        id: String,
        operation: String,
        #[arg(long)]
        stderr: bool,
        #[arg(long, short)]
        follow: bool,
        #[arg(long, default_value_t = 0)]
        from: u64,
    },
    /// Report VM disks, operation data, image cache and free space without deleting anything.
    Storage,
    /// Create a stopped instance from a JSON specification. IDs are stable for safe retries.
    Create {
        id: String,
        spec: PathBuf,
    },
    List,
    Inspect {
        id: String,
    },
    Start {
        id: String,
    },
    Stop {
        id: String,
    },
    /// Permanently delete a stopped VM's disk. Its ID remains reserved as a tombstone.
    Delete {
        id: String,
    },
}

/// Where a job may run, and what happens around it.
#[derive(clap::Args, Clone, Default)]
struct QueueArgs {
    /// Run on this machine only.
    #[arg(long)]
    on: Option<String>,
    /// Run only on a machine with this tag (repeatable), for example gpu:metal.
    #[arg(long = "tag")]
    tags: Vec<String>,
    /// The job needs the machine's GPU to itself.
    #[arg(long)]
    gpu: bool,
    /// Only on a machine running on AC power.
    #[arg(long)]
    ac_power: bool,
    /// Fetch the job's outputs into this folder as they appear: everything it writes except its unchanged inputs,
    /// unless --publish (or the job file's `publish`) narrows it.
    #[arg(long)]
    dest: Option<PathBuf>,
    /// Start only after this job has succeeded (repeatable).
    #[arg(long)]
    after: Vec<String>,
    #[arg(long, default_value_t = 0)]
    priority: i64,
    /// CPU threads the job uses (claimed from the machine's CPU slots).
    #[arg(long)]
    threads: Option<u32>,
    /// Execute in one disposable VM, configured by this JSON instance-spec file.
    #[arg(long)]
    vm: Option<PathBuf>,
    /// Seconds to allow graceful shutdown before SIGKILL (default 10).
    #[arg(long)]
    shutdown_grace_seconds: Option<u64>,
}

impl QueueArgs {
    fn request(&self, mut job: JobFile, dest: Option<PathBuf>) -> Result<DaemonRequest> {
        if let Some(path) = &self.vm {
            job.vm = Some(serde_json::from_slice(&std::fs::read(path)?)?);
        }
        if self.shutdown_grace_seconds.is_some() {
            job.shutdown_grace_seconds = self.shutdown_grace_seconds;
        }
        if self.threads.is_some() {
            job.threads = self.threads;
        }
        let placement = Placement {
            machine: self.on.clone(),
            tags: self.tags.clone(),
            gpu: self.gpu,
            ac_power: self.ac_power,
        };
        let dest = dest.map(std::path::absolute).transpose()?;
        if job.vm.is_some() {
            return Ok(DaemonRequest::SubmitVm {
                key: format!("request/{:032x}", rand::random::<u128>()),
                job: Box::new(job),
                placement,
                dest,
                after: self.after.clone(),
                priority: self.priority,
            });
        }
        // Old local daemons ignore unknown job fields. A new request kind makes unsupported grace fail clearly.
        if job.shutdown_grace_seconds.is_some() {
            Ok(DaemonRequest::SubmitOnce {
                key: format!("cli/{:032x}", rand::random::<u128>()),
                job: Box::new(job),
                placement,
                dest,
                after: self.after.clone(),
                priority: self.priority,
            })
        } else {
            Ok(DaemonRequest::Submit {
                job: Box::new(job),
                placement,
                dest,
                after: self.after.clone(),
                priority: self.priority,
            })
        }
    }
}

#[derive(Subcommand)]
enum MachineAction {
    Add {
        name: String,
        id: EndpointId,
        /// The agent's address (ip:port), needed only offline (FLEET_NETWORK=local); repeatable.
        #[arg(long = "addr")]
        addrs: Vec<std::net::SocketAddr>,
    },
    List,
}

#[derive(Clone, Copy, ValueEnum)]
enum Role {
    Agent,
    Coordinator,
}

impl Role {
    fn key_file(self) -> &'static str {
        match self {
            Role::Agent => "agent.key",
            Role::Coordinator => "coordinator.key",
        }
    }
}

const ALLOWLIST: &str = "allowed-coordinators";
const MACHINES: &str = "machines";

fn main() -> Result<()> {
    let cli = Cli::parse();
    // Set up first so the GUI logs too (with `RUST_LOG=iced_wgpu=info`, the graphics adapter it draws with).
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "fleet=info,warn".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    // The GUI runs its own event loop and Tokio runtime, so it starts before ours.
    #[cfg(feature = "gui")]
    if matches!(cli.command, Command::Gui) {
        return fleet::gui_legacy::run(keys::fleet_home()?).map_err(|e| anyhow::anyhow!("{e}"));
    }
    let runtime = tokio::runtime::Runtime::new()?;
    let result = runtime.block_on(run(cli));
    // Blocking work still going (an agent's wait on a job's supervisor lasts as long as the job) must not hold up the
    // exit: jobs run on without the agent, and a stopped agent has to be able to exit so launchd can restart it.
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
}

async fn run(cli: Cli) -> Result<()> {
    let home = keys::fleet_home()?;
    match cli.command {
        Command::Id { role } => println!(
            "{}",
            keys::load_or_create(&home.join(role.key_file()))?.public()
        ),
        Command::Allow { id, note } => {
            let path = home.join(ALLOWLIST);
            let added = keys::allow(&path, id, &note)?;
            println!(
                "{} {id} in {}",
                if added { "allowed" } else { "already allowed:" },
                path.display()
            );
        }
        Command::Agent => run_agent(&home).await?,
        Command::Supervise { dir } => supervise::run(&dir)?,
        Command::InstanceController { home, id } => fleet::instances::run_controller(&home, &id)?,
        Command::InstanceOperation {
            home,
            id,
            operation,
        } => fleet::instances::guest::supervise(&home, &id, &operation)?,
        Command::Daemon => {
            // Held for the daemon's life: no other process may use the coordinator's key meanwhile.
            let _lock = keys::lock_coordinator(&home, Duration::from_secs(60))?;
            let coordinator = coordinator(&home).await?;
            eprintln!("fleet daemon: coordinator {}", coordinator.endpoint.id());
            Core::run_daemon(&home, coordinator).await?;
        }
        Command::Machine { action } => match action {
            MachineAction::Add { name, id, addrs } => {
                machines::add(&home.join(MACHINES), &name, id, &addrs)?;
                println!("added {name} = {id}");
            }
            MachineAction::List => {
                for (name, id) in machines::read(&home.join(MACHINES))? {
                    println!("{name:<16} {id}");
                }
            }
        },
        Command::Hello {
            machine,
            watch,
            json,
            addrs,
            no_relay,
        } => {
            let Some(_lock) = keys::try_lock_coordinator(&home)? else {
                bail!(
                    "a fleet daemon (or another fleet command) holds the coordinator's key: use `fleet machines` instead"
                );
            };
            let agent = machines::resolve(&home.join(MACHINES), &machine)?;
            let target = addrs.iter().fold(agent, |a, s| a.with_ip_addr(*s));
            hello(&home, target, watch, json, no_relay).await?
        }
        command => run_core_command(&home, command).await?,
    }
    Ok(())
}

async fn coordinator(home: &Path) -> Result<Coordinator> {
    let key = keys::load_or_create(&home.join(Role::Coordinator.key_file()))?;
    let endpoint = fleet::endpoint_builder(key)?.bind().await?;
    let store = blobs::open(&home.join("coordinator-blobs")).await?;
    Ok(Coordinator::new(endpoint, store))
}

/// Sends a request to the running daemon, or, with none running, handles it in-process.
async fn ask_core(home: &Path, request: DaemonRequest) -> Result<DaemonResponse> {
    // The daemon if one answers; otherwise in-process, but only while holding the coordinator's key. If the key is
    // held (a daemon still starting, or another command), wait for the daemon's socket or for the key.
    let start = Instant::now();
    let response = loop {
        if let Some(response) = ask_daemon(home, &request).await? {
            break response;
        }
        if let Some(_lock) = keys::try_lock_coordinator(home)? {
            let coordinator = coordinator(home).await?;
            let core = Core::direct(home, coordinator)?;
            let response = core.handle(request).await;
            core.coordinator.endpoint.close().await;
            core.coordinator.store.shutdown().await.ok();
            break response;
        }
        if start.elapsed() > Duration::from_secs(30) {
            bail!(
                "the coordinator's key is held by another fleet process, and no daemon answers on its socket"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    match response {
        DaemonResponse::Error { message, .. } => bail!("{message}"),
        other => Ok(other),
    }
}

fn when(secs: u64) -> String {
    let ago = jobs::now().saturating_sub(secs);
    match ago {
        0..120 => format!("{ago} s ago"),
        120..7200 => format!("{} min ago", ago / 60),
        _ => format!("{:.1} h ago", ago as f64 / 3600.0),
    }
}

/// Sends a submission; returns the job's ID, its state and its machine.
async fn submit_only(
    home: &Path,
    request: DaemonRequest,
) -> Result<(String, QState, Option<String>)> {
    let DaemonResponse::Submitted {
        job,
        state,
        machine,
    } = ask_core(home, request).await?
    else {
        bail!("unexpected answer");
    };
    Ok((job, state, machine))
}

/// Sends a submission; prints the job's ID (or JSON) and returns it.
async fn submitted(home: &Path, request: DaemonRequest, json: bool) -> Result<String> {
    let (job, state, machine) = submit_only(home, request).await?;
    if json {
        print_json(&serde_json::json!({ "job": job, "state": state, "machine": machine }))?;
    } else {
        println!("{job}");
    }
    Ok(job)
}

/// Prints a job's output as it runs, until it ends; returns its exit code (1 if it ended any other way than exiting).
/// Asks the daemon, waiting while it is away (restarting) rather than acting in its place: a command that runs
/// in-process holds the coordinator's key, which would keep the new daemon from starting.
async fn ask_daemon_waiting(home: &Path, request: DaemonRequest) -> Result<DaemonResponse> {
    let mut noted = false;
    loop {
        match ask_daemon(home, &request).await {
            Ok(Some(DaemonResponse::Error { message, .. })) => bail!("{message}"),
            Ok(Some(response)) => return Ok(response),
            Ok(None) | Err(_) => {
                if !noted {
                    eprintln!("fleet: the daemon is not answering; waiting for it to come back");
                    noted = true;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn follow_job(home: &Path, id: &str) -> Result<i32> {
    use std::io::Write;
    // Following a daemon's job, keep to the daemon (see `ask_daemon_waiting`).
    let daemon = ask_daemon(home, &DaemonRequest::Ping).await?.is_some();
    let ask = |request: DaemonRequest| async move {
        if daemon {
            ask_daemon_waiting(home, request).await
        } else {
            ask_core(home, request).await
        }
    };
    let mut from = 0;
    loop {
        let job = match ask(DaemonRequest::Queue).await? {
            DaemonResponse::Queue { jobs } => jobs
                .into_iter()
                .find(|j| j.id == id)
                .context("the job left the queue")?,
            _ => bail!("unexpected answer"),
        };
        if job.machine.is_some()
            && let Ok(DaemonResponse::Logs { data, next, .. }) = ask(DaemonRequest::Logs {
                job: id.to_string(),
                stderr: false,
                from,
            })
            .await
        {
            std::io::stdout().write_all(data.as_bytes())?;
            std::io::stdout().flush()?;
            from = next;
        }
        if job.state.finished() {
            // One last read, in case the job wrote more as it ended.
            if let Ok(DaemonResponse::Logs { data, .. }) = ask(DaemonRequest::Logs {
                job: id.to_string(),
                stderr: false,
                from,
            })
            .await
            {
                std::io::stdout().write_all(data.as_bytes())?;
            }
            let code = job.exit.as_ref().and_then(|e| e.code);
            eprintln!("fleet: {id} {}", format!("{:?}", job.state).to_lowercase());
            return Ok(match (job.state, code) {
                (QState::Succeeded, _) => 0,
                (_, Some(c)) if c != 0 => c,
                _ => 1,
            });
        }
        tokio::time::sleep(Duration::from_millis(1500)).await;
    }
}

/// Prints what suspending a job would cost; returns whether a loss would be incurred.
async fn show_plan(home: &Path, job: &str) -> Result<bool> {
    let DaemonResponse::Plan { plan } = ask_core(
        home,
        DaemonRequest::SuspendPlan {
            job: job.to_string(),
        },
    )
    .await?
    else {
        bail!("unexpected answer");
    };
    for line in fleet::suspend::describe(&plan) {
        println!("{line}");
    }
    Ok(plan.loss_units.is_some_and(|u| u > 0.0) || plan.latest.is_none())
}

/// Asks before suspending now (a terminal is needed; otherwise pass --yes).
fn confirm(loses_work: bool) -> Result<()> {
    use std::io::{BufRead, IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        bail!("not suspending without confirmation: pass --yes");
    }
    print!(
        "{}Suspend now? [y/N] ",
        if loses_work {
            ""
        } else {
            "Nothing would be lost. "
        }
    );
    std::io::stdout().flush()?;
    let mut answer = String::new();
    std::io::stdin().lock().read_line(&mut answer)?;
    if !matches!(answer.trim(), "y" | "Y" | "yes") {
        bail!("not suspended");
    }
    Ok(())
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

async fn run_core_command(home: &Path, command: Command) -> Result<()> {
    if let Command::Instance { on, json, action } = command {
        return instance_command(
            home,
            &on.context("instance commands require --on <machine>")?,
            json,
            action,
        )
        .await;
    }
    match command {
        Command::Submit {
            job,
            queue,
            json,
            follow,
        } => {
            let mut file: JobFile = serde_json::from_slice(&std::fs::read(&job)?)
                .with_context(|| format!("reading the job file {}", job.display()))?;
            // Relative inputs are relative to the job file, not to the daemon's working directory.
            let base = std::path::absolute(&job)?
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default();
            fleet::compose::absolute_inputs(&mut file, &base);
            let dest = queue.dest.clone();
            let id = submitted(home, queue.request(file, dest)?, json).await?;
            if follow {
                std::process::exit(follow_job(home, &id).await?);
            }
        }
        Command::Run {
            queue,
            name,
            inputs,
            publish,
            env,
            max_seconds,
            json,
            follow,
            command,
        } => {
            let cwd = std::env::current_dir()?;
            let mut file = fleet::compose::job_from_command(&cwd, &command, &inputs)?;
            if let Some(name) = name {
                file.name = name;
            }
            file.publish = publish;
            file.max_seconds = max_seconds;
            for pair in env {
                let (k, v) = pair
                    .split_once('=')
                    .with_context(|| format!("--env {pair}: expected NAME=value"))?;
                file.env.insert(k.to_string(), v.to_string());
            }
            let dest = queue.dest.clone();
            let id = submitted(home, queue.request(file, dest)?, json).await?;
            if follow {
                std::process::exit(follow_job(home, &id).await?);
            }
        }
        Command::Sweep {
            job,
            params,
            queue,
            json,
        } => {
            let template: serde_json::Value = serde_json::from_slice(&std::fs::read(&job)?)
                .with_context(|| format!("reading the job file {}", job.display()))?;
            let params = fleet::compose::parse_params(&params)?;
            let base = std::path::absolute(&job)?
                .parent()
                .map(Path::to_path_buf)
                .unwrap_or_default();
            let mut made = Vec::new();
            for (combo, mut file) in fleet::compose::expand(&template, &params)? {
                fleet::compose::absolute_inputs(&mut file, &base);
                let dest = queue.dest.as_ref().map(|d| {
                    let filled =
                        fleet::compose::substitute(&serde_json::json!(d.to_string_lossy()), &combo);
                    let filled = PathBuf::from(filled.as_str().unwrap_or_default());
                    if filled == *d {
                        let folder: Vec<String> =
                            combo.iter().map(|(k, v)| format!("{k}-{v}")).collect();
                        d.join(folder.join("_"))
                    } else {
                        filled
                    }
                });
                let request = queue.request(file, dest)?;
                let id = if json {
                    submit_only(home, request).await?.0
                } else {
                    submitted(home, request, false).await?
                };
                made.push(serde_json::json!({ "job": id, "params": combo }));
            }
            if json {
                print_json(&made)?;
            } else if made.len() > 1 {
                eprintln!("queued {} jobs", made.len());
            }
        }
        Command::Queue { json } => {
            let DaemonResponse::Queue { jobs } = ask_core(home, DaemonRequest::Queue).await? else {
                bail!("unexpected answer")
            };
            if json {
                return print_json(&jobs);
            }
            for j in jobs {
                let detail = match (&j.exit, j.finished_at, j.assigned_at) {
                    (Some(e), _, _) => format!(
                        "{:?}, exit {}",
                        e.reason,
                        e.code.map_or_else(
                            || format!("signal {}", e.signal.unwrap_or(0)),
                            |c| c.to_string()
                        )
                    ),
                    (None, Some(f), _) => format!("ended {}", when(f)),
                    (None, None, Some(a)) => format!("placed {}", when(a)),
                    (None, None, None) => format!("queued {}", when(j.submitted_at)),
                };
                let fetched = if j.dest.is_some() {
                    format!("; fetched {} file(s)", j.fetched_files)
                } else {
                    String::new()
                };
                let progress = j
                    .progress
                    .as_deref()
                    .map(fleet::probes::summary)
                    .filter(|s| !s.is_empty() && !j.state.finished())
                    .map(|s| format!("; {s}"))
                    .unwrap_or_default();
                let segment = if j.segment > 1 {
                    format!("; segment {}", j.segment)
                } else {
                    String::new()
                };
                let continued = j
                    .continued_by
                    .as_ref()
                    .map(|c| format!("; continued as {c}"))
                    .unwrap_or_default();
                if let Some(stop) = &j.stopping {
                    println!(
                        "  stopping {:?}; force termination in {} s",
                        stop.reason,
                        stop.deadline.saturating_sub(jobs::now())
                    );
                }
                let phase = j
                    .vm_status
                    .as_ref()
                    .map(|s| format!("; {}", s.phase.label()))
                    .unwrap_or_default();
                let note = j
                    .error
                    .or_else(|| j.vm_status.and_then(|s| s.error))
                    .map(|e| format!("  ({e})"))
                    .unwrap_or_default();
                println!(
                    "{:<40} {:<10} {:<10} {detail}{fetched}{progress}{segment}{continued}{phase}{note}",
                    j.id,
                    format!("{:?}", j.state).to_lowercase(),
                    j.machine.unwrap_or_else(|| "-".into())
                );
            }
        }
        Command::Machines { json } => {
            let DaemonResponse::Machines { machines } =
                ask_core(home, DaemonRequest::Machines).await?
            else {
                bail!("unexpected answer")
            };
            if json {
                return print_json(&machines);
            }
            for m in machines {
                let path = m
                    .paths
                    .first()
                    .map(|p| format!("{} {:.0} ms", p.kind, p.rtt_ms))
                    .unwrap_or_default();
                let status = m.status.as_ref().map(|s| {
                    let power = match s.on_ac_power {
                        Some(true) => "AC".to_string(),
                        Some(false) => format!("battery {}%", s.battery_percent.unwrap_or(0)),
                        None => "power ?".into(),
                    };
                    let load = s
                        .load
                        .map(|l| format!("load {:.1}", l[0]))
                        .unwrap_or_default();
                    let gpu = s
                        .gpu_busy_external
                        .as_ref()
                        .map(|p| format!(", GPU busy ({p})"))
                        .unwrap_or_default();
                    let slots = m
                        .hello
                        .as_ref()
                        .map(|h| format!(", {}/{} threads", m.threads_used, h.cpu_slots))
                        .unwrap_or_default();
                    format!(
                        "{power}, {load}, {} running{slots}{gpu}",
                        s.running_jobs.len()
                    )
                });
                let version = m
                    .hello
                    .as_ref()
                    .map(|h| {
                        format!(
                            "; fleet {}, {}",
                            h.agent_version,
                            proto::describe(h.protocol)
                        )
                    })
                    .unwrap_or_default();
                println!(
                    "{:<12} {:<8} {:<16} {}{version}{}",
                    m.name,
                    if m.online { "online" } else { "offline" },
                    path,
                    status.unwrap_or_default(),
                    m.error.map(|e| format!("  ({e})")).unwrap_or_default()
                );
            }
        }
        Command::Jobs { machine, json } => {
            let DaemonResponse::MachineJobs { jobs } =
                ask_core(home, DaemonRequest::MachineJobs { machine }).await?
            else {
                bail!("unexpected answer")
            };
            if json {
                return print_json(&jobs);
            }
            for j in jobs {
                println!("{:<40} {:?}", j.id, j.state);
            }
        }
        Command::Logs {
            job,
            stderr,
            follow,
        } => {
            let mut from = 0;
            loop {
                let request = DaemonRequest::Logs {
                    job: job.clone(),
                    stderr,
                    from,
                };
                let DaemonResponse::Logs { data, next, done } = ask_core(home, request).await?
                else {
                    bail!("unexpected answer")
                };
                use std::io::Write;
                std::io::stdout().write_all(data.as_bytes())?;
                std::io::stdout().flush()?;
                let caught_up = next == from;
                from = next;
                if done || (!follow && caught_up) {
                    break;
                }
                if caught_up {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => break,
                        _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    }
                }
            }
        }
        Command::Cancel { job } => {
            ask_core(home, DaemonRequest::Cancel { job: job.clone() }).await?;
            println!("cancelling {job}");
        }
        Command::Pause { job } => {
            ask_core(home, DaemonRequest::Pause { job: job.clone() }).await?;
            println!("paused {job}");
        }
        Command::Unpause { job } => {
            ask_core(home, DaemonRequest::Continue { job: job.clone() }).await?;
            println!("continued {job}");
        }
        Command::Suspend {
            job,
            now,
            within_minutes,
            plan,
            yes,
        } => {
            let shown = show_plan(home, &job).await?;
            if plan {
                return Ok(());
            }
            if now && !yes {
                confirm(shown)?;
            }
            let request = DaemonRequest::Suspend {
                job: job.clone(),
                now,
                within_seconds: within_minutes.map(|m| m * 60),
            };
            ask_core(home, request).await?;
            println!(
                "{}",
                if now {
                    format!("suspending {job} now")
                } else {
                    format!("{job} will be suspended at its next checkpoint")
                }
            );
        }
        Command::Resume { job, on } => {
            let DaemonResponse::Resumed { job: next } =
                ask_core(home, DaemonRequest::Resume { job, on }).await?
            else {
                bail!("unexpected answer")
            };
            println!("{next}");
        }
        Command::Move { job, to, now, yes } => {
            let shown = show_plan(home, &job).await?;
            if now && !yes {
                confirm(shown)?;
            }
            ask_core(
                home,
                DaemonRequest::Move {
                    job: job.clone(),
                    to: to.clone(),
                    now,
                },
            )
            .await?;
            println!(
                "{job} will move to {to} {}",
                if now { "now" } else { "at its next checkpoint" }
            );
        }
        Command::Outputs { job, json } => {
            let DaemonResponse::Outputs { files } =
                ask_core(home, DaemonRequest::Outputs { job }).await?
            else {
                bail!("unexpected answer")
            };
            if json {
                return print_json(&files);
            }
            for f in files {
                println!("{:>12}  {}  {}", f.size, f.hash.fmt_short(), f.path);
            }
        }
        Command::Fetch { job, dest, json } => {
            let dest = std::path::absolute(dest)?;
            let DaemonResponse::Fetched { report } =
                ask_core(home, DaemonRequest::Fetch { job, dest }).await?
            else {
                bail!("unexpected answer")
            };
            if json {
                return print_json(&report);
            }
            println!(
                "fetched {} file(s), updated {}, {:.1} MB; {} already present",
                report.fetched.len(),
                report.updated.len(),
                report.bytes as f64 / 1e6,
                report.skipped.len()
            );
        }
        _ => unreachable!("handled in main"),
    }
    Ok(())
}

async fn instance_command(
    home: &Path,
    machine: &str,
    json: bool,
    action: InstanceAction,
) -> Result<()> {
    let response = match action {
        action @ (InstanceAction::Ready { .. }
        | InstanceAction::Exec { .. }
        | InstanceAction::Upload { .. }
        | InstanceAction::Download { .. }
        | InstanceAction::Fetch { .. }
        | InstanceAction::Operations { .. }
        | InstanceAction::Operation { .. }
        | InstanceAction::Logs { .. }
        | InstanceAction::Storage) => return guest_command(home, machine, json, action).await,
        InstanceAction::Create { id, spec } => {
            fleet::instances::valid_id(&id)?;
            let spec = serde_json::from_slice(&std::fs::read(spec)?)?;
            ask_core(
                home,
                DaemonRequest::InstanceCreate {
                    machine: machine.into(),
                    id,
                    spec,
                },
            )
            .await?
        }
        action => {
            let DaemonResponse::Instances { instances, online } = ask_core(
                home,
                DaemonRequest::Instances {
                    machine: machine.into(),
                },
            )
            .await?
            else {
                bail!("unexpected instance inventory response");
            };
            match action {
                InstanceAction::List => DaemonResponse::Instances { instances, online },
                InstanceAction::Inspect { id } => {
                    let instance = instances
                        .into_iter()
                        .find(|i| i.record.id == id)
                        .context("unknown instance")?;
                    if !online {
                        eprintln!("worker offline; showing cached instance state");
                    }
                    DaemonResponse::Instance { instance }
                }
                action => {
                    if !online {
                        bail!("worker offline; cannot change instance state");
                    }
                    let (id, desired) = match action {
                        InstanceAction::Start { id } => (id, fleet::instances::Desired::Running),
                        InstanceAction::Stop { id } => (id, fleet::instances::Desired::Stopped),
                        InstanceAction::Delete { id } => (id, fleet::instances::Desired::Deleted),
                        _ => unreachable!(),
                    };
                    let instance = instances
                        .into_iter()
                        .find(|i| i.record.id == id)
                        .context("unknown instance")?;
                    ask_core(
                        home,
                        DaemonRequest::InstanceSet {
                            machine: machine.into(),
                            id,
                            desired,
                            revision: instance.record.revision,
                        },
                    )
                    .await?
                }
            }
        }
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&response)?);
        return Ok(());
    }
    let instances = match response {
        DaemonResponse::Instance { instance } => vec![instance],
        DaemonResponse::Instances { instances, online } => {
            if !online {
                eprintln!("worker offline; showing cached instance state");
            }
            instances
        }
        _ => bail!("unexpected instance response"),
    };
    for i in instances {
        println!(
            "{}  {:?} (wanted {:?})  {} CPU, {} MiB, GPU {:?}, {:?}{}",
            i.record.id,
            i.observed.state,
            i.record.desired,
            i.record.spec.cpus,
            i.record.spec.memory_mb,
            i.record.spec.gpu,
            i.record.spec.network,
            i.observed
                .error
                .as_ref()
                .map(|s| format!(" — {s}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

async fn guest_request(
    home: &Path,
    machine: &str,
    request: fleet::instances::guest::Request,
) -> Result<fleet::instances::guest::Response> {
    let DaemonResponse::InstanceGuest { response } = ask_core(
        home,
        DaemonRequest::InstanceGuest {
            machine: machine.into(),
            request,
        },
    )
    .await?
    else {
        bail!("unexpected guest response");
    };
    Ok(response)
}

async fn instance_revision(home: &Path, machine: &str, id: &str) -> Result<u64> {
    let DaemonResponse::Instances { instances, online } = ask_core(
        home,
        DaemonRequest::Instances {
            machine: machine.into(),
        },
    )
    .await?
    else {
        bail!("unexpected VM inventory");
    };
    if !online {
        bail!("worker is offline");
    }
    Ok(instances
        .into_iter()
        .find(|i| i.record.id == id)
        .context("unknown instance")?
        .record
        .revision)
}

async fn guest_wait(
    home: &Path,
    machine: &str,
    id: &str,
    operation: &str,
    stream: bool,
) -> Result<fleet::instances::guest::Operation> {
    use fleet::instances::guest::{Request as G, Response as R};
    use std::io::Write;
    let mut offsets = [0, 0];
    loop {
        let R::Operation(op) = guest_request(
            home,
            machine,
            G::Get {
                id: id.into(),
                operation: operation.into(),
            },
        )
        .await?
        else {
            bail!("unexpected operation status");
        };
        let mut drained = true;
        if stream {
            for (index, stderr) in [false, true].into_iter().enumerate() {
                let R::Logs(log) = guest_request(
                    home,
                    machine,
                    G::Logs {
                        id: id.into(),
                        operation: operation.into(),
                        stderr,
                        from: offsets[index],
                    },
                )
                .await?
                else {
                    bail!("unexpected operation log");
                };
                if stderr {
                    std::io::stderr().write_all(&log.data)?;
                    std::io::stderr().flush()?;
                } else {
                    std::io::stdout().write_all(&log.data)?;
                    std::io::stdout().flush()?;
                }
                offsets[index] = log.next;
                drained &= log.done;
            }
        }
        if op.state.finished() && drained {
            return Ok(op);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn guest_command(
    home: &Path,
    machine: &str,
    json: bool,
    action: InstanceAction,
) -> Result<()> {
    use fleet::instances::guest::{OperationState, Request as G, Response as R, Spec};
    use std::io::Write;
    match action {
        InstanceAction::Storage => {
            let R::Storage(r) = guest_request(home, machine, G::Storage).await? else {
                bail!("unexpected storage response");
            };
            if json {
                return print_json(&r);
            }
            println!(
                "Free: {:.2} GiB; VM capacity: {} / {} GB; free-space guard: {} GiB",
                r.free_bytes as f64 / (1u64 << 30) as f64,
                r.reserved_capacity_gb,
                r.disk_budget_gb,
                r.min_free_disk_gb
            );
            for i in r.instances {
                println!(
                    "{}: {} GB capacity; disk {:.2} GiB logical, {:.2} GiB allocated; management {:.2} MiB allocated",
                    i.id,
                    i.capacity_gb,
                    i.disk.logical_bytes as f64 / (1u64 << 30) as f64,
                    i.disk.allocated_bytes as f64 / (1u64 << 30) as f64,
                    i.management.allocated_bytes as f64 / (1u64 << 20) as f64
                );
            }
            println!(
                "Image cache: {:.2} GiB logical, {:.2} GiB allocated; shared blob store: {:.2} GiB allocated\n{}",
                r.image_cache.logical_bytes as f64 / (1u64 << 30) as f64,
                r.image_cache.allocated_bytes as f64 / (1u64 << 30) as f64,
                r.blob_store.allocated_bytes as f64 / (1u64 << 30) as f64,
                r.note
            );
        }
        InstanceAction::Ready { id, wait, timeout } => {
            let deadline = Instant::now() + Duration::from_secs(timeout.min(3600));
            loop {
                let R::Ready(r) = guest_request(home, machine, G::Ready { id: id.clone() }).await?
                else {
                    bail!("unexpected readiness response");
                };
                if r.ready || !wait || Instant::now() >= deadline {
                    if json {
                        print_json(&r)?;
                    } else {
                        println!(
                            "{}: {}",
                            if r.ready { "ready" } else { "not ready" },
                            r.message
                        );
                    }
                    if !r.ready {
                        bail!("guest is not ready");
                    }
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        InstanceAction::Operations { id } => {
            let R::Operations(ops) = guest_request(home, machine, G::List { id }).await? else {
                bail!("unexpected operations response");
            };
            if json {
                print_json(&ops)?;
            } else {
                for op in ops {
                    println!(
                        "{}  {}  {:?}  exit={:?}",
                        op.id, op.kind, op.state, op.exit_code
                    );
                }
            }
        }
        InstanceAction::Operation { id, operation } => {
            let R::Operation(op) = guest_request(home, machine, G::Get { id, operation }).await?
            else {
                bail!("unexpected operation response");
            };
            print_json(&op)?;
        }
        InstanceAction::Fetch {
            id,
            operation,
            dest,
        } => {
            let response = ask_core(
                home,
                DaemonRequest::InstanceDownload {
                    machine: machine.into(),
                    id,
                    operation,
                    dest: std::path::absolute(dest)?,
                },
            )
            .await?;
            if json {
                print_json(&response)?;
            } else if let DaemonResponse::InstanceDownloaded { path, bytes, .. } = response {
                println!("Downloaded {bytes} bytes to {}", path.display());
            } else {
                bail!("unexpected download response");
            }
        }
        InstanceAction::Logs {
            id,
            operation,
            stderr,
            follow,
            mut from,
        } => loop {
            let R::Logs(log) = guest_request(
                home,
                machine,
                G::Logs {
                    id: id.clone(),
                    operation: operation.clone(),
                    stderr,
                    from,
                },
            )
            .await?
            else {
                bail!("unexpected guest logs");
            };
            if json {
                print_json(&log)?;
            } else {
                std::io::stdout().write_all(&log.data)?;
                std::io::stdout().flush()?;
            }
            from = log.next;
            if !follow || log.done {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        },
        action @ (InstanceAction::Exec { .. }
        | InstanceAction::Upload { .. }
        | InstanceAction::Download { .. }) => {
            let (id, operation) = match &action {
                InstanceAction::Exec { id, operation, .. }
                | InstanceAction::Upload { id, operation, .. }
                | InstanceAction::Download { id, operation, .. } => (
                    id.clone(),
                    operation
                        .clone()
                        .unwrap_or_else(|| format!("op-{:016x}", rand::random::<u64>())),
                ),
                _ => unreachable!(),
            };
            fleet::instances::valid_id(&operation)?;
            eprintln!(
                "Guest operation {operation} on {id}@{machine}; use instance operation/logs to reconnect."
            );
            let revision = instance_revision(home, machine, &id).await?;
            let mut dest = None;
            let mut detach = false;
            let mut stream = false;
            let response = match action {
                InstanceAction::Exec {
                    cwd,
                    env,
                    timeout,
                    command,
                    detach: d,
                    ..
                } => {
                    detach = d;
                    stream = !json;
                    let mut vars = std::collections::BTreeMap::new();
                    for entry in env {
                        let (k, v) = entry.split_once('=').context("--env requires NAME=value")?;
                        vars.insert(k.into(), v.into());
                    }
                    guest_request(
                        home,
                        machine,
                        G::Start {
                            id: id.clone(),
                            operation: operation.clone(),
                            revision,
                            spec: Spec::Exec {
                                argv: command,
                                cwd,
                                env: vars,
                                timeout_seconds: timeout,
                            },
                        },
                    )
                    .await?
                }
                InstanceAction::Upload { source, path, .. } => {
                    let DaemonResponse::InstanceGuest { response } = ask_core(
                        home,
                        DaemonRequest::InstanceUpload {
                            machine: machine.into(),
                            id: id.clone(),
                            operation: operation.clone(),
                            revision,
                            source: std::path::absolute(source)?,
                            path,
                        },
                    )
                    .await?
                    else {
                        bail!("unexpected upload response");
                    };
                    response
                }
                InstanceAction::Download {
                    path,
                    dest: d,
                    max_bytes,
                    ..
                } => {
                    dest = Some(std::path::absolute(d)?);
                    guest_request(
                        home,
                        machine,
                        G::Start {
                            id: id.clone(),
                            operation: operation.clone(),
                            revision,
                            spec: Spec::Download {
                                path,
                                max_bytes,
                                version: None,
                            },
                        },
                    )
                    .await?
                }
                _ => unreachable!(),
            };
            let R::Operation(op) = response else {
                bail!("unexpected guest start response");
            };
            if detach {
                if json {
                    print_json(&op)?;
                } else {
                    println!("{operation}");
                }
                return Ok(());
            }
            let op = guest_wait(home, machine, &id, &operation, stream).await?;
            if op.state != OperationState::Succeeded {
                if json {
                    print_json(&op)?;
                }
                bail!(
                    "operation {} {:?}: {}; retained stderr is available via instance logs --stderr",
                    op.id,
                    op.state,
                    op.error.unwrap_or_default()
                );
            }
            if let Some(dest) = dest {
                let response = ask_core(
                    home,
                    DaemonRequest::InstanceDownload {
                        machine: machine.into(),
                        id,
                        operation,
                        dest,
                    },
                )
                .await?;
                if json {
                    print_json(&response)?;
                } else if let DaemonResponse::InstanceDownloaded { path, bytes, .. } = response {
                    println!("Downloaded {bytes} bytes to {}", path.display());
                }
            } else if json {
                print_json(&op)?;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

async fn run_agent(home: &std::path::Path) -> Result<()> {
    let key = keys::load_or_create(&home.join(Role::Agent.key_file()))?;
    let allow = keys::read_allowlist(&home.join(ALLOWLIST))?;
    if allow.is_empty() {
        bail!(
            "no coordinator is allowed yet: run `fleet allow <coordinator-id>` on this machine first"
        );
    }
    let allow = Arc::new(allow);
    let mut hello = tokio::task::spawn_blocking(system::hello).await?;
    if let Ok(slots) = std::fs::read_to_string(home.join("cpu-slots")) {
        hello.cpu_slots = slots
            .trim()
            .parse()
            .context("the cpu-slots file must hold a number")?;
    }
    let store = blobs::open(&home.join("agent-blobs")).await?;
    let config = AgentConfig {
        home: home.to_path_buf(),
        supervisor: std::env::current_exe()?,
        store: store.clone(),
    };
    let endpoint = fleet::endpoint_builder(key)?.bind().await?;
    let id = endpoint.id();
    let running = jobs::list(home)?
        .into_iter()
        .filter(|j| j.state == State::Running)
        .count();
    let router = Router::builder(endpoint)
        .accept(
            proto::ALPN,
            Agent::start(allow.clone(), hello.clone(), config)?,
        )
        .accept(iroh_blobs::ALPN, blobs::protocol(&store, allow.clone()))
        .spawn();
    if running > 0 {
        eprintln!("re-adopted {running} running job(s) from a previous run");
    }
    eprintln!(
        "fleet agent {id} on {} ({}); allowing {} coordinator(s)",
        hello.hostname,
        hello.os,
        allow.len()
    );
    match tokio::time::timeout(Duration::from_secs(15), router.endpoint().online()).await {
        Ok(()) => eprintln!("online: reachable by ID through discovery and relays"),
        Err(_) => {
            eprintln!("not online after 15 s (no relay yet); direct connections may still work")
        }
    }
    fleet::shutdown_signal().await;
    router.shutdown().await?;
    Ok(())
}

#[derive(Serialize)]
struct Report {
    agent: EndpointId,
    connect_ms: f64,
    /// When a direct path was first selected, if within a few seconds of connecting.
    direct_after_ms: Option<f64>,
    hello: Hello,
    status: Status,
    paths: Vec<PathReport>,
}

async fn hello(
    home: &std::path::Path,
    target: iroh::EndpointAddr,
    watch: Option<u64>,
    json: bool,
    no_relay: bool,
) -> Result<()> {
    let key = keys::load_or_create(&home.join(Role::Coordinator.key_file()))?;
    let mut builder = fleet::endpoint_builder(key)?;
    if no_relay {
        builder = builder.relay_mode(iroh::RelayMode::Disabled);
    }
    let endpoint = builder.bind().await?;
    let result = session(&endpoint, target, watch, json).await;
    endpoint.close().await;
    result
}

async fn session(
    endpoint: &Endpoint,
    target: iroh::EndpointAddr,
    watch: Option<u64>,
    json: bool,
) -> Result<()> {
    let agent = target.id;
    let start = Instant::now();
    let connection = endpoint
        .connect(target, proto::ALPN)
        .await
        .with_context(|| format!("connecting to {agent}"))?;
    let connect_ms = ms(start.elapsed());
    let Response::Hello(hello) = proto::request(&connection, &Request::hello()).await? else {
        bail!("the agent did not answer Hello");
    };
    let first_status = status(&connection).await?;
    let mut direct_after_ms = None;
    for _ in 0..50 {
        if paths(&connection)
            .first()
            .is_some_and(|p| p.selected && p.kind == "direct")
        {
            direct_after_ms = Some(ms(start.elapsed()));
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let report = Report {
        agent,
        connect_ms,
        direct_after_ms,
        hello,
        status: first_status,
        paths: paths(&connection),
    };
    if json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_report(&report);
    }
    if let Some(every) = watch {
        let mut tick = tokio::time::interval(Duration::from_secs(every.max(1)));
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tick.tick() => {
                    let at = Instant::now();
                    let line = match status(&connection).await {
                        Ok(s) => {
                            let round_trip = ms(at.elapsed());
                            let p = paths(&connection);
                            if json {
                                serde_json::to_string(&serde_json::json!({"elapsed_s": start.elapsed().as_secs(), "request_ms": round_trip, "status": s, "paths": p}))?
                            } else {
                                format!("{:>6.0} s  request {round_trip:>6.1} ms  {}", start.elapsed().as_secs_f64(), path_summary(&p))
                            }
                        }
                        Err(e) => format!("{:>6.0} s  request failed: {e:#}", start.elapsed().as_secs_f64()),
                    };
                    println!("{line}");
                }
            }
        }
    }
    connection.close(0u32.into(), b"done");
    Ok(())
}

async fn status(connection: &iroh::endpoint::Connection) -> Result<Status> {
    match proto::request(connection, &Request::Status).await? {
        Response::Status(s) => Ok(s),
        other => bail!("unexpected answer to Status: {other:?}"),
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn path_summary(paths: &[PathReport]) -> String {
    paths
        .iter()
        .map(|p| {
            format!(
                "{}{} {:.1} ms",
                if p.selected { "*" } else { "" },
                p.kind,
                p.rtt_ms
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn print_report(r: &Report) {
    let gib = |b: u64| b as f64 / (1u64 << 30) as f64;
    let h = &r.hello;
    println!("agent    {} ({})", h.hostname, r.agent.fmt_short());
    println!(
        "         fleet {}, protocol {}; {}",
        h.agent_version, h.protocol, h.os
    );
    println!(
        "         {}, {} cores, {:.0} GiB",
        h.cpu,
        h.cores,
        gib(h.memory_bytes)
    );
    if let Some(gpu) = &h.gpu {
        println!("         GPU {gpu}");
    }
    let s = &r.status;
    let load = s
        .load
        .map(|l| format!("load {:.2} {:.2} {:.2}", l[0], l[1], l[2]))
        .unwrap_or_default();
    let power = match (s.on_ac_power, s.battery_percent) {
        (Some(true), Some(p)) => format!("AC power ({p}%)"),
        (Some(true), None) => "AC power".into(),
        (Some(false), Some(p)) => format!("battery {p}%"),
        _ => "power unknown".into(),
    };
    let disk = s
        .disk_free_bytes
        .map(|b| format!("{:.0} GiB free", gib(b)))
        .unwrap_or_default();
    let up = s
        .uptime_seconds
        .map(|u| format!("up {:.1} h", u as f64 / 3600.0))
        .unwrap_or_default();
    println!("status   {load}; {power}; {disk}; {up}");
    let direct = r
        .direct_after_ms
        .map(|d| format!("direct path after {:.0} ms", d))
        .unwrap_or_else(|| "no direct path within 5 s".into());
    println!("connect  {:.0} ms; {direct}", r.connect_ms);
    for p in &r.paths {
        println!(
            "path     {} {:<6} {:>7.1} ms  {}",
            if p.selected { "*" } else { " " },
            p.kind,
            p.rtt_ms,
            p.remote
        );
    }
}
