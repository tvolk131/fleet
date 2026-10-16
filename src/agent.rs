//! The agent: answers allowlisted coordinators, and closes every other connection at once.

use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{Read, Seek, SeekFrom},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, bail};
use iroh::{
    EndpointId,
    endpoint::{Connection, RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_blobs::{Hash, api::Store};

use crate::{
    events::{EventKind, EventLog},
    instances,
    jobs::{self, Job, JobSpec, OutputFile, State, now},
    proto::{Hello, MAX_LOG_CHUNK, MAX_MESSAGE, REFUSED, Request, Response},
    supervise, system,
};

/// Each job's numeric probes' first readings, by job and probe name.
type Origins = Arc<Mutex<HashMap<String, HashMap<String, crate::probes::Origin>>>>;

/// Output hashes by (path, size, modification time).
type HashCache = HashMap<(PathBuf, u64, SystemTime), Hash>;

/// Where the agent keeps its jobs and how it starts their supervisors.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// `$FLEET_HOME`: jobs live in `jobs/` under it.
    pub home: PathBuf,
    /// The `fleet` binary that runs `fleet supervise <job folder>`.
    pub supervisor: PathBuf,
    pub store: Store,
}

#[derive(Debug, Clone)]
pub struct Agent {
    /// Serializes output publication/input reservations with reclamation planning.
    blob_gate: Arc<tokio::sync::Mutex<()>>,
    _watchers: Arc<Watchers>,
    allow: Arc<HashSet<EndpointId>>,
    submitting: Arc<tokio::sync::Mutex<()>>,
    hello: Arc<Hello>,
    config: AgentConfig,
    /// Output hashes by (path, size, modification time), so unchanged files are hashed once.
    hashed: Arc<Mutex<HashCache>>,
    events: Arc<EventLog>,
    /// Each job's numeric probes' first readings (for rates), shared by the watcher and suspend plans.
    origins: Origins,
    /// The sleep assertion held for `KeepAwake`.
    awake: Arc<Mutex<Option<std::process::Child>>>,
}

#[derive(Debug)]
struct Watchers(Vec<tokio::task::JoinHandle<()>>);
impl Drop for Watchers {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Unpublished inputs are outside `jobs/`, so a slow import cannot be mistaken for a lost supervisor.
struct Staging(PathBuf);
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

impl Agent {
    #[cfg(feature = "control-plane")]
    pub(crate) async fn blob_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.blob_gate.lock().await
    }

    #[cfg(feature = "control-plane")]
    pub(crate) async fn reserve_inputs(&self, hashes: &[Hash]) -> Result<Response> {
        let _guard = self.blob_gate.lock().await;
        anyhow::ensure!(hashes.len() <= 4096, "too many input reservations");
        for hash in hashes {
            // Reservations intentionally survive the gap between push and committed Stage, including
            // client crashes. This milestone does not expire original input caches.
            self.config
                .store
                .tags()
                .set(
                    format!("input/{hash}"),
                    iroh_blobs::HashAndFormat::raw(*hash),
                )
                .await?;
        }
        self.config.store.sync_db().await?;
        Ok(Response::Missing(Vec::new()))
    }

    /// Opens the event log (`$FLEET_HOME/events.jsonl`) and starts watching the jobs. Must run inside a Tokio runtime.
    pub fn start(
        allow: Arc<HashSet<EndpointId>>,
        hello: Hello,
        config: AgentConfig,
    ) -> Result<Self> {
        Self::start_inner(allow, hello, config, true)
    }

    /// Replicated workers authorize backend effects in their own durable reconciliation loop.
    #[cfg(feature = "control-plane")]
    pub(crate) fn start_managed(
        allow: Arc<HashSet<EndpointId>>,
        hello: Hello,
        config: AgentConfig,
    ) -> Result<Self> {
        Self::start_inner(allow, hello, config, false)
    }

    fn start_inner(
        allow: Arc<HashSet<EndpointId>>,
        hello: Hello,
        config: AgentConfig,
        recover_backends: bool,
    ) -> Result<Self> {
        fs::create_dir_all(&config.home)?;
        if config.home.join("instances.json").exists() || config.home.join("instances").exists() {
            // Reboot recovery must use the current worker capacity, not the
            // capacity the VM happened to have when its disk was created.
            instances::write(&config.home.join("instance-capacity.json"), &hello)?;
            if recover_backends
                && let Err(error) = instances::guest::recover(&config.home, &config.supervisor)
            {
                tracing::warn!(%error, "guest operation recovery needs attention");
            }
        }
        let events = Arc::new(EventLog::open(&config.home.join("events.jsonl"))?);
        events.append(EventKind::AgentStarted {
            version: hello.agent_version.clone(),
        })?;
        let origins: Origins = Arc::default();
        let submitting = Arc::new(tokio::sync::Mutex::new(()));
        let vm_watch = tokio::spawn(watch_instances(
            config.home.clone(),
            config.supervisor.clone(),
            events.clone(),
            submitting.clone(),
            recover_backends,
        ));
        let job_watch = tokio::spawn(watch(
            config.home.clone(),
            events.clone(),
            config.store.clone(),
            origins.clone(),
        ));
        Ok(Self {
            blob_gate: Arc::default(),
            _watchers: Arc::new(Watchers(vec![vm_watch, job_watch])),
            allow,
            submitting,
            hello: Arc::new(hello),
            config,
            hashed: Arc::default(),
            events,
            origins,
            awake: Arc::default(),
        })
    }

    async fn serve(
        &self,
        peer: EndpointId,
        mut send: SendStream,
        mut recv: RecvStream,
    ) -> Result<()> {
        let bytes = recv.read_to_end(MAX_MESSAGE).await?;
        let response = match serde_json::from_slice::<Request>(&bytes) {
            Ok(request) => self
                .handle(peer, request)
                .await
                .unwrap_or_else(|e| Response::Error(format!("{e:#}"))),
            Err(e) => crate::proto::unreadable(&bytes, &e),
        };
        send.write_all(&crate::proto::encode(&response)?).await?;
        send.finish()?;
        Ok(())
    }

    /// Reserve metadata before choosing the backend's desired state; this does not start a VM.
    pub(crate) async fn reserve_instance(
        &self,
        peer: EndpointId,
        id: &str,
        spec: instances::Spec,
    ) -> Result<instances::Info> {
        let _admission = self.submitting.lock().await;
        instances::create(&self.config.home, id, spec, &peer.to_string(), &self.hello)
    }

    pub(crate) async fn handle(&self, peer: EndpointId, request: Request) -> Result<Response> {
        let _blobs = if matches!(&request, Request::Outputs { .. }) {
            Some(self.blob_gate.lock().await)
        } else {
            None
        };
        let home = &self.config.home;
        Ok(match request {
            Request::CancelVmJob { job, spec } => {
                let admission = self.submitting.clone().lock_owned().await;
                let home = home.clone();
                let executable = self.config.supervisor.clone();
                tokio::task::spawn_blocking(move || {
                    let _admission = admission;
                    crate::vm_job::cancel(&home, &job, *spec, &peer.to_string(), &executable)
                })
                .await??;
                Response::Cancelled
            }
            Request::InstanceGuest(request) => {
                Response::InstanceGuest(self.guest(peer, request).await?)
            }
            Request::Instances => Response::Instances(instances::list(home)?),
            Request::InstanceCreate { id, spec } => {
                let instance = self.reserve_instance(peer, &id, spec).await?;
                instances::ensure_controller(home, &id, &self.config.supervisor)?;
                Response::Instance(instance)
            }
            Request::InstanceSet {
                id,
                desired,
                revision,
            } => {
                let _admission = self.submitting.lock().await;
                let instance = instances::set_desired(
                    home,
                    &id,
                    desired,
                    revision,
                    &peer.to_string(),
                    &self.hello,
                )?;
                instances::ensure_controller(home, &id, &self.config.supervisor)?;
                Response::Instance(instance)
            }
            Request::SubmitAllocated { spec, id, gpu } => Response::Submitted {
                job: self.submit(peer, *spec, id, Some(gpu)).await?,
            },
            Request::Hello { fleet, protocol } => {
                tracing::info!(%peer, %fleet, %protocol, "hello from a coordinator");
                Response::Hello((*self.hello).clone())
            }
            Request::Status => {
                let home = home.clone();
                Response::Status(
                    tokio::task::spawn_blocking(move || {
                        let mut status = system::status();
                        status.instances = instances::list(&home)?;
                        status.vm_capacity = instances::Config::load(&home).ok().map(Into::into);
                        let patterns: Vec<String> = fs::read_to_string(home.join("gpu-processes"))
                            .unwrap_or_default()
                            .lines()
                            .map(|l| l.split('#').next().unwrap_or("").trim().to_string())
                            .filter(|l| !l.is_empty())
                            .collect();
                        let list = jobs::list(&home).unwrap_or_default();
                        // Fleet's own jobs are not "outside" work: their process groups (the supervisor's and the
                        // job's own) and their folders are left out.
                        let own_groups: HashSet<u32> = list
                            .iter()
                            .filter(|j| !j.state.finished())
                            .filter_map(|j| {
                                jobs::read_json::<jobs::Started>(
                                    &jobs::jobs_dir(&home).join(&j.id).join("supervisor.json"),
                                )
                                .ok()
                                .flatten()
                            })
                            .flat_map(|s| [s.supervisor_pid, s.child_pid])
                            .collect();
                        status.gpu_busy_external =
                            system::running_process(&patterns, &own_groups, &jobs::jobs_dir(&home));
                        status.running_jobs = list
                            .into_iter()
                            .filter(|j| j.state == State::Running)
                            .map(|j| j.id)
                            .collect();
                        Ok::<_, anyhow::Error>(status)
                    })
                    .await??,
                )
            }
            Request::Missing { hashes } => Response::Missing(self.missing(&hashes).await?),
            Request::Submit { spec, id } => Response::Submitted {
                job: self.submit(peer, *spec, id, None).await?,
            },
            Request::Events { since, wait_ms } => {
                let wait = Duration::from_millis(u64::from(wait_ms.min(25_000)));
                let events = self.events.wait_since(since, wait).await;
                Response::Events {
                    first: self.events.first(),
                    events,
                    last: self.events.last(),
                }
            }
            Request::KeepAwake { seconds } => {
                let mut held = self.awake.lock().expect("awake");
                if let Some(mut previous) = held.take() {
                    let _ = previous.kill();
                    let _ = previous.wait();
                }
                *held = Some(
                    Command::new("/usr/bin/caffeinate")
                        .args(["-i", "-t", &seconds.min(3600).to_string()])
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .spawn()?,
                );
                Response::Ok
            }
            Request::Jobs => Response::Jobs(jobs::list(home)?),
            Request::Logs {
                job,
                stderr,
                from,
                max,
            } => {
                let dir = jobs::dir(home, &job)?;
                let finished = jobs::info(&dir)?.state.finished();
                let stream = crate::history::Stream::log(stderr);
                if crate::history::root(&dir, stream)
                    .join("state.json")
                    .exists()
                {
                    let r =
                        crate::history::read(&dir, stream, from, max.min(MAX_LOG_CHUNK) as usize)?;
                    let (data, next) = log_text(
                        &r.data,
                        r.next,
                        if r.data.len() < (max.min(MAX_LOG_CHUNK) as usize) {
                            r.next
                        } else {
                            r.observed
                        },
                    );
                    return Ok(Response::Logs {
                        data,
                        next,
                        done: finished && next >= r.observed,
                        gap: r.gap,
                    });
                }
                let path = dir.join(if stderr { "stderr.log" } else { "stdout.log" });
                let (bytes, next, len) = read_from(&path, from, max.min(MAX_LOG_CHUNK))?;
                let (data, next) = log_text(&bytes, next, len);
                Response::Logs {
                    data,
                    next,
                    done: finished && next >= len,
                    gap: None,
                }
            }
            Request::Cancel { job } => {
                let dir = jobs::dir(home, &job)?;
                if jobs::info(&dir)?.state.finished() {
                    bail!("job {job} has already finished");
                }
                instances::write(&dir.join("cancel"), &now())?;
                Response::Cancelled
            }
            Request::Pause { job } => {
                let dir = jobs::dir(home, &job)?;
                let info = jobs::info(&dir)?;
                if info.vm.is_some() {
                    bail!("VM jobs do not support pause/continue; use cancellation");
                }
                if info.stopping.is_some()
                    || dir.join("cancel").exists()
                    || dir.join("suspend").exists()
                {
                    bail!("job {job} is stopping");
                }
                if info.state != State::Running {
                    bail!("job {job} is {:?}, not running", info.state);
                }
                let started: jobs::Started =
                    jobs::read_json(&dir.join("supervisor.json"))?.context("not started")?;
                fs::write(dir.join("paused"), now().to_string())?;
                supervise::signal_group(started.child_pid, libc::SIGSTOP);
                Response::Ok
            }
            Request::Continue { job } => {
                let _admission = self.submitting.lock().await;
                let dir = jobs::dir(home, &job)?;
                if jobs::info(&dir)?.state != State::Paused {
                    bail!("job {job} is not paused");
                }
                let started: jobs::Started =
                    jobs::read_json(&dir.join("supervisor.json"))?.context("not started")?;
                let record: Job =
                    jobs::read_json(&dir.join("job.json"))?.context("no job metadata")?;
                let gpu = jobs::read_json::<bool>(&dir.join("gpu-allocation.json"))?;
                instances::admit_job(home, record.spec.threads, gpu, Some(&job), &self.hello)?;
                fs::remove_file(dir.join("paused"))?;
                supervise::signal_group(started.child_pid, libc::SIGCONT);
                Response::Ok
            }
            Request::Outputs { job } => Response::Outputs(self.outputs(&job).await?),
            Request::SuspendPlan { job } => {
                let dir = jobs::dir(home, &job)?;
                let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
                let resume = record.spec.resume.as_ref().context("this job has no resume declaration, so it cannot be suspended (only paused or cancelled)")?;
                let mut job_origins = self
                    .origins
                    .lock()
                    .expect("origins")
                    .get(&job)
                    .cloned()
                    .unwrap_or_default();
                let probes = crate::probes::evaluate(
                    &record.spec.probes,
                    &dir,
                    &mut job_origins,
                    now() as f64,
                );
                let settle = Duration::from_secs(record.spec.settle_seconds);
                Response::Plan(crate::suspend::plan(
                    &dir.join("work"),
                    resume,
                    settle,
                    &probes,
                    now(),
                ))
            }
            Request::Suspend {
                job,
                now: at_once,
                within_seconds,
            } => {
                let dir = jobs::dir(home, &job)?;
                let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
                let resume = record.spec.resume.as_ref().context("this job has no resume declaration, so it cannot be suspended (only paused or cancelled)")?;
                let state = jobs::info(&dir)?.state;
                if state != State::Running && state != State::Paused {
                    bail!("job {job} is {state:?}, not running");
                }
                if at_once {
                    fs::write(dir.join("suspend"), now().to_string())?;
                } else {
                    let settle = Duration::from_secs(record.spec.settle_seconds);
                    let baseline =
                        crate::suspend::checkpoints(&dir.join("work"), resume, settle, false)
                            .pop()
                            .map(|c| c.path);
                    jobs::write_json(
                        &dir.join("suspend-when-safe.json"),
                        &WhenSafe {
                            baseline,
                            deadline: within_seconds.map(|s| now() + s),
                        },
                    )?;
                }
                Response::Ok
            }
            Request::Checkpoint { job } => {
                let dir = jobs::dir(home, &job)?;
                Response::Checkpoint(
                    jobs::read_json(&dir.join("suspended.json"))?
                        .context("no checkpoint recorded (yet) for this job")?,
                )
            }
        })
    }

    async fn guest(
        &self,
        peer: EndpointId,
        request: instances::guest::Request,
    ) -> Result<instances::guest::Response> {
        use instances::guest::{self, Request as G, Response as R};
        let home = self.config.home.clone();
        let id = match &request {
            G::Ready { id }
            | G::Start { id, .. }
            | G::Get { id, .. }
            | G::List { id }
            | G::Logs { id, .. }
            | G::Output { id, .. } => Some(id),
            G::Storage => None,
        };
        if let Some(id) = id {
            guest::owned(&home, id, &peer.to_string())?;
        }
        Ok(match request {
            G::Ready { id } => {
                R::Ready(tokio::task::spawn_blocking(move || guest::readiness(&home, &id)).await??)
            }
            G::Get { id, operation } => R::Operation(guest::inspect(&home, &id, &operation)?),
            G::List { id } => R::Operations(
                guest::list(&home, &id)?
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            ),
            G::Logs {
                id,
                operation,
                stderr,
                from,
            } => R::Logs(guest::logs(&home, &id, &operation, stderr, from)?),
            G::Storage => R::Storage(
                tokio::task::spawn_blocking(move || instances::storage::report(&home)).await??,
            ),
            G::Output { id, operation } => {
                let path = guest::output(&home, &id, &operation)?;
                let size = fs::metadata(&path)?.len();
                let hash = self.config.store.blobs().add_path(path).await?.hash;
                R::Output { hash, size }
            }
            G::Start {
                id,
                operation,
                revision,
                spec,
            } => {
                let admission = self.submitting.clone().lock_owned().await;
                spec.validate()?;
                let dir = guest::operation_dir(&home, &id, &operation)?;
                let mut staging = None;
                if !dir.join("operation.json").exists()
                    && let guest::Spec::Upload { hash, size, .. } = &spec
                {
                    guest::admit(&home, &id, revision)?;
                    if !matches!(self.config.store.blobs().status(*hash).await?,
                        iroh_blobs::api::blobs::BlobStatus::Complete { size: stored } if stored == *size)
                    {
                        bail!("upload blob has not arrived or has a different size");
                    }
                    let stage = Staging(
                        home.join("staging")
                            .join(format!("guest-{:016x}", rand::random::<u64>())),
                    );
                    fs::create_dir_all(&stage.0)?;
                    fs::set_permissions(&stage.0, fs::Permissions::from_mode(0o700))?;
                    let input = stage.0.join("input");
                    self.config.store.blobs().export(*hash, &input).await?;
                    fs::set_permissions(&input, fs::Permissions::from_mode(0o600))?;
                    let hash = *hash;
                    let size = *size;
                    tokio::task::spawn_blocking(move || guest::verify_input(&input, hash, size))
                        .await??;
                    staging = Some(stage);
                }
                let executable = self.config.supervisor.clone();
                R::Operation(
                    tokio::task::spawn_blocking(move || {
                        // Keep admission locked even if the request future is cancelled
                        // while this blocking task is persisting/launching the operation.
                        let _admission = admission;
                        if let Some(stage) = &staging {
                            fs::create_dir_all(&dir)?;
                            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
                            fs::rename(stage.0.join("input"), dir.join("input"))?;
                            fs::File::open(dir.join("input"))?.sync_all()?;
                        }
                        let result =
                            guest::start(&home, &id, &operation, revision, spec, &executable);
                        if result.is_err() && !dir.join("operation.json").exists() {
                            let _ = fs::remove_file(dir.join("input"));
                        }
                        result
                    })
                    .await??,
                )
            }
        })
    }

    async fn missing(&self, hashes: &[Hash]) -> Result<Vec<Hash>> {
        let mut missing = Vec::new();
        for &hash in hashes {
            if !self.config.store.blobs().has(hash).await? {
                missing.push(hash);
            }
        }
        Ok(missing)
    }

    async fn submit(
        &self,
        peer: EndpointId,
        spec: JobSpec,
        id: Option<String>,
        gpu: Option<bool>,
    ) -> Result<String> {
        let submission = self.submitting.clone().lock_owned().await;
        spec.validate()?;
        let id = match id {
            Some(id) => {
                let rel = jobs::safe_relative(&id)?;
                if rel.components().count() != 1 {
                    bail!("not a job ID: {id}");
                }
                id
            }
            None => jobs::new_id(&spec.name),
        };
        let dir = jobs::jobs_dir(&self.config.home).join(&id);
        if dir.exists() {
            if let Some(existing) = jobs::read_json::<Job>(&dir.join("job.json"))?
                && existing.spec == spec
                && existing.submitter == peer.to_string()
                && jobs::read_json::<bool>(&dir.join("gpu-allocation.json"))?
                    .is_none_or(|saved| Some(saved) == gpu)
            {
                return Ok(id);
            }
            bail!("job {id} already exists on this agent with different or incomplete metadata");
        }
        let missing = self
            .missing(&spec.inputs.values().map(|i| i.hash).collect::<Vec<_>>())
            .await?;
        if !missing.is_empty() {
            bail!(
                "{} input(s) are not in the agent's store yet; push them first",
                missing.len()
            );
        }
        if let Some(vm) = &spec.vm {
            instances::admit_vm(&self.config.home, vm, &self.hello)?;
        } else {
            instances::admit_job(&self.config.home, spec.threads, gpu, None, &self.hello)?;
        }
        let staging = Staging(
            self.config
                .home
                .join("staging")
                .join(format!("{id}-{:016x}", rand::random::<u64>())),
        );
        let work = staging.0.join(if spec.vm.is_some() {
            "vm-inputs"
        } else {
            "work"
        });
        fs::create_dir_all(&work)?;
        instances::write(
            &staging.0.join("job.json"),
            &Job {
                id: id.clone(),
                spec: spec.clone(),
                submitter: peer.to_string(),
                submitted_at: now(),
            },
        )?;
        if let Some(gpu) = gpu {
            instances::write(&staging.0.join("gpu-allocation.json"), &gpu)?;
        }
        let mut input_bytes = 0u64;
        for (path, input) in &spec.inputs {
            if spec.vm.is_some() {
                let iroh_blobs::api::blobs::BlobStatus::Complete { size } =
                    self.config.store.blobs().status(input.hash).await?
                else {
                    bail!("VM input is incomplete");
                };
                input_bytes = input_bytes
                    .checked_add(size)
                    .context("input size overflow")?;
                if size > instances::guest::MAX_FILE || input_bytes > crate::vm_job::MAX_TOTAL {
                    bail!("VM input limit: 1 GiB/file, 4 GiB total");
                }
                instances::storage::guard(&self.config.home)?;
            }
            let target = work.join(jobs::safe_relative(path)?);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            self.config
                .store
                .blobs()
                .export(input.hash, &target)
                .await
                .with_context(|| format!("placing input {path}"))?;
            anyhow::ensure!(
                crate::blobs::hash_file(&target)? == input.hash,
                "staged input hash mismatch: {path}"
            );
            if input.executable {
                supervise::make_executable(&target)?;
            }
        }
        let config = self.config.clone();
        let hello = self.hello.clone();
        let accepted_id = id.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            // Cancellation of a request cannot release admission during publication.
            let _submission = submission;
            if let Some(vm) = spec.vm {
                instances::reserve_job(&config.home, &accepted_id, vm, &peer.to_string(), &hello)?;
                fs::create_dir_all(staging.0.join("work"))?;
            }
            fs::create_dir_all(jobs::jobs_dir(&config.home))?;
            fs::rename(&staging.0, &dir)?;
            fs::File::open(jobs::jobs_dir(&config.home))?.sync_all()?;
            crate::vm_job::launch(&dir, &config.supervisor)?;
            Ok(())
        })
        .await??;
        tracing::info!(%peer, job = %id, "job submitted");
        Ok(id)
    }

    async fn outputs(&self, job: &str) -> Result<Vec<OutputFile>> {
        let dir = jobs::dir(&self.config.home, job)?;
        let info = jobs::info(&dir)?;
        let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
        let work = dir.join("work");
        let settle = if record.spec.vm.is_some() {
            Duration::ZERO
        } else {
            Duration::from_secs(record.spec.settle_seconds)
        };
        let files =
            jobs::published_files(&work, &record.spec.publish, settle, info.state.finished())?;
        let mut out = Vec::with_capacity(files.len());
        for (path, size) in files {
            let abs = work.join(&path);
            let modified = fs::metadata(&abs)?.modified()?;
            let key = (abs.clone(), size, modified);
            let cached = self.hashed.lock().expect("hash cache").get(&key).copied();
            let hash = match cached {
                Some(hash) => hash,
                None => {
                    let source = abs.clone();
                    let hash =
                        tokio::task::spawn_blocking(move || crate::blobs::hash_file(&source))
                            .await??;
                    crate::blobs::import_named(
                        &self.config.store,
                        &abs,
                        hash,
                        format!("output/{job}/{path}"),
                    )
                    .await?;
                    self.hashed.lock().expect("hash cache").insert(key, hash);
                    hash
                }
            };
            out.push(OutputFile { path, size, hash });
        }
        Ok(out)
    }
}

/// What a pending "suspend when safe" waits for: a checkpoint newer than `baseline`, until `deadline`.
#[derive(serde::Serialize, serde::Deserialize)]
struct WhenSafe {
    baseline: Option<String>,
    deadline: Option<u64>,
}

/// Acts on a pending safe suspension: stops the job once a newer checkpoint has settled, or gives up at the deadline.
fn when_safe(home: &std::path::Path, id: &str, events: &EventLog) -> Result<()> {
    let dir = jobs::dir(home, id)?;
    let Some(pending): Option<WhenSafe> = jobs::read_json(&dir.join("suspend-when-safe.json"))?
    else {
        return Ok(());
    };
    let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
    let Some(resume) = &record.spec.resume else {
        return Ok(());
    };
    let settle = Duration::from_secs(record.spec.settle_seconds);
    let latest = crate::suspend::checkpoints(&dir.join("work"), resume, settle, false)
        .pop()
        .map(|c| c.path);
    if latest.is_some() && latest != pending.baseline {
        fs::write(dir.join("suspend"), now().to_string())?;
        fs::remove_file(dir.join("suspend-when-safe.json"))?;
    } else if pending.deadline.is_some_and(|d| now() >= d) {
        fs::remove_file(dir.join("suspend-when-safe.json"))?;
        events.append(EventKind::SuspendGaveUp {
            job: id.to_string(),
        })?;
    }
    Ok(())
}

/// Adds a suspended job's latest checkpoint (and carried files) to the store, and writes `suspended.json`.
async fn record_checkpoint(home: &std::path::Path, id: &str, store: &Store) -> Result<()> {
    let dir = jobs::dir(home, id)?;
    if dir.join("suspended.json").exists() {
        return Ok(());
    }
    let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
    let resume = record
        .spec
        .resume
        .context("the job has no resume declaration")?;
    let work = dir.join("work");
    // The job has stopped, so every checkpoint is as settled as it will get.
    let checkpoint = crate::suspend::checkpoints(&work, &resume, Duration::ZERO, true).pop();
    let mut files = Vec::new();
    if let Some(cp) = &checkpoint {
        for rel in crate::suspend::files_to_carry(&work, &cp.path, &resume.carry) {
            let abs = work.join(&rel);
            let size = fs::metadata(&abs)?.len();
            let hash = store.blobs().add_path(&abs).await?.hash;
            files.push(OutputFile {
                path: rel.to_string_lossy().into_owned(),
                size,
                hash,
            });
        }
    }
    jobs::write_json(
        &dir.join("suspended.json"),
        &crate::suspend::Recorded { checkpoint, files },
    )
}

async fn watch_instances(
    home: PathBuf,
    executable: PathBuf,
    events: Arc<EventLog>,
    submitting: Arc<tokio::sync::Mutex<()>>,
    recover_backends: bool,
) {
    let mut last = HashMap::new();
    loop {
        let home2 = home.clone();
        let executable2 = executable.clone();
        let admission = submitting.clone().lock_owned().await;
        let result = tokio::task::spawn_blocking(move || -> Result<Vec<instances::Info>> {
            let _admission = admission;
            if recover_backends {
                crate::vm_job::clean_staging(&home2)?;
            }
            let list = instances::list(&home2)?;
            if !recover_backends {
                return Ok(list);
            }
            for instance in &list {
                if let Some(job) = &instance.record.job {
                    let dir = jobs::jobs_dir(&home2).join(job);
                    if dir.join("job.json").exists() {
                        crate::vm_job::ensure(&dir, &executable2)?;
                    } else if instance.observed.state != instances::State::Deleted {
                        // Crash between VM reservation and job publication: never run orphan work.
                        let desired = if instance.record.desired == instances::Desired::Stopped
                            && instance.observed.state == instances::State::Stopped
                            && instance.observed.revision == instance.record.revision
                        {
                            instances::Desired::Deleted
                        } else {
                            instances::Desired::Stopped
                        };
                        instances::release_job(&home2, job, desired)?;
                    }
                }
                instances::ensure_controller(&home2, &instance.record.id, &executable2)?;
            }
            Ok(list)
        })
        .await;
        match result {
            Ok(Ok(list)) => {
                for instance in list {
                    if last.get(&instance.record.id) != Some(&instance)
                        && events
                            .append(EventKind::InstanceState {
                                instance: instance.clone(),
                            })
                            .is_ok()
                    {
                        last.insert(instance.record.id.clone(), instance);
                    }
                }
            }
            other => tracing::warn!("watching VM instances: {other:?}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Logs each job's state changes, and changes to the set of its settled outputs, as events.
async fn watch(home: PathBuf, events: Arc<EventLog>, store: Store, origins: Origins) {
    let mut states = events.job_states();
    let mut outputs: HashMap<String, (u32, u64, u64)> = HashMap::new();
    let mut settled: HashSet<String> = HashSet::new();
    let mut stopping = HashSet::new();
    let mut vm_statuses = HashMap::new();
    // Progress probes: each job's last reported values and when, and each numeric probe's first reading.
    let mut reported: HashMap<String, (Vec<crate::probes::ProbeValue>, u64)> = HashMap::new();
    let mut tick: u64 = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        tick += 1;
        let home2 = home.clone();
        let Ok(Ok(list)) = tokio::task::spawn_blocking(move || jobs::list(&home2)).await else {
            continue;
        };
        for job in &list {
            if let Some(status) = &job.vm
                && vm_statuses.get(&job.id) != Some(status)
                && events
                    .append(EventKind::VmJob {
                        job: job.id.clone(),
                        status: status.clone(),
                    })
                    .is_ok()
            {
                vm_statuses.insert(job.id.clone(), status.clone());
            }
            if let Some(status) = &job.stopping
                && !stopping.contains(&job.id)
                && events
                    .append(EventKind::Stopping {
                        job: job.id.clone(),
                        stopping: status.clone(),
                    })
                    .is_ok()
            {
                stopping.insert(job.id.clone());
            }
            if job.state == State::Suspended
                && job.vm.is_none()
                && states.get(&job.id) != Some(&job.state)
            {
                // A continuation needs the checkpoint by hash: record it before the state is reported.
                if let Err(e) = record_checkpoint(&home, &job.id, &store).await {
                    tracing::warn!(job = %job.id, "recording the checkpoint failed: {e:#}");
                    continue;
                }
            }
            if (job.state == State::Running || job.state == State::Paused)
                && let Err(e) = when_safe(&home, &job.id, &events)
            {
                tracing::warn!(job = %job.id, "checking for a safe suspension failed: {e:#}");
            }
            if states.get(&job.id) != Some(&job.state)
                && events
                    .append(EventKind::JobState {
                        job: job.id.clone(),
                        state: job.state,
                    })
                    .is_ok()
            {
                states.insert(job.id.clone(), job.state);
            }
        }
        if !tick.is_multiple_of(5) {
            continue;
        }
        for job in list {
            let finished = job.state.finished();
            if finished && settled.contains(&job.id) {
                continue;
            }
            let mut history_complete = true;
            let dir = jobs::jobs_dir(&home).join(&job.id);
            let metrics_closed = crate::history::state(&dir, crate::history::Stream::Metrics)
                .is_ok_and(|s| s.closed);
            if let Ok(Some(record)) = jobs::read_json::<Job>(&dir.join("job.json"))
                && record.spec.vm.is_none()
                && !metrics_closed
                && !record.spec.metric_files.is_empty()
            {
                match crate::history::jsonl::capture(&dir, &record.spec.metric_files, finished) {
                    Ok(complete) => history_complete = complete,
                    Err(error) => {
                        let _ = crate::history::record_loss(
                            &dir,
                            crate::history::Stream::Metrics,
                            0,
                            &format!("JSONL capture: {error}"),
                        );
                    }
                }
            }
            let home2 = home.clone();
            let id = job.id.clone();
            let scan = tokio::task::spawn_blocking(move || -> Result<(u32, u64, u64)> {
                let dir = jobs::dir(&home2, &id)?;
                let record: Job = jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
                let settle = if record.spec.vm.is_some() {
                    Duration::ZERO
                } else {
                    Duration::from_secs(record.spec.settle_seconds)
                };
                let files = jobs::published_files(
                    &dir.join("work"),
                    &record.spec.publish,
                    settle,
                    finished,
                )?;
                Ok((
                    files.len() as u32,
                    files.iter().map(|(_, size)| size).sum(),
                    crate::vm_job::output_revision(&dir)?,
                ))
            })
            .await;
            if let Ok(Ok(Some(values))) = {
                let home2 = home.clone();
                let id = job.id.clone();
                let mut job_origins = origins
                    .lock()
                    .expect("origins")
                    .remove(&job.id)
                    .unwrap_or_default();
                let result = tokio::task::spawn_blocking(
                    move || -> Result<(Option<Vec<crate::probes::ProbeValue>>, _)> {
                        let dir = jobs::dir(&home2, &id)?;
                        let record: Job =
                            jobs::read_json(&dir.join("job.json"))?.context("no job.json")?;
                        if record.spec.probes.is_empty() {
                            return Ok((None, job_origins));
                        }
                        let values = if record.spec.vm.is_some() {
                            jobs::read_json::<crate::vm_job::Progress>(
                                &dir.join("vm-progress.json"),
                            )?
                            .map_or_else(Vec::new, |p| p.values)
                        } else {
                            crate::probes::evaluate(
                                &record.spec.probes,
                                &dir,
                                &mut job_origins,
                                now() as f64,
                            )
                        };
                        Ok((Some(values), job_origins))
                    },
                )
                .await;
                result.map(|r| {
                    r.map(|(values, job_origins)| {
                        origins
                            .lock()
                            .expect("origins")
                            .insert(job.id.clone(), job_origins);
                        values
                    })
                })
            } {
                let due = reported.get(&job.id).is_none_or(|(last, at)| {
                    crate::probes::changed(last, &values)
                        && (finished || now().saturating_sub(*at) >= 15)
                });
                if due && !values.is_empty() {
                    let dir = jobs::jobs_dir(&home).join(&job.id);
                    let captured = (|| -> Result<()> {
                        if metrics_closed {
                            return Ok(());
                        }
                        let mut bytes = serde_json::to_vec(
                            &serde_json::json!({"at":now(),"kind":"probes","values":values}),
                        )?;
                        bytes.push(b'\n');
                        crate::history::append(&dir, crate::history::Stream::Metrics, &bytes)
                    })();
                    if let Err(error) = captured {
                        tracing::warn!(job=%job.id,%error,"metric capture unavailable");
                    }
                    let kind = EventKind::Progress {
                        job: job.id.clone(),
                        probes: values.clone(),
                    };
                    if events.append(kind).is_ok() {
                        reported.insert(job.id.clone(), (values, now()));
                    }
                }
            }
            if let Ok(Ok(signature)) = scan {
                if signature.0 > 0 && outputs.get(&job.id) != Some(&signature) {
                    let kind = EventKind::Outputs {
                        job: job.id.clone(),
                        files: signature.0,
                        bytes: signature.1,
                    };
                    if events.append(kind).is_ok() {
                        outputs.insert(job.id.clone(), signature);
                    }
                }
                if finished && history_complete {
                    if let Err(error) = crate::history::close(
                        &jobs::jobs_dir(&home).join(&job.id),
                        crate::history::Stream::Metrics,
                    ) {
                        tracing::warn!(job=%job.id,%error,"metric history close unavailable");
                    }
                    settled.insert(job.id);
                }
            }
        }
    }
}

/// A log chunk as text: cut before a character that the chunk's end splits (it comes whole with the next chunk), so
/// returns the text and the offset to continue from. Bytes that are not text at all are replaced.
pub(crate) fn log_text(bytes: &[u8], next: u64, len: u64) -> (String, u64) {
    let keep = match std::str::from_utf8(bytes) {
        Err(e) if e.error_len().is_none() && next < len => e.valid_up_to(),
        _ => bytes.len(),
    };
    (
        String::from_utf8_lossy(&bytes[..keep]).into_owned(),
        next - (bytes.len() - keep) as u64,
    )
}

/// Reads up to `max` bytes of a file from `from`; returns the bytes, the next offset and the file's length.
fn read_from(path: &std::path::Path, from: u64, max: u32) -> Result<(Vec<u8>, u64, u64)> {
    let mut file = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), from, 0)),
        Err(e) => return Err(e.into()),
    };
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(from.min(len)))?;
    let mut data = Vec::new();
    file.take(u64::from(max)).read_to_end(&mut data)?;
    let next = from.min(len) + data.len() as u64;
    Ok((data, next, len))
}

impl ProtocolHandler for Agent {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        if !self.allow.contains(&peer) {
            tracing::warn!(%peer, "refused a connection from a key that is not on the allowlist");
            connection.close(REFUSED.into(), b"not on this agent's allowlist");
            return Ok(());
        }
        tracing::info!(%peer, "coordinator connected");
        while let Ok((send, recv)) = connection.accept_bi().await {
            let agent = self.clone();
            tokio::spawn(async move {
                if let Err(e) = agent.serve(peer, send, recv).await {
                    tracing::warn!(%peer, "request failed: {e:#}");
                }
            });
        }
        tracing::info!(%peer, "coordinator disconnected");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::log_text;

    #[test]
    fn a_character_split_across_chunks_comes_whole() {
        let text = "ab€cd".as_bytes(); // € is three bytes, at offsets 2..5
        let (first, next) = log_text(&text[..4], 4, text.len() as u64);
        assert_eq!((first.as_str(), next), ("ab", 2));
        let (second, next) = log_text(&text[next as usize..], text.len() as u64, text.len() as u64);
        assert_eq!((second.as_str(), next), ("€cd", 7));
    }
}
