//! Desktop client of the replicated API; closing this window never cancels work.
use crate::control::{
    client::{self, ApiRequest, AppClient},
    runtime::DataRequest,
};
use iced::{
    Alignment, Color, Length, Subscription, Task,
    theme::Mode,
    widget::{canvas, column, container, row, scrollable, text},
};
use iced_m3::{
    ButtonSize, ButtonVariant, Element, Segment, SegmentSelection, SurfaceVariant, Theme,
    TypeScale, button, checkbox, linear_progress, segmented_buttons, surface, text_field,
    typography,
};
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

pub fn run(home: PathBuf) -> iced::Result {
    iced::application(
        move || {
            (
                App::new(home.clone()),
                Task::batch([
                    Task::done(Message::Tick),
                    iced::system::theme().map(Message::Appearance),
                ]),
            )
        },
        App::update,
        App::view,
    )
    .title("Fleet")
    .theme(App::theme)
    .default_font(iced_m3::fonts::REGULAR)
    .subscription(App::subscription)
    .window_size((1220.0, 820.0))
    .run()
}
struct App {
    dark: bool,
    acting: bool,
    home: PathBuf,
    jobs: Vec<Value>,
    workers: Vec<Value>,
    instances: Vec<Value>,
    storage: Value,
    error: Option<String>,
    busy: bool,
    selected: Option<String>,
    log: String,
    stderr: bool,
    log_next: u64,
    metrics: BTreeMap<String, Vec<crate::probes::ProbeValue>>,
    cursors: BTreeMap<String, u64>,
    job_file: String,
    worker: String,
    destination: String,
    notice: Option<String>,
    pending: Option<ApiRequest>,
    gpu: bool,
    memory: String,
    requires: String,
}
#[derive(Debug, Clone)]
enum Message {
    Appearance(Mode),
    DismissNotice,
    Tick,
    Refreshed(Result<Refresh, String>),
    Select(String),
    Stderr(bool),
    JobFile(String),
    Worker(String),
    Destination(String),
    Gpu(bool),
    Memory(String),
    Requires(String),
    Submit,
    Cancel(String, u64),
    Suspend(String, u64, String),
    Resume(String, u64),
    Instance(String, u64, crate::control::workload::InstanceDesired),
    Fetch,
    Retry,
    Acted(Result<Value, String>),
}
#[derive(Debug, Clone)]
struct Refresh {
    jobs: Vec<Value>,
    workers: Vec<Value>,
    instances: Vec<Value>,
    storage: Value,
    selected: Option<String>,
    stderr: bool,
    log: Option<Value>,
    events: Vec<(String, Value)>,
}
fn id() -> String {
    format!("gui-{:032x}", rand::random::<u128>())
}
impl App {
    fn new(home: PathBuf) -> Self {
        Self {
            dark: false,
            acting: false,
            home,
            jobs: vec![],
            workers: vec![],
            instances: vec![],
            storage: Value::Null,
            error: None,
            busy: false,
            selected: None,
            log: String::new(),
            stderr: false,
            log_next: 0,
            metrics: BTreeMap::new(),
            cursors: BTreeMap::new(),
            job_file: String::new(),
            worker: String::new(),
            destination: String::new(),
            notice: None,
            pending: None,
            gpu: false,
            memory: "1024".into(),
            requires: String::new(),
        }
    }
    fn theme(&self) -> Theme {
        Theme::from_accent(Color::from_rgb(0.29, 0.44, 0.85), self.dark).expressive()
    }
    fn subscription(&self) -> Subscription<Message> {
        Subscription::batch([
            iced::time::every(Duration::from_secs(3)).map(|_| Message::Tick),
            iced::system::theme_changes().map(Message::Appearance),
        ])
    }
    fn act(&mut self, request: ApiRequest) -> Task<Message> {
        if self.acting {
            return Task::none();
        }
        self.acting = true;
        self.pending = Some(request.clone());
        let home = self.home.clone();
        Task::perform(
            async move {
                client::call(&home, request)
                    .await
                    .map_err(|e| format!("{e:#}"))
            },
            Message::Acted,
        )
    }
    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Appearance(mode) => self.dark = mode == Mode::Dark,
            Message::DismissNotice => self.notice = None,
            Message::Tick if !self.busy => {
                self.busy = true;
                let home = self.home.clone();
                let selected = self.selected.clone();
                let from = self.log_next;
                let stderr = self.stderr;
                let cursors = self.cursors.clone();
                return Task::perform(
                    async move {
                        let mut c = AppClient::connect(&home).await.map_err(|e| e.to_string())?;
                        let result = async {
                            let jobs = c
                                .execute(ApiRequest::Queue)
                                .await?
                                .as_array()
                                .cloned()
                                .unwrap_or_default();
                            let workers = c
                                .execute(ApiRequest::Workers)
                                .await?
                                .as_array()
                                .cloned()
                                .unwrap_or_default();
                            let instances = c
                                .execute(ApiRequest::Instances)
                                .await?
                                .as_array()
                                .cloned()
                                .unwrap_or_default();
                            let storage = c.execute(ApiRequest::Storage).await?;
                            // Pin the log read to the attempt in this queue snapshot. A resume can
                            // commit between these requests; never mix its log with the old offset.
                            let attempt = jobs
                                .iter()
                                .find(|j| j["id"].as_str() == selected.as_deref())
                                .and_then(|j| j.pointer("/assignment/attempt/number"))
                                .and_then(Value::as_u64)
                                .and_then(|n| u32::try_from(n).ok());
                            let log = if let (Some(id), Some(attempt)) = (&selected, attempt) {
                                c.execute(ApiRequest::Logs {
                                    attempt: Some(attempt),
                                    id: id.clone(),
                                    stderr,
                                    from,
                                    max: 65536,
                                })
                                .await
                                .ok()
                            } else {
                                None
                            };
                            let mut events = Vec::new();
                            for w in &workers {
                                let Some(worker) =
                                    w.pointer("/registration/id").and_then(Value::as_str)
                                else {
                                    continue;
                                };
                                if w["status"].is_null() {
                                    continue;
                                }
                                let since = cursors.get(worker).copied().unwrap_or(0);
                                if let Ok(v) = c
                                    .execute(ApiRequest::Data {
                                        worker: worker.into(),
                                        request: DataRequest::Events { since, wait_ms: 0 },
                                    })
                                    .await
                                {
                                    events.push((worker.into(), v));
                                }
                            }
                            Ok::<_, anyhow::Error>(Refresh {
                                jobs,
                                workers,
                                instances,
                                storage,
                                selected,
                                stderr,
                                log,
                                events,
                            })
                        }
                        .await;
                        c.close().await;
                        result.map_err(|e| format!("{e:#}"))
                    },
                    Message::Refreshed,
                );
            }
            Message::Refreshed(result) => {
                self.busy = false;
                match result {
                    Err(e) => self.error = Some(e),
                    Ok(r) => {
                        self.error = None;
                        let selected_execution = |jobs: &[Value]| {
                            jobs.iter()
                                .find(|j| j["id"].as_str() == self.selected.as_deref())
                                .and_then(|j| j.pointer("/assignment/execution"))
                                .cloned()
                        };
                        let attempt_changed =
                            selected_execution(&self.jobs) != selected_execution(&r.jobs);
                        if attempt_changed {
                            self.log.clear();
                            self.log_next = 0;
                        }
                        self.jobs = r.jobs;
                        self.workers = r.workers;
                        self.instances = r.instances;
                        self.storage = r.storage;
                        if !attempt_changed
                            && r.selected == self.selected
                            && r.stderr == self.stderr
                            && let Some(l) = r.log.and_then(|v| v.get("Logs").cloned())
                        {
                            self.log.push_str(l["data"].as_str().unwrap_or(""));
                            self.log_next = l["next"].as_u64().unwrap_or(self.log_next);
                            if self.log.len() > 200000 {
                                let mut n = self.log.len() - 200000;
                                while !self.log.is_char_boundary(n) {
                                    n += 1;
                                }
                                self.log.drain(..n);
                            }
                        }
                        for (worker, v) in r.events {
                            if let Some(events) =
                                v.pointer("/Events/events").and_then(Value::as_array)
                            {
                                for e in events {
                                    if let Some(n) = e["seq"].as_u64() {
                                        self.cursors.insert(worker.clone(), n);
                                    }
                                    if let Some(p) = e.pointer("/kind/Progress")
                                        && let (Some(job), Ok(probes)) = (
                                            p["job"].as_str(),
                                            serde_json::from_value(p["probes"].clone()),
                                        )
                                    {
                                        self.metrics.insert(job.into(), probes);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Message::Select(id) => {
                self.selected = Some(id);
                self.log.clear();
                self.log_next = 0;
            }
            Message::Stderr(v) => {
                self.stderr = v;
                self.log.clear();
                self.log_next = 0;
            }
            Message::Gpu(v) => self.gpu = v,
            Message::Memory(v) => self.memory = v,
            Message::Requires(v) => self.requires = v,
            Message::JobFile(v) => self.job_file = v,
            Message::Worker(v) => self.worker = v,
            Message::Destination(v) => self.destination = v,
            Message::Submit => {
                let file = std::fs::read(&self.job_file)
                    .map_err(anyhow::Error::from)
                    .and_then(|b| Ok(serde_json::from_slice(&b)?));
                match file {
                    Ok(job) => {
                        return self.act(ApiRequest::Submit {
                            request_id: id(),
                            id: format!("job-{:032x}", rand::random::<u128>()),
                            job,
                            worker: (!self.worker.is_empty()).then(|| self.worker.clone()),
                            memory_mb: self.memory.parse().unwrap_or(0),
                            gpu: self.gpu,
                            priority: 0,
                            after: Default::default(),
                            requires: self
                                .requires
                                .split(',')
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(str::to_owned)
                                .collect(),
                        });
                    }
                    Err(e) => self.notice = Some(e.to_string()),
                }
            }
            Message::Cancel(job, revision) => {
                return self.act(ApiRequest::Cancel {
                    request_id: id(),
                    id: job,
                    expected_revision: revision,
                });
            }
            Message::Suspend(job, revision, checkpoint) => {
                return self.act(ApiRequest::Suspend {
                    request_id: id(),
                    id: job,
                    expected_revision: revision,
                    checkpoint,
                });
            }
            Message::Resume(job, revision) => {
                return self.act(ApiRequest::Resume {
                    request_id: id(),
                    id: job,
                    expected_revision: revision,
                });
            }
            Message::Instance(instance, revision, desired) => {
                return self.act(ApiRequest::InstanceSet {
                    request_id: id(),
                    id: instance,
                    expected_revision: revision,
                    desired,
                });
            }
            Message::Fetch => {
                if let Some(id) = &self.selected {
                    return self.act(ApiRequest::Fetch {
                        attempt: None,
                        id: id.clone(),
                        destination: PathBuf::from(&self.destination),
                    });
                }
            }
            Message::Retry => {
                if let Some(request) = self.pending.clone() {
                    return self.act(request);
                }
            }
            Message::Acted(result) => {
                self.acting = false;
                match result {
                    Ok(value) => {
                        self.pending = None;
                        self.notice = Some(action_notice(&value));
                    }
                    Err(e) => self.notice = Some(e),
                }
                return Task::done(Message::Tick);
            }
            _ => {}
        }
        Task::none()
    }
    fn view(&self) -> Element<'_, Message> {
        let mut page = column![
            row![
                typography("Fleet", TypeScale::HeadlineLarge),
                typography("Replicated compute", TypeScale::TitleMedium).style(muted),
            ]
            .spacing(20)
            .align_y(Alignment::Center),
        ]
        .spacing(16);
        if let Some(error) = &self.error {
            page = page.push(
                surface(typography(
                    format!("Connection unavailable. Showing the last successful refresh. {error}"),
                    TypeScale::BodyMedium,
                ))
                .variant(SurfaceVariant::Outlined)
                .padding(12),
            );
        }
        if let Some(notice) = &self.notice {
            let mut line = row![typography(notice, TypeScale::BodyMedium).width(Length::Fill)]
                .spacing(12)
                .align_y(Alignment::Center);
            if self.pending.is_some() {
                line = line.push(
                    button("Retry same request")
                        .variant(ButtonVariant::Tonal)
                        .disabled(self.acting)
                        .on_press(Message::Retry),
                );
            }
            line = line.push(
                button("Dismiss")
                    .variant(ButtonVariant::Text)
                    .on_press(Message::DismissNotice),
            );
            page = page.push(surface(line).padding(12));
        }
        let submit = column![
            typography("Submit a job", TypeScale::TitleMedium),
            row![
                text_field("Job JSON file", &self.job_file).on_input(Message::JobFile),
                text_field("Worker (optional)", &self.worker)
                    .on_input(Message::Worker)
                    .width(220),
                button(if self.acting { "Working…" } else { "Submit" })
                    .disabled(self.acting || self.job_file.trim().is_empty())
                    .on_press(Message::Submit),
            ]
            .spacing(16)
            .align_y(Alignment::Center),
            row![
                checkbox(self.gpu)
                    .label("Reserve GPU for native jobs")
                    .on_toggle(Message::Gpu)
                    .width(300),
                text_field("Native RAM (MiB)", &self.memory)
                    .on_input(Message::Memory)
                    .width(180),
                text_field("Required tags (optional)", &self.requires).on_input(Message::Requires),
            ]
            .spacing(16)
            .align_y(Alignment::Center),
        ]
        .spacing(12);
        page = page.push(
            surface(submit)
                .variant(SurfaceVariant::Elevated)
                .padding(16),
        );
        if !self.workers.is_empty() {
            let mut machines = row![].spacing(12);
            for w in &self.workers {
                let name = w
                    .pointer("/registration/id")
                    .and_then(Value::as_str)
                    .unwrap_or("worker");
                let budget = &w["registration"]["budget"];
                machines = machines.push(
                    surface(
                        column![
                            typography(name, TypeScale::TitleMedium),
                            typography(
                                if w["status"].is_null() {
                                    "Unreachable"
                                } else if w["draining"] == true {
                                    "Draining"
                                } else {
                                    "Connected"
                                },
                                TypeScale::BodyMedium
                            ),
                            typography(
                                format!(
                                    "{} CPU · {} MiB budget",
                                    budget["cpu_slots"], budget["memory_mb"]
                                ),
                                TypeScale::BodySmall
                            )
                            .style(muted),
                        ]
                        .spacing(4),
                    )
                    .width(Length::Fill)
                    .padding(16),
                );
            }
            page = page.push(machines);
        }
        let mut jobs = column![typography("Jobs", TypeScale::TitleLarge)].spacing(12);
        if self.jobs.is_empty() {
            jobs = jobs.push(
                typography("No jobs submitted to this cluster.", TypeScale::BodyMedium)
                    .style(muted),
            );
        }
        for j in self.jobs.iter().rev() {
            let id = j["id"].as_str().unwrap_or("");
            let phase = j["phase"].as_str().unwrap_or("unknown");
            let name = j
                .pointer("/workload/Job/spec/name")
                .and_then(Value::as_str)
                .unwrap_or(id);
            let terminal = matches!(phase, "succeeded" | "failed" | "cancelled");
            let selected = self.selected.as_deref() == Some(id);
            let mut heading = row![
                button(name)
                    .variant(if selected {
                        ButtonVariant::Tonal
                    } else {
                        ButtonVariant::Text
                    })
                    .on_press(Message::Select(id.into())),
                typography(
                    if j["cancel_requested"] == true && !terminal {
                        "Cancellation pending"
                    } else {
                        phase
                    },
                    TypeScale::LabelLarge
                )
                .width(Length::Fill),
            ]
            .spacing(12)
            .align_y(Alignment::Center);
            if !terminal && j["cancel_requested"] != true {
                heading = heading.push(
                    button("Cancel")
                        .variant(ButtonVariant::Outlined)
                        .size(ButtonSize::Small)
                        .disabled(self.acting)
                        .on_press(Message::Cancel(
                            id.into(),
                            j["revision"].as_u64().unwrap_or(0),
                        )),
                );
            }
            if phase == "suspended" {
                heading = heading.push(
                    button("Resume")
                        .variant(ButtonVariant::Tonal)
                        .size(ButtonSize::Small)
                        .disabled(self.acting)
                        .on_press(Message::Resume(
                            id.into(),
                            j["revision"].as_u64().unwrap_or(0),
                        )),
                );
            } else if phase == "running"
                && let Some(checkpoint) = j.pointer("/latest_checkpoint/id").and_then(Value::as_str)
            {
                heading = heading.push(
                    button("Suspend at saved checkpoint")
                        .variant(ButtonVariant::Tonal)
                        .size(ButtonSize::Small)
                        .disabled(self.acting)
                        .on_press(Message::Suspend(
                            id.into(),
                            j["revision"].as_u64().unwrap_or(0),
                            checkpoint.into(),
                        )),
                );
            }
            let mut card =
                column![heading, typography(id, TypeScale::BodySmall).style(muted)].spacing(8);
            if let Some(attempt) = j
                .pointer("/assignment/attempt/number")
                .and_then(Value::as_u64)
            {
                card = card.push(
                    typography(format!("Attempt {attempt}"), TypeScale::BodySmall).style(muted),
                );
            }
            if let Some(cp) = j.get("latest_checkpoint").filter(|v| !v.is_null()) {
                let position = cp
                    .pointer("/content/manifest/position")
                    .and_then(Value::as_u64)
                    .map(|p| format!(" at position {p}"))
                    .unwrap_or_default();
                let backups = j["artifacts"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|a| a.pointer("/manifest/kind/Checkpoint") == cp.get("id"))
                    .and_then(|a| a["copies"].as_object())
                    .map(|copies| {
                        copies
                            .iter()
                            .filter(|(location, _)| location.starts_with("storage/"))
                            .map(|(location, copy)| {
                                format!(
                                    "{}: {}",
                                    location.trim_start_matches("storage/"),
                                    copy["phase"].as_str().unwrap_or("Unknown")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "no backup target configured".into());
                card = card.push(typography(format!("Verified checkpoint{position}. Backups — {backups}. Suspending discards work after it."), TypeScale::BodySmall).style(muted));
            }
            if let Some(worker) = j.pointer("/assignment/worker").and_then(Value::as_str) {
                card = card.push(
                    typography(format!("Worker: {worker}"), TypeScale::BodySmall).style(muted),
                );
            }
            if phase == "queued" {
                card = card.push(
                    typography(waiting_reason(&j["waiting"]), TypeScale::BodyMedium).style(muted),
                );
            }
            let mut measured = false;
            if let Some(execution) = j.pointer("/assignment/execution").and_then(Value::as_str)
                && let Some(probes) = self.metrics.get(execution)
            {
                for probe in probes {
                    card = card.push(typography(
                        format!("{}: {}", probe.name, reading(&probe.reading)),
                        TypeScale::BodyMedium,
                    ));
                    if let crate::probes::Reading::Series { values } = &probe.reading {
                        card =
                            card.push(canvas(Spark(values.clone())).width(Length::Fill).height(48));
                    }
                    if let crate::probes::Reading::Number {
                        value,
                        total: Some(total),
                        ..
                    } = probe.reading
                        && total > 0.0
                    {
                        measured = true;
                        card = card.push(
                            linear_progress((value / total).clamp(0.0, 1.0) as f32)
                                .wavy(phase == "running"),
                        );
                    }
                }
            }
            if !measured && matches!(phase, "running" | "starting") {
                card = card.push(linear_progress(0.0).indeterminate(true).wavy(true));
            }
            jobs = jobs.push(surface(card).padding(16).variant(if selected {
                SurfaceVariant::Outlined
            } else {
                SurfaceVariant::Elevated
            }));
        }
        let mut body = row![
            scrollable(jobs)
                .height(Length::Fill)
                .width(Length::FillPortion(3))
        ]
        .spacing(20);
        if let Some(id) = &self.selected {
            let logs = column![
                typography("Job output", TypeScale::TitleLarge),
                typography(id, TypeScale::BodySmall).style(muted),
                segmented_buttons(
                    [Segment::new(false, "stdout"), Segment::new(true, "stderr")],
                    SegmentSelection::Single(Some(self.stderr))
                )
                .on_change(|selection| Message::Stderr(matches!(
                    selection,
                    SegmentSelection::Single(Some(true))
                ))),
                scrollable(
                    text(if self.log.is_empty() {
                        "Waiting for output…"
                    } else {
                        &self.log
                    })
                    .font(iced::Font::MONOSPACE)
                    .size(12)
                )
                .height(Length::Fill),
                text_field("New output directory", &self.destination)
                    .on_input(Message::Destination),
                button("Fetch verified outputs")
                    .variant(ButtonVariant::Tonal)
                    .disabled(self.acting || self.destination.trim().is_empty())
                    .on_press(Message::Fetch),
            ]
            .spacing(12)
            .height(Length::Fill);
            body = body.push(
                surface(logs)
                    .variant(SurfaceVariant::Elevated)
                    .padding(16)
                    .width(Length::FillPortion(2)),
            );
        }
        page = page.push(body.height(Length::Fill));
        if !self.storage.is_null() {
            let mut storage = column![typography("Storage", TypeScale::TitleMedium)].spacing(6);
            for line in storage_lines(&self.storage) {
                storage = storage.push(typography(line, TypeScale::BodySmall));
            }
            page = page.push(
                surface(scrollable(storage).height(150))
                    .variant(SurfaceVariant::Outlined)
                    .padding(12),
            );
        }
        if !self.instances.is_empty() {
            let mut vms = column![typography("Persistent VMs", TypeScale::TitleLarge)].spacing(8);
            for vm in &self.instances {
                let id = vm["id"].as_str().unwrap_or("");
                let rev = vm["revision"].as_u64().unwrap_or(0);
                use crate::control::workload::InstanceDesired;
                let action = |label, desired| {
                    button(label)
                        .variant(ButtonVariant::Tonal)
                        .size(ButtonSize::Small)
                        .disabled(self.acting)
                        .on_press(Message::Instance(id.into(), rev, desired))
                };
                vms = vms.push(
                    row![
                        typography(
                            format!(
                                "{id} · {} · desired {}",
                                vm["worker"].as_str().unwrap_or("unknown"),
                                vm["desired"]["Instance"].as_str().unwrap_or("unknown")
                            ),
                            TypeScale::BodyMedium
                        )
                        .width(Length::Fill),
                        action("Start", InstanceDesired::Running),
                        action("Stop", InstanceDesired::Stopped),
                        action("Delete", InstanceDesired::Deleted).variant(ButtonVariant::Outlined),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                );
            }
            page = page.push(container(scrollable(vms)).max_height(160));
        }
        iced_m3::focus::scope(container(page).padding(24).height(Length::Fill))
    }
}
fn muted(theme: &Theme) -> text::Style {
    text::Style {
        color: Some(theme.colors.on_surface_variant),
    }
}
fn waiting_reason(value: &Value) -> String {
    if let Some(workers) = value.as_object() {
        workers
            .iter()
            .map(|(worker, reason)| format!("{worker}: {}", reason.as_str().unwrap_or("waiting")))
            .collect::<Vec<_>>()
            .join(" · ")
    } else {
        value.as_str().unwrap_or("Waiting for placement").to_owned()
    }
}
fn action_notice(value: &Value) -> String {
    if value.pointer("/reply/RecoveryUnknown").is_some() {
        return "The historical outcome is unknown after cluster recovery. This request was not reexecuted; inspect the job before submitting new work.".into();
    }
    if value.pointer("/reply/Expired").is_some() {
        return "This request has expired. Inspect its job before submitting new work.".into();
    }
    if let Some(reason) = value.pointer("/reply/Rejected/reason") {
        return format!(
            "Request rejected: {}",
            reason
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| reason.to_string())
        );
    }
    if let Some(job) = value.pointer("/reply/Accepted/job").and_then(Value::as_str) {
        return format!("Job {job} accepted. The queue shows its placement and input status.");
    }
    if let Some(request) = value["request_id"].as_str() {
        return format!("Request {request} confirmed.");
    }
    "Outputs fetched and verified.".into()
}

fn reading(value: &crate::probes::Reading) -> String {
    use crate::probes::Reading;
    match value {
        Reading::Number {
            value,
            total,
            per_hour,
            eta_seconds,
        } => format!(
            "{value:.2}{}{}{}",
            total.map(|v| format!(" / {v:.2}")).unwrap_or_default(),
            per_hour
                .map(|v| format!(" · {v:.2}/hour"))
                .unwrap_or_default(),
            eta_seconds
                .map(|v| format!(" · ETA {}", crate::probes::duration(v)))
                .unwrap_or_default()
        ),
        Reading::Series { values } => values
            .last()
            .map(|v| format!("{v:.4} · {} samples", values.len()))
            .unwrap_or_else(|| "No samples".into()),
        Reading::Count { count } => count.to_string(),
        Reading::Text { text } => text.clone(),
    }
}

struct Spark(Vec<f64>);
impl<Message> canvas::Program<Message, Theme> for Spark {
    type State = ();
    fn draw(
        &self,
        _: &(),
        renderer: &iced::Renderer,
        theme: &Theme,
        bounds: iced::Rectangle,
        _: iced::mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let values: Vec<_> = self.0.iter().copied().filter(|v| v.is_finite()).collect();
        if values.len() > 1 {
            let lo = values.iter().copied().fold(f64::INFINITY, f64::min);
            let hi = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let span = if hi > lo { hi - lo } else { 1.0 };
            let point = |i: usize, v: f64| {
                iced::Point::new(
                    i as f32 / (values.len() - 1) as f32 * bounds.width,
                    2.0 + (bounds.height - 4.0) * (1.0 - ((v - lo) / span) as f32),
                )
            };
            let line = canvas::Path::new(|p| {
                p.move_to(point(0, values[0]));
                for (i, v) in values.iter().enumerate().skip(1) {
                    p.line_to(point(i, *v));
                }
            });
            frame.stroke(
                &line,
                canvas::Stroke::default()
                    .with_width(2.0)
                    .with_color(theme.colors.primary),
            );
        }
        vec![frame.into_geometry()]
    }
}

fn storage_lines(storage: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    for location in storage["storage_reports"].as_array().into_iter().flatten() {
        let id = location["storage"].as_str().unwrap_or("unknown");
        let r = &location["report"];
        if r.is_null() {
            lines.push(format!("Backup {id}: storage report unavailable"));
            continue;
        }
        lines.push(format!("Backup {id}: {} file bytes · {} physically allocated · {} capacity · {} staging bytes · observed {}s ago",
            r["objects"]["file_bytes"],r["objects"]["allocated_bytes"],r["capacity_bytes"],r["staging"]["file_bytes"],
            crate::jobs::now().saturating_sub(r["observed_at"].as_u64().unwrap_or(0))));
    }
    if storage["cleanup_allowed"] == false {
        lines.push("Cleanup held: recovery review required".into());
    }
    for worker in storage["workers"].as_array().into_iter().flatten() {
        let name = worker["worker"].as_str().unwrap_or("unknown");
        let r = &worker["report"];
        if r.is_null() {
            lines.push(format!("{name}: storage report unavailable"));
            continue;
        }
        let age = crate::jobs::now().saturating_sub(r["observed_at"].as_u64().unwrap_or(0));
        lines.push(format!(
            "{name}: cache {} bytes · eligible for reclamation {} bytes · observed {age}s ago",
            r["blob_payload_bytes"], r["reclaimable_blob_bytes"]
        ));
        for (id, capture) in r["checkpoints"].as_object().into_iter().flatten() {
            lines.push(format!("{id}: {} retained checkpoints / {} captured · {} payload bytes · {} transfer bytes in budget window{}",
                capture["live_checkpoints"], capture["captures_total"], capture["payload_bytes"], capture["transfer_bytes"],
                capture["blocked"].as_str().map(|s| format!(" · Blocked: {s}")).unwrap_or_default()));
            if let Some(policy) = capture.get("retention").filter(|p| !p.is_null()) {
                lines.push(format!("Keep latest {} plus protected checkpoints · {} capture slots · {} bytes capture budget · {} bytes/hour transfer budget",
                    policy["keep_last"],policy["max_live_checkpoints"],policy["max_live_bytes"],policy["transfer_bytes_per_hour"]));
            }
        }
    }
    for decision in storage["retention"].as_array().into_iter().flatten() {
        if decision["deleted"] == true
            && decision["copy_problems"]
                .as_array()
                .is_some_and(|p| p.is_empty())
        {
            continue;
        }
        let artifact = decision["artifact"].as_str().unwrap_or("");
        let reasons = decision["reasons"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!(
            "{} · {} bytes · {reasons}",
            &artifact[..artifact.len().min(12)],
            decision["bytes"]
        ));
        for problem in decision["copy_problems"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            lines.push(problem.to_owned());
        }
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn sample() -> App {
        let mut app = App::new("/example/fleet-client".into());
        app.jobs = vec![
            json!({"id":"training","revision":2,"phase":"running","cancel_requested":false,"workload":{"Job":{"spec":{"name":"Training canary"}}},"assignment":{"worker":"m4pro","execution":"exec-training"}}),
        ];
        app.workers = vec![
            json!({"registration":{"id":"m4pro","budget":{"cpu_slots":12,"memory_mb":40960}},"draining":false,"status":{}}),
        ];
        app.metrics.insert(
            "exec-training".into(),
            vec![
                crate::probes::ProbeValue {
                    name: "Iteration".into(),
                    reading: crate::probes::Reading::Number {
                        value: 48.0,
                        total: Some(128.0),
                        per_hour: Some(12.0),
                        eta_seconds: Some(24000.0),
                    },
                },
                crate::probes::ProbeValue {
                    name: "Loss".into(),
                    reading: crate::probes::Reading::Series {
                        values: vec![0.8, 0.7, 0.65, 0.5, 0.55, 0.4],
                    },
                },
            ],
        );
        app
    }
    #[test]
    fn replicated_dashboard_renders_and_routes_actions() {
        let app = sample();
        let mut ui = simulator(&app);
        ui.click("Cancel").unwrap();
        assert!(
            ui.into_messages()
                .any(|m| matches!(m,Message::Cancel(id,2) if id=="training"))
        );
        for dark in [false, true] {
            let mut app = sample();
            app.dark = dark;
            app.selected = Some("training".into());
            app.log = "Iteration 48 / 128\nloss = 0.4000\nCheckpoint saved\n".into();
            let snapshot = simulator(&app).snapshot(&app.theme()).unwrap();
            if let Some(dir) = std::env::var_os("FLEET_GUI_PREVIEW") {
                assert!(
                    snapshot
                        .matches_image(PathBuf::from(dir).join(if dark {
                            "replicated-dashboard-dark"
                        } else {
                            "replicated-dashboard-light"
                        }))
                        .unwrap()
                );
            }
            let mut ui = simulator(&app);
            ui.click("stderr").unwrap();
            assert!(
                ui.into_messages()
                    .any(|m| matches!(m, Message::Stderr(true)))
            );
        }
    }
    #[test]
    fn checkpoint_actions_and_attempt_log_reset() {
        let mut app = sample();
        app.jobs[0]["latest_checkpoint"] =
            json!({"id":"saved-point","content":{"manifest":{"position":48}}});
        app.jobs[0]["artifacts"] = json!([{"manifest":{"kind":{"Checkpoint":"saved-point"}},"copies":{"storage/backup-1":{"phase":"Ready"},"storage/backup-2":{"phase":"Pending"}}}]);
        app.jobs[0]["assignment"]["attempt"] = json!({"number":1});
        app.selected = Some("training".into());
        let mut ui = simulator(&app);
        ui.click("Suspend at saved checkpoint").unwrap();
        assert!(ui.into_messages().any(
            |m| matches!(m, Message::Suspend(id,2,cp) if id=="training" && cp=="saved-point")
        ));
        for (phase, filename) in [
            ("running", "checkpoint-running"),
            ("suspended", "checkpoint-suspended"),
        ] {
            app.jobs[0]["phase"] = json!(phase);
            let snapshot = simulator(&app).snapshot(&app.theme()).unwrap();
            if let Some(dir) = std::env::var_os("FLEET_GUI_PREVIEW") {
                assert!(
                    snapshot
                        .matches_image(PathBuf::from(dir).join(filename))
                        .unwrap()
                );
            }
        }
        let mut ui = simulator(&app);
        ui.click("Resume").unwrap();
        assert!(
            ui.into_messages()
                .any(|m| matches!(m, Message::Resume(id,2) if id=="training"))
        );
        app.log = "old-attempt".into();
        app.log_next = 11;
        let mut jobs = app.jobs.clone();
        jobs[0]["assignment"]["execution"] = json!("exec-training-a2");
        let _ = app.update(Message::Refreshed(Ok(Refresh {
            jobs,
            workers: vec![],
            instances: vec![],
            storage: Value::Null,
            selected: Some("training".into()),
            stderr: false,
            log: Some(json!({"Logs":{"data":"wrong-offset","next":20}})),
            events: vec![],
        })));
        assert!(app.log.is_empty());
        assert_eq!(app.log_next, 0);
    }
    #[test]
    fn storage_dashboard_renders_budgets_and_unavailable_locations() {
        let mut app = sample();
        app.storage = json!({"cleanup_allowed":true,"workers":[
            {"worker":"m4","report":{"observed_at":crate::jobs::now(),"blob_payload_bytes":131072,
            "reclaimable_blob_bytes":65536,"checkpoints":{"training-a1":{"live_checkpoints":2,
            "captures_total":24,"payload_bytes":131072,"transfer_bytes":1576335,"blocked":null}}}},
            {"worker":"remote","report":null}],"retention":[{"artifact":"research-checkpoint","bytes":65536,"reasons":["pinned"]}]});
        for dark in [false, true] {
            app.dark = dark;
            let snapshot = simulator(&app).snapshot(&app.theme()).unwrap();
            if let Some(dir) = std::env::var_os("FLEET_GUI_PREVIEW") {
                assert!(
                    snapshot
                        .matches_image(PathBuf::from(dir).join(if dark {
                            "storage-dark"
                        } else {
                            "storage-light"
                        }))
                        .unwrap()
                );
            }
        }
    }
    fn simulator(app: &App) -> iced_test::Simulator<'_, Message, Theme> {
        iced_test::Simulator::with_size(
            iced::Settings {
                default_font: iced_m3::fonts::REGULAR,
                ..Default::default()
            },
            (1220.0, 820.0),
            app.view(),
        )
    }
    #[test]
    fn unknown_recovery_outcome_is_never_presented_as_confirmed() {
        let notice = action_notice(
            &json!({"request_id":"saved", "reply":{"RecoveryUnknown":{"origin":"old","current":"new"}}}),
        );
        assert!(notice.contains("unknown"));
        assert!(!notice.contains("confirmed"));
        assert_eq!(
            action_notice(&json!({"reply":{"Rejected":{"reason":"worker is drained"}}})),
            "Request rejected: worker is drained"
        );
    }
    #[test]
    fn stale_refresh_never_appends_to_a_different_jobs_log() {
        let mut app = sample();
        app.selected = Some("new-job".into());
        let _ = app.update(Message::Refreshed(Ok(Refresh {
            jobs: vec![],
            workers: vec![],
            instances: vec![],
            storage: Value::Null,
            selected: Some("old-job".into()),
            stderr: false,
            log: Some(json!({"Logs":{"data":"wrong job","next":9}})),
            events: vec![],
        })));
        assert!(app.log.is_empty());
        assert_eq!(app.log_next, 0);
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn storage_shows_unavailability_blockage_retention_and_report_age() {
        let lines = storage_lines(&json!({"cleanup_allowed":false,"workers":[
            {"worker":"offline","report":null},
            {"worker":"worker","report":{"observed_at":0,"blob_payload_bytes":123,
            "reclaimable_blob_bytes":12,"checkpoints":{"attempt":{"live_checkpoints":3,
            "captures_total":29,"payload_bytes":90,"transfer_bytes":300,"blocked":"pin pressure"}}}}
        ],"retention":[{"artifact":"abc","bytes":30,"reasons":["pinned"]}]}))
        .join("\n");
        for label in [
            "recovery review",
            "offline: storage report unavailable",
            "123 bytes",
            "12 bytes",
            "observed",
            "3 retained",
            "29 captured",
            "Blocked: pin pressure",
            "pinned",
        ] {
            assert!(lines.contains(label), "missing {label}: {lines}");
        }
    }
}
