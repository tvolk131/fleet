//! `fleet gui`: the machines and the queue at a glance, with progress bars, estimated finishes, sparklines and live
//! logs. A client of the daemon: it asks the daemon's socket every two seconds, so closing it changes nothing.
//!
//! Drawn with iced-m3's Material 3 Expressive components: a wavy bar for a job that is running (flat while it is paused
//! or once it has finished), a wavy loading bar for one running without a measure, the shape-morphing loading
//! indicator while it starts, and button groups for its actions. It follows the system's light or dark appearance.
//!
//! The cards stay short: a machine's connection, power and thread use; a job's bar and a few large figures. A click on
//! a card opens a dialog with everything else, and a job's dialog holds its rarer actions (suspend, cancel).

use std::{
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use iced::{
    Alignment, Border, Color, Length, Rectangle, Renderer, Subscription, Task, mouse,
    theme::Mode,
    widget::{Space, canvas, column, container, row, scrollable, text},
};
use iced_m3::{
    Button, ButtonSize, ButtonVariant, Element, Segment, SegmentSelection, SurfaceVariant, Theme,
    TypeScale, button, button_group, dialog, filter_chip, linear_progress, loading_indicator,
    segmented_buttons, snackbar, surface, tooltip, typography,
};

use crate::{
    daemon::{
        DaemonRequest, DaemonResponse, MachineView, Placement, QState, QueuedJob, ask_daemon,
    },
    jobs::{EndReason, Exit, now},
    probes::{Reading, duration},
};

/// The most log text kept in the panel.
const LOG_KEEP: usize = 200_000;

/// The accent the Material palette is generated from.
const ACCENT: Color = Color::from_rgb(0.29, 0.44, 0.85);

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
    .title("fleet")
    .subscription(App::subscription)
    .theme(App::theme)
    .default_font(iced_m3::fonts::REGULAR)
    .window_size((1280.0, 840.0))
    .run()
}

struct App {
    home: PathBuf,
    dark: bool,
    machines: Vec<MachineView>,
    jobs: Vec<QueuedJob>,
    /// Why the last refresh failed (for example, no daemon running).
    problem: Option<String>,
    refreshed_at: Option<u64>,
    /// The wall clock in whole seconds, as of the last tick. Every age and clock reading on screen is computed from
    /// this one reading, so they all advance together, once a second.
    now: u64,
    refreshing: bool,
    show_finished: bool,
    log: Option<LogView>,
    /// The machine or job in the details dialog. Kept after the dialog closes so it can animate out.
    details: Option<Details>,
    details_open: bool,
    /// The suspend plan in the dialog, once fetched. Kept after the dialog closes so it can animate out.
    plan: Option<(String, Result<crate::suspend::Plan, String>)>,
    plan_open: bool,
    /// The failed action shown in a snackbar, numbered so a repeat shows again.
    notice: Option<(u64, String)>,
    notices: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Details {
    Machine(String),
    Job(String),
}

struct LogView {
    job: String,
    stderr: bool,
    text: String,
    next: u64,
    done: bool,
    loading: bool,
}

#[derive(Debug, Clone)]
enum Message {
    Tick,
    /// A new second on the wall clock.
    Second(u64),
    Appearance(Mode),
    Refreshed(Result<(Vec<MachineView>, Vec<QueuedJob>), String>),
    ToggleFinished,
    ShowMachine(String),
    ShowJob(String),
    HideDetails,
    OpenLog(String),
    CloseLog,
    ShowStderr(bool),
    LogChunk {
        job: String,
        stderr: bool,
        result: Result<(String, u64, bool), String>,
    },
    Cancel(String),
    Pause(String, bool),
    ShowPlan(String),
    PlanReady(String, Result<crate::suspend::Plan, String>),
    HidePlan,
    Suspend(String, bool),
    Resume(String),
    Acted(Result<(), String>),
    Dismiss(u64),
}

async fn ask(home: PathBuf, request: DaemonRequest) -> Result<DaemonResponse, String> {
    match ask_daemon(&home, &request).await {
        Ok(Some(DaemonResponse::Error { message, .. })) => Err(message),
        Ok(Some(response)) => Ok(response),
        Ok(None) => Err("no fleet daemon is running: start one with `fleet daemon`".into()),
        Err(e) => Err(format!("{e:#}")),
    }
}

/// The wall clock, read just after each whole second turns.
fn seconds() -> impl iced::futures::Stream<Item = Message> {
    iced::futures::stream::unfold((), |()| async {
        let since = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        tokio::time::sleep(until_next_second(since)).await;
        Some((Message::Second(now()), ()))
    })
}

/// How long from `since` (a time since the epoch) until a few milliseconds past the next whole second: late enough
/// that the second has turned when the clock is read, whatever the timer's slack.
fn until_next_second(since: Duration) -> Duration {
    Duration::from_nanos(u64::from(1_000_000_000 - since.subsec_nanos())) + Duration::from_millis(5)
}

async fn refresh(home: PathBuf) -> Result<(Vec<MachineView>, Vec<QueuedJob>), String> {
    let DaemonResponse::Machines { machines } = ask(home.clone(), DaemonRequest::Machines).await?
    else {
        return Err("unexpected answer".into());
    };
    let DaemonResponse::Queue { jobs } = ask(home, DaemonRequest::Queue).await? else {
        return Err("unexpected answer".into());
    };
    Ok((machines, jobs))
}

impl App {
    fn new(home: PathBuf) -> Self {
        Self {
            home,
            dark: true,
            machines: Vec::new(),
            jobs: Vec::new(),
            problem: None,
            refreshed_at: None,
            now: now(),
            refreshing: false,
            show_finished: false,
            log: None,
            details: None,
            details_open: false,
            plan: None,
            plan_open: false,
            notice: None,
            notices: 0,
        }
    }

    fn theme(&self) -> Theme {
        Theme::from_accent(ACCENT, self.dark).expressive()
    }

    fn subscription(&self) -> Subscription<Message> {
        Subscription::batch([
            iced::time::every(Duration::from_secs(2)).map(|_| Message::Tick),
            Subscription::run(seconds),
            iced::system::theme_changes().map(Message::Appearance),
        ])
    }

    fn log_task(&mut self) -> Task<Message> {
        let Some(log) = &mut self.log else {
            return Task::none();
        };
        if log.loading || log.done {
            return Task::none();
        }
        log.loading = true;
        let (job, stderr, from) = (log.job.clone(), log.stderr, log.next);
        Task::perform(
            ask(
                self.home.clone(),
                DaemonRequest::Logs {
                    job: job.clone(),
                    stderr,
                    from,
                },
            ),
            move |answer| Message::LogChunk {
                job: job.clone(),
                stderr,
                result: answer.and_then(|a| match a {
                    DaemonResponse::Logs { data, next, done } => Ok((data, next, done)),
                    _ => Err("unexpected answer".into()),
                }),
            },
        )
    }

    /// Sends `request`, reporting a refusal in the snackbar.
    fn act(&self, request: DaemonRequest) -> Task<Message> {
        Task::perform(ask(self.home.clone(), request), |a| {
            Message::Acted(a.map(|_| ()))
        })
    }

    fn update(&mut self, message: Message) -> Task<Message> {
        match message {
            Message::Tick => {
                let logs = self.log_task();
                if self.refreshing {
                    return logs;
                }
                self.refreshing = true;
                Task::batch([
                    Task::perform(refresh(self.home.clone()), Message::Refreshed),
                    logs,
                ])
            }
            Message::Second(at) => {
                self.now = at;
                Task::none()
            }
            Message::Appearance(mode) => {
                self.dark = mode != Mode::Light;
                Task::none()
            }
            Message::Refreshed(result) => {
                self.refreshing = false;
                match result {
                    Ok((machines, jobs)) => {
                        self.machines = machines;
                        self.jobs = jobs;
                        self.problem = None;
                        self.refreshed_at = Some(now());
                    }
                    Err(e) => self.problem = Some(e),
                }
                Task::none()
            }
            Message::ToggleFinished => {
                self.show_finished = !self.show_finished;
                Task::none()
            }
            Message::ShowMachine(name) => {
                self.details = Some(Details::Machine(name));
                self.details_open = true;
                Task::none()
            }
            Message::ShowJob(id) => {
                self.details = Some(Details::Job(id));
                self.details_open = true;
                Task::none()
            }
            Message::HideDetails => {
                self.details_open = false;
                Task::none()
            }
            Message::OpenLog(job) => {
                // The panel sits under the dialog, so opening it from a job's dialog closes the dialog.
                self.details_open = false;
                self.log = Some(LogView {
                    job,
                    stderr: false,
                    text: String::new(),
                    next: 0,
                    done: false,
                    loading: false,
                });
                self.log_task()
            }
            Message::CloseLog => {
                self.log = None;
                Task::none()
            }
            Message::ShowStderr(stderr) => {
                if let Some(log) = &mut self.log
                    && log.stderr != stderr
                {
                    *log = LogView {
                        job: log.job.clone(),
                        stderr,
                        text: String::new(),
                        next: 0,
                        done: false,
                        loading: false,
                    };
                }
                self.log_task()
            }
            Message::LogChunk {
                job,
                stderr,
                result,
            } => {
                let Some(log) = &mut self.log else {
                    return Task::none();
                };
                if log.job != job || log.stderr != stderr {
                    return Task::none();
                }
                log.loading = false;
                match result {
                    Ok((data, next, done)) => {
                        log.text.push_str(&data);
                        if log.text.len() > LOG_KEEP {
                            let cut = log.text.len() - LOG_KEEP;
                            let cut = (cut..log.text.len())
                                .find(|&i| log.text.is_char_boundary(i))
                                .unwrap_or(cut);
                            log.text.drain(..cut);
                        }
                        let more = next > log.next && !done;
                        log.next = next;
                        log.done = done;
                        if more {
                            return self.log_task();
                        }
                    }
                    Err(e) => {
                        log.text.push_str(&format!("\n[fleet: {e}]\n"));
                        log.done = true;
                    }
                }
                Task::none()
            }
            Message::Cancel(job) => self.act(DaemonRequest::Cancel { job }),
            Message::Pause(job, pause) => self.act(if pause {
                DaemonRequest::Pause { job }
            } else {
                DaemonRequest::Continue { job }
            }),
            Message::ShowPlan(job) => {
                self.plan = None;
                self.plan_open = false;
                Task::perform(
                    ask(
                        self.home.clone(),
                        DaemonRequest::SuspendPlan { job: job.clone() },
                    ),
                    move |a| {
                        Message::PlanReady(
                            job.clone(),
                            a.and_then(|a| match a {
                                DaemonResponse::Plan { plan } => Ok(plan),
                                _ => Err("unexpected answer".into()),
                            }),
                        )
                    },
                )
            }
            Message::PlanReady(job, plan) => {
                self.plan = Some((job, plan));
                self.plan_open = true;
                Task::none()
            }
            Message::HidePlan => {
                self.plan_open = false;
                Task::none()
            }
            Message::Suspend(job, now) => {
                self.plan_open = false;
                self.act(DaemonRequest::Suspend {
                    job,
                    now,
                    within_seconds: None,
                })
            }
            Message::Resume(job) => self.act(DaemonRequest::Resume { job, on: None }),
            Message::Acted(result) => {
                if let Err(e) = result {
                    self.notices += 1;
                    self.notice = Some((self.notices, e));
                }
                Task::done(Message::Tick)
            }
            Message::Dismiss(id) => {
                if self.notice.as_ref().is_some_and(|(n, _)| *n == id) {
                    self.notice = None;
                }
                Task::none()
            }
        }
    }

    fn view(&self) -> Element<'_, Message> {
        let theme = self.theme();
        let online = self.machines.iter().filter(|m| m.online).count();
        let count = |state: QState| self.jobs.iter().filter(|j| j.state == state).count();
        let status: Element<'_, Message> = match (&self.problem, self.refreshed_at) {
            (Some(p), _) => typography(p.clone(), TypeScale::BodyMedium)
                .style(|t: &Theme| text::Style {
                    color: Some(t.colors.error),
                })
                .into(),
            (None, Some(at)) => typography(
                format!(
                    "{online} of {} machines online · {} running · {} queued · updated {} s ago",
                    self.machines.len(),
                    count(QState::Running),
                    count(QState::Queued),
                    self.now.saturating_sub(at)
                ),
                TypeScale::BodyMedium,
            )
            .style(muted)
            .into(),
            (None, None) => row![
                loading_indicator().size(28.0),
                typography("Connecting to the daemon…", TypeScale::BodyMedium).style(muted),
            ]
            .spacing(8)
            .align_y(Alignment::Center)
            .into(),
        };
        let header = row![
            typography("fleet", TypeScale::HeadlineMedium),
            container(status).padding([0, 16]),
            Space::new().width(Length::Fill),
            filter_chip("Show finished", self.show_finished).on_press(Message::ToggleFinished),
        ]
        .align_y(Alignment::Center)
        .spacing(8);

        let machines = column(
            self.machines
                .iter()
                .map(|m| machine_card(m, &self.jobs, &theme, self.now)),
        )
        .spacing(12)
        .padding(iced::Padding::ZERO.right(16));
        let mut jobs: Vec<&QueuedJob> = self
            .jobs
            .iter()
            .filter(|j| self.show_finished || !j.state.finished())
            .collect();
        jobs.sort_by_key(|j| {
            (
                j.state.finished(),
                j.state != QState::Running,
                std::cmp::Reverse(j.submitted_at),
            )
        });
        let queue: Element<'_, Message> = if jobs.is_empty() {
            container(
                typography(
                    "No jobs. Submit one with `fleet submit job.json`.",
                    TypeScale::BodyLarge,
                )
                .style(muted),
            )
            .padding(20)
            .into()
        } else {
            column(jobs.into_iter().map(|j| job_card(j, &theme, self.now)))
                .spacing(12)
                .padding(iced::Padding::ZERO.right(16))
                .into()
        };
        let body = row![
            column![
                section("Machines"),
                scrollable(machines).height(Length::Fill)
            ]
            .spacing(8)
            .width(340),
            column![section("Queue"), scrollable(queue).height(Length::Fill)]
                .spacing(8)
                .width(Length::Fill),
        ]
        .spacing(24)
        .height(Length::Fill);

        let mut page = column![header, body].spacing(16).padding(24);
        if let Some(log) = &self.log {
            page = page.push(log_panel(log));
        }
        let notice = self.notice.as_ref().map(|(id, e)| {
            snackbar(e.clone())
                .id(*id)
                .duration(Duration::from_secs(8))
                .on_dismiss(Message::Dismiss(*id))
        });
        // The suspend dialog opens from a job's dialog, so it stacks above it.
        iced_m3::focus::scope(dialog::stack(
            snackbar::host(page, notice),
            [
                (self.details_dialog(&theme), self.details_open),
                (self.plan_dialog(), self.plan_open),
            ],
        ))
    }

    /// The open card's dialog: everything the card leaves out, live as the queue refreshes.
    fn details_dialog(&self, theme: &Theme) -> dialog::Dialog<'_, Message> {
        let close = || {
            button("Close")
                .variant(ButtonVariant::Text)
                .on_press(Message::HideDetails)
        };
        let found = match &self.details {
            Some(Details::Machine(name)) => self
                .machines
                .iter()
                .find(|m| &m.name == name)
                .map(|m| machine_dialog(m, &self.jobs, theme, self.now).actions(close())),
            Some(Details::Job(id)) => self.jobs.iter().find(|j| &j.id == id).map(|j| {
                let mut footer = row![].spacing(16).align_y(Alignment::Center);
                if let Some(actions) = actions(j, true) {
                    footer = footer.push(actions);
                }
                job_dialog(j, theme, self.now).actions(footer.push(close()))
            }),
            None => return dialog(Space::new()).on_dismiss(Message::HideDetails),
        };
        found
            .unwrap_or_else(|| {
                dialog(typography("It is no longer listed.", TypeScale::BodyLarge)).actions(close())
            })
            .on_dismiss(Message::HideDetails)
    }

    /// The suspend dialog: what suspending now or at the next checkpoint costs, and the two ways to do it.
    fn plan_dialog(&self) -> dialog::Dialog<'_, Message> {
        let Some((id, plan)) = &self.plan else {
            return dialog(Space::new()).on_dismiss(Message::HidePlan);
        };
        let name = self
            .jobs
            .iter()
            .find(|j| &j.id == id)
            .map_or(id.as_str(), |j| j.name.as_str());
        let mut body = column![typography(
            format!("Suspend {name}?"),
            TypeScale::HeadlineSmall
        )]
        .spacing(12);
        let actions: Element<'_, Message> = match plan {
            Ok(plan) => {
                for line in crate::suspend::describe(plan) {
                    body = body.push(typography(line, TypeScale::BodyMedium).style(muted));
                }
                // The lines above give what each choice costs, so the buttons stay short.
                row![
                    button("Keep running")
                        .variant(ButtonVariant::Text)
                        .on_press(Message::HidePlan),
                    danger(button("Suspend now").variant(ButtonVariant::Tonal))
                        .on_press(Message::Suspend(id.clone(), true)),
                    button("At the next checkpoint").on_press(Message::Suspend(id.clone(), false)),
                ]
                .spacing(8)
                .into()
            }
            Err(e) => {
                body = body.push(typography(e.clone(), TypeScale::BodyMedium).style(
                    |t: &Theme| text::Style {
                        color: Some(t.colors.error),
                    },
                ));
                button("Close")
                    .variant(ButtonVariant::Text)
                    .on_press(Message::HidePlan)
                    .into()
            }
        };
        dialog(body)
            .actions(actions)
            .on_dismiss(Message::HidePlan)
            .width(560.0)
    }
}

/// Secondary text: the surface's variant color.
fn muted(t: &Theme) -> text::Style {
    text::Style {
        color: Some(t.colors.on_surface_variant),
    }
}

/// A destructive action: the error container's colors.
fn danger<'a>(b: Button<'a, Message>) -> Button<'a, Message> {
    b.palette(|t| (t.colors.error_container, t.colors.on_error_container))
}

fn section(title: &str) -> Element<'_, Message> {
    typography(title.to_string(), TypeScale::TitleSmall)
        .style(muted)
        .into()
}

/// A panel that is not clickable (the log panel).
fn panel<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    surface(content)
        .variant(SurfaceVariant::Elevated)
        .padding(16)
        .into()
}

/// A card that opens `message`'s dialog when clicked; the buttons inside it keep their own actions.
fn clickable<'a>(
    content: impl Into<Element<'a, Message>>,
    message: Message,
) -> Element<'a, Message> {
    iced_m3::card(content)
        .variant(SurfaceVariant::Elevated)
        .on_press(message)
        .into()
}

/// A connection or health dot; green does not come from the Material palette, so it is tuned to the appearance.
fn dot<'a>(color: impl Fn(&Theme) -> Color + 'a) -> Element<'a, Message> {
    container(Space::new().width(10).height(10))
        .style(move |t: &Theme| container::Style {
            background: Some(color(t).into()),
            border: Border {
                radius: 5.0.into(),
                ..Border::default()
            },
            ..container::Style::default()
        })
        .into()
}

fn success(t: &Theme) -> Color {
    if t.is_dark() {
        Color::from_rgb(0.47, 0.85, 0.56)
    } else {
        Color::from_rgb(0.12, 0.53, 0.25)
    }
}

/// Amber, for a machine reached only through a relay.
fn warning(t: &Theme) -> Color {
    if t.is_dark() {
        Color::from_rgb(0.96, 0.76, 0.36)
    } else {
        Color::from_rgb(0.68, 0.45, 0.0)
    }
}

fn colored(color: fn(&Theme) -> Color) -> impl Fn(&Theme) -> text::Style {
    move |t| text::Style {
        color: Some(color(t)),
    }
}

/// The path a machine is reached by: the selected one, or else the first.
fn path(m: &MachineView) -> Option<&crate::PathReport> {
    m.paths.iter().find(|p| p.selected).or(m.paths.first())
}

/// The connection as a dot: green when reached directly, amber through a relay, red when offline; the hint says which.
fn connection<'a>(m: &MachineView, now: u64) -> Element<'a, Message> {
    let path = path(m);
    let relay = path.is_some_and(|p| p.kind == "relay");
    let online = m.online;
    let hint = match (online, path) {
        (false, _) => format!(
            "Offline{}",
            m.last_seen
                .map(|t| format!(", last seen {}", ago(t, now)))
                .unwrap_or_default()
        ),
        (true, Some(p)) => format!("Online, {} path, {:.0} ms", p.kind, p.rtt_ms),
        (true, None) => "Online".into(),
    };
    tooltip(
        dot(move |t| match (online, relay) {
            (false, _) => t.colors.error,
            (true, true) => warning(t),
            (true, false) => success(t),
        }),
        hint,
    )
    .into()
}

/// Power as a glyph, the way macOS shows it: a battery filled to its charge (green on AC, red when low) with a bolt on
/// AC, or a bolt alone for a machine without a battery. The percentage is written beside it while on battery.
fn power<'a>(m: &MachineView) -> Option<Element<'a, Message>> {
    let s = m.status.as_ref().filter(|_| m.online)?;
    let ac = s.on_ac_power == Some(true);
    let hint = match (ac, s.battery_percent) {
        (true, Some(p)) => format!("On AC power, battery {p}%"),
        (true, None) => "On AC power".to_string(),
        (false, Some(p)) => format!("On battery, {p}%"),
        (false, None) => return None,
    };
    let glyph = canvas(Power {
        ac,
        percent: s.battery_percent,
    })
    .width(if s.battery_percent.is_some() { 30 } else { 12 })
    .height(15);
    let mut content = row![].spacing(6).align_y(Alignment::Center);
    if let (false, Some(p)) = (ac, s.battery_percent) {
        content = content.push(typography(format!("{p}%"), TypeScale::LabelMedium).style(muted));
    }
    Some(tooltip(content.push(glyph), hint).into())
}

/// What a machine's card should warn about, if anything: an error, an older agent, or a GPU taken outside fleet.
fn attention(m: &MachineView) -> Option<Element<'_, Message>> {
    if let Some(e) = &m.error {
        return Some(
            typography(e.clone(), TypeScale::BodySmall)
                .style(colored(|t| t.colors.error))
                .into(),
        );
    }
    let hello = m.hello.as_ref()?;
    if hello.protocol != crate::proto::PROTOCOL {
        return Some(
            typography(crate::proto::describe(hello.protocol), TypeScale::BodySmall)
                .style(colored(|t| t.colors.tertiary))
                .into(),
        );
    }
    let busy = m.status.as_ref()?.gpu_busy_external.as_ref()?;
    Some(
        typography(
            format!("GPU busy outside fleet: {busy}"),
            TypeScale::BodySmall,
        )
        .style(colored(|t| t.colors.tertiary))
        .into(),
    )
}

/// The names of the jobs running on a machine.
fn running<'a>(m: &'a MachineView, jobs: &'a [QueuedJob]) -> Vec<(&'a str, &'a str)> {
    m.status
        .iter()
        .flat_map(|s| &s.running_jobs)
        .map(|id| {
            let name = jobs
                .iter()
                .find(|j| &j.id == id)
                .map_or(id.as_str(), |j| j.name.as_str());
            (id.as_str(), name)
        })
        .collect()
}

/// The share of fleet's thread slots its jobs hold, as a bar.
fn threads<'a>(m: &MachineView, theme: &Theme) -> Option<Element<'a, Message>> {
    let h = m.hello.as_ref().filter(|h| m.online && h.cpu_slots > 0)?;
    let used = m.threads_used.min(h.cpu_slots) as f32 / h.cpu_slots as f32;
    Some(
        row![
            container(
                linear_progress(used)
                    .colors([theme.colors.secondary])
                    .track_color(theme.colors.secondary_container)
            )
            .width(Length::Fill),
            typography(
                format!("{} / {} threads", m.threads_used, h.cpu_slots),
                TypeScale::LabelMedium
            )
            .style(muted),
        ]
        .spacing(10)
        .align_y(Alignment::Center)
        .into(),
    )
}

fn machine_card<'a>(
    m: &'a MachineView,
    jobs: &'a [QueuedJob],
    theme: &Theme,
    now: u64,
) -> Element<'a, Message> {
    let mut top = row![
        connection(m, now),
        typography(m.name.clone(), TypeScale::TitleMedium),
        Space::new().width(Length::Fill),
    ]
    .spacing(8)
    .align_y(Alignment::Center);
    if let Some(power) = power(m) {
        top = top.push(power);
    }
    let mut lines = column![top].spacing(10);
    if let Some(bar) = threads(m, theme) {
        lines = lines.push(bar);
    }
    if m.online {
        let names: Vec<&str> = running(m, jobs).into_iter().map(|(_, name)| name).collect();
        lines = lines.push(if names.is_empty() {
            typography("Idle", TypeScale::BodyMedium).style(muted)
        } else {
            typography(names.join(" · "), TypeScale::BodyMedium)
        });
    } else if let Some(at) = m.last_seen {
        lines = lines.push(
            typography(
                format!("Offline, last seen {}", ago(at, now)),
                TypeScale::BodyMedium,
            )
            .style(muted),
        );
    }
    if let Some(warning) = attention(m) {
        lines = lines.push(warning);
    }
    clickable(lines, Message::ShowMachine(m.name.clone()))
}

/// Label and value rows for a dialog.
fn fields<'a>(rows: Vec<(&'static str, String)>) -> Element<'a, Message> {
    column(rows.into_iter().map(|(label, value)| {
        row![
            typography(label, TypeScale::BodyMedium)
                .style(muted)
                .width(140),
            typography(value, TypeScale::BodyMedium).width(Length::Fill),
        ]
        .spacing(12)
        .into()
    }))
    .spacing(6)
    .into()
}

fn gigabytes(bytes: u64) -> String {
    format!("{:.0} GB", bytes as f64 / 1e9)
}

fn machine_dialog<'a>(
    m: &'a MachineView,
    jobs: &'a [QueuedJob],
    theme: &Theme,
    now: u64,
) -> dialog::Dialog<'a, Message> {
    let mut top = row![
        connection(m, now),
        typography(m.name.clone(), TypeScale::HeadlineSmall),
        Space::new().width(Length::Fill),
    ]
    .spacing(12)
    .align_y(Alignment::Center);
    if let Some(power) = power(m) {
        top = top.push(power);
    }
    let mut body = column![top].spacing(16);
    if let Some(bar) = threads(m, theme) {
        body = body.push(bar);
    }
    let mut rows = vec![(
        "Status",
        if m.online { "online" } else { "offline" }.to_string(),
    )];
    for p in &m.paths {
        rows.push((
            if p.selected { "Path" } else { "Other path" },
            format!("{} to {}, {:.0} ms", p.kind, p.remote, p.rtt_ms),
        ));
    }
    if let Some(at) = m.last_seen {
        rows.push(("Last seen", ago(at, now)));
    }
    if let Some(h) = &m.hello {
        rows.push(("Host", format!("{}, {}", h.hostname, h.os)));
        rows.push(("CPU", format!("{}, {} cores", h.cpu, h.cores)));
        rows.push(("Memory", gigabytes(h.memory_bytes)));
        if let Some(gpu) = &h.gpu {
            rows.push(("GPU", gpu.clone()));
        }
        rows.push((
            "Threads",
            format!(
                "{} of {} in use by fleet's jobs",
                m.threads_used, h.cpu_slots
            ),
        ));
        rows.push((
            "fleet",
            format!("version {}, protocol {}", h.agent_version, h.protocol),
        ));
        if !h.tags.is_empty() {
            rows.push(("Tags", h.tags.join(", ")));
        }
        if let Some(minutes) = h.sleep_minutes {
            rows.push((
                "Idle sleep",
                if minutes == 0 {
                    "never".to_string()
                } else {
                    format!("after {minutes} min")
                },
            ));
        }
    }
    if let Some(s) = &m.status {
        rows.push((
            "Power",
            match (s.on_ac_power, s.battery_percent) {
                (Some(true), Some(p)) => format!("AC, battery {p}%"),
                (Some(true), None) => "AC, no battery".into(),
                (Some(false), Some(p)) => format!("battery, {p}%"),
                _ => "unknown".into(),
            },
        ));
        if let Some(l) = s.load {
            rows.push((
                "Load",
                format!("{:.1}, {:.1}, {:.1} (1, 5, 15 min)", l[0], l[1], l[2]),
            ));
        }
        if let Some(b) = s.disk_free_bytes {
            rows.push(("Disk free", gigabytes(b)));
        }
        if let Some(u) = s.uptime_seconds {
            rows.push(("Up", duration(u as f64)));
        }
        if let Some(p) = &s.gpu_busy_external {
            rows.push(("GPU busy", format!("outside fleet: {p}")));
        }
    }
    if let Some(e) = &m.error {
        rows.push(("Problem", e.clone()));
    }
    body = body.push(fields(rows));
    let running = running(m, jobs);
    if !running.is_empty() {
        let mut list = column![section("Running")].spacing(4);
        for (id, name) in running {
            list = list.push(
                button(name.to_string())
                    .size(ButtonSize::ExtraSmall)
                    .variant(ButtonVariant::Text)
                    .on_press(Message::ShowJob(id.to_string())),
            );
        }
        body = body.push(list);
    }
    if let Some(warning) = attention(m) {
        body = body.push(warning);
    }
    dialog(body).width(560.0)
}

/// A job's state as a tonal label: primary for work under way, tertiary for work held, error for work lost.
fn state_label(state: QState) -> Element<'static, Message> {
    let label = match state {
        QState::Queued => "queued",
        QState::Starting => "starting",
        QState::Running => "running",
        QState::Paused => "paused",
        QState::Suspending => "suspending",
        QState::Suspended => "suspended",
        QState::Succeeded => "succeeded",
        QState::Failed => "failed",
        QState::Cancelled => "cancelled",
        QState::TimedOut => "timed out",
        QState::Lost => "lost",
    };
    container(typography(label, TypeScale::LabelMedium))
        .padding([2, 10])
        .style(move |t: &Theme| {
            let c = t.colors;
            let (background, foreground) = match state {
                QState::Starting | QState::Running => (c.primary_container, c.on_primary_container),
                QState::Paused | QState::Suspending | QState::Suspended => {
                    (c.tertiary_container, c.on_tertiary_container)
                }
                QState::Succeeded => (c.secondary_container, c.on_secondary_container),
                QState::Failed | QState::Lost => (c.error_container, c.on_error_container),
                QState::Queued | QState::Cancelled | QState::TimedOut => {
                    (c.surface_container_highest, c.on_surface_variant)
                }
            };
            container::Style {
                background: Some(background.into()),
                text_color: Some(foreground),
                border: Border {
                    radius: 8.0.into(),
                    ..Border::default()
                },
                ..container::Style::default()
            }
        })
        .into()
}

/// The job's actions as one Expressive button group: the pressed button grows and its neighbours give way. A card
/// holds the everyday ones (logs, pause or continue, resume); its dialog adds suspending and cancelling.
fn actions(j: &QueuedJob, all: bool) -> Option<Element<'_, Message>> {
    let small = |label: &'static str| button(label).size(ButtonSize::ExtraSmall);
    let mut buttons = Vec::new();
    if j.machine.is_some() {
        buttons.push(
            small("Logs")
                .variant(ButtonVariant::Tonal)
                .on_press(Message::OpenLog(j.id.clone())),
        );
    }
    match j.state {
        QState::Running if j.vm.is_none() => buttons.push(
            small("Pause")
                .variant(ButtonVariant::Tonal)
                .on_press(Message::Pause(j.id.clone(), true)),
        ),
        QState::Paused => {
            buttons.push(small("Continue").on_press(Message::Pause(j.id.clone(), false)))
        }
        QState::Suspended if j.continued_by.is_none() => {
            buttons.push(small("Resume").on_press(Message::Resume(j.id.clone())))
        }
        _ => {}
    }
    if all && j.suspendable && matches!(j.state, QState::Running | QState::Paused) {
        buttons.push(
            small("Suspend…")
                .variant(ButtonVariant::Tonal)
                .on_press(Message::ShowPlan(j.id.clone())),
        );
    }
    if all && !j.state.finished() {
        buttons.push(
            danger(small("Cancel").variant(ButtonVariant::Tonal))
                .on_press(Message::Cancel(j.id.clone())),
        );
    }
    if buttons.is_empty() {
        return None;
    }
    let width = 108.0 * buttons.len() as f32;
    Some(container(button_group(buttons)).width(width).into())
}

/// The job's measured progress (the first probe with a total), as a fraction.
fn fraction(j: &QueuedJob) -> Option<f32> {
    j.progress.as_ref()?.iter().find_map(|p| match &p.reading {
        Reading::Number {
            value,
            total: Some(total),
            ..
        } if *total > 0.0 => Some((*value / *total).clamp(0.0, 1.0) as f32),
        _ => None,
    })
}

/// A wavy bar while the job works, a flat one while it is held or done, the loading indicator while it starts.
fn progress(j: &QueuedJob, theme: &Theme) -> Option<Element<'static, Message>> {
    let c = theme.colors;
    if !j.state.finished()
        && let Some(vm) = &j.vm_status
        && vm.phase != crate::vm_job::Phase::Executing
    {
        return Some(
            row![
                loading_indicator().size(32.0),
                typography(vm.phase.label(), TypeScale::BodyMedium).style(muted)
            ]
            .spacing(8)
            .align_y(Alignment::Center)
            .into(),
        );
    }
    Some(match (j.state, fraction(j)) {
        (QState::Starting, _) => row![
            loading_indicator().size(32.0),
            typography(
                "Starting: sending inputs and launching",
                TypeScale::BodyMedium
            )
            .style(muted),
        ]
        .spacing(8)
        .align_y(Alignment::Center)
        .into(),
        (QState::Running | QState::Suspending, Some(f)) => linear_progress(f).wavy(true).into(),
        (QState::Running | QState::Suspending, None) => {
            linear_progress(0.0).wavy(true).indeterminate(true).into()
        }
        (QState::Paused | QState::Suspended, Some(f)) => linear_progress(f)
            .colors([c.tertiary])
            .track_color(c.tertiary_container)
            .into(),
        (_, Some(f)) => linear_progress(f)
            .colors([c.outline])
            .track_color(c.surface_container_highest)
            .into(),
        (_, None) => return None,
    })
}

/// A probe's number, with as many decimals as its size warrants.
fn number(v: f64) -> String {
    match v.abs() {
        a if v.fract() == 0.0 && a < 1e9 => format!("{v:.0}"),
        a if a >= 100.0 => format!("{v:.1}"),
        a if a >= 1.0 => format!("{v:.2}"),
        _ => format!("{v:.3}"),
    }
}

/// One figure, large, over a short label.
fn tile<'a>(value: String, label: String) -> Element<'a, Message> {
    tile_box(
        column![
            typography(value, TypeScale::TitleLarge),
            typography(label, TypeScale::LabelMedium).style(muted),
        ]
        .spacing(2),
    )
}

fn tile_box<'a>(content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
    container(content)
        .padding([8, 14])
        .style(|t: &Theme| container::Style {
            background: Some(t.colors.surface_container_highest.into()),
            border: Border {
                radius: 12.0.into(),
                ..Border::default()
            },
            ..container::Style::default()
        })
        .into()
}

/// A series' latest value, with a sparkline once it has two points.
fn series_tile<'a>(name: &str, values: &[f64]) -> Element<'a, Message> {
    let label = match values.len() {
        0 => format!("{name}, no points yet"),
        1 => format!("{name}, 1 point"),
        n => format!("{name}, {n} points"),
    };
    let value = values
        .last()
        .map_or_else(|| "–".to_string(), |v| number(*v));
    let mut content = row![
        column![
            typography(value, TypeScale::TitleLarge),
            typography(label, TypeScale::LabelMedium).style(muted),
        ]
        .spacing(2)
    ]
    .spacing(14)
    .align_y(Alignment::Center);
    if values.len() >= 2 {
        content = content.push(canvas(Spark(values.to_vec())).width(120).height(36));
    }
    tile_box(content)
}

/// The job's probes as tiles: a measure's share done (with its rate and time left while the job works), each series'
/// latest value and sparkline, counts and text.
fn tiles(j: &QueuedJob, now: u64) -> Option<Element<'_, Message>> {
    let values = j.progress.as_ref()?;
    let working = matches!(j.state, QState::Running | QState::Suspending)
        && j.vm_status
            .as_ref()
            .is_none_or(|v| v.phase == crate::vm_job::Phase::Executing);
    let mut tiles = Vec::new();
    for p in values {
        match &p.reading {
            Reading::Number {
                value,
                total,
                per_hour,
                eta_seconds,
            } => {
                tiles.push(match total {
                    Some(t) if *t > 0.0 => tile(
                        format!("{:.0}%", 100.0 * value / t),
                        format!("{} {} of {}", p.name, number(*value), number(*t)),
                    ),
                    _ => tile(number(*value), p.name.clone()),
                });
                if !working {
                    continue;
                }
                if let Some(rate) = per_hour {
                    tiles.push(tile(
                        if *rate >= 10.0 {
                            format!("{rate:.0}")
                        } else {
                            format!("{rate:.1}")
                        },
                        "per hour".into(),
                    ));
                }
                if let Some(eta) = eta_seconds {
                    tiles.push(tile(
                        duration(*eta),
                        format!("left, done {}", clock(now + *eta as u64, now)),
                    ));
                }
            }
            Reading::Series { values } => tiles.push(series_tile(&p.name, values)),
            Reading::Count { count } => tiles.push(tile(count.to_string(), p.name.clone())),
            Reading::Text { text } => tiles.push(tile(text.clone(), p.name.clone())),
        }
    }
    (!tiles.is_empty()).then(|| row(tiles).spacing(8).into())
}

/// How a job ended, in words.
fn ending(e: &Exit) -> String {
    match e.reason {
        EndReason::Exited => match (e.code, e.signal) {
            (Some(0), _) => "exited cleanly".into(),
            (Some(c), _) => format!("exited with code {c}"),
            (None, Some(s)) => format!("killed by signal {s}"),
            (None, None) => "exited".into(),
        },
        EndReason::Cancelled => "cancelled".into(),
        EndReason::TimeLimit => "stopped at its time limit".into(),
        EndReason::StartFailed => "could not start".into(),
        EndReason::Suspended => "suspended".into(),
    }
}

/// One line for what the bar and tiles do not say: why a job waits, how it ended, where a suspended one stopped.
fn note(j: &QueuedJob, now: u64) -> Option<Element<'_, Message>> {
    let color: fn(&Theme) -> Color = match j.state {
        QState::Queued => |t| t.colors.tertiary,
        QState::Failed | QState::Lost => |t| t.colors.error,
        _ if j.error.is_some() => |t| t.colors.error,
        _ => |t| t.colors.on_surface_variant,
    };
    let line = if let Some(stop) = &j.stopping {
        format!(
            "Stopping ({:?}); force termination in {} s",
            stop.reason,
            stop.deadline.saturating_sub(now)
        )
    } else if let Some(e) = &j.error {
        e.clone()
    } else {
        match j.state {
            QState::Queued if !j.after.is_empty() => {
                format!("waiting for {} other job(s) to finish", j.after.len())
            }
            QState::Queued => format!("queued {}", ago(j.submitted_at, now)),
            QState::Suspended => match (&j.continued_by, &j.checkpoint) {
                (Some(next), _) => format!("continued as {next}"),
                (None, Some(c)) => format!("stopped at {}; Resume continues from it", c.path),
                (None, None) => "stopped before its first checkpoint".into(),
            },
            _ => {
                let e = j.exit.as_ref().filter(|_| j.state.finished())?;
                format!("{} {}", ending(e), ago(e.finished_at, now))
            }
        }
    };
    Some(
        typography(line, TypeScale::BodyMedium)
            .style(colored(color))
            .into(),
    )
}

fn job_card<'a>(j: &'a QueuedJob, theme: &Theme, now: u64) -> Element<'a, Message> {
    let mut top = row![
        state_label(j.state),
        typography(j.name.clone(), TypeScale::TitleMedium),
    ]
    .spacing(10)
    .align_y(Alignment::Center);
    if let Some(vm) = &j.vm_status {
        top = top.push(typography(vm.phase.label(), TypeScale::LabelMedium).style(muted));
    }
    if let Some(machine) = &j.machine {
        top = top.push(typography(format!("on {machine}"), TypeScale::BodyMedium).style(muted));
    }
    top = top.push(Space::new().width(Length::Fill));
    if let Some(actions) = actions(j, false) {
        top = top.push(actions);
    }
    let mut lines = column![top].spacing(12);
    if let Some(bar) = progress(j, theme) {
        lines = lines.push(bar);
    }
    if let Some(tiles) = tiles(j, now) {
        lines = lines.push(tiles);
    }
    if let Some(note) = note(j, now) {
        lines = lines.push(note);
    }
    clickable(lines, Message::ShowJob(j.id.clone()))
}

fn placement(p: &Placement) -> String {
    let mut parts = Vec::new();
    if let Some(m) = &p.machine {
        parts.push(format!("only on {m}"));
    }
    if !p.tags.is_empty() {
        parts.push(format!("needs {}", p.tags.join(", ")));
    }
    if p.gpu {
        parts.push("the GPU to itself".into());
    }
    if p.ac_power {
        parts.push("only on AC power".into());
    }
    if parts.is_empty() {
        "any machine with room".into()
    } else {
        parts.join("; ")
    }
}

/// A time as a clock reading and an age.
fn stamp(at: u64, now: u64) -> String {
    format!("{} ({})", clock(at, now), ago(at, now))
}

fn job_dialog<'a>(j: &'a QueuedJob, theme: &Theme, now: u64) -> dialog::Dialog<'a, Message> {
    let mut title = row![
        state_label(j.state),
        typography(j.name.clone(), TypeScale::HeadlineSmall),
    ]
    .spacing(12)
    .align_y(Alignment::Center);
    if let Some(machine) = &j.machine {
        title = title.push(typography(format!("on {machine}"), TypeScale::BodyLarge).style(muted));
    }
    let mut body = column![title].spacing(16);
    if let Some(bar) = progress(j, theme) {
        body = body.push(bar);
    }
    if let Some(tiles) = tiles(j, now) {
        body = body.push(tiles);
    }
    for p in j.progress.iter().flatten() {
        if let Reading::Series { values } = &p.reading
            && values.len() >= 2
        {
            let (lo, hi) = range(values);
            body = body.push(
                column![
                    row![
                        typography(p.name.clone(), TypeScale::TitleSmall),
                        typography(
                            format!(
                                "{} points, latest {}, from {} to {}",
                                values.len(),
                                number(values[values.len() - 1]),
                                number(lo),
                                number(hi)
                            ),
                            TypeScale::BodySmall
                        )
                        .style(muted),
                    ]
                    .spacing(12)
                    .align_y(Alignment::Center),
                    canvas(Chart(values.clone()))
                        .width(Length::Fill)
                        .height(140),
                ]
                .spacing(6),
            );
        }
    }
    let mut rows = vec![("ID", j.id.clone()), ("Placement", placement(&j.placement))];
    if let Some(vm) = &j.vm_status {
        rows.push(("VM", vm.instance.clone()));
        rows.push(("Phase", vm.phase.label().into()));
        if let Some(error) = &vm.error {
            rows.push(("VM problem", error.clone()));
        }
    }
    if j.priority != 0 {
        rows.push(("Priority", j.priority.to_string()));
    }
    if !j.after.is_empty() {
        rows.push(("Waits for", j.after.join(", ")));
    }
    rows.push(("Submitted", stamp(j.submitted_at, now)));
    if let Some(at) = j.assigned_at {
        rows.push(("Started", stamp(at, now)));
    }
    if let Some(e) = &j.exit {
        rows.push((
            "Ended",
            format!("{}, {}", ending(e), stamp(e.finished_at, now)),
        ));
    }
    if j.segment > 1 {
        rows.push((
            "Segment",
            format!(
                "{}{}",
                j.segment,
                j.parent
                    .as_ref()
                    .map(|p| format!(", continuing {p}"))
                    .unwrap_or_default()
            ),
        ));
    }
    if let Some(next) = &j.continued_by {
        rows.push(("Continued as", next.clone()));
    }
    if let Some(on) = &j.resume_on {
        rows.push((
            "Resumes on",
            if on == "*" {
                "any machine".into()
            } else {
                on.clone()
            },
        ));
    }
    if let Some(other) = &j.preempted_for {
        rows.push(("Making room for", other.clone()));
    }
    rows.push((
        "Suspendable",
        if j.suspendable {
            "yes: it says how to resume"
        } else {
            "no"
        }
        .into(),
    ));
    if let Some(c) = &j.checkpoint {
        rows.push((
            "Checkpoint",
            format!(
                "{}{}, written {}",
                c.path,
                c.position
                    .map(|p| format!(" at {}", number(p)))
                    .unwrap_or_default(),
                ago(c.written_at, now)
            ),
        ));
    }
    if let Some(dest) = &j.dest {
        rows.push((
            "Results",
            format!(
                "{}: {} file(s), {:.1} MB fetched",
                dest.display(),
                j.fetched_files,
                j.fetched_bytes as f64 / 1e6
            ),
        ));
    }
    if let Some(at) = j.progress_at {
        rows.push(("Probes read", ago(at, now)));
    }
    if let Some(e) = &j.error {
        rows.push(("Problem", e.clone()));
    }
    body = body.push(fields(rows));
    dialog(body).width(760.0)
}

/// How long before `now` the time `at` was: "12 s ago".
fn ago(at: u64, now: u64) -> String {
    duration(now.saturating_sub(at) as f64) + " ago"
}

/// A local clock reading of `at`: "14:05" on `now`'s day, "Sat 14:05" within the week, "12 Oct 14:05" beyond.
fn clock(at: u64, now: u64) -> String {
    fn local(t: u64) -> Option<libc::tm> {
        let t = t as libc::time_t;
        // SAFETY: `localtime_r` writes only the `tm` it is handed, and a zeroed `tm` is a valid value.
        let mut tm: libc::tm = unsafe { std::mem::zeroed() };
        (!unsafe { libc::localtime_r(&t, &mut tm) }.is_null()).then_some(tm)
    }
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let (Some(tm), Some(today)) = (local(at), local(now)) else {
        return String::new();
    };
    let time = format!("{:02}:{:02}", tm.tm_hour, tm.tm_min);
    if (tm.tm_year, tm.tm_yday) == (today.tm_year, today.tm_yday) {
        time
    } else if now.abs_diff(at) < 6 * 86_400 {
        format!("{} {time}", DAYS[tm.tm_wday.rem_euclid(7) as usize])
    } else {
        format!(
            "{} {} {time}",
            tm.tm_mday,
            MONTHS[tm.tm_mon.rem_euclid(12) as usize]
        )
    }
}

fn log_panel(log: &LogView) -> Element<'_, Message> {
    let header = row![
        typography(format!("Logs · {}", log.job), TypeScale::TitleSmall),
        Space::new().width(Length::Fill),
        segmented_buttons(
            [Segment::new(false, "stdout"), Segment::new(true, "stderr")],
            SegmentSelection::Single(Some(log.stderr)),
        )
        .on_change(|s| Message::ShowStderr(s.contains(&true))),
        button("Close")
            .variant(ButtonVariant::Text)
            .on_press(Message::CloseLog),
    ]
    .spacing(8)
    .align_y(Alignment::Center);
    let body = scrollable(
        text(if log.text.is_empty() {
            "(empty)".to_string()
        } else {
            log.text.clone()
        })
        .size(12)
        .font(iced::Font::MONOSPACE),
    )
    .anchor_bottom()
    .height(240)
    .width(Length::Fill);
    panel(column![header, body].spacing(8))
}

fn range(values: &[f64]) -> (f64, f64) {
    values
        .iter()
        .fold((f64::MAX, f64::MIN), |(lo, hi), &v| (lo.min(v), hi.max(v)))
}

/// A battery glyph (or a bolt alone, for a machine on AC without a battery).
struct Power {
    ac: bool,
    percent: Option<u8>,
}

impl<Message> canvas::Program<Message, Theme> for Power {
    type State = ();

    fn draw(
        &self,
        _: &(),
        renderer: &Renderer,
        theme: &Theme,
        bounds: Rectangle,
        _: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let c = theme.colors;
        let (w, h) = (bounds.width, bounds.height);
        // A lightning bolt in a unit box, scaled into `area`.
        let bolt = |area: Rectangle| {
            const POINTS: [(f32, f32); 6] = [
                (0.62, 0.0),
                (0.1, 0.58),
                (0.46, 0.58),
                (0.38, 1.0),
                (0.9, 0.42),
                (0.54, 0.42),
            ];
            canvas::Path::new(|b| {
                for (i, (x, y)) in POINTS.into_iter().enumerate() {
                    let p = iced::Point::new(area.x + x * area.width, area.y + y * area.height);
                    if i == 0 { b.move_to(p) } else { b.line_to(p) }
                }
                b.close();
            })
        };
        let Some(percent) = self.percent else {
            if self.ac {
                frame.fill(
                    &bolt(Rectangle::new(iced::Point::ORIGIN, bounds.size())),
                    c.on_surface_variant,
                );
            }
            return vec![frame.into_geometry()];
        };
        let outline = Color {
            a: 0.55,
            ..c.on_surface
        };
        let body = Rectangle::new(
            iced::Point::new(0.5, 0.5),
            iced::Size::new(w - 4.0, h - 1.0),
        );
        frame.stroke(
            &canvas::Path::rounded_rectangle(body.position(), body.size(), 3.5.into()),
            canvas::Stroke::default()
                .with_width(1.0)
                .with_color(outline),
        );
        frame.fill(
            &canvas::Path::rounded_rectangle(
                iced::Point::new(w - 2.5, h / 2.0 - 2.5),
                iced::Size::new(2.0, 5.0),
                1.0.into(),
            ),
            outline,
        );
        let inner = body.shrink(2.0);
        let fill = if self.ac {
            success(theme)
        } else if percent <= 20 {
            c.error
        } else {
            c.on_surface
        };
        frame.fill(
            &canvas::Path::rounded_rectangle(
                inner.position(),
                iced::Size::new(
                    inner.width * f32::from(percent.min(100)) / 100.0,
                    inner.height,
                ),
                2.0.into(),
            ),
            fill,
        );
        if self.ac {
            // The bolt is cut out of the fill by a stroke in the card's color, as macOS draws it.
            let size = h + 1.0;
            let shape = bolt(Rectangle::new(
                iced::Point::new(body.center_x() - size * 0.3, -0.5),
                iced::Size::new(size * 0.6, size),
            ));
            frame.stroke(
                &shape,
                canvas::Stroke::default()
                    .with_width(2.0)
                    .with_color(c.surface_container_low),
            );
            frame.fill(&shape, c.on_surface);
        }
        vec![frame.into_geometry()]
    }
}

/// A sparkline of recent values.
struct Spark(Vec<f64>);

impl<Message> canvas::Program<Message, Theme> for Spark {
    type State = ();

    fn draw(
        &self,
        _: &(),
        renderer: &Renderer,
        theme: &Theme,
        bounds: Rectangle,
        _: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let values = &self.0;
        if values.len() >= 2 {
            let (lo, hi) = range(values);
            let span = if hi > lo { hi - lo } else { 1.0 };
            let (w, h) = (bounds.width, bounds.height - 2.0);
            let point = |i: usize, v: f64| {
                iced::Point::new(
                    i as f32 / (values.len() - 1) as f32 * w,
                    1.0 + h - ((v - lo) / span) as f32 * h,
                )
            };
            let path = canvas::Path::new(|b| {
                b.move_to(point(0, values[0]));
                for (i, &v) in values.iter().enumerate().skip(1) {
                    b.line_to(point(i, v));
                }
            });
            frame.stroke(
                &path,
                canvas::Stroke::default()
                    .with_width(1.5)
                    .with_color(theme.colors.primary),
            );
        }
        vec![frame.into_geometry()]
    }
}

/// A series drawn large for a job's dialog: its points joined, the highest and lowest marked by dotted lines.
struct Chart(Vec<f64>);

impl<Message> canvas::Program<Message, Theme> for Chart {
    type State = ();

    fn draw(
        &self,
        _: &(),
        renderer: &Renderer,
        theme: &Theme,
        bounds: Rectangle,
        _: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        let values = &self.0;
        let c = theme.colors;
        if values.len() >= 2 {
            let (lo, hi) = range(values);
            let span = if hi > lo { hi - lo } else { 1.0 };
            // Room on the left for the labels, and for the dots at the edges.
            let (left, top, right) = (56.0, 8.0, 6.0);
            let (w, h) = (bounds.width - left - right, bounds.height - 2.0 * top);
            let y = |v: f64| top + h - ((v - lo) / span) as f32 * h;
            let point = |i: usize, v: f64| {
                iced::Point::new(left + i as f32 / (values.len() - 1) as f32 * w, y(v))
            };
            for v in [lo, hi] {
                frame.stroke(
                    &canvas::Path::line(
                        iced::Point::new(left, y(v)),
                        iced::Point::new(left + w, y(v)),
                    ),
                    canvas::Stroke {
                        line_dash: canvas::LineDash {
                            segments: &[3.0, 4.0],
                            offset: 0,
                        },
                        ..canvas::Stroke::default()
                            .with_width(1.0)
                            .with_color(c.outline_variant)
                    },
                );
                frame.fill_text(canvas::Text {
                    content: number(v),
                    position: iced::Point::new(left - 8.0, y(v)),
                    color: c.on_surface_variant,
                    size: 12.0.into(),
                    align_x: iced::widget::text::Alignment::Right,
                    align_y: iced::alignment::Vertical::Center,
                    ..canvas::Text::default()
                });
            }
            let path = canvas::Path::new(|b| {
                b.move_to(point(0, values[0]));
                for (i, &v) in values.iter().enumerate().skip(1) {
                    b.line_to(point(i, v));
                }
            });
            frame.stroke(
                &path,
                canvas::Stroke::default()
                    .with_width(2.0)
                    .with_color(c.primary),
            );
            if values.len() <= 64 {
                for (i, &v) in values.iter().enumerate() {
                    frame.fill(&canvas::Path::circle(point(i, v), 2.5), c.primary);
                }
            }
            let last = values.len() - 1;
            frame.fill(
                &canvas::Path::circle(point(last, values[last]), 4.5),
                c.primary,
            );
        }
        vec![frame.into_geometry()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PathReport,
        probes::ProbeValue,
        proto::{Hello, PROTOCOL, Status},
    };

    /// The fixtures' wall clock, fixed so their drawings do not depend on when the tests run.
    const NOW: u64 = 1_791_500_000;

    fn machine(name: &str, online: bool, slots: u32, used: u32) -> MachineView {
        MachineView {
            name: name.into(),
            id: format!("{name}-id"),
            online,
            paths: if online {
                vec![PathReport {
                    kind: "direct".into(),
                    remote: "192.168.1.20:4433".into(),
                    selected: true,
                    rtt_ms: 2.0,
                }]
            } else {
                Vec::new()
            },
            hello: Some(Hello {
                agent_version: "0.1.0".into(),
                protocol: PROTOCOL,
                hostname: name.into(),
                os: "macos".into(),
                cpu: "Apple M4 Pro".into(),
                cores: slots + 2,
                memory_bytes: 48 << 30,
                gpu: Some("Apple M4 Pro (Metal)".into()),
                tags: vec!["gpu:metal".into()],
                sleep_minutes: Some(0),
                cpu_slots: slots,
            }),
            status: online.then(|| Status {
                load: Some([5.8, 5.5, 5.1]),
                on_ac_power: Some(true),
                disk_free_bytes: Some(412_000_000_000),
                running_jobs: vec!["a".into()],
                ..Status::default()
            }),
            last_seen: Some(NOW),
            error: (!online).then(|| "not reachable: no path to the machine".into()),
            threads_used: used,
        }
    }

    fn job(id: &str, name: &str, state: QState, progress: Option<Vec<ProbeValue>>) -> QueuedJob {
        QueuedJob {
            vm_status: None,
            vm: None,
            stopping: None,
            id: id.into(),
            name: name.into(),
            state,
            machine: (state != QState::Queued).then(|| "m4pro".into()),
            placement: Placement::default(),
            dest: Some(PathBuf::from("runs/x")),
            after: Vec::new(),
            priority: 0,
            submitted_at: NOW - 3600,
            assigned_at: Some(NOW - 3000),
            finished_at: None,
            exit: state.finished().then_some(Exit {
                code: Some(1),
                signal: None,
                reason: EndReason::Exited,
                finished_at: NOW,
            }),
            error: None,
            fetched_files: 12,
            fetched_bytes: 48_000_000,
            progress,
            progress_at: Some(NOW),
            segment: 1,
            parent: None,
            resume_on: None,
            preempted_for: None,
            continued_by: None,
            suspendable: state == QState::Running,
            checkpoint: None,
        }
    }

    fn iteration(value: f64, total: f64) -> Vec<ProbeValue> {
        vec![
            ProbeValue {
                name: "iteration".into(),
                reading: Reading::Number {
                    value,
                    total: Some(total),
                    per_hour: Some(14.0),
                    eta_seconds: Some((total - value) / 14.0 * 3600.0),
                },
            },
            ProbeValue {
                name: "validation score".into(),
                reading: Reading::Series {
                    values: vec![0.52, 0.55, 0.54, 0.58, 0.61, 0.6, 0.63],
                },
            },
        ]
    }

    /// A dashboard showing every way a job is drawn (measured and unmeasured running, paused, starting, queued with a
    /// reason, and finished) and every way a machine is: on AC with a battery, on battery through a relay, offline.
    fn app(dark: bool) -> App {
        let mut app = App::new(PathBuf::new());
        app.dark = dark;
        app.show_finished = true;
        app.now = NOW;
        app.refreshed_at = Some(NOW);
        let mut laptop = machine("m1pro", true, 6, 6);
        laptop.paths[0].kind = "relay".into();
        laptop.paths[0].rtt_ms = 48.0;
        let status = laptop.status.as_mut().unwrap();
        status.on_ac_power = Some(false);
        status.battery_percent = Some(64);
        let mut here = machine("m4pro", true, 12, 8);
        let status = here.status.as_mut().unwrap();
        status.battery_percent = Some(100);
        status.running_jobs = vec!["c".into(), "b".into()];
        app.machines = vec![laptop, here, machine("spare", false, 6, 0)];
        let mut queued = job("e", "evaluation pass", QState::Queued, None);
        queued.error = Some("waiting for room: m4pro's GPU is taken".into());
        let mut twin = job(
            "a",
            "rollout twin T",
            QState::Running,
            Some(iteration(300.0, 512.0)),
        );
        twin.machine = Some("m1pro".into());
        app.jobs = vec![
            twin,
            job("b", "pilot", QState::Running, None),
            job(
                "c",
                "K2 phase 1",
                QState::Paused,
                Some(iteration(1300.0, 2048.0)),
            ),
            job("d", "screen arm", QState::Starting, None),
            queued,
            job("f", "smoke", QState::Failed, Some(iteration(2.0, 2.0))),
        ];
        app
    }

    fn simulator(app: &App) -> iced_test::Simulator<'_, Message, Theme> {
        let settings = iced::Settings {
            default_font: iced_m3::fonts::REGULAR,
            ..iced::Settings::default()
        };
        iced_test::Simulator::with_size(settings, (1280.0, 840.0), app.view())
    }

    /// Saves a drawing as `<name>-<renderer>.png` (`wgpu` where a GPU is usable, else `tiny-skia`) in
    /// `FLEET_GUI_PREVIEW`'s folder, if set (an existing file must match).
    fn preview(app: &App, name: &str) {
        let snapshot = simulator(app).snapshot(&app.theme()).expect("drawn");
        if let Some(folder) = std::env::var_os("FLEET_GUI_PREVIEW") {
            assert!(
                snapshot
                    .matches_image(PathBuf::from(folder).join(name))
                    .unwrap()
            );
        }
    }

    /// Draws the dashboard in both appearances (`dashboard-light`, `dashboard-dark`).
    #[test]
    fn draws_the_dashboard() {
        preview(&app(false), "dashboard-light");
        preview(&app(true), "dashboard-dark");
    }

    /// Draws a job's dialog, a machine's dialog, and the suspend dialog over a job's dialog with a log panel open
    /// (`job-dialog`, `machine-dialog`, `suspend-dialog`).
    #[test]
    fn draws_the_dialogs() {
        let mut app = app(true);
        app.details = Some(Details::Job("a".into()));
        app.details_open = true;
        preview(&app, "job-dialog");

        app.details = Some(Details::Machine("m4pro".into()));
        preview(&app, "machine-dialog");

        app.details = Some(Details::Job("a".into()));
        app.plan = Some((
            "a".into(),
            Ok(crate::suspend::Plan {
                unit: Some("iteration".into()),
                current: Some(300.0),
                per_hour: Some(14.0),
                latest: Some(crate::suspend::Checkpoint {
                    path: "T/resume/iter-0256".into(),
                    position: Some(256.0),
                    written_at: NOW - 11_000,
                }),
                loss_units: Some(44.0),
                loss_wall_seconds: Some(11_000),
                loss_compute_seconds: Some(11_314.0),
                next_position: Some(384.0),
                next_eta_seconds: Some(21_600.0),
            }),
        ));
        app.plan_open = true;
        app.log = Some(LogView {
            job: "a".into(),
            stderr: true,
            text: "iteration 299: 4096 games, 279.4 s\niteration 300: 4096 games, 280.1 s\n".into(),
            next: 0,
            done: false,
            loading: false,
        });
        preview(&app, "suspend-dialog");
    }

    fn clicks(app: &App, targets: &[&str]) -> Vec<String> {
        let mut ui = simulator(app);
        for target in targets {
            ui.click(*target)
                .unwrap_or_else(|_| panic!("no {target:?}"));
        }
        ui.into_messages().map(|m| format!("{m:?}")).collect()
    }

    #[test]
    fn vm_job_phases_and_metrics_are_visible_without_pause_controls() {
        let mut app = app(false);
        app.jobs.truncate(1);
        app.jobs[0].vm = Some(crate::instances::Spec {
            image: "macos".into(),
            cpus: 2,
            memory_mb: 4096,
            disk_gb: 50,
            network: crate::instances::Network::Offline,
            gpu: crate::instances::Gpu::Exclusive,
        });
        app.jobs[0].suspendable = false;
        for (phase, name) in [
            (crate::vm_job::Phase::Preparing, "vm-preparing"),
            (crate::vm_job::Phase::Executing, "vm-metrics"),
            (crate::vm_job::Phase::Collecting, "vm-collecting"),
            (crate::vm_job::Phase::Deleting, "vm-deleting"),
        ] {
            app.jobs[0].vm_status = Some(crate::vm_job::Status {
                instance: "job-fixture".into(),
                phase,
                error: None,
                outcome: None,
            });
            app.details = Some(Details::Job(app.jobs[0].id.clone()));
            app.details_open = true;
            let mut ui = simulator(&app);
            assert!(ui.click("Pause").is_err());
            preview(&app, name);
        }
    }

    /// A card opens its dialog, and the buttons on it send their own actions instead.
    #[test]
    fn cards_open_their_dialogs_and_their_buttons_act() {
        let app = app(true);
        let messages = clicks(&app, &["K2 phase 1", "Continue", "m1pro"]);
        assert_eq!(
            messages,
            [
                r#"ShowJob("c")"#,
                r#"Pause("c", false)"#,
                r#"ShowMachine("m1pro")"#
            ],
            "{messages:?}"
        );
    }

    /// A job's dialog holds the rarer actions, and a machine's dialog leads to the jobs running there.
    #[test]
    fn dialogs_send_their_actions() {
        let mut app = app(true);
        app.details = Some(Details::Job("a".into()));
        app.details_open = true;
        let messages = clicks(&app, &["Suspend…", "Close"]);
        assert_eq!(
            messages,
            [r#"ShowPlan("a")"#, "HideDetails"],
            "{messages:?}"
        );

        app.details = Some(Details::Machine("m4pro".into()));
        let messages = clicks(&app, &["pilot"]);
        assert_eq!(messages, [r#"ShowJob("b")"#], "{messages:?}");
    }

    /// Every age on screen is read from the one clock in the app's state, so a new second moves them all at once.
    #[test]
    fn ages_advance_together_each_second() {
        let mut app = app(true);
        app.details = Some(Details::Machine("m4pro".into()));
        for second in [0, 1] {
            let _ = app.update(Message::Second(NOW + second));
            app.details_open = false;
            let mut ui = simulator(&app);
            ui.find(format!(
                "2 of 3 machines online · 2 running · 1 queued · updated {second} s ago"
            ))
            .expect("the header's age");
            ui.find(format!("Offline, last seen {second} s ago"))
                .expect("the offline machine's last contact");
            drop(ui);
            // An open dialog takes the page's place for selectors, so it is checked on its own.
            app.details_open = true;
            let mut ui = simulator(&app);
            ui.find(format!("{second} s ago"))
                .expect("the dialog's last contact");
        }
    }

    /// The clock wakes just after the next whole second, wherever in a second it starts.
    #[test]
    fn the_clock_wakes_just_after_each_second() {
        for millis in [10_000, 10_300, 10_999] {
            let since = Duration::from_millis(millis);
            let woken = since + until_next_second(since);
            assert_eq!(woken.as_secs(), since.as_secs() + 1, "{millis}");
            assert_eq!(woken.subsec_millis(), 5, "{millis}");
        }
    }
}
