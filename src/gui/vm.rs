use super::*;
use crate::control::vm::{Action, Connections, Route, Target};
use iced::widget::column;
#[derive(Debug, Clone)]
pub(super) enum Event {
    Close,
    Select(String),
    Field(usize, String),
    Exec,
    Upload,
    Download,
    Export,
    Connect,
    RemoveExport(String),
    RemoveRoute(usize),
    Fetch(String),
    Logs(String, bool),
    Release(String, u64),
    Stage(String),
    Output(u64, Result<Value, String>),
}
pub(super) struct Panel {
    pub selected: Option<String>,
    pub snapshot: Value,
    fields: [String; 10],
    output: String,
    output_epoch: u64,
}
impl Default for Panel {
    fn default() -> Self {
        Self {
            selected: None,
            snapshot: Value::Null,
            fields: [
                "/bin/echo".into(),
                "[\"hello\"]".into(),
                String::new(),
                "/tmp/application".into(),
                String::new(),
                "api".into(),
                "8080".into(),
                String::new(),
                String::new(),
                "18080".into(),
            ],
            output: String::new(),
            output_epoch: 0,
        }
    }
}
impl App {
    pub(super) fn vm_update(&mut self, e: Event) -> Task<Message> {
        match e {
            Event::Close => {
                self.vm.selected = None;
                self.vm.output_epoch += 1;
                return Task::none();
            }
            Event::Select(id) => {
                self.vm.output_epoch += 1;
                self.vm.selected = Some(id);
                self.vm.output.clear();
                return Task::none();
            }
            Event::Field(i, v) => {
                self.vm.fields[i] = v;
                return Task::none();
            }
            Event::Output(epoch, r) => {
                if epoch != self.vm.output_epoch {
                    return Task::none();
                }
                self.vm.output = match r {
                    Ok(v) => v["data"]
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| v.to_string()),
                    Err(e) => e,
                };
                return Task::none();
            }
            _ => (),
        }
        let request = (|| -> anyhow::Result<ApiRequest> {
            let instance = self
                .vm
                .selected
                .clone()
                .ok_or_else(|| anyhow::anyhow!("select a VM"))?;
            let revision = self
                .instances
                .iter()
                .find(|a| a["id"] == instance)
                .and_then(|a| a["revision"].as_u64())
                .ok_or_else(|| anyhow::anyhow!("VM unavailable"))?;
            let f = &self.vm.fields;
            let operation = id();
            let start = |spec| ApiRequest::VmManage {
                request_id: id(),
                action: Action::Start {
                    id: operation.clone(),
                    instance: instance.clone(),
                    expected_revision: revision,
                    spec,
                    retention_seconds: crate::control::vm::RETENTION,
                },
            };
            let network = &self.vm.snapshot["catalog"]["networks"][&instance];
            let mut config: Connections = if network.is_null() {
                Default::default()
            } else {
                serde_json::from_value(network["config"].clone())?
            };
            let config_revision = network["revision"].as_u64().unwrap_or(0);
            let address = |v: &str| -> anyhow::Result<std::net::SocketAddr> {
                Ok(format!("127.0.0.1:{}", v.parse::<u16>()?).parse()?)
            };
            match e {
                Event::Exec => {
                    return Ok(start(crate::instances::guest::Spec::Exec {
                        argv: std::iter::once(f[0].clone())
                            .chain(serde_json::from_str::<Vec<String>>(&f[1])?)
                            .collect(),
                        cwd: None,
                        env: Default::default(),
                        timeout_seconds: 300,
                    }));
                }
                Event::Upload => {
                    return Ok(ApiRequest::VmUpload {
                        request_id: id(),
                        id: operation,
                        instance,
                        expected_revision: revision,
                        source: f[2].clone().into(),
                        path: f[3].clone(),
                        executable: false,
                        retention_seconds: crate::control::vm::RETENTION,
                    });
                }
                Event::Download => {
                    return Ok(start(crate::instances::guest::Spec::Download {
                        path: f[3].clone(),
                        max_bytes: 1 << 30,
                        version: None,
                    }));
                }
                Event::Fetch(op) => {
                    return Ok(ApiRequest::VmFetch {
                        id: op,
                        destination: f[4].clone().into(),
                    });
                }
                Event::Logs(op, stderr) => {
                    return Ok(ApiRequest::VmLogs {
                        id: op,
                        stderr,
                        from: 0,
                    });
                }
                Event::Release(op, revision) => {
                    return Ok(ApiRequest::VmManage {
                        request_id: id(),
                        action: Action::Release {
                            id: op,
                            expected_revision: revision,
                        },
                    });
                }
                Event::Stage(op) => return Ok(ApiRequest::VmStage { id: op }),
                Event::Export => {
                    config.exports.insert(f[5].clone(), address(&f[6])?);
                }
                Event::Connect => {
                    config.routes.push(Route {
                        service: if f[7].is_empty() {
                            Target::Host {
                                service: f[8].clone(),
                            }
                        } else {
                            Target::Vm {
                                instance: f[7].clone(),
                                service: f[8].clone(),
                            }
                        },
                        listen: address(&f[9])?,
                    });
                }
                Event::RemoveExport(name) => {
                    config.exports.remove(&name);
                }
                Event::RemoveRoute(i) => {
                    anyhow::ensure!(i < config.routes.len(), "connection changed; refresh");
                    config.routes.remove(i);
                }
                _ => unreachable!(),
            }
            Ok(ApiRequest::VmManage {
                request_id: id(),
                action: Action::Connections {
                    instance,
                    expected_revision: config_revision,
                    config,
                },
            })
        })();
        match request {
            Ok(api @ ApiRequest::VmLogs { .. }) => {
                let home = self.home.clone();
                self.vm.output_epoch += 1;
                let epoch = self.vm.output_epoch;
                Task::perform(
                    async move { client::call(&home, api).await.map_err(|e| format!("{e:#}")) },
                    move |r| Message::Vm(Event::Output(epoch, r)),
                )
            }
            Ok(api) => self.act(api),
            Err(e) => {
                self.error = Some(e.to_string());
                Task::none()
            }
        }
    }
    pub(super) fn vm_panel(&self) -> Element<'_, Message> {
        let Some(id) = self.vm.selected.as_ref() else {
            return column![].into();
        };
        let field = |index: usize, label| {
            text_field(label, self.vm.fields[index].as_str())
                .on_input(move |v| Message::Vm(Event::Field(index, v)))
                .width(Length::Fill)
        };
        let action = |label, e| {
            button(label)
                .variant(ButtonVariant::Tonal)
                .size(ButtonSize::Small)
                .disabled(self.acting)
                .on_press(Message::Vm(e))
        };
        let config = &self.vm.snapshot["catalog"]["networks"][id];
        let binding = &self.vm.snapshot["catalog"]["bindings"][id];
        let mut page = column![
            typography(format!("Manage {id}"), TypeScale::TitleLarge),
            typography(
                format!(
                    "Connections revision {} · identity {}",
                    config["revision"].as_u64().unwrap_or(0),
                    if config.is_null() {
                        "not configured"
                    } else if binding.is_null() {
                        "awaiting worker"
                    } else {
                        "bound on host"
                    }
                ),
                TypeScale::BodySmall
            )
        ]
        .spacing(8);
        if let Some(error) = &self.error {
            page = page.push(typography(error, TypeScale::BodyMedium));
        }
        if let Some(notice) = &self.notice {
            page = page.push(typography(notice, TypeScale::BodyMedium));
        }
        let adapter = crate::control::vm::adapter_id(id);
        let mut enforcement = self.vm.snapshot["enforcement"].clone();
        if let Some(rows) = enforcement["adapters"].as_array_mut() {
            rows.retain(|r| r["adapter"] == adapter);
        }
        for line in tunnel_lines(
            &enforcement,
            self.tunnels_checked.elapsed(),
            self.error.is_some(),
        ) {
            page = page.push(typography(line.replace(&adapter, id), TypeScale::BodySmall));
        }
        page = page.push(typography("Private services", TypeScale::TitleMedium));
        page = page.push(
            row![
                field(5, "Service name"),
                field(6, "Guest TCP port"),
                action("Expose service", Event::Export)
            ]
            .spacing(8),
        );
        page = page.push(
            row![
                field(7, "Target VM (blank for host service)"),
                field(8, "Target service"),
                field(9, "Local guest port"),
                action("Allow connection", Event::Connect)
            ]
            .spacing(8),
        );
        if let Ok(c) = serde_json::from_value::<Connections>(config["config"].clone()) {
            for (name, target) in c.exports {
                page = page.push(row![
                    typography(format!("Expose {name} at {target}"), TypeScale::BodyMedium)
                        .width(Length::Fill),
                    action("Remove", Event::RemoveExport(name))
                ]);
            }
            for (i, r) in c.routes.into_iter().enumerate() {
                if let Target::Vm { instance, service } = &r.service {
                    let catalog = &self.vm.snapshot["catalog"];
                    if catalog["bindings"][instance].is_null() {
                        page = page.push(typography(
                            format!("Waiting for {instance}'s worker to bind {service}."),
                            TypeScale::BodySmall,
                        ));
                    } else if catalog["networks"][instance]["config"]["exports"][service].is_null()
                    {
                        page = page.push(typography(
                            format!(
                                "{instance} no longer exposes {service}; this route is unavailable."
                            ),
                            TypeScale::BodySmall,
                        ));
                    }
                }
                page = page.push(row![
                    typography(
                        format!("{} → {:?}", r.listen, r.service),
                        TypeScale::BodyMedium
                    )
                    .width(Length::Fill),
                    action("Remove", Event::RemoveRoute(i))
                ]);
            }
        }
        page = page.push(typography(
            "Guest commands and files",
            TypeScale::TitleMedium,
        ));
        page = page.push(
            row![
                field(0, "Guest executable"),
                field(1, "Arguments (JSON array)"),
                action("Run command", Event::Exec)
            ]
            .spacing(8),
        );
        page = page.push(
            row![
                field(2, "Local source file"),
                field(3, "Absolute guest file path"),
                action("Upload", Event::Upload),
                action("Capture download", Event::Download)
            ]
            .spacing(8),
        );
        page = page.push(field(4, "New absolute directory for fetched results"));
        page=page.push(typography("Commands run for up to 5 minutes. Results are retained for 7 days on the worker. Uploads preserve different existing files. Command arguments are recorded; do not put secrets in them.",TypeScale::BodySmall));
        if let Some(ops) = self.vm.snapshot["catalog"]["operations"].as_object() {
            let mut rows: Vec<_> = ops.iter().filter(|(_, o)| o["instance"] == *id).collect();
            rows.sort_by_key(|(_, o)| std::cmp::Reverse(o["ordinal"].as_u64().unwrap_or(0)));
            for (op, o) in rows.into_iter().take(32) {
                let phase = o["outcome"]["phase"].as_str().unwrap_or("unknown");
                let released = o["released"] == true;
                let mut buttons = row![
                    typography(
                        format!(
                            "{op} · {phase}{}",
                            if released { " · output released" } else { "" }
                        ),
                        TypeScale::BodyMedium
                    )
                    .width(Length::Fill)
                ]
                .spacing(8);
                if !released {
                    buttons = buttons
                        .push(action("Stdout", Event::Logs(op.clone(), false)))
                        .push(action("Stderr", Event::Logs(op.clone(), true)));
                    if ["succeeded", "failed", "interrupted"].contains(&phase) {
                        buttons = buttons
                            .push(action("Fetch", Event::Fetch(op.clone())))
                            .push(action(
                                "Release output",
                                Event::Release(op.clone(), o["revision"].as_u64().unwrap_or(0)),
                            ));
                    } else if o["spec"]["kind"] == "upload" {
                        buttons = buttons.push(action("Retry staging", Event::Stage(op.clone())));
                    }
                }
                page = page.push(buttons);
                if let Some(error) = o["outcome"]["error"].as_str() {
                    page = page.push(typography(error, TypeScale::BodySmall));
                }
            }
        }
        if !self.vm.output.is_empty() {
            page = page
                .push(scrollable(text(&self.vm.output).font(iced::Font::MONOSPACE)).height(120));
        }
        surface(page)
            .variant(SurfaceVariant::Outlined)
            .padding(12)
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn managed_panel_renders_and_routes_commands_and_discards_stale_output() {
        let mut app = App::new("/example/client".into());
        app.instances = vec![
            json!({"id":"web","revision":1,"worker":"worker","desired":{"Instance":"Running"}}),
        ];
        app.vm.selected = Some("web".into());
        let mut ui = iced_test::Simulator::with_size(
            iced::Settings {
                default_font: iced_m3::fonts::REGULAR,
                ..Default::default()
            },
            (1220.0, 820.0),
            app.view(),
        );
        if let Some(dir) = std::env::var_os("FLEET_GUI_PREVIEW") {
            assert!(
                ui.snapshot(&app.theme())
                    .unwrap()
                    .matches_image(PathBuf::from(dir).join("managed-vm-light"))
                    .unwrap()
            );
        }
        ui.click("Run command").unwrap();
        assert!(
            ui.into_messages()
                .any(|m| matches!(m, Message::Vm(Event::Exec)))
        );
        let _ = app.vm_update(Event::Exec);
        assert!(matches!(
            app.pending,
            Some(ApiRequest::VmManage {
                action: Action::Start { .. },
                ..
            })
        ));
        let _ = app.vm_update(Event::Select("database".into()));
        let _ = app.vm_update(Event::Output(0, Ok(json!({"data":"web secret output"}))));
        assert!(app.vm.output.is_empty());
        assert!(
            action_notice(
                &json!({"operation":"bad","reply":{"Rejected":{"reason":"stale revision"}}})
            )
            .contains("rejected")
        );
    }
}
