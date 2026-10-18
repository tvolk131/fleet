//! Persistent service roles. Colocation does not bypass the replicated worker protocol.
use super::{Config, Replica, admin};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub controller_home: Option<PathBuf>,
    pub worker_home: Option<PathBuf>,
    #[serde(default)]
    pub storage_home: Option<PathBuf>,
    #[serde(default)]
    pub client_home: Option<PathBuf>,
    #[serde(default)]
    pub bootstrap: bool,
}
impl ServiceConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.controller_home.is_some()
                || self.worker_home.is_some()
                || self.client_home.is_some()
                || self.storage_home.is_some(),
            "choose controller, worker, storage or client-staging roles"
        );
        let mut homes = Vec::new();
        for home in [
            &self.controller_home,
            &self.worker_home,
            &self.client_home,
            &self.storage_home,
        ]
        .into_iter()
        .flatten()
        {
            ensure!(home.is_absolute(), "service homes must be absolute");
            ensure!(
                !home
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir)),
                "service homes must not contain .."
            );
            let resolved = home.canonicalize().unwrap_or_else(|_| home.clone());
            ensure!(
                !homes
                    .iter()
                    .any(|other: &PathBuf| resolved.starts_with(other)
                        || other.starts_with(&resolved)),
                "service roles require separate, non-overlapping homes"
            );
            homes.push(resolved);
        }
        ensure!(
            !self.bootstrap || self.controller_home.is_some(),
            "only a controller can bootstrap"
        );
        Ok(())
    }
}
pub async fn run(path: &Path) -> Result<()> {
    let config: ServiceConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    config.validate()?;
    if let Some(home) = &config.controller_home {
        let cfg: Config = serde_json::from_slice(&std::fs::read(home.join("config.json"))?)?;
        super::Store::preflight(&home.join("control.redb"), &cfg.cluster, cfg.id)?;
    }
    if let Some(home) = &config.worker_home
        && let Some(raw) = super::version::preflight(&home.join("worker.redb"), "worker_journal")?
    {
        ensure!(
            raw["state"]["format"] == super::FORMAT,
            "worker metadata needs explicit baseline migration"
        );
    }
    let replica = if let Some(home) = &config.controller_home {
        let cfg: Config = serde_json::from_slice(&std::fs::read(home.join("config.json"))?)?;
        let r = Replica::start(home, cfg.clone()).await?;
        if config.bootstrap && !r.raft.is_initialized().await? {
            r.bootstrap(&cfg.peers).await?;
        }
        Some(r)
    } else {
        None
    };
    let (storage_stop, mut storage_signal) = tokio::sync::watch::channel(false);
    let storage = if let Some(home) = &config.storage_home {
        let cfg = serde_json::from_slice(&std::fs::read(home.join("storage.json"))?)?;
        let mut agent = super::storage::Agent::start(home, cfg).await?;
        Some(tokio::spawn(async move {
            loop {
                if let Err(error) = agent.tick().await {
                    tracing::warn!(%error, "storage reconciliation will retry");
                }
                tokio::select! { _ = storage_signal.changed() => break, _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {} }
            }
            agent.shutdown().await
        }))
    } else {
        None
    };
    let staging = config.client_home.map(|home| {
        tokio::spawn(async move {
            loop {
                if let Err(error) =
                    super::client::call(&home, super::client::ApiRequest::Stage { id: None }).await
                {
                    tracing::warn!(%error,"client input staging will retry");
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        })
    });
    let result = if let Some(home) = &config.worker_home {
        match admin::start_worker(home).await {
            Ok(mut w) => {
                admin::run_worker(&mut w).await;
                w.shutdown().await
            }
            Err(e) => Err(e),
        }
    } else {
        crate::shutdown_signal().await;
        Ok(())
    };
    let _ = storage_stop.send(true);
    if let Some(task) = storage {
        task.await??;
    }
    if let Some(task) = staging {
        task.abort();
        let _ = task.await;
    }
    if let Some(r) = replica {
        r.shutdown().await?;
    }
    result
}
fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn systemd(s: &str) -> String {
    format!(
        "\"{}\"",
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    )
}

pub fn render(
    kind: &str,
    name: &str,
    user: &str,
    user_home: &Path,
    executable: &Path,
    config: &Path,
) -> Result<String> {
    ensure!(
        !name.is_empty()
            && name.len() <= 64
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.'),
        "invalid service name"
    );
    ensure!(
        !user.is_empty()
            && user
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "invalid service user"
    );
    for p in [user_home, executable, config] {
        ensure!(
            p.is_absolute() && !p.to_string_lossy().chars().any(|c| c.is_control()),
            "service paths must be absolute without control characters"
        );
    }
    let c: ServiceConfig = serde_json::from_slice(
        &std::fs::read(config).context("read prepared service configuration")?,
    )?;
    c.validate()?;
    let exe = executable.to_string_lossy();
    let cfg = config.to_string_lossy();
    let home = user_home.to_string_lossy();
    match kind {
        "launchd" => Ok(format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{name}</string>
<key>ProgramArguments</key><array><string>{exe}</string><string>service</string><string>run</string><string>--config</string><string>{cfg}</string></array>
<key>UserName</key><string>{user}</string>
<key>WorkingDirectory</key><string>{home}</string>
<key>EnvironmentVariables</key><dict><key>HOME</key><string>{home}</string><key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string><key>RUST_LOG</key><string>fleet=info,warn</string></dict>
<key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>5</integer>
<key>AbandonProcessGroup</key><true/>
<key>StandardOutPath</key><string>{home}/{name}.log</string>
<key>StandardErrorPath</key><string>{home}/{name}.log</string>
</dict></plist>
"#,
            name = xml(name),
            exe = xml(&exe),
            cfg = xml(&cfg),
            user = xml(user),
            home = xml(&home)
        )),
        "systemd" => {
            // WorkingDirectory takes a literal path, unlike ExecStart's quoted words.
            // Unit parsers trim surrounding whitespace and treat a trailing backslash as continuation.
            ensure!(
                home.trim() == home && !home.ends_with('\\'),
                "systemd home must not end with a backslash or surrounding whitespace"
            );
            Ok(format!(
                "[Unit]\nDescription=Fleet replicated control plane and worker\nAfter=network-online.target\nWants=network-online.target\n\n[Service]\nType=simple\nUser={user}\nWorkingDirectory={}\nEnvironment={}\nEnvironment=PATH=/usr/local/bin:/usr/bin:/bin\nEnvironment=RUST_LOG=fleet=info,warn\nExecStart={} service run --config {}\nRestart=always\nRestartSec=5\nKillMode=process\nTimeoutStopSec=60\nUMask=0077\n\n[Install]\nWantedBy=multi-user.target\n",
                home.replace('%', "%%"),
                systemd(&format!("HOME={home}")),
                systemd(&exe).replace('$', "$$"),
                systemd(&cfg).replace('$', "$$")
            ))
        }
        _ => anyhow::bail!("service kind must be launchd or systemd"),
    }
}
