//! The agent protocol: one request per bidirectional QUIC stream, each message one JSON value.
//!
//! **Versions.** A protocol version is `major.minor` ([`PROTOCOL`]). The major version is in the ALPN
//! (`fleet/agent/3`), so machines on different majors refuse each other when they connect. Within a major, each side
//! states its version in `Hello`, and two machines use the lower of the two ([`Protocol::agreed`]). The version stands
//! for the features it has, through one table, [`ADDED`], of which minor version added which request or field. A
//! request that uses an item newer than the agreed version is refused before it is sent, naming the item and the
//! version that added it; a job that uses one is placed only on machines whose agents have it. That shorthand is sound
//! only while three rules hold:
//! 1. **A release that adds to the protocol bumps the minor version** and lists what it added in [`ADDED`]. (For a
//!    change in what an agent does that adds nothing to the messages, compare the agreed version directly:
//!    `agreed.minor >= 2`.)
//! 2. **Agents are deployed only from the main line**, so one version number never names two different sets of
//!    features (a build from a branch could carry the number of a release it differs from).
//! 3. **What depends on the machine rather than the code is not part of the version.** A GPU, the OS, keep-awake, CPU
//!    slots: each machine reports these as facts in its `Hello` (fields and tags), and placement reads them there.
//!
//! **Compatibility** within a major version: machines may run different releases, so changes must be additive.
//! - A new field in a message gets a default (`#[serde(default)]`, or an `Option`), so an older peer's messages still
//!   read; fields a peer does not know are ignored. A new field must read as null, false or empty when unused (an
//!   `Option`, say), since [`ADDED`] counts any other value as using it.
//! - A request an agent does not know is answered with an error naming it ("this agent does not support …; upgrade
//!   it"): the fallback for an agent that breaks rule 2.
//! - Lists read leniently (`lenient`): an item a peer cannot read, such as a job in a state it does not know, is
//!   skipped rather than failing the answer. An event of a kind the coordinator does not know is read as
//!   `EventKind::Unknown`, so its number still advances the coordinator's place in the agent's log.
//! - A change that cannot be additive bumps the major version, and with it the ALPN (`fleet/agent/4`), so old and new
//!   refuse each other clearly.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use iroh::endpoint::Connection;
use iroh_blobs::Hash;
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    events::Event,
    jobs::{JobInfo, JobSpec, OutputFile},
};

/// A protocol version: the major version (named by the ALPN) and the minor version within it.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Protocol {
    pub major: u32,
    pub minor: u32,
}

impl Protocol {
    /// The version two machines use: the lower of their two (their majors match, or they could not have connected).
    pub fn agreed(self, other: Protocol) -> Protocol {
        self.min(other)
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// This build's protocol version. Changes in what an agent does that add nothing to the messages (rule 1's
/// parenthesis), by minor version:
/// - 3.7: VM job live observations, intermediate output capture and lifecycle events.
/// - 3.6: queued disposable VM jobs and worker VM capacity reporting.
/// - 3.5: durable VM guest operations, readiness, file transfers and storage inventory.
/// - 3.4: explicit shared GPU scheduling for VM instances, with aggregate CPU/memory admission.
/// - 3.3: VM instances and worker-side CPU/GPU admission alongside native jobs.
/// - 3.2: configurable shutdown grace, supervisor-owned termination, and idempotent worker submission IDs.
/// - 3.1: a JSON-lines probe's file may contain `*` (the newest match is read); an agent before 3.1 reads the
///   pattern literally and reports no progress for that probe, so nothing is gated on it.
pub const PROTOCOL: Protocol = Protocol { major: 3, minor: 7 };
/// The ALPN that names the protocol's major version.
pub const ALPN: &[u8] = b"fleet/agent/3";

/// What each minor version of this major added, as `(item, minor)`. An item is a request's name (`"Suspend"`), or
/// a field or enum variant inside a request by its path through the request's JSON (`"Suspend.within_seconds"`,
/// `"Submit.spec.probes.histogram"`: lists are looked through), or a string value as `path=value`. Everything
/// through 3.0 is the baseline, so the table starts empty; rule 1 above keeps it complete.
pub const ADDED: &[(&str, u32)] = &[
    ("InstanceGuest.Start.spec.version", 7),
    ("CancelVmJob", 6),
    ("Submit.spec.vm", 6),
    ("SubmitAllocated.spec.vm", 6),
    ("InstanceGuest", 5),
    ("Submit.spec.shutdown_grace_seconds", 2),
    ("SubmitAllocated", 3),
    ("Instances", 3),
    ("InstanceCreate", 3),
    ("InstanceSet", 3),
    ("InstanceCreate.spec.gpu", 4),
];
/// The largest message either side reads.
pub const MAX_MESSAGE: usize = 8 << 20;
/// The application close code an agent sends to a peer that is not on its allowlist.
pub const REFUSED: u32 = 403;

/// Reads a list, skipping items that do not read (for example, written by a newer peer).
pub fn lenient<'de, D: Deserializer<'de>, T: DeserializeOwned>(
    d: D,
) -> std::result::Result<Vec<T>, D::Error> {
    let raw: Vec<serde_json::Value> = Vec::deserialize(d)?;
    Ok(raw
        .into_iter()
        .filter_map(|v| serde_json::from_value(v).ok())
        .collect())
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Request {
    /// A durable cancellation tombstone also covers a submission still in flight.
    CancelVmJob {
        job: String,
        spec: Box<crate::jobs::JobSpec>,
    },
    InstanceGuest(crate::instances::guest::Request),
    Instances,
    InstanceCreate {
        id: String,
        spec: crate::instances::Spec,
    },
    InstanceSet {
        id: String,
        desired: crate::instances::Desired,
        revision: u64,
    },
    /// Placement declarations for worker-side admission alongside VM instances.
    SubmitAllocated {
        spec: Box<JobSpec>,
        id: Option<String>,
        gpu: bool,
    },
    /// What the agent is: its version and the machine's hardware. Carries the coordinator's own versions.
    Hello {
        fleet: String,
        protocol: Protocol,
    },
    /// How the machine is doing right now.
    Status,
    /// Which of these blobs the agent lacks (push them before `Submit`).
    Missing {
        hashes: Vec<Hash>,
    },
    /// Start a job; its inputs must already be in the agent's store. `id`, if given, becomes the job's ID (a
    /// coordinator's queue uses one ID end to end); otherwise the agent makes one.
    Submit {
        spec: Box<JobSpec>,
        #[serde(default)]
        id: Option<String>,
    },
    /// Events after `since`, waiting up to `wait_ms` (at most 25 s) for the first.
    Events {
        since: u64,
        wait_ms: u32,
    },
    /// Hold off idle sleep for this long (at most an hour): the coordinator has work queued for this machine.
    KeepAwake {
        seconds: u32,
    },
    /// Every job on the agent.
    Jobs,
    /// A job's output from a byte offset (at most `max` bytes, capped at `MAX_LOG_CHUNK`).
    Logs {
        job: String,
        stderr: bool,
        from: u64,
        max: u32,
    },
    /// Stop a job (SIGTERM to its process group, SIGKILL after a grace period).
    Cancel {
        job: String,
    },
    /// Freeze a running job (SIGSTOP to its process group); paused time does not count toward its time limit.
    Pause {
        job: String,
    },
    /// Continue a paused job (SIGCONT).
    Continue {
        job: String,
    },
    /// What suspending the job would lose now, and when its next checkpoint is due.
    SuspendPlan {
        job: String,
    },
    /// Stop the job at a checkpoint: at once (`now`, rolling back to the latest), or once a newer checkpoint has
    /// settled (giving up after `within_seconds`, if set).
    Suspend {
        job: String,
        now: bool,
        #[serde(default)]
        within_seconds: Option<u64>,
    },
    /// A suspended job's recorded checkpoint (its files, by hash, ready to fetch).
    Checkpoint {
        job: String,
    },
    /// The job's settled outputs, hashed and ready to fetch through the blobs protocol.
    Outputs {
        job: String,
    },
}

impl Request {
    /// The `Hello` this build sends.
    pub fn hello() -> Self {
        Request::Hello {
            fleet: env!("CARGO_PKG_VERSION").to_string(),
            protocol: PROTOCOL,
        }
    }
}

/// The most log bytes one `Logs` answer carries.
pub const MAX_LOG_CHUNK: u32 = 512 * 1024;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Response {
    InstanceGuest(crate::instances::guest::Response),
    Instances(Vec<crate::instances::Info>),
    Instance(crate::instances::Info),
    Hello(Hello),
    Status(Status),
    Missing(Vec<Hash>),
    Submitted {
        job: String,
    },
    Jobs(#[serde(deserialize_with = "lenient")] Vec<JobInfo>),
    /// Log text from a byte offset: cut at a character boundary, so `next` may stop short of the bytes read.
    Logs {
        data: String,
        next: u64,
        done: bool,
    },
    Cancelled,
    Outputs(#[serde(deserialize_with = "lenient")] Vec<OutputFile>),
    Events {
        events: Vec<Event>,
        last: u64,
    },
    Plan(crate::suspend::Plan),
    Checkpoint(crate::suspend::Recorded),
    Ok,
    Error(String),
}

fn unknown_slots() -> u32 {
    // An agent too old to say: assume room, and let the machine's own load sort it out.
    64
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Hello {
    pub agent_version: String,
    pub protocol: Protocol,
    pub hostname: String,
    pub os: String,
    pub cpu: String,
    pub cores: u32,
    pub memory_bytes: u64,
    /// The GPU and its graphics API, if the machine reports one.
    #[serde(default)]
    pub gpu: Option<String>,
    /// What placement can ask for, for example `gpu:metal` (plus any in `$FLEET_HOME/tags`).
    #[serde(default)]
    pub tags: Vec<String>,
    /// The machine's idle-sleep timer in minutes (0: never), if known.
    #[serde(default)]
    pub sleep_minutes: Option<u32>,
    /// How many CPU threads fleet may give jobs at once (its cores minus 2, unless `$FLEET_HOME/cpu-slots` says
    /// otherwise).
    #[serde(default = "unknown_slots")]
    pub cpu_slots: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
#[serde(default)]
pub struct Status {
    pub vm_capacity: Option<crate::instances::Capacity>,
    pub instances: Vec<crate::instances::Info>,
    /// The 1-, 5- and 15-minute load averages.
    pub load: Option<[f64; 3]>,
    pub on_ac_power: Option<bool>,
    pub battery_percent: Option<u8>,
    /// Free space on the volume holding the agent's home folder.
    pub disk_free_bytes: Option<u64>,
    pub uptime_seconds: Option<u64>,
    /// IDs of the jobs running now.
    pub running_jobs: Vec<String>,
    /// A GPU process started outside fleet (named in `$FLEET_HOME/gpu-processes`), if one is running.
    pub gpu_busy_external: Option<String>,
}

/// The answer an agent gives to a request it cannot read: names it, if it can, so the coordinator can say what to
/// upgrade.
pub fn unreadable(bytes: &[u8], error: &serde_json::Error) -> Response {
    let name = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Object(m) if m.len() == 1 => m.keys().next().cloned(),
            _ => None,
        });
    match name {
        Some(name) => Response::Error(format!(
            "this agent (fleet {}, protocol {PROTOCOL}) does not support `{name}`; upgrade it ({error})",
            env!("CARGO_PKG_VERSION")
        )),
        None => Response::Error(format!("unreadable request: {error}")),
    }
}

/// The first item of `added` a message uses that the agreed version predates, with the minor version that added it.
fn first_too_new(
    added: &[(&'static str, u32)],
    message: &Value,
    agreed: Protocol,
) -> Option<(&'static str, u32)> {
    let newer: Vec<_> = added.iter().filter(|(_, m)| *m > agreed.minor).collect();
    if newer.is_empty() {
        return None;
    }
    let mut used = BTreeSet::new();
    match message {
        Value::String(name) => {
            used.insert(name.clone());
        }
        Value::Object(map) => {
            for (name, body) in map {
                used.insert(name.clone());
                items_used(name, body, &mut used);
            }
        }
        _ => {}
    }
    newer
        .into_iter()
        .find(|(item, _)| used.contains(*item))
        .copied()
}

/// The paths a message's value uses, below `path` (fields that are null, false or empty are unused).
fn items_used(path: &str, value: &Value, used: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                let in_use = match v {
                    Value::Null | Value::Bool(false) => false,
                    Value::Array(a) => !a.is_empty(),
                    Value::Object(o) => !o.is_empty(),
                    _ => true,
                };
                if in_use {
                    let p = format!("{path}.{key}");
                    items_used(&p, v, used);
                    used.insert(p);
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|v| items_used(path, v, used)),
        Value::String(s) => {
            used.insert(format!("{path}={s}"));
        }
        _ => {}
    }
}

/// The first item of [`ADDED`] a request uses that the agreed version predates, with the minor version that added it.
pub fn too_new(request: &Request, agreed: Protocol) -> Option<(&'static str, u32)> {
    let old = first_too_new(ADDED, &serde_json::to_value(request).ok()?, agreed);
    old.or_else(|| match request {
        Request::Submit { spec, .. } | Request::SubmitAllocated { spec, .. }
            if spec.vm.is_some() && agreed.minor < 7 =>
        {
            Some(("VM job observations", 7))
        }
        _ => None,
    })
}

/// As [`too_new`], for a job (its specification as JSON) as `Submit` would send it.
pub fn job_too_new(spec: &Value, agreed: Protocol) -> Option<(&'static str, u32)> {
    first_too_new(
        ADDED,
        &serde_json::json!({ "Submit": { "spec": spec } }),
        agreed,
    )
    .or_else(|| {
        (spec.get("vm").is_some_and(|v| !v.is_null()) && agreed.minor < 7)
            .then_some(("VM job observations", 7))
    })
}

/// An agent's protocol version as this build sees it: equal, older (with what it lacks), or newer.
pub fn describe(agent: Protocol) -> String {
    if agent < PROTOCOL {
        let lacks: Vec<&str> = ADDED
            .iter()
            .filter(|(_, m)| *m > agent.minor)
            .map(|(item, _)| *item)
            .collect();
        if lacks.is_empty() {
            format!("protocol {agent}, older than this coordinator's {PROTOCOL} (upgrade it)")
        } else {
            format!(
                "protocol {agent}, older than this coordinator's {PROTOCOL} (lacks {}; upgrade it)",
                lacks.join(", ")
            )
        }
    } else if agent > PROTOCOL {
        format!("protocol {agent}, newer than this coordinator's {PROTOCOL}")
    } else {
        format!("protocol {agent}")
    }
}

/// Encodes a message.
pub fn encode<T: Serialize>(message: &T) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(message)?)
}

/// How long any one request may take (an `Events` long-poll waits at most 25 s; hashing large outputs is the slowest).
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// A request could not reach its peer or receive a complete response. Semantic refusals stay ordinary errors.
#[derive(Debug)]
pub struct TransportError(pub anyhow::Error);
impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}
impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub fn transport(error: impl Into<anyhow::Error>) -> anyhow::Error {
    TransportError(error.into()).into()
}

/// Sends one request on a new stream and reads the answer, giving up after `REQUEST_TIMEOUT`.
pub async fn request(connection: &Connection, request: &Request) -> Result<Response> {
    tokio::time::timeout(REQUEST_TIMEOUT, request_inner(connection, request))
        .await
        .map_err(|_| {
            transport(anyhow::anyhow!(
                "no answer within {} s",
                REQUEST_TIMEOUT.as_secs()
            ))
        })?
}

async fn request_inner(connection: &Connection, request: &Request) -> Result<Response> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| refusal(connection, e.into()))?;
    send.write_all(&encode(request)?)
        .await
        .map_err(|e| refusal(connection, e.into()))?;
    send.finish().map_err(|e| refusal(connection, e.into()))?;
    let bytes = recv
        .read_to_end(MAX_MESSAGE)
        .await
        .map_err(|e| refusal(connection, e.into()))?;
    serde_json::from_slice(&bytes).context("decoding the agent's answer")
}

/// Explains a failed request: a refusal by the agent's allowlist reads as one.
pub(crate) fn refusal(connection: &Connection, error: anyhow::Error) -> anyhow::Error {
    match connection.close_reason() {
        Some(iroh::endpoint::ConnectionError::ApplicationClosed(close))
            if close.error_code == REFUSED.into() =>
        {
            anyhow::anyhow!(
                "refused by the agent: this machine's key is not on its allowlist ({})",
                String::from_utf8_lossy(&close.reason)
            )
        }
        _ => transport(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_older_agents_hello_reads_with_defaults() {
        // As an agent from before tags, the sleep timer and CPU slots would send it.
        let old = r#"{"Hello": {"agent_version": "0.0.9", "protocol": {"major": 3, "minor": 0}, "hostname": "old", "os": "macOS",
                      "cpu": "M1", "cores": 8, "memory_bytes": 1, "gpu": null, "a_field_from_the_future": 1}}"#;
        let Response::Hello(h) = serde_json::from_str(old).unwrap() else {
            panic!()
        };
        assert_eq!((h.tags.len(), h.sleep_minutes, h.cpu_slots), (0, None, 64));
    }

    #[test]
    fn unknown_requests_are_named_and_unknown_list_items_skipped() {
        let bytes = br#"{"Teleport": {"to": "mars"}}"#;
        let error = serde_json::from_slice::<Request>(bytes).unwrap_err();
        let Response::Error(message) = unreadable(bytes, &error) else {
            panic!()
        };
        assert!(message.contains("does not support `Teleport`"), "{message}");
        let jobs = r#"{"Jobs": [{"id": "a", "name": "a", "state": "Running", "submitted_at": 1, "started_at": null,
                       "exit": null}, {"id": "b", "name": "b", "state": "Migrating", "submitted_at": 1,
                       "started_at": null, "exit": null}]}"#;
        let Response::Jobs(list) = serde_json::from_str(jobs).unwrap() else {
            panic!()
        };
        assert_eq!(
            list.len(),
            1,
            "the job in a state this version does not know is skipped"
        );
    }

    #[test]
    fn versions_stand_for_the_features_they_added() {
        assert_eq!(
            ALPN,
            format!("fleet/agent/{}", PROTOCOL.major).as_bytes(),
            "the ALPN names the major version"
        );
        let mut seen = BTreeSet::new();
        for (item, minor) in ADDED {
            assert!(
                *minor <= PROTOCOL.minor,
                "{item} is listed as added by {minor}, after this build's {PROTOCOL}"
            );
            assert!(seen.insert(*item), "{item} is listed twice");
        }
        let v = |minor| Protocol { major: 3, minor };
        assert_eq!(v(2).agreed(v(1)), v(1));
        assert_eq!(v(1).to_string(), "3.1");
        assert_eq!(describe(PROTOCOL), format!("protocol {PROTOCOL}"));
        let older = describe(v(0));
        assert!(
            older.starts_with(&format!(
                "protocol 3.0, older than this coordinator's {PROTOCOL} ("
            )) && older.ends_with("upgrade it)"),
            "{older}"
        );

        // As if 3.1 had added a request, a field, a probe kind and a value of an existing field.
        let added = [
            ("Teleport", 1),
            ("Suspend.within_seconds", 1),
            ("Submit.spec.env.MODE=fast", 1),
            ("Submit.spec.probes.histogram", 2),
        ];
        let check = |message: Value, agreed| first_too_new(&added, &message, agreed);
        assert_eq!(
            check(serde_json::json!("Teleport"), v(0)),
            Some(("Teleport", 1))
        );
        assert_eq!(check(serde_json::json!("Teleport"), v(1)), None);
        let suspend = |within: Option<u64>| {
            serde_json::to_value(Request::Suspend {
                job: "j".into(),
                now: false,
                within_seconds: within,
            })
            .unwrap()
        };
        assert_eq!(
            check(suspend(Some(60)), v(0)),
            Some(("Suspend.within_seconds", 1))
        );
        assert_eq!(
            check(suspend(None), v(0)),
            None,
            "an unused field is not needed"
        );
        let spec = serde_json::json!({"name": "a", "env": {"MODE": "fast"},
            "probes": [{"jsonl_field": {"file": "m", "field": "x"}}, {"histogram": {"file": "m"}}]});
        let submit = serde_json::json!({ "Submit": { "spec": spec } });
        assert_eq!(
            check(submit.clone(), v(0)),
            Some(("Submit.spec.env.MODE=fast", 1))
        );
        assert_eq!(
            check(submit, v(1)),
            Some(("Submit.spec.probes.histogram", 2)),
            "list items and enum variants are looked through"
        );
    }
}
