//! Bounded worker notification history. This is not the worker's authority journal or durable job
//! archive. A reader can detect an expired notification cursor using `first` and refresh its view.

use std::{
    fs,
    io::{BufRead, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use crate::jobs::{State, now};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum EventKind {
    VmJob {
        job: String,
        status: crate::vm_job::Status,
    },
    InstanceState {
        instance: crate::instances::Info,
    },
    AgentStarted {
        version: String,
    },
    JobState {
        job: String,
        state: State,
    },
    Stopping {
        job: String,
        stopping: crate::jobs::Stopping,
    },
    /// The set of a job's settled outputs changed (new or changed files).
    Outputs {
        job: String,
        files: u32,
        bytes: u64,
    },
    /// A job's progress probes, when what they measure changed.
    Progress {
        job: String,
        #[serde(deserialize_with = "crate::proto::lenient")]
        probes: Vec<crate::probes::ProbeValue>,
    },
    /// A suspension waiting for the next checkpoint gave up (none settled in time); the job keeps running.
    SuspendGaveUp {
        job: String,
    },
    /// An event of a kind this version does not know (from a newer agent). Read, never written: it keeps the event's
    /// number, so the coordinator's place in the log still advances.
    Unknown,
}

/// Reads an event's kind, falling back to `Unknown` for kinds this version does not know.
fn lenient_kind<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<EventKind, D::Error> {
    let raw = serde_json::Value::deserialize(d)?;
    Ok(serde_json::from_value(raw).unwrap_or(EventKind::Unknown))
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Event {
    pub seq: u64,
    pub at: u64,
    #[serde(deserialize_with = "lenient_kind")]
    pub kind: EventKind,
}

/// The most events one answer carries.
pub const MAX_EVENTS: usize = 500;
const RETAINED_EVENTS: usize = 5000;

#[derive(Debug)]
pub struct EventLog {
    path: PathBuf,
    events: Mutex<Vec<Event>>,
    notify: Notify,
}

fn persist(path: &Path, events: &[Event]) -> Result<()> {
    let temporary = path.with_extension("jsonl.tmp");
    let mut compact = fs::File::create(&temporary)?;
    for event in events {
        writeln!(compact, "{}", serde_json::to_string(event)?)?;
    }
    compact.sync_all()?;
    fs::rename(temporary, path)?;
    fs::File::open(path.parent().expect("event log parent"))?.sync_all()?;
    Ok(())
}

impl EventLog {
    pub fn open(path: &Path) -> Result<Self> {
        let mut events = Vec::new();
        if path.exists() {
            for line in std::io::BufReader::new(fs::File::open(path)?).lines() {
                let line = line?;
                // A line cut off by a crash is skipped; every complete one is kept.
                if let Ok(event) = serde_json::from_str::<Event>(&line) {
                    events.push(event);
                }
                if events.len() > RETAINED_EVENTS * 2 {
                    events.drain(..RETAINED_EVENTS);
                }
            }
        }
        if events.len() > RETAINED_EVENTS {
            events.drain(..events.len() - RETAINED_EVENTS);
        }
        persist(path, &events)?;
        Ok(Self {
            path: path.to_path_buf(),
            events: Mutex::new(events),
            notify: Notify::new(),
        })
    }

    pub fn last(&self) -> u64 {
        self.events
            .lock()
            .expect("events")
            .last()
            .map_or(0, |e| e.seq)
    }
    pub fn first(&self) -> u64 {
        self.events
            .lock()
            .expect("events")
            .first()
            .map_or(0, |e| e.seq)
    }

    pub fn append(&self, kind: EventKind) -> Result<u64> {
        let mut events = self.events.lock().expect("events");
        let event = Event {
            seq: events.last().map_or(0, |e| e.seq) + 1,
            at: now(),
            kind,
        };
        let mut file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        writeln!(file, "{}", serde_json::to_string(&event)?)?;
        let seq = event.seq;
        events.push(event);
        if events.len() > RETAINED_EVENTS {
            events.drain(..MAX_EVENTS);
            persist(&self.path, &events)?;
        }
        drop(events);
        self.notify.notify_waiters();
        Ok(seq)
    }

    pub fn since(&self, seq: u64) -> Vec<Event> {
        let events = self.events.lock().expect("events");
        let start = events.partition_point(|e| e.seq <= seq);
        events[start..].iter().take(MAX_EVENTS).cloned().collect()
    }

    /// Events after `seq`, waiting up to `wait` for the first one.
    pub async fn wait_since(&self, seq: u64, wait: Duration) -> Vec<Event> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.notify.notified();
            let events = self.since(seq);
            if !events.is_empty() || tokio::time::Instant::now() >= deadline {
                return events;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.since(seq);
            }
        }
    }

    /// The last state logged for each job.
    pub fn job_states(&self) -> std::collections::HashMap<String, State> {
        let mut out = std::collections::HashMap::new();
        for event in self.events.lock().expect("events").iter() {
            if let EventKind::JobState { job, state } = &event.kind {
                out.insert(job.clone(), *state);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn events_are_numbered_kept_and_waited_for() {
        let dir = std::env::temp_dir().join(format!("fleet-events-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("events.jsonl");
        let log = EventLog::open(&path).unwrap();
        assert_eq!(
            log.append(EventKind::AgentStarted {
                version: "t".into()
            })
            .unwrap(),
            1
        );
        let job = || EventKind::JobState {
            job: "j".into(),
            state: State::Running,
        };
        assert_eq!(log.append(job()).unwrap(), 2);
        assert_eq!(log.since(1).len(), 1);

        let reopened = EventLog::open(&path).unwrap();
        assert_eq!(reopened.last(), 2, "numbering survives a restart");
        assert_eq!(reopened.job_states().get("j"), Some(&State::Running));

        let log = std::sync::Arc::new(reopened);
        let waiter = tokio::spawn({
            let log = log.clone();
            async move { log.wait_since(2, Duration::from_secs(5)).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        log.append(job()).unwrap();
        let got = waiter.await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].seq, 3);
        assert!(
            log.wait_since(3, Duration::from_millis(50))
                .await
                .is_empty()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rollover_and_partial_crash_tail_preserve_cursor_and_bound_disk_history() {
        let dir =
            std::env::temp_dir().join(format!("fleet-events-{:032x}", rand::random::<u128>()));
        fs::create_dir(&dir).unwrap();
        let path = dir.join("events.jsonl");
        let log = EventLog::open(&path).unwrap();
        for _ in 0..5501 {
            log.append(EventKind::Unknown).unwrap();
        }
        assert!(log.first() > 1);
        assert_eq!(log.last(), 5501);
        assert!(fs::read_to_string(&path).unwrap().lines().count() <= RETAINED_EVENTS);
        drop(log);
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{partial")
            .unwrap();
        let log = EventLog::open(&path).unwrap();
        assert_eq!(log.append(EventKind::Unknown).unwrap(), 5502);
        drop(log);
        assert_eq!(EventLog::open(&path).unwrap().last(), 5502);
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn an_event_of_an_unknown_kind_keeps_its_number() {
        let event: Event =
            serde_json::from_str(r#"{"seq": 7, "at": 1, "kind": {"Migrated": {"job": "a"}}}"#)
                .unwrap();
        assert_eq!((event.seq, event.kind), (7, EventKind::Unknown));
    }
}
