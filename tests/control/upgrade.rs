use super::*;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde_json::{Value, json};

fn flatten(v: &mut Value) {
    if let Some(o) = v.as_object() {
        *v = o.values().next().unwrap().clone();
    }
}
fn old_assignment(a: &mut Value) {
    a.as_object_mut().unwrap().remove("attempt");
    a.as_object_mut().unwrap().remove("suspend_checkpoint");
    flatten(&mut a["desired"]);
    if !a["observed"].is_null() {
        flatten(&mut a["observed"][1]["phase"]);
    }
}
fn old_mutation(m: &mut Value) {
    if let Some(d) = m.get_mut("Desire") {
        flatten(&mut d["desired"]);
    }
    if let Some(r) = m.get_mut("WorkerReport") {
        flatten(&mut r["observation"]["phase"]);
    }
}
fn edit(path: &std::path::Path, table: TableDefinition<&str, &[u8]>, f: impl FnOnce(&mut Value)) {
    let db = Database::open(path).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table).unwrap();
        let mut value: Value =
            serde_json::from_slice(t.get("state").unwrap().unwrap().value()).unwrap();
        f(&mut value);
        t.insert("state", serde_json::to_vec(&value).unwrap().as_slice())
            .unwrap();
        if table.name() == "meta" {
            let mut identity: Value =
                serde_json::from_slice(t.get("identity").unwrap().unwrap().value()).unwrap();
            identity[0] = value["format"].clone();
            t.insert(
                "identity",
                serde_json::to_vec(&identity).unwrap().as_slice(),
            )
            .unwrap();
        }
    }
    tx.commit().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offline_upgrade_preserves_jobs_receipts_backend_ids_and_restarts_from_new_snapshot() {
    let mut c = Cluster::new(1).await;
    let (home, config) = prepare(&mut c, false).await;
    let mut w = Worker::start(&home, config.clone(), hello()).await.unwrap();
    let submitted = assign("old-job", workload("echo preserved > result.txt"));
    let accepted = c.client.write(submitted.clone(), WAIT).await.unwrap();
    wait(
        &mut c,
        &mut w,
        "old-job",
        WorkPhase::Job(AttemptPhase::Succeeded),
    )
    .await;
    let rejected_request = Request {
        origin: c.config.cluster.clone(),
        id: "historical-cancel".into(),
        mutation: Mutation::Desire {
            id: "old-job".into(),
            expected_revision: 1,
            desired: Desired::Job(JobDesired::Cancelled),
        },
    };
    let rejected = c
        .client
        .write(rejected_request.clone(), WAIT)
        .await
        .unwrap();
    let before = c.client.read(WAIT).await.unwrap();
    w.shutdown().await.unwrap();
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    let controller = c.home.0.join("1");
    edit(
        &controller.join("control.redb"),
        TableDefinition::new("meta"),
        |v| {
            v["format"] = json!(5);
            v.as_object_mut().unwrap().remove("checkpoints");
            let mut jobs = v["jobs"].take();
            for j in jobs.as_object_mut().unwrap().values_mut() {
                j["cancelled"] = json!(false);
                for key in ["phase", "attempts", "resume_from"] {
                    j.as_object_mut().unwrap().remove(key);
                }
            }
            v["queue"] = jobs;
            v["jobs"] = json!({});
            for a in v["assignments"].as_object_mut().unwrap().values_mut() {
                old_assignment(a);
            }
            for r in v["receipts"].as_object_mut().unwrap().values_mut() {
                old_mutation(&mut r["command"]);
            }
        },
    );
    edit(
        &home.join("worker.redb"),
        TableDefinition::new("worker_journal"),
        |v| {
            v["format"] = json!(5);
            v.as_object_mut().unwrap().remove("checkpoints");
            v.as_object_mut().unwrap().remove("known_checkpoints");
            for a in v["assignments"].as_object_mut().unwrap().values_mut() {
                old_assignment(a);
            }
            for o in v["observations"].as_object_mut().unwrap().values_mut() {
                flatten(&mut o[1]["phase"]);
            }
            for r in v["outbox"].as_object_mut().unwrap().values_mut() {
                flatten(&mut r["observation"]["phase"]);
            }
        },
    );
    let client = c.home.0.join("upgrade-client");
    std::fs::create_dir_all(client.join("requests")).unwrap();
    instances::write(&client.join("config.json"), &c.config).unwrap();
    let mut request = serde_json::to_value(&rejected_request).unwrap();
    old_mutation(&mut request["mutation"]);
    instances::write(
        &client.join("requests/cancel.json"),
        &json!({"intent":{},"request":request,"reply":rejected}),
    )
    .unwrap();
    // Validate every participant before writing the first database.
    edit(
        &home.join("worker.redb"),
        TableDefinition::new("worker_journal"),
        |v| {
            v["observations"]["old-job"][1]["phase"] = json!("Running");
        },
    );
    assert!(
        fleet::control::upgrade::run(
            &controller,
            std::slice::from_ref(&home),
            std::slice::from_ref(&client)
        )
        .is_err()
    );
    assert!(!controller.join("control.redb.v5-backup").exists());
    edit(
        &home.join("worker.redb"),
        TableDefinition::new("worker_journal"),
        |v| {
            v["observations"]["old-job"][1]["phase"] = json!("Succeeded");
        },
    );
    let mut wrong = c.config.clone();
    wrong.cluster = "different-cluster".into();
    instances::write(&client.join("config.json"), &wrong).unwrap();
    assert!(
        fleet::control::upgrade::run(
            &controller,
            std::slice::from_ref(&home),
            std::slice::from_ref(&client)
        )
        .is_err()
    );
    assert!(!controller.join("control.redb.v5-backup").exists());
    instances::write(&client.join("config.json"), &c.config).unwrap();
    let held = Database::open(home.join("worker.redb")).unwrap();
    assert!(
        fleet::control::upgrade::run(
            &controller,
            std::slice::from_ref(&home),
            std::slice::from_ref(&client)
        )
        .is_err()
    );
    drop(held);
    assert!(!controller.join("control.redb.v5-backup").exists());
    fleet::control::upgrade::run(
        &controller,
        std::slice::from_ref(&home),
        std::slice::from_ref(&client),
    )
    .unwrap();
    // A repeated invocation is safe, including the already-converted prepared request.
    fleet::control::upgrade::run(
        &controller,
        std::slice::from_ref(&home),
        std::slice::from_ref(&client),
    )
    .unwrap();
    assert!(controller.join("control.redb.v5-backup").is_file());
    assert!(home.join("worker.redb.v5-backup").is_file());
    let prepared: Value =
        serde_json::from_slice(&std::fs::read(client.join("requests/cancel.json")).unwrap())
            .unwrap();
    let upgraded_request: Request = serde_json::from_value(prepared["request"].clone()).unwrap();
    assert_eq!(upgraded_request, rejected_request);
    c.start(1).await;
    let mut w = Worker::start(&home, config, hello()).await.unwrap();
    w.tick().await.unwrap();
    let after = c.client.read(WAIT).await.unwrap();
    assert_eq!(after.jobs, before.jobs);
    assert_eq!(after.assignments, before.assignments);
    assert_eq!(c.client.write(submitted, WAIT).await.unwrap(), accepted);
    assert_eq!(
        c.client.write(upgraded_request, WAIT).await.unwrap(),
        rejected
    );
    assert_eq!(jobs::list(&home.join("backend")).unwrap().len(), 1);
    assert_eq!(
        std::fs::read_to_string(home.join("backend/jobs/exec-old-job/work/result.txt")).unwrap(),
        "preserved\n"
    );
    let backup = Database::open(controller.join("control.redb.v5-backup")).unwrap();
    let tx = backup.begin_read().unwrap();
    let t = tx
        .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
        .unwrap();
    let old: Value = serde_json::from_slice(t.get("state").unwrap().unwrap().value()).unwrap();
    assert_eq!(old["format"], 5);
    assert!(old["queue"]["old-job"].is_object());
    w.shutdown().await.unwrap();
    c.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn format_nine_upgrade_preserves_receipts_and_adds_history_defaults_without_inventing_archive()
 {
    let mut c = Cluster::new(1).await;
    let submitted = super::super::submit("legacy-submit", "legacy");
    let accepted = c.client.write(submitted.clone(), WAIT).await.unwrap();
    let cancelled = Request {
        origin: "cluster".into(),
        id: "legacy-cancel".into(),
        mutation: Mutation::CancelQueued {
            id: "legacy".into(),
            expected_revision: 1,
        },
    };
    c.client.write(cancelled, WAIT).await.unwrap();
    let before = c.client.read(WAIT).await.unwrap();
    c.replicas.remove(&1).unwrap().shutdown().await.unwrap();
    let controller = c.home.0.join("1");
    edit(
        &controller.join("control.redb"),
        TableDefinition::new("meta"),
        |v| {
            fn strip(v: &mut Value) {
                match v {
                    Value::Object(o) => {
                        o.remove("history");
                        o.remove("metric_files");
                        for v in o.values_mut() {
                            strip(v);
                        }
                    }
                    Value::Array(a) => {
                        for v in a {
                            strip(v);
                        }
                    }
                    _ => (),
                }
            }
            strip(v);
            v["format"] = json!(9);
        },
    );
    fleet::control::upgrade::run(&controller, &[], &[]).unwrap();
    assert!(controller.join("control.redb.v9-backup").exists());
    c.start(1).await;
    let after = c.client.read(WAIT).await.unwrap();
    assert_eq!(after.format, fleet::control::FORMAT);
    assert_eq!(after.jobs, before.jobs);
    assert!(after.history.attempts.is_empty());
    assert!(after.storage.artifacts.is_empty());
    assert_eq!(c.client.write(submitted, WAIT).await.unwrap(), accepted);
    assert_eq!(
        after.history_settings("legacy").unwrap().policy,
        fleet::history::Policy::default()
    );
    c.close().await;
}
