//! Current VM backend invariants independent of any retired coordinator API.
use fleet::vm_job::{Capture, validate_manifest};

#[test]
fn guest_manifest_rejects_unsafe_overlapping_unpublished_and_oversized_files() {
    for path in ["../escape", "/absolute", "a/../b", "a//b", "a/./b"] {
        assert!(
            validate_manifest(
                &[Capture {
                    version: None,
                    path: path.into(),
                    size: 1
                }],
                &["*".into()]
            )
            .is_err()
        );
    }
    assert!(
        validate_manifest(
            &[
                Capture {
                    version: None,
                    path: "a".into(),
                    size: 1
                },
                Capture {
                    version: None,
                    path: "a/b".into(),
                    size: 1
                },
            ],
            &["*".into()]
        )
        .is_err()
    );
    for (path, size) in [("secret", 1), ("out/big", (1 << 30) + 1)] {
        assert!(
            validate_manifest(
                &[Capture {
                    version: None,
                    path: path.into(),
                    size
                }],
                &["out/*".into()]
            )
            .is_err()
        );
    }
}

#[test]
fn startup_only_removes_unpublished_vm_staging() {
    let home =
        std::env::temp_dir().join(format!("fleet-vm-staging-{:032x}", rand::random::<u128>()));
    let vm = serde_json::json!({"image":"macos","cpus":2,"memory_mb":4096,"disk_gb":50,"network":"offline"});
    for (id, vm) in [("native", serde_json::Value::Null), ("vm", vm)] {
        let path = home.join("staging").join(id);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("job.json"),serde_json::to_vec(&serde_json::json!({"id":id,"submitter":"test","submitted_at":0,"spec":{"name":id,"argv":["true"],"vm":vm}})).unwrap()).unwrap();
    }
    fleet::vm_job::clean_staging(&home).unwrap();
    assert!(!home.join("staging/vm").exists());
    assert!(home.join("staging/native/job.json").exists());
    std::fs::remove_dir_all(home).unwrap();
}
