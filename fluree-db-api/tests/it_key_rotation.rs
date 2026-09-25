//! Key rotation end to end on file storage: a dry run counts without
//! writing, a scoped run leaves stragglers that verification reports, a
//! full run re-envelopes everything and stamps completion, a record left
//! by another holder is refused while fresh and taken over when stale, and
//! the data reads back through a client holding only the new key.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::key_rotation::{
    KeyRotationOptions, KeyRotationProgress, KeyRotationState, RECORD_PATH,
};
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::{StorageRead, StorageWrite};
use serde_json::json;
use std::path::Path;

const KEY1: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const KEY2: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";

fn config(path: &Path, keys: &[(u32, &str)], current: u32) -> serde_json::Value {
    let keys: Vec<_> = keys
        .iter()
        .map(|(id, key)| json!({"keyId": id, "AES256Key": key}))
        .collect();
    json!({
        "@context": {
            "@base": "https://ns.flur.ee/config/connection/",
            "@vocab": "https://ns.flur.ee/system#"
        },
        "@graph": [
            {
                "@id": "storage",
                "@type": "Storage",
                "filePath": path.to_string_lossy(),
                "AES256Keys": keys,
                "AES256CurrentKey": current
            },
            {"@id": "connection", "@type": "Connection", "indexStorage": {"@id": "storage"}}
        ]
    })
}

async fn client(path: &Path, keys: &[(u32, &str)], current: u32) -> Fluree {
    FlureeBuilder::from_json_ld(&config(path, keys, current))
        .expect("config")
        .build_client()
        .await
        .expect("build_client")
}

fn opts(retire: u32) -> KeyRotationOptions {
    KeyRotationOptions {
        retire_key_id: retire,
        dry_run: false,
        ledger: None,
        max_bytes_per_sec: None,
        holder: "test".to_string(),
    }
}

async fn seed(fluree: &Fluree, name: &str, n: usize) {
    let ledger = fluree.create_ledger(name).await.expect("create");
    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": (0..n).map(|i| json!({
            "@id": format!("ex:{name}-{i}"),
            "@type": "ex:Thing",
            "ex:name": format!("{name} {i}")
        })).collect::<Vec<_>>()
    });
    fluree.insert(ledger, &tx).await.expect("insert");
    support::build_and_publish_index(fluree, &format!("{name}:main")).await;
}

async fn count_things(fluree: &Fluree, name: &str) -> usize {
    let ledger = fluree
        .ledger(&format!("{name}:main"))
        .await
        .expect("ledger");
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?s"],
        "where": {"@id": "?s", "@type": "ex:Thing"}
    });
    support::query_jsonld(fluree, &ledger, &query)
        .await
        .expect("query")
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld")
        .as_array()
        .map(Vec::len)
        .unwrap_or(0)
}

/// A sweep over a few hundred blobs finishes in well under a second; a
/// stall is a bug, and it must fail the test rather than hang it.
const SWEEP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

async fn wait_done(fluree: &Fluree) -> KeyRotationProgress {
    tokio::time::timeout(SWEEP_DEADLINE, fluree.wait_for_key_rotation())
        .await
        .expect("sweep finished within the deadline")
        .expect("wait")
        .expect("a sweep ran")
}

async fn run_to_end(fluree: &Fluree, opts: KeyRotationOptions) -> KeyRotationProgress {
    fluree.start_key_rotation(opts).await.expect("start");
    wait_done(fluree).await
}

#[tokio::test(flavor = "multi_thread")]
async fn rotation_dry_run_scoped_run_full_run_and_verify() {
    let data = tempfile::TempDir::new().expect("tempdir");

    {
        let fluree = client(data.path(), &[(1, KEY1)], 1).await;
        seed(&fluree, "rot-a", 40).await;
        seed(&fluree, "rot-b", 25).await;
    }

    let fluree = client(data.path(), &[(1, KEY1), (2, KEY2)], 2).await;

    // Preflight refusals.
    assert!(
        fluree.start_key_rotation(opts(2)).await.is_err(),
        "current key"
    );
    assert!(
        fluree.start_key_rotation(opts(9)).await.is_err(),
        "unknown key"
    );

    // Dry run: counts, writes nothing, leaves no record.
    let dry = run_to_end(
        &fluree,
        KeyRotationOptions {
            dry_run: true,
            ..opts(1)
        },
    )
    .await;
    assert_eq!(dry.state, KeyRotationState::Completed);
    assert!(dry.dry_run);
    assert!(dry.on_retired > 0, "seeded blobs are on key 1");
    assert_eq!(dry.rewritten, 0);
    assert_eq!(
        dry.units_total, 5,
        "2 ledgers + 2 shared dictionary prefixes + graph sources"
    );
    let status = fluree.key_rotation_status().await.expect("status");
    assert_eq!(status.key_ids, vec![2, 1]);
    assert!(status.progress.as_ref().is_some_and(|p| p.dry_run));

    // Scoped run: only rot-a is rewritten; verification finds rot-b's blobs.
    let scoped = run_to_end(
        &fluree,
        KeyRotationOptions {
            ledger: Some("rot-a".to_string()),
            ..opts(1)
        },
    )
    .await;
    assert_eq!(scoped.state, KeyRotationState::Swept);
    assert_eq!(
        scoped.units_total, 2,
        "rot-a:main and its shared dictionaries"
    );
    assert!(scoped.rewritten > 0);
    assert_eq!(scoped.failed, 0);
    let completion = scoped.completion.expect("verified");
    assert!(
        completion.remaining_on_retired > 0,
        "rot-b is still on key 1"
    );
    assert!(completion.remaining_on_retired < dry.on_retired);

    // The record persisted and reads back through a fresh handle.
    let status = client(data.path(), &[(1, KEY1), (2, KEY2)], 2)
        .await
        .key_rotation_status()
        .await
        .expect("status");
    let recorded = status.progress.expect("record");
    assert_eq!(recorded.state, KeyRotationState::Swept);
    assert!(!status.active_here);
    assert!(!status.stalled);

    // Widening the scope is a new job, not a resumption of the scoped record
    // (its counters describe the scoped units). It finishes the rest.
    let full = run_to_end(&fluree, opts(1)).await;
    assert_eq!(full.state, KeyRotationState::Completed);
    assert_eq!(full.ledger_scope, None, "a new, unscoped record");
    assert_eq!(full.units_total, 5);
    assert_eq!(full.failed, 0);
    assert_eq!(full.completion.expect("verified").remaining_on_retired, 0);
    assert_eq!(full.on_retired, full.rewritten);
    assert!(full.rewritten + scoped.rewritten >= dry.on_retired);

    // Verify on its own agrees and re-stamps.
    let verified = fluree.verify_key_rotation(1).await.expect("verify");
    assert_eq!(verified.state, KeyRotationState::Completed);

    // The data reads back through a client holding key 2 alone.
    drop(fluree);
    let fluree = client(data.path(), &[(2, KEY2)], 2).await;
    assert_eq!(count_things(&fluree, "rot-a").await, 40);
    assert_eq!(count_things(&fluree, "rot-b").await, 25);
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_running_record_is_refused_while_fresh_and_taken_over_when_stale() {
    let data = tempfile::TempDir::new().expect("tempdir");
    {
        let fluree = client(data.path(), &[(1, KEY1)], 1).await;
        seed(&fluree, "rot-c", 10).await;
    }
    let fluree = client(data.path(), &[(1, KEY1), (2, KEY2)], 2).await;
    let storage = fluree
        .backend()
        .admin_storage_cloned()
        .expect("managed backend");
    let record_address = format!("fluree:file://{RECORD_PATH}");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let record = |updated_at: u64| {
        json!({
            "state": "running",
            "retire_key_id": 1,
            "current_key_id": 2,
            "holder": "node-other",
            "dry_run": false,
            "ledger_scope": null,
            "started_at": updated_at,
            "updated_at": updated_at,
            "units_total": 3,
            "units_done": 0,
            "unit": null,
            "cursor": null,
            "scanned": 0, "rewritten": 0, "already_current": 0, "on_other_keys": 0,
            "not_enveloped": 0, "on_retired": 0, "failed": 0, "failed_addresses": [],
            "bytes_rewritten": 0, "last_error": null, "completion": null
        })
    };

    // Fresh heartbeat from another holder: refused, and not resumable here.
    storage
        .write_bytes(&record_address, record(now).to_string().as_bytes())
        .await
        .unwrap();
    let err = fluree.start_key_rotation(opts(1)).await.unwrap_err();
    assert_eq!(err.status_code(), 409, "{err}");
    assert!(fluree
        .resume_pending_key_rotation("node-me")
        .await
        .expect("resume")
        .is_none());
    let status = fluree.key_rotation_status().await.unwrap();
    assert!(!status.stalled);

    // Released by its holder on leadership loss (`updated_at == 0`): mid-
    // handover, not stalled — the next leader takes it over at once.
    storage
        .write_bytes(&record_address, record(0).to_string().as_bytes())
        .await
        .unwrap();
    let status = fluree.key_rotation_status().await.unwrap();
    assert!(!status.stalled, "a released record is not stalled");

    // Stale heartbeat: reported stalled, taken over, run to completion.
    storage
        .write_bytes(&record_address, record(now - 3600).to_string().as_bytes())
        .await
        .unwrap();
    let status = fluree.key_rotation_status().await.unwrap();
    assert!(status.stalled);
    let resumed = fluree
        .resume_pending_key_rotation("node-me")
        .await
        .expect("resume")
        .expect("taken over");
    assert_eq!(resumed.holder, "node-me");
    assert_eq!(resumed.started_at, now - 3600, "kept the record's start");
    let done = wait_done(&fluree).await;
    assert_eq!(done.state, KeyRotationState::Completed);
    assert!(done.rewritten > 0);

    // A completed record is not resumed.
    assert!(fluree
        .resume_pending_key_rotation("node-me")
        .await
        .unwrap()
        .is_none());
    let _ = storage
        .read_bytes(&record_address)
        .await
        .expect("record exists");
}

/// `addressIdentifiers` puts a read router over the encrypted storage. The
/// router must report the encrypted default rather than read as plaintext:
/// status names the keys, and a rotation runs to completion.
#[tokio::test(flavor = "multi_thread")]
async fn rotation_through_an_address_identifier_router() {
    let data = tempfile::TempDir::new().expect("tempdir");
    let routed = tempfile::TempDir::new().expect("tempdir");
    let with_router = |keys: &[(u32, &str)], current: u32| {
        let mut config = config(data.path(), keys, current);
        let graph = config["@graph"].as_array_mut().unwrap();
        graph.push(json!({
            "@id": "routed",
            "@type": "Storage",
            "filePath": routed.path().to_string_lossy()
        }));
        graph[1]["addressIdentifiers"] = json!({"elsewhere": {"@id": "routed"}});
        config
    };

    {
        let fluree = FlureeBuilder::from_json_ld(&with_router(&[(1, KEY1)], 1))
            .expect("config")
            .build_client()
            .await
            .expect("build_client");
        seed(&fluree, "rot-routed", 20).await;
    }

    let fluree = FlureeBuilder::from_json_ld(&with_router(&[(1, KEY1), (2, KEY2)], 2))
        .expect("config")
        .build_client()
        .await
        .expect("build_client");
    assert_eq!(fluree.encryption_key_ids(), Some((vec![2, 1], 2)));
    let status = fluree.key_rotation_status().await.expect("status");
    assert_eq!(status.key_ids, vec![2, 1]);

    let done = run_to_end(&fluree, opts(1)).await;
    assert_eq!(done.state, KeyRotationState::Completed);
    assert!(done.rewritten > 0);
    assert_eq!(done.completion.expect("verified").remaining_on_retired, 0);
    assert_eq!(count_things(&fluree, "rot-routed").await, 20);
}
