//! Real TCP HTTP parity and SIGKILL/restart gate for the opt-in WAL backend.
#![cfg(all(feature = "experimental-local-journal", unix))]
use fluree_db_api::local_journal_ledger::JournalLedger;
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_core::ContentId;
use fluree_db_server::{AppState, ServerConfig, TelemetryConfig};
use serde_json::{json, Value};
use std::os::unix::process::ExitStatusExt;
use std::{path::Path, sync::Arc, time::Duration};
const LEDGER: &str = "pokec:main";
const OBSERVE: &[&str] = &[
    "MATCH (n) RETURN count(n)",
    "MATCH (n:User) RETURN count(n)",
    "MATCH (n:L1) RETURN count(n)",
    "MATCH ()-[r]->() RETURN count(r)",
    "MATCH (n:User {id:1}) RETURN n.id, n.age, n.property",
];
const WRITES: &[(&str, &str)] = &[
    ("arango__single_edge_write", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/arango__single_edge_write.cypher")),
    ("arango__single_vertex_write", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/arango__single_vertex_write.cypher")),
    ("arango__unwind_range_vertex_write", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/arango__unwind_range_vertex_write.cypher")),
    ("create__edge", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/create__edge.cypher")),
    ("create__pattern", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/create__pattern.cypher")),
    ("create__vertex", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/create__vertex.cypher")),
    ("create__vertex_big", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/create__vertex_big.cypher")),
    ("update__vertex_on_property", include_str!("../../fluree-db-api/src/local_journal_ledger/fixtures/cypher/update__vertex_on_property.cypher")),
];
async fn post(client: &reqwest::Client, url: &str, body: &Value) -> Value {
    let response = client
        .post(url)
        .header("content-type", "application/cypher")
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {text}");
    serde_json::from_str(&text).unwrap()
}
async fn observations(client: &reqwest::Client, base: &str) -> Value {
    let mut rows = vec![];
    for query in OBSERVE {
        let value = post(
            client,
            &format!("{base}/v1/fluree/query/{LEDGER}"),
            &json!({"cypher":query}),
        )
        .await;
        rows.push(json!(value["results"][0]["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["row"].clone())
            .collect::<Vec<_>>()));
    }
    json!(rows)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn journal_http_child() {
    let Ok(root) = std::env::var("JOURNAL_HTTP_ROOT") else {
        return;
    };
    let mode = std::env::var("JOURNAL_HTTP_MODE").unwrap();
    let action = std::env::var("JOURNAL_HTTP_ACTION").unwrap();
    let oracle = std::env::var("JOURNAL_HTTP_ORACLE").unwrap();
    let index_dir = std::env::var("JOURNAL_HTTP_INDEXES").unwrap();
    let config = ServerConfig {
        storage_path: Some(root.clone().into()),
        journal_root: (mode == "wal").then(|| root.clone().into()),
        journal_index_path: (mode == "wal").then(|| index_dir.into()),
        record_raw_transactions: true,
        indexing_enabled: true,
        reindex_min_bytes: 100,
        reindex_max_bytes: Some(64 * 1024 * 1024),
        cors_enabled: false,
        ..Default::default()
    };
    let telemetry = TelemetryConfig::with_server_config(&config);
    let state = Arc::new(AppState::new(config, telemetry).await.unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = fluree_db_server::routes::build_router(state.clone());
    let http = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let store = state.fluree.content_store(LEDGER);
    if action == "write" {
        let mut acks = vec![];
        let mut observations_after = vec![];
        for (name, query) in WRITES {
            let body = json!({"cypher":query,"params":{"from":1,"to":2,"id":if *name=="arango__single_vertex_write" {10} else {1}}});
            post(&client, &format!("{base}/v1/fluree/update/{LEDGER}"), &body).await;
            let head = state
                .fluree
                .nameservice()
                .lookup(LEDGER)
                .await
                .unwrap()
                .unwrap();
            let id = head.commit_head_id.unwrap();
            let bytes = store.get(&id).await.unwrap();
            assert!(id.verify(&bytes));
            let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
            let raw_id = commit.txn.unwrap();
            let raw = store.get(&raw_id).await.unwrap();
            assert!(raw_id.verify(&raw));
            assert_eq!(serde_json::from_slice::<Value>(&raw).unwrap(), body);
            acks.push(json!({"id":id.to_string(),"raw_id":raw_id.to_string(),"t":head.commit_t,"raw":body}));
            observations_after.push(observations(&client, &base).await);
        }
        // No-op and unsupported writes must not advance the accepted head.
        let before = state
            .fluree
            .nameservice()
            .lookup(LEDGER)
            .await
            .unwrap()
            .unwrap();
        post(
            &client,
            &format!("{base}/v1/fluree/update/{LEDGER}"),
            &json!({"cypher":"MATCH (n:DoesNotExist) SET n.x=1"}),
        )
        .await;
        let after = state
            .fluree
            .nameservice()
            .lookup(LEDGER)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.commit_head_id, after.commit_head_id);
        if mode == "wal" {
            let bad = client
                .post(format!("{base}/v1/fluree/create"))
                .json(&json!({"ledger":"unsupported"}))
                .send()
                .await
                .unwrap();
            assert!(!bad.status().is_success());
        }
        // Controller-only wait AFTER acknowledgments: prove the actual background
        // worker published and the normal manager adopted its completed index.
        tokio::time::timeout(
            Duration::from_secs(30),
            state.fluree.indexer_handle().unwrap().wait_for_idle(LEDGER),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let view = state.fluree.db(LEDGER).await.unwrap();
                if view.snapshot.t >= after.commit_t {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background index was not adopted");
        let final_rows = observations(&client, &base).await;
        assert_eq!(final_rows, *observations_after.last().unwrap());
        let mut file = std::fs::File::create(&oracle).unwrap();
        use std::io::Write;
        file.write_all(
            serde_json::to_vec(
                &json!({"acks":acks,"observations":observations_after,"final":final_rows}),
            )
            .unwrap()
            .as_slice(),
        )
        .unwrap();
        file.sync_all().unwrap();
        if mode == "wal" {
            // Deliberate process death, not graceful disconnect/replay teardown.
            std::process::Command::new("kill")
                .args(["-KILL", &std::process::id().to_string()])
                .status()
                .unwrap();
            panic!("SIGKILL did not terminate child");
        }
    } else {
        let expected: Value = serde_json::from_slice(&std::fs::read(&oracle).unwrap()).unwrap();
        let acks = expected["acks"].as_array().unwrap();
        let head = state
            .fluree
            .nameservice()
            .lookup(LEDGER)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            head.commit_head_id.unwrap().to_string(),
            acks.last().unwrap()["id"]
        );
        let mut previous = None;
        for ack in acks {
            let id: ContentId = ack["id"].as_str().unwrap().parse().unwrap();
            let raw_id: ContentId = ack["raw_id"].as_str().unwrap().parse().unwrap();
            let bytes = store.get(&id).await.unwrap();
            assert!(id.verify(&bytes));
            let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
            assert_eq!(commit.t, ack["t"].as_i64().unwrap());
            assert_eq!(commit.txn, Some(raw_id.clone()));
            if let Some(previous) = previous {
                assert_eq!(commit.parents, vec![previous]);
            }
            previous = Some(id);
            let bytes = store.get(&raw_id).await.unwrap();
            assert!(raw_id.verify(&bytes));
            assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), ack["raw"]);
        }
        assert_eq!(observations(&client, &base).await, expected["final"]);
    }
    http.abort();
    state.fluree.disconnect().await;
}
fn child(root: &Path, indexes: &Path, oracle: &Path, mode: &str, action: &str) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "journal_http_child", "--nocapture"])
        .env("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1")
        .env("JOURNAL_HTTP_ROOT", root)
        .env("JOURNAL_HTTP_INDEXES", indexes)
        .env("JOURNAL_HTTP_ORACLE", oracle)
        .env("JOURNAL_HTTP_MODE", mode)
        .env("JOURNAL_HTTP_ACTION", action)
        .output()
        .unwrap();
    let expected_kill = mode == "wal" && action == "write";
    assert!(
        if expected_kill {
            output.status.signal() == Some(9)
        } else {
            output.status.success()
        },
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct SmokeProcess(std::process::Child);
impl Drop for SmokeProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn binary_smoke(root: &Path, indexes: &Path, journal: bool) {
    let config = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
    std::fs::write(config.path(), "[server]\n").unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_fluree-server"));
    command
        .args([
            "--config-file",
            config.path().to_str().unwrap(),
            "--listen-addr",
            &address.to_string(),
            "--storage-path",
            root.to_str().unwrap(),
            "--record-raw-transactions",
            "--reindex-min-bytes",
            "100",
            "--reindex-max-bytes",
            "67108864",
        ])
        .env("FLUREE_CYPHER_ALLOW_FULL_SCAN", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit());
    if journal {
        command.args([
            "--journal-root",
            root.to_str().unwrap(),
            "--journal-index-path",
            indexes.to_str().unwrap(),
        ]);
    }
    let mut process = SmokeProcess(command.spawn().unwrap());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "server binary exited during startup"
            );
            if let Ok(response) = client
                .get(format!("http://{address}/v1/fluree/info/{LEDGER}"))
                .send()
                .await
            {
                if response.status().is_success() {
                    let value: Value = response.json().await.unwrap();
                    assert_eq!(value["ledger"]["commit-t"], 1);
                    assert_eq!(value["ledger"]["index-t"], 1);
                    assert!(value["commitId"].as_str().is_some());
                    break;
                }
                panic!(
                    "metadata endpoint failed: {}",
                    response.text().await.unwrap()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    drop(process);
    result.expect("server binary did not become ready");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordinary_and_journal_http_corpus_background_indexing_and_crash_recovery() {
    let source_dir = tempfile::tempdir().unwrap();
    let wal_dir = tempfile::tempdir().unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let audit = tempfile::tempdir().unwrap();
    let source = FlureeBuilder::file(source_dir.path().to_string_lossy().to_string())
        .without_indexing()
        .build()
        .unwrap();
    let objects=fluree_db_api::cypher_import::cypher_to_jsonld("CREATE (:User {id:1, age:30}); CREATE (:User {id:2, age:20}); MATCH (a:User {id:1}), (b:User {id:2}) CREATE (a)-[:knows {weight:7}]->(b);",&Default::default()).unwrap();
    let input = audit.path().join("seed.jsonld");
    std::fs::write(
        &input,
        serde_json::to_vec(&json!({"@graph":objects})).unwrap(),
    )
    .unwrap();
    source
        .create(LEDGER)
        .import(&input)
        .threads(1)
        .memory_budget_mb(256)
        .execute()
        .await
        .unwrap();
    source
        .reindex(LEDGER, ReindexOptions::default())
        .await
        .unwrap();
    drop(
        JournalLedger::bootstrap(
            wal_dir.path().into(),
            source_dir.path().into(),
            LEDGER.into(),
            "http-v1".into(),
        )
        .await
        .unwrap(),
    );
    source.disconnect().await;
    drop(source);
    binary_smoke(source_dir.path(), outputs.path(), false).await;
    binary_smoke(wal_dir.path(), outputs.path(), true).await;
    let ordinary = audit.path().join("ordinary.json");
    let wal = audit.path().join("wal.json");
    child(
        source_dir.path(),
        outputs.path(),
        &ordinary,
        "ordinary",
        "write",
    );
    child(wal_dir.path(), outputs.path(), &wal, "wal", "write");
    let a: Value = serde_json::from_slice(&std::fs::read(&ordinary).unwrap()).unwrap();
    let b: Value = serde_json::from_slice(&std::fs::read(&wal).unwrap()).unwrap();
    assert_eq!(a["observations"], b["observations"]);
    assert_eq!(b["acks"].as_array().unwrap().len(), 8);
    for _ in 0..2 {
        for item in std::fs::read_dir(outputs.path()).unwrap() {
            std::fs::remove_dir_all(item.unwrap().path()).unwrap();
        }
        let material = wal_dir.path().join(".fluree-wal/data");
        std::fs::remove_dir_all(&material).unwrap();
        std::fs::create_dir(&material).unwrap();
        child(wal_dir.path(), outputs.path(), &wal, "wal", "verify");
    }
}
