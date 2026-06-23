//! Streaming SELECT query (NDJSON) integration tests.
//!
//! Drives the API producer (`plan_stream_query` + `run_stream_query`) directly
//! — the same path the server's `/v1/fluree/stream/query` endpoint spawns — and
//! asserts the NDJSON record protocol: a `head` record, one `row` per result
//! row, and a single `end` terminator. Also covers eligibility rejection.

mod support;

use fluree_db_api::{
    FlureeBuilder, OwnedStreamQuery, QueryExecutionOptions, Tracker, TrackingOptions,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

async fn seed_three() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "stream/sel:main");
    let seed = json!({
        "@context": { "a": "http://a.co/" },
        "@graph": [
            { "@id": "http://a.co/x", "a:name": "Xavier" },
            { "@id": "http://a.co/y", "a:name": "Yolanda" },
            { "@id": "http://a.co/z", "a:name": "Zane" },
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;
    (fluree, ledger)
}

fn stream_tracker() -> Tracker {
    Tracker::new(TrackingOptions {
        track_time: true,
        track_fuel: true,
        ..Default::default()
    })
}

/// Run a streaming query to completion and return the parsed NDJSON records.
async fn collect_records(
    fluree: &support::MemoryFluree,
    ledger: support::MemoryLedger,
    input: OwnedStreamQuery,
) -> Vec<Value> {
    let graph = support::graphdb_from_ledger(&ledger);
    let plan = fluree
        .plan_stream_query(&graph, &input)
        .await
        .expect("plan should succeed");
    drop(graph);

    let (tx, mut rx) = mpsc::channel(1024);
    fluree
        .run_stream_query(
            ledger,
            plan,
            stream_tracker(),
            QueryExecutionOptions::default(),
            tx,
        )
        .await;

    let mut bytes = Vec::new();
    while let Some(chunk) = rx.recv().await {
        bytes.extend_from_slice(&chunk);
    }

    let text = String::from_utf8(bytes).expect("ndjson is utf-8");
    text.lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("each line is valid JSON"))
        .collect()
}

#[tokio::test]
async fn jsonld_select_streams_head_rows_end() {
    let (fluree, ledger) = seed_three().await;

    let query = json!({
        "@context": { "a": "http://a.co/" },
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" }
    });

    let records = collect_records(&fluree, ledger, OwnedStreamQuery::JsonLd(query)).await;

    // First record is the head with the projected var.
    assert_eq!(records[0]["type"], "head");
    assert_eq!(records[0]["vars"], json!(["name"]));

    // Last record is the success terminator with the row count.
    let last = records.last().expect("at least a terminal record");
    assert_eq!(last["type"], "end", "stream must end with an `end` record");
    assert_eq!(last["rows"], 3);

    // Everything between head and end is a row record.
    let rows: Vec<&Value> = records[1..records.len() - 1].iter().collect();
    assert_eq!(rows.len(), 3, "expected one row record per result row");
    for r in &rows {
        assert_eq!(r["type"], "row");
        assert!(r["row"]["name"]["value"].is_string());
    }

    // The streamed names match the seeded data.
    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r["row"]["name"]["value"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Xavier", "Yolanda", "Zane"]);

    // `end` carries fuel + time since the tracker enabled them.
    assert!(last["fuel"].as_f64().unwrap() >= 1.0);
    assert!(last["time"].is_string());
}

#[tokio::test]
async fn sparql_select_streams_rows() {
    let (fluree, ledger) = seed_three().await;

    let sparql = "SELECT ?name WHERE { ?s <http://a.co/name> ?name }".to_string();
    let records = collect_records(&fluree, ledger, OwnedStreamQuery::Sparql(sparql)).await;

    assert_eq!(records[0]["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 3);
    let row_count = records.iter().filter(|r| r["type"] == "row").count();
    assert_eq!(row_count, 3);
}

/// Seed a single named subject into `ledger_id` on a shared Fluree instance.
async fn seed_named(fluree: &support::MemoryFluree, ledger_id: &str, name: &str) {
    let ledger0 = support::genesis_ledger(fluree, ledger_id);
    let seed = json!({
        "@context": { "a": "http://a.co/" },
        "@graph": [{ "@id": format!("http://a.co/{name}"), "a:name": name }]
    });
    fluree.insert(ledger0, &seed).await.expect("seed");
}

/// Run a streaming dataset/connection query (build dataset → plan → run) and
/// return the parsed NDJSON records.
async fn collect_dataset_records(fluree: &support::MemoryFluree, query_json: Value) -> Vec<Value> {
    let dataset = fluree
        .build_stream_dataset(&query_json)
        .await
        .expect("dataset build should succeed");
    let input = OwnedStreamQuery::JsonLd(query_json);
    let plan = fluree
        .plan_stream_query_dataset(&dataset, &input)
        .await
        .expect("dataset plan should succeed");

    let (tx, mut rx) = mpsc::channel(1024);
    fluree
        .run_stream_query_dataset(
            dataset,
            plan,
            stream_tracker(),
            QueryExecutionOptions::default(),
            tx,
        )
        .await;

    let mut bytes = Vec::new();
    while let Some(chunk) = rx.recv().await {
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .expect("ndjson is utf-8")
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str::<Value>(l).expect("valid JSON"))
        .collect()
}

#[tokio::test]
async fn dataset_select_streams_via_from() {
    let (fluree, _ledger) = seed_three().await;

    // `from` routes through the connection/dataset streaming path.
    let query = json!({
        "@context": { "a": "http://a.co/" },
        "from": "stream/sel:main",
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" }
    });

    let records = collect_dataset_records(&fluree, query).await;
    assert_eq!(records[0]["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");
    assert_eq!(records.last().unwrap()["rows"], 3);
    assert_eq!(records.iter().filter(|r| r["type"] == "row").count(), 3);
}

#[tokio::test]
async fn multi_ledger_dataset_streams_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    seed_named(&fluree, "stream/a:main", "Alice").await;
    seed_named(&fluree, "stream/b:main", "Bob").await;

    let query = json!({
        "@context": { "a": "http://a.co/" },
        "from": ["stream/a:main", "stream/b:main"],
        "select": ["?name"],
        "where": { "@id": "?s", "a:name": "?name" }
    });

    let records = collect_dataset_records(&fluree, query).await;
    assert_eq!(records[0]["type"], "head");
    assert_eq!(records.last().unwrap()["type"], "end");

    let mut names: Vec<String> = records
        .iter()
        .filter(|r| r["type"] == "row")
        .map(|r| r["row"]["name"]["value"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Bob"], "union of both ledgers");
}

#[tokio::test]
async fn ask_query_is_rejected_before_streaming() {
    let (fluree, ledger) = seed_three().await;
    let graph = support::graphdb_from_ledger(&ledger);

    let result = fluree
        .plan_stream_query(
            &graph,
            &OwnedStreamQuery::Sparql("ASK { ?s ?p ?o }".to_string()),
        )
        .await;

    match result {
        Ok(_) => panic!("ASK must be rejected on the streaming endpoint"),
        Err(e) => assert!(
            e.to_string().to_lowercase().contains("ask"),
            "error should mention ASK, got: {e}"
        ),
    }
}
