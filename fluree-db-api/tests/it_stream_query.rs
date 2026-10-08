//! Streaming SELECT query (NDJSON) integration tests.
//!
//! Drives the API producer (`plan_stream_query` + `run_stream_query`) directly
//! — the same path the server's `/v1/fluree/stream/query` endpoint spawns — and
//! asserts the NDJSON record protocol: a `head` record, one `row` per result
//! row, and a single `end` terminator. Also covers eligibility rejection.

use crate::support;
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

/// Streamed rows are the `bindings` entries `/query` returns — the documented
/// contract — so their IRIs are absolute even when the query declares a prefix
/// for them: a row carries no prefix map to expand a compact IRI against.
#[tokio::test]
async fn streamed_rows_match_buffered_sparql_json_iris() {
    let (fluree, ledger) = seed_three().await;
    let sparql = "PREFIX a: <http://a.co/> SELECT ?s WHERE { ?s a:name \"Xavier\" }";

    let buffered = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("buffered query")
        .to_sparql_json(&ledger.snapshot)
        .expect("to_sparql_json");
    let expected = &buffered["results"]["bindings"][0];
    assert_eq!(
        expected["s"],
        json!({"type": "uri", "value": "http://a.co/x"})
    );

    let records = collect_records(
        &fluree,
        ledger,
        OwnedStreamQuery::Sparql(sparql.to_string()),
    )
    .await;
    let row = records.iter().find(|r| r["type"] == "row").expect("a row");
    assert_eq!(&row["row"], expected, "single-ledger stream row");

    let from_query = json!({
        "@context": { "a": "http://a.co/" },
        "from": "stream/sel:main",
        "select": ["?s"],
        "where": { "@id": "?s", "a:name": "Xavier" }
    });
    let records = collect_dataset_records(&fluree, from_query).await;
    let row = records.iter().find(|r| r["type"] == "row").expect("a row");
    assert_eq!(&row["row"], expected, "dataset stream row");
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

/// N5 lock: the streaming query path rejects a SPARQL `FROM`/`FROM NAMED`
/// dataset clause (it does not build datasets), whereas the buffered `query`
/// path supports a within-ledger `FROM`. This is a deliberate surface
/// asymmetry, and it exercises the very same `validate_sparql_for_view` guard
/// the R2RML query path uses to reject `FROM` — so it locks both.
#[tokio::test]
async fn sparql_from_clause_is_rejected_before_streaming() {
    let (fluree, ledger) = seed_three().await;
    let graph = support::graphdb_from_ledger(&ledger);

    let result = fluree
        .plan_stream_query(
            &graph,
            &OwnedStreamQuery::Sparql(
                "PREFIX a: <http://a.co/> SELECT ?name FROM <urn:g1> { ?s a:name ?name }"
                    .to_string(),
            ),
        )
        .await;

    match result {
        Ok(_) => panic!("a SPARQL FROM clause must be rejected on the streaming endpoint"),
        Err(e) => assert!(
            e.to_string().contains("FROM"),
            "error should mention the unsupported FROM clause, got: {e}"
        ),
    }
}

/// e1–e3 Net, e4–e5 Local, e6 Remote.
async fn seed_areas() -> (support::MemoryFluree, support::MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "stream/areas:main");
    let seed = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:e1", "ex:area": "Net"},
            {"@id": "ex:e2", "ex:area": "Net"},
            {"@id": "ex:e3", "ex:area": "Net"},
            {"@id": "ex:e4", "ex:area": "Local"},
            {"@id": "ex:e5", "ex:area": "Local"},
            {"@id": "ex:e6", "ex:area": "Remote"}
        ]
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;
    (fluree, ledger)
}

/// #1978 on the stream lane: a grouped SELECT expression streams one row per
/// group (it streamed one row per solution).
#[tokio::test]
async fn sparql_grouped_select_expression_streams_one_row_per_group() {
    let (fluree, ledger) = seed_areas().await;
    let sparql = r#"PREFIX ex: <http://example.org/>
        SELECT (IF(?a = "Net", "network", "other") AS ?seg) (COUNT(?e) AS ?n)
        WHERE { ?e ex:area ?a } GROUP BY ?a"#
        .to_string();
    let records = collect_records(&fluree, ledger, OwnedStreamQuery::Sparql(sparql)).await;
    let last = records.last().expect("terminal record");
    assert_eq!(last["type"], "end", "{records:?}");
    assert_eq!(last["rows"], 3);
    let mut rows: Vec<(String, String)> = records
        .iter()
        .filter(|r| r["type"] == "row")
        .map(|r| {
            (
                r["row"]["seg"]["value"].as_str().unwrap().to_string(),
                r["row"]["n"]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("network".to_string(), "3".to_string()),
            ("other".to_string(), "1".to_string()),
            ("other".to_string(), "2".to_string())
        ]
    );
}

/// NDJSON rows are SPARQL-results bindings, which have no list type. A JSON-LD
/// query projecting a per-group list is refused before the stream starts (it
/// used to stream one row per list element: 97 rows from 2 groups on a
/// two-list query), and the error names the column.
#[tokio::test]
async fn jsonld_per_group_list_is_rejected_before_streaming() {
    let (fluree, ledger) = seed_areas().await;
    let graph = support::graphdb_from_ledger(&ledger);
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?e"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    match fluree
        .plan_stream_query(&graph, &OwnedStreamQuery::JsonLd(query))
        .await
    {
        Ok(_) => panic!("a per-group list must be rejected on the streaming endpoint"),
        Err(e) => assert!(
            e.to_string()
                .contains("list-valued column ?e cannot be streamed as SPARQL-results rows"),
            "{e}"
        ),
    }

    drop(graph);

    // Aggregated, the same grouping streams.
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "(as (count ?e) ?n)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let records = collect_records(&fluree, ledger, OwnedStreamQuery::JsonLd(query)).await;
    assert_eq!(records.last().expect("terminal")["rows"], 3, "{records:?}");
}

/// `select *` under `groupBy` on the `GroupByOperator` lane includes the
/// per-group lists: `/query` returns them, and the stream refuses the query
/// before it starts instead of silently dropping those columns. With every
/// WHERE variable a key, it streams. So does a query whose aggregates all
/// stream: `/query` then returns the keys and aggregates only.
#[tokio::test]
async fn jsonld_wildcard_with_list_columns_is_rejected_before_streaming() {
    let (fluree, ledger) = seed_areas().await;
    let query = |group_by: Value| {
        json!({
            "@context": {"ex": "http://example.org/"},
            "select": "*",
            "where": {"@id": "?e", "ex:area": "?a"},
            "groupBy": group_by
        })
    };

    // `/query`: keys and per-group lists.
    let rows = support::query_jsonld(&fluree, &ledger, &query(json!(["?a"])))
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    let net = rows
        .as_array()
        .expect("rows")
        .iter()
        .find(|row| row.to_string().contains("\"Net\""))
        .cloned()
        .unwrap_or_else(|| panic!("a Net row: {rows}"));
    assert!(
        net.to_string().contains("ex:e1") && net.to_string().contains("ex:e3"),
        "the Net row lists its entities: {net}"
    );

    // The stream: a 4xx before the first row.
    let graph = support::graphdb_from_ledger(&ledger);
    match fluree
        .plan_stream_query(&graph, &OwnedStreamQuery::JsonLd(query(json!(["?a"]))))
        .await
    {
        Ok(_) => panic!("select * with a per-group list must be rejected on the stream"),
        Err(e) => {
            let message = e.to_string();
            assert!(
                message.contains("select * under groupBy includes list-valued columns (?e)")
                    && message.contains("use /query"),
                "{message}"
            );
        }
    }
    drop(graph);

    let records = collect_records(
        &fluree,
        ledger.clone(),
        OwnedStreamQuery::JsonLd(query(json!(["?a", "?e"]))),
    )
    .await;
    assert_eq!(records.last().expect("terminal")["rows"], 6, "{records:?}");

    // A `count` in HAVING: every aggregate streams, and `/query` has no list.
    let mut counted = query(json!(["?a"]));
    counted["having"] = json!("(> (count ?e) 0)");
    let rows = support::query_jsonld(&fluree, &ledger, &counted)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert!(
        rows.as_array().is_some_and(|rows| rows.len() == 3) && !rows.to_string().contains("ex:e"),
        "the keys only: {rows}"
    );
    let records = collect_records(&fluree, ledger, OwnedStreamQuery::JsonLd(counted)).await;
    assert_eq!(records.last().expect("terminal")["rows"], 3, "{records:?}");
}

/// A grouped projection of a variable nothing binds is the same named 400 on
/// `/query` and on the stream (before it starts): unbound, not a per-group
/// list, and never an internal id.
#[tokio::test]
async fn jsonld_unbound_projection_is_the_same_4xx_on_query_and_stream() {
    let (fluree, ledger) = seed_areas().await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "?nosuch", "(as (count ?e) ?n)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let expected = "projected variable ?nosuch is unbound: nothing in the query binds it";

    let Err(e) = support::query_jsonld(&fluree, &ledger, &query).await else {
        panic!("/query: an unbound projected variable must be rejected");
    };
    let message = e.to_string();
    assert!(
        message.contains(expected) && !message.contains("VarId("),
        "/query: {message}"
    );
    assert_eq!(e.status_code(), 400, "/query: {message}");

    let graph = support::graphdb_from_ledger(&ledger);
    let Err(e) = fluree
        .plan_stream_query(&graph, &OwnedStreamQuery::JsonLd(query))
        .await
    else {
        panic!("stream: an unbound projected variable must be rejected");
    };
    let message = e.to_string();
    assert!(
        message.contains(expected) && !message.contains("list-valued"),
        "stream: {message}"
    );
    assert_eq!(e.status_code(), 400, "stream: {message}");
}

/// A grouped SELECT expression that reads a non-key variable is the same named
/// 400 on `/query` and on the stream, before it starts. The plan that finds it
/// is built after the stream's head record, so the stream runs the plan's
/// check first (the error used to arrive as a record after the 200).
#[tokio::test]
async fn jsonld_grouped_read_is_the_same_4xx_on_query_and_stream() {
    let (fluree, ledger) = seed_areas().await;
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": ["?a", "(as (+ (count ?e) (strlen (str ?e))) ?x)"],
        "where": {"@id": "?e", "ex:area": "?a"},
        "groupBy": ["?a"]
    });
    let expected = "the SELECT expression for ?x reads variable ?e, which is neither";

    let Err(e) = support::query_jsonld(&fluree, &ledger, &query).await else {
        panic!("/query: a grouped read of a non-key variable must be rejected");
    };
    let message = e.to_string();
    assert!(message.contains(expected), "/query: {message}");
    assert_eq!(e.status_code(), 400, "/query: {message}");

    let graph = support::graphdb_from_ledger(&ledger);
    let Err(e) = fluree
        .plan_stream_query(&graph, &OwnedStreamQuery::JsonLd(query))
        .await
    else {
        panic!("stream: a grouped read of a non-key variable must be rejected before it starts");
    };
    let message = e.to_string();
    assert!(message.contains(expected), "stream: {message}");
    assert_eq!(e.status_code(), 400, "stream: {message}");
}

/// A sort key nothing binds orders nothing on the stream too: every row
/// streams (it was a plan error after the head record).
#[tokio::test]
async fn order_by_a_variable_nothing_binds_streams_every_row() {
    let (fluree, ledger) = seed_areas().await;
    let sparql = r"PREFIX ex: <http://example.org/>
        SELECT ?e WHERE { ?e ex:area ?a } ORDER BY ?nosuch"
        .to_string();
    let records = collect_records(&fluree, ledger, OwnedStreamQuery::Sparql(sparql)).await;
    let last = records.last().expect("terminal record");
    assert_eq!(last["type"], "end", "{records:?}");
    assert_eq!(last["rows"], 6, "{records:?}");
}
