//! Time travel integration tests
//!
//! Focus:
//! - `query_connection` `"from"` entries containing `@t:`, `@iso:`, `@commit:`
//! - Error cases for invalid formats and missing values
//! - Branch interaction (`ledger:main@t:<t>`)

mod support;

use chrono::{DateTime, Duration, FixedOffset, SecondsFormat, TimeZone, Utc};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{
    assert_index_defaults, genesis_ledger, normalize_rows_array, MemoryFluree, MemoryLedger,
};
use tokio::time::sleep;

fn ctx_test() -> JsonValue {
    // Be explicit about "name"/"age"/"Person" so we don't rely on implicit vocab behavior.
    json!({
        "test": "http://example.org/test#",
        "name": "test:name",
        "age": "test:age",
        "Person": "test:Person",
        "value": "test:value",
        "Data": "test:Data"
    })
}

async fn seed_time_travel_ledger(
    fluree: &MemoryFluree,
    ledger_id: &str,
) -> (MemoryLedger, String, std::collections::HashMap<i64, String>) {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    // Record wallclock times for ISO time-travel tests.
    // We record times AFTER each commit to ensure they're at or after the commit's actual timestamp.
    let mut iso_by_t: std::collections::HashMap<i64, String> = std::collections::HashMap::new();

    // t=1 Alice
    let tx1 = json!({
        "@context": ctx_test(),
        "@graph": [{"@id":"test:person1","@type":"Person","name":"Alice","age":30}]
    });
    let out1 = fluree.insert(ledger0, &tx1).await.expect("insert t=1");
    let commit_hex_t1 = out1.receipt.commit_id.digest_hex();
    let ledger1 = out1.ledger;
    let time_t1 = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    iso_by_t.insert(1, time_t1);

    // Ensure commit times differ (ISO time-travel tests rely on ordered timestamps).
    sleep(std::time::Duration::from_millis(10)).await;

    // t=2 Bob
    let tx2 = json!({
        "@context": ctx_test(),
        "@graph": [{"@id":"test:person2","@type":"Person","name":"Bob","age":25}]
    });
    let ledger2 = fluree
        .insert(ledger1, &tx2)
        .await
        .expect("insert t=2")
        .ledger;
    let time_t2 = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    iso_by_t.insert(2, time_t2);

    sleep(std::time::Duration::from_millis(10)).await;

    // t=3 Carol
    let tx3 = json!({
        "@context": ctx_test(),
        "@graph": [{"@id":"test:person3","@type":"Person","name":"Carol","age":28}]
    });
    let ledger3 = fluree
        .insert(ledger2, &tx3)
        .await
        .expect("insert t=3")
        .ledger;
    let time_t3 = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    iso_by_t.insert(3, time_t3);

    (ledger3, commit_hex_t1, iso_by_t)
}

async fn query_names_at(
    fluree: &MemoryFluree,
    db_for_formatting: fluree_db_core::GraphDbRef<'_>,
    from_spec: &str,
) -> Vec<Vec<JsonValue>> {
    let q = json!({
        "@context": ctx_test(),
        "from": [from_spec],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let jsonld = result
        .to_jsonld_async(db_for_formatting)
        .await
        .expect("to_jsonld_async");

    // JSON-LD formatter returns single-column results as a flat array.
    // Normalize to array-of-rows for easy comparison with expectations.
    let names: Vec<Vec<JsonValue>> = jsonld
        .as_array()
        .expect("result array")
        .iter()
        .map(|v| vec![v.clone()])
        .collect();

    normalize_rows_array(&JsonValue::Array(
        names.iter().map(|r| JsonValue::Array(r.clone())).collect(),
    ))
}

#[tokio::test]
async fn time_travel_query_connection_at_t_iso_and_sha() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-test";

    let (ledger3, commit_hex_t1, iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;
    let iso_t1 = iso_by_t.get(&1).expect("iso for t=1").clone();

    // @t:
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@t:1")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@t:2")
        )
        .await,
        normalize_rows_array(&json!([["Alice"], ["Bob"]]))
    );
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@t:3")
        )
        .await,
        normalize_rows_array(&json!([["Alice"], ["Bob"], ["Carol"]]))
    );

    // @iso: (use commit timestamp for t=1; should resolve to exactly t=1)
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@iso:{iso_t1}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );

    // Also ensure @iso: at "now" returns head state.
    let iso_now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@iso:{iso_now}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"], ["Bob"], ["Carol"]]))
    );

    // @commit: — uses hex SHA-256 digest from the ContentId
    let sha_7 = &commit_hex_t1[..7];
    let sha_52 = &commit_hex_t1[..52];
    let sha_6 = &commit_hex_t1[..6];

    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@commit:{sha_7}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@commit:{sha_52}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@commit:{sha_6}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
}

#[tokio::test]
async fn time_travel_invalid_format_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-invalid";
    let (ledger3, _commit_hex_t1, _iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;

    let q = json!({
        "@context": ctx_test(),
        "from": [format!("{ledger_id}@invalid:format")],
        "select": ["?s"],
        "where": [{"@id":"?s"}]
    });

    let err = fluree.query_connection(&q).await.unwrap_err().to_string();
    assert!(
        err.contains("Invalid time travel format"),
        "expected invalid time travel error, got: {err}"
    );

    // sanity: ledger still usable
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@t:1")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
}

#[tokio::test]
async fn time_travel_missing_value_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-missing";
    let (_ledger3, _commit_hex_t1, _iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;

    for (spec, expect) in [
        ("@t:", "Missing value after '@t:'"),
        ("@iso:", "Missing value after '@iso:'"),
        ("@commit:", "Missing value after '@commit:'"),
    ] {
        let q = json!({
            "@context": ctx_test(),
            "from": [format!("{ledger_id}{spec}")],
            "select": ["?s"],
            "where": [{"@id":"?s"}]
        });
        let err = fluree.query_connection(&q).await.unwrap_err().to_string();
        assert!(err.contains(expect), "expected '{expect}', got: {err}");
    }
}

#[tokio::test]
async fn time_travel_nonexistent_sha_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-no-sha";
    let (_ledger3, _commit_hex_t1, _iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;

    let q = json!({
        "@context": ctx_test(),
        "from": [format!("{ledger_id}@commit:zzzzzz")],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });

    let err = fluree.query_connection(&q).await.unwrap_err().to_string();
    assert!(
        err.contains("No commit found") || err.contains("SHA") || err.contains("commit"),
        "expected commit-not-found style error, got: {err}"
    );
}

#[tokio::test]
async fn time_travel_iso_too_early_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-iso-too-early";
    let (ledger3, _commit_hex_t1, iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;
    let iso_t1 = iso_by_t.get(&1).expect("iso for t=1").clone();

    // Compute a time before the first commit.
    let dt: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&iso_t1).expect("parse iso_t1");
    let too_early = (dt - Duration::days(1)).to_rfc3339_opts(SecondsFormat::Millis, true);

    let q = json!({
        "@context": ctx_test(),
        "from": [format!("{ledger_id}@iso:{too_early}")],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });

    let err = fluree.query_connection(&q).await.unwrap_err().to_string();
    assert!(
        err.contains("There is no data as of"),
        "expected no-data-as-of error, got: {err}"
    );

    // sanity: querying at t=1 still works
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@t:1")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
}

#[tokio::test]
async fn time_travel_iso_between_commits_resolves_to_previous_commit() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-iso-between";
    let (ledger3, _commit_hex_t1, iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;

    let iso1 = iso_by_t.get(&1).expect("iso for t=1");
    let iso2 = iso_by_t.get(&2).expect("iso for t=2");
    let iso3 = iso_by_t.get(&3).expect("iso for t=3");

    let dt1 = DateTime::parse_from_rfc3339(iso1).expect("parse t=1 iso");
    let dt2 = DateTime::parse_from_rfc3339(iso2).expect("parse t=2 iso");
    let dt3 = DateTime::parse_from_rfc3339(iso3).expect("parse t=3 iso");

    // Choose midpoints to avoid boundary issues with equal timestamps.
    // If any timestamps are equal, fall back to +1ms after the earlier time.
    let mid_12_ms = if dt2.timestamp_millis() > dt1.timestamp_millis() {
        i64::midpoint(dt1.timestamp_millis(), dt2.timestamp_millis())
    } else {
        dt1.timestamp_millis() + 1
    };
    let mid_23_ms = if dt3.timestamp_millis() > dt2.timestamp_millis() {
        i64::midpoint(dt2.timestamp_millis(), dt3.timestamp_millis())
    } else {
        dt2.timestamp_millis() + 1
    };

    let mid_12 = Utc
        .timestamp_millis_opt(mid_12_ms)
        .single()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let mid_23 = Utc
        .timestamp_millis_opt(mid_23_ms)
        .single()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Millis, true);

    // Mid between t1 and t2 should resolve to t1
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@iso:{mid_12}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"]]))
    );
    // Mid between t2 and t3 should resolve to t2
    assert_eq!(
        query_names_at(
            &fluree,
            ledger3.as_graph_db_ref(0),
            &format!("{ledger_id}@iso:{mid_23}")
        )
        .await,
        normalize_rows_array(&json!([["Alice"], ["Bob"]]))
    );
}

#[tokio::test]
async fn time_travel_commit_prefix_too_short_errors() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/time-travel-commit-too-short";
    let (_ledger3, _commit_hex_t1, _iso_by_t) = seed_time_travel_ledger(&fluree, ledger_id).await;

    let q = json!({
        "@context": ctx_test(),
        "from": [format!("{ledger_id}@commit:abcde")],
        "select": ["?name"],
        "where": [{"@id":"?s","name":"?name"}],
        "orderBy": ["?name"]
    });
    let err = fluree.query_connection(&q).await.unwrap_err().to_string();
    assert!(
        err.contains("at least 6") || err.contains("6 characters"),
        "expected sha-prefix-too-short error, got: {err}"
    );
}

#[tokio::test]
async fn time_travel_branch_interaction_main_at_t() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let base = "it/branch-time-test";
    let ledger_id = format!("{base}:main");

    let ledger0 = genesis_ledger(&fluree, &ledger_id);
    let out = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx_test(),
                "@graph": [{"@id":"test:main-data","@type":"Data","value":"main-value"}]
            }),
        )
        .await
        .expect("insert main data");
    let t_main = out.receipt.t;
    let ledger1 = out.ledger;

    let q = json!({
        "@context": ctx_test(),
        "from": [format!("{ledger_id}@t:{t_main}")],
        "select": ["?s", "?value"],
        "where": [{"@id":"?s","value":"?value"}]
    });

    let result = fluree.query_connection(&q).await.expect("query_connection");
    let jsonld = result
        .to_jsonld_async(ledger1.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");

    let rows = normalize_rows_array(&jsonld);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], json!("main-value"));
}
