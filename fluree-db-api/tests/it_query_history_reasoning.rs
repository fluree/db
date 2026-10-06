//! Config `f:reasoningDefaults` must not reach a history/changes query.
//!
//! Regression for fluree/db#1806: once config defaults moved to the
//! query-preparation choke point, a history dataset picked up the ledger's
//! configured reasoning modes and `reject_reasoning_in_history_mode` refused
//! every from–to query on a configured ledger, even with no `reasoning` key.

use crate::support::MemoryFluree;
use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value};

const LEDGER_ID: &str = "it/history-reasoning:main";

/// Ledger with one data triple at t=1 and `f:reasoningModes f:rdfs` in `#config`.
async fn ledger_with_reasoning_defaults() -> MemoryFluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger(LEDGER_ID).await.expect("create");

    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice",
            }),
        )
        .await
        .expect("tx1");

    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        GRAPH <urn:fluree:{LEDGER_ID}#config> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:cfg:reason> .
            <urn:cfg:reason> f:reasoningModes f:rdfs .
        }}
    "
    );
    fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    fluree
}

fn history_query() -> Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "from": format!("{LEDGER_ID}@t:1"),
        "to": format!("{LEDGER_ID}@t:latest"),
        "select": ["?p", "?v", "?t", "?op"],
        "where": [{
            "@id": "ex:alice",
            "?p": {"@value": "?v", "@t": "?t", "@op": "?op"}
        }],
        "orderBy": "?t",
    })
}

async fn run(fluree: &MemoryFluree, q: &Value) -> Result<Value, String> {
    fluree
        .query_from()
        .jsonld(q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .map(|r| serde_json::to_value(&r.result).expect("serialize"))
        .map_err(|e| e.error)
}

/// No `reasoning` key: the configured default must not be applied, and the
/// history query returns the t=1 assert.
#[tokio::test]
async fn history_query_ignores_config_reasoning_defaults() {
    let fluree = ledger_with_reasoning_defaults().await;

    let rows = run(&fluree, &history_query())
        .await
        .expect("history query on a ledger with f:reasoningDefaults must succeed");
    let rows = rows.as_array().expect("rows array");
    assert_eq!(
        rows.len(),
        1,
        "expected the single t=1 assert, got {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(row["?p"]["@id"], json!("ex:name"));
    assert_eq!(row["?v"]["@value"], json!("Alice"));
    assert_eq!(row["?t"]["@value"], json!(1));
    assert_eq!(row["?op"]["@value"], json!(true));
}

/// The gate still refuses a mode the query itself asks for.
#[tokio::test]
async fn history_query_still_rejects_explicit_reasoning_mode() {
    let fluree = ledger_with_reasoning_defaults().await;

    let mut q = history_query();
    q["reasoning"] = json!("rdfs");
    let err = run(&fluree, &q)
        .await
        .expect_err("explicit reasoning mode on a history query must be refused");
    assert!(
        err.contains("reasoning is not supported for history/changes queries"),
        "unexpected error: {err}"
    );
}
