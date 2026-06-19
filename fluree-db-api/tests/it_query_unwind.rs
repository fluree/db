//! UNWIND + `range` / `list` JSON-LD query-surface tests.
//!
//! UNWIND expands a list-valued expression into one row per element. Its
//! headline use is as a *row generator* over a computed list (`range`, or the
//! result of `collect`) — the one thing `VALUES` (constants only) and triple
//! patterns (stored data only) cannot do. The canonical case is a dense /
//! gap-filled series: generate the axis with `range`, LEFT JOIN the data.

mod support;

use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger, MemoryFluree};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

async fn seed_orders(fluree: &MemoryFluree, id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, id);
    // Orders in 2019, 2020, 2022 — note 2021 and 2023 have NONE.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:o1", "@type": "ex:Order", "ex:orderYear": 2019},
            {"@id": "ex:o2", "@type": "ex:Order", "ex:orderYear": 2020},
            {"@id": "ex:o3", "@type": "ex:Order", "ex:orderYear": 2020},
            {"@id": "ex:o4", "@type": "ex:Order", "ex:orderYear": 2022}
        ]
    });
    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

/// `range(1, 5)` expanded by UNWIND yields one row per integer, 1..=5.
#[tokio::test]
async fn unwind_range_generates_rows() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_orders(&fluree, "it/unwind:range").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [["unwind", "?n", "(range 1 5)"]],
        "orderBy": ["?n"]
    });
    let result = fluree.query(&db, &q).await.expect("unwind range query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");

    let nums: Vec<i64> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| match r {
            JsonValue::Array(cols) => cols[0].as_i64().expect("int"),
            other => other.as_i64().expect("int"),
        })
        .collect();
    assert_eq!(nums, vec![1, 2, 3, 4, 5]);
}

/// `list(...)` literal expanded by UNWIND yields one row per element.
#[tokio::test]
async fn unwind_list_literal_generates_rows() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_orders(&fluree, "it/unwind:list").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?x"],
        "where": [["unwind", "?x", "(list 10 20 30)"]],
        "orderBy": ["?x"]
    });
    let result = fluree.query(&db, &q).await.expect("unwind list query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let nums: Vec<i64> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| match r {
            JsonValue::Array(cols) => cols[0].as_i64().expect("int"),
            other => other.as_i64().expect("int"),
        })
        .collect();
    assert_eq!(nums, vec![10, 20, 30]);
}

/// The canonical "can't do this today" case: a dense year series with
/// zero-filled gaps. `range` generates every year 2019..=2023; the OPTIONAL
/// contributes a count only where orders exist, so 2021 and 2023 come back 0.
#[tokio::test]
async fn unwind_range_dense_series_gap_fill() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_orders(&fluree, "it/unwind:dense").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?year", "(as (count ?o) ?orders)"],
        "where": [
            ["unwind", "?year", "(range 2019 2023)"],
            ["optional", {"@id": "?o", "@type": "ex:Order", "ex:orderYear": "?year"}]
        ],
        "groupBy": ["?year"],
        "orderBy": ["?year"]
    });
    let result = fluree.query(&db, &q).await.expect("dense series query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");

    let pairs: Vec<(i64, i64)> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| {
            let c = r.as_array().expect("row");
            (c[0].as_i64().expect("year"), c[1].as_i64().expect("count"))
        })
        .collect();
    assert_eq!(
        pairs,
        vec![(2019, 1), (2020, 2), (2021, 0), (2022, 1), (2023, 0)],
        "every year 2019..=2023 present, with zero-filled gaps for 2021 and 2023"
    );
}

/// An empty range (`start > end`) expands to zero rows rather than erroring.
#[tokio::test]
async fn unwind_empty_range_zero_rows() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_orders(&fluree, "it/unwind:empty").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [["unwind", "?n", "(range 5 1)"]]
    });
    let result = fluree.query(&db, &q).await.expect("empty range query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    assert_eq!(rows.as_array().map(Vec::len).unwrap_or(0), 0);
}

/// UNWIND over a list bound earlier with BIND (range computed into a variable
/// first, then expanded) — the list source can be any list-valued expression.
#[tokio::test]
async fn unwind_bound_list_variable() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed_orders(&fluree, "it/unwind:boundvar").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?n"],
        "where": [
            ["bind", "?nums", "(range 1 3)"],
            ["unwind", "?n", "?nums"]
        ],
        "orderBy": ["?n"]
    });
    let result = fluree
        .query(&db, &q)
        .await
        .expect("bound-list unwind query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let nums: Vec<i64> = rows
        .as_array()
        .expect("array")
        .iter()
        .map(|r| match r {
            JsonValue::Array(cols) => cols[0].as_i64().expect("int"),
            other => other.as_i64().expect("int"),
        })
        .collect();
    assert_eq!(nums, vec![1, 2, 3]);
}
