//! List accessor / transform function tests: `size`, `head`, `last`, `tail`,
//! `reverse`, `nth`. These operate on list values produced by `range`, `list`,
//! or `collect`. List-returning functions (`tail`, `reverse`) are composed with
//! scalar accessors here so each assertion is a single value.

mod support;

use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, graphdb_from_ledger, MemoryFluree};

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/", "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

async fn seed(fluree: &MemoryFluree, id: &str) -> fluree_db_api::LedgerState {
    let ledger0 = genesis_ledger(fluree, id);
    let txn = json!({"@context": ctx(), "@graph": [{"@id": "ex:anchor", "ex:n": 1}]});
    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

/// Evaluate a single scalar expression via `bind` and return it as i64.
async fn eval_scalar_i64(
    fluree: &MemoryFluree,
    ledger: &fluree_db_api::LedgerState,
    expr: &str,
) -> i64 {
    let db = graphdb_from_ledger(ledger);
    let q = json!({
        "@context": ctx(),
        "select": ["?v"],
        "where": [["bind", "?v", expr]]
    });
    let result = fluree.query(&db, &q).await.expect("scalar query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    let arr = rows.as_array().expect("array");
    match &arr[0] {
        JsonValue::Array(c) => c[0].as_i64().expect("i64"),
        other => other.as_i64().expect("i64"),
    }
}

#[tokio::test]
async fn list_accessor_functions() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "it/listfns:main").await;

    // size — element count of a list
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(size (range 1 5))").await,
        5
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(size (list 10 20 30))").await,
        3
    );

    // head / last — first / last element
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(head (range 10 12))").await,
        10
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(last (range 10 12))").await,
        12
    );

    // nth — 0-based index, negatives count from the end
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(nth (list 10 20 30) 0)").await,
        10
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(nth (list 10 20 30) 1)").await,
        20
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(nth (list 10 20 30) -1)").await,
        30
    );

    // tail — list without its first element (composed with size/head)
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(size (tail (range 1 5)))").await,
        4
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(head (tail (list 7 8 9)))").await,
        8
    );

    // reverse — reversed list (head of reverse == original last)
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(head (reverse (list 1 2 3)))").await,
        3
    );
    assert_eq!(
        eval_scalar_i64(&fluree, &ledger, "(nth (reverse (range 1 4)) 0)").await,
        4
    );
}

/// `nth` with an out-of-range index yields unbound (the row is dropped from a
/// projection of just that variable).
#[tokio::test]
async fn nth_out_of_range_is_unbound() {
    let fluree = fluree_db_api::FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "it/listfns:oob").await;
    let db = graphdb_from_ledger(&ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?v"],
        "where": [["bind", "?v", "(nth (list 1 2 3) 9)"]]
    });
    let result = fluree.query(&db, &q).await.expect("oob query");
    let rows = result
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("rows");
    // Out-of-range index → unbound → the projected cell is null.
    let arr = rows.as_array().expect("array");
    let cell = match &arr[0] {
        JsonValue::Array(c) => &c[0],
        other => other,
    };
    assert!(cell.is_null(), "out-of-range nth yields null, got {cell}");
}
