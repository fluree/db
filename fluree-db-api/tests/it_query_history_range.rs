//! Regression test for history-range queries against indexed ledgers.
//!
//! Reporter scenario: a query with explicit `"from"`/`"to"` keys (e.g.
//! `"from": "ledger@t:1", "to": "ledger@t:latest"`) should emit every
//! assert and retract event with `t` in that range, and the `@op`
//! binding should resolve to `"assert"` or `"retract"` per event.
//!
//! Before the fix:
//! - The binary cursor only emitted currently-asserted base rows, so
//!   the assert at t=1 and the retract at t=2 for an overwritten value
//!   never appeared in history-range output on indexed ledgers.
//! - `@op` always serialised as `null` because every scan constructed
//!   `Binding::Lit { op: None, .. }` (the only populating constructor,
//!   `from_object_with_t_op`, had zero call sites).
//!
//! After the fix, a dedicated `BinaryHistoryScanOperator` merges:
//! - history sidecar events (both assert and retract, with explicit op)
//! - base rows whose `t` falls in range (emitted as assert)
//! - overlay/novelty events when `to_t > index_t`
//!
//! and each emitted row carries `t` and `op` on the binding.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, FormatterConfig, ReindexOptions};
use serde_json::json;

fn ctx() -> serde_json::Value {
    json!({
        "ex": "http://example.org/",
    })
}

/// Reindex the ledger. Returns the indexed `index_t`.
async fn reindex_to_current(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let status = fluree.index_status(ledger_id).await.expect("index_status");
    status.index_t
}

/// History-range query should emit assert + retract events from the
/// history sidecar, with `@op` bound to `"assert"` / `"retract"`.
///
/// Sequence:
/// - t=1: insert `ex:alice ex:name "Alice"`
/// - reindex (index_t = 1)
/// - t=2: upsert `ex:alice ex:name "Alice Smith"`
///   (retracts "Alice", asserts "Alice Smith")
/// - reindex (index_t = 2; sidecar now carries the retract + old assert)
///
/// A history query `from t:1 to t:latest` on `ex:name` for `ex:alice`
/// must return three rows:
/// - `("Alice",       t=1, assert)` — original assert, now in sidecar
/// - `("Alice",       t=2, retract)` — retract from the upsert, in sidecar
/// - `("Alice Smith", t=2, assert)`  — current value, in base columns
#[tokio::test]
async fn history_range_emits_sidecar_events_with_op() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let path = tmp.path().to_str().unwrap();
    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger_id = "test/history-range:main";

    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1: assert name="Alice"
    let tx1 = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:name": "Alice",
    });
    let r1 = fluree.insert(ledger0, &tx1).await.expect("tx1");
    assert_eq!(r1.receipt.t, 1);

    // Index at t=1 so the next transaction's retract lands in the sidecar.
    let index_t_a = reindex_to_current(&fluree, ledger_id).await;
    assert_eq!(index_t_a, 1);

    // t=2: upsert to name="Alice Smith" — retracts "Alice", asserts "Alice Smith".
    let tx2 = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:name": "Alice Smith",
    });
    let r2 = fluree.upsert(r1.ledger, &tx2).await.expect("tx2");
    assert_eq!(r2.receipt.t, 2);

    // Index at t=2 so the retract + old assert live in the sidecar,
    // and the new assert lives in base columns.
    let index_t_b = reindex_to_current(&fluree, ledger_id).await;
    assert_eq!(index_t_b, 2);

    // History-range query: ex:alice ex:name ?v over [t=1, t=latest].
    // Bind @t and @op so we can assert on both.
    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t", "?op"],
        "where": [{
            "@id": "ex:alice",
            "ex:name": {"@value": "?v", "@t": "?t", "@op": "?op"}
        }],
        "orderBy": ["?t", "?op", "?v"],
    });

    let result = fluree
        .query_from()
        .jsonld(&q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("history range query");

    let value = serde_json::to_value(&result.result).expect("serialize");
    let rows = value.as_array().expect("rows array").clone();

    // Helper: flatten one formatted row `{"?v": ..., "?t": ..., "?op": ...}`
    // into `(v_str, t_i64, op_str)` so assertions are easy to read.
    fn flatten(row: &serde_json::Value) -> (String, i64, String) {
        let v = row
            .get("?v")
            .and_then(|x| x.get("@value"))
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        let t = row
            .get("?t")
            .and_then(|x| x.get("@value"))
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(-1);
        let op = row
            .get("?op")
            .and_then(|x| x.get("@value").or(Some(x)))
            .and_then(|x| x.as_str())
            .unwrap_or("null")
            .to_string();
        (v, t, op)
    }

    let flattened: Vec<(String, i64, String)> = rows.iter().map(flatten).collect();
    // orderBy (?t, ?op, ?v) with lexicographic ordering:
    //   "assert" < "retract", so at t=2 the assert of "Alice Smith" comes
    //   before the retract of "Alice".
    let expected: Vec<(String, i64, String)> = vec![
        ("Alice".to_string(), 1, "assert".to_string()),
        ("Alice Smith".to_string(), 2, "assert".to_string()),
        ("Alice".to_string(), 2, "retract".to_string()),
    ];
    assert_eq!(
        flattened, expected,
        "history range must emit sidecar events with @op bound; got rows {rows:#?}"
    );
}
