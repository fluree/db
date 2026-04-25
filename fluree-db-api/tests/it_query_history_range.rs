//! Regression test for history-range queries against indexed ledgers.
//!
//! Reporter scenario: a query with explicit `"from"`/`"to"` keys (e.g.
//! `"from": "ledger@t:1", "to": "ledger@t:latest"`) should emit every
//! assert and retract event with `t` in that range, and the `@op`
//! binding should resolve to `true` (assert) or `false` (retract)
//! per event — mirroring `Flake.op` on disk.
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
/// history sidecar, with `@op` bound to `true` (assert) / `false`
/// (retract).
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

    let flattened: Vec<(String, i64, bool)> = rows.iter().map(flatten_v_t_op).collect();
    // orderBy (?t, ?op, ?v): false (retract) sorts before true (assert)
    // numerically, so at t=2 the retract of "Alice" precedes the assert
    // of "Alice Smith".
    let expected: Vec<(String, i64, bool)> = vec![
        ("Alice".to_string(), 1, true),
        ("Alice".to_string(), 2, false),
        ("Alice Smith".to_string(), 2, true),
    ];
    assert_eq!(
        flattened, expected,
        "history range must emit sidecar events with @op bound; got rows {rows:#?}"
    );
}

// ---------------------------------------------------------------------------
// Helpers shared with the coverage cases below
// ---------------------------------------------------------------------------

/// Flatten a formatted row into `(?v: String, ?t: i64, ?op: bool)`.
fn flatten_v_t_op(row: &serde_json::Value) -> (String, i64, bool) {
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
        .and_then(serde_json::Value::as_bool)
        .expect("?op should be a boolean");
    (v, t, op)
}

async fn run_history_query(
    fluree: &fluree_db_api::Fluree,
    q: &serde_json::Value,
) -> Vec<(String, i64, bool)> {
    let result = fluree
        .query_from()
        .jsonld(q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("history range query");
    let value = serde_json::to_value(&result.result).expect("serialize");
    value
        .as_array()
        .expect("rows array")
        .iter()
        .map(flatten_v_t_op)
        .collect()
}

/// Variant for queries that select only `?v, ?t` (e.g. when `@op` is a
/// constant filter rather than a bound variable).
async fn run_history_query_no_op(
    fluree: &fluree_db_api::Fluree,
    q: &serde_json::Value,
) -> Vec<(String, i64)> {
    let result = fluree
        .query_from()
        .jsonld(q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("history range query");
    let value = serde_json::to_value(&result.result).expect("serialize");
    value
        .as_array()
        .expect("rows array")
        .iter()
        .map(|row| {
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
            (v, t)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Case: novelty-only history (no reindex between commits).
//
// Verifies the path through `flakes_to_bindings:~704`, which already
// populated `op` from `flake.op` for overlay/novelty flakes. The new
// `BinaryHistoryScanOperator` must not regress that path.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn history_range_novelty_only() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-novelty:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1 Alice / t=2 rename Alice→Alice Smith. No reindex: everything
    // stays in novelty.
    let r1 = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .expect("tx1");
    let r2 = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice Smith"}),
        )
        .await
        .expect("tx2");
    assert_eq!(r2.receipt.t, 2);

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

    let rows = run_history_query(&fluree, &q).await;
    let expected: Vec<(String, i64, bool)> = vec![
        ("Alice".to_string(), 1, true),
        ("Alice".to_string(), 2, false),
        ("Alice Smith".to_string(), 2, true),
    ];
    assert_eq!(rows, expected, "novelty-only history must also bind @op");
}

// ---------------------------------------------------------------------------
// Case: `@op` as a constant filter — asserts only.
//
// The parser lowers `{"@op": true}` into `FILTER(op(?v) = true)`.
// That filter runs downstream of the scan, so the history operator
// just needs to emit rows with op populated and the FILTER does the rest.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn history_range_op_constant_filter_assert() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-op-filter:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    let r1 = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .expect("tx1");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice Smith"}),
        )
        .await
        .expect("tx2");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 2);

    // Ask only for asserts. `@op: true` is a FILTER constant, not a
    // BIND — `?op` never exists as a variable, so select only `?v`/`?t`
    // and assert the filter returns both assert events and no retracts.
    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t"],
        "where": [{
            "@id": "ex:alice",
            "ex:name": {"@value": "?v", "@t": "?t", "@op": true}
        }],
        "orderBy": ["?t", "?v"],
    });
    let rows = run_history_query_no_op(&fluree, &q).await;
    let expected: Vec<(String, i64)> =
        vec![("Alice".to_string(), 1), ("Alice Smith".to_string(), 2)];
    assert_eq!(
        rows, expected,
        "@op=true filter must return only assert events"
    );
}

#[tokio::test]
async fn history_range_op_constant_filter_retract() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-op-filter-retract:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    let r1 = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .expect("tx1");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice Smith"}),
        )
        .await
        .expect("tx2");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 2);

    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t"],
        "where": [{
            "@id": "ex:alice",
            "ex:name": {"@value": "?v", "@t": "?t", "@op": false}
        }],
        "orderBy": ["?t", "?v"],
    });
    let rows = run_history_query_no_op(&fluree, &q).await;
    let expected: Vec<(String, i64)> = vec![("Alice".to_string(), 2)];
    assert_eq!(
        rows, expected,
        "@op=false filter must return only retract events"
    );
}

// ---------------------------------------------------------------------------
// Case: sidecar + novelty boundary. Reindex t=1, transact t=2 (stays in
// novelty), query spanning the boundary. Exercises the `to_t > index_t`
// novelty merge path.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn history_range_sidecar_plus_novelty_boundary() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-boundary:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1: assert "Alice". Index at t=1.
    let r1 = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .expect("tx1");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);

    // t=2: upsert "Alice Smith". DO NOT reindex — retract+assert stay
    // in novelty, crossing the index_t boundary.
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice Smith"}),
        )
        .await
        .expect("tx2");
    let status = fluree.index_status(ledger_id).await.expect("index_status");
    assert_eq!(status.index_t, 1);
    assert_eq!(status.commit_t, 2);

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
    let rows = run_history_query(&fluree, &q).await;
    let expected: Vec<(String, i64, bool)> = vec![
        // t=1 assert comes from base (base t=1 ≤ persisted_to_t=1)
        ("Alice".to_string(), 1, true),
        // t=2 retract+assert come from novelty ((index_t, to_t])
        ("Alice".to_string(), 2, false),
        ("Alice Smith".to_string(), 2, true),
    ];
    assert_eq!(
        rows, expected,
        "history merge across index_t boundary must include novelty events"
    );
}

// ---------------------------------------------------------------------------
// Case: subject-unbound history. No subject in the pattern; walks the
// branch (predicate-bound so leaflet p_const filter helps).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn history_range_subject_unbound() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-unbound-subject:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1: two subjects get names.
    let r1 = fluree
        .insert(
            ledger0,
            &json!({"@context": ctx(), "@graph": [
                {"@id": "ex:alice", "ex:name": "Alice"},
                {"@id": "ex:bob",   "ex:name": "Bob"},
            ]}),
        )
        .await
        .expect("tx1");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);

    // t=2: rename Alice only.
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice Smith"}),
        )
        .await
        .expect("tx2");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 2);

    // Subject is a variable; only predicate is bound.
    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t", "?op"],
        "where": [{
            "@id": "?s",
            "ex:name": {"@value": "?v", "@t": "?t", "@op": "?op"}
        }],
        "orderBy": ["?t", "?op", "?v"],
    });
    let rows = run_history_query(&fluree, &q).await;
    // t=1: Alice+assert, Bob+assert; t=2: Alice+retract, Alice Smith+assert
    // (false<true so retract sorts before assert at t=2).
    let expected: Vec<(String, i64, bool)> = vec![
        ("Alice".to_string(), 1, true),
        ("Bob".to_string(), 1, true),
        ("Alice".to_string(), 2, false),
        ("Alice Smith".to_string(), 2, true),
    ];
    assert_eq!(
        rows, expected,
        "subject-unbound history must walk all matching leaflets"
    );
}

// ---------------------------------------------------------------------------
// IRI-object regression coverage.
//
// The original fix only threaded `t` / `op` onto literal-valued objects.
// Ref-valued objects (rdf:type, foaf:knows, skos:inScheme, etc.) showed
// up in the result set with `?v` populated but `?t` and `?op` null,
// because `Binding::Sid` had no metadata channel. After making the Sid
// variant metadata-capable, the history scan must populate `t` / `op`
// for ref-valued objects too.
// ---------------------------------------------------------------------------

/// Helper: flatten a row whose `?v` is an IRI into `(iri: String, t: i64, op: bool)`.
fn flatten_iri_v_t_op(row: &serde_json::Value) -> (String, i64, bool) {
    let v = row
        .get("?v")
        .and_then(|x| x.get("@value").or(Some(x)))
        .and_then(|x| x.get("@id").or(Some(x)))
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
        .and_then(serde_json::Value::as_bool)
        .expect("?op should be a boolean");
    (v, t, op)
}

async fn run_iri_history_query(
    fluree: &fluree_db_api::Fluree,
    q: &serde_json::Value,
) -> Vec<(String, i64, bool)> {
    let result = fluree
        .query_from()
        .jsonld(q)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("history range query");
    let value = serde_json::to_value(&result.result).expect("serialize");
    value
        .as_array()
        .expect("rows array")
        .iter()
        .map(flatten_iri_v_t_op)
        .collect()
}

/// Sidecar + base case: `ex:knows` (ref-valued) over a span where the
/// initial assert lives in the persisted base columns and a later
/// retract+assert sit in the sidecar. Verifies that `?t` / `?op` are
/// populated identically for ref-valued and literal-valued objects.
#[tokio::test]
async fn history_range_iri_object_sidecar_plus_base() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-iri-sidecar:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    // t=1: alice knows bob (ref-valued).
    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:bob", "ex:name": "Bob"},
                    {"@id": "ex:carol", "ex:name": "Carol"},
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
                ],
            }),
        )
        .await
        .expect("tx1");
    assert_eq!(r1.receipt.t, 1);
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 1);

    // t=2: replace alice ex:knows bob → alice ex:knows carol.
    // Upsert retracts the previous ref and asserts the new one.
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:knows": {"@id": "ex:carol"},
            }),
        )
        .await
        .expect("tx2");
    assert_eq!(reindex_to_current(&fluree, ledger_id).await, 2);

    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t", "?op"],
        "where": [{
            "@id": "ex:alice",
            "ex:knows": {"@value": "?v", "@type": "@id", "@t": "?t", "@op": "?op"}
        }],
        "orderBy": ["?t", "?op", "?v"],
    });
    let rows = run_iri_history_query(&fluree, &q).await;
    let expected: Vec<(String, i64, bool)> = vec![
        ("ex:bob".to_string(), 1, true),
        ("ex:bob".to_string(), 2, false),
        ("ex:carol".to_string(), 2, true),
    ];
    assert_eq!(
        rows, expected,
        "history range over a ref-valued predicate must bind @t and @op"
    );
}

/// Novelty-only case: same ref-valued predicate but with no reindex,
/// so all assert / retract events stay in novelty. Verifies the
/// novelty branch of the history collector also threads metadata
/// through the ref binding.
#[tokio::test]
async fn history_range_iri_object_novelty_only() {
    let tmp = tempfile::tempdir().expect("create temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = "test/history-iri-novelty:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {"@id": "ex:bob", "ex:name": "Bob"},
                    {"@id": "ex:carol", "ex:name": "Carol"},
                    {"@id": "ex:alice", "ex:knows": {"@id": "ex:bob"}},
                ],
            }),
        )
        .await
        .expect("tx1");
    let _ = fluree
        .upsert(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:knows": {"@id": "ex:carol"},
            }),
        )
        .await
        .expect("tx2");

    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?v", "?t", "?op"],
        "where": [{
            "@id": "ex:alice",
            "ex:knows": {"@value": "?v", "@type": "@id", "@t": "?t", "@op": "?op"}
        }],
        "orderBy": ["?t", "?op", "?v"],
    });
    let rows = run_iri_history_query(&fluree, &q).await;
    let expected: Vec<(String, i64, bool)> = vec![
        ("ex:bob".to_string(), 1, true),
        ("ex:bob".to_string(), 2, false),
        ("ex:carol".to_string(), 2, true),
    ];
    assert_eq!(
        rows, expected,
        "novelty-only history over a ref-valued predicate must bind @t and @op"
    );
}
