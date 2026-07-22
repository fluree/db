//! End-to-end coverage for the SPARQL history surface.
//!
//! `fluree-db-sparql` had parser-level tests for the legacy RDF-star form
//! (`<< s p ?o >> f:t ?t`) but no test that ran one against a real ledger.
//! The lowering path in `fluree-db-sparql/src/lower/rdf_star.rs` was
//! therefore uncovered end-to-end, and the docs drifted away from it.
//!
//! These tests pin the documented behaviour in `docs/query/sparql.md`
//! and `docs/guides/cookbook-time-travel.md`.

#![cfg(feature = "native")]

use fluree_db_api::{FlureeBuilder, FormatterConfig};
use serde_json::{json, Value};

fn ctx() -> Value {
    json!({ "ex": "http://example.org/" })
}

/// Ledger with `ex:alice ex:name` asserted at t=1 and overwritten at t=2.
async fn alice_ledger(name: &str) -> (tempfile::TempDir, fluree_db_api::Fluree, String) {
    let tmp = tempfile::tempdir().expect("temp dir");
    let fluree = FlureeBuilder::file(tmp.path().to_str().unwrap())
        .build()
        .expect("build");
    let ledger_id = format!("test/history-sparql-{name}:main");

    let l0 = fluree.create_ledger(&ledger_id).await.expect("create");
    let r1 = fluree
        .insert(
            l0,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alice"}),
        )
        .await
        .expect("t=1");
    assert_eq!(r1.receipt.t, 1);
    let r2 = fluree
        .upsert(
            r1.ledger,
            &json!({"@context": ctx(), "@id": "ex:alice", "ex:name": "Alicia"}),
        )
        .await
        .expect("t=2");
    assert_eq!(r2.receipt.t, 2);

    (tmp, fluree, ledger_id)
}

async fn run_sparql(fluree: &fluree_db_api::Fluree, sparql: &str) -> Value {
    let result = fluree
        .query_from()
        .sparql(sparql)
        .format(FormatterConfig::typed_json().with_normalize_arrays())
        .execute_tracked()
        .await
        .expect("sparql history query");
    serde_json::to_value(&result.result).expect("serialize")
}

/// Pull `(?name, ?t, ?op)` out of a typed-json binding row.
fn row_name_t_op(row: &Value) -> (String, i64, bool) {
    let name = row["?name"]["@value"].as_str().expect("?name").to_string();
    let t = row["?t"]["@value"].as_i64().expect("?t bound");
    let op = row["?op"]["@value"].as_bool().expect("?op bound");
    (name, t, op)
}

const EXPECTED: [(&str, i64, bool); 3] = [
    ("Alice", 1, true),  // original assert
    ("Alice", 2, false), // retracted by the upsert
    ("Alicia", 2, true), // new value
];

/// The documented history form: `FROM <l@t:1> TO <l@t:latest>` with the
/// legacy RDF-star quoted triple binding `f:t` and `f:op`.
#[tokio::test]
async fn rdf_star_history_binds_t_and_op() {
    let (_tmp, fluree, ledger_id) = alice_ledger("basic").await;

    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>
SELECT ?name ?t ?op
FROM <{ledger_id}@t:1>
TO <{ledger_id}@t:latest>
WHERE {{
  << ex:alice ex:name ?name >> f:t ?t .
  << ex:alice ex:name ?name >> f:op ?op .
}}
ORDER BY ?t ?op"
    );

    let rows = run_sparql(&fluree, &sparql).await;
    let got: Vec<(String, i64, bool)> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(row_name_t_op)
        .collect();

    let want: Vec<(String, i64, bool)> = EXPECTED
        .iter()
        .map(|(n, t, o)| (n.to_string(), *t, *o))
        .collect();
    assert_eq!(got, want, "rows: {rows:#?}");
}

/// The `;`-continued form binds one inner triple, not two. Same results.
#[tokio::test]
async fn rdf_star_history_semicolon_form() {
    let (_tmp, fluree, ledger_id) = alice_ledger("semicolon").await;

    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>
SELECT ?name ?t ?op
FROM <{ledger_id}@t:1>
TO <{ledger_id}@t:latest>
WHERE {{ << ex:alice ex:name ?name >> f:t ?t ; f:op ?op . }}
ORDER BY ?t ?op"
    );

    let rows = run_sparql(&fluree, &sparql).await;
    let got: Vec<(String, i64, bool)> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(row_name_t_op)
        .collect();

    let want: Vec<(String, i64, bool)> = EXPECTED
        .iter()
        .map(|(n, t, o)| (n.to_string(), *t, *o))
        .collect();
    assert_eq!(got, want, "rows: {rows:#?}");
}

/// `f:op` is a boolean, so retractions are selected with `= false`.
/// (`"retract"` as a string matches nothing — see the gotchas section of
/// docs/guides/cookbook-time-travel.md.)
#[tokio::test]
async fn rdf_star_history_op_filter_is_boolean() {
    let (_tmp, fluree, ledger_id) = alice_ledger("opfilter").await;

    let query = |pred: &str| {
        format!(
            r"PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>
SELECT ?name ?t ?op
FROM <{ledger_id}@t:1>
TO <{ledger_id}@t:latest>
WHERE {{
  << ex:alice ex:name ?name >> f:t ?t ; f:op ?op .
  FILTER({pred})
}}"
        )
    };

    let retracts = run_sparql(&fluree, &query("?op = false")).await;
    let got: Vec<(String, i64, bool)> = retracts
        .as_array()
        .expect("rows")
        .iter()
        .map(row_name_t_op)
        .collect();
    assert_eq!(got, vec![("Alice".to_string(), 2, false)]);

    // The string form is a type mismatch, not a synonym: it matches nothing.
    let as_string = run_sparql(&fluree, &query(r#"?op = "retract""#)).await;
    assert_eq!(
        as_string.as_array().expect("rows").len(),
        0,
        "f:op is xsd:boolean; string constants must not match"
    );
}

/// The quoted triple's object must be a variable — there is nothing to
/// hang the metadata binding off otherwise. This must be a clean error,
/// not silently-null metadata.
#[tokio::test]
async fn rdf_star_history_requires_variable_object() {
    let (_tmp, fluree, ledger_id) = alice_ledger("constobj").await;

    let sparql = format!(
        r#"PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>
SELECT ?t
FROM <{ledger_id}@t:1>
TO <{ledger_id}@t:latest>
WHERE {{ << ex:alice ex:name "Alice" >> f:t ?t . }}"#
    );

    let err = fluree
        .query_from()
        .sparql(&sparql)
        .execute_tracked()
        .await
        .expect_err("constant object must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("variable"),
        "expected a variable-object diagnostic, got: {msg}"
    );
}

/// Documents a known sharp edge: selecting `?t` / `?op` in history mode
/// WITHOUT the `<< ... >> f:t|f:op` bindings succeeds and returns history
/// rows with both metadata columns null, giving no way to tell an assert
/// from a retract.
///
/// This is a regression lock on *current* behaviour, not an endorsement of
/// it. If unbound history metadata is ever promoted to a warning or an
/// error, update this test — do not delete the coverage.
#[tokio::test]
async fn history_metadata_vars_without_bindings_are_null() {
    let (_tmp, fluree, ledger_id) = alice_ledger("unbound").await;

    let sparql = format!(
        r"PREFIX ex: <http://example.org/>
SELECT ?name ?t ?op
FROM <{ledger_id}@t:1>
TO <{ledger_id}@t:latest>
WHERE {{ ex:alice ex:name ?name . }}"
    );

    let rows = run_sparql(&fluree, &sparql).await;
    let rows = rows.as_array().expect("rows");
    assert_eq!(rows.len(), 3, "history rows are still emitted: {rows:#?}");
    for row in rows {
        assert!(row["?t"].is_null(), "?t is unbound without f:t: {row}");
        assert!(row["?op"].is_null(), "?op is unbound without f:op: {row}");
    }
}
