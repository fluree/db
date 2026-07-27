//! Regression tests: correlation variables a sub-SELECT produces via an
//! aggregate (or BIND) must still be join-checked against the parent's binding
//! (issue #1388).
//!
//! When a sub-SELECT exposes a correlation variable through `(COUNT(?x) AS ?c)`
//! rather than a top-level triple, the variable is not a hash-join key (the
//! subquery does not self-produce it in its WHERE body), so the natural join
//! must be enforced at merge time — a row whose aggregate value conflicts with
//! the parent's binding is dropped, while an unbound side stays compatible and
//! adopts the bound side's value, per SPARQL §18.4 compatible mappings.
//!
//! All inserts and queries are explicit with `@context` / `PREFIX`.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// `ex:a` has `ex:p 2` and two `ex:r` links (COUNT = 2, matches).
/// `ex:b` has `ex:p 5` and one `ex:r` link (COUNT = 1, does not match).
/// `ex:c` has no `ex:p` and three `ex:r` links (parent side unbound).
async fn seed_counts(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a", "ex:name": "a", "ex:p": 2,
             "ex:r": [{"@id": "ex:x1"}, {"@id": "ex:x2"}]},
            {"@id": "ex:b", "ex:name": "b", "ex:p": 5,
             "ex:r": [{"@id": "ex:y1"}]},
            {"@id": "ex:c", "ex:name": "c",
             "ex:r": [{"@id": "ex:z1"}, {"@id": "ex:z2"}, {"@id": "ex:z3"}]}
        ]
    });
    fluree.insert(ledger0, &insert).await.unwrap().ledger
}

async fn sparql_rows(fluree: &MemoryFluree, ledger: &MemoryLedger, q: &str) -> serde_json::Value {
    support::query_sparql(fluree, ledger, q)
        .await
        .expect("sparql query should succeed")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

/// Issue #1388, verbatim shape: the aggregate-produced `?c` must join against
/// the parent's `?c` binding, keeping only `ex:a` (COUNT 2 = `ex:p` 2).
#[tokio::test]
async fn sparql_subselect_aggregate_correlation_var_is_joined() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:aggregate").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {
          ?s ex:p ?c .
          { SELECT ?s (COUNT(?x) AS ?c) WHERE { ?s ex:r ?x } GROUP BY ?s }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a"]])),
        "aggregate-produced ?c must be join-checked against the parent, got {rows}"
    );
}

/// Same shape with the sub-SELECT written first: the parent triple then joins
/// after the subquery, which must produce the identical result.
#[tokio::test]
async fn sparql_subselect_aggregate_correlation_var_order_independent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:aggregate-swap").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {
          { SELECT ?s (COUNT(?x) AS ?c) WHERE { ?s ex:r ?x } GROUP BY ?s }
          ?s ex:p ?c .
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a"]])),
        "aggregate-produced ?c must be join-checked against the parent, got {rows}"
    );
}

/// A scalar (ungrouped) aggregate correlation var: the sub-SELECT produces one
/// row with the total `ex:r` count (6), so only the parent row whose `ex:p`
/// equals 6 survives the join — here, none.
#[tokio::test]
async fn sparql_subselect_scalar_aggregate_correlation_var_is_joined() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:scalar").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {
          ?s ex:p ?c .
          { SELECT (COUNT(?x) AS ?c) WHERE { ?any ex:r ?x } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([])),
        "no parent ex:p equals the total count 6, got {rows}"
    );
}

/// Unbound-compatible merge: `ex:c` has no `ex:p`, so its parent `?c` is
/// unbound — compatible with the subquery's COUNT, which fills the value in.
/// `ex:a` matches (2 = 2), `ex:b` conflicts (5 ≠ 1) and is dropped.
#[tokio::test]
async fn sparql_subselect_aggregate_correlation_var_unbound_parent_is_compatible() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:unbound").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s ?c WHERE {
          ?s ex:name ?n .
          OPTIONAL { ?s ex:p ?c }
          { SELECT ?s (COUNT(?x) AS ?c) WHERE { ?s ex:r ?x } GROUP BY ?s }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a", 2], ["ex:c", 3]])),
        "unbound parent ?c must adopt the subquery count; bound conflicts drop, got {rows}"
    );
}

/// End-to-end correctness for a BIND-produced correlation variable: `?c` is
/// bound by `BIND(2 AS ?c)` in the sub-SELECT and the result must agree with
/// the parent's `?c`. The sub-SELECT emits `(ex:a, 2)` once per `ex:r` link,
/// so `ex:a` appears twice (multiset join semantics); `ex:b` (parent `?c` = 5)
/// does not agree and is dropped.
///
/// NOTE: unlike the aggregate cases above, this shape eliminates `ex:b`
/// *before* the merge-time reconcile check — disabling that check (subquery.rs
/// `process_parent_batch`) leaves this test green — so it is an end-to-end
/// correctness assertion, not a regression guard for the reconcile mechanism.
/// The aggregate cases here, and the OPTIONAL Family-B tests in
/// `it_query_filter_scope.rs`, pin the reconcile merge itself.
#[tokio::test]
async fn sparql_subselect_bind_correlation_var_end_to_end() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:bind").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?s WHERE {
          ?s ex:p ?c .
          { SELECT ?s ?c WHERE { ?s ex:r ?x . BIND(2 AS ?c) } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a"], ["ex:a"]])),
        "BIND-produced ?c must be join-checked against the parent, got {rows}"
    );
}

/// JSON-LD `["query", ...]` surface parity for the issue-#1388 shape: the
/// correlated (per-row seeded) path must enforce the same merge-time check on
/// the aggregate-produced `?c`.
#[tokio::test]
async fn jsonld_subquery_aggregate_correlation_var_is_joined() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_counts(&fluree, "subsel-corr:jsonld").await;

    let ctx = json!({"ex": "http://example.org/"});
    let q = json!({
        "@context": ctx,
        "select": ["?s"],
        "where": [
            {"@id": "?s", "ex:p": "?c"},
            ["query", {
                "@context": ctx,
                "select": ["?s", "(as (count ?x) ?c)"],
                "where": {"@id": "?s", "ex:r": "?x"},
                "groupBy": ["?s"]
            }]
        ]
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .expect("jsonld query should succeed")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:a"]])),
        "JSON-LD subquery aggregate ?c must be join-checked against the parent, got {rows}"
    );
}
