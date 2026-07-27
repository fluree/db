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
/// `ex:d` has `ex:p 6` — equal to the TOTAL `ex:r` count (2 + 1 + 3 = 6) — and
/// no `ex:r` links, so it is the sole match for the scalar-aggregate test.
/// Having no `ex:r`, it never appears in the per-subject subqueries (they
/// require an `ex:r` join), so it leaves every other test's result unchanged.
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
             "ex:r": [{"@id": "ex:z1"}, {"@id": "ex:z2"}, {"@id": "ex:z3"}]},
            {"@id": "ex:d", "ex:name": "d", "ex:p": 6}
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
/// equals 6 survives the join — here, `ex:d`. Asserting a specific row (not
/// merely the absence of rows) also catches a regression where the scalar
/// subquery path stops producing at all.
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
        normalize_rows(&json!([["ex:d"]])),
        "only ex:d (ex:p 6) equals the total count 6, got {rows}"
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

/// End-to-end (multiset-semantics) assertion for a BIND'd sub-SELECT
/// projection. `?c` is bound by `BIND(2 AS ?c)` and the result must agree with
/// the parent's `?c`: the sub-SELECT emits `(ex:a, 2)` once per `ex:r` link, so
/// `ex:a` appears twice (`normalize_rows` sorts but does not dedupe, so the
/// duplicate has teeth); `ex:b` (parent `?c` = 5) does not agree and is dropped.
///
/// NOTE: this does NOT exercise the merge-time reconcile check. On this
/// fixture the sub-SELECT plans UNCORRELATED — the `SubqueryOperator` builds
/// with `correlation_vars=[]` / `reconcile_vars=[]` (`?c` never becomes a
/// correlation variable), so `reconcile_vars.is_empty()` short-circuits the
/// check at `subquery.rs:472` and `ex:b` is dropped by the outer `?s ex:p ?c`
/// join instead. Disabling the reconcile check leaves this test green. The
/// reconcile mechanism itself is pinned by the aggregate cases above and the
/// OPTIONAL Family-B tests in `it_query_filter_scope.rs`.
#[tokio::test]
async fn sparql_subselect_bind_projection_multiset() {
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
        "BIND'd ?c must agree with the parent, keeping ex:a twice (multiset), got {rows}"
    );
}

/// JSON-LD `["query", ...]` surface parity for the issue-#1388 shape: the
/// aggregate-produced `?c` must be reconciled at merge time, the same as the
/// SPARQL sub-SELECT above.
///
/// NOTE: on this fixture this runs `join_mode=true`, NOT the per-row seeded
/// route. The sub-SELECT builds `uncorrelated=false, est_rows=None`, so
/// `must_materialize_once` is false and the cardinality guard
/// (`child.estimated_rows().is_none_or(|n| n >= SUBQUERY_MATERIALIZE_MIN_PARENT_ROWS)`,
/// subquery.rs) resolves the `None` estimate to join-mode — like every other
/// test here. The reconcile check is load-bearing in join-mode too, so this
/// still exercises it (disabling the check turns this test red). The per-row
/// seeded route — the one c0f63e4c3 singles out as needing the seed fix — is
/// covered by the Family-B tests in `it_query_filter_scope.rs`; a parent whose
/// child reports a concrete estimate below the threshold would flip this test
/// onto it.
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
