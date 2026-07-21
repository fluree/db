//! JSON-LD parity tests for PR-W1 (algebra scope & subquery correlation).
//!
//! The W3C submodule only guards the SPARQL surface, so these are the JSON-LD
//! (FQL) analogues of the three W3C tests PR-W1 greens. Families A and B live
//! in the shared IR/executor (`fluree-db-query`'s `subquery.rs`/`optional.rs`),
//! so the JSON-LD front-end must exhibit the same scope/correlation semantics.
//! See `docs/audit/burn-down/algebra-serialization.md`.
//!
//!   A — a FILTER in a nested scope must not see an enclosing-scope variable
//!       (W3C `filter-nested-2`, `dawg-optional-filter-005-not-simplified`).
//!   B — a sub-SELECT that binds a correlation variable only via OPTIONAL must
//!       reconcile it against the parent at the join, not pin it to the parent
//!       value (W3C `var-scope-join-1`, aka join-scope-1).

use crate::support::{genesis_ledger, graphdb_from_ledger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({ "ex": "http://example.org/ns/" })
}

/// Family A — JSON-LD analogue of W3C `filter-nested-2`
/// (`{ :x :p ?v . { FILTER(?v = 1) } }` expects 0).
///
/// A FILTER inside a nested sub-SELECT references `?title`, which the sub-SELECT
/// does not select and therefore does not bind. Evaluated in its own scope
/// `?title` is unbound, so `(= ?title "T2")` is a type error → EBV false → the
/// sub-SELECT is empty → the join yields 0 rows. If the nested scope leaked the
/// enclosing `?title` (the defect PR-W1 fixes on the SPARQL surface), the FILTER
/// would match `ex:b2` and a row would survive.
#[tokio::test]
async fn nested_subquery_filter_does_not_see_enclosing_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "w1/parity:a1");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:b1", "ex:title": "T1"},
            {"@id": "ex:b2", "ex:title": "T2"}
        ]
    });
    let db = graphdb_from_ledger(&fluree.insert(ledger0, &txn).await.expect("seed").ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?book", "?title"],
        "where": [
            {"@id": "?book", "ex:title": "?title"},
            ["query", {
                "@context": ctx(),
                "select": ["?book"],
                "where": [
                    {"@id": "?book", "ex:title": "?t2"},
                    ["filter", "(= ?title \"T2\")"]
                ]
            }]
        ]
    });

    let rows = fluree
        .query(&db, &q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");

    assert_eq!(
        rows,
        json!([]),
        "the nested sub-SELECT's FILTER must see ?title as unbound (own scope), \
         so the whole query is empty; got {rows}"
    );
}

/// Family A — JSON-LD analogue of W3C `dawg-optional-filter-005-not-simplified`
/// (`{ ?book :title ?t . OPTIONAL { { ?book :price ?p . FILTER(?t = "TITLE 2") } } }`
/// expects every book title-only, no price).
///
/// The OPTIONAL wraps a nested sub-SELECT whose FILTER references the enclosing
/// `?title`. Because the sub-SELECT is an independent scope, `?title` is unbound
/// inside it → the FILTER errors → the OPTIONAL body is empty → no book is
/// assigned a price. (Were `?title` visible inside the nested scope, `ex:b2`
/// would wrongly acquire its price — the "not simplified" bug.)
#[tokio::test]
async fn optional_nested_filter_referencing_outer_var_does_not_bind() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "w1/parity:a2");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:b1", "ex:title": "T1", "ex:price": 10},
            {"@id": "ex:b2", "ex:title": "T2", "ex:price": 20}
        ]
    });
    let db = graphdb_from_ledger(&fluree.insert(ledger0, &txn).await.expect("seed").ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?book", "?title", "?price"],
        "where": [
            {"@id": "?book", "ex:title": "?title"},
            ["optional",
                ["query", {
                    "@context": ctx(),
                    "select": ["?book", "?price"],
                    "where": [
                        {"@id": "?book", "ex:price": "?price"},
                        ["filter", "(= ?title \"T2\")"]
                    ]
                }]
            ]
        ],
        "orderBy": "?title"
    });

    let rows = fluree
        .query(&db, &q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");

    // Both books present, title-only; the nested-scope FILTER never binds price.
    assert_eq!(
        rows,
        json!([["ex:b1", "T1", null], ["ex:b2", "T2", null]]),
        "the FILTER inside the nested OPTIONAL scope must not see ?title, so no \
         price is bound; got {rows}"
    );
}

/// Family B — JSON-LD analogue of W3C `var-scope-join-1` (join-scope-1)
/// (`{ ?X :name "paul" { ?Y :name "george" . OPTIONAL { ?X :email ?Z } } }`
/// expects 0).
///
/// The sub-SELECT binds `?x` only via an inner OPTIONAL (to the subjects that
/// have an `ex:email` — `ex:john`, `ex:ringo`), while the parent binds
/// `?x = ex:paul`, who has no email. SPARQL §18.2 evaluates the sub-SELECT
/// independently then joins on `?x`, so `ex:paul` ∉ {john, ringo} → 0 rows.
///
/// This exercises the Family B fix end-to-end on the JSON-LD surface: `?x` is a
/// correlation var the sub-SELECT does not self-produce, so it must NOT be
/// seeded (pinned to `ex:paul`) — it is produced independently and reconciled at
/// the merge, dropping the incompatible rows.
#[tokio::test]
async fn subselect_optional_bound_correlation_var_reconciles_to_empty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "w1/parity:b");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:paul", "ex:name": "paul"},
            {"@id": "ex:george", "ex:name": "george"},
            {"@id": "ex:john", "ex:email": "john@example.org"},
            {"@id": "ex:ringo", "ex:email": "ringo@example.org"}
        ]
    });
    let db = graphdb_from_ledger(&fluree.insert(ledger0, &txn).await.expect("seed").ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?x", "?y", "?z"],
        "where": [
            {"@id": "?x", "ex:name": "paul"},
            ["query", {
                "@context": ctx(),
                "select": ["?x", "?y", "?z"],
                "where": [
                    {"@id": "?y", "ex:name": "george"},
                    ["optional", {"@id": "?x", "ex:email": "?z"}]
                ]
            }]
        ]
    });

    let rows = fluree
        .query(&db, &q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");

    assert_eq!(
        rows,
        json!([]),
        "?x is bound only via the sub-SELECT's OPTIONAL (email subjects); it must \
         reconcile against the parent ?x = ex:paul and drop every row; got {rows}"
    );
}

/// Family B, TWO levels deep: the reconcile must compose through nested
/// sub-SELECTs. The innermost sub-SELECT binds the correlation var `?x` only
/// via OPTIONAL (email subjects — john/ringo); the middle sub-SELECT passes it
/// through; the parent binds `?x = ex:paul`. §18.4 compatible-mapping joins at
/// EACH level must drop the incompatible rows → 0 rows total. A regression
/// that seeds (pins) the var at either nesting level would leak `ex:paul`
/// through and return a row.
#[tokio::test]
async fn nested_subselects_reconcile_optional_bound_correlation_var() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "w1/parity:b-nested");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:paul", "ex:name": "paul"},
            {"@id": "ex:george", "ex:name": "george"},
            {"@id": "ex:john", "ex:email": "john@example.org"},
            {"@id": "ex:ringo", "ex:email": "ringo@example.org"}
        ]
    });
    let db = graphdb_from_ledger(&fluree.insert(ledger0, &txn).await.expect("seed").ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?x", "?y", "?z"],
        "where": [
            {"@id": "?x", "ex:name": "paul"},
            ["query", {
                "@context": ctx(),
                "select": ["?x", "?y", "?z"],
                "where": [
                    {"@id": "?y", "ex:name": "george"},
                    ["query", {
                        "@context": ctx(),
                        "select": ["?x", "?z"],
                        "where": [
                            {"@id": "?mid", "ex:name": "george"},
                            ["optional", {"@id": "?x", "ex:email": "?z"}]
                        ]
                    }]
                ]
            }]
        ]
    });

    let rows = fluree
        .query(&db, &q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");

    assert_eq!(
        rows,
        json!([]),
        "the OPTIONAL-bound ?x must reconcile against the parent through BOTH \
         nesting levels (john/ringo vs ex:paul → incompatible); got {rows}"
    );
}

/// Family B + aggregation: a correlation var used as the GROUP BY key of a
/// correlated sub-SELECT. The sub-SELECT groups emails per subject `?x`
/// independently; joining with the parent's `?x = ex:john` must keep exactly
/// john's group (count 2), not pin the grouping to the parent binding or leak
/// other groups.
#[tokio::test]
async fn subselect_group_by_correlation_var_joins_per_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "w1/parity:b-groupby");
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:john", "ex:name": "john",
             "ex:email": ["john@a.org", "john@b.org"]},
            {"@id": "ex:ringo", "ex:name": "ringo", "ex:email": "ringo@a.org"}
        ]
    });
    let db = graphdb_from_ledger(&fluree.insert(ledger0, &txn).await.expect("seed").ledger);

    let q = json!({
        "@context": ctx(),
        "select": ["?x", "?n"],
        "where": [
            {"@id": "?x", "ex:name": "john"},
            ["query", {
                "@context": ctx(),
                "select": ["?x", "(as (count ?e) ?n)"],
                "where": [{"@id": "?x", "ex:email": "?e"}],
                "groupBy": ["?x"]
            }]
        ]
    });

    let rows = fluree
        .query(&db, &q)
        .await
        .expect("query")
        .to_jsonld_async(db.as_graph_db_ref())
        .await
        .expect("format");

    assert_eq!(
        rows,
        json!([["ex:john", 2]]),
        "the grouped sub-SELECT must join on ?x per §18.2 (john's group only, \
         count of BOTH emails); got {rows}"
    );
}
