//! Regression tests: SPARQL sub-SELECT as an operand of a set operation.
//!
//! Fixes azure-chat #42 (sub-SELECT in UNION) and #43 (sub-SELECT in MINUS),
//! plus the broader family (OPTIONAL, mixed arms) sharing the same parser root
//! cause: `parse_group_graph_pattern` only recognised a `{ SELECT ... }`
//! sub-SELECT inside its own nested-`{` branch, so a sub-SELECT used as a set-op
//! operand was mis-parsed — UNION dropped the operator, MINUS/OPTIONAL dropped
//! the `(expr AS ?v)` projection. See `fluree-db-sparql` parser tests for the
//! AST-level guards; these assert the end-to-end query results.
//!
//! All inserts and queries are explicit with `@context` / `PREFIX`.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// 3 Person, 2 Company, 5 Country, 2 Maker (France + Japan have a maker).
/// This is the exact dataset from the azure-chat repro.
async fn seed_setop(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:alice",  "@type": "ex:Person",  "ex:name": "Alice"},
            {"@id": "ex:bob",    "@type": "ex:Person",  "ex:name": "Bob"},
            {"@id": "ex:carol",  "@type": "ex:Person",  "ex:name": "Carol"},
            {"@id": "ex:acme",   "@type": "ex:Company", "ex:name": "Acme"},
            {"@id": "ex:globex", "@type": "ex:Company", "ex:name": "Globex"},
            {"@id": "ex:france",  "@type": "ex:Country", "ex:cname": "France"},
            {"@id": "ex:germany", "@type": "ex:Country", "ex:cname": "Germany"},
            {"@id": "ex:japan",   "@type": "ex:Country", "ex:cname": "Japan"},
            {"@id": "ex:brazil",  "@type": "ex:Country", "ex:cname": "Brazil"},
            {"@id": "ex:canada",  "@type": "ex:Country", "ex:cname": "Canada"},
            {"@id": "ex:maker1", "@type": "ex:Maker", "ex:inCountry": {"@id": "ex:france"}},
            {"@id": "ex:maker2", "@type": "ex:Maker", "ex:inCountry": {"@id": "ex:japan"}}
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

/// azure-chat #42, verbatim: `{ SELECT (?pn AS ?n) } UNION { SELECT (?cn AS ?n) }`
/// must return the union of both arms, not just one.
#[tokio::test]
async fn sparql_subselect_union_returns_all_arms() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:union").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { SELECT (?pn AS ?n) WHERE { ?p a ex:Person ; ex:name ?pn } }
          UNION
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Company ; ex:name ?cn } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Alice"],
            ["Bob"],
            ["Carol"],
            ["Acme"],
            ["Globex"]
        ])),
        "sub-SELECT UNION must return both arms, got {rows}"
    );
}

/// The surviving arm was cardinality-dependent (smaller arm won), so swapping
/// the arms must not change the result.
#[tokio::test]
async fn sparql_subselect_union_is_order_independent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:union-swap").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Company ; ex:name ?cn } }
          UNION
          { SELECT (?pn AS ?n) WHERE { ?p a ex:Person ; ex:name ?pn } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Alice"],
            ["Bob"],
            ["Carol"],
            ["Acme"],
            ["Globex"]
        ]))
    );
}

/// The sub-SELECT-wrapped UNION must agree with the semantically identical flat
/// UNION on the same data (the flat form was always correct).
#[tokio::test]
async fn sparql_subselect_union_matches_flat_union() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:union-vs-flat").await;

    let subselect = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { SELECT (?pn AS ?n) WHERE { ?p a ex:Person ; ex:name ?pn } }
          UNION
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Company ; ex:name ?cn } }
        }
    ";
    let flat = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { ?p a ex:Person ; ex:name ?n }
          UNION
          { ?c a ex:Company ; ex:name ?n }
        }
    ";

    let sub_rows = sparql_rows(&fluree, &ledger, subselect).await;
    let flat_rows = sparql_rows(&fluree, &ledger, flat).await;
    assert_eq!(normalize_rows(&sub_rows), normalize_rows(&flat_rows));
}

/// Either arm may be a sub-SELECT: a plain-group left arm with a sub-SELECT
/// right arm must still union both (the right arm previously produced a stray
/// unbound row instead of the company names).
#[tokio::test]
async fn sparql_subselect_union_mixed_arms() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:union-mixed").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { ?p a ex:Person ; ex:name ?n }
          UNION
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Company ; ex:name ?cn } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([
            ["Alice"],
            ["Bob"],
            ["Carol"],
            ["Acme"],
            ["Globex"]
        ]))
    );
}

/// azure-chat #43, verbatim: `{ SELECT (?cn AS ?n) } MINUS { SELECT (?mn AS ?n) }`
/// must subtract the right arm (countries with a maker) from the left.
#[tokio::test]
async fn sparql_subselect_minus_subtracts() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:minus").await;

    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Country ; ex:cname ?cn } }
          MINUS
          { SELECT (?mn AS ?n) WHERE { ?c2 a ex:Country ; ex:cname ?mn . ?m a ex:Maker ; ex:inCountry ?c2 } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["Brazil"], ["Canada"], ["Germany"]])),
        "sub-SELECT MINUS must subtract France + Japan, got {rows}"
    );
}

/// The sub-SELECT MINUS must agree with the semantically identical
/// FILTER NOT EXISTS form (which was always correct).
#[tokio::test]
async fn sparql_subselect_minus_matches_filter_not_exists() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:minus-vs-fne").await;

    let minus = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          { SELECT (?cn AS ?n) WHERE { ?c a ex:Country ; ex:cname ?cn } }
          MINUS
          { SELECT (?mn AS ?n) WHERE { ?c2 a ex:Country ; ex:cname ?mn . ?m a ex:Maker ; ex:inCountry ?c2 } }
        }
    ";
    let fne = r"
        PREFIX ex: <http://example.org/>
        SELECT DISTINCT ?n WHERE {
          ?c a ex:Country ; ex:cname ?n .
          FILTER NOT EXISTS { ?m a ex:Maker ; ex:inCountry ?c }
        }
    ";

    let minus_rows = sparql_rows(&fluree, &ledger, minus).await;
    let fne_rows = sparql_rows(&fluree, &ledger, fne).await;
    assert_eq!(normalize_rows(&minus_rows), normalize_rows(&fne_rows));
}

/// A sub-SELECT inside OPTIONAL must keep its `(expr AS ?v)` projection so the
/// aggregate value binds. Previously `?cnt` was dropped and rows duplicated.
#[tokio::test]
async fn sparql_subselect_in_optional_binds_projection() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_setop(&fluree, "setop:optional").await;

    // The sub-SELECT shares no variable with the outer query, so it is an
    // UNCORRELATED aggregate: it evaluates once (COUNT of companies = 2) and that
    // single row broadcasts to each of the 3 people. This is correct SPARQL
    // semantics and the point of the test — do not "fix" it into a per-person
    // correlated count.
    let q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?n ?cnt WHERE {
          ?p a ex:Person ; ex:name ?n .
          OPTIONAL { SELECT (COUNT(?c) AS ?cnt) WHERE { ?c a ex:Company } }
        }
    ";

    let rows = sparql_rows(&fluree, &ledger, q).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["Alice", 2], ["Bob", 2], ["Carol", 2]])),
        "OPTIONAL sub-SELECT must bind ?cnt=2 for each person, got {rows}"
    );
}
