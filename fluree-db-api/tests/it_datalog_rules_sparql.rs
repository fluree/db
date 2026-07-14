//! SPARQL datalog rule integration tests
//!
//! `f:rule` datalog rules can be written as SPARQL `CONSTRUCT ... WHERE ...`
//! queries by storing the literal with the `f:sparql` datatype. The
//! CONSTRUCT template is the rule head (insert); the WHERE clause is the
//! rule body.

use crate::support;
use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Grandparent derivation via a SPARQL CONSTRUCT rule — the SPARQL twin of
/// `datalog_grandparent_rule` in `it_datalog_rules.rs`.
#[tokio::test]
async fn sparql_rule_grandparent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/sparql-grandparent");

    let rule_data = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@graph": [
            {
                "@id": "http://example.org/grandparentRule",
                "f:rule": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { ?person ex:grandparent ?grandparent } \
                               WHERE { ?person ex:parent ?p . ?p ex:parent ?grandparent }"
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let family_data = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
            {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
        ]
    });
    let ledger = fluree.insert(ledger, &family_data).await.unwrap().ledger;

    let q = json!({
        "@context": { "ex": "http://example.org/" },
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:charlie")),
        "Alice should have grandparent Charlie via SPARQL rule, got {results:?}"
    );
}

/// SPARQL rule with a FILTER comparison in the body.
#[tokio::test]
async fn sparql_rule_with_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/sparql-filter");

    let rule_data = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@graph": [
            {
                "@id": "http://example.org/seniorRule",
                "f:rule": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
                               CONSTRUCT { ?person rdf:type ex:Senior } \
                               WHERE { ?person ex:age ?age FILTER(?age >= 62) }"
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let people = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [
            {"@id": "ex:alice", "ex:age": 70},
            {"@id": "ex:bob", "ex:age": 30}
        ]
    });
    let ledger = fluree.insert(ledger, &people).await.unwrap().ledger;

    let q = json!({
        "@context": { "ex": "http://example.org/" },
        "select": "?person",
        "where": {"@id": "?person", "@type": "ex:Senior"},
        "reasoning": "datalog"
    });

    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    let results = normalize_rows(&rows);

    assert!(
        results.contains(&json!("ex:alice")),
        "Alice (70) should be derived as Senior, got {results:?}"
    );
    assert!(
        !results.contains(&json!("ex:bob")),
        "Bob (30) must not be derived as Senior, got {results:?}"
    );
}

/// A SPARQL rule using constructs the datalog engine cannot execute
/// (OPTIONAL) is skipped with a warning — it must not derive anything and
/// must not break other rules.
#[tokio::test]
async fn sparql_rule_unsupported_construct_skipped() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "datalog/sparql-unsupported");

    let rule_data = json!({
        "@context": { "f": "https://ns.flur.ee/db#" },
        "@graph": [
            {
                "@id": "http://example.org/badRule",
                "f:rule": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { ?x ex:derived true } \
                               WHERE { ?x ex:a ?y OPTIONAL { ?x ex:b ?z } }"
                }
            },
            {
                "@id": "http://example.org/goodRule",
                "f:rule": {
                    "@type": "https://ns.flur.ee/db#sparql",
                    "@value": "PREFIX ex: <http://example.org/> \
                               CONSTRUCT { ?x ex:hasA true } \
                               WHERE { ?x ex:a ?y }"
                }
            }
        ]
    });
    let ledger = fluree.insert(ledger0, &rule_data).await.unwrap().ledger;

    let data = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [ {"@id": "ex:thing", "ex:a": 1} ]
    });
    let ledger = fluree.insert(ledger, &data).await.unwrap().ledger;

    // The good rule still derives; the bad rule derives nothing.
    let q = json!({
        "@context": { "ex": "http://example.org/" },
        "select": "?x",
        "where": {"@id": "?x", "ex:hasA": true},
        "reasoning": "datalog"
    });
    let rows = support::query_jsonld(&fluree, &ledger, &q)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert!(
        normalize_rows(&rows).contains(&json!("ex:thing")),
        "good rule should still run when a sibling rule is unsupported, got {rows:?}"
    );

    let q_bad = json!({
        "@context": { "ex": "http://example.org/" },
        "select": ["?x", "?v"],
        "where": {"@id": "?x", "ex:derived": "?v"},
        "reasoning": "datalog"
    });
    let rows_bad = support::query_jsonld(&fluree, &ledger, &q_bad)
        .await
        .unwrap()
        .to_jsonld(&ledger.snapshot)
        .unwrap();
    assert_eq!(
        rows_bad.as_array().map(Vec::len),
        Some(0),
        "unsupported rule must be skipped, not partially applied"
    );
}
