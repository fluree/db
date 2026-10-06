//! Wildcard (variable-predicate) visibility of `f:`-namespace data.
//!
//! User-authored data in the Fluree namespace (e.g. stored `f:AccessPolicy`
//! definitions) must be visible to `?s ?p ?o` scans like any other triple —
//! before and after indexing. Historically the default graph hid the whole
//! `f:` namespace from variable-predicate scans (a fossil from the era when
//! commit metadata was stored in the main graph), which made policy nodes
//! look truncated to a wildcard dump and made results flip when the
//! background indexer caught up. Only the `f:reifies*` annotation-encoding
//! predicates remain hidden (covered by `it_edge_annotations.rs`).

use crate::support;
use crate::support::{genesis_ledger, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn policy_tx() -> JsonValue {
    json!({
        "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/ns/"},
        "@graph": [{
            "@id": "ex:p1",
            "@type": ["f:AccessPolicy"],
            "f:action": [{"@id": "f:view"}],
            "f:allow": true,
            "f:exMessage": "visible to wildcards"
        }]
    })
}

async fn wildcard_predicates(fluree: &MemoryFluree, ledger: &MemoryLedger) -> Vec<String> {
    let q = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "select": ["?p", "?o"],
        "where": [{"@id": "ex:p1", "?p": "?o"}]
    });
    let rows = support::query_jsonld_formatted(fluree, ledger, &q)
        .await
        .expect("wildcard query");
    let mut preds: Vec<String> = rows
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|row| row.as_array())
        .filter_map(|cols| cols.first())
        .filter_map(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.get("@id").and_then(|i| i.as_str()).map(String::from))
        })
        .collect();
    preds.sort();
    preds
}

fn expected_policy_predicates() -> Vec<String> {
    let mut v = vec![
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string(),
        "https://ns.flur.ee/db#action".to_string(),
        "https://ns.flur.ee/db#allow".to_string(),
        "https://ns.flur.ee/db#exMessage".to_string(),
    ];
    v.sort();
    v
}

/// Pre-index (novelty-only): user `f:` data is visible to wildcard scans.
#[tokio::test]
async fn wildcard_returns_user_fluree_ns_data_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "wildcard-sysfacts/novelty");
    let ledger = fluree
        .insert(ledger0, &policy_tx())
        .await
        .expect("insert")
        .ledger;
    assert_eq!(
        wildcard_predicates(&fluree, &ledger).await,
        expected_policy_predicates()
    );
}

/// Post-index: identical result — visibility must not flip when the
/// background indexer catches up (the historical hide only applied on the
/// indexed scan path, so the same query changed answers after indexing).
#[tokio::test]
async fn wildcard_returns_user_fluree_ns_data_indexed() {
    use fluree_db_api::ReindexOptions;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "wildcard-sysfacts/indexed";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree.insert(ledger0, &policy_tx()).await.expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("load indexed");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "expected binary range provider after reindex"
    );
    assert_eq!(
        wildcard_predicates(&fluree, &ledger).await,
        expected_policy_predicates()
    );
}

/// Commit metadata lives in the txn-meta graph, not the default graph, so a
/// full default-graph wildcard dump stays free of system bookkeeping even
/// with the namespace hide removed.
#[tokio::test]
async fn wildcard_dump_carries_no_commit_metadata() {
    use fluree_db_api::ReindexOptions;
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "wildcard-sysfacts/no-commit-meta";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree.insert(ledger0, &policy_tx()).await.expect("insert");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("load indexed");

    let q = json!({
        "select": ["?s", "?p", "?o"],
        "where": [{"@id": "?s", "?p": "?o"}]
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &q)
        .await
        .expect("dump query");
    let commit_meta = [
        "address", "alias", "time", "t", "asserts", "retracts", "size",
    ];
    for row in rows.as_array().expect("array") {
        let p = row.as_array().and_then(|c| c.get(1)).and_then(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.get("@id").and_then(|i| i.as_str()).map(String::from))
        });
        if let Some(p) = p {
            if let Some(local) = p.strip_prefix("https://ns.flur.ee/db#") {
                assert!(
                    !commit_meta.contains(&local),
                    "commit metadata predicate {p} leaked into default-graph wildcard dump"
                );
            }
        }
    }
}
