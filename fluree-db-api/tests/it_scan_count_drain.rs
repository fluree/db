//! Routing for a variable-predicate `COUNT(*)` under novelty: the scan
//! counts its cursor without binding rows unless the ledger holds legacy
//! `f:reifies*` bundles.
//!
//! The only test in its own `[[test]]` binary, as the other routing-stamp
//! tests are: tracing's callsite interest and the `set_default` capture are
//! process- and thread-shared, and a bundled sibling that installs a global
//! subscriber leaves this capture empty.
#![cfg(feature = "native")]

mod support;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::genesis_ledger;

fn ctx() -> JsonValue {
    json!({"ex": "http://example.org/"})
}

/// The `scan-count-drain` outcomes `query` stamps on `ledger`.
async fn scan_count_drain_outcomes(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    query: &str,
) -> (JsonValue, Vec<String>) {
    // Register the stamp callsite before this thread's subscriber reads it.
    let _ = support::query_sparql_formatted(fluree, ledger, query).await;
    let (store, _guard) = support::span_capture::init_test_tracing();
    tracing::callsite::rebuild_interest_cache();
    let result = support::query_sparql_formatted(fluree, ledger, query)
        .await
        .expect("count");
    let outcomes = store
        .find_events("fast-path outcome")
        .iter()
        .filter(|e| e.fields.get("site").map(String::as_str) == Some("scan-count-drain"))
        .filter_map(|e| e.fields.get("outcome").cloned())
        .collect();
    (result, outcomes)
}

/// A variable-predicate `COUNT(*)` under novelty counts the scan's cursor
/// without binding a row unless the ledger holds legacy bundles, whose rows
/// the scan has to drop one by one. The index knows `ex:worksFor`, so the
/// novelty link's term translates.
#[tokio::test(flavor = "current_thread")]
async fn variable_predicate_count_counts_the_cursor_without_legacy_bundles() {
    let fluree = FlureeBuilder::memory().build_memory();
    let count = "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }";

    let ledger_id = "it/edge-annotations:count-drain";
    let indexed = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({"@context": ctx(), "@id": "ex:x", "ex:worksFor": {"@id": "ex:z"}}),
        )
        .await
        .expect("plain insert");
    drop(indexed);
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let ledger = fluree
        .insert(
            fluree.ledger(ledger_id).await.expect("load"),
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {"@id": "ex:acme", "@annotation": {"ex:role": "Engineer"}}
            }),
        )
        .await
        .expect("annotated insert into novelty")
        .ledger;
    let (result, outcomes) = scan_count_drain_outcomes(&fluree, &ledger, count).await;
    assert_eq!(result, json!([[4]]), "plain edge, base edge, link, body");
    assert_eq!(outcomes, ["proceed"], "{outcomes:?}");

    let ledger_id = "it/edge-annotations:count-drain-legacy";
    let indexed = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({"@context": ctx(), "@id": "ex:x", "ex:worksFor": {"@id": "ex:z"}}),
        )
        .await
        .expect("plain insert");
    drop(indexed);
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let ledger =
        support::commit_legacy_bundle(&fluree, fluree.ledger(ledger_id).await.expect("load")).await;
    let (result, outcomes) = scan_count_drain_outcomes(&fluree, &ledger, count).await;
    assert_eq!(
        result,
        json!([[4]]),
        "plain edge, base edge, derived link, body"
    );
    assert_eq!(outcomes, ["fallback:gate_declined"], "{outcomes:?}");
}
