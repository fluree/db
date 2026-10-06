//! Regression: retracting a `@list` container must hydrate each
//! retraction's list-index metadata BEFORE any dedup/cancellation runs.
//!
//! ## Why this test exists
//!
//! `FlakeMeta.i` holds the list index for `@list` entries, and `Flake`'s
//! `Eq`/`Hash` includes `m`. So the asserted `(s, p, o, dt, m={i:k})`
//! flake at list position `k` is not equal to the same triple at list
//! position `k+1`. A retraction generated from a DELETE template that
//! doesn't specify the list index comes out with `m = None` — which
//! matches *nothing* in the index until
//! `hydrate_list_index_meta_for_retractions` looks up the asserted flake's
//! actual `m` and copies it onto the retraction.
//!
//! Hydration MUST run before any step that treats `Flake` identity as a
//! dedup/cancellation key (the mixed-mode `FlakeAccumulator`, the
//! pure-delete `FlakeAccumulator`, any downstream novelty apply step).
//! If hydration is deferred until after finalization, N raw retractions
//! with `m = None` all collapse to one survivor, and only one list entry
//! gets retracted.
//!
//! This test pins the correct timing: a wildcard DELETE WHERE over a
//! three-element `@list` retracts all three entries. It's the regression
//! target for the forthcoming streaming-WHERE refactor — any version of
//! `stage()` that loses the "hydrate before accumulate" guarantee will
//! fail this test.
//!
//! ## Note on duplicate-value list positions
//!
//! A diagnostic run against `["a","a","a"]` shows that today's JSON-LD
//! insert path collapses identical-value list items to a single flake.
//! That means the "hydrate-after-finalize collapses distinct list
//! positions with identical values" hazard is not currently reachable
//! through JSON-LD insert — but the architectural concern still holds
//! for any other code path that may produce such flakes (raw flake sink,
//! import pipeline, future insert semantics). Pre-hydration is the
//! correct design regardless of whether today's insert exposes the gap.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "ex:items": { "@container": "@list" }
    })
}

async fn count_items(fluree: &fluree_db_api::Fluree, ledger: &fluree_db_api::LedgerState) -> usize {
    // SPARQL `SELECT (COUNT(*) AS ?c)` counts binding rows — each distinct
    // flake matching the pattern contributes one row, so list entries at
    // distinct positions with distinct values each get counted.
    let sparql = "\
        PREFIX ex: <http://example.org/> \
        SELECT (COUNT(*) AS ?c) WHERE { ex:alice ex:items ?o }";
    let result = support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("sparql count");
    let jsonld = result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async");
    let arr = jsonld.as_array().expect("array result");
    if arr.is_empty() {
        return 0;
    }
    // SPARQL always returns array-of-arrays (one inner array per binding row).
    arr[0]
        .as_array()
        .and_then(|row| row.first())
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(0)
}

/// Three distinct `@list` entries, wildcard DELETE WHERE.
///
/// Each asserted flake carries a distinct `FlakeMeta.i` (0, 1, 2). The
/// DELETE template doesn't specify the list index, so raw retractions
/// come out with `m = None`. Hydration must fill each one's `m` from the
/// matching asserted flake BEFORE dedup/cancellation, otherwise the
/// retractions would fail to match the indexed assertions and the list
/// entries would silently remain.
#[tokio::test]
async fn wildcard_delete_retracts_all_distinct_list_entries() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/list-retract-distinct:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:items": ["a", "b", "c"]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");

    assert_eq!(
        count_items(&fluree, &receipt.ledger).await,
        3,
        "precondition: three distinct list entries asserted"
    );

    // Wildcard-shaped DELETE that omits the list index. Every retraction
    // comes out of `generate_retractions` with `m = None` — the only way
    // these match the asserted flakes (which have `m.i` set) is if
    // hydration runs before the retractions are consumed by the
    // accumulator / cancellation path.
    let delete_txn = json!({
        "@context": { "ex": "http://example.org/" },
        "where":  { "@id": "ex:alice", "ex:items": "?o" },
        "delete": { "@id": "ex:alice", "ex:items": "?o" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("wildcard retract");

    assert_eq!(
        count_items(&fluree, &out.ledger).await,
        0,
        "all three list entries must be retracted — surviving entries \
         indicate that retractions failed to match asserted flakes, \
         which happens when hydration doesn't populate `m.i` before \
         the retractions enter the dedup/novelty path"
    );
}

/// Filtered two-pattern DELETE over subjects carrying `@list` properties,
/// on a novelty-heavy ledger (delete-everything then re-insert, no index
/// rebuild in between).
///
/// Mirrors the field-reported staging livelock: `{?s tag <doc>} {?s ?p ?o}
/// DELETE {?s ?p ?o}` matching subjects with large `@list` vectors, where
/// all matched data lives in novelty. List-index hydration is grouped by
/// (graph, subject, predicate) — one range lookup per group — so this pins
/// that the grouped path still fills `m.i` on every retraction: every list
/// entry and scalar of the tagged subjects must be retracted, and untagged
/// subjects must be untouched.
#[tokio::test]
async fn filtered_delete_retracts_tagged_subjects_with_lists_in_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/list-retract-filtered:main")
        .await
        .expect("create");

    let list_ctx = json!({
        "ex": "http://example.org/",
        "ex:vector": { "@container": "@list" }
    });
    let make_docs = |tag: &str| {
        let subjects: Vec<JsonValue> = (0..4)
            .map(|i| {
                let vector: Vec<f64> = (0..32).map(|k| (i * 100 + k) as f64 * 0.5).collect();
                json!({
                    "@id": format!("ex:chunk-{tag}-{i}"),
                    "ex:sourceDocument": { "@id": format!("ex:doc-{tag}") },
                    "ex:label": format!("chunk {i} of {tag}"),
                    "ex:vector": vector
                })
            })
            .collect();
        json!({ "@context": list_ctx, "@graph": subjects })
    };

    // Build the novelty-heavy state: insert both docs' chunks, delete
    // everything, then re-insert — all without an index rebuild, so every
    // matched flake lives in the novelty overlay (assert + retract + assert).
    let receipt = fluree
        .insert(ledger0, &make_docs("a"))
        .await
        .expect("insert a");
    let receipt = fluree
        .insert(receipt.ledger, &make_docs("b"))
        .await
        .expect("insert b");
    let receipt = fluree
        .update(
            receipt.ledger,
            &json!({
                "where":  { "@id": "?s", "?p": "?o" },
                "delete": { "@id": "?s", "?p": "?o" }
            }),
        )
        .await
        .expect("delete everything");
    let receipt = fluree
        .insert(receipt.ledger, &make_docs("a"))
        .await
        .expect("re-insert a");
    let receipt = fluree
        .insert(receipt.ledger, &make_docs("b"))
        .await
        .expect("re-insert b");

    let count_all = |ledger: fluree_db_api::LedgerState, tag: &'static str| {
        let fluree = &fluree;
        async move {
            let sparql = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT (COUNT(*) AS ?c) WHERE {{ \
                   ?s ex:sourceDocument ex:doc-{tag} . ?s ?p ?o }}"
            );
            let result = support::query_sparql(fluree, &ledger, &sparql)
                .await
                .expect("sparql count");
            let jsonld = result
                .to_jsonld_async(ledger.as_graph_db_ref(0))
                .await
                .expect("to_jsonld_async");
            let arr = jsonld.as_array().expect("array result");
            arr.first()
                .and_then(JsonValue::as_array)
                .and_then(|row| row.first())
                .and_then(JsonValue::as_u64)
                .unwrap_or(0)
        }
    };

    // 4 subjects × (1 sourceDocument + 1 label + 32 list entries) per doc.
    let per_doc_triples = 4 * (1 + 1 + 32);
    assert_eq!(
        count_all(receipt.ledger.clone(), "a").await,
        per_doc_triples,
        "precondition: doc-a chunks fully re-inserted into novelty"
    );

    // The reported shape: tag pattern + wildcard pattern, wildcard delete.
    let out = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "where": [
                    { "@id": "?s", "ex:sourceDocument": { "@id": "ex:doc-a" } },
                    { "@id": "?s", "?p": "?o" }
                ],
                "delete": { "@id": "?s", "?p": "?o" }
            }),
        )
        .await
        .expect("filtered delete");

    assert_eq!(
        count_all(out.ledger.clone(), "a").await,
        0,
        "every triple of the tagged subjects must be retracted, including \
         all @list entries — survivors mean grouped hydration failed to \
         populate `m.i` on some retraction"
    );
    assert_eq!(
        count_all(out.ledger, "b").await,
        per_doc_triples,
        "untagged doc-b subjects must be untouched by the filtered delete"
    );
}

/// Indexed variant of the filtered-delete case: a binary index is published
/// mid-history, so staging's list-meta hydration lookups route through the
/// V3 range provider (`binary_range_eq_v3`) and its cross-call overlay
/// translation cache, with the delete-everything + re-insert novelty stacked
/// on top of the persisted base. Pins that cached overlay translations are
/// (a) correct on repeated same-state lookups and (b) invalidated across the
/// intervening commits — a stale entry would surface pre-delete flakes or
/// miss re-inserted ones, breaking the counts below.
#[tokio::test]
async fn filtered_delete_with_lists_on_indexed_base_plus_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "tx/list-retract-indexed:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.expect("create");

    let list_ctx = json!({
        "ex": "http://example.org/",
        "ex:vector": { "@container": "@list" }
    });
    let make_docs = |tag: &str| {
        let subjects: Vec<JsonValue> = (0..4)
            .map(|i| {
                let vector: Vec<f64> = (0..32).map(|k| (i * 100 + k) as f64 * 0.5).collect();
                json!({
                    "@id": format!("ex:chunk-{tag}-{i}"),
                    "ex:sourceDocument": { "@id": format!("ex:doc-{tag}") },
                    "ex:label": format!("chunk {i} of {tag}"),
                    "ex:vector": vector
                })
            })
            .collect();
        json!({ "@context": list_ctx, "@graph": subjects })
    };

    // Base state: both docs inserted, then persisted into a binary index.
    let receipt = fluree
        .insert(ledger0, &make_docs("a"))
        .await
        .expect("insert a");
    fluree
        .insert(receipt.ledger, &make_docs("b"))
        .await
        .expect("insert b");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = fluree.ledger(ledger_id).await.expect("reload indexed");

    // Novelty on top of the index: delete everything, re-insert both docs.
    let receipt = fluree
        .update(
            indexed,
            &json!({
                "where":  { "@id": "?s", "?p": "?o" },
                "delete": { "@id": "?s", "?p": "?o" }
            }),
        )
        .await
        .expect("delete everything");
    let receipt = fluree
        .insert(receipt.ledger, &make_docs("a"))
        .await
        .expect("re-insert a");
    let receipt = fluree
        .insert(receipt.ledger, &make_docs("b"))
        .await
        .expect("re-insert b");

    let count_all = |ledger: fluree_db_api::LedgerState, tag: &'static str| {
        let fluree = &fluree;
        async move {
            let sparql = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT (COUNT(*) AS ?c) WHERE {{ \
                   ?s ex:sourceDocument ex:doc-{tag} . ?s ?p ?o }}"
            );
            let result = support::query_sparql(fluree, &ledger, &sparql)
                .await
                .expect("sparql count");
            let jsonld = result
                .to_jsonld_async(ledger.as_graph_db_ref(0))
                .await
                .expect("to_jsonld_async");
            let arr = jsonld.as_array().expect("array result");
            arr.first()
                .and_then(JsonValue::as_array)
                .and_then(|row| row.first())
                .and_then(JsonValue::as_u64)
                .unwrap_or(0)
        }
    };

    let per_doc_triples = 4 * (1 + 1 + 32);
    assert_eq!(
        count_all(receipt.ledger.clone(), "a").await,
        per_doc_triples,
        "precondition: doc-a re-inserted into novelty over the indexed base"
    );

    let out = fluree
        .update(
            receipt.ledger,
            &json!({
                "@context": { "ex": "http://example.org/" },
                "where": [
                    { "@id": "?s", "ex:sourceDocument": { "@id": "ex:doc-a" } },
                    { "@id": "?s", "?p": "?o" }
                ],
                "delete": { "@id": "?s", "?p": "?o" }
            }),
        )
        .await
        .expect("filtered delete");

    assert_eq!(
        count_all(out.ledger.clone(), "a").await,
        0,
        "every triple of the tagged subjects must be retracted through the \
         indexed range-provider path, including all @list entries"
    );
    assert_eq!(
        count_all(out.ledger, "b").await,
        per_doc_triples,
        "untagged doc-b subjects must be untouched"
    );
}

/// Companion to the three-entry case: retracting a single-entry `@list`
/// where the asserted flake has `m.i = 0`. Pins the hydration behavior
/// for the simplest case.
#[tokio::test]
async fn wildcard_delete_retracts_single_list_entry() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/list-retract-single:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:items": ["only"]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    assert_eq!(count_items(&fluree, &receipt.ledger).await, 1);

    let delete_txn = json!({
        "@context": { "ex": "http://example.org/" },
        "where":  { "@id": "ex:alice", "ex:items": "?o" },
        "delete": { "@id": "ex:alice", "ex:items": "?o" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("wildcard retract");

    assert_eq!(
        count_items(&fluree, &out.ledger).await,
        0,
        "single list entry must be retracted via wildcard DELETE"
    );
}
