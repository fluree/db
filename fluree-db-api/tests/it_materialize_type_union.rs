//! Regression tests for additive-mode `@type` union in R2RML materialization.
//!
//! When several sources materialize the SAME subject IRI into a shared target
//! ledger (a shared knowledge graph, or a join table that adds an edge to a
//! parent entity, or an `entity_type` table adding an extra class), the classes
//! must UNION — not clobber. The materialize engine achieves this in additive
//! mode by asserting `@type` via an idempotent `insert` and `upsert`ing only the
//! non-type predicates. These tests exercise that exact transaction sequence
//! end-to-end against a memory ledger.
//!
//! The `..._clobbers` control documents WHY the split is needed: a single upsert
//! carrying `@type` retracts-then-inserts rdf:type per predicate, so the last
//! writer wins and earlier classes are lost — which is precisely how `as:Article`
//! disappeared from a shared `silver:main` in the field.

use crate::support;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

const SUBJ: &str = "https://example.org/Article%2F1";
const ARTICLE: &str = "https://www.w3.org/ns/activitystreams#Article";
const ANNOUNCE: &str = "https://www.w3.org/ns/activitystreams#Announce";
const CONTENT: &str = "https://www.w3.org/ns/activitystreams#content";

async fn types_of_subject(
    fluree: &support::MemoryFluree,
    ledger: &support::MemoryLedger,
) -> String {
    let q = json!({
        "select": ["?s", "?type"],
        "where": { "@id": "?s", "@type": "?type" }
    });
    support::query_jsonld_formatted(fluree, ledger, &q)
        .await
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn additive_type_union_across_sources() {
    // Mirror the additive-mode apply: source "article" inserts its class then
    // upserts its predicate; source "entity_type" inserts a SECOND class on the
    // same subject. Both classes must survive.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "mat/typeunion:main");

    // Source A ("article"): @type via insert, predicate via upsert.
    let ledger = fluree
        .insert(ledger, &json!({ "@id": SUBJ, "@type": [ARTICLE] }))
        .await
        .unwrap()
        .ledger;
    let ledger = fluree
        .upsert(ledger, &json!({ "@id": SUBJ, CONTENT: "hello" }))
        .await
        .unwrap()
        .ledger;

    // Source B ("entity_type"): adds a second class to the same subject (insert).
    let ledger = fluree
        .insert(ledger, &json!({ "@id": SUBJ, "@type": [ANNOUNCE] }))
        .await
        .unwrap()
        .ledger;

    let types = types_of_subject(&fluree, &ledger).await;
    assert!(
        types.contains(ARTICLE),
        "as:Article must survive the second source, got: {types}"
    );
    assert!(
        types.contains(ANNOUNCE),
        "as:Announce must be added by the second source, got: {types}"
    );

    // The predicate written by source A is intact.
    let content_q = json!({ "select": ["?c"], "where": { "@id": SUBJ, CONTENT: "?c" } });
    let out = support::query_jsonld_formatted(&fluree, &ledger, &content_q)
        .await
        .unwrap()
        .to_string();
    assert!(out.contains("hello"), "predicate lost, got: {out}");
}

#[tokio::test]
async fn single_upsert_carrying_type_clobbers() {
    // Control: the OLD behavior (a single upsert that carries @type) replaces
    // rdf:type per predicate, so the second writer clobbers the first class.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = support::genesis_ledger(&fluree, "mat/typeclobber:main");

    let ledger = fluree
        .upsert(
            ledger,
            &json!({ "@id": SUBJ, "@type": [ARTICLE], CONTENT: "hello" }),
        )
        .await
        .unwrap()
        .ledger;
    let ledger = fluree
        .upsert(ledger, &json!({ "@id": SUBJ, "@type": [ANNOUNCE] }))
        .await
        .unwrap()
        .ledger;

    let types = types_of_subject(&fluree, &ledger).await;
    assert!(
        types.contains(ANNOUNCE),
        "second upsert's class should be present, got: {types}"
    );
    assert!(
        !types.contains(ARTICLE),
        "single-upsert-with-@type CLOBBERS the earlier class (this is the bug the \
         insert/upsert split fixes), got: {types}"
    );
    // The non-type predicate untouched by the second upsert survives.
    let content_q = json!({ "select": ["?c"], "where": { "@id": SUBJ, CONTENT: "?c" } });
    let out = support::query_jsonld_formatted(&fluree, &ledger, &content_q)
        .await
        .unwrap()
        .to_string();
    assert!(
        out.contains("hello"),
        "predicate should be preserved, got: {out}"
    );
}
