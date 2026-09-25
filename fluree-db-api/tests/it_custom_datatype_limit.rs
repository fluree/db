//! Regression tests: a ledger with more than 241 distinct custom datatypes
//! must still index (issue #1751).
//!
//! The indexer used to reject any datatype dictionary id above `u8::MAX`.
//! Fifteen ids are reserved for built-in datatypes, so the 242nd custom
//! datatype overflowed. The insert succeeded and every later index attempt
//! failed. The storage format itself carries datatype ids in a 14-bit
//! `OType` payload, so the 8-bit limit was never a format constraint.

#![cfg(feature = "native")]

use crate::support::{self, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

/// Comfortably past the old boundary of 241 custom datatypes.
const N: usize = 300;

fn ctx() -> serde_json::Value {
    json!({"ex": "http://example.org/"})
}

/// One subject per custom datatype: `ex:s{i} ex:p "v{i}"^^ex:U{i}`.
fn insert_range(range: std::ops::Range<usize>) -> serde_json::Value {
    let graph: Vec<_> = range
        .map(|i| {
            json!({
                "@id": format!("ex:s{i}"),
                "ex:p": {"@value": format!("v{i}"), "@type": format!("ex:U{i}")}
            })
        })
        .collect();
    json!({"@context": ctx(), "@graph": graph})
}

fn expected_rows(range: std::ops::Range<usize>) -> serde_json::Value {
    let rows: Vec<_> = range
        .map(|i| {
            json!([
                format!("ex:s{i}"),
                {"@value": format!("v{i}"), "@type": format!("ex:U{i}")}
            ])
        })
        .collect();
    json!(rows)
}

async fn all_rows(fluree: &MemoryFluree, ledger: &MemoryLedger) -> serde_json::Value {
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?v"],
        "where": {"@id": "?s", "ex:p": "?v"}
    });
    support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query should succeed")
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
}

async fn load_indexed(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let indexed = fluree.ledger(ledger_id).await.expect("load indexed ledger");
    assert!(
        indexed.snapshot.range_provider.is_some(),
        "expected binary range provider after indexing"
    );
    indexed
}

/// Full rebuild with 300 custom datatypes, read back through JSON-LD.
#[tokio::test]
async fn full_rebuild_past_u8_custom_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:rebuild";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..N))
        .await
        .expect("insert");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let rows = all_rows(&fluree, &indexed).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&expected_rows(0..N)),
        "every custom-typed literal must round-trip through the index"
    );

    // A bound object whose datatype id is above the old u8 limit must match
    // exactly its own subject.
    let q = json!({
        "@context": ctx(),
        "select": ["?s"],
        "where": {"@id": "?s", "ex:p": {"@value": "v299", "@type": "ex:U299"}}
    });
    let bound = support::query_jsonld(&fluree, &indexed, &q)
        .await
        .expect("bound query should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&bound),
        normalize_rows(&json!([["ex:s299"]])),
        "bound custom-typed object must match one subject, got {bound}"
    );
}

/// SPARQL twin of the full-rebuild test.
#[tokio::test]
async fn full_rebuild_past_u8_custom_datatype_limit_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:sparql";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..N))
        .await
        .expect("insert");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let count_q = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(DISTINCT ?dt) AS ?n)
        WHERE { ?s ex:p ?v . BIND(DATATYPE(?v) AS ?dt) }
    ";
    let rows = support::query_sparql(&fluree, &indexed, count_q)
        .await
        .expect("sparql count should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([[N]])),
        "every custom datatype must survive indexing, got {rows}"
    );

    let bound_q = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?s
        WHERE { ?s ex:p "v299"^^ex:U299 }
    "#;
    let rows = support::query_sparql(&fluree, &indexed, bound_q)
        .await
        .expect("sparql bound query should succeed")
        .to_jsonld(&indexed.snapshot)
        .expect("to_jsonld");
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&json!([["ex:s299"]])),
        "bound custom-typed object must match one subject, got {rows}"
    );
}

/// Cross the old boundary on the second index cycle, so the overflow happens
/// in incremental indexing rather than in a full rebuild.
#[tokio::test]
async fn incremental_index_crosses_u8_custom_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:incremental";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let ledger1 = fluree
        .insert(ledger0, &insert_range(0..200))
        .await
        .expect("first insert")
        .ledger;
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    fluree
        .insert(ledger1, &insert_range(200..N))
        .await
        .expect("second insert");
    support::build_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    let rows = all_rows(&fluree, &indexed).await;
    assert_eq!(
        normalize_rows(&rows),
        normalize_rows(&expected_rows(0..N)),
        "custom-typed literals from both index cycles must round-trip"
    );
}
