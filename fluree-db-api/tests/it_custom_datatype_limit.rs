//! Regression tests: a ledger with more than 241 distinct custom datatypes
//! must still index (issue #1751).
//!
//! The indexer used to reject any datatype dictionary id above `u8::MAX`.
//! Fifteen ids are reserved for built-in datatypes, so the 242nd custom
//! datatype overflowed. The insert succeeded and every later index attempt
//! failed. The storage format itself carries datatype ids in a 14-bit
//! `OType` payload, so the 8-bit limit was never a format constraint.
//!
//! The 14-bit limit is real, so a ledger that reaches it must refuse the
//! next new datatype at write time. An insert error is recoverable. A
//! ledger that accepted data it can never index is not.

#![cfg(feature = "native")]

use crate::support::{self, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_core::DatatypeDictId;
use serde_json::json;

/// Comfortably past the old boundary of 241 custom datatypes.
const N: usize = 300;

/// Custom datatypes a ledger can hold: every datatype dict id from
/// `RESERVED_COUNT` through `MAX`.
const CAPACITY: usize = (DatatypeDictId::MAX - DatatypeDictId::RESERVED_COUNT + 1) as usize;

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

/// Assert that a write was refused for exceeding the datatype limit.
fn assert_datatype_limit_rejection<T, E: std::fmt::Display>(result: Result<T, E>, what: &str) {
    match result {
        Ok(_) => panic!("{what}: a write past the datatype limit was accepted"),
        Err(e) => assert!(
            e.to_string().contains("datatype limit"),
            "{what}: expected a datatype limit error, got: {e}"
        ),
    }
}

/// A new subject typed with a datatype the ledger already holds.
fn insert_known_datatype() -> serde_json::Value {
    json!({
        "@context": ctx(),
        "@id": "ex:known",
        "ex:p": {"@value": "known", "@type": "ex:U0"}
    })
}

/// The ledger is full and every existing datatype is still in novelty.
#[tokio::test]
async fn insert_past_datatype_limit_is_rejected_from_novelty() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-novelty";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let full = fluree
        .insert(ledger0, &insert_range(0..CAPACITY))
        .await
        .expect("filling the datatype dictionary exactly is allowed")
        .ledger;

    assert_datatype_limit_rejection(
        fluree
            .insert(full.clone(), &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "new datatype over novelty",
    );
    fluree
        .insert(full, &insert_known_datatype())
        .await
        .expect("a datatype the ledger already holds is still accepted");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
}

/// The ledger is full and every existing datatype is in the index.
#[tokio::test]
async fn insert_past_datatype_limit_is_rejected_from_index() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-indexed";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    fluree
        .insert(ledger0, &insert_range(0..CAPACITY))
        .await
        .expect("filling the datatype dictionary exactly is allowed");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let indexed = load_indexed(&fluree, ledger_id).await;

    assert_datatype_limit_rejection(
        fluree
            .insert(indexed.clone(), &insert_range(CAPACITY..CAPACITY + 1))
            .await,
        "new datatype over the index",
    );
    fluree
        .insert(indexed, &insert_known_datatype())
        .await
        .expect("a datatype the ledger already holds is still accepted");

    support::build_and_publish_index(&fluree, ledger_id).await;
}

/// A single transaction that brings more new datatypes than the ledger has
/// room for.
#[tokio::test]
async fn single_insert_past_datatype_limit_is_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "custom-dt-limit:reject-single");
    assert_datatype_limit_rejection(
        fluree.insert(ledger0, &insert_range(0..CAPACITY + 1)).await,
        "one transaction over the limit",
    );
}

/// Two writes on disjoint subjects race for the last free datatype id. The
/// one that commits second is staged before the first commits, so a check
/// made only at stage time would pass both. It must be refused instead of
/// being re-based over the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_cannot_jointly_pass_datatype_limit() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "custom-dt-limit:reject-concurrent";
    fluree.create_ledger(ledger_id).await.expect("create");
    let handle = fluree.ledger_cached(ledger_id).await.expect("cache");
    fluree
        .stage(&handle)
        .insert(&insert_range(0..CAPACITY - 1))
        .execute()
        .await
        .expect("filling all but one datatype id is allowed");

    let (parked, release) = handle.gate_next_optimistic_stage_for_test();
    let behind = {
        let fluree = fluree.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            fluree
                .stage(&handle)
                .insert(&insert_range(CAPACITY..CAPACITY + 1))
                .execute()
                .await
                .map(|_| ())
        })
    };
    parked.await.expect("the write parks after staging");

    fluree
        .stage(&handle)
        .insert(&insert_range(CAPACITY - 1..CAPACITY))
        .execute()
        .await
        .expect("the write that takes the last datatype id commits");
    release.send(true).unwrap();
    assert_datatype_limit_rejection(
        behind.await.expect("task"),
        "write staged before the last id was taken",
    );

    support::rebuild_and_publish_index(&fluree, ledger_id).await;
}
