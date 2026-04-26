//! Shared test harness for fluree-db-api integration tests.
//!
//! Provides type aliases, helpers, and utilities used by integration tests.

// Many helpers are used by *some* integration test crates but not others.
// Keep them centralized here and silence dead_code warnings in crates that
// don't reference every helper.
//
// Kept as a shared utility module across many integration tests. Individual
// test crates intentionally do not use every helper.
#![expect(dead_code)]

pub mod span_capture;

use fluree_db_api::{IndexConfig, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Value as JsonValue};
use std::sync::Arc;

#[cfg(feature = "native")]
use tokio::task::LocalSet;

use fluree_db_api::{GraphDb, QueryResult};

// =============================================================================
// Type aliases (reduce boilerplate in test signatures)
// =============================================================================

/// Type alias for memory-backed Fluree instance.
pub type MemoryFluree = fluree_db_api::Fluree;

/// Type alias for memory-backed ledger state.
pub type MemoryLedger = LedgerState;

// =============================================================================
// Context helpers
// =============================================================================

/// Standard default context used in Fluree tests
pub fn default_context() -> JsonValue {
    json!({
        "xsd": "http://www.w3.org/2001/XMLSchema#",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "sh": "http://www.w3.org/ns/shacl#",
        "schema": "http://schema.org/",
        "skos": "http://www.w3.org/2008/05/skos#",
        "wiki": "https://www.wikidata.org/wiki/",
        "f": "https://ns.flur.ee/db#"
    })
}

// =============================================================================
// Ledger helpers
// =============================================================================

/// Build a `GraphDb` view directly from a loaded `LedgerState`.
///
/// This is the preferred test-time bridge: it exercises the normal `GraphDb`
/// query execution path.
pub fn graphdb_from_ledger(ledger: &LedgerState) -> GraphDb {
    GraphDb::from_ledger_state(ledger)
}

/// Execute a JSON-LD query against a loaded `LedgerState` via the normal `GraphDb` query path.
pub async fn query_jsonld(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query_json: &JsonValue,
) -> fluree_db_api::Result<QueryResult> {
    let db = graphdb_from_ledger(ledger);
    fluree.query(&db, query_json).await
}

/// Execute a SPARQL query against a loaded `LedgerState` via the normal `GraphDb` query path.
pub async fn query_sparql(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    sparql: &str,
) -> fluree_db_api::Result<QueryResult> {
    let db = graphdb_from_ledger(ledger);
    fluree.query(&db, sparql).await
}

/// Execute a JSON-LD query and return formatted JSON-LD output (async formatting path).
pub async fn query_jsonld_formatted(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query_json: &JsonValue,
) -> fluree_db_api::Result<JsonValue> {
    let db = graphdb_from_ledger(ledger);
    let result = fluree.query(&db, query_json).await?;
    Ok(result.to_jsonld_async(db.as_graph_db_ref()).await?)
}

/// Execute a JSON-LD query and format using a provided formatter config (async).
pub async fn query_jsonld_format(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query_json: &JsonValue,
    config: &fluree_db_api::FormatterConfig,
) -> fluree_db_api::Result<JsonValue> {
    let db = graphdb_from_ledger(ledger);
    let result = fluree.query(&db, query_json).await?;
    Ok(result.format_async(db.as_graph_db_ref(), config).await?)
}

/// Execute a JSON-LD query with policy enforcement via view composition.
pub async fn query_jsonld_with_policy(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query_json: &JsonValue,
    policy: &fluree_db_policy::PolicyContext,
) -> fluree_db_api::Result<QueryResult> {
    let db = graphdb_from_ledger(ledger).with_policy(Arc::new(policy.clone()));
    fluree.query(&db, query_json).await
}

/// Execute a JSON-LD query with tracking enabled (fuel/time/policy stats).
///
/// This uses the public builder path (`GraphDb::query(...).execute_tracked()`), not
/// the `Fluree::query_ledger_tracked` convenience.
pub async fn query_jsonld_tracked(
    fluree: &fluree_db_api::Fluree,
    ledger: &LedgerState,
    query_json: &JsonValue,
) -> std::result::Result<fluree_db_api::TrackedQueryResponse, fluree_db_api::TrackedErrorResponse> {
    let db = graphdb_from_ledger(ledger);
    db.query(fluree).jsonld(query_json).execute_tracked().await
}

/// Create a genesis ledger state for the given ledger ID.
///
/// This is the Rust equivalent of `(fluree/create conn "ledger")` prior to the first commit:
/// the nameservice has no record yet, and `commit()` will create one via `publish_commit()`.
pub fn genesis_ledger(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    genesis_ledger_for_fluree(fluree, ledger_id)
}

/// Generic version of `genesis_ledger` for any `Fluree` storage backend.
///
/// The ledger ID is normalized to canonical `name:branch` form (e.g., `"mydb"` → `"mydb:main"`)
/// so that the `LedgerSnapshot.ledger_id` matches the canonical form used by the nameservice and
/// content-addressed storage paths.
pub fn genesis_ledger_for_fluree(_fluree: &fluree_db_api::Fluree, ledger_id: &str) -> LedgerState {
    let canonical = fluree_db_core::ledger_id::normalize_ledger_id(ledger_id)
        .unwrap_or_else(|_| ledger_id.to_string());
    let db = LedgerSnapshot::genesis(&canonical);
    LedgerState::new(db, Novelty::new(0))
}

// =============================================================================
// Common seeding helpers
// =============================================================================

/// Seed a ledger with a user that has a sensitive `schema:ssn` property.
///
/// Uses the default graph and returns the updated ledger.
pub async fn seed_user_with_ssn(
    fluree: &MemoryFluree,
    ledger_id: &str,
    user_id: &str,
    ssn: &str,
) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": user_id,
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:ssn": ssn,
                "ex:age": 25
            }
        ]
    });

    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

/// Rebuild and publish a binary index for the ledger's current commit head.
///
/// Use this in regression tests that need to exercise the FIR6/binary-only
/// reload path rather than the purely in-memory novelty path.
pub async fn rebuild_and_publish_index(fluree: &fluree_db_api::Fluree, ledger_id: &str) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("nameservice lookup")
        .expect("ledger record should exist");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        fluree.content_store(ledger_id),
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("index rebuild should succeed");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");
}

// =============================================================================
// Indexing helpers (native tests)
// =============================================================================

/// Trigger background indexing for `ledger_id` at `t` and wait for completion.
#[cfg(feature = "native")]
pub async fn trigger_index_and_wait(
    handle: &fluree_db_indexer::IndexerHandle,
    ledger_id: &str,
    t: i64,
) {
    let _ = trigger_index_and_wait_outcome(handle, ledger_id, t).await;
}

/// Trigger background indexing for `ledger_id` at `t` and return the completion outcome.
#[cfg(feature = "native")]
pub async fn trigger_index_and_wait_outcome(
    handle: &fluree_db_indexer::IndexerHandle,
    ledger_id: &str,
    t: i64,
) -> fluree_db_api::IndexOutcome {
    let completion = handle.trigger(ledger_id, t).await;
    match completion.wait().await {
        ok @ fluree_db_api::IndexOutcome::Completed { .. } => ok,
        fluree_db_api::IndexOutcome::Failed(e) => panic!("indexing failed: {e}"),
        fluree_db_api::IndexOutcome::Cancelled => panic!("indexing cancelled"),
    }
}

// =============================================================================
// Background indexing helpers (tests)
// =============================================================================

/// Start a `BackgroundIndexerWorker` on a `tokio::task::LocalSet` and return the handle.
///
/// Equivalent to tests that wait on indexing completion
/// and then block until indexing completes. In Rust, tests should:
/// - transact (get `receipt.t`)
/// - `handle.trigger(ledger_id, receipt.t)`
/// - `completion.wait().await`
#[cfg(feature = "native")]
pub fn start_background_indexer_local(
    backend: fluree_db_core::StorageBackend,
    nameservice: Arc<dyn fluree_db_nameservice::ReadWriteNameService>,
    config: fluree_db_indexer::IndexerConfig,
) -> (LocalSet, fluree_db_indexer::IndexerHandle) {
    let (worker, handle) =
        fluree_db_api::BackgroundIndexerWorker::new(backend, nameservice, config);

    let local = LocalSet::new();
    local.spawn_local(worker.run());

    (local, handle)
}

// =============================================================================
// Index config assertions
// =============================================================================

/// Assert that IndexConfig defaults match expected defaults.
///
/// Reindex threshold defaults:
/// - min: 100_000
/// - max: 1_000_000
pub fn assert_index_defaults() {
    let cfg = IndexConfig::default();
    assert_eq!(cfg.reindex_min_bytes, 100_000);
    assert_eq!(cfg.reindex_max_bytes, 1_000_000);
}

// =============================================================================
// Result normalization (for unordered comparisons)
// =============================================================================

/// Normalize JSON-LD row results for unordered comparison.
///
/// Sorts rows by their JSON string representation so tests can compare
/// result sets without relying on a specific order.
pub fn normalize_rows(v: &JsonValue) -> Vec<JsonValue> {
    let mut rows = v.as_array().expect("expected JSON array of rows").to_vec();

    rows.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default())
    });

    rows
}

/// Normalize SPARQL JSON bindings for unordered comparison.
///
/// Extracts `results.bindings` from a SPARQL JSON response and sorts
/// the bindings by their JSON string representation.
pub fn normalize_sparql_bindings(v: &JsonValue) -> Vec<JsonValue> {
    let bindings = v
        .get("results")
        .and_then(|r| r.get("bindings"))
        .and_then(|b| b.as_array())
        .expect("SPARQL JSON results.bindings should be an array");
    let mut out: Vec<JsonValue> = bindings.to_vec();
    out.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap()
            .cmp(&serde_json::to_string(b).unwrap())
    });
    out
}

/// Normalize single-variable results (flat array) for comparison.
pub fn normalize_flat_results(v: &JsonValue) -> Vec<JsonValue> {
    let mut items: Vec<JsonValue> = v.as_array().expect("expected JSON array").to_vec();
    items.sort_by(|a, b| {
        serde_json::to_string(a)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(b).unwrap_or_default())
    });
    items
}

// =============================================================================
// Common @context helpers
// =============================================================================

/// Common @context for ex: and schema: prefixes.
///
/// Returns:
/// ```json
/// {
///   "ex": "http://example.org/ns/",
///   "schema": "http://schema.org/",
///   "xsd": "http://www.w3.org/2001/XMLSchema#"
/// }
/// ```
pub fn context_ex_schema() -> JsonValue {
    json!({
        "ex": "http://example.org/ns/",
        "schema": "http://schema.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

// =============================================================================
// Test data fixtures
// =============================================================================

/// People test data
pub fn people_data() -> JsonValue {
    json!([
        {
            "@id": "ex:brian",
            "@type": "ex:User",
            "schema:name": "Brian",
            "schema:email": "brian@example.org",
            "schema:age": 50,
            "ex:favNums": 7
        },
        {
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice",
            "schema:email": "alice@example.org",
            "schema:age": 50,
            "ex:favNums": [42, 76, 9],
            "schema:birthDate": {"@value": "1974-09-26", "@type": "xsd:date"}
        },
        {
            "@id": "ex:cam",
            "@type": "ex:User",
            "schema:name": "Cam",
            "schema:email": "cam@example.org",
            "schema:age": 34,
            "ex:favNums": [5, 10],
            "ex:friend": ["ex:brian", "ex:alice"]
        },
        {
            "@id": "ex:liam",
            "@type": "ex:User",
            "schema:name": "Liam",
            "schema:email": "liam@example.org",
            "schema:age": 13,
            "ex:favNums": [42, 11],
            "ex:friend": ["ex:brian", "ex:alice", "ex:cam"],
            "schema:birthDate": {"@value": "2011-09-26", "@type": "xsd:date"}
        }
    ])
}

/// Seed the "people" dataset used by JSON-LD filter/optional/union tests.
pub async fn seed_people_filter_dataset(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            {
                "@id": "ex:brian",
                "@type": "ex:User",
                "schema:name": "Brian",
                "schema:email": "brian@example.org",
                "schema:age": 50,
                "ex:last": "Smith",
                "ex:favNums": 7
            },
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@example.org",
                "schema:age": 42,
                "ex:last": "Smith",
                "ex:favColor": "Green",
                "ex:favNums": [42, 76, 9]
            },
            {
                "@id": "ex:cam",
                "@type": "ex:User",
                "schema:name": "Cam",
                "schema:email": "cam@example.org",
                "schema:age": 34,
                "ex:last": "Jones",
                "ex:favColor": "Blue",
                "ex:favNums": [5, 10],
                "ex:friend": [{"@id": "ex:brian"}, {"@id": "ex:alice"}]
            },
            {
                "@id": "ex:david",
                "@type": "ex:User",
                "schema:name": "David",
                "schema:email": "david@example.org",
                "schema:age": 46,
                "ex:last": "Jones",
                "ex:favNums": [15, 70],
                "ex:friend": [{"@id": "ex:cam"}]
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

/// Seed the "people" dataset used by JSON-LD compound query tests.
pub async fn seed_people_compound_dataset(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {
            "id":"@id",
            "type":"@type",
            "schema":"http://schema.org/",
            "ex":"http://example.org/ns/"
        },
        "@graph": [
            {"@id":"ex:brian","@type":"ex:User","schema:name":"Brian","schema:email":"brian@example.org","schema:age":50,"ex:favNums":7},
            {"@id":"ex:alice","@type":"ex:User","schema:name":"Alice","schema:email":"alice@example.org","schema:age":50,"ex:favNums":[42,76,9]},
            {"@id":"ex:cam","@type":"ex:User","schema:name":"Cam","schema:email":"cam@example.org","schema:age":34,"ex:favNums":[5,10],"ex:friend":[{"@id":"ex:brian"},{"@id":"ex:alice"}]}
        ]
    });

    fluree.insert(ledger0, &insert).await.expect("seed").ledger
}

/// Seed a small "people" dataset used by policy + query-connection tests.
///
/// Includes `schema:ssn` so view-policy behavior can be tested.
pub async fn seed_people_with_ssn(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);

    let txn = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@flur.ee",
                "schema:birthDate": "2022-08-17",
                "schema:ssn": "111-11-1111"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:email": "john@flur.ee",
                "schema:birthDate": "2021-08-17",
                "schema:ssn": "888-88-8888"
            }
        ]
    });

    fluree
        .insert(ledger0, &txn)
        .await
        .expect("seed should succeed")
        .ledger
}

/// Load people test data into a new ledger
pub async fn load_people(fluree: &MemoryFluree) -> Result<String, Box<dyn std::error::Error>> {
    let ledger_id = "test/people:main";
    let ledger = fluree.create_ledger(ledger_id).await?;

    let ctx = json!([
        default_context(),
        {"ex": "http://example.org/ns/"}
    ]);

    let insert_txn = json!({
        "@context": ctx,
        "@graph": people_data()
    });

    fluree.insert(ledger, &insert_txn).await?;
    Ok(ledger_id.to_string())
}
