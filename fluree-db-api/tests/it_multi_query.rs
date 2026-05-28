//! Integration tests for `Fluree::multi_query()` — the in-process
//! multi-query envelope API.
//!
//! These exercise the public builder against a `MemoryFluree` directly,
//! without going through HTTP. They are the canonical "downstream Rust
//! consumer" smoke tests — equivalent to a library user calling:
//!
//! ```ignore
//! let fluree: Arc<Fluree> = Arc::new(FlureeBuilder::memory().build_memory());
//! let response = fluree.multi_query()
//!     .envelope(envelope)
//!     .execute()
//!     .await?;
//! ```
//!
//! Per-piece coverage (envelope shape, validation rules, snapshot
//! resolution, response assembly, span emission) lives in lib unit
//! tests and the server / CLI integration suites. This file's job is
//! to confirm the public API is wired together correctly end to end
//! for non-server consumers.

mod support;

use std::sync::Arc;

use fluree_db_api::query::multi::{MultiQueryBounds, MultiQueryRequest, MultiQueryStatus};
use fluree_db_api::query::multi_dispatch::MultiQueryError;
use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

/// Seed two ledgers with a couple of named entities each so we can run
/// envelopes that hit both.
async fn seed_two_ledgers(fluree: &MemoryFluree) -> (MemoryLedger, MemoryLedger) {
    let users = fluree
        .insert(
            genesis_ledger(fluree, "mq:users"),
            &json!({
                "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
                "@graph": [
                    { "@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice" },
                    { "@id": "ex:brian", "@type": "ex:User", "schema:name": "Brian" }
                ]
            }),
        )
        .await
        .expect("seed users")
        .ledger;
    let orders = fluree
        .insert(
            genesis_ledger(fluree, "mq:orders"),
            &json!({
                "@context": { "ex": "http://example.org/" },
                "@graph": [
                    { "@id": "ex:o1", "ex:orderId": "ord-1" },
                    { "@id": "ex:o2", "ex:orderId": "ord-2" }
                ]
            }),
        )
        .await
        .expect("seed orders")
        .ledger;
    (users, orders)
}

/// Convert the seeded ledgers into something we can ignore — the
/// envelope's `from` carries the ledger id, so we don't need to thread
/// the LedgerState into the multi-query call. This helper just exists
/// so the seed function's return shape stays informative.
fn ignore_ledgers(_: (MemoryLedger, MemoryLedger)) {}

// =============================================================================
// Public API end-to-end
// =============================================================================

#[tokio::test]
async fn multi_query_builder_runs_envelope_in_process() {
    // The headline test: `Fluree::multi_query()` works for a downstream
    // Rust consumer with no server involvement. Two aliases (one JSON-LD,
    // one SPARQL) against the same ledger; envelope `@context` lifts to
    // both sub-queries.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "queries": {
            "people_jsonld": {
                "language": "jsonld",
                "query": {
                    "from":   "mq:users",
                    "select": ["?name"],
                    "where":  { "@id": "?p", "schema:name": "?name" }
                }
            },
            "people_sparql": {
                "language": "sparql",
                "query": "SELECT ?name FROM <mq:users> WHERE { ?p schema:name ?name }"
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);
    assert_eq!(response.results.len(), 2);
    assert!(
        response.errors.is_empty(),
        "no per-alias errors expected, got: {:?}",
        response.errors
    );

    // The snapshot map echoes the per-ledger `t` so a downstream caller
    // can replay against the same moment.
    assert!(
        response.snapshot.ledgers.get("mq:users").is_some(),
        "snapshot.ledgers should contain mq:users, got: {:?}",
        response.snapshot.ledgers
    );

    // Both aliases produced results that mention at least one of the
    // seeded names.
    let jsonld_dump = serde_json::to_string(&response.results["people_jsonld"]).unwrap();
    assert!(
        jsonld_dump.contains("Alice") || jsonld_dump.contains("Brian"),
        "JSON-LD alias should return seeded names, got: {jsonld_dump}"
    );
    let sparql_dump = serde_json::to_string(&response.results["people_sparql"]).unwrap();
    assert!(
        sparql_dump.contains("Alice") || sparql_dump.contains("Brian"),
        "SPARQL alias should return seeded names, got: {sparql_dump}"
    );
}

#[tokio::test]
async fn multi_query_two_ledgers_share_one_snapshot() {
    // Two sub-queries hit two different ledgers. The snapshot block
    // should contain both ledgers' resolved `t`, proving the envelope
    // resolved one moment and applied it across.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "ex": "http://example.org/", "schema": "http://schema.org/" },
        "queries": {
            "users":  {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "orders": {
                "language": "jsonld",
                "query": { "from": "mq:orders",
                           "select": ["?id"],
                           "where": { "@id": "?o", "ex:orderId": "?id" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.status, MultiQueryStatus::Ok);
    assert!(response.snapshot.ledgers.contains_key("mq:users"));
    assert!(response.snapshot.ledgers.contains_key("mq:orders"));
}

// =============================================================================
// Per-alias tracking surface
// =============================================================================

#[tokio::test]
async fn multi_query_envelope_meta_populates_per_alias_tracking() {
    // Envelope-level opts.meta enables tracking for every sub-query.
    // Response's `tracking` map mirrors `results` with per-alias
    // time / fuel / policy siblings; `meta` carries the envelope-level
    // rollup.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "opts": { "meta": true },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "b": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?u"],
                           "where": { "@id": "?u", "schema:name": "Alice" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    // Both aliases tracked → both appear in the per-alias tracking map.
    assert_eq!(response.tracking.len(), 2);
    assert!(response.tracking.contains_key("a"));
    assert!(response.tracking.contains_key("b"));
    // At least one of time / fuel was recorded for each.
    for (alias, tally) in &response.tracking {
        assert!(
            tally.time.is_some() || tally.fuel.is_some(),
            "tracking[{alias}] should record at least time or fuel, got: {tally:?}"
        );
    }
    // Envelope-level rollup populated when opts.meta is true.
    let meta = response.meta.unwrap();
    assert!(meta.elapsed_ms.is_some());
}

#[tokio::test]
async fn multi_query_per_subquery_opts_meta_only_tracks_that_alias() {
    // Per-sub-query opts.meta on one alias only → the response's
    // tracking map holds only that alias.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "tracked": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } },
                "opts": { "meta": true }
            },
            "untracked": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes");

    assert_eq!(response.tracking.len(), 1);
    assert!(response.tracking.contains_key("tracked"));
    assert!(!response.tracking.contains_key("untracked"));
}

// =============================================================================
// Failure plumbing surfaces through the public error type
// =============================================================================

#[tokio::test]
async fn multi_query_validation_error_surfaces_as_typed_variant() {
    // Envelope with no sub-queries should surface as
    // MultiQueryError::Validation, not panic or lose error context.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "queries": {}
    }))
    .expect("envelope deserialises");

    let err = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect_err("empty envelope should error");

    assert!(
        matches!(err, MultiQueryError::Validation(_)),
        "expected MultiQueryError::Validation, got: {err:?}"
    );
}

#[tokio::test]
async fn multi_query_partial_failure_per_alias_via_in_process_api() {
    // Two aliases — one good JSON-LD, one syntactically-broken SPARQL.
    // Validation passes (the multi-query validator defers SPARQL
    // grammar to the downstream parser); the dispatcher's per-alias
    // error path lands the broken SPARQL in `response.errors` while the
    // good alias produces results. Status is "partial."
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "good": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            },
            "bad": {
                "language": "sparql",
                "query": "SELECT ?x FROM <mq:users> WHERE { this is not valid SPARQL }"
            }
        }
    }))
    .expect("envelope deserialises");

    let response = fluree
        .multi_query()
        .envelope(envelope)
        .execute()
        .await
        .expect("envelope executes even with one broken sub-query");

    assert_eq!(response.status, MultiQueryStatus::Partial);
    assert!(response.results.contains_key("good"));
    assert!(response.errors.contains_key("bad"));
}

// =============================================================================
// Builder shape (drop-in for `.query()` discipline)
// =============================================================================

#[tokio::test]
async fn multi_query_builder_envelope_required_error_when_unset() {
    // Programmer error: calling .execute() without .envelope() returns
    // a typed error rather than panicking.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    let err = fluree
        .multi_query()
        .execute()
        .await
        .expect_err("envelope required");
    assert!(matches!(err, MultiQueryError::EnvelopeRequired));
}

#[tokio::test]
async fn multi_query_builder_bounds_override_is_threaded_through() {
    // Setting a tiny max_response_size on the builder should reject an
    // envelope whose result would exceed it, surfacing as
    // MultiQueryError::ResponseAssembly. Confirms the .bounds()
    // builder setter actually reaches the assembler.
    let fluree = Arc::new(FlureeBuilder::memory().build_memory());
    ignore_ledgers(seed_two_ledgers(&fluree).await);

    let envelope: MultiQueryRequest = serde_json::from_value(json!({
        "@context": { "schema": "http://schema.org/" },
        "queries": {
            "a": {
                "language": "jsonld",
                "query": { "from": "mq:users",
                           "select": ["?n"],
                           "where": { "@id": "?u", "schema:name": "?n" } }
            }
        }
    }))
    .expect("envelope deserialises");

    let tiny = MultiQueryBounds {
        max_response_size_bytes: 16, // smaller than the response overhead
        ..MultiQueryBounds::DEFAULT
    };

    let err = fluree
        .multi_query()
        .envelope(envelope)
        .bounds(tiny)
        .execute()
        .await
        .expect_err("bounds override should make assembly fail");

    assert!(
        matches!(err, MultiQueryError::ResponseAssembly(_)),
        "expected ResponseAssembly error from oversized response, got: {err:?}"
    );
}
