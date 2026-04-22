//! Integration tests for the fast-path COUNT + GROUP BY operators.
//!
//! These operators (`PredicateGroupCountFirstsOperator` and
//! `PredicateObjectCountFirstsOperator`) are detected in `operator_tree.rs`
//! and use POST leaflet FIRST headers for skip-decoding optimization.
//!
//! In memory-backed tests, the fast path falls back to the standard
//! scan+aggregate pipeline (since there's no binary graph view), which
//! exercises the fallback path including EmitMask pruning.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::{context_ex_schema, genesis_ledger, normalize_rows, MemoryFluree, MemoryLedger};

// =============================================================================
// Seed helpers
// =============================================================================

/// Seed data with a well-known predicate (`schema:age`) and multiple objects
/// sharing the same age, suitable for GROUP BY ?age COUNT(?s) queries.
async fn seed_age_data(fluree: &MemoryFluree, ledger_id: &str) -> MemoryLedger {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    let insert = json!({
        "@context": ctx,
        "@graph": [
            { "@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:age": 30 },
            { "@id": "ex:bob",   "@type": "ex:User", "schema:name": "Bob",   "schema:age": 25 },
            { "@id": "ex:carol", "@type": "ex:User", "schema:name": "Carol", "schema:age": 30 },
            { "@id": "ex:dave",  "@type": "ex:User", "schema:name": "Dave",  "schema:age": 25 },
            { "@id": "ex:eve",   "@type": "ex:User", "schema:name": "Eve",   "schema:age": 30 },
            { "@id": "ex:frank", "@type": "ex:User", "schema:name": "Frank", "schema:age": 40 }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert should succeed")
        .ledger
}

/// Seed data across two transactions for fallback-via-time-travel tests.
async fn seed_two_txns(fluree: &MemoryFluree, ledger_id: &str) -> (MemoryLedger, MemoryLedger) {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let ctx = context_ex_schema();

    // t=1: three people
    let insert1 = json!({
        "@context": ctx,
        "@graph": [
            { "@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:age": 30 },
            { "@id": "ex:bob",   "@type": "ex:User", "schema:name": "Bob",   "schema:age": 30 },
            { "@id": "ex:carol", "@type": "ex:User", "schema:name": "Carol", "schema:age": 25 }
        ]
    });

    let out1 = fluree.insert(ledger0, &insert1).await.expect("insert t=1");
    let ledger1 = out1.ledger;

    // t=2: two more people
    let insert2 = json!({
        "@context": ctx,
        "@graph": [
            { "@id": "ex:dave", "@type": "ex:User", "schema:name": "Dave", "schema:age": 30 },
            { "@id": "ex:eve",  "@type": "ex:User", "schema:name": "Eve",  "schema:age": 25 }
        ]
    });

    let out2 = fluree
        .insert(ledger1.clone(), &insert2)
        .await
        .expect("insert t=2");

    (ledger1, out2.ledger)
}

// =============================================================================
// Op1: PredicateGroupCountFirstsOperator fallback tests
// =============================================================================

/// The canonical fast-path query shape:
/// `SELECT ?age (COUNT(?s) AS ?count) WHERE { ?s schema:age ?age } GROUP BY ?age ORDER BY DESC(?count) LIMIT k`
///
/// In memory mode this falls back to the standard pipeline, exercising
/// EmitMask{s:false, p:false, o:true} pruning.
#[tokio::test]
async fn group_count_topk_fallback_basic() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/group-count-topk:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": "?age" }],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 3
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // age=30 has 3, age=25 has 2, age=40 has 1 → top-3 desc
    assert_eq!(json_rows, json!([[30, 3], [25, 2], [40, 1]]));
}

/// Top-k with LIMIT smaller than the number of groups.
#[tokio::test]
async fn group_count_topk_limit_truncates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/group-count-limit:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": "?age" }],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 1
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Only the top group
    assert_eq!(json_rows, json!([[30, 3]]));
}

// =============================================================================
// Op2: PredicateObjectCountFirstsOperator fallback tests
// =============================================================================

/// The canonical Op2 shape:
/// `SELECT (COUNT(?s) AS ?count) WHERE { ?s schema:age 30 }`
///
/// In memory mode this falls back to the standard pipeline, exercising
/// EmitMask{s:true, p:false, o:false} pruning.
#[tokio::test]
async fn object_count_fallback_basic() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/object-count:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": 30 }]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(json_rows, json!([3]));
}

/// Op2 with a bound object value that has fewer matches.
#[tokio::test]
async fn object_count_single_match() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/object-count-single:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": 40 }]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    assert_eq!(json_rows, json!([1]));
}

/// Op2 with a bound object that matches no subjects → empty result set.
#[tokio::test]
async fn object_count_zero_matches() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/object-count-zero:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": 999 }]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // SPARQL semantics: ungrouped aggregates return exactly one row even with zero matches.
    assert_eq!(json_rows, json!([0]));
}

// =============================================================================
// Fallback: policy enforcer active
// =============================================================================

/// When a policy enforcer is active, the fast-path operators must fall back
/// to the standard pipeline. Verify that an allow-all policy returns the
/// same results as no policy.
#[tokio::test]
async fn group_count_topk_with_allow_all_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/group-count-policy:main").await;
    let ctx = context_ex_schema();

    // Op1 query shape
    let query = json!({
        "@context": ctx,
        "from": "fast/group-count-policy:main",
        "opts": {
            "policy": [{
                "@id": "ex:allowAll",
                "@type": "f:AccessPolicy",
                "f:action": "f:view",
                "f:allow": true
            }],
            "default-allow": true
        },
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": "?age" }],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([[30, 3], [25, 2], [40, 1]]))
    );
}

/// Op2 with allow-all policy — same results as without policy.
#[tokio::test]
async fn object_count_with_allow_all_policy() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/object-count-policy:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "from": "fast/object-count-policy:main",
        "opts": {
            "policy": [{
                "@id": "ex:allowAll",
                "@type": "f:AccessPolicy",
                "f:action": "f:view",
                "f:allow": true
            }],
            "default-allow": true
        },
        "select": ["(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": 30 }]
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([3]));
}

// =============================================================================
// Multi-pattern queries (standard path, NOT fast path)
// =============================================================================

/// A GROUP BY + COUNT query with multiple triple patterns does NOT trigger the
/// fast path. Verify correct results via the standard pipeline.
#[tokio::test]
async fn multi_pattern_group_count_uses_standard_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/multi-pattern:main").await;
    let ctx = context_ex_schema();

    // Two triple patterns → can't use Op1 fast path.
    let query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [
            { "@id": "?s", "schema:age": "?age" },
            { "@id": "?s", "schema:name": "?name" }
        ],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Same result as single-pattern (each person has one name)
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([[30, 3], [25, 2], [40, 1]]))
    );
}

/// Multi-pattern with a filter that reduces the result set.
#[tokio::test]
async fn multi_pattern_group_count_with_filter() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/multi-pattern-filter:main").await;
    let ctx = context_ex_schema();

    let query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [
            { "@id": "?s", "schema:age": "?age" },
            { "@id": "?s", "schema:name": "?name" },
            ["filter", ["!=", "?name", "Alice"]]
        ],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let json_rows = result.to_jsonld(&ledger.snapshot).expect("jsonld");

    // Alice (age=30) is excluded: age=30 now has 2, age=25 has 2, age=40 has 1
    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([[30, 2], [25, 2], [40, 1]]))
    );
}

// =============================================================================
// Consistency: fast-path query vs equivalent standard-path query
// =============================================================================

/// Verify that the Op1 fast-path query shape returns the same results as an
/// equivalent query that doesn't match the fast-path detection (by adding
/// a no-op BIND that breaks the single-pattern requirement).
#[tokio::test]
async fn fast_vs_standard_path_consistency() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_age_data(&fluree, "fast/consistency:main").await;
    let ctx = context_ex_schema();

    // Fast-path eligible query
    let fast_query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": "?age" }],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    // Equivalent query that avoids fast-path by adding a second triple pattern
    let std_query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [
            { "@id": "?s", "schema:age": "?age" },
            { "@id": "?s", "schema:name": "?name" }
        ],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    let fast_result = support::query_jsonld(&fluree, &ledger, &fast_query)
        .await
        .expect("fast query");
    let fast_rows = fast_result
        .to_jsonld(&ledger.snapshot)
        .expect("fast jsonld");

    let std_result = support::query_jsonld(&fluree, &ledger, &std_query)
        .await
        .expect("std query");
    let std_rows = std_result.to_jsonld(&ledger.snapshot).expect("std jsonld");

    assert_eq!(
        normalize_rows(&fast_rows),
        normalize_rows(&std_rows),
        "fast-path and standard-path should produce identical results"
    );
}

// =============================================================================
// Fallback: time-travel (history mode)
// =============================================================================

/// Query at an earlier `t` forces history mode, which triggers fallback.
/// Verify correct results at both t=1 and t=2.
#[tokio::test]
async fn group_count_at_earlier_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (ledger_t1, _ledger_t2) = seed_two_txns(&fluree, "fast/group-count-tt:main").await;

    // Query at t=1: Alice(30), Bob(30), Carol(25)
    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["?age", "(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": "?age" }],
        "groupBy": ["?age"],
        "orderBy": "(desc ?count)",
        "limit": 10
    });

    let result = support::query_jsonld(&fluree, &ledger_t1, &query)
        .await
        .expect("query at t=1");
    let json_rows = result.to_jsonld(&ledger_t1.snapshot).expect("jsonld");

    assert_eq!(
        normalize_rows(&json_rows),
        normalize_rows(&json!([[30, 2], [25, 1]]))
    );
}

/// Verify Op2 returns correct count at an earlier time point.
#[tokio::test]
async fn object_count_at_earlier_t() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (ledger_t1, ledger_t2) = seed_two_txns(&fluree, "fast/object-count-tt:main").await;

    let ctx = context_ex_schema();
    let query = json!({
        "@context": ctx,
        "select": ["(as (count ?s) ?count)"],
        "where": [{ "@id": "?s", "schema:age": 30 }]
    });

    // At t=1: Alice + Bob → 2
    let r1 = support::query_jsonld(&fluree, &ledger_t1, &query)
        .await
        .expect("query at t=1");
    let j1 = r1.to_jsonld(&ledger_t1.snapshot).expect("jsonld t=1");
    assert_eq!(j1, json!([2]));

    // At t=2: Alice + Bob + Dave → 3
    let r2 = support::query_jsonld(&fluree, &ledger_t2, &query)
        .await
        .expect("query at t=2");
    let j2 = r2.to_jsonld(&ledger_t2.snapshot).expect("jsonld t=2");
    assert_eq!(j2, json!([3]));
}
