//! Integration tests for deep tracing span instrumentation.
//!
//! Verifies that queries, transactions, and commits produce the expected
//! span hierarchy with correct names, levels, and parent-child relationships.
//!
//! These tests programmatically verify what the OTEL/Jaeger harness validates
//! visually — they are the automated acceptance criteria for Phases 1-5.
//!
//! All tests use `current_thread` tokio flavor to ensure the thread-local
//! `set_default()` subscriber captures spans from all async work.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::json;
use support::span_capture;

/// Seed a small dataset and return the ledger state.
async fn seed_people(fluree: &support::MemoryFluree, ledger_id: &str) -> support::MemoryLedger {
    let ledger0 = support::genesis_ledger(fluree, ledger_id);
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:age": 30
            },
            {
                "@id": "ex:bob",
                "@type": "ex:User",
                "schema:name": "Bob",
                "schema:age": 25
            }
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("seed insert")
        .ledger
}

// =============================================================================
// AC-1: Query waterfall — verify query preparation and execution spans
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac1_fql_query_waterfall() {
    let (_store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac1-jsonld:main").await;

    // Clear seeding spans by creating a fresh store (re-init tracing)
    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = support::query_jsonld(&fluree, &ledger, &query).await;
    assert!(result.is_ok(), "query should succeed: {:?}", result.err());

    // Verify query preparation hierarchy exists
    assert!(
        store.has_span("query_prepare"),
        "query_prepare span should exist. Captured spans: {:?}",
        store.span_names()
    );
    assert!(
        store.has_span("reasoning_prep"),
        "reasoning_prep span should exist"
    );
    assert!(
        store.has_span("pattern_rewrite"),
        "pattern_rewrite span should exist"
    );
    assert!(store.has_span("plan"), "plan span should exist");
    assert!(store.has_span("query_run"), "query_run span should exist");

    // Verify levels
    let qp = store.find_span("query_prepare").unwrap();
    assert_eq!(
        qp.level,
        tracing::Level::DEBUG,
        "query_prepare should be DEBUG"
    );

    let rp = store.find_span("reasoning_prep").unwrap();
    assert_eq!(
        rp.level,
        tracing::Level::DEBUG,
        "reasoning_prep should be DEBUG"
    );

    let pr = store.find_span("pattern_rewrite").unwrap();
    assert_eq!(
        pr.level,
        tracing::Level::DEBUG,
        "pattern_rewrite should be DEBUG"
    );

    let plan = store.find_span("plan").unwrap();
    assert_eq!(plan.level, tracing::Level::DEBUG, "plan should be DEBUG");

    let qr = store.find_span("query_run").unwrap();
    assert_eq!(qr.level, tracing::Level::DEBUG, "query_run should be DEBUG");

    // Verify parent-child: reasoning_prep should be child of query_prepare
    assert_eq!(
        rp.parent_name.as_deref(),
        Some("query_prepare"),
        "reasoning_prep should be child of query_prepare"
    );

    // pattern_rewrite should be child of query_prepare
    assert_eq!(
        pr.parent_name.as_deref(),
        Some("query_prepare"),
        "pattern_rewrite should be child of query_prepare"
    );

    // plan should be child of query_prepare
    assert_eq!(
        plan.parent_name.as_deref(),
        Some("query_prepare"),
        "plan should be child of query_prepare"
    );

    // Verify deferred fields on pattern_rewrite
    assert!(
        pr.fields.contains_key("patterns_before"),
        "pattern_rewrite should have patterns_before field"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ac1_sparql_query_waterfall() {
    let (_store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac1-sparql:main").await;

    // Re-init tracing to capture only query spans
    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    let sparql = r"
        PREFIX schema: <http://schema.org/>
        PREFIX ex: <http://example.org/ns/>
        SELECT ?name WHERE {
            ?s a ex:User .
            ?s schema:name ?name .
        }
    ";

    let result = support::query_sparql(&fluree, &ledger, sparql).await;
    assert!(
        result.is_ok(),
        "SPARQL query should succeed: {:?}",
        result.err()
    );

    // Same hierarchy as JSON-LD — execution path is shared
    assert!(
        store.has_span("query_prepare"),
        "query_prepare span should exist"
    );
    assert!(
        store.has_span("reasoning_prep"),
        "reasoning_prep span should exist"
    );
    assert!(
        store.has_span("pattern_rewrite"),
        "pattern_rewrite span should exist"
    );
    assert!(store.has_span("plan"), "plan span should exist");
    assert!(store.has_span("query_run"), "query_run span should exist");
}

// =============================================================================
// AC-2a: Insert waterfall — verify staging and commit spans
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac2a_insert_waterfall() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac2a:main");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice"
        }]
    });

    let result = fluree.insert(ledger0, &insert).await;
    assert!(result.is_ok(), "insert should succeed: {:?}", result.err());

    // Verify staging spans
    assert!(
        store.has_span("txn_stage"),
        "txn_stage span should exist. Captured spans: {:?}",
        store.span_names()
    );

    let stage = store.find_span("txn_stage").unwrap();
    assert_eq!(
        stage.level,
        tracing::Level::DEBUG,
        "txn_stage should be DEBUG"
    );

    // Insert should produce insert_gen and cancellation sub-spans
    assert!(store.has_span("insert_gen"), "insert_gen span should exist");
    assert!(
        store.has_span("cancellation"),
        "cancellation span should exist"
    );

    // Verify staging sub-span levels
    let ig = store.find_span("insert_gen").unwrap();
    assert_eq!(
        ig.level,
        tracing::Level::DEBUG,
        "insert_gen should be DEBUG"
    );

    // Verify parent-child relationships.
    //
    // Post streaming-WHERE refactor, `insert_gen` is emitted per-batch
    // inside the `where_exec` loop (was previously a sibling of `where_exec`
    // under `txn_stage` in the eager path). `cancellation` stays a direct
    // child of `txn_stage` since it runs after the WHERE stream closes.
    assert_eq!(
        ig.parent_name.as_deref(),
        Some("where_exec"),
        "insert_gen should be child of where_exec (per-batch emission)"
    );

    let cancel = store.find_span("cancellation").unwrap();
    assert_eq!(
        cancel.parent_name.as_deref(),
        Some("txn_stage"),
        "cancellation should be child of txn_stage"
    );

    // Verify commit hierarchy
    assert!(store.has_span("txn_commit"), "txn_commit span should exist");

    let commit = store.find_span("txn_commit").unwrap();
    assert_eq!(
        commit.level,
        tracing::Level::DEBUG,
        "txn_commit should be DEBUG"
    );

    // Verify commit sub-spans exist (key I/O phases)
    assert!(
        store.has_span("commit_nameservice_lookup"),
        "commit_nameservice_lookup should exist"
    );
    assert!(
        store.has_span("commit_verify_sequencing"),
        "commit_verify_sequencing should exist"
    );
    assert!(
        store.has_span("commit_apply_to_novelty"),
        "commit_apply_to_novelty should exist"
    );

    // Verify commit sub-spans are children of txn_commit
    let ns_lookup = store.find_span("commit_nameservice_lookup").unwrap();
    assert_eq!(
        ns_lookup.parent_name.as_deref(),
        Some("txn_commit"),
        "commit_nameservice_lookup should be child of txn_commit"
    );
}

// =============================================================================
// AC-2b: Update waterfall — verify WHERE + DELETE + INSERT staging
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac2b_update_waterfall() {
    let (_store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac2b:main").await;

    // Re-init to capture only update spans
    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    let update = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "where": {
            "@id": "?s",
            "schema:name": "Alice"
        },
        "delete": {
            "@id": "?s",
            "schema:age": "?any"
        },
        "insert": {
            "@id": "?s",
            "schema:age": 31
        }
    });

    let result = fluree.update(ledger, &update).await;
    assert!(result.is_ok(), "update should succeed: {:?}", result.err());

    // Verify full staging hierarchy for updates
    assert!(
        store.has_span("txn_stage"),
        "txn_stage should exist. Captured spans: {:?}",
        store.span_names()
    );

    // Updates should produce WHERE execution span
    assert!(
        store.has_span("where_exec"),
        "where_exec span should exist for updates"
    );

    let we = store.find_span("where_exec").unwrap();
    assert_eq!(
        we.level,
        tracing::Level::DEBUG,
        "where_exec should be DEBUG"
    );
    assert_eq!(
        we.parent_name.as_deref(),
        Some("txn_stage"),
        "where_exec should be child of txn_stage"
    );

    // Updates should produce delete_gen span
    assert!(
        store.has_span("delete_gen"),
        "delete_gen span should exist for updates"
    );

    let dg = store.find_span("delete_gen").unwrap();
    assert_eq!(
        dg.level,
        tracing::Level::DEBUG,
        "delete_gen should be DEBUG"
    );
    // Post streaming-WHERE refactor, `delete_gen` is emitted per-batch
    // inside the `where_exec` loop.
    assert_eq!(
        dg.parent_name.as_deref(),
        Some("where_exec"),
        "delete_gen should be child of where_exec (per-batch emission)"
    );

    // And insert_gen + cancellation
    assert!(store.has_span("insert_gen"), "insert_gen should exist");
    assert!(store.has_span("cancellation"), "cancellation should exist");

    // Verify deferred fields on staging spans
    let we = store.find_span("where_exec").unwrap();
    assert!(
        we.fields.contains_key("pattern_count"),
        "where_exec should have pattern_count field"
    );

    let ig = store.find_span("insert_gen").unwrap();
    assert!(
        ig.fields.contains_key("template_count"),
        "insert_gen should have template_count field"
    );
}

// =============================================================================
// AC-5: Zero noise at INFO — no debug/trace spans at production default
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac5_zero_noise_at_info() {
    let (store, _guard) = span_capture::init_info_only_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac5:main").await;

    // Run a query
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });
    let _ = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    // Verify: no debug or trace spans captured
    let debug_spans = store.debug_spans();
    assert!(
        debug_spans.is_empty(),
        "At INFO level, no debug spans should be captured. Found: {:?}",
        debug_spans.iter().map(|s| s.name).collect::<Vec<_>>()
    );

    let trace_spans = store.trace_spans();
    assert!(
        trace_spans.is_empty(),
        "At INFO level, no trace spans should be captured. Found: {:?}",
        trace_spans.iter().map(|s| s.name).collect::<Vec<_>>()
    );

    // All operation spans are now debug_span!, so at INFO level the API layer
    // should produce ZERO spans. This validates the true zero-overhead guarantee:
    // without otel or debug logging, no span metadata is allocated.
    let all_spans = store.span_names();
    assert!(
        all_spans.is_empty(),
        "At INFO level, zero spans should be captured from the API layer (all are debug_span!). Found: {all_spans:?}"
    );
}

// =============================================================================
// AC-3: Deferred field recording — verify fields are populated after creation
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac3_deferred_fields_recorded() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac3:main");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice"},
            {"@id": "ex:bob", "@type": "ex:User", "schema:name": "Bob"}
        ]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed");

    // Verify insert_gen has deferred assertion_count recorded
    let ig = store
        .find_span("insert_gen")
        .expect("insert_gen should exist");
    assert!(
        ig.fields.contains_key("assertion_count"),
        "insert_gen should have assertion_count deferred field recorded. Fields: {:?}",
        ig.fields
    );

    // The assertion count should be > 0 (we inserted two entities with type + name)
    let count: u64 = ig
        .fields
        .get("assertion_count")
        .unwrap()
        .parse()
        .expect("assertion_count should be a u64");
    assert!(count > 0, "assertion_count should be > 0, got {count}");

    // Verify pattern_rewrite has patterns_after deferred field recorded
    // (patterns_after is set via Span::current().record())
    // Note: pattern_rewrite only happens during queries, not during inserts.
    // We'll verify it in the query path.
    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    let ledger = fluree
        .insert(support::genesis_ledger(&fluree, "ac3b:main"), &insert)
        .await
        .unwrap()
        .ledger;

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    let pr = store
        .find_span("pattern_rewrite")
        .expect("pattern_rewrite should exist");
    assert!(
        pr.fields.contains_key("patterns_before"),
        "pattern_rewrite should have patterns_before. Fields: {:?}",
        pr.fields
    );
}

// =============================================================================
// AC-4: Commit sub-span hierarchy — verify all I/O phases
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn ac4_commit_subspan_hierarchy() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac4:main");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice"
        }]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed");

    // All commit sub-spans should be children of txn_commit
    let expected_commit_children = [
        "commit_nameservice_lookup",
        "commit_verify_sequencing",
        "commit_namespace_delta",
        "commit_build_record",
        "commit_generate_metadata_flakes",
        "commit_apply_to_novelty",
    ];

    for &child_name in &expected_commit_children {
        let child = store.find_span(child_name);
        assert!(
            child.is_some(),
            "{child_name} span should exist. Captured: {:?}",
            store.span_names()
        );
        let child = child.unwrap();
        assert_eq!(
            child.level,
            tracing::Level::DEBUG,
            "{child_name} should be DEBUG level"
        );
        assert_eq!(
            child.parent_name.as_deref(),
            Some("txn_commit"),
            "{child_name} should be child of txn_commit"
        );
    }
}

// =============================================================================
// AC-6: Top-level operation span coverage (H-5)
// =============================================================================

/// Verify the query span hierarchy in the API layer.
///
/// In the API layer, the query runner emits `query_prepare` and `query_run`.
/// There is no wrapping `query_execute` span here — that span exists only in the
/// server route handler (and other top-level wrappers).
///
/// The `query_execute` span only exists in:
/// - The server route handler (`routes/query.rs`)
/// - The `execute()` convenience function (`execute.rs`)
#[tokio::test(flavor = "current_thread")]
async fn api_query_hierarchy_has_prepare_and_run_at_top() {
    let (_store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac6-qe:main").await;

    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    // query_prepare and query_run should exist (these are the API-layer top-level spans)
    assert!(
        store.has_span("query_prepare"),
        "query_prepare should exist. Captured: {:?}",
        store.span_names()
    );
    assert!(store.has_span("query_run"), "query_run should exist");

    // query_execute does NOT exist in the API layer — it's a server route span
    assert!(
        !store.has_span("query_execute"),
        "query_execute should NOT exist in the API layer (it wraps prepare+run in the server route only)"
    );

    // query_prepare sub-spans should be children of query_prepare
    let rp = store.find_span("reasoning_prep").unwrap();
    assert_eq!(rp.parent_name.as_deref(), Some("query_prepare"));
    let pr = store.find_span("pattern_rewrite").unwrap();
    assert_eq!(pr.parent_name.as_deref(), Some("query_prepare"));
    let plan = store.find_span("plan").unwrap();
    assert_eq!(plan.parent_name.as_deref(), Some("query_prepare"));
}

/// Verify the transaction span hierarchy in the API layer.
///
/// In the API layer, `fluree.insert()` calls `stage()` + `commit()` directly,
/// so there is no wrapping `transact_execute` span (that only exists in the
/// server route handler).
#[tokio::test(flavor = "current_thread")]
async fn api_transaction_hierarchy_has_stage_and_commit_at_top() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac6-te:main");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice"
        }]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed");

    // txn_stage and txn_commit should both exist
    assert!(store.has_span("txn_stage"), "txn_stage should exist");
    assert!(store.has_span("txn_commit"), "txn_commit should exist");

    // In the API layer, transact_execute does NOT exist (it's a server-only span)
    assert!(
        !store.has_span("transact_execute"),
        "transact_execute should NOT exist in the API layer — it's a server route span only"
    );
}

// =============================================================================
// AC-7: Negative tests — spans do NOT cross operation boundaries (L-6)
// =============================================================================

/// Verify query spans do NOT appear in the transaction path and vice versa.
#[tokio::test(flavor = "current_thread")]
async fn query_spans_not_in_transaction_path() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac7-neg:main");

    // Only do an insert (no query)
    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice"
        }]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed");

    // Transaction path should NOT produce query-specific spans
    assert!(
        !store.has_span("query_prepare"),
        "query_prepare should NOT appear in insert path"
    );
    assert!(
        !store.has_span("query_run"),
        "query_run should NOT appear in insert path"
    );
    assert!(
        !store.has_span("reasoning_prep"),
        "reasoning_prep should NOT appear in insert path"
    );
    assert!(
        !store.has_span("pattern_rewrite"),
        "pattern_rewrite should NOT appear in insert path"
    );
    assert!(
        !store.has_span("plan"),
        "plan should NOT appear in pure insert path"
    );
}

/// Verify transaction spans do NOT appear in the query path.
#[tokio::test(flavor = "current_thread")]
async fn transaction_spans_not_in_query_path() {
    let (_store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_people(&fluree, "ac7-neg2:main").await;

    // Re-init to capture only query spans
    drop(_guard);
    let (store, _guard) = span_capture::init_test_tracing();

    // Only do a query (no transaction)
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");

    // Query path should NOT produce transaction-specific spans
    assert!(
        !store.has_span("txn_stage"),
        "txn_stage should NOT appear in query path"
    );
    assert!(
        !store.has_span("txn_commit"),
        "txn_commit should NOT appear in query path"
    );
    assert!(
        !store.has_span("insert_gen"),
        "insert_gen should NOT appear in query path"
    );
    assert!(
        !store.has_span("delete_gen"),
        "delete_gen should NOT appear in query path"
    );
    assert!(
        !store.has_span("commit_nameservice_lookup"),
        "commit_nameservice_lookup should NOT appear in query path"
    );
}

// =============================================================================
// AC-8: Span close tracking — verify spans are properly closed (L-2)
// =============================================================================

/// Verify that all captured spans are properly closed (no leaked guards).
#[tokio::test(flavor = "current_thread")]
async fn all_spans_properly_closed() {
    let (store, _guard) = span_capture::init_test_tracing();

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, "ac8:main");

    let insert = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "@graph": [{
            "@id": "ex:alice",
            "@type": "ex:User",
            "schema:name": "Alice"
        }]
    });

    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert should succeed");

    // All spans should be closed — no leaked guards
    let unclosed = store.unclosed_spans();
    assert!(
        unclosed.is_empty(),
        "All spans should be closed after operation completes. Unclosed: {:?}",
        unclosed.iter().map(|s| s.name).collect::<Vec<_>>()
    );
}
