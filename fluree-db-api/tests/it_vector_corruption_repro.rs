#![cfg(feature = "native")]

//! Round-trip tests for vector-typed values across ingest paths.
//!
//! These tests pin the post-fix behavior for the vector-corruption bug
//! (context-coerced bare-array `@vector` and SPARQL `f:embeddingVector`
//! typed literals). Both ingest paths must now produce a single
//! `FlakeValue::Vector` flake with `dt = embeddingVector` — never the
//! previous corrupt shape of N scalar flakes each tagged with the
//! vector datatype.
//!
//! Vector retraction is pinned here at three layers:
//!
//! - `jsonld_context_vector_bare_array_retracts_after_indexing` — the
//!   post-rebuild path (full rebuild from commit chain). The indexer's
//!   `resolve_object` resolves a vector retraction to the existing
//!   assertion's arena handle so merge cancellation by
//!   `(s, p, dt, o_kind, o_key)` matches.
//! - `jsonld_context_vector_bare_array_retracts_via_novelty_overlay` —
//!   the pre-rebuild novelty-overlay path. `binary_scan::translate_one_flake_v3_pub`
//!   short-circuits `FlakeValue::Vector` retractions through
//!   `BinaryIndexStore::find_vector_handle_by_fact` (SPOT scan + value
//!   compare) so the overlay subtracts the right indexed row without a
//!   republish.
//! - `jsonld_vector_retracts_via_incremental_publish` — the
//!   incremental-publish path (most common in production once a base
//!   index exists). `incremental_resolve` pre-loads base vector arenas
//!   into `shared.vectors` and SPOT-scans the base for VECTOR_ID rows to
//!   pre-populate `vector_fact_handles`. Chunk inserts then append to
//!   the unified arena (handles already global), and chunk retractions
//!   of base-asserted vectors find their handle via the pre-populated
//!   fact map.

mod support;

use fluree_db_api::{FlureeBuilder, LedgerState};
use fluree_db_transact::{NamespaceRegistry, Txn, TxnOpts};
use serde_json::json;

fn lower_sparql_update(ledger: &LedgerState, sparql: &str) -> Txn {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse failed: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = NamespaceRegistry::from_db(&ledger.snapshot);
    fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect("lower SPARQL update")
}

#[tokio::test]
async fn jsonld_context_vector_bare_array_round_trips_after_indexing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/jsonld-context:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{
            "@id": "ex:doc1",
            "@type": "ex:VectorTest",
            "ex:embedding": [0.1, 0.2, 0.3, 0.4]
        }]
    });

    let receipt = fluree
        .insert(ledger0, &insert)
        .await
        .expect("context-coerced bare-array vector insert");
    assert_eq!(receipt.receipt.t, 1, "single-flake commit");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load indexed ledger");

    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &loaded, select)
        .await
        .expect("query indexed vector")
        .to_jsonld_async(loaded.as_graph_db_ref(0))
        .await
        .expect("format vector result");
    // Pre-fix this query failed with "vector handle out of arena" because the
    // JSON-LD expansion split the array into 4 scalar flakes each tagged with
    // VECTOR_ID. Post-fix expansion produces ONE FlakeValue::Vector flake
    // and the index materializes it as a 4-element JSON array.
    let vector = rows
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|row| row.first())
        .and_then(|value| value.as_array())
        .expect("single vector result row");
    assert_eq!(vector.len(), 4, "expected 4 vector elements");
    for (actual, expected) in vector.iter().zip([0.1_f64, 0.2, 0.3, 0.4]) {
        let actual = actual.as_f64().expect("vector element");
        assert!(
            (actual - expected).abs() < 0.000_001,
            "expected {expected}, got {actual}"
        );
    }
}

#[tokio::test]
async fn jsonld_context_vector_empty_array_is_rejected() {
    // Belt-and-suspenders for the user-facing path: an empty `[]` vector
    // value must fail the insert with a clear error before any flake is
    // committed. Empty `FlakeValue::Vector(Vec::new())` is reserved as the
    // `FlakeValue::max()` upper-bound sentinel and is hard-rejected by the
    // shared vector arena. The corruption fix layered two guards so this
    // can't sneak through any ingest path:
    //   1. `core::coerce::coerce_array_to_vector` rejects upstream
    //      (the live transact JSON-LD path goes through this).
    //   2. `transact::generate::flakes::validate_value_dt_pair` rejects
    //      at the write-path bottleneck (catches anything that somehow
    //      bypassed layer 1).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/jsonld-empty:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{
            "@id": "ex:doc1",
            "ex:embedding": []
        }]
    });

    let err = fluree
        .insert(ledger0, &insert)
        .await
        .expect_err("empty vector must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("at least one element") || msg.contains("embeddingVector"),
        "expected empty-vector rejection diagnostic, got: {msg}"
    );
}

/// Vector retraction round-trip. Pre-fix the index-merge cancellation
/// missed because the retraction's vector went through
/// `VectorArena::insert_f32` and was assigned a fresh arena handle that
/// never matched the assertion's `(s, p, dt, o_kind, o_key)`. Post-fix,
/// the indexer's `resolve_object` re-resolves vector retractions to the
/// existing assertion handle via `VectorArena::find_handle_by_value`.
#[tokio::test]
async fn jsonld_context_vector_bare_array_retracts_after_indexing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/jsonld-retract:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{
            "@id": "ex:doc1",
            "ex:embedding": [0.1, 0.2, 0.3, 0.4]
        }]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert vector");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc1 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&loaded, delete);
    fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("DELETE WHERE executes");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let reloaded = fluree.ledger(ledger_id).await.expect("reload");

    let count = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?count) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &reloaded, count)
        .await
        .expect("count")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(count_rows, json!([[0]]));
}

/// Vector retraction via the *novelty-overlay* path: insert + publish
/// index, then DELETE WHERE, then COUNT *without* republishing. The
/// retraction sits in novelty and must overlay-suppress the indexed
/// assertion at query time. Pre-fix this returned 1 because the overlay
/// translation in `binary_scan` couldn't translate `FlakeValue::Vector`
/// retractions back into base-index `o_key` space; post-fix it goes
/// through `BinaryIndexStore::find_vector_handle` to re-resolve the
/// assertion's handle so the overlay subtracts it.
#[tokio::test]
async fn jsonld_context_vector_bare_array_retracts_via_novelty_overlay() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/jsonld-novelty-retract:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{
            "@id": "ex:doc1",
            "ex:embedding": [0.1, 0.2, 0.3, 0.4]
        }]
    });
    fluree
        .insert(ledger0, &insert)
        .await
        .expect("insert vector");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc1 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&loaded, delete);
    let deleted = fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("DELETE WHERE executes");

    // Note: NO second rebuild_and_publish_index — this exercises the
    // novelty-overlay path against the still-base index.
    let count = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?count) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &deleted.ledger, count)
        .await
        .expect("count after delete (novelty-overlay path)")
        .to_jsonld_async(deleted.ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[0]]),
        "novelty overlay must suppress indexed vector flake"
    );
}

/// Two subjects storing the *same* vector value under the same predicate.
/// Each gets its own arena handle. Retracting one must cancel ONLY that
/// row — not the other subject's. The pre-fix value-only handle lookup
/// (`find_handle_by_value`) returned the FIRST matching handle, so
/// retracting `doc2` aliased to `doc1`'s handle and corrupted both rows.
/// Full fact-identity lookup `(g_id, s_id, p_id, o_i, f32_bits)` is
/// required (the value bits are needed because the same `(s, p, o_i)`
/// can also hold multiple distinct values — see the multi-valued test).
#[tokio::test]
async fn jsonld_context_vector_duplicate_values_retract_one_keeps_other() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/duplicate-values:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [
            {"@id": "ex:doc1", "ex:embedding": [0.1, 0.2, 0.3]},
            {"@id": "ex:doc2", "ex:embedding": [0.1, 0.2, 0.3]}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    // Retract only doc2.
    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc2 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&loaded, delete);
    fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("delete doc2");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let reloaded = fluree.ledger(ledger_id).await.expect("reload");

    // doc1 must still have its vector.
    let select_doc1 = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &reloaded, select_doc1)
        .await
        .expect("query doc1")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_ne!(
        rows,
        json!([]),
        "doc1's vector must survive a same-value retraction of doc2"
    );

    // doc2 must be empty.
    let select_doc2 = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc2 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &reloaded, select_doc2)
        .await
        .expect("query doc2")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(rows, json!([]), "doc2's vector must be retracted");
}

/// One subject holding TWO different vector values under the same
/// predicate (multi-cardinality, no list indices). Both flakes have
/// `o_i = LIST_INDEX_NONE`, so a fact-identity key of just
/// `(s_id, p_id, o_i)` collides between them. Retracting one must
/// cancel ONLY the matching value, not the other.
#[tokio::test]
async fn jsonld_multi_valued_vectors_retract_one_keeps_other() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/multi-valued-rebuild:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{
            "@id": "ex:doc1",
            "ex:embedding": [
                {"@value": [0.1, 0.2, 0.3], "@type": "@vector"},
                {"@value": [0.7, 0.8, 0.9], "@type": "@vector"}
            ]
        }]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    // Retract only [0.1, 0.2, 0.3].
    let delete = r#"
        PREFIX ex: <http://example.org/>
        PREFIX f: <https://ns.flur.ee/db#>
        DELETE DATA {
            ex:doc1 ex:embedding "[0.1, 0.2, 0.3]"^^f:embeddingVector .
        }
    "#;
    let txn = lower_sparql_update(&loaded, delete);
    fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("delete one of two");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let reloaded = fluree.ledger(ledger_id).await.expect("reload");

    let count = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?n) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &reloaded, count)
        .await
        .expect("count")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[1]]),
        "exactly one of two same-(s,p) vector values must remain"
    );

    // The surviving row must be [0.7, 0.8, 0.9], not [0.1, 0.2, 0.3].
    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &reloaded, select)
        .await
        .expect("select")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    let surviving = rows
        .as_array()
        .and_then(|rs| rs.first())
        .and_then(|r| r.as_array())
        .and_then(|r| r.first())
        .and_then(|v| v.as_array())
        .expect("one surviving vector");
    let first = surviving[0].as_f64().expect("element");
    assert!(
        (first - 0.7).abs() < 0.000_001,
        "surviving vector must be the one we did NOT retract; got first element {first}"
    );
}

/// Re-asserting the same logical vector fact across commits must not
/// produce a second arena handle. Pre-fix the resolver always called
/// `insert_f64` on assert, so two assertions of `(s, p, o_i, value)`
/// got two distinct `o_key`s — the rebuild ended up with two encoded
/// facts and a subsequent retraction couldn't cancel both. The fix
/// dedups: if `fact_map` already has the key, reuse the existing
/// handle. Stable encoded identity means assert/retract/assert cycles
/// work naturally under latest-op semantics.
#[tokio::test]
async fn jsonld_re_asserting_same_vector_dedups() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/dedup-reassert:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let same_value = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{ "@id": "ex:doc1", "ex:embedding": [0.1, 0.2, 0.3] }]
    });

    // Commit 1: insert.
    let after1 = fluree
        .insert(ledger0, &same_value)
        .await
        .expect("first insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded1 = fluree.ledger(ledger_id).await.expect("load after first");

    // Commit 2: insert the EXACT same fact again.
    fluree
        .insert(after1.ledger, &same_value)
        .await
        .expect("re-assert same vector");
    let _ = loaded1; // sanity drop
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let reloaded = fluree
        .ledger(ledger_id)
        .await
        .expect("load after re-assert");

    let count_q = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?n) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &reloaded, count_q)
        .await
        .expect("count after re-assert")
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[1]]),
        "re-asserting same vector fact must dedup to one encoded row"
    );

    // Now delete it; count must be 0.
    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc1 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&reloaded, delete);
    fluree
        .stage_owned(reloaded)
        .txn(txn)
        .execute()
        .await
        .expect("delete after re-assert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let final_ledger = fluree.ledger(ledger_id).await.expect("final reload");
    let count_rows = support::query_sparql(&fluree, &final_ledger, count_q)
        .await
        .expect("count after delete")
        .to_jsonld_async(final_ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[0]]),
        "single retraction must cancel the deduped assertion"
    );
}

/// Multi-valued vector retraction via the *novelty-overlay* path.
/// The SPOT-prefix scan in `find_vector_handle_by_fact` returns multiple
/// candidate rows (same s/p/o_i, different vectors); the value-bit
/// comparison must pick the right one.
#[tokio::test]
async fn jsonld_multi_valued_vectors_overlay_retract_one_keeps_other() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/multi-valued-overlay:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": { "ex": "http://example.org/" },
        "@graph": [{
            "@id": "ex:doc1",
            "ex:embedding": [
                {"@value": [0.1, 0.2, 0.3], "@type": "@vector"},
                {"@value": [0.7, 0.8, 0.9], "@type": "@vector"}
            ]
        }]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    let delete = r#"
        PREFIX ex: <http://example.org/>
        PREFIX f: <https://ns.flur.ee/db#>
        DELETE DATA {
            ex:doc1 ex:embedding "[0.1, 0.2, 0.3]"^^f:embeddingVector .
        }
    "#;
    let txn = lower_sparql_update(&loaded, delete);
    let deleted = fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("delete one of two via overlay");

    // No second rebuild — overlay path.
    let count = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?n) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &deleted.ledger, count)
        .await
        .expect("count")
        .to_jsonld_async(deleted.ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[1]]),
        "overlay must cancel exactly one of two same-(s,p) vector values"
    );

    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &deleted.ledger, select)
        .await
        .expect("select")
        .to_jsonld_async(deleted.ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    let surviving = rows
        .as_array()
        .and_then(|rs| rs.first())
        .and_then(|r| r.as_array())
        .and_then(|r| r.first())
        .and_then(|v| v.as_array())
        .expect("one surviving vector");
    let first = surviving[0].as_f64().expect("element");
    assert!(
        (first - 0.7).abs() < 0.000_001,
        "overlay surviving vector must be the one we did NOT retract; got first element {first}"
    );
}

/// Same as `jsonld_context_vector_duplicate_values_retract_one_keeps_other`
/// but exercises the *novelty-overlay* path (no rebuild between the two
/// commits). The overlay translation in `binary_scan` must also use fact
/// identity, not value-only lookup, otherwise it can resolve the
/// retraction to the wrong base-arena handle and cancel the wrong row.
#[tokio::test]
async fn jsonld_context_vector_duplicate_values_overlay_retract_one_keeps_other() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/duplicate-overlay:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [
            {"@id": "ex:doc1", "ex:embedding": [0.1, 0.2, 0.3]},
            {"@id": "ex:doc2", "ex:embedding": [0.1, 0.2, 0.3]}
        ]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load");

    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc2 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&loaded, delete);
    let deleted = fluree
        .stage_owned(loaded)
        .txn(txn)
        .execute()
        .await
        .expect("delete doc2");

    // No second rebuild — overlay path.
    let select_doc1 = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &deleted.ledger, select_doc1)
        .await
        .expect("query doc1 via overlay")
        .to_jsonld_async(deleted.ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_ne!(
        rows,
        json!([]),
        "overlay must not cancel doc1 when retracting doc2 with same value"
    );

    let select_doc2 = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc2 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &deleted.ledger, select_doc2)
        .await
        .expect("query doc2 via overlay")
        .to_jsonld_async(deleted.ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(rows, json!([]), "overlay must retract doc2");
}

/// Incremental publish path: vector asserted in commit 1 (publish triggers
/// full rebuild), then retracted in commit 2 (publish triggers
/// *incremental* indexing because a base index exists). The retraction
/// must apply on the incremental publish — not silently drop with the
/// base assertion left in place.
///
/// Pre-fix the chunk-local `vector_fact_handles` only knew assertions
/// from THIS chunk; retractions of base-asserted vectors returned
/// `Ok(None)` and the merge dropped them. After incremental publish the
/// base assertion was still queryable, the user's DELETE silently
/// reverted.
#[tokio::test]
async fn jsonld_vector_retracts_via_incremental_publish() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/incremental-retract:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    // Commit 1: insert vector. Publish (full rebuild — no prior index).
    let insert = json!({
        "@context": {
            "ex": "http://example.org/",
            "ex:embedding": { "@type": "@vector" }
        },
        "@graph": [{ "@id": "ex:doc1", "ex:embedding": [0.1, 0.2, 0.3, 0.4] }]
    });
    fluree.insert(ledger0, &insert).await.expect("insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let after_publish_1 = fluree
        .ledger(ledger_id)
        .await
        .expect("load after publish 1");

    // Commit 2: delete the vector. Publish via `build_and_publish_index`
    // which dispatches to incremental indexing when a base exists.
    let delete = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:doc1 ex:embedding ?v }
    ";
    let txn = lower_sparql_update(&after_publish_1, delete);
    fluree
        .stage_owned(after_publish_1)
        .txn(txn)
        .execute()
        .await
        .expect("DELETE WHERE");
    support::build_and_publish_index(&fluree, ledger_id).await;
    let final_ledger = fluree
        .ledger(ledger_id)
        .await
        .expect("load after publish 2");

    let count = r"
        PREFIX ex: <http://example.org/>
        SELECT (COUNT(*) AS ?n) WHERE { ex:doc1 ex:embedding ?v }
    ";
    let count_rows = support::query_sparql(&fluree, &final_ledger, count)
        .await
        .expect("count after incremental")
        .to_jsonld_async(final_ledger.as_graph_db_ref(0))
        .await
        .expect("format");
    assert_eq!(
        count_rows,
        json!([[0]]),
        "incremental publish must apply the retraction of a base-asserted vector"
    );
}

/// SPARQL `DELETE DATA` for a vector that was never inserted must NOT
/// fail the transaction. Other datatypes encode unmatched retractions
/// harmlessly; vectors must do the same. Pre-fix this returned an
/// `InvalidOp` because `find_handle_by_value` returned None and the
/// resolver propagated it as an error.
#[tokio::test]
async fn sparql_delete_data_unmatched_vector_is_no_op() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/unmatched-retract:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    // First commit: insert SOMETHING so the ledger has at least one
    // namespace allocation; this is a generic warm-up, not a vector.
    let warm = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:doc1", "ex:name": "Alice"}]
    });
    let inserted = fluree.insert(ledger0, &warm).await.expect("warm insert");

    // Now DELETE DATA a vector that doesn't exist anywhere.
    let delete = r#"
        PREFIX ex: <http://example.org/>
        PREFIX f: <https://ns.flur.ee/db#>
        DELETE DATA {
            ex:ghost ex:embedding "[0.1, 0.2, 0.3]"^^f:embeddingVector .
        }
    "#;
    let txn = lower_sparql_update(&inserted.ledger, delete);
    let _ = fluree
        .stage_owned(inserted.ledger)
        .txn(txn)
        .execute()
        .await
        .expect("unmatched vector retraction must not error");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
}

#[tokio::test]
async fn sparql_insert_data_embedding_vector_literal_round_trips_after_indexing() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/vector-corruption/sparql-insert:main";
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);

    let insert = r#"
        PREFIX ex: <http://example.org/>
        PREFIX f: <https://ns.flur.ee/db#>
        INSERT DATA {
            ex:doc1 ex:embedding "[0.1, 0.2, 0.3, 0.4]"^^f:embeddingVector .
        }
    "#;
    let txn = lower_sparql_update(&ledger0, insert);
    let _inserted = fluree
        .stage_owned(ledger0)
        .txn(txn)
        .execute()
        .await
        .expect("SPARQL vector typed literal insert");
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let loaded = fluree.ledger(ledger_id).await.expect("load indexed ledger");

    // Pre-fix this returned [] because the lowering step's namespace
    // allocations (e.g. `ex/` → 13) lived only in the caller-owned
    // NamespaceRegistry — `stage_transaction_from_txn` built its own
    // registry from the (pre-commit, empty-namespace) snapshot, never saw
    // the lowering's allocations, and committed flakes whose namespace
    // codes the post-commit snapshot couldn't resolve back to IRIs. Fixed
    // by `Txn.namespace_delta` + `adopt_delta_for_persistence`.
    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?v WHERE { ex:doc1 ex:embedding ?v }
    ";
    let rows = support::query_sparql(&fluree, &loaded, select)
        .await
        .expect("query should produce results")
        .to_jsonld_async(loaded.as_graph_db_ref(0))
        .await
        .expect("format result");
    let vector = rows
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|row| row.first())
        .and_then(|value| value.as_array())
        .expect("single vector result row");
    assert_eq!(vector.len(), 4);
    for (actual, expected) in vector.iter().zip([0.1_f64, 0.2, 0.3, 0.4]) {
        let actual = actual.as_f64().expect("vector element");
        assert!(
            (actual - expected).abs() < 0.000_001,
            "expected {expected}, got {actual}"
        );
    }
}
