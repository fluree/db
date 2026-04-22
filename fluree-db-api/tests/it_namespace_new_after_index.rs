//! Reproduction: new namespace introduced after binary index is attached.
//!
//! This demonstrates a subtle mismatch between:
//! - The in-memory `LedgerSnapshot.namespace_codes` (updated by commit deltas), and
//! - The already-attached `BinaryIndexStore` namespace table / prefix trie (built from the index root)
//!
//! When a new namespace is introduced *after* indexing and the binary store stays attached,
//! queries that bind IRIs in the new namespace can incorrectly return empty results because
//! the binary scan path normalizes bound SIDs through the store's namespace table.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_namespace_new_after_index --features native

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use fluree_db_ledger::TypeErasedStore;
use serde_json::json;

#[tokio::test]
async fn query_bound_iri_in_new_namespace_after_index_returns_row() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ns-new-after-index:main";

    // Commit t=1: introduce namespace A and some data.
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [
            { "@id": "a:thing1", "a:val": "seed" }
        ]
    });
    let r1 = fluree.insert(ledger0, &tx1).await.unwrap();
    assert_eq!(r1.receipt.t, 1);

    // Build the binary index at t=1 (binary store contains ns A in its prefix trie).
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");

    // Load a fresh LedgerState with an attached binary store + range provider.
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let ledger_indexed = handle.snapshot().await.to_ledger_state();
    assert!(
        ledger_indexed.snapshot.range_provider.is_some(),
        "expected binary range_provider after reindex"
    );
    assert!(
        ledger_indexed.binary_store.is_some(),
        "expected binary_store after reindex"
    );

    // Commit t=2: introduce a NEW namespace B and data that uses it.
    // IMPORTANT: we keep using the same loaded ledger state (binary store stays attached).
    let tx2 = json!({
        "@context": { "b": "http://example.org/b/" },
        "@graph": [
            { "@id": "b:thing2", "b:name": "Thing 2" }
        ]
    });
    let r2 = fluree.insert(ledger_indexed, &tx2).await.unwrap();
    assert_eq!(r2.receipt.t, 2);
    let ledger2 = r2.ledger;

    // Sanity: snapshot namespace codes now include the new prefix.
    let b_prefix = "http://example.org/b/";
    let Some((&b_code, _)) = ledger2
        .snapshot
        .namespaces()
        .iter()
        .find(|(_, p)| p.as_str() == b_prefix)
    else {
        panic!("expected snapshot to contain namespace prefix {b_prefix}");
    };

    // Sanity: the attached binary store is still based on the index root and does NOT know code B.
    let store = extract_binary_store_ref(&ledger2.binary_store).expect("downcast binary store");
    assert!(
        !store.namespace_codes().contains_key(&b_code),
        "expected binary store to be missing newly introduced namespace code; code={b_code}"
    );

    // Regression: a query with bound IRIs in namespace B should return 1 row.
    let sparql = r"
        PREFIX b: <http://example.org/b/>
        SELECT ?name WHERE {
          b:thing2 b:name ?name .
        }
    ";
    let rows = support::query_sparql(&fluree, &ledger2, sparql)
        .await
        .unwrap()
        .to_jsonld_async(ledger2.as_graph_db_ref(0))
        .await
        .unwrap();

    assert_eq!(
        rows.as_array().map(std::vec::Vec::len).unwrap_or(0),
        1,
        "expected 1 row for ns B data"
    );
}

#[tokio::test]
async fn query_predicate_var_in_new_namespace_after_index_preserves_prefix() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ns-new-after-index-pvar:main";

    // Commit t=1: introduce namespace A and some data.
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [
            { "@id": "a:thing1", "a:val": "seed" }
        ]
    });
    let _r1 = fluree.insert(ledger0, &tx1).await.unwrap();

    // Build the binary index at t=1.
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");

    // Load a fresh LedgerState with an attached binary store.
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let ledger_indexed = handle.snapshot().await.to_ledger_state();
    assert!(
        ledger_indexed.binary_store.is_some(),
        "expected binary_store"
    );

    // Commit t=2: introduce NEW namespace B with a novel predicate.
    let tx2 = json!({
        "@context": { "b": "http://example.org/b/" },
        "@graph": [
            { "@id": "b:thing2", "b:name": "Thing 2" }
        ]
    });
    let ledger2 = fluree.insert(ledger_indexed, &tx2).await.unwrap().ledger;

    // Query with predicate variable, but FILTER by subject constant.
    // This exercises binary-scan cursor + overlay ops translation, not overlay-only fallback.
    let sparql = r"
        PREFIX b: <http://example.org/b/>
        SELECT ?p ?o WHERE {
          ?s ?p ?o .
          FILTER(?s = b:thing2)
        }
    ";
    let result = support::query_sparql(&fluree, &ledger2, sparql)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger2.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["b:name", "Thing 2"]]));
}

#[tokio::test]
async fn query_subject_and_predicate_vars_in_new_namespace_after_index_preserves_prefixes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ns-new-after-index-spvar:main";

    // Commit t=1: namespace A and one fact, then index.
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [
            { "@id": "a:thing1", "a:val": "seed" }
        ]
    });
    let _ = fluree.insert(ledger0, &tx1).await.unwrap();
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");

    // Load indexed ledger state, then commit t=2 with new namespace B.
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let ledger_indexed = handle.snapshot().await.to_ledger_state();
    let tx2 = json!({
        "@context": { "b": "http://example.org/b/" },
        "@graph": [
            { "@id": "b:thing2", "b:name": "Thing 2" }
        ]
    });
    let ledger2 = fluree.insert(ledger_indexed, &tx2).await.unwrap().ledger;

    // Fully-variable pattern + filter by literal to select the novelty row.
    let sparql = r#"
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?s ?p WHERE {
          ?s ?p ?o .
          FILTER(?o = "Thing 2")
        }
    "#;
    let result = support::query_sparql(&fluree, &ledger2, sparql)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&ledger2.snapshot).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!([["http://example.org/b/thing2", "http://example.org/b/name"]])
    );
}

/// Regression test for fluree/db-r#145:
/// After reindex + new namespace commit, a fresh load from the commit chain
/// must resolve post-index namespace codes correctly in JSON-LD output.
///
/// This is the RELOAD variant of the tests above — it uses `fluree.ledger()`
/// (fresh nameservice + commit chain load) rather than the in-memory cached
/// handle. The bug was that `RuntimeSmallDicts` were created un-seeded during
/// `load_with_store()` and passed to `BinaryRangeProvider` before being
/// reseeded, causing novelty predicate IDs to collide with persisted p_ids.
#[tokio::test]
async fn reload_after_new_namespace_commit_resolves_correctly() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ns-new-after-index-reload:main";

    // t=1: insert with namespace A
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [{ "@id": "a:thing1", "a:name": "hello" }]
    });
    let _r1 = fluree.insert(ledger0, &tx1).await.unwrap();

    // Reindex — namespace A is now in the binary index
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let l_indexed = handle.snapshot().await.to_ledger_state();

    // t=2: insert with NEW namespace B (not in binary index)
    let tx2 = json!({
        "@context": { "a": "http://example.org/a/", "b": "http://example.org/b/" },
        "@graph": [{ "@id": "a:thing1", "b:color": "blue" }]
    });
    let _r2 = fluree.insert(l_indexed, &tx2).await.unwrap();

    // Reload fresh from nameservice (bypasses cache — simulates cold start)
    let reloaded = fluree
        .ledger(ledger_id)
        .await
        .expect("reload from commit chain");

    let query = json!({
        "@context": { "a": "http://example.org/a/", "b": "http://example.org/b/" },
        "select": { "a:thing1": ["*"] }
    });
    let result = support::query_jsonld(&fluree, &reloaded, &query)
        .await
        .unwrap()
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .unwrap();

    let thing = &result[0];
    assert_eq!(
        thing["b:color"], "blue",
        "Post-index namespace data should resolve correctly after reload. Got: {thing}"
    );
    assert_eq!(thing["a:name"], "hello");
}

/// FileStorage variant of the reload test above.
/// Drops the entire Fluree instance and creates a fresh one from the same
/// directory — a true cold-start that goes through the full file-backed
/// nameservice lookup → commit chain walk → binary index load path.
#[tokio::test]
async fn reload_file_storage_after_new_namespace_commit_resolves_correctly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build file-backed Fluree");

    let ledger_id = "it/ns-new-after-index-file-reload:main";
    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();

    // t=1: insert with namespace A
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [{ "@id": "a:thing1", "a:name": "hello" }]
    });
    let _r1 = fluree.insert(ledger0, &tx1).await.unwrap();

    // Reindex — namespace A is now in the binary index on disk
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");
    let handle = fluree.ledger_cached(ledger_id).await.unwrap();
    let l_indexed = handle.snapshot().await.to_ledger_state();

    // t=2: insert with NEW namespace B (not in binary index)
    let tx2 = json!({
        "@context": { "a": "http://example.org/a/", "b": "http://example.org/b/" },
        "@graph": [{ "@id": "a:thing1", "b:color": "blue" }]
    });
    let _r2 = fluree.insert(l_indexed, &tx2).await.unwrap();

    // Drop everything — true cold start
    drop(handle);
    drop(fluree);

    // Fresh Fluree instance from the same directory
    let fluree2 = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("rebuild file-backed Fluree");
    let reloaded = fluree2
        .ledger(ledger_id)
        .await
        .expect("reload from file storage");

    let query = json!({
        "@context": { "a": "http://example.org/a/", "b": "http://example.org/b/" },
        "select": { "a:thing1": ["*"] }
    });
    let result = support::query_jsonld(&fluree2, &reloaded, &query)
        .await
        .unwrap()
        .to_jsonld_async(reloaded.as_graph_db_ref(0))
        .await
        .unwrap();

    let thing = &result[0];
    assert_eq!(
        thing["b:color"], "blue",
        "Post-index namespace data should resolve correctly after file storage reload. Got: {thing}"
    );
    assert_eq!(thing["a:name"], "hello");
}

fn extract_binary_store_ref(
    binary_store: &Option<TypeErasedStore>,
) -> Option<&fluree_db_binary_index::BinaryIndexStore> {
    let te = binary_store.as_ref()?;
    // `TypeErasedStore` stores the *inner* `BinaryIndexStore` as an `Arc<dyn Any>`.
    // We only need a shared reference for inspection.
    te.0.downcast_ref::<fluree_db_binary_index::BinaryIndexStore>()
}

#[tokio::test]
async fn cached_handle_query_after_new_namespace_commit_still_works() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/ns-new-after-index-cached:main";

    let ledger0 = fluree.create_ledger(ledger_id).await.unwrap();
    let tx1 = json!({
        "@context": { "a": "http://example.org/a/" },
        "@graph": [
            { "@id": "a:thing1", "a:val": "seed" }
        ]
    });
    let _ = fluree.insert(ledger0, &tx1).await.unwrap();

    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex at t=1");

    let handle = fluree.ledger_cached(ledger_id).await.unwrap();

    let tx2 = json!({
        "@context": { "b": "http://example.org/b/" },
        "@graph": [
            { "@id": "b:thing2", "b:name": "Thing 2" }
        ]
    });
    let _out = fluree.stage(&handle).insert(&tx2).execute().await.unwrap();

    // Re-read through the cached handle. This is the path that can retain a
    // stale binary store after commit-time namespace changes.
    let cached = handle.snapshot().await.to_ledger_state();
    let b_prefix = "http://example.org/b/";
    let Some((&b_code, _)) = cached
        .snapshot
        .namespaces()
        .iter()
        .find(|(_, p)| p.as_str() == b_prefix)
    else {
        panic!("expected cached snapshot to contain namespace prefix {b_prefix}");
    };
    let store = extract_binary_store_ref(&cached.binary_store).expect("cached binary store");
    assert!(
        store.namespace_codes().contains_key(&b_code),
        "cached binary store should include newly introduced namespace code {b_code}"
    );

    let sparql = r#"
        PREFIX b: <http://example.org/b/>
        SELECT ?s ?name WHERE {
          ?s ?p ?o .
          ?s b:name ?name .
          FILTER(?o = "Thing 2")
        }
    "#;
    let result = support::query_sparql(&fluree, &cached, sparql)
        .await
        .unwrap();
    let jsonld = result.to_jsonld(&cached.snapshot).expect("to_jsonld");

    assert_eq!(jsonld, json!([["b:thing2", "Thing 2"]]));
}
