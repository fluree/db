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
//! One pre-existing concern surfaced (but not introduced) by the corruption
//! fix is pinned here as a `#[ignore]`'d test with an inline `TODO(...)`
//! block above its attribute that documents the root cause and remediation
//! options:
//!
//! - `jsonld_context_vector_bare_array_retracts_after_indexing` —
//!   SPARQL DELETE WHERE on an indexed vector flake doesn't cancel the
//!   assertion because the retraction allocates a fresh vector arena
//!   handle, so index-merge cancellation (which keys on `o_kind/o_key`)
//!   never pairs them. See `TODO(vector-retraction)`.

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

// TODO(vector-retraction): when SPARQL DELETE WHERE matches an indexed
// vector flake, materialize_template generates a correct retraction
// (op=false, dt=embeddingVector, FlakeValue::Vector with the matched values)
// and the transaction commits. But the index-merge cancellation pairs
// assertions and retractions by `(s, p, dt, o_kind, o_key)`, and the
// retraction's vector goes through `VectorArena::insert_f32` which
// allocates a fresh arena slot — so the retraction's `o_key` (handle) is
// different from the original assertion's, the pair never matches, and
// COUNT after delete still returns 1.
//
// Two viable remediations (both deferred — out of scope for the JSON-LD
// corruption fix that landed alongside this test):
//   1. At retraction time, look up the existing arena handle for the
//      matched (s, p, value) and reuse it on the retraction flake. Tighter
//      blast radius; needs an arena→handle reverse-lookup index.
//   2. For `ObjKind::VECTOR_ID` (and any future arena-handle kind), do
//      cancellation by decoded value rather than by `o_key`. More general
//      but slower at merge time.
//
// The fix that this test pins (single well-formed FlakeValue::Vector flake
// with `dt = embeddingVector`) is the prerequisite for either remediation —
// before it, the corrupt scalar-as-vector flakes couldn't even be matched
// for retraction.
#[tokio::test]
#[ignore = "vector retraction is a separate latent issue surfaced by the \
            JSON-LD corruption fix; see TODO(vector-retraction) above for \
            the root cause and remediation options."]
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
