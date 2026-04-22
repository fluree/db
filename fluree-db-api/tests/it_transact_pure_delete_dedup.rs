//! Pure-DELETE staging regression tests.
//!
//! These tests pin the behavior of two staging optimizations introduced together:
//!
//! 1. **Pure-DELETE fast path** — when a transaction has no INSERT templates and
//!    is not an Upsert, staging skips assertion generation and the
//!    assertion/retraction `apply_cancellation` hashmap, and instead runs
//!    `dedup_retractions` (sort + `Vec::dedup`) over the retractions.
//!
//! 2. **Template-var projection** — the WHERE-result `Batch` is projected down
//!    to only the variables actually referenced by INSERT/DELETE templates
//!    *before* encoded-binding materialization. Helper vars bound by WHERE that
//!    aren't read by any template are dropped.
//!
//! The risk being mitigated: a SPARQL `DELETE WHERE` that joins through a
//! one-to-many predicate (e.g. multiple types per subject) produces duplicate
//! solution rows for the same retraction. Both the old hashmap-based
//! cancellation and the new sort+dedup path must collapse those duplicates so
//! that the resulting commit contains one retraction per unique fact.

#![cfg(feature = "native")]

mod support;

use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "schema": "http://schema.org/",
        "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    })
}

async fn count_subjects_with_predicate(
    fluree: &fluree_db_api::Fluree,
    ledger: &fluree_db_api::LedgerState,
    subject: &str,
    predicate: &str,
) -> usize {
    let q = json!({
        "@context": ctx(),
        "select": ["?o"],
        "where": { "@id": subject, predicate: "?o" }
    });
    let result = support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query");
    result
        .to_jsonld_async(ledger.as_graph_db_ref(0))
        .await
        .expect("to_jsonld_async")
        .as_array()
        .map(std::vec::Vec::len)
        .unwrap_or(0)
}

/// Pure DELETE WHERE where the WHERE clause produces multiple solution rows for
/// the same retraction (subject has several types, so the type-join multiplies
/// rows). The pure-delete fast path must dedup those duplicate retractions so
/// the commit succeeds and the targeted facts are gone.
#[tokio::test]
async fn pure_delete_with_multiplying_type_join_dedups_retractions() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/pure-delete-dedup-multiplying-join:main")
        .await
        .expect("create");

    // Insert one subject with three rdf:type values and a single :name. The
    // WHERE pattern below joins on type, so binding ?name once produces three
    // solution rows — and three identical retractions in the absence of dedup.
    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@type": ["ex:Person", "ex:Employee", "ex:Manager"],
        "ex:name": "Alice"
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:alice", "ex:name").await,
        1,
        "precondition: alice has one ex:name"
    );

    // Pure-DELETE: no `insert` key, so insert_templates is empty and the
    // staging fast path runs `dedup_retractions` over the retractions.
    let delete_txn = json!({
        "@context": ctx(),
        "where": [
            { "@id": "?s", "@type": "?t" },
            { "@id": "?s", "ex:name": "?name" }
        ],
        "delete": { "@id": "?s", "ex:name": "?name" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("pure-delete update");

    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "ex:name").await,
        0,
        "ex:name should be retracted after pure-delete update"
    );

    // Types are not in the DELETE template, so they must remain.
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "@type").await,
        3,
        "rdf:type triples must remain — only ex:name was deleted"
    );
}

/// Pure DELETE where WHERE binds extra helper variables that no template
/// references. The template-var projection must drop those columns before
/// materialization — but it must NOT drop columns the DELETE template needs.
///
/// If projection were buggy and dropped ?name, the DELETE template would fail
/// to resolve ?name (`Binding::Unbound`) and silently produce zero retractions,
/// leaving the data intact. This test would then fail.
#[tokio::test]
async fn pure_delete_template_var_projection_keeps_referenced_vars() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/pure-delete-projection:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:dept": "Sales", "ex:name": "Alice" },
            { "@id": "ex:bob",   "ex:dept": "Sales", "ex:name": "Bob"   },
            { "@id": "ex:carol", "ex:dept": "Eng",   "ex:name": "Carol" }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:alice", "ex:name").await,
        1
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:bob", "ex:name").await,
        1
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:carol", "ex:name").await,
        1
    );

    // WHERE binds three vars: ?s, ?name, ?dept. The DELETE template only
    // references ?s and ?name — ?dept is a helper used to scope the match.
    // After template-var projection, the WHERE batch should hold only ?s and
    // ?name when entering flake generation.
    let delete_txn = json!({
        "@context": ctx(),
        "where": [
            { "@id": "?s", "ex:dept": "?dept" },
            { "@id": "?s", "ex:name": "?name" }
        ],
        "delete": { "@id": "?s", "ex:name": "?name" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("pure-delete with helper var");

    // All three names should be deleted; if ?name had been projected away,
    // these would still be present.
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "ex:name").await,
        0
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:bob", "ex:name").await,
        0
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:carol", "ex:name").await,
        0
    );

    // Departments (the helper var's predicate) must be untouched.
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "ex:dept").await,
        1
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:bob", "ex:dept").await,
        1
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:carol", "ex:dept").await,
        1
    );
}

/// Pure DELETE with an all-literal template (no variables) over a multi-row
/// WHERE. Template-var projection produces an empty schema with the original
/// row count, so the literal template fires once per row, generating
/// duplicate retractions that `dedup_retractions` must collapse to one.
#[tokio::test]
async fn pure_delete_all_literal_template_over_multirow_where_dedups() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/pure-delete-all-literal:main")
        .await
        .expect("create");

    // Three Person subjects, plus one fixed status triple to be retracted.
    let insert = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice",  "@type": "ex:Person" },
            { "@id": "ex:bob",    "@type": "ex:Person" },
            { "@id": "ex:carol",  "@type": "ex:Person" },
            { "@id": "ex:system", "ex:status": "active" }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:system", "ex:status").await,
        1,
        "precondition: ex:system has status"
    );

    // DELETE template has zero variables; WHERE returns three rows (one per
    // Person). After projection the WHERE schema is empty but its row count
    // is 3, so the literal template fires three times -> three identical
    // retractions -> dedup to one.
    let delete_txn = json!({
        "@context": ctx(),
        "where":  { "@id": "?s", "@type": "ex:Person" },
        "delete": { "@id": "ex:system", "ex:status": "active" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("pure-delete all-literal");

    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:system", "ex:status").await,
        0,
        "ex:status should be retracted exactly once after dedup"
    );

    // Person subjects untouched.
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "@type").await,
        1
    );
}

/// All-literal pure DELETE over a multi-row WHERE *with the binary index
/// engaged*. Same shape as `pure_delete_all_literal_template_over_multirow_where_dedups`,
/// but a `reindex` is forced first so that:
///
/// 1. `ledger.binary_store` is `Some(_)`, which means
///    `materialize_encoded_bindings_for_txn` runs its in-place rewrite path
///    instead of the no-binary-store early return.
/// 2. The WHERE result may contain `Encoded*` bindings that exercise the
///    materialization step.
///
/// The historically-broken shape was: `project_owned(&[])` produces an
/// `empty_schema_with_len(N)` batch; the materialize step round-trips it
/// through `into_parts` -> `Batch::new(schema, columns)`, and `Batch::new`
/// infers `len = 0` when `columns` is empty. That silently turns "fire once
/// per WHERE solution" into "fire once total" — currently masked by dedup,
/// but still a real semantic regression. This test pins the post-fix
/// behavior end-to-end.
#[tokio::test]
async fn pure_delete_all_literal_template_post_reindex_engages_binary_store() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().to_str().expect("path");

    let fluree = FlureeBuilder::file(path).build().expect("build");
    let ledger0 = fluree
        .create_ledger("tx/pure-delete-all-literal-indexed:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice",  "@type": "ex:Person" },
            { "@id": "ex:bob",    "@type": "ex:Person" },
            { "@id": "ex:carol",  "@type": "ex:Person" },
            { "@id": "ex:system", "ex:status": "active" }
        ]
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");

    // Force a reindex so subsequent WHERE execution reads from the binary
    // index — this is what makes `binary_store` Some and routes through the
    // materialization path that exercises into_parts/from_parts.
    let _ = fluree
        .reindex(
            "tx/pure-delete-all-literal-indexed:main",
            ReindexOptions::default(),
        )
        .await
        .expect("reindex");

    assert_eq!(
        count_subjects_with_predicate(&fluree, &receipt.ledger, "ex:system", "ex:status").await,
        1,
        "precondition: ex:system has status post-reindex"
    );

    let delete_txn = json!({
        "@context": ctx(),
        "where":  { "@id": "?s", "@type": "ex:Person" },
        "delete": { "@id": "ex:system", "ex:status": "active" }
    });
    let out = fluree
        .update(receipt.ledger, &delete_txn)
        .await
        .expect("post-reindex pure-delete all-literal");

    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:system", "ex:status").await,
        0,
        "ex:status should be retracted exactly once even with binary store engaged"
    );

    // Person types must remain.
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "@type").await,
        1
    );
}

/// Mixed DELETE + INSERT must NOT take the pure-delete fast path: it still
/// needs the full assertion/retraction `apply_cancellation` pass so that
/// retract+assert pairs of the same fact collapse to a no-op. This pins the
/// guard `pure_delete = insert_templates.is_empty() && txn_type != Upsert`.
#[tokio::test]
async fn mixed_delete_insert_still_cancels_identical_pairs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = fluree
        .create_ledger("tx/mixed-delete-insert-cancel:main")
        .await
        .expect("create");

    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:name": "Alice"
    });
    let receipt = fluree.insert(ledger0, &insert).await.expect("insert");
    let t_after_insert = receipt.ledger.t();

    // Delete and re-insert the same (s, p, o, dt) within a single transaction.
    // apply_cancellation must collapse the pair to a no-op — the ledger t
    // should not bump because no flakes survive cancellation.
    let no_op_update = json!({
        "@context": ctx(),
        "where":  { "@id": "ex:alice", "ex:name": "?name" },
        "delete": { "@id": "ex:alice", "ex:name": "?name" },
        "insert": { "@id": "ex:alice", "ex:name": "Alice" }
    });
    let out = fluree
        .update(receipt.ledger, &no_op_update)
        .await
        .expect("mixed delete+insert no-op");

    assert_eq!(
        out.ledger.t(),
        t_after_insert,
        "identical retract+assert in one txn must cancel — no new commit"
    );
    assert_eq!(
        count_subjects_with_predicate(&fluree, &out.ledger, "ex:alice", "ex:name").await,
        1,
        "ex:name must still be present after the canceled no-op"
    );
}
