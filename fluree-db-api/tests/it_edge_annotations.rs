//! Edge annotations — broader M1b integration tests.
//!
//! Pins the user-facing contracts that the M1 design doc commits to:
//!
//! - **Parallel annotations** on the same `(s, p, o)` edge produce one
//!   row per annotation occurrence — Cypher fidelity.
//! - **Multiplicity contract**: a bare `?s ?p ?o` triple pattern
//!   returns a single row regardless of how many annotations are
//!   attached to that edge.
//! - **Annotation-rooted lookup** (`@reifies`) finds the edge from
//!   metadata; the base-edge triple in the expansion gives the
//!   visibility check for free.
//! - **Named-graph round-trip**: an annotated edge in a named graph
//!   stays paired with its annotation across query boundaries —
//!   regression coverage for the M1a `f:reifiesGraph` fix.
//!
//! See `EDGE_ANNOTATIONS.md` for the surface contract and
//! `EDGE_ANNOTATIONS_IMPL_PLAN.md` for the milestone split.
//!
//! Tests deliberately scope themselves to single-graph queries (or
//! `Pattern::Graph`-wrapped patterns) to stay within the correctness
//! envelope of the M1b expansion. The cross-graph misjoin gap is
//! tracked in the plan and lands with the M2 custom-operator path.

mod support;

use std::sync::Arc;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Match a row column against the compact or expanded form of an IRI,
/// in either bare-string or `{"@id": "..."}` shape. Mirrors the helper
/// in `it_edge_annotations_parse.rs` so the broader tests stay robust
/// to formatter changes.
fn iri_matches(value: &JsonValue, compact: &str, expanded: &str) -> bool {
    [compact, expanded].iter().any(|expect| {
        value.as_str() == Some(*expect)
            || value.get("@id").and_then(|v| v.as_str()) == Some(*expect)
    })
}

async fn seed_single_annotation(ledger_id: &str) -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed insert");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn parallel_annotations_on_one_edge_return_one_row_per_occurrence() {
    // Cypher fidelity: two distinct annotations on the same (s, p, o)
    // edge must produce two rows under the inline-form query, with
    // the role binding distinguishing them. This is the multiplicity
    // contract for `Pattern::EdgeAnnotation`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:parallel";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Insert two parallel annotations on the same edge. Each annotation
    // has an explicit @id so they can be told apart on retract paths
    // later (and so neither is anonymous).
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/2020",
                        "ex:role": "Engineer"
                    }
                }
            },
            {
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/2024",
                        "ex:role": "Manager"
                    }
                }
            }
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?role"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("parallel-annotation query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        2,
        "two parallel annotations must produce two rows, got: {arr:#?}"
    );

    // Pull out the ?role bindings (each row is a single-column tuple
    // because select is `["?role"]`).
    let roles: std::collections::BTreeSet<String> = arr
        .iter()
        .filter_map(|row| row.as_array())
        .filter_map(|cols| cols.first().and_then(|v| v.as_str()))
        .map(String::from)
        .collect();
    assert_eq!(
        roles,
        ["Engineer", "Manager"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    );
}

#[tokio::test]
async fn bare_triple_pattern_returns_one_row_per_edge_regardless_of_annotations() {
    // Multiplicity contract: the `Pattern::Triple(?s, ex:worksFor, ?o)`
    // surface returns one row per *edge*, even when multiple
    // annotations exist for that edge. Annotations only affect
    // cardinality through the `@annotation` / `@reifies` IR variants.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:bare-multiplicity";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Two annotations on the same edge.
    let txn = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
            }},
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
            }}
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    // Bare triple pattern — no @annotation block, no `@reifies`.
    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("bare triple query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        1,
        "bare triple cardinality must be 1 per edge regardless of annotations, got: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row tuple");
    assert!(iri_matches(&row[0], "ex:alice", "http://example.org/alice"));
    assert!(iri_matches(&row[1], "ex:acme", "http://example.org/acme"));
}

#[tokio::test]
async fn select_distinct_collapses_parallel_annotations_when_projecting_edge_only() {
    // Even when the WHERE clause uses `@annotation` (which produces
    // per-occurrence cardinality), `selectDistinct` over edge-only
    // projection columns (?person, ?org) collapses to one row.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:select-distinct";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@graph": [
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
            }},
            { "@id": "ex:alice", "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
            }}
        ]
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "selectDistinct": ["?person", "?org"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "ex:role": "?role" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("selectDistinct query");
    let arr = rows.as_array().expect("Select array");
    assert_eq!(
        arr.len(),
        1,
        "selectDistinct over (?person, ?org) collapses parallel annotations: {arr:#?}"
    );
}

#[tokio::test]
async fn annotation_rooted_query_finds_matching_edge() {
    // `@reifies`: filter by annotation metadata, return the edge it
    // reifies. Smoke test — broader visibility-check coverage lives
    // in `it_edge_annotations_parse.rs`.
    let (fluree, ledger) = seed_single_annotation("it/edge-annotations:reifies-roundtrip").await;

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Engineer",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("@reifies query");
    let arr = rows.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(&row[0], "ex:alice", "http://example.org/alice"));
    assert!(iri_matches(&row[1], "ex:acme", "http://example.org/acme"));
}

#[tokio::test]
async fn annotation_rooted_query_returns_no_rows_when_metadata_doesnt_match() {
    // Negative case: filtering by a role that no annotation carries
    // must produce zero rows. Pins that the body patterns actually
    // join — a bug here would mean the f:reifies* lookup is short-
    // circuiting before reading metadata.
    let (fluree, ledger) = seed_single_annotation("it/edge-annotations:reifies-no-match").await;

    let query = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Salesperson",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });

    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("@reifies negative query");
    let arr = rows.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "expected zero rows when the role doesn't match, got: {arr:#?}"
    );
}

#[tokio::test]
async fn retracting_base_edge_cascades_f_reifies_bundle() {
    // M1b cascade: when a base edge is retracted, the `f:reifies*`
    // bundle pointing at it must be retracted in the same
    // transaction so the durable encoding doesn't keep orphaned
    // attachment pointers.
    //
    // The naïve "post-delete @reifies returns zero rows" check is
    // ambiguous: the base-edge triple emitted by the M1b expansion
    // *also* drops the row when the edge isn't currently asserted,
    // so zero rows after delete tells us nothing about whether the
    // f:reifies* bundle was retracted or merely orphaned.
    //
    // The discriminating test: after the cascade-eligible delete,
    // re-insert *just* the base edge (no `@annotation` block). This
    // re-asserts the visibility-check edge but does not re-emit any
    // f:reifies* facts. So:
    //   - if cascade fired, the f:reifies* facts are retracted,
    //     re-inserting the edge doesn't bring them back, and
    //     `@reifies` returns zero rows.
    //   - if cascade didn't fire, the f:reifies* facts are still
    //     asserted from the original insert, the visibility check
    //     now passes, and `@reifies` returns the original
    //     annotation — proving the bundle was orphaned, not cleaned.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cascade-base-retract";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // 1. Insert an annotated edge.
    let insert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let after_insert = fluree
        .insert(ledger0, &insert)
        .await
        .expect("annotated insert");

    let q = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": {
            "ex:role": "Engineer",
            "@reifies": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
        }
    });

    // Sanity: the annotation is reachable via @reifies before delete.
    let pre = support::query_jsonld_formatted(&fluree, &after_insert.ledger, &q)
        .await
        .expect("pre-cascade query");
    assert_eq!(
        pre.as_array().expect("array").len(),
        1,
        "@reifies should find the annotation before cascade: {pre:#?}"
    );

    // 2. Retract the base edge via SPARQL-style update. The
    //    transactor's cascade pass should retract the corresponding
    //    `f:reifies*` bundle automatically.
    let delete = json!({
        "@context": ctx(),
        "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
        "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } }
    });
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("base-edge delete");

    // 3. Re-insert *only* the base edge. No `@annotation` block,
    //    so no f:reifies* assertions are emitted by the lowering.
    let reinsert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": { "@id": "ex:acme" }
    });
    let after_reinsert = fluree
        .insert(after_delete.ledger, &reinsert)
        .await
        .expect("plain re-insert");

    // 4. The base edge is now currently asserted again (visibility
    //    check passes), so any zero-row result must come from the
    //    f:reifies* facts being retracted — the cascade contract.
    let post = support::query_jsonld_formatted(&fluree, &after_reinsert.ledger, &q)
        .await
        .expect("post-cascade-and-reinsert query");
    let arr = post.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "after cascade + plain re-insert, @reifies must return zero rows \
         (proving the f:reifies* bundle was retracted, not just orphaned). got: {arr:#?}"
    );

    // Cross-check: a bare-triple query for the re-inserted edge
    // must return one row, confirming the visibility-check side of
    // the proof — the edge IS currently asserted, so zero rows
    // above isn't a visibility miss.
    let bare = json!({
        "@context": ctx(),
        "select": ["?person", "?org"],
        "where": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
    });
    let bare_rows = support::query_jsonld_formatted(&fluree, &after_reinsert.ledger, &bare)
        .await
        .expect("bare triple query after re-insert");
    assert_eq!(
        bare_rows.as_array().expect("array").len(),
        1,
        "the re-inserted base edge must be currently asserted (cross-check)"
    );
}

#[tokio::test]
async fn subject_expansion_emits_annotation_block_for_annotated_edge() {
    // M1b round-trip: when subject expansion materializes a base edge
    // that has an annotation attached, the rendered value must carry
    // an `@annotation` key whose body is the annotation's
    // user-property view (with `f:reifies*` filtered out, which the
    // wildcard-hydration filter already handles).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:expand-annotation";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    // Wildcard hydrate the base subject. The `ex:worksFor` value
    // should expand to a node-map carrying both `@id` (the org) and
    // an `@annotation` key with the annotation's body.
    let query = json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("annotated subject expansion");
    let arr = rows.as_array().expect("array");
    assert_eq!(arr.len(), 1, "single subject row, got: {arr:#?}");

    let person = arr[0].as_object().expect("hydrated person object");
    let works_for = person
        .get("ex:worksFor")
        .or_else(|| person.get("http://example.org/worksFor"))
        .expect("ex:worksFor must be present");

    // The value is a single object (one annotation, one edge) — pull
    // it out regardless of single-vs-array shape.
    let edge_obj = works_for
        .as_object()
        .or_else(|| works_for.as_array().and_then(|a| a.first().and_then(|v| v.as_object())))
        .expect("worksFor value must be a node object: {works_for:#?}");

    assert!(
        iri_matches(
            edge_obj.get("@id").unwrap_or(&JsonValue::Null),
            "ex:acme",
            "http://example.org/acme",
        ),
        "edge object @id should be ex:acme, got: {edge_obj:#?}"
    );

    let ann = edge_obj
        .get("@annotation")
        .expect("@annotation key must be injected for annotated edge");
    let ann_obj = ann
        .as_object()
        .or_else(|| ann.as_array().and_then(|a| a.first().and_then(|v| v.as_object())))
        .expect("@annotation value must be an object or single-element array");

    assert_eq!(
        ann_obj
            .get("ex:role")
            .or_else(|| ann_obj.get("http://example.org/role"))
            .and_then(|v| v.as_str()),
        Some("Engineer"),
        "annotation body must surface ex:role: {ann_obj:#?}"
    );
    // System facts must still be filtered.
    for k in ann_obj.keys() {
        assert!(
            !k.starts_with("https://ns.flur.ee/db#reifies"),
            "f:reifies* must not leak into @annotation body: {k}"
        );
    }
}

#[tokio::test]
#[cfg(feature = "native")]
async fn cascade_fires_for_indexed_annotation_when_edge_is_retracted() {
    // M2 indexed-readpath proof for the cascade path: insert an
    // annotated edge, force a reindex (drains novelty into base),
    // retract the base edge, and verify the f:reifies* bundle is
    // also retracted. With the M1b novelty-only cascade this would
    // fail post-index (the overlay's `attachments` map is empty
    // after novelty drains).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations:cascade-after-reindex";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);

            let txn = json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer"
                    }
                }
            });
            let after_insert = fluree.insert(ledger0, &txn).await.expect("insert");

            // Drain novelty into base via reindex.
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t)
                .await;
            let reloaded = fluree.ledger(ledger_id).await.expect("reload");

            // Retract the base edge. With the M1b novelty-only
            // cascade this would not fire because
            // `ledger.novelty.attachments` is empty post-index.
            let delete = json!({
                "@context": ctx(),
                "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
                "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } }
            });
            let after_delete = fluree.update(reloaded, &delete).await.expect("delete");

            // Re-insert just the base edge (no @annotation block)
            // and verify the discriminating cascade test:
            //   - if cascade fired, @reifies returns 0 rows.
            //   - if it didn't, the still-indexed f:reifies* facts
            //     would join with the re-inserted base edge and
            //     surface the original annotation.
            let reinsert = json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            });
            let after_reinsert = fluree
                .insert(after_delete.ledger, &reinsert)
                .await
                .expect("plain re-insert");

            let q = json!({
                "@context": ctx(),
                "select": ["?person", "?org"],
                "where": {
                    "ex:role": "Engineer",
                    "@reifies": { "@id": "?person", "ex:worksFor": { "@id": "?org" } }
                }
            });
            let post = support::query_jsonld_formatted(&fluree, &after_reinsert.ledger, &q)
                .await
                .expect("post-cascade-and-reinsert query");
            let arr = post.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "cascade must fire for indexed annotations on base-edge retract; \
                 got rows: {arr:#?}"
            );
        })
        .await;
}

#[tokio::test]
#[cfg(feature = "native")]
async fn subject_expansion_finds_annotation_after_reindex() {
    // M2 indexed-readpath proof: insert an annotated edge, force a
    // full reindex (which drains novelty into base storage), reload
    // the ledger so the snapshot is fresh, and verify the hydrator
    // still emits `@annotation` blocks. With the M1b novelty-only
    // approach this test would have failed (downcast finds an empty
    // overlay post-index); the M2 scan-based lookup goes through
    // `db.range` which reads novelty + base, so it finds the
    // f:reifies* facts in their indexed location.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations:indexed-readpath";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);

            // Insert an annotated edge. The lowering produces
            // f:reifies* flakes that land in the novelty overlay.
            let txn = json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer"
                    }
                }
            });
            let after_insert = fluree
                .insert(ledger0, &txn)
                .await
                .expect("annotated insert");

            // Force a full reindex so the f:reifies* flakes roll
            // from novelty into base storage.
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t)
                .await;

            // Reload the ledger to pick up the post-index snapshot.
            let reloaded = fluree
                .ledger(ledger_id)
                .await
                .expect("reload ledger after index");

            // Wildcard-hydrate the base subject. The expansion
            // should reach the indexed f:reifies* facts via
            // `db.range` and surface them under `@annotation`.
            let query = json!({
                "@context": ctx(),
                "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
                "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
            });
            let rows = support::query_jsonld_formatted(&fluree, &reloaded, &query)
                .await
                .expect("post-reindex annotated subject expansion");
            let arr = rows.as_array().expect("array");
            assert_eq!(arr.len(), 1, "single subject row, got: {arr:#?}");

            let person = arr[0].as_object().expect("person object");
            let works_for = person
                .get("ex:worksFor")
                .or_else(|| person.get("http://example.org/worksFor"))
                .expect("ex:worksFor must be present after reindex");
            let edge_obj = works_for
                .as_object()
                .or_else(|| {
                    works_for
                        .as_array()
                        .and_then(|a| a.first().and_then(|v| v.as_object()))
                })
                .expect("worksFor value must be a node object");
            let ann = edge_obj
                .get("@annotation")
                .expect("@annotation must survive reindex (M2 scan-based lookup)");
            let ann_obj = ann
                .as_object()
                .or_else(|| {
                    ann.as_array()
                        .and_then(|a| a.first().and_then(|v| v.as_object()))
                })
                .expect("@annotation body must be an object");
            assert_eq!(
                ann_obj
                    .get("ex:role")
                    .or_else(|| ann_obj.get("http://example.org/role"))
                    .and_then(|v| v.as_str()),
                Some("Engineer"),
                "annotation body must surface ex:role after reindex: {ann_obj:#?}"
            );
        })
        .await;
}

#[tokio::test]
async fn subject_expansion_emits_no_annotation_when_edge_has_none() {
    // Negative case: a plain (un-annotated) edge must not carry an
    // `@annotation` key in its expanded form. Pins the
    // has_annotations gate / empty-iter path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:expand-no-annotation";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:bob",
        "ex:worksFor": {"@id": "ex:acme"}
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("plain insert");

    let query = json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("plain subject expansion");
    let arr = rows.as_array().expect("array");
    let person = arr[0].as_object().unwrap();
    let works_for = person
        .get("ex:worksFor")
        .or_else(|| person.get("http://example.org/worksFor"))
        .unwrap();
    let edge_obj = works_for
        .as_object()
        .or_else(|| works_for.as_array().and_then(|a| a.first().and_then(|v| v.as_object())))
        .unwrap();
    assert!(
        edge_obj.get("@annotation").is_none(),
        "plain edge must not carry @annotation: {edge_obj:#?}"
    );
}

#[tokio::test]
async fn cascade_cleans_up_anonymous_annotation_metadata() {
    // RDF-mode cleanup contract: when the cascade retracts the
    // `f:reifies*` bundle for an anonymous (blank-node) annotation,
    // it must also retract the annotation's body metadata. Without
    // this, the body flakes (`_:fluree_ann_0 ex:role "Engineer"`)
    // remain in the graph as orphaned RDF — unreachable through
    // `@reifies` (the bundle is gone) but still discoverable via
    // a `?s ex:role "Engineer"` scan.
    //
    // Explicit-IRI annotations are deliberately NOT cleaned up in
    // RDF mode — they're user-addressable subjects that may have
    // independent meaning. The opt-in `lpgEdgeLifecycle` flag
    // would extend cleanup to those; not in scope here.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cascade-anonymous-metadata";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Insert with an *anonymous* annotation (no @id on the
    // annotation block — the lowering mints a blank-node SID).
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let after_insert = fluree.insert(ledger0, &txn).await.expect("insert");

    // Sanity: the role is queryable before the cascade.
    let q_role = json!({
        "@context": ctx(),
        "select": ["?role"],
        "where": { "ex:role": "?role" }
    });
    let pre = support::query_jsonld_formatted(&fluree, &after_insert.ledger, &q_role)
        .await
        .expect("pre-cascade role query");
    assert_eq!(
        pre.as_array().expect("array").len(),
        1,
        "ex:role should be present before cascade: {pre:#?}"
    );

    // Retract the base edge — cascade fires.
    let delete = json!({
        "@context": ctx(),
        "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
        "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } }
    });
    let after_delete = fluree.update(after_insert.ledger, &delete).await.expect("delete");

    // After the cascade, no row should match `?s ex:role ?role` —
    // the anonymous annotation's body metadata is gone too.
    let post = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q_role)
        .await
        .expect("post-cascade role query");
    let arr = post.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "anonymous annotation's metadata must be cleaned up by RDF-mode cascade; \
         got: {arr:#?}"
    );
}

#[tokio::test]
async fn cascade_keeps_explicit_iri_annotation_metadata() {
    // Counterpart: explicit-IRI annotation subjects are NOT
    // cleaned up by the default RDF-mode cascade. The bundle
    // disappears (so `@reifies` returns nothing) but the body
    // metadata stays queryable as ordinary RDF on the named
    // subject.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cascade-explicit-keeps-metadata";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let after_insert = fluree.insert(ledger0, &txn).await.expect("insert");

    let delete = json!({
        "@context": ctx(),
        "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
        "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } }
    });
    let after_delete = fluree.update(after_insert.ledger, &delete).await.expect("delete");

    // The explicit-IRI annotation's role is still queryable.
    let q = json!({
        "@context": ctx(),
        "select": ["?ann", "?role"],
        "where": { "@id": "?ann", "ex:role": "?role" }
    });
    let rows = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q)
        .await
        .expect("post-cascade explicit-IRI role query");
    let arr = rows.as_array().expect("array");
    assert_eq!(
        arr.len(),
        1,
        "explicit-IRI annotation's metadata must survive RDF-mode cascade: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(
        &row[0],
        "ex:emp/alice-acme",
        "http://example.org/emp/alice-acme",
    ));
    assert_eq!(row[1].as_str(), Some("Engineer"));
}

#[tokio::test]
async fn variable_predicate_scan_hides_f_reifies() {
    // A triple pattern with a variable predicate (`?s ?p ?o`) used
    // to surface `f:reifies*` system flakes from the annotation
    // subject's overlay rows. The scan-layer filter in
    // `flakes_to_bindings` skips Fluree-system-namespace predicates
    // when the user's predicate slot is a variable, mirroring the
    // existing filter on the binary-cursor path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:variable-predicate-no-leak";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    // Bind ?p to every predicate the annotation subject carries.
    let query = json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": { "@id": "ex:emp/alice-acme", "?p": "?o" }
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("variable-predicate query");
    let arr = rows.as_array().expect("array");

    // Collect the predicate bindings as strings.
    let predicates: Vec<String> = arr
        .iter()
        .filter_map(|row| row.as_array())
        .filter_map(|cols| cols.first())
        .filter_map(|v| {
            v.as_str()
                .map(String::from)
                .or_else(|| v.get("@id").and_then(|i| i.as_str()).map(String::from))
        })
        .collect();

    // No `f:reifies*` predicate may leak.
    for p in &predicates {
        assert!(
            !p.starts_with("https://ns.flur.ee/db#reifies"),
            "f:reifies* must not leak through variable-predicate scan: {p} \
             (full bindings: {predicates:?})"
        );
        assert!(
            !p.starts_with("f:reifies"),
            "compact f:reifies* form must not leak: {p}"
        );
    }
    // The user-authored `ex:role` must still be visible.
    assert!(
        predicates.iter().any(|p| p == "http://example.org/role"
            || p == "ex:role"),
        "user-authored ex:role must be visible: {predicates:?}"
    );
}

#[tokio::test]
async fn wildcard_subject_hydration_hides_f_reifies_predicates() {
    // Annotation subjects minted by the M1a transactor lowering carry
    // `f:reifies*` system facts in addition to the user-authored body
    // properties. Wildcard subject hydration (`select: {"?s": ["*"]}`)
    // expands all properties of a subject, which would otherwise leak
    // these system facts to the user.
    //
    // The hydration-layer filter in `format/hydration.rs` skips any
    // predicate where `is_reserved_reifies_predicate(&p)` returns
    // true. This test pins that contract: the wildcard projection
    // sees the user's `ex:role` but not any `f:reifies*` predicate.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:wildcard-hides-reifies";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": {"?ann": ["*"]},
        "where": { "@id": "?ann", "ex:role": "Engineer" }
    });

    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("wildcard hydration over annotation subject");
    let arr = rows.as_array().expect("array");
    assert!(
        !arr.is_empty(),
        "wildcard hydration should find the annotation subject"
    );

    // The user's `ex:role` is visible.
    let node = arr[0]
        .as_object()
        .expect("hydrated node should be an object");
    let role_visible = node
        .get("ex:role")
        .or_else(|| node.get("http://example.org/role"))
        .is_some();
    assert!(
        role_visible,
        "user-authored ex:role must remain visible under wildcard hydration: {node:#?}"
    );

    // No `f:reifies*` predicate may appear under any namespace form
    // (full IRI or compact alias). The hydration formatter compacts
    // through the query's `@context`, but we don't declare an `f:`
    // alias in our test ctx, so any leak would surface as the
    // expanded IRI.
    for key in node.keys() {
        assert!(
            !key.starts_with("https://ns.flur.ee/db#reifies"),
            "f:reifies* predicate '{key}' must not leak through wildcard hydration"
        );
        assert!(
            !key.starts_with("f:reifies"),
            "compact f:reifies* form '{key}' must not leak"
        );
    }
}

#[tokio::test]
async fn cascade_retracts_named_graph_annotations_in_their_own_graph() {
    // Regression: cascade retract bundles must carry the same
    // `g = Some(graph_sid)` as the original named-graph assertion.
    // A default-graph retract would not match named-graph
    // assertions in Fluree's flake identity model, leaving the
    // annotation orphaned in the named graph.
    //
    // We can't directly inspect the flake graph from the public
    // API, but we *can* observe the retract via the
    // `AttachmentNovelty` overlay: if the cascade emitted retracts
    // in the named graph, the overlay's observer would record
    // them, and `current_annotations_for_at` would return zero
    // for the edge after the retract. If the retracts went to the
    // default graph, the named-graph assertion would still be
    // active in the overlay.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cascade-named-graph";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@graph": "ex:hr-graph",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    });
    let after_insert = fluree
        .insert(ledger0, &txn)
        .await
        .expect("named-graph annotated insert");

    // Direct retract — no WHERE binding needed since the IRIs are
    // explicit. The named-graph selector tells the transactor to
    // emit the retract flake in the named graph.
    let insert_t = after_insert.ledger.t();
    let delete = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:alice",
            "@graph": "ex:hr-graph",
            "ex:worksFor": { "@id": "ex:acme" }
        }
    });
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("named-graph base delete");
    assert!(
        after_delete.ledger.t() > insert_t,
        "delete must produce a new t (precondition for cascade test)"
    );

    // Re-insert just the base edge in the same named graph.
    let reinsert = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@graph": "ex:hr-graph",
        "ex:worksFor": { "@id": "ex:acme" }
    });
    let after_reinsert = fluree
        .insert(after_delete.ledger, &reinsert)
        .await
        .expect("named-graph base re-insert");

    // After the cascade, the AttachmentNovelty observer should
    // have recorded both the named-graph assertion AND a matching
    // named-graph retract. With the named-graph fix, both events
    // share the same `EdgeKey { g: Some(graph_a), ... }` so the
    // forward map's latest event for that key is a retract (op=false).
    //
    // Without the fix, the assertion is keyed by `g=Some(graph_a)`
    // but the retract would be keyed by `g=None` (different
    // EdgeKey), so the named-graph forward rows would still show
    // the annotation as currently asserted.
    //
    // We don't reconstruct the EdgeKey directly — we walk the
    // forward map and assert that *no* named-graph edge has any
    // currently-attached annotation.
    let attachments = &after_reinsert.ledger.novelty.attachments;
    let as_of = after_reinsert.ledger.t();
    let mut leaked_named_graph_attachments: Vec<String> = Vec::new();
    for (edge_key, _rows) in attachments.iter_forward() {
        if edge_key.g.is_none() {
            continue; // default-graph edge — not what this test guards
        }
        let live: Vec<fluree_db_core::Sid> = attachments
            .current_annotations_for_at(edge_key, as_of)
            .collect();
        if !live.is_empty() {
            leaked_named_graph_attachments
                .push(format!("{edge_key:?} -> {live:?}"));
        }
    }
    assert!(
        leaked_named_graph_attachments.is_empty(),
        "after named-graph cascade, no named-graph edge should have currently-attached \
         annotations; got: {leaked_named_graph_attachments:#?}"
    );
}

#[tokio::test]
async fn annotation_in_named_graph_insert_succeeds() {
    // Regression coverage for the M1a `f:reifiesGraph` fix on the
    // *write* path. An annotated edge in a named graph must be
    // accepted by the transactor, with the lowering emitting
    // `f:reifiesGraph` on the synthetic annotation sibling and
    // pinning the sibling's own `@graph` to the same named graph.
    //
    // Full round-trip query coverage for named graphs needs the
    // `from` / `fromNamed` dataset wiring (see `it_named_graphs.rs`)
    // and pairs naturally with the cross-graph custom operator
    // tracked in the M1b plan TODO list. This test scopes itself to
    // the lowering-side guarantee.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:named-graph-insert";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "@graph": "ex:hr-graph",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    fluree
        .insert(ledger0, &txn)
        .await
        .expect("M1a fix: named-graph annotation insert must succeed end-to-end");
}
