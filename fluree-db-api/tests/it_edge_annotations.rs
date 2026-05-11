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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
            .map(std::string::ToString::to_string)
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
        .or_else(|| {
            works_for
                .as_array()
                .and_then(|a| a.first().and_then(|v| v.as_object()))
        })
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
        .or_else(|| {
            ann.as_array()
                .and_then(|a| a.first().and_then(|v| v.as_object()))
        })
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
async fn first_annotation_through_incremental_index_flips_has_annotations() {
    // Regression: when a ledger is first reindexed via the
    // incremental path AFTER receiving its first annotation,
    // `IndexRoot.has_annotations` must flip to `true`. Otherwise
    // post-reindex (when the novelty bit drains) the cascade
    // fast-path would skip the scan and leave dangling
    // `f:reifies*` flakes for any later retract.
    //
    // Test path:
    //   1. Insert plain (non-annotated) data, reindex → indexed
    //      root has `has_annotations = false`.
    //   2. Insert annotated edge → novelty bit fires.
    //   3. Reindex (incremental path clones the old root) →
    //      indexed root MUST flip `has_annotations` to `true`.
    //   4. Retract the base edge → the cascade gate must NOT
    //      short-circuit (it skips only when both snapshot and
    //      novelty annotation flags are false).
    //   5. Re-insert the base edge alone, query @reifies → zero
    //      rows (cascade actually fired and retracted the bundle).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations:incremental-flips-flag";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        Arc::new(fluree.nameservice_mode().clone()),
        fluree_db_indexer::IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);

            // Step 1: plain insert → indexed root with no annotations.
            let plain = json!({
                "@context": ctx(),
                "@id": "ex:bob",
                "ex:name": "Bob"
            });
            let after_plain = fluree.insert(ledger0, &plain).await.expect("plain insert");
            support::trigger_index_and_wait(&handle, ledger_id, after_plain.receipt.t).await;

            // Step 2: insert an annotated edge.
            let after_plain_reload = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after plain reindex");
            let annotated = json!({
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
            let after_annotated = fluree
                .insert(after_plain_reload, &annotated)
                .await
                .expect("annotated insert");

            // Step 3: reindex → incremental path clones the old
            // root and must OR-update `has_annotations` from the
            // newly-added `f:reifies*` predicates.
            support::trigger_index_and_wait(&handle, ledger_id, after_annotated.receipt.t).await;
            let post_reindex = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after annotated reindex");
            assert!(
                post_reindex.snapshot.has_annotations,
                "incremental indexing of the first annotation must flip \
                 IndexRoot.has_annotations to true (otherwise the cascade \
                 fast-path would skip post-reindex retracts)"
            );

            // Step 4: retract the base edge. The cascade gate sees
            // `snapshot.has_annotations = true` so the scan runs.
            let delete = json!({
                "@context": ctx(),
                "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
                "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } }
            });
            let after_delete = fluree
                .update(post_reindex, &delete)
                .await
                .expect("base-edge delete");

            // Step 5: re-insert just the base edge, query @reifies
            // — must return zero rows (proving cascade actually ran
            // post-reindex).
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
                    "@reifies": {
                        "@id": "?person",
                        "ex:worksFor": { "@id": "?org" }
                    }
                }
            });
            let post = support::query_jsonld_formatted(&fluree, &after_reinsert.ledger, &q)
                .await
                .expect("post-cascade @reifies query");
            let arr = post.as_array().expect("array");
            assert!(
                arr.is_empty(),
                "post-incremental-reindex retract must cascade (proving the \
                 sticky bit flipped on incremental rebuild): {arr:#?}"
            );
        })
        .await;
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
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
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
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;

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
        .or_else(|| {
            works_for
                .as_array()
                .and_then(|a| a.first().and_then(|v| v.as_object()))
        })
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
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("delete");

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
async fn retracting_all_annotation_metadata_cleans_bundle_too() {
    // When a user retracts every asserted user-property flake of
    // an annotation subject in a single transaction, the cascade
    // should also retract the `f:reifies*` bundle pointing at the
    // (still-asserted) base edge. Without this auto-cleanup, the
    // bundle stays asserted as an orphan: an inline `@annotation`
    // query would still surface the annotation subject (because
    // `f:reifiesSubject/Predicate/Object` still pin it to the
    // base edge), even though the user clearly intended to delete
    // the whole annotation.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:metadata-retract-cleans-bundle";
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

    // Sanity: inline `@annotation` finds the annotation subject before
    // we do anything else.
    let q_ann = json!({
        "@context": ctx(),
        "select": ["?ann"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "@id": "?ann" }
            }
        }
    });
    let pre = support::query_jsonld_formatted(&fluree, &after_insert.ledger, &q_ann)
        .await
        .expect("pre-cleanup ?ann query");
    assert_eq!(pre.as_array().expect("array").len(), 1);

    // Retract all of the annotation subject's user metadata
    // (here: just the single `ex:role` flake) without touching the
    // base edge.
    let delete = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "Engineer"
        }
    });
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("metadata-only retract");

    // The bundle should also be gone — inline `@annotation` no
    // longer finds the orphaned annotation.
    let post = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q_ann)
        .await
        .expect("post-cleanup ?ann query");
    let arr = post.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "after metadata retract, the bundle must be cleaned too; got: {arr:#?}"
    );

    // The base edge itself should still be queryable — the cleanup
    // is annotation-scoped, not edge-scoped.
    let q_bare = json!({
        "@context": ctx(),
        "select": ["?o"],
        "where": { "@id": "ex:alice", "ex:worksFor": { "@id": "?o" } }
    });
    let bare = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q_bare)
        .await
        .expect("base-edge query post-cleanup");
    assert_eq!(
        bare.as_array().expect("array").len(),
        1,
        "the base edge must survive metadata-only retract: {bare:#?}"
    );
}

#[tokio::test]
async fn replacing_annotation_metadata_in_one_txn_keeps_bundle() {
    // Same-transaction metadata replacement: delete the only
    // existing user-property fact AND insert a new one in a single
    // SPARQL UPDATE. The user's intent is "update the metadata,"
    // not "remove the annotation." The orphan-cleanup pass must
    // see the surviving same-txn assertion and NOT cascade the
    // bundle. Otherwise the new metadata lands as ordinary RDF
    // but loses its edge attachment.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:metadata-replacement";
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

    // Replace `ex:role "Engineer"` with `ex:role "Manager"` in
    // a single update — the only existing metadata is being
    // retracted, but a new metadata fact is being asserted in
    // the same txn.
    let update = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "Engineer"
        },
        "insert": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "Manager"
        }
    });
    let after_update = fluree
        .update(after_insert.ledger, &update)
        .await
        .expect("metadata replacement");

    // The annotation must still be findable via @reifies — the
    // bundle is preserved because the post-transaction metadata
    // set is non-empty (`ex:role "Manager"` survives).
    let q_reifies = json!({
        "@context": ctx(),
        "select": ["?person", "?org", "?role"],
        "where": {
            "ex:role": "?role",
            "@reifies": {
                "@id": "?person",
                "ex:worksFor": { "@id": "?org" }
            }
        }
    });
    let rows = support::query_jsonld_formatted(&fluree, &after_update.ledger, &q_reifies)
        .await
        .expect("@reifies query post-replacement");
    let arr = rows.as_array().expect("array");
    assert_eq!(
        arr.len(),
        1,
        "metadata replacement must preserve the bundle so the new role is reachable: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row");
    assert_eq!(row[2].as_str(), Some("Manager"));
}

#[tokio::test]
async fn retracting_partial_annotation_metadata_keeps_bundle() {
    // Counterpart: if the user retracts ONLY SOME metadata flakes
    // (leaving others asserted), the annotation is still meaningful
    // and the bundle must NOT be cascaded. The auto-cleanup only
    // fires when the annotation subject is left with zero asserted
    // body metadata.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:metadata-retract-partial";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer",
                "ex:since": "2020"
            }
        }
    });
    let after_insert = fluree.insert(ledger0, &txn).await.expect("insert");

    // Retract only `ex:role`; leave `ex:since` asserted.
    let delete = json!({
        "@context": ctx(),
        "delete": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "Engineer"
        }
    });
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("delete");

    // The annotation should still be findable via @reifies, since
    // `ex:since` (and the bundle) are still asserted.
    let q = json!({
        "@context": ctx(),
        "select": ["?ann"],
        "where": {
            "@id": "?person",
            "ex:worksFor": {
                "@id": "?org",
                "@annotation": { "@id": "?ann" }
            }
        }
    });
    let post = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q)
        .await
        .expect("post-partial-retract ?ann query");
    let arr = post.as_array().expect("array");
    assert_eq!(
        arr.len(),
        1,
        "partial metadata retract must NOT cascade the bundle: {arr:#?}"
    );
}

#[tokio::test]
async fn cascade_lpg_mode_cleans_explicit_iri_metadata_too() {
    // LPG mode (`opts.lpgEdgeLifecycle: true`) extends cascade
    // cleanup to explicit-IRI annotations, matching Cypher's
    // relationship lifecycle. Compare against
    // `cascade_keeps_explicit_iri_annotation_metadata` (default
    // RDF mode), which preserves the metadata.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cascade-lpg-cleans-explicit";
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

    // Retract the base edge with `lpgEdgeLifecycle: true`.
    let delete = json!({
        "@context": ctx(),
        "where": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
        "delete": { "@id": "?s", "ex:worksFor": { "@id": "?o" } },
        "opts": { "lpgEdgeLifecycle": true }
    });
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("delete");

    // The explicit-IRI annotation's role must be GONE (LPG semantics).
    let q = json!({
        "@context": ctx(),
        "select": ["?ann", "?role"],
        "where": { "@id": "?ann", "ex:role": "?role" }
    });
    let rows = support::query_jsonld_formatted(&fluree, &after_delete.ledger, &q)
        .await
        .expect("post-cascade explicit-IRI role query");
    let arr = rows.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "LPG mode must clean explicit-IRI annotation metadata too: got {arr:#?}"
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
    let after_delete = fluree
        .update(after_insert.ledger, &delete)
        .await
        .expect("delete");

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
async fn variable_predicate_scan_hides_f_reifies_in_named_graph() {
    // Annotation bundles are emitted in the reified edge's graph,
    // so a variable-predicate scan scoped to a named graph would
    // expose `f:reifies*` flakes there too. The filter must apply
    // to every graph, not only the default graph.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:variable-predicate-named-graph";
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("named-graph annotated insert");

    // Scope the variable-predicate scan to the named graph via the
    // dataset alias.
    let named_graph_alias = format!("{ledger_id}#http://example.org/hr-graph");
    let query = json!({
        "@context": ctx(),
        "from": &named_graph_alias,
        "select": ["?p"],
        "where": { "@id": "ex:emp/alice-acme", "?p": "?o" }
    });

    // `query_connection` is the dataset-aware path; pair with a
    // formatter against the post-insert snapshot.
    let result = fluree
        .query_connection(&query)
        .await
        .expect("named-graph variable-predicate query");
    let ledger = fluree.ledger(ledger_id).await.expect("reload ledger");
    let json = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let arr = json.as_array().expect("array");

    // Collect predicate bindings and assert no `f:reifies*` leaks.
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
    for p in &predicates {
        assert!(
            !p.starts_with("https://ns.flur.ee/db#reifies"),
            "f:reifies* must not leak from named-graph variable-predicate scan: {p} \
             (full bindings: {predicates:?})"
        );
    }
    // The user-authored predicate should still be visible.
    assert!(
        predicates
            .iter()
            .any(|p| p == "http://example.org/role" || p == "ex:role"),
        "user-authored ex:role must be visible in named-graph scan: {predicates:?}"
    );

    // Drop unused suppression: the test is the assertion.
    drop(committed);
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
        predicates
            .iter()
            .any(|p| p == "http://example.org/role" || p == "ex:role"),
        "user-authored ex:role must be visible: {predicates:?}"
    );
}

#[tokio::test]
async fn opts_include_system_facts_does_not_relax_direct_mention_firewall() {
    // Contract: `opts.includeSystemFacts: true` only relaxes the
    // variable-predicate scan filter. Direct mention of an
    // `f:reifies*` IRI in a query is rejected at parse time
    // regardless of the flag. The parser firewall is the
    // contract-level boundary.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:opts-include-direct-still-rejected";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let _ = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "ex:role": "Engineer" }
                }
            }),
        )
        .await
        .expect("annotated insert");

    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let direct_query = json!({
        "@context": {
            "ex": "http://example.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "select": ["?ann"],
        "where": { "@id": "?ann", "f:reifiesPredicate": "?p" },
        "opts": { "includeSystemFacts": true }
    });

    let result = support::query_jsonld_formatted(&fluree, &ledger, &direct_query).await;
    let err = result.expect_err("direct f:reifies* mention must be rejected even with opt-in");
    let msg = err.to_string();
    assert!(
        msg.contains("system-controlled") && msg.contains("reifiesPredicate"),
        "expected system-controlled rejection regardless of includeSystemFacts; got: {msg}"
    );
}

#[tokio::test]
async fn opts_include_system_facts_surfaces_f_reifies() {
    // The `opts.includeSystemFacts: true` escape disables the
    // variable-predicate filter so debug / inspection callers can see
    // the underlying `f:reifies*` system facts. Without the flag, the
    // filter hides them (covered by `variable_predicate_scan_hides_f_reifies`).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:opts-include-system-facts";
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "select": ["?p"],
        "where": { "@id": "ex:emp/alice-acme", "?p": "?o" },
        "opts": { "includeSystemFacts": true }
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("opts.includeSystemFacts query");
    let arr = rows.as_array().expect("array");

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

    // With the escape, all three required `f:reifies*` predicates are
    // visible from the annotation subject.
    let leaked_reifies: Vec<&String> = predicates
        .iter()
        .filter(|p| p.starts_with("https://ns.flur.ee/db#reifies") || p.starts_with("f:reifies"))
        .collect();
    assert!(
        leaked_reifies.len() >= 3,
        "opts.includeSystemFacts: true must surface the f:reifies* bundle \
         (got predicates: {predicates:?})"
    );
}

#[tokio::test]
async fn opts_include_system_facts_propagates_through_dataset_path() {
    // The dataset/connection query path
    // (`view::dataset_query::execute_dataset_with_r2rml`) builds its
    // own `ContextConfig` separate from the single-graph view path.
    // Both must thread `executable.query.include_system_facts` so the
    // opt-in works whichever path the api dispatcher picks.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:opts-dataset-path";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let _ = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer"
                    }
                }
            }),
        )
        .await
        .expect("annotated insert");

    let query = json!({
        "@context": ctx(),
        "from": ledger_id,
        "select": ["?p"],
        "where": { "@id": "ex:emp/alice-acme", "?p": "?o" },
        "opts": { "includeSystemFacts": true }
    });
    let result = fluree
        .query_connection(&query)
        .await
        .expect("dataset-path query");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let json = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let arr = json.as_array().expect("array");

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
    let leaked: Vec<&String> = predicates
        .iter()
        .filter(|p| p.starts_with("https://ns.flur.ee/db#reifies") || p.starts_with("f:reifies"))
        .collect();
    assert!(
        leaked.len() >= 3,
        "dataset-path query must propagate opts.includeSystemFacts to the scan operator \
         (got predicates: {predicates:?})"
    );
}

#[tokio::test]
async fn opts_include_system_facts_works_for_ask_queries() {
    // ASK queries return from the parser before `parse_options()`
    // runs, so `opts.includeSystemFacts` has to be parsed inline on
    // that branch. Without that, an ASK against an annotation
    // subject's `?p`-shape would always answer false even with the
    // opt-in set.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:opts-ask";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let _ = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer"
                    }
                }
            }),
        )
        .await
        .expect("annotated insert");

    let ledger = fluree.ledger(ledger_id).await.expect("reload");

    // Ask whether the annotation subject has *any* predicate. With
    // the filter on (default) this still answers true via the
    // ex:role flake. Pin the discriminating shape: ask via a SID-
    // bound predicate of `f:reifiesSubject`-via-variable that only
    // matches when the f:reifies* row passes the scan filter.
    let q_default = json!({
        "@context": ctx(),
        "ask": [{
            "@id": "ex:emp/alice-acme",
            "?p": { "@id": "ex:alice" }
        }]
    });
    let resp_default = fluree
        .query(&support::graphdb_from_ledger(&ledger), &q_default)
        .await
        .expect("ask default");
    let json_default: JsonValue = resp_default
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld default");
    assert_eq!(
        json_default,
        JsonValue::Bool(false),
        "without includeSystemFacts, ASK over a hidden f:reifies* row must answer false: {json_default}"
    );

    // With the opt-in, the ASK now returns true because the scan
    // filter is bypassed and the f:reifiesSubject row binds.
    let q_opt = json!({
        "@context": ctx(),
        "ask": [{
            "@id": "ex:emp/alice-acme",
            "?p": { "@id": "ex:alice" }
        }],
        "opts": { "includeSystemFacts": true }
    });
    let resp_opt = fluree
        .query(&support::graphdb_from_ledger(&ledger), &q_opt)
        .await
        .expect("ask opt");
    let json_opt: JsonValue = resp_opt.to_jsonld(&ledger.snapshot).expect("to_jsonld opt");
    assert_eq!(
        json_opt,
        JsonValue::Bool(true),
        "ASK + opts.includeSystemFacts must surface f:reifies* rows: {json_opt}"
    );
}

#[tokio::test]
async fn history_query_surfaces_f_reifies_events() {
    // History-range queries dispatch through `BinaryHistoryScanOperator`,
    // not `BinaryScanOperator`. The history operator has no
    // variable-predicate filter for `f:reifies*` because attachment
    // lifecycle events are part of the ledger's history and the
    // history-range surface is the documented inspection path for
    // them. This test pins the carve-out: a history scan over the
    // annotation subject sees its `f:reifies*` events without any
    // opt-in.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:history-surfaces-reifies";
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

    // History-range query over the annotation subject. `from`/`to`
    // dispatches through `view::dataset_query` which selects
    // `BinaryHistoryScanOperator` instead of the normal
    // `BinaryScanOperator`.
    let query = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}@t:1"),
        "to":   format!("{ledger_id}@t:latest"),
        "select": ["?p", "?o"],
        "where": [{
            "@id": "ex:emp/alice-acme",
            "?p": {"@value": "?o"}
        }]
    });
    let result = fluree
        .query_connection(&query)
        .await
        .expect("history range query");
    let json = result
        .to_jsonld(&committed.ledger.snapshot)
        .expect("to_jsonld");

    // At least one `f:reifies*` predicate IRI must appear in the
    // bound-?p column — the history operator does not apply the
    // variable-predicate filter.
    let json_str = serde_json::to_string(&json).expect("serialize history rows");
    assert!(
        json_str.contains("reifies"),
        "history-range query must surface f:reifies* events for inspection \
         (payload: {json_str})"
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
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("annotated insert");

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
async fn wildcard_subject_hydration_hides_anonymous_annotation_sids() {
    // Anonymous (blank-node) annotation subjects are LPG-style
    // internal occurrence ids per the design contract. When a
    // wildcard subject hydration query happens to bind a row's
    // subject variable to such an anonymous SID — typically because
    // the user matched on a body property like `ex:role` —
    // expanding it would leak the bnode identifier as a top-level
    // result. The hydration filter at the top of `format_subject`
    // returns Null for anonymous annotation subjects so the row
    // surfaces as `null` rather than `{"@id": "_:bnode_x", ...}`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:wildcard-hides-anon-anns";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Anonymous annotation: no `@id` → blank-node SID minted by
    // the transactor.
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("anonymous annotation insert");

    let query = json!({
        "@context": ctx(),
        "select": {"?ann": ["*"]},
        "where": { "@id": "?ann", "ex:role": "Engineer" }
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("wildcard hydration over anonymous annotation");
    let arr = rows.as_array().expect("array");

    // Single-column hydration drops `null` rows entirely, so a
    // hidden anonymous annotation surfaces as an empty array
    // rather than `[null]`. The contract is: no blank-node `@id`
    // leaks through the result, regardless of array length.
    let serialized = serde_json::to_string(&rows).expect("serialize");
    assert!(
        !serialized.contains("_:") && !serialized.contains("\"@id\""),
        "anonymous annotation subject must not leak its blank-node @id: {serialized}"
    );
    assert_eq!(
        arr.len(),
        0,
        "hidden anonymous annotation should drop the row entirely (got {arr:#?})"
    );
}

#[tokio::test]
async fn wildcard_subject_hydration_keeps_explicit_iri_annotations_visible() {
    // Counterpart to the anonymous-hide test. Explicit-IRI
    // annotation subjects are ordinary user-named resources — the
    // user wrote the `@id` so they want to see it. The hydration
    // filter only triggers on blank-node SIDs, so explicit IRIs
    // pass through unchanged.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:wildcard-keeps-explicit-anns";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
        }
    });
    let committed = fluree
        .insert(ledger0, &txn)
        .await
        .expect("explicit annotation insert");

    let query = json!({
        "@context": ctx(),
        "select": {"?ann": ["*"]},
        "where": { "@id": "?ann", "ex:role": "Engineer" }
    });
    let rows = support::query_jsonld_formatted(&fluree, &committed.ledger, &query)
        .await
        .expect("wildcard hydration over explicit annotation");
    let arr = rows.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    let node = arr[0].as_object().expect("hydrated node");
    let id = node.get("@id").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        id == "ex:emp/A" || id == "http://example.org/emp/A",
        "explicit-IRI annotation @id must remain visible: {node:#?}"
    );
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
            leaked_named_graph_attachments.push(format!("{edge_key:?} -> {live:?}"));
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

#[tokio::test]
async fn delete_by_annotation_id_retracts_only_targeted_occurrence() {
    // Two parallel annotations on the same edge:
    //   ex:emp/A → role=Engineer
    //   ex:emp/B → role=Manager
    // A delete with `@annotation: { @id: ex:emp/A }` must retract
    // only A's f:reifies* bundle. B and the base edge survive
    // unchanged. This is the design's "Delete by Annotation Id"
    // shape — exactly that occurrence, not the base edge.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-by-id";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    {
                        "@id": "ex:alice",
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                        }
                    },
                    {
                        "@id": "ex:alice",
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
                        }
                    }
                ]
            }),
        )
        .await
        .expect("insert two parallel annotations");

    let r2 = fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "@id": "ex:emp/A" }
                    }
                }
            }),
        )
        .await
        .expect("delete by annotation id");
    assert_eq!(
        r2.receipt.flake_count, 3,
        "exactly three f:reifies* retracts (subject/predicate/object)"
    );

    // Surviving annotations: only B should appear in `@reifies`
    // queries because A's bundle is gone.
    let surviving = json!({
        "@context": ctx(),
        "select": ["?ann", "?role"],
        "where": {
            "@id": "?ann",
            "ex:role": "?role",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        }
    });
    let rows = support::query_jsonld_formatted(&fluree, &r2.ledger, &surviving)
        .await
        .expect("query survivors");
    let arr = rows.as_array().expect("rows");
    assert_eq!(
        arr.len(),
        1,
        "only ex:emp/B should survive @reifies probe: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(&row[0], "ex:emp/B", "http://example.org/emp/B"));
    assert_eq!(row[1].as_str(), Some("Manager"));

    // Base edge survives — a bare triple query still finds it.
    let base = json!({
        "@context": ctx(),
        "select": ["?org"],
        "where": { "@id": "ex:alice", "ex:worksFor": { "@id": "?org" } }
    });
    let base_rows = support::query_jsonld_formatted(&fluree, &r2.ledger, &base)
        .await
        .expect("base edge query");
    assert_eq!(
        base_rows.as_array().map(Vec::len),
        Some(1),
        "base edge must still be asserted after annotation-only delete"
    );
}

#[tokio::test]
async fn delete_by_annotation_id_lpg_mode_cleans_explicit_iri_body() {
    // Cypher relationship-delete semantics: in LPG mode
    // (`opts.lpgEdgeLifecycle: true`), a by-id retract of an
    // explicit-IRI annotation should also retract the body, not
    // just the f:reifies* bundle. Default RDF mode preserves the
    // body — covered by
    // `delete_by_annotation_id_explicit_iri_preserves_body_in_rdf_mode`.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-by-id-lpg-cleans-body";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer",
                        "ex:since": "2024-01-01"
                    }
                }
            }),
        )
        .await
        .expect("annotated insert");

    fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "@id": "ex:emp/alice-acme" }
                    }
                },
                "opts": { "lpgEdgeLifecycle": true }
            }),
        )
        .await
        .expect("by-id delete with lpgEdgeLifecycle");

    // Body must be GONE — both ex:role and ex:since.
    let body = json!({
        "@context": ctx(),
        "select": ["?role", "?since"],
        "where": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "?role",
            "ex:since": "?since"
        }
    });
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &body)
        .await
        .expect("post-delete body query");
    let arr = rows.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "LPG mode by-id retract must clean explicit-IRI annotation body: got {arr:#?}"
    );

    // Base edge still survives — by-id retract targets the
    // annotation occurrence, not the edge it reifies.
    let base = json!({
        "@context": ctx(),
        "select": ["?org"],
        "where": { "@id": "ex:alice", "ex:worksFor": { "@id": "?org" } }
    });
    let base_rows = support::query_jsonld_formatted(&fluree, &ledger, &base)
        .await
        .expect("base edge query");
    assert_eq!(
        base_rows.as_array().map(Vec::len),
        Some(1),
        "base edge must survive a by-id annotation retract even in LPG mode"
    );
}

#[tokio::test]
async fn delete_by_annotation_id_explicit_iri_preserves_body_in_rdf_mode() {
    // Per the design contract, deleting an explicit-IRI annotation
    // by @id retracts only the f:reifies* bundle in default RDF
    // mode — the user-named body metadata stays as ordinary RDF.
    // The LPG-mode counterpart
    // (`delete_by_annotation_id_lpg_mode_cleans_explicit_iri_body`)
    // pins the opt-in cleanup path.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-by-id-rdf-body";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer",
                        "ex:since": "2024-01-01"
                    }
                }
            }),
        )
        .await
        .expect("annotated insert");

    fluree
        .update(
            r1.ledger.clone(),
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "@id": "ex:emp/alice-acme" }
                    }
                }
            }),
        )
        .await
        .expect("delete by id (RDF mode)");

    // The body should still be queryable as ordinary RDF — the
    // user-named `ex:emp/alice-acme` resource has its `ex:role`
    // and `ex:since` predicates preserved.
    let body = json!({
        "@context": ctx(),
        "select": ["?role", "?since"],
        "where": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "?role",
            "ex:since": "?since"
        }
    });
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &body)
        .await
        .expect("body query");
    assert_eq!(
        rows.as_array().map(Vec::len),
        Some(1),
        "explicit-IRI annotation body must remain queryable after by-id retract in RDF mode"
    );
}

#[tokio::test]
async fn delete_by_annotation_id_named_graph_retracts_in_correct_graph() {
    // Regression: when an annotated edge lives in a named graph, the
    // f:reifies* assertion flakes carry `g = Some(graph_sid)` and
    // the synthetic annotation node also lives in that named graph.
    // The by-id retract pre-pass must thread the @graph through to
    // the synthesized retract template — otherwise the retract
    // flakes land in the default graph (`g = None`), Fluree's
    // flake identity includes `g`, and the retract fails to cancel
    // the named-graph assertion (annotation stays asserted, query
    // continues to surface it).
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-by-id-named-graph";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:hr-graph",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/A",
                        "ex:role": "Engineer"
                    }
                }
            }),
        )
        .await
        .expect("named-graph annotated insert");

    let r2 = fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "@graph": "ex:hr-graph",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "@id": "ex:emp/A" }
                    }
                }
            }),
        )
        .await
        .expect("named-graph by-id delete");
    // Three retract flakes for the bundle (subject/predicate/object).
    // f:reifiesGraph is also retracted because the synthesized
    // template carries it explicitly to match the original
    // assertion's identity. So flake_count = 4: subject + predicate
    // + object + reifiesGraph.
    assert_eq!(
        r2.receipt.flake_count, 4,
        "named-graph by-id retract must cancel all four reifies* flakes \
         emitted at insert time (subject/predicate/object + reifiesGraph)"
    );

    // The annotation should no longer surface via @reifies. If the
    // graph wasn't threaded, the retract flakes would have landed
    // in the default graph and the named-graph assertion would
    // still exist — this query would still find ex:emp/A.
    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}#http://example.org/hr-graph"),
        "select": ["?ann", "?role"],
        "where": {
            "@id": "?ann",
            "ex:role": "?role",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        }
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("named-graph @reifies query");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let arr = json.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "named-graph annotation must be retracted; found survivors: {arr:#?}"
    );
}

#[tokio::test]
async fn delete_by_annotation_selector_retracts_matching_occurrence() {
    // Selector form: `@annotation: { ex:role: "Engineer" }` (no @id)
    // names the *kind* of annotation to retract. The pre-pass mints a
    // fresh variable, synthesizes a WHERE pattern that constrains it
    // to every live annotation matching the body and reifying the
    // named edge, and emits a by-variable delete template. The
    // standard UPDATE machinery binds and retracts.
    //
    // Setup: two parallel annotations on the same edge — Engineer
    // (with two body fields) and Manager. The selector retract must
    // cancel exactly the Engineer occurrence, leaving Manager and
    // the base edge intact.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-selector-basic";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@graph": [
                    { "@id": "ex:alice",
                      "ex:worksFor": {
                          "@id": "ex:acme",
                          "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                      }},
                    { "@id": "ex:alice",
                      "ex:worksFor": {
                          "@id": "ex:acme",
                          "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
                      }}
                ]
            }),
        )
        .await
        .expect("insert two parallel annotations");

    fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "ex:role": "Engineer" }
                    }
                }
            }),
        )
        .await
        .expect("selector-form retract");

    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let surviving = json!({
        "@context": ctx(),
        "select": ["?ann", "?role"],
        "where": {
            "@id": "?ann",
            "ex:role": "?role",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        }
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &surviving)
        .await
        .expect("query survivors");
    let arr = rows.as_array().expect("rows");
    assert_eq!(
        arr.len(),
        1,
        "only the Manager annotation should survive: {arr:#?}"
    );
    let row = arr[0].as_array().expect("row");
    assert!(iri_matches(&row[0], "ex:emp/B", "http://example.org/emp/B"));
    assert_eq!(row[1].as_str(), Some("Manager"));

    // Base edge survives — selector retract targets the annotation
    // occurrence, not the edge it reifies.
    let base = json!({
        "@context": ctx(),
        "select": ["?org"],
        "where": { "@id": "ex:alice", "ex:worksFor": { "@id": "?org" } }
    });
    let base_rows = support::query_jsonld_formatted(&fluree, &ledger, &base)
        .await
        .expect("base edge query");
    assert_eq!(
        base_rows.as_array().map(Vec::len),
        Some(1),
        "base edge must survive a selector annotation retract"
    );
}

#[tokio::test]
async fn delete_by_annotation_selector_lpg_mode_cleans_explicit_iri_body() {
    // Cypher-style relationship delete by metadata match. In LPG
    // mode the selector retract also cleans the explicit-IRI
    // annotation's body, not just the f:reifies* bundle.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-selector-lpg";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/alice-acme",
                        "ex:role": "Engineer",
                        "ex:since": "2024-01-01"
                    }
                }
            }),
        )
        .await
        .expect("annotated insert");

    fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "ex:role": "Engineer" }
                    }
                },
                "opts": { "lpgEdgeLifecycle": true }
            }),
        )
        .await
        .expect("selector retract with lpgEdgeLifecycle");

    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let body = json!({
        "@context": ctx(),
        "select": ["?role", "?since"],
        "where": {
            "@id": "ex:emp/alice-acme",
            "ex:role": "?role",
            "ex:since": "?since"
        }
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &body)
        .await
        .expect("post-delete body query");
    let arr = rows.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "LPG-mode selector retract must clean the explicit-IRI annotation body: {arr:#?}"
    );

    let base = json!({
        "@context": ctx(),
        "select": ["?org"],
        "where": { "@id": "ex:alice", "ex:worksFor": { "@id": "?org" } }
    });
    let base_rows = support::query_jsonld_formatted(&fluree, &ledger, &base)
        .await
        .expect("base edge query");
    assert_eq!(
        base_rows.as_array().map(Vec::len),
        Some(1),
        "base edge must survive a selector annotation retract even in LPG mode"
    );
}

#[tokio::test]
async fn delete_by_annotation_selector_named_graph_retracts_in_correct_graph() {
    // Regression: the synthesized WHERE pattern and delete template
    // must thread the named graph through `f:reifiesGraph` (plus
    // their own `@graph` selectors), or the selector retract binds
    // nothing / fires in the wrong graph and the named-graph
    // assertion survives.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-selector-named-graph";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:hr-graph",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": {
                        "@id": "ex:emp/A",
                        "ex:role": "Engineer"
                    }
                }
            }),
        )
        .await
        .expect("named-graph annotated insert");

    fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:alice",
                    "@graph": "ex:hr-graph",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "ex:role": "Engineer" }
                    }
                }
            }),
        )
        .await
        .expect("named-graph selector retract");

    let q = json!({
        "@context": ctx(),
        "from": format!("{ledger_id}#http://example.org/hr-graph"),
        "select": ["?ann", "?role"],
        "where": {
            "@id": "?ann",
            "ex:role": "?role",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        }
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("named-graph @reifies query");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let arr = json.as_array().expect("array");
    assert!(
        arr.is_empty(),
        "named-graph annotation must be retracted by selector; found survivors: {arr:#?}"
    );
}

#[tokio::test]
async fn delete_by_annotation_selector_avoids_user_var_collision() {
    // Regression: the pre-pass mints `?_fluree_del_ann_<N>` for
    // selector retracts. If the user happens to reference the same
    // internal variable in their own `where` clause, naive minting
    // would collide and the selector retract would over-constrain
    // against the user's bindings instead of binding fresh. The
    // mint counter is seeded past any user-visible occurrence so
    // the synthesized variable is always fresh.
    //
    // Setup: annotate one edge with role=Engineer. Issue an UPDATE
    // whose user-provided `where` and `delete` use the colliding
    // name `?_fluree_del_ann_0` to bind something *unrelated*
    // (alice's plain `ex:name` triple) so the user's variable maps
    // to a literal. The selector retract must still find the
    // annotation and retract it; if the counter collided, the
    // synthesized WHERE would unify the annotation SID against the
    // literal "Alice" and bind nothing.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:delete-selector-var-collision";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "ex:name": "Alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                }
            }),
        )
        .await
        .expect("insert");

    fluree
        .update(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "where": { "@id": "ex:alice", "ex:name": "?_fluree_del_ann_0" },
                "delete": {
                    "@id": "ex:alice",
                    "ex:worksFor": {
                        "@id": "ex:acme",
                        "@annotation": { "ex:role": "Engineer" }
                    }
                }
            }),
        )
        .await
        .expect("selector retract with colliding user var");

    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let surviving = json!({
        "@context": ctx(),
        "select": ["?ann", "?role"],
        "where": {
            "@id": "?ann",
            "ex:role": "?role",
            "@reifies": {
                "@id": "ex:alice",
                "ex:worksFor": { "@id": "ex:acme" }
            }
        }
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &surviving)
        .await
        .expect("query survivors");
    let arr = rows.as_array().expect("rows");
    assert!(
        arr.is_empty(),
        "selector retract must run even when user already uses our \
         internal variable prefix: {arr:#?}"
    );
}

#[tokio::test]
async fn multi_source_default_pairs_annotations_per_source_graph() {
    // Architectural-fix coverage: under multi-source default-graph
    // queries (`from: [g1, g2]`), each base-edge match must pair
    // only with annotations from the SAME source graph — N+M rows,
    // not N×M.
    //
    // Scenario: the same `(s, p, o)` edge is asserted in two named
    // graphs (g1, g2) with two different annotations. A query
    // unions both graphs into the default graph and asks for the
    // edge plus its annotation role *without* a `Pattern::Graph`
    // wrapper. Before the fix the expansion's f:reifies* lookups
    // fanned across both sources via `DatasetOperator` and produced
    // an N×M cross-product (4 rows for the 2×2 case). The fix wraps
    // each expanded triple chain in `Pattern::DefaultGraphSource`,
    // which iterates dataset.default_graphs() and runs the inner
    // subplan once per source — correlating base edge with its
    // own-source annotations only.
    //
    // The previous bug-pinning test asserted 4 rows; this is the
    // flipped correctness test it became.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:cross-graph-misjoin";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-A",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                }
            }),
        )
        .await
        .expect("graph-A insert");
    let _ = fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-B",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
                }
            }),
        )
        .await
        .expect("graph-B insert");

    let q = json!({
        "@context": ctx(),
        "from": [
            format!("{ledger_id}#http://example.org/graph-A"),
            format!("{ledger_id}#http://example.org/graph-B"),
        ],
        "select": ["?role", "?ann"],
        "where": {
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "?ann", "ex:role": "?role" }
            }
        }
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("multi-source default query");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let arr = json.as_array().expect("array");

    // Correctness: exactly one row per (base-edge, own-source
    // annotation) pair — Engineer/ex:emp/A from graph-A and
    // Manager/ex:emp/B from graph-B, no cross-pairs.
    assert_eq!(
        arr.len(),
        2,
        "must produce exactly N+M rows (one per source's annotation), \
         not the pre-fix N×M cross-product. Got: {arr:#?}"
    );

    // Verify the row content is the per-source pairing, not any
    // accidental misjoin with the right total.
    let pairs: std::collections::HashSet<(String, String)> = arr
        .iter()
        .map(|row| {
            let row = row.as_array().expect("row");
            let role = row[0].as_str().expect("role").to_string();
            let ann = row[1].as_str().expect("ann").to_string();
            (role, ann)
        })
        .collect();
    assert!(
        pairs.contains(&("Engineer".to_string(), "ex:emp/A".to_string())),
        "missing graph-A pair (Engineer, ex:emp/A) in: {pairs:?}"
    );
    assert!(
        pairs.contains(&("Manager".to_string(), "ex:emp/B".to_string())),
        "missing graph-B pair (Manager, ex:emp/B) in: {pairs:?}"
    );
}

#[tokio::test]
async fn multi_source_default_wildcard_does_not_panic_on_synthetic_var() {
    // Regression: an earlier shape of the per-source-correlation fix
    // exposed a planner-minted graph variable in the operator schema
    // without registering it with `VarRegistry`. Wildcard formatters
    // call `vars.name(var_id)`, which would panic for an unregistered
    // VarId. The fix removed the synthetic var entirely — per-source
    // correlation comes from the context switch, not a join key.
    //
    // This test exercises the wildcard path against the same
    // multi-source default-graph annotated dataset to pin the
    // contract: wildcard select must not panic and must surface only
    // user-visible bindings.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:multi-source-wildcard";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-A",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                }
            }),
        )
        .await
        .expect("graph-A insert");
    let _ = fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-B",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
                }
            }),
        )
        .await
        .expect("graph-B insert");

    let q = json!({
        "@context": ctx(),
        "from": [
            format!("{ledger_id}#http://example.org/graph-A"),
            format!("{ledger_id}#http://example.org/graph-B"),
        ],
        "select": "*",
        "where": {
            "@id": "ex:alice",
            "ex:worksFor": {
                "@id": "ex:acme",
                "@annotation": { "@id": "?ann", "ex:role": "?role" }
            }
        }
    });
    let result = fluree
        .query_connection(&q)
        .await
        .expect("wildcard select must not panic on synthetic var name lookup");
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let arr = json.as_array().expect("array");
    assert_eq!(
        arr.len(),
        2,
        "wildcard must still produce one row per source's annotation: {arr:#?}"
    );
}

#[tokio::test]
async fn graph_wrapped_query_correctly_pairs_annotations_per_graph() {
    // Positive coverage: when the user wraps a multi-source query in
    // `Pattern::Graph` (here via JSON-LD's named-graph access form),
    // the executor iterates one graph at a time and the expanded
    // f:reifies* triples scope per iteration. Each annotation pairs
    // only with its own graph's edge — no cross-graph misjoin.
    //
    // This is the workaround documented on the bug pinning test.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations:graph-wrapped-correct";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-A",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/A", "ex:role": "Engineer" }
                }
            }),
        )
        .await
        .expect("graph-A insert");
    let _ = fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:graph-B",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "ex:emp/B", "ex:role": "Manager" }
                }
            }),
        )
        .await
        .expect("graph-B insert");

    // Single-graph query against graph A: two iterations would have
    // produced 2 rows under the bug. Scoping to one graph gives the
    // correct 1 row. Repeat for graph B as a sanity check.
    for (graph_alias, expected_role, expected_ann) in [
        ("ex:graph-A", "Engineer", "ex:emp/A"),
        ("ex:graph-B", "Manager", "ex:emp/B"),
    ] {
        let q = json!({
            "@context": ctx(),
            "from": format!("{ledger_id}#http://example.org/{}",
                graph_alias.strip_prefix("ex:").unwrap()),
            "select": ["?role", "?ann"],
            "where": {
                "@id": "ex:alice",
                "ex:worksFor": {
                    "@id": "ex:acme",
                    "@annotation": { "@id": "?ann", "ex:role": "?role" }
                }
            }
        });
        let result = fluree
            .query_connection(&q)
            .await
            .unwrap_or_else(|e| panic!("query for {graph_alias}: {e:?}"));
        let ledger = fluree.ledger(ledger_id).await.expect("reload");
        let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
        let arr = json.as_array().expect("array");
        assert_eq!(
            arr.len(),
            1,
            "single-graph scope must produce exactly one row for {graph_alias}: {arr:#?}"
        );
        let row = arr[0].as_array().expect("row");
        assert_eq!(
            row[0].as_str(),
            Some(expected_role),
            "role binding for {graph_alias}"
        );
        assert!(
            iri_matches(
                &row[1],
                expected_ann,
                &format!(
                    "http://example.org/{}",
                    expected_ann.strip_prefix("ex:").unwrap()
                ),
            ),
            "annotation IRI for {graph_alias}: {row:#?}"
        );
    }
}
