//! Edge annotations once their `rdf:reifies` links are indexed: hydration,
//! explain, graph transfers, writes and cascades against the indexed links.

#![cfg(feature = "native")]

use crate::support;
use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use fluree_db_indexer::IndexerConfig;
use serde_json::{json, Value as JsonValue};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
    })
}

/// Standard one-edge-with-one-annotation insert used across tests.
fn annotated_insert() -> JsonValue {
    json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": {
                "@id": "ex:emp/alice-acme",
                "ex:role": "Engineer"
            }
        }
    })
}

/// Subject-hydration query that exercises
/// `HydrationFormatter::inject_annotations` — the only call site
/// that reads the annotation's link. A flat `select` with
/// `@annotation` in the `where` clause goes through query
/// expansion and the **sync** JSON-LD formatter, which never
/// touches `inject_annotations`. The hydration path fires only
/// when a subject's `@annotation` block is materialized while
/// formatting a ref value during subject expansion.
fn annotated_hydration_query() -> JsonValue {
    json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    })
}

/// Pull the annotation body's `ex:role` out of a hydration result
/// row, returning `None` when the row's shape doesn't carry one.
/// Tolerates compact-IRI vs expanded-IRI keys and bare-object vs
/// single-element-array shapes (both forms are formatter-legal).
fn extract_role_from_hydration(rows: &JsonValue) -> Option<String> {
    let arr = rows.as_array()?;
    let first = arr.first()?.as_object()?;
    let works_for = first
        .get("ex:worksFor")
        .or_else(|| first.get("http://example.org/worksFor"))?;
    let edge_obj = works_for.as_object().or_else(|| {
        works_for
            .as_array()
            .and_then(|a| a.first().and_then(|v| v.as_object()))
    })?;
    let ann = edge_obj.get("@annotation")?;
    let ann_obj = ann.as_object().or_else(|| {
        ann.as_array()
            .and_then(|a| a.first().and_then(|v| v.as_object()))
    })?;
    ann_obj
        .get("ex:role")
        .or_else(|| ann_obj.get("http://example.org/role"))?
        .as_str()
        .map(String::from)
}

#[tokio::test]
async fn hydration_reads_indexed_annotations() {
    // Subject hydration surfaces an annotation whose link has rolled
    // into the index.
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:scan-fallback";

    let (local, handle) = support::start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        IndexerConfig::small(),
    );

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_insert = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
            // No `wait_for_index_application` here: the test loads
            // fresh via `fluree.ledger()` (no cache participation),
            // so the api's notify-driven cache update isn't on the
            // critical path.

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert!(post.snapshot.has_annotations, "sticky bit set");
            assert_eq!(post.index_t(), post.t(), "the link is indexed");
            let rows =
                support::query_jsonld_formatted(&fluree, &post, &annotated_hydration_query())
                    .await
                    .expect("hydration against the indexed snapshot");
            assert_eq!(
                extract_role_from_hydration(&rows).as_deref(),
                Some("Engineer"),
                "hydration must surface the indexed annotation body"
            );
        })
        .await;
}

#[tokio::test]
async fn non_annotation_ledger_skips_inject_annotations() {
    // Hydration on a ledger that has never seen an `f:reifies*`
    // flake must NOT pay the per-ref-value POST scan that
    // `inject_annotations` does on the M2a fallback path. The gate
    // (mirror of the cascade fast-path) checks both
    // `snapshot.has_annotations` and the overlay's
    // `attachments.has_annotations()`. We can't directly observe
    // "the scan didn't run," but we can verify three positive
    // signals:
    //
    // 1. `snapshot.has_annotations == false` — sticky bit never
    //    flipped on an annotation-free ledger.
    // 2. The overlay's `attachments.has_annotations()` is also
    //    false — no novelty-side `f:reifies*` events.
    // 3. The hydration query returns the right shape with no
    //    `@annotation` keys anywhere — the only output the gate
    //    short-circuits on (the keys would still be absent on the
    //    scan path, but we'd pay the POST scan to find that out).
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:non-annotation-skip";

    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let plain_insert = json!({
        "@context": ctx(),
        "@graph": [
            {"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme"}},
            {"@id": "ex:acme", "ex:name": "Acme"}
        ]
    });
    let after = fluree
        .insert(ledger0, &plain_insert)
        .await
        .expect("plain insert");

    assert!(
        !after.ledger.snapshot.has_annotations,
        "non-annotation ledger must not have sticky bit set"
    );
    assert!(
        !after.ledger.novelty.attachments.has_annotations(),
        "novelty overlay must report zero annotations"
    );
    assert!(
        after.ledger.snapshot.annotation_index.is_none(),
        "non-annotation ledger must not have an annotation_index"
    );
    assert!(
        !after.ledger.snapshot.has_arena_reader(),
        "non-annotation ledger must not advertise an arena reader \
         (gate guarantees no CAS reads on hydration either)"
    );

    // Subject hydration that would otherwise call `inject_annotations`
    // on the worksFor ref value. Confirm output is correct AND has
    // no `@annotation` artifacts.
    let query = json!({
        "@context": ctx(),
        "select": {"?person": ["*", {"ex:worksFor": ["*"]}]},
        "where": {"@id": "?person", "ex:worksFor": {"@id": "?org"}}
    });
    let rows = support::query_jsonld_formatted(&fluree, &after.ledger, &query)
        .await
        .expect("hydration on non-annotation ledger");
    let arr = rows.as_array().expect("rows array");
    assert_eq!(arr.len(), 1, "single subject row");
    let json_str = serde_json::to_string(&arr[0]).expect("serialize row");
    assert!(
        !json_str.contains("@annotation"),
        "non-annotation ledger must not produce any @annotation keys: {json_str}"
    );

    // Reindex with provider attached. Even with the provider asking
    // for events, an annotation-free ledger must produce a fresh
    // root with `annotation_index = None` (no arena artifacts in
    // CAS at all). Verifies the indexer's "non-annotation fast
    // path" — no CAS writes for branch/leaf blobs that would just
    // be empty placeholders.
    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());
    local
        .run_until(async {
            let _ = fluree.ledger_cached(ledger_id).await.unwrap();
            let completion = handle
                .trigger(
                    &fluree_db_api::LedgerId::parse(ledger_id).unwrap(),
                    after.receipt.t,
                )
                .await;
            let _ = completion.wait().await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;

            let post = fluree.ledger(ledger_id).await.expect("post-reindex");
            assert!(
                !post.snapshot.has_annotations,
                "indexed root must not flip sticky bit on non-annotation ledger"
            );
            assert!(
                post.snapshot.annotation_index.is_none(),
                "indexed root must not carry an annotation_index"
            );
            assert!(
                !post.snapshot.has_arena_reader(),
                "post-reindex snapshot must still skip arena reader"
            );
        })
        .await;
}

#[tokio::test]
async fn explain_expands_annotations_as_the_executor_does() {
    // `/explain` must expand an `@annotation` pattern the way the executor
    // does (the body and the reifier's `rdf:reifies` link), or an annotated
    // query explains as nearly empty, and must report the stats the index
    // build wrote.
    use crate::support::graphdb_from_ledger;

    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:explain-expansion";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after = fluree
                .insert(ledger0, &annotated_insert())
                .await
                .expect("annotated insert");
            let _pre = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("pre-reindex cached load");
            support::trigger_index_and_wait(&handle, ledger_id, after.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after.receipt.t).await;
            let post = fluree.ledger(ledger_id).await.expect("reload");

            let query = json!({
                "@context": ctx(),
                "select": ["?person", "?org"],
                "where": {
                    "@id": "?person",
                    "ex:worksFor": {
                        "@id": "?org",
                        "@annotation": { "ex:role": "Engineer" }
                    }
                }
            });
            let resp = fluree
                .explain(&graphdb_from_ledger(&post), &query)
                .await
                .expect("explain");
            assert_ne!(
                resp["plan"]["optimization"], "none",
                "an indexed ledger must report stats availability (got plan: {})",
                resp["plan"]
            );

            let optimized = resp["plan"]["optimized"]
                .as_array()
                .expect("optimized order is an array");
            let properties: Vec<&str> = optimized
                .iter()
                .filter_map(|entry| entry["pattern"]["property"].as_str())
                .collect();
            for expected in [
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies",
                "ex:role",
            ] {
                assert!(
                    properties.contains(&expected),
                    "the expansion's {expected} triple must be planned: {properties:?}"
                );
            }
            for entry in optimized {
                assert!(
                    entry.get("selectivity").is_some(),
                    "optimized entry missing selectivity: {entry}"
                );
            }
        })
        .await;
}

/// #1467: a COPY of a named-graph annotation must survive a reindex: the
/// copied link reads back out of the indexed snapshot in the destination
/// graph, and the source's stays.
#[tokio::test]
async fn transfer_named_to_named_survives_reindex() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:xfer-named-to-named";
    let g1 = "http://example.org/g1";
    let g2 = "http://example.org/g2";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let seeded = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "@graph": g1,
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/alice-acme", "ex:role": "Engineer" }
                        }
                    }),
                )
                .await
                .expect("seed named-graph annotation");

            // COPY <g1> TO <g2> — lower the SPARQL op and stage it.
            let sparql = format!("COPY <{g1}> TO <{g2}>");
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            assert!(
                !parsed.has_errors(),
                "parse errors: {:?}",
                parsed.diagnostics
            );
            let ast = parsed.ast.expect("AST");
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&seeded.ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &ast,
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower COPY");
            let after_copy = fluree
                .stage_owned(seeded.ledger)
                .txn(txn)
                .execute()
                .await
                .expect("stage COPY");

            // Force the manager to cache the running ledger so the indexer's
            // attachment-events provider finds it (mirrors the pattern in
            // `incremental_arena_seal_then_arena_backed_query`).
            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_copy.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_copy.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert_eq!(post.index_t(), post.t());
            for g in [g1, g2] {
                let g_id = post
                    .snapshot
                    .graph_registry
                    .graph_id_for_iri(g)
                    .expect("registered graph");
                let anns = support::decode_annotations_for_subject(
                    &post,
                    g_id,
                    "http://example.org/alice",
                )
                .await;
                assert_eq!(anns.len(), 1, "graph {g} holds its own link: {anns:?}");
            }
        })
        .await;
}

/// default→named at the DURABLE-INDEX layer (#1483 review: the copy was only
/// checked in-memory). After `COPY DEFAULT TO <g2>` and a reindex, the link
/// reads back out of both graphs.
#[tokio::test]
async fn transfer_default_to_named_synthesized_anchor_survives_reindex() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:xfer-default-to-named";
    let g2 = "http://example.org/g2";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            // Default-graph annotated edge.
            let seeded = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "ex:worksFor": {
                            "@id": "ex:acme",
                            "@annotation": { "@id": "ex:emp/alice-acme", "ex:role": "Engineer" }
                        }
                    }),
                )
                .await
                .expect("seed default-graph annotation");

            let sparql = format!("COPY DEFAULT TO <{g2}>");
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            assert!(
                !parsed.has_errors(),
                "parse errors: {:?}",
                parsed.diagnostics
            );
            let ast = parsed.ast.expect("AST");
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&seeded.ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &ast,
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower COPY DEFAULT");
            let after_copy = fluree
                .stage_owned(seeded.ledger)
                .txn(txn)
                .execute()
                .await
                .expect("stage COPY DEFAULT");

            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_copy.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_copy.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert_eq!(post.index_t(), post.t());
            let g2_id = post
                .snapshot
                .graph_registry
                .graph_id_for_iri(g2)
                .expect("registered graph");
            for g_id in [0, g2_id] {
                let anns = support::decode_annotations_for_subject(
                    &post,
                    g_id,
                    "http://example.org/alice",
                )
                .await;
                assert_eq!(anns.len(), 1, "graph {g_id} holds its own link: {anns:?}");
            }
        })
        .await;
}

/// An indexed annotation on a LITERAL edge, read with a variable object: the
/// link keys the literal by (value, datatype, tag) exactly as it was written,
/// so the bound value must match it.
#[tokio::test]
async fn indexed_literal_object_annotation_matches() {
    let fluree = FlureeBuilder::memory()
        .with_ledger_cache_config(fluree_db_api::LedgerManagerConfig::default())
        .build_memory();
    let ledger_id = "it/edge-annotations-indexed:literal-object";

    let (local, handle) =
        support::start_background_indexer_with_attachments(&fluree, IndexerConfig::small());

    local
        .run_until(async move {
            let ledger0 = genesis_ledger(&fluree, ledger_id);
            let after_insert = fluree
                .insert(
                    ledger0,
                    &json!({
                        "@context": ctx(),
                        "@id": "ex:alice",
                        "ex:score": {
                            "@value": 42,
                            "@annotation": {"ex:confidence": "high"}
                        }
                    }),
                )
                .await
                .expect("literal annotated insert");
            let _ = fluree
                .ledger_cached(ledger_id)
                .await
                .expect("cached load before reindex");

            support::trigger_index_and_wait(&handle, ledger_id, after_insert.receipt.t).await;
            support::wait_for_index_application(&fluree, ledger_id, after_insert.receipt.t).await;

            let post = fluree
                .ledger(ledger_id)
                .await
                .expect("reload after reindex");
            assert_eq!(post.index_t(), post.t());

            let sparql = r"
                PREFIX ex: <http://example.org/>
                SELECT ?score ?conf WHERE {
                  ex:alice ex:score ?score {| ex:confidence ?conf |} .
                }
            ";
            let result = support::query_sparql(&fluree, &post, sparql)
                .await
                .expect("var-object literal annotation query");
            let rows = result.to_sparql_json(&post.snapshot).expect("sparql json");
            let bindings = rows["results"]["bindings"]
                .as_array()
                .expect("bindings array")
                .clone();
            assert_eq!(
                bindings.len(),
                1,
                "the literal reified triple must match its indexed link: {bindings:#?}"
            );
            assert_eq!(bindings[0]["score"]["value"].as_str(), Some("42"));
            assert_eq!(bindings[0]["conf"]["value"].as_str(), Some("high"));
        })
        .await;
}

// =============================================================================
// Named-graph links after indexing
// =============================================================================

/// A named-graph annotation must stay writable once its link has been
/// indexed: re-asserting it, enriching its body, and pointing its reifier at
/// a second edge as well (a reifier may reify several triples).
#[tokio::test]
async fn indexed_named_graph_annotation_stays_writable() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations-indexed:named-graph-rewrite";

    let annotate = |props: JsonValue| {
        json!({
            "@context": ctx(),
            "@id": "ex:alice",
            "@graph": "ex:claims-graph",
            "ex:knows": {
                "@id": "ex:bob",
                "@annotation": props
            }
        })
    };

    let committed = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &annotate(json!({"@id": "ex:claim1", "ex:confidence": 0.9})),
        )
        .await
        .expect("annotated named-graph insert");
    assert!(committed.ledger.t() > 0);

    // Move the link out of novelty and into the index.
    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    // Re-assert the identical annotation against the indexed link.
    let reloaded = fluree.ledger(ledger_id).await.expect("reload indexed");
    fluree
        .insert(
            reloaded,
            &annotate(json!({"@id": "ex:claim1", "ex:confidence": 0.9})),
        )
        .await
        .expect("re-asserting an unchanged indexed named-graph annotation must be accepted");

    // Add a property to the same claim — the ordinary "enrich a claim" edit.
    let reloaded = fluree.ledger(ledger_id).await.expect("reload indexed");
    fluree
        .insert(
            reloaded,
            &annotate(json!({"@id": "ex:claim1", "ex:source": "hr"})),
        )
        .await
        .expect("adding a property to an indexed named-graph claim must be accepted");

    // The same reifier on a second edge names both.
    let reloaded = fluree.ledger(ledger_id).await.expect("reload indexed");
    let both = fluree
        .insert(
            reloaded,
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:claims-graph",
                "ex:knows": {
                    "@id": "ex:carol",
                    "@annotation": {"@id": "ex:claim1", "ex:confidence": 0.5}
                }
            }),
        )
        .await
        .expect("a reifier on a second edge");
    let g_id = both
        .ledger
        .snapshot
        .graph_registry
        .graph_id_for_iri("http://example.org/claims-graph")
        .expect("registered graph");
    let anns =
        support::decode_annotations_for_subject(&both.ledger, g_id, "http://example.org/alice")
            .await;
    assert_eq!(anns.len(), 2, "claim1 names both edges: {anns:?}");
}

/// Count live `rdf:reifies` links across every graph.
///
/// The two cascade tests below need to tell an *orphaned* link from a
/// *retracted* one, and the annotation query surface cannot: annotation
/// syntax reads the link only through its base edge's body join, so once
/// the edge is gone it returns nothing either way. Reading the links
/// directly is the view that distinguishes them.
async fn live_reifies_flakes(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> usize {
    let ledger = fluree.ledger(ledger_id).await.expect("reload");
    let mut live = 0;
    for g_id in 0..6u16 {
        let flakes = fluree_db_core::range_with_overlay(
            &ledger.snapshot,
            g_id,
            ledger.novelty.as_ref(),
            fluree_db_core::comparator::IndexType::Psot,
            fluree_db_core::range::RangeTest::Eq,
            fluree_db_core::range::RangeMatch::predicate(fluree_db_core::rdf_reifies_sid().clone()),
            fluree_db_core::range::RangeOptions::new().with_to_t(ledger.t()),
        )
        .await
        .unwrap_or_default();
        live += flakes.len();
    }
    live
}

/// Deleting a base edge must retract the claim that reifies it, even once the
/// link has been indexed in a named graph — the cascade's link lookup and the
/// retract it writes are both scoped to the edge's graph.
#[tokio::test]
async fn deleting_an_indexed_named_graph_edge_cascades_to_its_claim() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations-indexed:cascade-named-graph";

    let committed = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:claims-graph",
                "ex:knows": {
                    "@id": "ex:bob",
                    "@annotation": {"@id": "ex:claim1", "ex:confidence": 0.9}
                }
            }),
        )
        .await
        .expect("annotated named-graph insert");
    assert!(committed.ledger.t() > 0);

    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    // Guards the counter itself: if this read cannot see the link at all,
    // the post-delete assertion below would pass for the wrong reason.
    let before = live_reifies_flakes(&fluree, ledger_id).await;
    assert!(
        before > 0,
        "the indexed link must be visible to this read before the delete"
    );

    let deleted = fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/>\n\
             DELETE DATA { GRAPH <http://example.org/claims-graph> \
             { ex:alice ex:knows ex:bob } }",
        )
        .commit()
        .await
        .expect("delete the base edge");
    assert!(deleted.receipt.t > committed.ledger.t());

    assert_eq!(
        live_reifies_flakes(&fluree, ledger_id).await,
        0,
        "the claim's link must not outlive the edge it reifies"
    );
}

/// The *other* cascade pass against an indexed named-graph link.
///
/// Pass 1 fires when the base edge is deleted. Pass 2 fires when the user
/// deletes an annotation's last piece of metadata without touching the edge,
/// which would leave the link behind with nothing to describe.
#[tokio::test]
async fn deleting_an_indexed_named_graph_claim_body_cascades_its_link() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/edge-annotations-indexed:cascade-named-graph-orphan";

    let committed = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": ctx(),
                "@id": "ex:alice",
                "@graph": "ex:claims-graph",
                "ex:knows": {
                    "@id": "ex:bob",
                    // A string, deliberately. A bare `0.9` in the SPARQL below
                    // is an `xsd:decimal` while JSON-LD stores it as an
                    // `xsd:double`, so `DELETE DATA` would match nothing, the
                    // transaction would still commit, and this test would pass
                    // without the cascade ever running.
                    "@annotation": {"@id": "ex:claim1", "ex:role": "Engineer"}
                }
            }),
        )
        .await
        .expect("annotated named-graph insert");

    support::rebuild_and_publish_index(&fluree, ledger_id).await;

    assert!(
        live_reifies_flakes(&fluree, ledger_id).await > 0,
        "the indexed link must be visible to this read before the delete"
    );

    // Delete the claim's only metadata fact, leaving the edge itself alone.
    let deleted = fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/>\n\
             DELETE DATA { GRAPH <http://example.org/claims-graph> \
             { ex:claim1 ex:role \"Engineer\" } }",
        )
        .commit()
        .await
        .expect("delete the claim body");
    assert!(deleted.receipt.t > committed.ledger.t());

    assert_eq!(
        live_reifies_flakes(&fluree, ledger_id).await,
        0,
        "a link whose claim has no body left must not survive as an orphan"
    );
}
