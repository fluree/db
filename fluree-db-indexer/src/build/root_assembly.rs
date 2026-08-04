//! Root encode, CAS write, garbage chain, and IndexResult derivation.
//!
//! Both the full-rebuild and incremental pipelines end by encoding an
//! `IndexRoot` or `IndexRoot`, optionally attaching a garbage manifest,
//! writing the root to CAS, and deriving an `IndexResult`. This module
//! provides shared helpers to avoid duplicating that logic.

use fluree_db_binary_index::format::index_root::{DefaultGraphOrder, IndexRoot};
use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef, DictRefs, GraphArenaRefs};
use fluree_db_core::{ContentId, ContentKind, ContentStore};
use std::collections::BTreeMap;

use super::types::{UploadedDicts, UploadedIndexes};

use crate::error::{IndexerError, Result};
use crate::gc;
use crate::{IndexResult, IndexStats};

/// Validate that an index root's materialized namespace table matches the
/// commit-derived table exactly. A mismatch indicates an indexer or publisher
/// bug — fail fast rather than silently diverging.
pub(crate) fn reconcile_ns_at_publish(
    root_ns: &BTreeMap<u16, String>,
    commit_derived_ns: &std::collections::HashMap<u16, String>,
    index_t: i64,
) -> Result<()> {
    let expected: BTreeMap<u16, String> = commit_derived_ns
        .iter()
        .map(|(&code, prefix)| (code, prefix.clone()))
        .collect();
    if *root_ns != expected {
        // Find a representative mismatch for a targeted error message.
        let detail = find_ns_mismatch(root_ns, &expected);
        return Err(IndexerError::Core(fluree_db_core::Error::invalid_index(
            format!(
                "namespace reconciliation failure at index publish (index_t={index_t}): \
                 root namespace_codes does not match commit-derived table \
                 — indexer/publisher bug ({detail})"
            ),
        )));
    }
    Ok(())
}

/// Find a representative mismatch between two namespace tables for diagnostics.
fn find_ns_mismatch(root_ns: &BTreeMap<u16, String>, commit_ns: &BTreeMap<u16, String>) -> String {
    for (code, commit_prefix) in commit_ns {
        match root_ns.get(code) {
            Some(root_prefix) if root_prefix == commit_prefix => {}
            other => {
                return format!(
                    "example mismatch: code {code} commit={:?} root={:?}",
                    Some(commit_prefix),
                    other
                );
            }
        }
    }
    for (code, root_prefix) in root_ns {
        if !commit_ns.contains_key(code) {
            return format!(
                "example mismatch: code {code} commit=None root={:?}",
                Some(root_prefix)
            );
        }
    }
    "tables differ (no specific mismatch found)".to_string()
}

/// Context for linking the GC chain to a previous index root.
///
/// Used by both pipelines, but computed differently:
/// - **Rebuild**: loads the old root from CAS, computes `all_cas_ids()` set
///   difference to find garbage CIDs.
/// - **Incremental**: `IncrementalRootBuilder` tracks replaced CIDs explicitly.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use encode_and_write_root().
pub(crate) struct GarbageContext {
    /// CIDs that should be recorded as garbage (replaced by this new root).
    pub garbage_cids: Vec<ContentId>,
    /// Previous root linkage (for GC chain traversal).
    pub prev_index: Option<BinaryPrevIndexRef>,
}

/// Encode an `IndexRoot`, attach garbage/prev_index, write to CAS,
/// and return an `IndexResult`.
///
/// This is the shared "last mile" for both rebuild and incremental pipelines.
// Kept for: shared root finalization for both rebuild and incremental pipelines.
// Use when: rebuild.rs Phase F is refactored to use this shared helper.
#[expect(dead_code)]
pub(crate) async fn encode_and_write_root(
    content_store: &dyn ContentStore,
    ledger_id: &str,
    mut root: IndexRoot,
    garbage_ctx: Option<GarbageContext>,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    // Attach garbage manifest and prev_index if provided.
    if let Some(ctx) = garbage_ctx {
        if let Some(prev) = ctx.prev_index {
            root.prev_index = Some(prev);
        }

        if !ctx.garbage_cids.is_empty() {
            let garbage_strings: Vec<String> = ctx
                .garbage_cids
                .iter()
                .map(std::string::ToString::to_string)
                .collect();
            let cid =
                gc::write_garbage_record(content_store, ledger_id, root.index_t, garbage_strings)
                    .await
                    .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            root.garbage = Some(BinaryGarbageRef { id: cid });

            tracing::info!(
                garbage_count = ctx.garbage_cids.len(),
                "GC chain: garbage record written"
            );
        }
    }

    tracing::info!(
        index_t = root.index_t,
        default_orders = root.default_graph_orders.len(),
        named_graphs = root.named_graphs.len(),
        "encoding and writing FIR6 root to CAS"
    );

    // Encode and write root.
    let root_bytes = root.encode();
    let root_id = content_store
        .put(ContentKind::IndexRoot, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    tracing::info!(
        %root_id,
        index_t = root.index_t,
        root_bytes = root_bytes.len(),
        "index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: ledger_id.to_string(),
        stats: IndexStats {
            total_bytes: root_bytes.len(),
            ..result_stats
        },
        // Outer entry point fills fuel from the tracker tally.
        fuel: None,
    })
}

/// Load the previous index root and return its sealed annotation arena, if
/// any. Best-effort: any load/decode failure degrades to `None` (the caller
/// stays in scan-fallback rather than failing the rebuild).
async fn load_prev_annotation_index(
    content_store: &dyn fluree_db_core::storage::ContentStore,
    prev_root_id: &ContentId,
) -> Option<fluree_db_core::AnnotationIndexRoot> {
    let prev_bytes = content_store.get(prev_root_id).await.ok()?;
    let prev_root = IndexRoot::decode(&prev_bytes).ok()?;
    prev_root.annotation_index
}

/// Compute garbage CIDs by diffing the previous root's reachable CAS set
/// against the new root's reachable CAS set.
///
/// "Reachable" includes leaves behind named-graph and annotation branch
/// manifests via `collect_root_cas_ids_expanded`. Diffing only the direct
/// CAS refs (`all_cas_ids()`) would silently leak those leaves on every
/// reindex.
/// Note the ORDER of concerns: the `prev_index` link is established as soon as the
/// previous root decodes, and the garbage diff is best-effort ON TOP of it. That
/// separation is deliberate and load-bearing.
///
/// The two are independent facts — the link says "this version follows that one",
/// the manifest says "these blobs were replaced" — and conflating them severs
/// chains. An earlier shape returned `None` if either CAS expansion failed, which
/// would publish a root with NO `prev_index`, silently starting a fresh lineage and
/// making every earlier version unreachable. Unreachable means GC cannot see it,
/// and nothing else reclaims by reachability, so that space is gone for good.
///
/// Measured on a live ledger while the rebuild path passed no chain context at all:
/// 226 roots on disk, **15 reachable**, 211 orphaned — with 0 fork points and 211
/// distinct `index_t` (so not a race), traced to two roots carrying no `prev_index`
/// at `index_t` 6739 and 8627, each severing everything below it.
///
/// Failing to compute garbage costs some unreleased blobs, which a later sweep can
/// still find. Failing to link the chain costs the entire history, permanently.
/// Never trade the second for the first.
pub(crate) async fn compute_garbage_from_prev_root(
    content_store: &dyn fluree_db_core::storage::ContentStore,
    new_root: &IndexRoot,
    prev_root_id: &ContentId,
) -> Option<GarbageContext> {
    // The only unrecoverable case: without decoding the previous root there is no
    // `t` for the link, so there is nothing well-formed to point at.
    let prev_bytes = content_store.get(prev_root_id).await.ok()?;
    let prev_root = IndexRoot::decode(&prev_bytes).ok()?;

    let prev_index = Some(BinaryPrevIndexRef {
        t: prev_root.index_t,
        id: prev_root_id.clone(),
    });

    // Strict expansion: a partial new-root set would misclassify still-reachable
    // leaves as garbage; a partial prev-root set would leave replaced blobs
    // unreleased. On failure record NO garbage but KEEP the link.
    let diff = async {
        let old_ids =
            fluree_db_binary_index::collect_root_cas_ids_expanded(content_store, &prev_root)
                .await
                .ok()?;
        let new_ids =
            fluree_db_binary_index::collect_root_cas_ids_expanded(content_store, new_root)
                .await
                .ok()?;
        Some(
            old_ids
                .difference(&new_ids)
                .cloned()
                .collect::<Vec<ContentId>>(),
        )
    }
    .await;

    let garbage_cids = diff.unwrap_or_else(|| {
        tracing::warn!(
            prev_root_id = %prev_root_id,
            prev_t = prev_root.index_t,
            new_t = new_root.index_t,
            "Could not expand CAS sets to diff garbage; publishing with the \
             prev_index link and an EMPTY garbage manifest. Replaced blobs persist \
             until an orphan sweep finds them, which is recoverable — dropping the \
             link would not be."
        );
        Vec::new()
    });

    Some(GarbageContext {
        garbage_cids,
        prev_index,
    })
}

// ============================================================================
// V6 (FIR6) root assembly
// ============================================================================

/// Inputs for assembling a V6 (FIR6) index root.
///
/// Collects all the pieces produced by the build pipeline (dicts, V3 indexes,
/// namespace codes, predicate SIDs) into a single struct for the root encoder.
pub(crate) struct Fir6Inputs {
    pub ledger_id: String,
    pub index_t: i64,
    pub namespace_codes: BTreeMap<u16, String>,
    /// Commit-derived namespace table for index-root/commit-chain namespace reconciliation.
    /// `encode_and_write_root_v6` validates that the index root's `namespace_codes`
    /// matches this table entry-by-entry. A mismatch indicates an indexer/publisher bug.
    pub commit_derived_ns: std::collections::HashMap<u16, String>,
    /// Ledger-fixed split mode — persisted in the index root.
    pub ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode,
    pub predicate_sids: Vec<(u16, String)>,
    pub uploaded_dicts: UploadedDicts,
    pub v3_uploaded: UploadedIndexes,
    pub graph_arenas: Vec<GraphArenaRefs>,
    pub datatype_iris: Vec<String>,
    pub language_tags: Vec<String>,
    pub total_commit_size: u64,
    pub total_asserts: u64,
    pub total_retracts: u64,
    /// Full query-time stats (HLL-derived cardinalities, per-graph properties).
    /// `None` if stats collection was skipped or deferred.
    pub db_stats: Option<fluree_db_core::index_stats::IndexStats>,
    /// Schema hierarchy (rdfs:subClassOf / rdfs:subPropertyOf).
    pub db_schema: Option<fluree_db_core::IndexSchema>,
    /// CAS reference for the serialized HLL sketch blob.
    pub sketch_ref: Option<ContentId>,
    /// Edge-annotation event coverage envelope (M2b slice 3g).
    ///
    /// Routed from `IndexerConfig.attachment_events` through
    /// `rebuild.rs`. Same coverage semantics as the incremental
    /// path, but without a base arena to merge against:
    ///
    /// - `Authoritative(events)` — caller guarantees full history;
    ///   seal the arena from this set.
    /// - `Augment(events)` / `Unknown` / `None` — we can't prove
    ///   completeness without a base arena (the rebuild path
    ///   doesn't yet collect events from the resolver). Stay in
    ///   scan-fallback (`annotation_index = None`) until an
    ///   explicitly `Authoritative` source is supplied — i.e. a
    ///   future rebuild whose caller passes the full event set,
    ///   or an incremental pass that can prove its overlay
    ///   coverage is authoritative. Augment alone cannot reseal
    ///   from this state because the indexer has no way to
    ///   recover the missing history.
    pub attachment_events: Option<crate::config::AttachmentEventCoverage>,
    /// The previous index root's CID (`NsRecord.index_head_id`), when one
    /// exists. Lets the `Augment` arena arm recover the base arena's event
    /// history from the prior root — without it a full rebuild under
    /// `Augment` coverage silently drops a previously-sealed arena.
    pub prev_index_root_id: Option<ContentId>,
}

/// Encode an `IndexRoot` (FIR6), write to CAS, and return an `IndexResult`.
///
/// This is the V3 equivalent of the V5 root assembly. It constructs the
/// `IndexRoot`, encodes it, writes to CAS with `ContentKind::IndexRoot`,
/// and derives the CID.
///
/// Takes `prev_root_id` — the ledger's CURRENT published index head, if it has one
/// — and derives the GC chain itself, rather than accepting a pre-built
/// `GarbageContext`. That is deliberate: the previous shape let a caller pass
/// `None` and silently publish a root with no `prev_index`, which is exactly what
/// happened. `rebuild.rs` passed `None, // GC chain deferred for V3 milestone`, so
/// **every full rebuild severed the chain** and orphaned the whole prior history —
/// 226 roots on a live ledger with only 15 reachable. Deriving it here means the
/// link cannot be forgotten, only genuinely absent.
///
/// Pass `None` **only** when the ledger truly has no prior index (genesis).
pub(crate) async fn encode_and_write_root_v6(
    content_store: &dyn ContentStore,
    inputs: Fir6Inputs,
    prev_root_id: Option<ContentId>,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    reconcile_ns_at_publish(
        &inputs.namespace_codes,
        &inputs.commit_derived_ns,
        inputs.index_t,
    )?;

    // Convert DictRefs for root assembly.
    let dr = inputs.uploaded_dicts.dict_refs;
    let dict_refs = DictRefs {
        forward_packs: dr.forward_packs,
        subject_reverse: dr.subject_reverse,
        string_reverse: dr.string_reverse,
    };

    // Build default_graph_orders from V3 upload result.
    let default_graph_orders: Vec<DefaultGraphOrder> = inputs
        .v3_uploaded
        .default_graph_orders
        .into_iter()
        .map(|(order, leaves)| DefaultGraphOrder { order, leaves })
        .collect();

    // Custom datatype IRIs (non-reserved only, for o_type table).
    let custom_dt_iris: Vec<String> = inputs
        .datatype_iris
        .iter()
        .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
        .cloned()
        .collect();

    // Sticky bit: `true` once any `f:reifies*` predicate has been
    // observed in the ledger's history. Detection is cheap — if any
    // of the seven reserved reifies SIDs appears in the indexer's
    // accumulated predicate dictionary, annotations exist (or did).
    // Once a predicate enters the dict it stays there across
    // reindexes, so this naturally inherits sticky-bit semantics.
    let has_annotations = inputs.predicate_sids.iter().any(|(ns, name)| {
        fluree_db_core::is_reserved_reifies_predicate(&fluree_db_core::Sid::new(*ns, name.as_str()))
    });

    let mut root = IndexRoot {
        ledger_id: inputs.ledger_id.clone(),
        index_t: inputs.index_t,
        base_t: 0,
        subject_id_encoding: inputs.uploaded_dicts.subject_id_encoding,
        namespace_codes: inputs.namespace_codes,
        predicate_sids: inputs.predicate_sids,
        ns_split_mode: inputs.ns_split_mode,
        graph_iris: inputs.uploaded_dicts.graph_iris,
        datatype_iris: inputs.datatype_iris,
        language_tags: inputs.language_tags.clone(),
        dict_refs,
        subject_watermarks: inputs.uploaded_dicts.subject_watermarks,
        string_watermark: inputs.uploaded_dicts.string_watermark,
        lex_sorted_string_ids: false,
        total_commit_size: inputs.total_commit_size,
        total_asserts: inputs.total_asserts,
        total_retracts: inputs.total_retracts,
        graph_arenas: inputs.graph_arenas,
        o_type_table: IndexRoot::build_o_type_table(&custom_dt_iris, &inputs.language_tags),
        default_graph_orders,
        named_graphs: inputs.v3_uploaded.named_graphs,
        stats: inputs.db_stats,
        schema: inputs.db_schema,
        prev_index: None,
        garbage: None,
        sketch_ref: inputs.sketch_ref,
        has_annotations,
        annotation_index: None,
        // Sticky bit flipped to `true` below if the rebuild path
        // seals an `Authoritative` arena. Rebuilds always start
        // from scratch with no prior root, so this is the only
        // signal carried forward — defensive-drop semantics live
        // exclusively on the incremental path.
        had_annotation_arena: false,
    };

    // `IndexStats.size` is defined as total commit data size (bytes) for the ledger.
    // The root carries this as `total_commit_size`; ensure stats reflect it.
    if let Some(stats) = root.stats.as_mut() {
        stats.distribute_total_size_by_flakes(root.total_commit_size);
    }

    // ---- Annotation arena seal (M2b slice 3g, full-rebuild path) ----
    //
    // Same coverage envelope as the incremental Phase 3d, but
    // without a previous arena to merge against (full rebuild
    // starts from scratch). Decision matrix:
    //
    //   Authoritative(events) → caller asserts complete history;
    //                            seal authoritative arena.
    //   Augment(events)       → caller has events but can't prove
    //                            completeness; without a base arena
    //                            to merge with, we have no way to
    //                            recover historical attachments
    //                            beyond the supplied events. Stay
    //                            in scan-fallback (annotation_index
    //                            = None) until an explicitly
    //                            `Authoritative` source is provided
    //                            (a future rebuild whose caller
    //                            passes the full event set, or
    //                            resolver-side event collection
    //                            that lets the indexer produce its
    //                            own complete history). Augment
    //                            alone cannot reseal from this
    //                            state — the indexer cannot
    //                            reconstruct the missing history.
    //   Unknown / None        → no caller events; no-op.
    //
    // Events clipped to t <= inputs.index_t for the same reason as
    // incremental: keeps `AnnotationIndexRoot.max_t <=
    // IndexRoot.index_t`.
    use crate::config::AttachmentEventCoverage;
    let job_t = inputs.index_t;
    match inputs.attachment_events {
        Some(AttachmentEventCoverage::Authoritative(mut events)) => {
            events.retain(|(_, _, t, _)| *t <= job_t);
            let result = crate::build::annotation_arena::build_and_persist_annotation_arena(
                content_store,
                None,
                events,
            )
            .await?;
            if let Some(ref ann) = result.new_index {
                debug_assert!(
                    ann.max_t <= job_t,
                    "AnnotationIndexRoot.max_t ({}) must not exceed IndexRoot.index_t ({})",
                    ann.max_t,
                    job_t
                );
            }
            if result.new_index.is_some() {
                // Sticky flag: an arena was sealed at this t. Even if
                // a later pass defensively drops it, this bit stays
                // true so the provider's bootstrap scan-fallback is
                // suppressed.
                root.had_annotation_arena = true;
            }
            root.annotation_index = result.new_index;
            // No previous arena → no leaves to GC; replaced_leaf_cids
            // is empty by construction.
            debug_assert!(result.replaced_leaf_cids.is_empty());
        }
        Some(AttachmentEventCoverage::Augment(events)) => {
            // A full rebuild starts from scratch, but the *previous root*
            // may carry a sealed arena whose event history plus the
            // Augment delta is complete — the same merge contract the
            // incremental Phase 3d applies. Without recovering it here, a
            // full reindex under `Augment` coverage silently drops a
            // previously-sealed arena (and the sticky bit then blocks the
            // bootstrap scan from ever resealing).
            let prev_arena = match inputs.prev_index_root_id.as_ref() {
                Some(prev_id) => load_prev_annotation_index(content_store, prev_id).await,
                None => None,
            };
            match prev_arena {
                Some(prev) => {
                    let reader =
                        fluree_db_binary_index::annotation_arena::AnnotationArenaReader::new(
                            &prev,
                            content_store,
                        );
                    match reader.collect_all_forward_events().await {
                        Ok(mut merged) => {
                            merged.extend(events);
                            // Full-tuple sort + dedup: an event indexed by a
                            // prior pass may also still sit in the running
                            // overlay's delta.
                            merged.sort();
                            merged.dedup();
                            merged.retain(|(_, _, t, _)| *t <= job_t);
                            // `previous_index: None`: the prior arena's blobs
                            // stay reachable from the previous root and follow
                            // the same old-generation GC lifecycle as every
                            // other replaced artifact of a full rebuild.
                            // Identical re-sealed leaves re-derive the same
                            // CIDs, so nothing is duplicated.
                            let result =
                                crate::build::annotation_arena::build_and_persist_annotation_arena(
                                    content_store,
                                    None,
                                    merged,
                                )
                                .await?;
                            if let Some(ref ann) = result.new_index {
                                debug_assert!(
                                    ann.max_t <= job_t,
                                    "AnnotationIndexRoot.max_t ({}) must not exceed \
                                     IndexRoot.index_t ({})",
                                    ann.max_t,
                                    job_t
                                );
                            }
                            if result.new_index.is_some() {
                                root.had_annotation_arena = true;
                            }
                            root.annotation_index = result.new_index;
                            tracing::debug!(
                                ledger_id = %inputs.ledger_id,
                                "full-rebuild resealed annotation arena from previous \
                                 root's arena + Augment delta"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                ledger_id = %inputs.ledger_id,
                                error = %e,
                                "failed to read previous arena events for Augment \
                                 merge; leaving annotation_index=None (scan-fallback)"
                            );
                        }
                    }
                }
                None => {
                    tracing::warn!(
                        ledger_id = %inputs.ledger_id,
                        "full-rebuild path received Augment coverage but has no \
                         base arena to merge with; cannot prove history \
                         completeness. Leaving annotation_index=None — the next \
                         incremental pass with running overlay coverage will \
                         seal an authoritative arena."
                    );
                }
            }
        }
        Some(AttachmentEventCoverage::Unknown) | None => {
            // Non-annotation ledger fast path or scan-fallback state.
        }
    }

    // Sticky-bit coercion: see the canonical contract on
    // `IndexRoot.had_annotation_arena` in
    // `fluree-db-binary-index/src/format/index_root.rs`. Mirrors
    // `IncrementalRootBuilder::build()` — every indexer pass on
    // an annotation-bearing ledger represents history the indexer
    // owns; the provider must not later reconstruct a live-only
    // `Authoritative` arena from such a root. Bulk import is the
    // only path that leaves the bit false.
    if root.has_annotations {
        root.had_annotation_arena = true;
    }

    // Derive the GC chain from the previous head, then attach it.
    //
    // Done HERE rather than taken from the caller so that publishing a root without
    // a `prev_index` requires there to be no previous head — it can no longer
    // happen because a call site omitted it.
    let gc_ctx = match prev_root_id.as_ref() {
        Some(prev) => {
            let ctx = compute_garbage_from_prev_root(content_store, &root, prev).await;
            if ctx.is_none() {
                // The head exists but would not decode. Publishing anyway severs the
                // chain and orphans everything behind it permanently, so say so
                // loudly rather than leaving it to be discovered as lost disk.
                tracing::warn!(
                    prev_root_id = %prev,
                    index_t = inputs.index_t,
                    ledger_id = %inputs.ledger_id,
                    "Previous index head could not be read; publishing WITHOUT a \
                     prev_index link. Every earlier index version becomes \
                     unreachable and unreclaimable — expect an orphan-sweep report \
                     to grow by roughly one version's artifacts."
                );
            }
            ctx
        }
        None => None,
    };

    if let Some(ctx) = gc_ctx {
        if let Some(prev) = ctx.prev_index {
            root.prev_index = Some(prev);
        }

        if !ctx.garbage_cids.is_empty() {
            let garbage_strings: Vec<String> = ctx
                .garbage_cids
                .iter()
                .map(std::string::ToString::to_string)
                .collect();
            let cid = gc::write_garbage_record(
                content_store,
                &inputs.ledger_id,
                inputs.index_t,
                garbage_strings,
            )
            .await
            .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
            root.garbage = Some(BinaryGarbageRef { id: cid });

            tracing::info!(
                garbage_count = ctx.garbage_cids.len(),
                "GC chain: garbage record written"
            );
        }
    }

    tracing::info!(
        index_t = root.index_t,
        o_type_entries = root.o_type_table.len(),
        default_orders = root.default_graph_orders.len(),
        named_graphs = root.named_graphs.len(),
        "encoding and writing FIR6 root to CAS"
    );

    // Encode and write root.
    let root_bytes = root.encode();
    let root_id = content_store
        .put(ContentKind::IndexRoot, &root_bytes)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;

    tracing::info!(
        %root_id,
        index_t = root.index_t,
        root_bytes = root_bytes.len(),
        "FIR6 index root published"
    );

    Ok(IndexResult {
        root_id,
        index_t: root.index_t,
        ledger_id: inputs.ledger_id,
        stats: IndexStats {
            total_bytes: root_bytes.len(),
            ..result_stats
        },
        // Outer entry point fills fuel from the tracker tally.
        fuel: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn btree(pairs: &[(u16, &str)]) -> BTreeMap<u16, String> {
        pairs.iter().map(|&(c, p)| (c, p.to_string())).collect()
    }

    fn hash(pairs: &[(u16, &str)]) -> HashMap<u16, String> {
        pairs.iter().map(|&(c, p)| (c, p.to_string())).collect()
    }

    /// A root whose reverse dict tree points at a leaf that is not in the store.
    /// Useful because it is cheap to build and needs no real index.
    fn unresolvable_root(ledger: &str, t: i64, tag: &[u8]) -> fluree_db_binary_index::IndexRoot {
        use fluree_db_binary_index::{DictPackRefs, DictTreeRefs, IndexRoot};
        use fluree_db_core::{ContentId, ContentKind};
        let tree = DictTreeRefs {
            branch: ContentId::new(ContentKind::IndexBranch, tag),
            leaves: vec![ContentId::new(ContentKind::IndexLeaf, tag)],
        };
        IndexRoot {
            ledger_id: ledger.to_string(),
            index_t: t,
            base_t: 0,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: BTreeMap::new(),
            predicate_sids: Vec::new(),
            graph_iris: Vec::new(),
            datatype_iris: Vec::new(),
            language_tags: Vec::new(),
            dict_refs: DictRefs {
                forward_packs: DictPackRefs {
                    string_fwd_packs: Vec::new(),
                    subject_fwd_ns_packs: Vec::new(),
                },
                subject_reverse: tree.clone(),
                string_reverse: tree,
            },
            subject_watermarks: Vec::new(),
            string_watermark: 0,
            lex_sorted_string_ids: false,
            total_commit_size: 0,
            total_asserts: 0,
            total_retracts: 0,
            graph_arenas: Vec::new(),
            default_graph_orders: Vec::new(),
            named_graphs: Vec::new(),
            stats: None,
            schema: None,
            prev_index: None,
            garbage: None,
            sketch_ref: None,
            has_annotations: false,
            annotation_index: None,
            had_annotation_arena: false,
            o_type_table: IndexRoot::build_o_type_table(&[], &[]),
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
        }
    }

    #[test]
    fn reconcile_ns_at_publish_matching_tables() {
        let root = btree(&[(1, "http://a.org/"), (2, "http://b.org/")]);
        let commit = hash(&[(1, "http://a.org/"), (2, "http://b.org/")]);
        reconcile_ns_at_publish(&root, &commit, 5).expect("matching tables should succeed");
    }

    #[test]
    fn reconcile_ns_at_publish_rejects_prefix_mismatch() {
        let root = btree(&[(1, "http://a.org/"), (2, "http://b.org/")]);
        let commit = hash(&[(1, "http://WRONG.org/"), (2, "http://b.org/")]);
        let err = reconcile_ns_at_publish(&root, &commit, 7).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("namespace reconciliation failure"),
            "expected reconciliation error, got: {msg}"
        );
        assert!(msg.contains("index_t=7"));
    }

    #[test]
    fn reconcile_ns_at_publish_rejects_extra_root_code() {
        let root = btree(&[
            (1, "http://a.org/"),
            (2, "http://b.org/"),
            (3, "http://c.org/"),
        ]);
        let commit = hash(&[(1, "http://a.org/"), (2, "http://b.org/")]);
        let err = reconcile_ns_at_publish(&root, &commit, 10).unwrap_err();
        assert!(err.to_string().contains("namespace reconciliation failure"));
    }

    #[test]
    fn reconcile_ns_at_publish_rejects_extra_commit_code() {
        let root = btree(&[(1, "http://a.org/")]);
        let commit = hash(&[(1, "http://a.org/"), (2, "http://b.org/")]);
        let err = reconcile_ns_at_publish(&root, &commit, 3).unwrap_err();
        assert!(err.to_string().contains("namespace reconciliation failure"));
    }

    #[test]
    fn reconcile_ns_at_publish_empty_tables_match() {
        let root = BTreeMap::new();
        let commit = HashMap::new();
        reconcile_ns_at_publish(&root, &commit, 0).expect("empty tables should match");
    }

    #[test]
    fn find_ns_mismatch_reports_prefix_difference() {
        let root = btree(&[(1, "http://a.org/")]);
        let commit = btree(&[(1, "http://b.org/")]);
        let msg = find_ns_mismatch(&root, &commit);
        assert!(
            msg.contains("code 1"),
            "should name the conflicting code: {msg}"
        );
    }

    #[test]
    fn find_ns_mismatch_reports_missing_from_root() {
        let root = BTreeMap::new();
        let commit = btree(&[(5, "http://x.org/")]);
        let msg = find_ns_mismatch(&root, &commit);
        assert!(
            msg.contains("code 5") && msg.contains("root=None"),
            "should report missing root entry: {msg}"
        );
    }

    /// A decodable previous head ALWAYS yields a `prev_index` link.
    ///
    /// This is the regression that matters. A root published without `prev_index`
    /// silently starts a fresh lineage: every earlier version becomes unreachable,
    /// and since nothing reclaims by reachability their artifacts can never be
    /// freed. Live evidence before the fix: 226 roots on one ledger, 15 reachable,
    /// 211 orphaned.
    ///
    /// The previous root here references a leaf that is not in the store — which,
    /// usefully, turned out NOT to make `collect_root_cas_ids_expanded` fail:
    /// expansion tolerates unresolvable leaves. So the "diff failed, keep the link
    /// anyway" branch is **not covered by this test**; triggering it needs a store
    /// that errors on `get`, and it is left uncovered rather than faked. What is
    /// covered is the property that actually broke production: the link is derived
    /// from the head and cannot be omitted.
    #[tokio::test]
    async fn decodable_prev_head_always_yields_a_prev_index_link() {
        use fluree_db_core::prelude::*;
        use fluree_db_core::storage::content_store_for;

        const LEDGER: &str = "chainlink:main";

        let store = content_store_for(MemoryStorage::new(), LEDGER);

        let prev = unresolvable_root(LEDGER, 10, b"prev");
        let prev_id = store
            .put(ContentKind::IndexRoot, &prev.encode())
            .await
            .expect("write prev root");

        let new_root = unresolvable_root(LEDGER, 11, b"new");

        let ctx = compute_garbage_from_prev_root(&store, &new_root, &prev_id)
            .await
            .expect("a decodable previous head must always yield a context");

        let link = ctx
            .prev_index
            .expect("prev_index MUST be set even when the garbage diff fails");
        assert_eq!(link.id, prev_id, "link must point at the previous head");
        assert_eq!(
            link.t, 10,
            "link must carry the previous index_t, or GC cannot order the chain"
        );
    }

    /// An UNREADABLE previous head yields `None` — never a bogus link.
    ///
    /// `None` here is honest: without decoding the head there is no `index_t`, so no
    /// well-formed link exists. The caller logs a loud warning in that case, because
    /// publishing anyway severs the chain. What must never happen is inventing a
    /// link, or dropping one that was available.
    #[tokio::test]
    async fn unreadable_prev_head_yields_no_link_rather_than_a_bogus_one() {
        use fluree_db_core::prelude::*;
        use fluree_db_core::storage::content_store_for;

        const LEDGER: &str = "absent:main";
        let store = content_store_for(MemoryStorage::new(), LEDGER);

        // Never written, so `get` cannot resolve it.
        let never_written = ContentId::new(ContentKind::IndexRoot, b"absent-head");
        let new_root = unresolvable_root(LEDGER, 5, b"new");

        assert!(
            compute_garbage_from_prev_root(&store, &new_root, &never_written)
                .await
                .is_none(),
            "an unreadable previous head must yield None"
        );
    }
}
