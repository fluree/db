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

/// Write `garbage_cids` as this root's garbage manifest and attach it.
///
/// Always writes, even when the set is empty: an empty manifest records that
/// the root replaced nothing, which a root with no manifest cannot express.
/// The collector stops its oldest-first walk at the first absent manifest, so
/// a publisher that skipped the write would strand every newer version.
pub(crate) async fn attach_garbage_manifest(
    content_store: &dyn ContentStore,
    root: &mut IndexRoot,
    ledger_id: &str,
    garbage_cids: &[ContentId],
) -> Result<()> {
    let garbage_strings: Vec<String> = garbage_cids
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    let cid = gc::write_garbage_record(content_store, ledger_id, root.index_t, garbage_strings)
        .await
        .map_err(|e| IndexerError::StorageWrite(e.to_string()))?;
    root.garbage = Some(BinaryGarbageRef { id: cid });

    tracing::info!(
        garbage_count = garbage_cids.len(),
        "GC chain: garbage record written"
    );
    Ok(())
}

/// Load and decode the previous index root.
///
/// Best-effort: any load/decode failure degrades to `None`. The annotation
/// arm then stays in scan-fallback, and the garbage manifest is omitted
/// rather than written empty, instead of failing the rebuild.
async fn load_prev_root(
    content_store: &dyn ContentStore,
    prev_root_id: &ContentId,
) -> Option<IndexRoot> {
    let prev_bytes = content_store.get(prev_root_id).await.ok()?;
    IndexRoot::decode(&prev_bytes).ok()
}

/// The CIDs `new_root` supersedes: everything the prior root reached that
/// this one no longer does.
///
/// "Reachable" includes leaves behind named-graph and annotation branch
/// manifests via `collect_root_cas_ids_expanded`. Diffing only the direct
/// CAS refs (`all_cas_ids()`) would silently leak those leaves on every
/// reindex.
///
/// Returns `None` when either root cannot be read or expanded. Callers must
/// keep that distinct from an empty set: "superseded nothing" and "could not
/// determine" are different claims, and recording the first after a full
/// rebuild would let GC release the prior root while leaving behind every
/// blob it referenced. The collector defers an absent manifest to the sweep.
async fn superseded_cids(
    content_store: &dyn ContentStore,
    new_root: &IndexRoot,
    prev_root: &IndexRoot,
) -> Option<Vec<ContentId>> {
    // Strict expansion: a partial new-root set would misclassify
    // still-reachable leaves as garbage; a partial prev-root set would
    // leave replaced blobs unreleased. Either way silently — so a failure
    // here means publishing no manifest rather than a corrupt one.
    let old_ids = fluree_db_binary_index::collect_root_cas_ids_expanded(content_store, prev_root)
        .await
        .ok()?;
    let new_ids = fluree_db_binary_index::collect_root_cas_ids_expanded(content_store, new_root)
        .await
        .ok()?;
    Some(old_ids.difference(&new_ids).cloned().collect())
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
    /// The index version this root supersedes — the prior head root's CID and
    /// `index_t` (`NsRecord`'s `index_head_id` and `index_t`) — when one
    /// exists.
    ///
    /// Serves two purposes. It becomes the published root's `prev_index` link,
    /// which GC and drop walk to enumerate superseded artifacts. It also lets
    /// the `Augment` arena arm recover the base arena's event history from the
    /// prior root — without it a full rebuild under `Augment` coverage
    /// silently drops a previously-sealed arena.
    pub prev_index: Option<BinaryPrevIndexRef>,
}

/// Encode an `IndexRoot` (FIR6), write to CAS, and return an `IndexResult`.
///
/// This is the V3 equivalent of the V5 root assembly. It constructs the
/// `IndexRoot`, encodes it, writes to CAS with `ContentKind::IndexRoot`,
/// and derives the CID.
///
/// The published root links [`Fir6Inputs::prev_index`] and carries a garbage
/// manifest naming what that version superseded, so it participates in the GC
/// chain like any incremental build.
pub(crate) async fn encode_and_write_root_v6(
    content_store: &dyn ContentStore,
    inputs: Fir6Inputs,
    result_stats: IndexStats,
) -> Result<IndexResult> {
    reconcile_ns_at_publish(
        &inputs.namespace_codes,
        &inputs.commit_derived_ns,
        inputs.index_t,
    )?;

    // Loaded once and shared: the annotation arm reads its sealed arena and
    // the garbage manifest diffs against its reachable set.
    let prev_root = match inputs.prev_index.as_ref() {
        Some(prev) => load_prev_root(content_store, &prev.id).await,
        None => None,
    };

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
            let prev_arena = prev_root
                .as_ref()
                .and_then(|prev| prev.annotation_index.clone());
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

    // GC and drop both enumerate superseded artifacts by walking the
    // prev-index chain, so a root published without this link orphans every
    // earlier version and the blobs only those versions reference.
    root.prev_index = inputs.prev_index.clone();

    // A rebuild replaces the whole prior index, so no upstream stage can name
    // what it superseded; diff it here, where the assembled root is available.
    //
    // "No prior index" and "prior index unreadable" must stay distinct. The
    // first genuinely supersedes nothing, so an empty manifest is accurate.
    // The second is unknown, and recording it as empty would let GC release
    // the prior root while leaving behind every blob it referenced.
    let garbage_cids = match (inputs.prev_index.as_ref(), prev_root.as_ref()) {
        (None, _) => Some(Vec::new()),
        (Some(_), Some(prev)) => superseded_cids(content_store, &root, prev).await,
        (Some(_), None) => None,
    };

    match garbage_cids {
        Some(cids) => {
            attach_garbage_manifest(content_store, &mut root, &inputs.ledger_id, &cids).await?;
        }
        None => tracing::warn!(
            index_t = root.index_t,
            "could not determine which artifacts the prior root superseded; \
             publishing without a garbage manifest"
        ),
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
}
