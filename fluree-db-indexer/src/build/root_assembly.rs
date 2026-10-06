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

/// Abort an incremental build whose forward-pack routing table is too large to
/// encode, so the caller falls back to a full rebuild instead of panicking in
/// [`IndexRoot::encode`].
///
/// Pack counts are `u16` on the wire. `encode()` panics rather than truncating
/// (a truncated table reads back cleanly having lost ID ranges), which is right
/// for paths with nowhere else to go — but it is the wrong outcome here.
/// `encode()` runs at the END of the build, after compaction has already
/// uploaded its merged packs, so a panic discards the routing-table progress
/// compaction just made and the next cycle starts from the same oversized base
/// root. With a per-cycle compaction budget, a table far enough over the cap can
/// never shrink its way back under: every cycle re-does the work, panics, and
/// leaks the packs it uploaded (their garbage record lives in the root that is
/// never published).
///
/// Returning [`IndexerError::IncrementalAbort`] instead routes into the existing
/// fallback in `index_ledger`, and a full rebuild genuinely cures this: the
/// rebuild path re-cuts packs by SIZE from `id = 0`
/// (`upload_dicts::build_string_forward_packs`), so a table of many small packs
/// accumulated over many incremental builds collapses to `bytes / pack target`.
///
/// It does NOT cure a table that is large because the DATA is large — roughly a
/// terabyte of dictionary in one stream, where a rebuild produces the same count
/// — but that needs a wider wire field, not a different build path.
pub(crate) fn ensure_pack_counts_encodable(root: &IndexRoot) -> Result<()> {
    match root.dict_refs.forward_packs.wire_count_overflow() {
        None => Ok(()),
        Some((what, len)) => Err(IndexerError::IncrementalAbort(format!(
            "{what} forward pack count {len} exceeds the u16 wire limit of {}; \
             falling back to a full rebuild, which re-cuts packs by size",
            fluree_db_binary_index::format::wire_helpers::PACK_COUNT_WIRE_MAX,
        ))),
    }
}

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
/// Best-effort: any load/decode failure degrades to `None`, and the garbage
/// manifest is omitted rather than written empty, instead of failing the
/// rebuild.
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
/// "Reachable" includes leaves behind named-graph and legacy arena branch
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
    /// Whether the resolver observed any list-carrying (`@list`) row.
    /// Full rebuilds observe every row, so the root records `Some(_)`.
    pub saw_list_meta: bool,
    /// Full query-time stats (HLL-derived cardinalities, per-graph properties).
    /// `None` if stats collection was skipped or deferred.
    pub db_stats: Option<fluree_db_core::index_stats::IndexStats>,
    /// Schema hierarchy (rdfs:subClassOf / rdfs:subPropertyOf).
    pub db_schema: Option<fluree_db_core::IndexSchema>,
    /// CAS reference for the serialized HLL sketch blob.
    pub sketch_ref: Option<ContentId>,
    /// The index version this root supersedes — the prior head root's CID and
    /// `index_t` (`NsRecord`'s `index_head_id` and `index_t`) — when one
    /// exists.
    ///
    /// It becomes the published root's `prev_index` link, which GC and drop
    /// walk to enumerate superseded artifacts.
    pub prev_index: Option<BinaryPrevIndexRef>,
    /// Triple-term dictionary interned by this build, if any reification
    /// links were synthesized.
    pub term_dict: Option<fluree_db_binary_index::TermDictRefs>,
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

    // The garbage manifest diffs against the prior root's reachable set.
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

    // Sticky bit: `true` once `rdf:reifies` or a legacy `f:reifies*`
    // predicate has been observed in the ledger's history. Detection is
    // cheap — if one appears in the indexer's accumulated predicate
    // dictionary, annotations exist (or did).
    // Once a predicate enters the dict it stays there across
    // reindexes, so this naturally inherits sticky-bit semantics.
    let has_annotations = inputs.predicate_sids.iter().any(|(ns, name)| {
        fluree_db_core::is_annotation_predicate(&fluree_db_core::Sid::new(*ns, name.as_str()))
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
        legacy_annotation_arena: None,
        term_dict: inputs.term_dict,
        has_list_meta: Some(inputs.saw_list_meta),
    };

    // `IndexStats.size` is defined as total commit data size (bytes) for the ledger.
    // The root carries this as `total_commit_size`; ensure stats reflect it.
    if let Some(stats) = root.stats.as_mut() {
        stats.distribute_total_size_by_flakes(root.total_commit_size);
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
        ledger_id: fluree_db_core::IntoLedgerId::into_ledger_id(inputs.ledger_id.as_str()),
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

    /// A root carrying `string_packs` string forward pack refs and nothing else
    /// of interest — enough to exercise the pre-encode wire-count check.
    fn root_with_string_packs(string_packs: usize) -> IndexRoot {
        use fluree_db_binary_index::format::wire_helpers::{DictPackRefs, PackBranchEntry};
        use fluree_db_binary_index::DictTreeRefs;
        use fluree_db_core::{ContentId, ContentKind};

        let cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let tree = DictTreeRefs {
            branch: cid.clone(),
            leaves: Vec::new(),
        };
        IndexRoot {
            ledger_id: "test:main".to_string(),
            index_t: 1,
            base_t: 0,
            subject_id_encoding: fluree_db_core::SubjectIdEncoding::Narrow,
            namespace_codes: BTreeMap::new(),
            predicate_sids: Vec::new(),
            graph_iris: Vec::new(),
            datatype_iris: Vec::new(),
            language_tags: Vec::new(),
            dict_refs: DictRefs {
                forward_packs: DictPackRefs {
                    string_fwd_packs: (0..string_packs)
                        .map(|i| PackBranchEntry {
                            first_id: i as u64,
                            last_id: i as u64,
                            pack_cid: cid.clone(),
                        })
                        .collect(),
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
            has_list_meta: None,
            legacy_annotation_arena: None,
            term_dict: None,
            o_type_table: IndexRoot::build_o_type_table(&[], &[]),
            ns_split_mode: fluree_db_core::ns_encoding::NsSplitMode::default(),
        }
    }

    /// An oversized routing table must abort the incremental build with the
    /// variant `index_ledger` falls back on, NOT reach `encode()`'s panic —
    /// that panic discards the compaction progress made earlier in the same
    /// build, so the ledger can never shrink its way back under the cap.
    #[test]
    fn oversized_pack_table_aborts_to_full_rebuild() {
        use fluree_db_binary_index::format::wire_helpers::PACK_COUNT_WIRE_MAX;

        // At the limit the root still encodes, so the check must not fire.
        ensure_pack_counts_encodable(&root_with_string_packs(PACK_COUNT_WIRE_MAX))
            .expect("a table at exactly the wire limit must still publish");

        let err = ensure_pack_counts_encodable(&root_with_string_packs(PACK_COUNT_WIRE_MAX + 1))
            .expect_err("one past the limit must abort");
        assert!(
            matches!(err, IndexerError::IncrementalAbort(_)),
            "must be the variant index_ledger falls back on, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("u16 wire limit"), "{msg}");
        assert!(msg.contains("full rebuild"), "names the recovery: {msg}");
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
