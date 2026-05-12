//! Expanded CAS reachability for an `IndexRoot`.
//!
//! `IndexRoot::all_cas_ids()` returns only the CIDs the root references
//! directly. Several use cases — drop / unpin, pack / branch transfer,
//! garbage-record diff production — also need the CIDs sitting behind
//! the root's branch manifests:
//!
//! - **Named-graph branches** (`FBR3`) routing to leaf + sidecar CIDs.
//! - **Annotation forward / reverse branches** (`EAFB1` / `EARB1`)
//!   routing to annotation leaf CIDs.
//!
//! This module owns the single async expansion path so callers stay in
//! lockstep when new branch-shaped artifacts are added to the root.
//!
//! ## Strict vs tolerant
//!
//! Two entry points with different correctness contracts:
//!
//! - [`collect_root_cas_ids_expanded`] — **strict.** Returns
//!   `Err` on the first branch read or decode failure. Use when an
//!   incomplete reachability set would corrupt the caller's invariant
//!   (pack / branch-copy: missing leaves yield a non-self-contained
//!   index snapshot; garbage-record diff: missing leaves on the *new*
//!   root would misclassify still-reachable blobs as garbage).
//!
//! - [`collect_root_cas_ids_expanded_tolerant`] — **best-effort.**
//!   Logs and skips per-branch failures, returning whatever it could
//!   collect. Use only when partial coverage is strictly safer than
//!   bailing out — e.g. `drop_ledger`'s CID-walk fallback, where
//!   skipping a leaf only means a stray pin survives, never data
//!   corruption.
//!
//! ## Use sites (must all stay in sync)
//!
//! - `fluree-db-indexer::drop::collect_index_chain_cids` — drop / unpin
//!   (tolerant).
//! - `fluree-db-indexer::build::root_assembly::compute_garbage_from_prev_root`
//!   — garbage-record diff (strict).
//! - `fluree-db-api::pack::compute_missing_index_artifacts` — pack
//!   transfer (strict).
//! - `fluree-db-api::ledger::loading::copy_index_to_branch` — branch
//!   fork (strict).

use std::collections::HashSet;

use fluree_db_core::content_id::ContentId;
use fluree_db_core::storage::ContentStore;
use fluree_db_core::{Error, Result};

use crate::annotation_arena::format::{AnnotationForwardBranch, AnnotationReverseBranch};
use crate::format::branch::read_branch_from_bytes;
use crate::format::index_root::IndexRoot;

/// Strict expansion: returns the complete reachable CAS set or an error.
///
/// Starts from `root.all_cas_ids()` and additionally fetches every
/// named-graph branch + annotation arena branch from `store`, decoding
/// each manifest to discover the leaf (and named-graph sidecar) CIDs
/// they route to. The first read or decode failure short-circuits and
/// returns `Err` — partial sets are never returned.
///
/// Does NOT include the root's own CID, the garbage manifest CID, the
/// `prev_index` link, or anything older in the chain — callers
/// composing a chain-wide set should call this for each retained root
/// and union the results.
pub async fn collect_root_cas_ids_expanded(
    store: &dyn ContentStore,
    root: &IndexRoot,
) -> Result<HashSet<ContentId>> {
    let mut ids: HashSet<ContentId> = root.all_cas_ids().into_iter().collect();

    // Named-graph branches → leaf (+ sidecar) CIDs.
    for ng in &root.named_graphs {
        for (_, branch_cid) in &ng.orders {
            let bytes = store.get(branch_cid).await.map_err(|e| {
                Error::invalid_index(format!(
                    "failed to read named-graph branch {branch_cid} during CID expansion: {e}"
                ))
            })?;
            let manifest = read_branch_from_bytes(&bytes).map_err(|e| {
                Error::invalid_index(format!(
                    "failed to decode named-graph branch {branch_cid} during CID expansion: {e}"
                ))
            })?;
            for leaf in &manifest.leaves {
                ids.insert(leaf.leaf_cid.clone());
                if let Some(ref sc) = leaf.sidecar_cid {
                    ids.insert(sc.clone());
                }
            }
        }
    }

    // Annotation arena: forward + reverse branches → leaf CIDs.
    if let Some(ref ann) = root.annotation_index {
        let fwd_bytes = store.get(&ann.forward_branch_cid).await.map_err(|e| {
            Error::invalid_index(format!(
                "failed to read annotation forward branch {} during CID expansion: {e}",
                ann.forward_branch_cid
            ))
        })?;
        let fwd_branch = AnnotationForwardBranch::decode(&fwd_bytes).map_err(|e| {
            Error::invalid_index(format!(
                "failed to decode annotation forward branch {} during CID expansion: {e}",
                ann.forward_branch_cid
            ))
        })?;
        for entry in &fwd_branch.leaves {
            ids.insert(entry.leaf_cid.clone());
        }

        let rev_bytes = store.get(&ann.reverse_branch_cid).await.map_err(|e| {
            Error::invalid_index(format!(
                "failed to read annotation reverse branch {} during CID expansion: {e}",
                ann.reverse_branch_cid
            ))
        })?;
        let rev_branch = AnnotationReverseBranch::decode(&rev_bytes).map_err(|e| {
            Error::invalid_index(format!(
                "failed to decode annotation reverse branch {} during CID expansion: {e}",
                ann.reverse_branch_cid
            ))
        })?;
        for entry in &rev_branch.leaves {
            ids.insert(entry.leaf_cid.clone());
        }
    }

    Ok(ids)
}

/// Tolerant expansion: logs and skips per-branch failures.
///
/// Returns whatever could be collected, including the root's direct
/// CAS refs even if every branch fails to expand. Suitable only for
/// best-effort cleanup paths (drop / unpin) where leaving an extra
/// blob behind is strictly safer than bailing out.
///
/// Pack / branch-copy / garbage-diff callers must use the strict
/// [`collect_root_cas_ids_expanded`] instead — silently dropping
/// reachable leaves there yields incomplete snapshots or misclassified
/// garbage.
pub async fn collect_root_cas_ids_expanded_tolerant(
    store: &dyn ContentStore,
    root: &IndexRoot,
) -> HashSet<ContentId> {
    let mut ids: HashSet<ContentId> = root.all_cas_ids().into_iter().collect();

    for ng in &root.named_graphs {
        for (_, branch_cid) in &ng.orders {
            match store.get(branch_cid).await {
                Ok(bytes) => match read_branch_from_bytes(&bytes) {
                    Ok(manifest) => {
                        for leaf in &manifest.leaves {
                            ids.insert(leaf.leaf_cid.clone());
                            if let Some(ref sc) = leaf.sidecar_cid {
                                ids.insert(sc.clone());
                            }
                        }
                    }
                    Err(e) => tracing::warn!(
                        branch_cid = %branch_cid,
                        error = %e,
                        "failed to decode named-graph branch during CID expansion, skipping"
                    ),
                },
                Err(e) => tracing::warn!(
                    branch_cid = %branch_cid,
                    error = %e,
                    "failed to read named-graph branch during CID expansion, skipping"
                ),
            }
        }
    }

    if let Some(ref ann) = root.annotation_index {
        match store.get(&ann.forward_branch_cid).await {
            Ok(bytes) => match AnnotationForwardBranch::decode(&bytes) {
                Ok(branch) => {
                    for entry in &branch.leaves {
                        ids.insert(entry.leaf_cid.clone());
                    }
                }
                Err(e) => tracing::warn!(
                    branch_cid = %ann.forward_branch_cid,
                    error = %e,
                    "failed to decode annotation forward branch during CID expansion, skipping"
                ),
            },
            Err(e) => tracing::warn!(
                branch_cid = %ann.forward_branch_cid,
                error = %e,
                "failed to read annotation forward branch during CID expansion, skipping"
            ),
        }

        match store.get(&ann.reverse_branch_cid).await {
            Ok(bytes) => match AnnotationReverseBranch::decode(&bytes) {
                Ok(branch) => {
                    for entry in &branch.leaves {
                        ids.insert(entry.leaf_cid.clone());
                    }
                }
                Err(e) => tracing::warn!(
                    branch_cid = %ann.reverse_branch_cid,
                    error = %e,
                    "failed to decode annotation reverse branch during CID expansion, skipping"
                ),
            },
            Err(e) => tracing::warn!(
                branch_cid = %ann.reverse_branch_cid,
                error = %e,
                "failed to read annotation reverse branch during CID expansion, skipping"
            ),
        }
    }

    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::annotation_arena::format::{
        AnnotationForwardBranch, AnnotationForwardBranchEntry, AnnotationForwardLeaf,
        AnnotationReverseBranch, AnnotationReverseBranchEntry, AnnotationReverseLeaf,
    };
    use crate::format::wire_helpers::{DictPackRefs, DictRefs, DictTreeRefs};
    use fluree_db_core::storage::MemoryContentStore;
    use fluree_db_core::{
        AnnotationIndexRoot, AnnotationStats, ContentKind, EdgeKey, FlakeValue, Sid,
    };
    use std::collections::BTreeMap;

    fn cid(kind: ContentKind, seed: &[u8]) -> ContentId {
        ContentId::new(kind, seed)
    }

    fn sample_edge() -> EdgeKey {
        EdgeKey {
            g: None,
            s: Sid::new(1, "s"),
            p: Sid::new(1, "p"),
            o: FlakeValue::Ref(Sid::new(1, "o")),
            dt: Sid::new(0, "http://www.w3.org/2001/XMLSchema#anyURI"),
            lang: None,
            list_i: None,
        }
    }

    fn minimal_root() -> IndexRoot {
        let dummy_cid = ContentId::new(ContentKind::IndexLeaf, b"dummy");
        let dummy_tree = DictTreeRefs {
            branch: dummy_cid.clone(),
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
                    string_fwd_packs: Vec::new(),
                    subject_fwd_ns_packs: Vec::new(),
                },
                subject_reverse: dummy_tree.clone(),
                string_reverse: dummy_tree,
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

    /// Build a root carrying an annotation arena that points to two real
    /// branch blobs (forward + reverse) each routing to a single leaf.
    /// Returns (root, fwd_leaf_cid, rev_leaf_cid).
    async fn build_root_with_arena(
        store: &MemoryContentStore,
    ) -> (IndexRoot, ContentId, ContentId) {
        // Write empty leaves to CAS so the branch entries point somewhere
        // real. Their content doesn't matter — the helper only walks
        // branches, not leaves.
        let fwd_leaf_bytes = AnnotationForwardLeaf::default().encode();
        let fwd_leaf_cid = store
            .put(ContentKind::AnnotationForwardLeaf, &fwd_leaf_bytes)
            .await
            .unwrap();

        let rev_leaf_bytes = AnnotationReverseLeaf::default().encode();
        let rev_leaf_cid = store
            .put(ContentKind::AnnotationReverseLeaf, &rev_leaf_bytes)
            .await
            .unwrap();

        let fwd_branch = AnnotationForwardBranch {
            leaves: vec![AnnotationForwardBranchEntry {
                first_edge: sample_edge(),
                first_ann: Sid::new(2, "a"),
                last_edge: sample_edge(),
                last_ann: Sid::new(2, "a"),
                row_count: 0,
                leaf_cid: fwd_leaf_cid.clone(),
            }],
        };
        let fwd_branch_cid = store
            .put(ContentKind::AnnotationForwardBranch, &fwd_branch.encode())
            .await
            .unwrap();

        let rev_branch = AnnotationReverseBranch {
            leaves: vec![AnnotationReverseBranchEntry {
                first_ann: Sid::new(2, "a"),
                first_edge: sample_edge(),
                last_ann: Sid::new(2, "a"),
                last_edge: sample_edge(),
                row_count: 0,
                leaf_cid: rev_leaf_cid.clone(),
            }],
        };
        let rev_branch_cid = store
            .put(ContentKind::AnnotationReverseBranch, &rev_branch.encode())
            .await
            .unwrap();

        let mut root = minimal_root();
        root.has_annotations = true;
        root.annotation_index = Some(AnnotationIndexRoot {
            version: 1,
            max_t: 0,
            forward_branch_cid: fwd_branch_cid,
            reverse_branch_cid: rev_branch_cid,
            stats: AnnotationStats::default(),
        });
        (root, fwd_leaf_cid, rev_leaf_cid)
    }

    #[tokio::test]
    async fn expands_annotation_branches_to_leaves() {
        let store = MemoryContentStore::new();
        let (root, fwd_leaf_cid, rev_leaf_cid) = build_root_with_arena(&store).await;

        let ids = collect_root_cas_ids_expanded(&store, &root).await.unwrap();

        // Annotation branch CIDs appear via all_cas_ids().
        let ann = root.annotation_index.as_ref().unwrap();
        assert!(
            ids.contains(&ann.forward_branch_cid),
            "annotation forward branch CID missing"
        );
        assert!(
            ids.contains(&ann.reverse_branch_cid),
            "annotation reverse branch CID missing"
        );
        // Leaves added by branch expansion.
        assert!(
            ids.contains(&fwd_leaf_cid),
            "forward leaf CID missing — annotation branch was not expanded"
        );
        assert!(
            ids.contains(&rev_leaf_cid),
            "reverse leaf CID missing — annotation branch was not expanded"
        );
    }

    #[tokio::test]
    async fn strict_errors_on_missing_annotation_branch() {
        let store = MemoryContentStore::new();
        let mut root = minimal_root();
        root.has_annotations = true;
        root.annotation_index = Some(AnnotationIndexRoot {
            version: 1,
            max_t: 0,
            forward_branch_cid: cid(ContentKind::AnnotationForwardBranch, b"missing-fwd"),
            reverse_branch_cid: cid(ContentKind::AnnotationReverseBranch, b"missing-rev"),
            stats: AnnotationStats::default(),
        });

        // Strict: must surface the read failure rather than return a
        // partial set that pack / GC-diff would treat as authoritative.
        let err = collect_root_cas_ids_expanded(&store, &root)
            .await
            .expect_err("strict mode should error on missing branch");
        assert!(
            err.to_string().contains("annotation forward branch"),
            "error should identify the missing branch: {err}"
        );
    }

    #[tokio::test]
    async fn tolerant_expansion_swallows_missing_annotation_branch() {
        let store = MemoryContentStore::new();
        let mut root = minimal_root();
        root.has_annotations = true;
        root.annotation_index = Some(AnnotationIndexRoot {
            version: 1,
            max_t: 0,
            forward_branch_cid: cid(ContentKind::AnnotationForwardBranch, b"missing-fwd"),
            reverse_branch_cid: cid(ContentKind::AnnotationReverseBranch, b"missing-rev"),
            stats: AnnotationStats::default(),
        });

        let ids = collect_root_cas_ids_expanded_tolerant(&store, &root).await;
        // Still contains the direct branch CIDs from all_cas_ids().
        let ann = root.annotation_index.as_ref().unwrap();
        assert!(ids.contains(&ann.forward_branch_cid));
        assert!(ids.contains(&ann.reverse_branch_cid));
    }

    #[tokio::test]
    async fn diff_produces_replaced_annotation_leaves() {
        let store = MemoryContentStore::new();
        let (prev_root, prev_fwd_leaf, prev_rev_leaf) = build_root_with_arena(&store).await;
        let (new_root, new_fwd_leaf, new_rev_leaf) = build_root_with_arena(&store).await;

        // Leaves are content-addressed empty blobs, so the new and prev
        // build pull the *same* leaf CIDs from CAS — assert that and
        // then build a synthetic new root whose annotation branches are
        // genuinely fresh, to exercise the diff.
        assert_eq!(prev_fwd_leaf, new_fwd_leaf);
        assert_eq!(prev_rev_leaf, new_rev_leaf);

        // Build a "new" root by writing distinct leaf bytes so leaf CIDs differ.
        let fwd_leaf2 = AnnotationForwardLeaf {
            rows: vec![crate::annotation_arena::format::AnnotationForwardRow {
                edge: sample_edge(),
                ann: Sid::new(2, "a"),
                t: 1,
                op: true,
            }],
        };
        let fwd_leaf2_cid = store
            .put(ContentKind::AnnotationForwardLeaf, &fwd_leaf2.encode())
            .await
            .unwrap();
        let rev_leaf2 = AnnotationReverseLeaf {
            rows: vec![crate::annotation_arena::format::AnnotationReverseRow {
                ann: Sid::new(2, "a"),
                edge: sample_edge(),
                t: 1,
                op: true,
            }],
        };
        let rev_leaf2_cid = store
            .put(ContentKind::AnnotationReverseLeaf, &rev_leaf2.encode())
            .await
            .unwrap();

        let fwd_branch2 = AnnotationForwardBranch {
            leaves: vec![AnnotationForwardBranchEntry {
                first_edge: sample_edge(),
                first_ann: Sid::new(2, "a"),
                last_edge: sample_edge(),
                last_ann: Sid::new(2, "a"),
                row_count: 1,
                leaf_cid: fwd_leaf2_cid.clone(),
            }],
        };
        let fwd_branch2_cid = store
            .put(ContentKind::AnnotationForwardBranch, &fwd_branch2.encode())
            .await
            .unwrap();
        let rev_branch2 = AnnotationReverseBranch {
            leaves: vec![AnnotationReverseBranchEntry {
                first_ann: Sid::new(2, "a"),
                first_edge: sample_edge(),
                last_ann: Sid::new(2, "a"),
                last_edge: sample_edge(),
                row_count: 1,
                leaf_cid: rev_leaf2_cid.clone(),
            }],
        };
        let rev_branch2_cid = store
            .put(ContentKind::AnnotationReverseBranch, &rev_branch2.encode())
            .await
            .unwrap();

        let mut new_root = new_root;
        new_root.annotation_index = Some(AnnotationIndexRoot {
            version: 1,
            max_t: 1,
            forward_branch_cid: fwd_branch2_cid,
            reverse_branch_cid: rev_branch2_cid,
            stats: AnnotationStats::default(),
        });

        let prev_ids = collect_root_cas_ids_expanded(&store, &prev_root)
            .await
            .unwrap();
        let new_ids = collect_root_cas_ids_expanded(&store, &new_root)
            .await
            .unwrap();

        let replaced: HashSet<_> = prev_ids.difference(&new_ids).cloned().collect();

        // The previous arena's leaves should appear in the diff. Without
        // branch expansion they would silently leak.
        assert!(
            replaced.contains(&prev_fwd_leaf),
            "prev forward leaf missing from garbage diff"
        );
        assert!(
            replaced.contains(&prev_rev_leaf),
            "prev reverse leaf missing from garbage diff"
        );
    }
}
