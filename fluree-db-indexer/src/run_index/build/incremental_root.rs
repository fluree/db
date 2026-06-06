//! Incremental root builder for V6 (FIR6) index format.
//!
//! Provides a selective mutation builder that clones an existing `IndexRoot`
//! and applies only the fields that changed during incremental indexing.
//! Tracks replaced CIDs for garbage collection.

use fluree_db_binary_index::format::branch::LeafEntry;
use fluree_db_binary_index::format::index_root::{DefaultGraphOrder, IndexRoot, NamedGraphRouting};
use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_binary_index::{BinaryGarbageRef, BinaryPrevIndexRef};
use fluree_db_binary_index::{DictRefs, GraphArenaRefs};
use fluree_db_core::ContentId;
use fluree_db_core::{IndexSchema, IndexStats};
use std::collections::BTreeMap;
use std::collections::HashSet;

/// Selective mutation builder for `IndexRoot`.
///
/// Clones an existing root and applies only the fields that changed.
/// Accumulates replaced CIDs for GC.
pub struct IncrementalRootBuilder {
    root: IndexRoot,
    replaced_cids: Vec<ContentId>,
}

impl IncrementalRootBuilder {
    /// Create a builder from an existing root (cloned).
    pub fn from_old_root(root: IndexRoot) -> Self {
        // This flag is only valid for roots produced by the bulk import pipeline.
        // Incremental dictionary updates append new string IDs above the watermark,
        // which breaks lex-order preservation. Clear on first post-import write.
        let mut root = root;
        root.lex_sorted_string_ids = false;
        Self {
            root,
            replaced_cids: Vec::new(),
        }
    }

    /// Update the index timestamp.
    pub fn set_index_t(&mut self, t: i64) {
        self.root.index_t = t;
    }

    /// Replace the default graph's leaf entries for one sort order.
    ///
    /// If an existing entry for `order` exists, it is replaced; otherwise appended.
    ///
    /// **GC note**: This does NOT track replaced CIDs. The caller must pass
    /// replaced CIDs from `BranchUpdateResult` via `add_replaced_cids()`,
    /// which knows exactly which leaves were rewritten vs carried forward.
    pub fn set_default_graph_order(&mut self, order: RunSortOrder, leaves: Vec<LeafEntry>) {
        if let Some(existing) = self
            .root
            .default_graph_orders
            .iter_mut()
            .find(|o| o.order == order)
        {
            existing.leaves = leaves;
        } else {
            self.root
                .default_graph_orders
                .push(DefaultGraphOrder { order, leaves });
        }
    }

    /// Replace a named graph's branch CID for one sort order.
    ///
    /// **GC note**: Records the old branch CID as replaced. The caller should
    /// also pass leaf-level replaced CIDs via `add_replaced_cids()`.
    pub fn set_named_graph_branch(
        &mut self,
        g_id: u16,
        order: RunSortOrder,
        branch_cid: ContentId,
    ) {
        if let Some(ng) = self.root.named_graphs.iter_mut().find(|ng| ng.g_id == g_id) {
            if let Some(entry) = ng.orders.iter_mut().find(|(o, _)| *o == order) {
                self.replaced_cids.push(entry.1.clone());
                entry.1 = branch_cid;
            } else {
                ng.orders.push((order, branch_cid));
            }
        } else {
            self.root.named_graphs.push(NamedGraphRouting {
                g_id,
                orders: vec![(order, branch_cid)],
            });
        }
    }

    /// Replace dictionary references.
    pub fn set_dict_refs(&mut self, refs: DictRefs) {
        // Record old dict CIDs as replaced **only if** they are not referenced by the new refs.
        //
        // This is critical for correctness when dictionaries are unchanged: callers may rebuild
        // `DictRefs` from the base root even when novelty has no new subjects/strings. In that
        // case, the new root still references the same dict CIDs, and GC must not delete them.
        let old_cids: HashSet<ContentId> = collect_dict_cids(&self.root.dict_refs)
            .into_iter()
            .collect();
        let new_cids: HashSet<ContentId> = collect_dict_cids(&refs).into_iter().collect();

        let mut replaced: Vec<ContentId> = old_cids.difference(&new_cids).cloned().collect();
        // Keep ordering deterministic for garbage manifest stability.
        replaced.sort_by_key(std::string::ToString::to_string);
        self.replaced_cids.extend(replaced);
        self.root.dict_refs = refs;
    }

    /// Update subject and string watermarks.
    pub fn set_watermarks(&mut self, subject_watermarks: Vec<u64>, string_watermark: u32) {
        self.root.subject_watermarks = subject_watermarks;
        self.root.string_watermark = string_watermark;
    }

    /// Update namespace codes (e.g., when new namespaces are discovered).
    pub fn set_namespace_codes(&mut self, codes: BTreeMap<u16, String>) {
        self.root.namespace_codes = codes;
    }

    /// Update inline predicate SIDs.
    ///
    /// Also OR-updates `has_annotations` whenever any of the seven
    /// reserved `f:reifies*` predicate SIDs appears in the new dict.
    /// The full-rebuild path computes the same bit at root-assembly
    /// time, but the incremental path clones the old root and would
    /// otherwise carry forward `has_annotations: false` even when the
    /// first annotation has just rolled into the indexed predicate
    /// dictionary. Sticky semantics: once flipped, stays set across
    /// reindexes (predicate dicts only accumulate).
    pub fn set_predicate_sids(&mut self, sids: Vec<(u16, String)>) {
        let saw_reifies = sids.iter().any(|(ns, name)| {
            fluree_db_core::is_reserved_reifies_predicate(&fluree_db_core::Sid::new(
                *ns,
                name.as_str(),
            ))
        });
        self.root.has_annotations |= saw_reifies;
        self.root.predicate_sids = sids;
    }

    /// Update graph IRIs.
    pub fn set_graph_iris(&mut self, iris: Vec<String>) {
        self.root.graph_iris = iris;
    }

    /// Update datatype IRIs and rebuild the o_type table.
    pub fn set_datatype_iris(&mut self, iris: Vec<String>) {
        let custom_dt_iris: Vec<String> = iris
            .iter()
            .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
            .cloned()
            .collect();
        self.root.o_type_table =
            IndexRoot::build_o_type_table(&custom_dt_iris, &self.root.language_tags);
        self.root.datatype_iris = iris;
    }

    /// Update language tags and rebuild the o_type table.
    pub fn set_language_tags(&mut self, tags: Vec<String>) {
        let custom_dt_iris: Vec<String> = self
            .root
            .datatype_iris
            .iter()
            .skip(fluree_db_core::DatatypeDictId::RESERVED_COUNT as usize)
            .cloned()
            .collect();
        self.root.o_type_table = IndexRoot::build_o_type_table(&custom_dt_iris, &tags);
        self.root.language_tags = tags;
    }

    /// Update per-graph arena references.
    pub fn set_graph_arenas(&mut self, arenas: Vec<GraphArenaRefs>) {
        self.root.graph_arenas = arenas;
    }

    /// Update index stats.
    pub fn set_stats(&mut self, stats: Option<IndexStats>) {
        self.root.stats = stats;
    }

    /// Update schema (class/property hierarchy).
    pub fn set_schema(&mut self, schema: Option<IndexSchema>) {
        self.root.schema = schema;
    }

    /// Set prev_index ref (points to old root).
    pub fn set_prev_index(&mut self, prev: Option<BinaryPrevIndexRef>) {
        self.root.prev_index = prev;
    }

    /// Set garbage ref (points to garbage manifest).
    pub fn set_garbage(&mut self, garbage: Option<BinaryGarbageRef>) {
        self.root.garbage = garbage;
    }

    /// Update HLL sketch ref.
    pub fn set_sketch_ref(&mut self, cid: Option<ContentId>) {
        self.root.sketch_ref = cid;
    }

    /// Replace the on-disk annotation arena pointer.
    ///
    /// Pass `Some(_)` after building the new arena and writing its
    /// branch + leaf blobs to CAS. The encoder enforces the truth-
    /// table invariant that any populated `annotation_index` implies
    /// `has_annotations = true` on the wire (see
    /// `fluree_db_core::annotation_index`), so callers don't need to
    /// flip the sticky bit separately.
    ///
    /// `previous_leaf_cids` must enumerate **every** leaf CID
    /// referenced by the arena currently in `root.annotation_index`.
    /// `new_leaf_cids` must enumerate every leaf CID referenced by
    /// `new_index` (empty when `new_index` is `None`).
    /// `ContentStore::release` deletes exact CIDs (not child graphs),
    /// so without these lists the old leaves leak when the new root
    /// supersedes the chain. The orchestrator computes these sets from
    /// [`PersistedArenaResult`](crate::build::annotation_arena::PersistedArenaResult);
    /// pass empty `Vec`s when there's no previous arena. Old branch
    /// CIDs are reconciled automatically from `root.annotation_index`.
    ///
    /// Old CIDs (branches + leaves) that the **new** arena still
    /// references are NOT recorded as garbage. Content-addressed
    /// storage means a re-sealed unchanged arena (e.g. the `Augment`
    /// path on a continuously-running ledger whose overlay still holds
    /// pre-index events matching the base arena) produces identical
    /// CIDs; recording them would let GC delete leaves/branches the
    /// new live root still points at. Mirrors `set_dict_refs`.
    pub fn set_annotation_index(
        &mut self,
        new_index: Option<fluree_db_core::AnnotationIndexRoot>,
        previous_leaf_cids: Vec<ContentId>,
        new_leaf_cids: Vec<ContentId>,
    ) {
        // CIDs the new arena references (branches + leaves).
        let mut new_cids: HashSet<ContentId> = HashSet::new();
        if let Some(new) = new_index.as_ref() {
            new_cids.insert(new.forward_branch_cid.clone());
            new_cids.insert(new.reverse_branch_cid.clone());
        }
        new_cids.extend(new_leaf_cids);

        // CIDs the old arena referenced (branches + leaves).
        let mut old_cids: HashSet<ContentId> = HashSet::new();
        if let Some(prev) = self.root.annotation_index.as_ref() {
            old_cids.insert(prev.forward_branch_cid.clone());
            old_cids.insert(prev.reverse_branch_cid.clone());
        }
        old_cids.extend(previous_leaf_cids);

        // Only old CIDs the new arena no longer references are garbage.
        let mut replaced: Vec<ContentId> = old_cids.difference(&new_cids).cloned().collect();
        // Keep ordering deterministic for garbage manifest stability.
        replaced.sort_by_key(std::string::ToString::to_string);
        self.replaced_cids.extend(replaced);
        // Sticky bit: flip `had_annotation_arena` to `true` the
        // moment any arena is sealed, and *never* clear it on
        // subsequent calls — including when this call sets
        // `new_index = None` (defensive drop). Without this, the
        // post-drop root would look identical to a fresh import
        // and the provider's bootstrap base-index scan-fallback
        // could resurrect a live-only `Authoritative` arena,
        // losing historical retract/reassert rows.
        if new_index.is_some() {
            self.root.had_annotation_arena = true;
        }
        self.root.annotation_index = new_index;
    }

    /// Add to cumulative commit stats.
    pub fn add_commit_stats(&mut self, size: u64, asserts: u64, retracts: u64) {
        self.root.total_commit_size += size;
        self.root.total_asserts += asserts;
        self.root.total_retracts += retracts;
    }

    /// Accumulate replaced CIDs from another phase.
    pub fn add_replaced_cids(&mut self, cids: Vec<ContentId>) {
        self.replaced_cids.extend(cids);
    }

    /// Consume the builder, returning the final root and all replaced CIDs.
    pub fn build(mut self) -> (IndexRoot, Vec<ContentId>) {
        // Sticky-bit coercion: see the canonical contract on
        // `IndexRoot.had_annotation_arena` in
        // `fluree-db-binary-index/src/format/index_root.rs`. Every
        // indexer-produced root with `has_annotations = true`
        // sets the bit so the provider's base-index scan-fallback
        // can't later resurrect a live-only `Authoritative` arena
        // from a defensive-drop or no-seal pass. Bulk import is
        // the only path that leaves the bit false (it bypasses
        // this builder entirely; see `fluree-db-api/src/import.rs`).
        if self.root.has_annotations {
            self.root.had_annotation_arena = true;
        }
        (self.root, self.replaced_cids)
    }
}

fn collect_dict_cids(refs: &DictRefs) -> Vec<ContentId> {
    let mut out: Vec<ContentId> = Vec::new();

    // Forward packs (FPK1)
    for entry in &refs.forward_packs.string_fwd_packs {
        out.push(entry.pack_cid.clone());
    }
    for (_, ns_packs) in &refs.forward_packs.subject_fwd_ns_packs {
        for entry in ns_packs {
            out.push(entry.pack_cid.clone());
        }
    }

    // Reverse trees (DTB1 + leaves)
    out.push(refs.subject_reverse.branch.clone());
    out.extend(refs.subject_reverse.leaves.iter().cloned());
    out.push(refs.string_reverse.branch.clone());
    out.extend(refs.string_reverse.leaves.iter().cloned());

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_binary_index::{DictPackRefs, DictTreeRefs};
    use fluree_db_core::{
        ns_encoding::NsSplitMode, AnnotationIndexRoot, AnnotationStats, ContentKind,
        SubjectIdEncoding,
    };

    fn cid(label: &[u8]) -> ContentId {
        ContentId::new(ContentKind::IndexLeaf, label)
    }

    fn minimal_root() -> IndexRoot {
        let dummy = DictTreeRefs {
            branch: cid(b"dummy"),
            leaves: Vec::new(),
        };
        IndexRoot {
            ledger_id: "test".to_string(),
            index_t: 0,
            base_t: 0,
            subject_id_encoding: SubjectIdEncoding::Narrow,
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
                subject_reverse: dummy.clone(),
                string_reverse: dummy,
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
            ns_split_mode: NsSplitMode::default(),
        }
    }

    fn arena(fwd: ContentId, rev: ContentId) -> AnnotationIndexRoot {
        AnnotationIndexRoot {
            version: 1,
            max_t: 5,
            forward_branch_cid: fwd,
            reverse_branch_cid: rev,
            stats: AnnotationStats::default(),
        }
    }

    #[test]
    fn set_annotation_index_keeps_unchanged_reseal_cids_out_of_garbage() {
        // STOR-1: re-sealing an unchanged arena produces identical
        // content-addressed CIDs. The live CIDs must NOT enter the
        // garbage manifest, or GC deletes data the new root references.
        let fwd = cid(b"fwd-branch");
        let rev = cid(b"rev-branch");
        let leaf_a = cid(b"leaf-a");
        let leaf_b = cid(b"leaf-b");

        let mut root = minimal_root();
        root.annotation_index = Some(arena(fwd.clone(), rev.clone()));
        let mut b = IncrementalRootBuilder::from_old_root(root);
        // Same branches + same leaves (byte-identical re-seal).
        b.set_annotation_index(
            Some(arena(fwd.clone(), rev.clone())),
            vec![leaf_a.clone(), leaf_b.clone()],
            vec![leaf_a.clone(), leaf_b.clone()],
        );
        let (_root, garbage) = b.build();
        for c in [&fwd, &rev, &leaf_a, &leaf_b] {
            assert!(
                !garbage.contains(c),
                "unchanged re-seal must not GC live arena CID {c}"
            );
        }
    }

    #[test]
    fn set_annotation_index_retires_changed_arena_cids() {
        // Control: when the arena genuinely changes, the old now-unused
        // CIDs ARE retired to garbage, but CIDs the new arena still
        // references (the shared reverse branch) are kept.
        let old_fwd = cid(b"fwd-old");
        let rev = cid(b"rev-shared");
        let old_leaf = cid(b"leaf-old");

        let mut root = minimal_root();
        root.annotation_index = Some(arena(old_fwd.clone(), rev.clone()));
        let mut b = IncrementalRootBuilder::from_old_root(root);
        let new_fwd = cid(b"fwd-new");
        b.set_annotation_index(
            Some(arena(new_fwd.clone(), rev.clone())),
            vec![old_leaf.clone()],
            Vec::new(), // new arena references no leaves
        );
        let (_root, garbage) = b.build();
        assert!(garbage.contains(&old_fwd), "changed forward branch retired");
        assert!(garbage.contains(&old_leaf), "unused old leaf retired");
        assert!(
            !garbage.contains(&rev),
            "reverse branch still referenced by new arena must be kept"
        );
    }
}
