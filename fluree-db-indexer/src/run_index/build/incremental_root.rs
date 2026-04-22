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
    pub fn set_predicate_sids(&mut self, sids: Vec<(u16, String)>) {
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
    pub fn build(self) -> (IndexRoot, Vec<ContentId>) {
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
