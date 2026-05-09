//! Lazy reader for the edge-annotation arenas.
//!
//! Wraps an [`AnnotationIndexRoot`] + a `ContentStore` and provides
//! visibility-filtered lookups in either direction:
//!
//! - [`AnnotationArenaReader::current_annotations_for`] — given an
//!   edge, return the annotation subjects whose latest event at or
//!   before `as_of_t` is `op = true`.
//! - [`AnnotationArenaReader::current_targets_for`] — given an
//!   annotation subject, return the edges it currently reifies.
//! - [`AnnotationArenaReader::target_history_for`] — given an
//!   annotation, return every `(edge, t, op)` event (history view).
//!
//! ## Loading strategy
//!
//! Branches are loaded lazily on first lookup and cached for the
//! reader's lifetime. Leaves are loaded only when the branch
//! binary-search resolves to a candidate leaf, and cached by CID
//! (multiple lookups of the same hot leaf hit the cache).
//!
//! ## Visibility model
//!
//! Forward-arena rows are sorted by `(edge, ann, t, op)`. Within a
//! single `(edge, ann)` group:
//!
//! - The latest row whose `t <= as_of_t` is the visible event.
//! - If that event is `op = true`, the annotation is live at that t.
//! - If `op = false` (or no event ≤ as_of_t exists), the annotation
//!   is not visible.
//!
//! Same model for reverse, swapping the routing key.
//!
//! ## What this module is not
//!
//! - It does not merge with [`fluree_db_novelty::AttachmentNovelty`].
//!   Slice 4b adds the merged-iterator API that combines indexed +
//!   novelty state under a single `as_of_t` rule.
//! - It does not validate the on-disk arena. Slice 5 adds the
//!   storage-inspector path.

use super::format::{
    AnnotationForwardBranch, AnnotationForwardLeaf, AnnotationReverseBranch, AnnotationReverseLeaf,
};
use fluree_db_core::{
    storage::ContentStore, AnnotationIndexRoot, ContentId, EdgeKey, Result as CoreResult, Sid,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Lazy reader over a single [`AnnotationIndexRoot`].
///
/// Reuse one instance across multiple lookups in the same query — it
/// caches the forward/reverse branches and any loaded leaves. The
/// reader holds borrowed references to the root and store, so its
/// lifetime is tied to the surrounding query / cascade scope.
#[derive(Debug)]
pub struct AnnotationArenaReader<'a, S: ContentStore + ?Sized> {
    root: &'a AnnotationIndexRoot,
    store: &'a S,
    forward_branch: Mutex<Option<Arc<AnnotationForwardBranch>>>,
    reverse_branch: Mutex<Option<Arc<AnnotationReverseBranch>>>,
    forward_leaves: Mutex<HashMap<ContentId, Arc<AnnotationForwardLeaf>>>,
    reverse_leaves: Mutex<HashMap<ContentId, Arc<AnnotationReverseLeaf>>>,
}

impl<'a, S: ContentStore + ?Sized> AnnotationArenaReader<'a, S> {
    pub fn new(root: &'a AnnotationIndexRoot, store: &'a S) -> Self {
        Self {
            root,
            store,
            forward_branch: Mutex::new(None),
            reverse_branch: Mutex::new(None),
            forward_leaves: Mutex::new(HashMap::new()),
            reverse_leaves: Mutex::new(HashMap::new()),
        }
    }

    /// Annotations whose latest event at or before `as_of_t` is
    /// `op = true` for the given edge.
    ///
    /// Returns an empty vec when:
    /// - the edge has no rows in the arena;
    /// - all rows for the edge are at `t > as_of_t`;
    /// - every `(edge, ann)` group's visible event is a retract.
    pub async fn current_annotations_for(
        &self,
        edge: &EdgeKey,
        as_of_t: i64,
    ) -> CoreResult<Vec<Sid>> {
        let branch = self.load_forward_branch().await?;
        // Forward branch routes on `(edge, ann)`; we want all leaves
        // that could contain rows for this edge, regardless of ann.
        // Since rows are sorted by edge first, all rows for one edge
        // are in a contiguous span of leaves. We scan branch entries
        // whose `[first_edge, last_edge]` covers `edge`.
        let mut out: Vec<Sid> = Vec::new();
        for entry in &branch.leaves {
            if entry.last_edge < *edge {
                continue;
            }
            if entry.first_edge > *edge {
                break;
            }
            let leaf = self.load_forward_leaf(&entry.leaf_cid).await?;
            // Within a leaf, walk groups by `(edge, ann)`. Only those
            // matching our edge contribute.
            collect_live_anns_from_forward_leaf(&leaf, edge, as_of_t, &mut out);
        }
        Ok(out)
    }

    /// Edges whose latest event at or before `as_of_t` for the given
    /// annotation is `op = true`. Multiple results are possible if the
    /// annotation has been re-pointed across history (legitimate or
    /// from replayed-corrupt-history anomalies — the reader surfaces
    /// what the arena actually contains).
    pub async fn current_targets_for(&self, ann: &Sid, as_of_t: i64) -> CoreResult<Vec<EdgeKey>> {
        let branch = self.load_reverse_branch().await?;
        let mut out: Vec<EdgeKey> = Vec::new();
        for entry in &branch.leaves {
            if entry.last_ann < *ann {
                continue;
            }
            if entry.first_ann > *ann {
                break;
            }
            let leaf = self.load_reverse_leaf(&entry.leaf_cid).await?;
            collect_live_edges_from_reverse_leaf(&leaf, ann, as_of_t, &mut out);
        }
        Ok(out)
    }

    /// Every `(edge, t, op)` event for the given annotation, in arena
    /// sort order — `(edge, t, op)` ascending. Used by history queries
    /// to surface attach/detach timelines without applying a
    /// visibility filter.
    pub async fn target_history_for(&self, ann: &Sid) -> CoreResult<Vec<(EdgeKey, i64, bool)>> {
        let branch = self.load_reverse_branch().await?;
        let mut out: Vec<(EdgeKey, i64, bool)> = Vec::new();
        for entry in &branch.leaves {
            if entry.last_ann < *ann {
                continue;
            }
            if entry.first_ann > *ann {
                break;
            }
            let leaf = self.load_reverse_leaf(&entry.leaf_cid).await?;
            for row in &leaf.rows {
                if row.ann == *ann {
                    out.push((row.edge.clone(), row.t, row.op));
                }
            }
        }
        Ok(out)
    }

    // ── Loaders / cache ─────────────────────────────────────────────

    async fn load_forward_branch(&self) -> CoreResult<Arc<AnnotationForwardBranch>> {
        if let Some(b) = self.forward_branch.lock().clone() {
            return Ok(b);
        }
        let bytes = self.store.get(&self.root.forward_branch_cid).await?;
        let branch = AnnotationForwardBranch::decode(&bytes).map_err(|e| {
            fluree_db_core::Error::invalid_index(format!("annotation forward branch decode: {e}"))
        })?;
        let arc = Arc::new(branch);
        *self.forward_branch.lock() = Some(arc.clone());
        Ok(arc)
    }

    async fn load_reverse_branch(&self) -> CoreResult<Arc<AnnotationReverseBranch>> {
        if let Some(b) = self.reverse_branch.lock().clone() {
            return Ok(b);
        }
        let bytes = self.store.get(&self.root.reverse_branch_cid).await?;
        let branch = AnnotationReverseBranch::decode(&bytes).map_err(|e| {
            fluree_db_core::Error::invalid_index(format!("annotation reverse branch decode: {e}"))
        })?;
        let arc = Arc::new(branch);
        *self.reverse_branch.lock() = Some(arc.clone());
        Ok(arc)
    }

    async fn load_forward_leaf(&self, cid: &ContentId) -> CoreResult<Arc<AnnotationForwardLeaf>> {
        if let Some(l) = self.forward_leaves.lock().get(cid).cloned() {
            return Ok(l);
        }
        let bytes = self.store.get(cid).await?;
        let leaf = AnnotationForwardLeaf::decode(&bytes).map_err(|e| {
            fluree_db_core::Error::invalid_index(format!("annotation forward leaf decode: {e}"))
        })?;
        let arc = Arc::new(leaf);
        self.forward_leaves.lock().insert(cid.clone(), arc.clone());
        Ok(arc)
    }

    async fn load_reverse_leaf(&self, cid: &ContentId) -> CoreResult<Arc<AnnotationReverseLeaf>> {
        if let Some(l) = self.reverse_leaves.lock().get(cid).cloned() {
            return Ok(l);
        }
        let bytes = self.store.get(cid).await?;
        let leaf = AnnotationReverseLeaf::decode(&bytes).map_err(|e| {
            fluree_db_core::Error::invalid_index(format!("annotation reverse leaf decode: {e}"))
        })?;
        let arc = Arc::new(leaf);
        self.reverse_leaves.lock().insert(cid.clone(), arc.clone());
        Ok(arc)
    }
}

// ── Visibility-filtered scanners (pure, no I/O) ─────────────────────

/// Walk a forward leaf and append every annotation that is live at
/// `as_of_t` for the given edge. Within the leaf, rows are sorted by
/// `(edge, ann, t, op)`, so we advance to the first row matching
/// `edge` and walk groups until the edge differs.
fn collect_live_anns_from_forward_leaf(
    leaf: &AnnotationForwardLeaf,
    edge: &EdgeKey,
    as_of_t: i64,
    out: &mut Vec<Sid>,
) {
    let rows = &leaf.rows;
    // Skip rows below `edge`.
    let start = rows.partition_point(|r| r.edge < *edge);
    let mut i = start;
    while i < rows.len() && rows[i].edge == *edge {
        // Walk this `(edge, ann)` group.
        let group_ann = rows[i].ann.clone();
        let mut latest_visible: Option<bool> = None;
        while i < rows.len() && rows[i].edge == *edge && rows[i].ann == group_ann {
            if rows[i].t <= as_of_t {
                // Sort within the group is `(t, op)` ascending and
                // `false < true`, so the last row at or before
                // `as_of_t` is the latest visible event.
                latest_visible = Some(rows[i].op);
            }
            i += 1;
        }
        if latest_visible == Some(true) {
            out.push(group_ann);
        }
    }
}

fn collect_live_edges_from_reverse_leaf(
    leaf: &AnnotationReverseLeaf,
    ann: &Sid,
    as_of_t: i64,
    out: &mut Vec<EdgeKey>,
) {
    let rows = &leaf.rows;
    let start = rows.partition_point(|r| r.ann < *ann);
    let mut i = start;
    while i < rows.len() && rows[i].ann == *ann {
        let group_edge = rows[i].edge.clone();
        let mut latest_visible: Option<bool> = None;
        while i < rows.len() && rows[i].ann == *ann && rows[i].edge == group_edge {
            if rows[i].t <= as_of_t {
                latest_visible = Some(rows[i].op);
            }
            i += 1;
        }
        if latest_visible == Some(true) {
            out.push(group_edge);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::annotation_arena::{
        build_arenas_from_flakes, build_forward_branch, build_reverse_branch,
        DEFAULT_TARGET_ROWS_PER_LEAF,
    };
    use fluree_db_core::storage::MemoryContentStore;
    use fluree_db_core::{AnnotationStats, ContentKind, FlakeValue};
    use fluree_vocab::db as db_predicates;

    fn ann_sid(name: &str) -> Sid {
        Sid::new(20, name)
    }
    fn ref_sid(name: &str) -> Sid {
        Sid::new(11, name)
    }
    fn p(suffix: &str) -> Sid {
        Sid::new(fluree_vocab::namespaces::FLUREE_DB, suffix)
    }
    fn id_dt() -> Sid {
        fluree_db_core::id_datatype_sid()
    }

    fn make_bundle(
        ann: &str,
        s: &str,
        pname: &str,
        o: &str,
        t: i64,
        op: bool,
    ) -> Vec<fluree_db_core::Flake> {
        let a = ann_sid(ann);
        vec![
            fluree_db_core::Flake::new(
                a.clone(),
                p(db_predicates::REIFIES_SUBJECT),
                FlakeValue::Ref(ref_sid(s)),
                id_dt(),
                t,
                op,
                None,
            ),
            fluree_db_core::Flake::new(
                a.clone(),
                p(db_predicates::REIFIES_PREDICATE),
                FlakeValue::Ref(ref_sid(pname)),
                id_dt(),
                t,
                op,
                None,
            ),
            fluree_db_core::Flake::new(
                a,
                p(db_predicates::REIFIES_OBJECT),
                FlakeValue::Ref(ref_sid(o)),
                id_dt(),
                t,
                op,
                None,
            ),
        ]
    }

    /// Build an arena from a batch of bundle flakes, write all blobs
    /// to the given store, return the populated `AnnotationIndexRoot`.
    async fn build_and_store(
        flakes: &[fluree_db_core::Flake],
        target_rows_per_leaf: usize,
        store: &MemoryContentStore,
    ) -> AnnotationIndexRoot {
        let out = build_arenas_from_flakes(flakes, target_rows_per_leaf);

        let mut fwd_pairs = Vec::new();
        for (summary, blob) in out.forward_leaves {
            let cid = store
                .put(ContentKind::AnnotationForwardLeaf, &blob)
                .await
                .unwrap();
            fwd_pairs.push((summary, cid));
        }
        let fwd_branch_bytes = build_forward_branch(&fwd_pairs);
        let fwd_branch_cid = store
            .put(ContentKind::AnnotationForwardBranch, &fwd_branch_bytes)
            .await
            .unwrap();

        let mut rev_pairs = Vec::new();
        for (summary, blob) in out.reverse_leaves {
            let cid = store
                .put(ContentKind::AnnotationReverseLeaf, &blob)
                .await
                .unwrap();
            rev_pairs.push((summary, cid));
        }
        let rev_branch_bytes = build_reverse_branch(&rev_pairs);
        let rev_branch_cid = store
            .put(ContentKind::AnnotationReverseBranch, &rev_branch_bytes)
            .await
            .unwrap();

        AnnotationIndexRoot {
            version: 1,
            max_t: out.max_t,
            forward_branch_cid: fwd_branch_cid,
            reverse_branch_cid: rev_branch_cid,
            stats: out.stats,
        }
    }

    #[tokio::test]
    async fn current_annotations_returns_only_live_attachments() {
        // Two annotations on the same edge:
        // - ann_a: attached at t=1, retracted at t=3 → not live
        // - ann_b: attached at t=2 → live
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", "alice", "worksFor", "acme", 1, true));
        flakes.extend(make_bundle("ann_b", "alice", "worksFor", "acme", 2, true));
        flakes.extend(make_bundle("ann_a", "alice", "worksFor", "acme", 3, false));

        let store = MemoryContentStore::new();
        let root = build_and_store(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF, &store).await;

        let edge = EdgeKey {
            g: None,
            s: ref_sid("alice"),
            p: ref_sid("worksFor"),
            o: FlakeValue::Ref(ref_sid("acme")),
            dt: id_dt(),
            lang: None,
            list_i: None,
        };

        let reader = AnnotationArenaReader::new(&root, &store);
        let live = reader.current_annotations_for(&edge, 100).await.unwrap();
        assert_eq!(live, vec![ann_sid("ann_b")]);

        // History view: target_history sees every event for ann_a.
        let hist_a = reader.target_history_for(&ann_sid("ann_a")).await.unwrap();
        assert_eq!(hist_a.len(), 2);
        assert_eq!(hist_a[0].1, 1);
        assert!(hist_a[0].2);
        assert_eq!(hist_a[1].1, 3);
        assert!(!hist_a[1].2);
    }

    #[tokio::test]
    async fn current_annotations_respects_as_of_t() {
        // Same data: ann_a attach at t=1, retract at t=3.
        // - as_of_t=2 → ann_a is still live
        // - as_of_t=3 → ann_a has been retracted
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", "alice", "worksFor", "acme", 1, true));
        flakes.extend(make_bundle("ann_a", "alice", "worksFor", "acme", 3, false));

        let store = MemoryContentStore::new();
        let root = build_and_store(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF, &store).await;

        let edge = EdgeKey {
            g: None,
            s: ref_sid("alice"),
            p: ref_sid("worksFor"),
            o: FlakeValue::Ref(ref_sid("acme")),
            dt: id_dt(),
            lang: None,
            list_i: None,
        };

        let reader = AnnotationArenaReader::new(&root, &store);
        let at_t2 = reader.current_annotations_for(&edge, 2).await.unwrap();
        assert_eq!(at_t2, vec![ann_sid("ann_a")], "live at t=2");
        let at_t3 = reader.current_annotations_for(&edge, 3).await.unwrap();
        assert!(at_t3.is_empty(), "retracted at t=3");
        // Earlier than the first event: nothing visible.
        let at_t0 = reader.current_annotations_for(&edge, 0).await.unwrap();
        assert!(at_t0.is_empty());
    }

    #[tokio::test]
    async fn current_targets_returns_live_edges_per_annotation() {
        // ann_x reifies edge_1 (live) and edge_2 (retracted).
        // current_targets_for("ann_x") returns only edge_1.
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_x", "alice", "worksFor", "acme", 1, true));
        flakes.extend(make_bundle("ann_x", "bob", "worksFor", "acme", 2, true));
        flakes.extend(make_bundle("ann_x", "bob", "worksFor", "acme", 3, false));

        let store = MemoryContentStore::new();
        let root = build_and_store(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF, &store).await;
        let reader = AnnotationArenaReader::new(&root, &store);

        let mut targets = reader
            .current_targets_for(&ann_sid("ann_x"), 100)
            .await
            .unwrap();
        // Order is `(ann, edge)` arena sort; we just check membership.
        assert_eq!(targets.len(), 1);
        let only = targets.pop().unwrap();
        assert_eq!(only.s, ref_sid("alice"));
    }

    #[tokio::test]
    async fn empty_arena_returns_empty_results() {
        let store = MemoryContentStore::new();
        let root = build_and_store(&[], DEFAULT_TARGET_ROWS_PER_LEAF, &store).await;
        assert_eq!(root.stats, AnnotationStats::default());

        let reader = AnnotationArenaReader::new(&root, &store);
        let edge = EdgeKey {
            g: None,
            s: ref_sid("alice"),
            p: ref_sid("worksFor"),
            o: FlakeValue::Ref(ref_sid("acme")),
            dt: id_dt(),
            lang: None,
            list_i: None,
        };
        assert!(reader
            .current_annotations_for(&edge, 100)
            .await
            .unwrap()
            .is_empty());
        assert!(reader
            .current_targets_for(&ann_sid("ann_a"), 100)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn cache_avoids_repeat_loads() {
        // After a first lookup loads the branch + leaf, subsequent
        // lookups for the same data must not error if we corrupt the
        // store underneath. (We can't easily delete from
        // MemoryContentStore, so we instead assert that a second call
        // returns the same result — the cache is exercised by
        // construction since we only put once.)
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_a", "alice", "worksFor", "acme", 1, true));

        let store = MemoryContentStore::new();
        let root = build_and_store(&flakes, DEFAULT_TARGET_ROWS_PER_LEAF, &store).await;
        let reader = AnnotationArenaReader::new(&root, &store);

        let edge = EdgeKey {
            g: None,
            s: ref_sid("alice"),
            p: ref_sid("worksFor"),
            o: FlakeValue::Ref(ref_sid("acme")),
            dt: id_dt(),
            lang: None,
            list_i: None,
        };
        let first = reader.current_annotations_for(&edge, 100).await.unwrap();
        let second = reader.current_annotations_for(&edge, 100).await.unwrap();
        assert_eq!(first, second);
        // Cache should hold one branch + at least one leaf.
        assert!(reader.forward_branch.lock().is_some());
        assert!(!reader.forward_leaves.lock().is_empty());
    }

    #[tokio::test]
    async fn lookups_route_through_branch_to_correct_leaf() {
        // Multiple edges across more than one leaf — exercises the
        // branch routing. Use target=2 and three edges so we get
        // multiple forward leaves.
        let mut flakes = Vec::new();
        flakes.extend(make_bundle("ann_1", "s1", "worksFor", "acme", 1, true));
        flakes.extend(make_bundle("ann_2", "s2", "worksFor", "acme", 2, true));
        flakes.extend(make_bundle("ann_3", "s3", "worksFor", "acme", 3, true));

        let store = MemoryContentStore::new();
        let root = build_and_store(&flakes, 1, &store).await;
        // 3 distinct edges → at least 2 leaves with target=1.
        assert!(root.stats.forward_rows == 3);

        let reader = AnnotationArenaReader::new(&root, &store);
        for (s, ann) in [("s1", "ann_1"), ("s2", "ann_2"), ("s3", "ann_3")] {
            let edge = EdgeKey {
                g: None,
                s: ref_sid(s),
                p: ref_sid("worksFor"),
                o: FlakeValue::Ref(ref_sid("acme")),
                dt: id_dt(),
                lang: None,
                list_i: None,
            };
            let live = reader.current_annotations_for(&edge, 100).await.unwrap();
            assert_eq!(live, vec![ann_sid(ann)], "edge {s} → {ann}");
        }
    }
}
