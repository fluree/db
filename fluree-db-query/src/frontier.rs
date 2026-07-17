//! Shared raw-id frontier expansion for graph traversal operators.
//!
//! Path searches (shortest path, property paths) explore the graph one BFS
//! level at a time. Expanding a level node-by-node costs one index descent,
//! full `Flake` materialization, and a dictionary-backed `Sid` per neighbor —
//! per node. This module holds the batched alternative both operators share:
//! frontier nodes keyed by persisted `s_id` (u64), each level expanded with a
//! handful of galloping [`fluree_db_binary_index`] batched-lookup sweeps, and
//! neighbors taken as raw `o_key` ids (for `IRI_REF` rows, `o_key` IS the
//! target's `s_id` — no dictionary in the loop).
//!
//! # Overlay correctness
//!
//! Batched sweeps read base index rows only. Overlay (novelty) correctness is
//! per-node: [`overlay_dirty_ids`] summarizes which persisted subjects the
//! overlay touches on each side (as subject → out-edges incomplete in base; as
//! ref-object → in-edges incomplete; retracts stamp both), and traversal
//! expands those nodes — plus subjects that exist only in novelty
//! ([`PathNode::Novel`]) — through the operator's per-node `Sid`-space
//! fallback, whose reads merge novelty. An overlay that can't be summarized
//! (no content version) declines the raw-id lane entirely.

use crate::error::{QueryError, Result};
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::{FlakeValue, GraphId, IndexType, OverlayProvider, Sid};
use rustc_hash::FxHashSet;
use std::sync::Arc;

/// A BFS node in the raw-id lane: a persisted subject id, or a subject that
/// exists only in novelty (no persisted id — always expands via the Sid
/// fallback). The two never alias: batched base rows only ever produce
/// persisted ids, and Sid-lane neighbors resolve to `Id` whenever a
/// persisted id exists.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum PathNode {
    Id(u64),
    Novel(Sid),
}

/// Persisted subject ids the overlay touches, split by the side they
/// invalidate: `subjects` (any overlay flake with the node as subject —
/// out-edges incomplete in base) and `objects` (as ref-object — in-edges
/// incomplete). Retract flakes stamp both, so batched base reads are only
/// trusted where they are provably the whole truth.
pub(crate) struct DirtyIds {
    pub(crate) subjects: FxHashSet<u64>,
    pub(crate) objects: FxHashSet<u64>,
}

/// Build (and LRU-cache, keyed on overlay content version) the overlay's
/// dirty-id sets for one graph. `None` = the overlay can't be summarized
/// (no content version) — the caller must decline the raw-id lane.
pub(crate) fn overlay_dirty_ids(
    overlay: &dyn OverlayProvider,
    g_id: GraphId,
    store: &Arc<BinaryIndexStore>,
) -> Option<Arc<DirtyIds>> {
    use std::sync::OnceLock;
    type Cache = parking_lot::Mutex<lru::LruCache<(u64, u16, u64), Arc<DirtyIds>>>;
    static CACHE: OnceLock<Cache> = OnceLock::new();

    if overlay.is_effectively_empty() {
        static EMPTY: OnceLock<Arc<DirtyIds>> = OnceLock::new();
        return Some(Arc::clone(EMPTY.get_or_init(|| {
            Arc::new(DirtyIds {
                subjects: FxHashSet::default(),
                objects: FxHashSet::default(),
            })
        })));
    }
    let version = overlay.content_version()?;
    // Store-instance id, not the raw pointer: a dropped store can be
    // reallocated at the same address, so a pointer-keyed cache is subject to
    // an ABA misread (ABA on the store, not just a coinciding version/g_id).
    let store_key = store.store_id();
    let cache = CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(8).expect("nonzero"),
        ))
    });
    if let Some(hit) = cache.lock().get(&(version, g_id, store_key)) {
        return Some(Arc::clone(hit));
    }

    // Collect with no to_t cap: a superset stays conservative (a node whose
    // overlay flakes all lie beyond the view's t just takes the fallback).
    let mut subject_sids: std::collections::HashSet<Sid> = std::collections::HashSet::new();
    let mut object_sids: std::collections::HashSet<Sid> = std::collections::HashSet::new();
    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Spot,
        None,
        None,
        true,
        i64::MAX,
        &mut |f| {
            subject_sids.insert(f.s.clone());
            if let FlakeValue::Ref(o) = &f.o {
                object_sids.insert(o.clone());
            }
        },
    );
    let resolve = |sids: std::collections::HashSet<Sid>| -> FxHashSet<u64> {
        let mut out = FxHashSet::default();
        for sid in sids {
            if let Ok(Some(s_id)) = store.find_subject_id_by_parts(sid.namespace_code, &sid.name) {
                out.insert(s_id);
            }
            // No persisted id: the node can never appear as a PathNode::Id,
            // so it needs no dirty marker.
        }
        out
    };
    let dirty = Arc::new(DirtyIds {
        subjects: resolve(subject_sids),
        objects: resolve(object_sids),
    });
    cache
        .lock()
        .put((version, g_id, store_key), Arc::clone(&dirty));
    Some(dirty)
}

/// Reserved edge predicates (`rdf:type`, `f:reifies*`) as base `p_id`s, for
/// wildcard expansion to drop (mirrors
/// [`crate::property_path::is_reserved_edge_predicate`]). Absent from the base
/// dict means absent from base rows; the Sid fallback re-checks by Sid.
pub(crate) fn reserved_edge_pids(store: &BinaryIndexStore) -> FxHashSet<u32> {
    let mut reserved: FxHashSet<u32> = FxHashSet::default();
    for sid in fluree_db_core::reifies_predicate_sids() {
        if let Some(p_id) = store.sid_to_p_id(&sid) {
            reserved.insert(p_id);
        }
    }
    let rdf_type_sid = Sid::new(
        fluree_vocab::namespaces::RDF,
        fluree_vocab::predicates::RDF_TYPE,
    );
    if let Some(p_id) = store.sid_to_p_id(&rdf_type_sid) {
        reserved.insert(p_id);
    }
    reserved
}

/// One graph view for batched raw-id expansion: the base store, the graph and
/// time bound the sweeps read at, and the overlay's dirty-id summary that
/// routes untrustworthy nodes to the per-node fallback.
pub(crate) struct FrontierView {
    pub(crate) store: Arc<BinaryIndexStore>,
    pub(crate) g_id: GraphId,
    pub(crate) to_t: i64,
    pub(crate) dirty: Arc<DirtyIds>,
}

impl FrontierView {
    pub(crate) fn node_for_sid(&self, sid: &Sid) -> PathNode {
        match self
            .store
            .find_subject_id_by_parts(sid.namespace_code, &sid.name)
        {
            Ok(Some(s_id)) => PathNode::Id(s_id),
            _ => PathNode::Novel(sid.clone()),
        }
    }

    pub(crate) fn sid_for_node(&self, node: &PathNode) -> Result<Sid> {
        match node {
            PathNode::Novel(sid) => Ok(sid.clone()),
            PathNode::Id(s_id) => {
                let (ns_code, suffix) = self
                    .store
                    .resolve_subject_parts(*s_id)
                    .map_err(|e| QueryError::Internal(format!("resolve path node {s_id}: {e}")))?;
                Ok(Sid::new(ns_code, suffix))
            }
        }
    }

    /// A persisted node's base rows are the whole truth for this side iff
    /// the overlay never touches the orientations being followed.
    pub(crate) fn is_clean(&self, s_id: u64, use_out: bool, use_in: bool) -> bool {
        (!use_out || !self.dirty.subjects.contains(&s_id))
            && (!use_in || !self.dirty.objects.contains(&s_id))
    }

    /// Typed out-edges of `subjects` under one predicate, from base rows:
    /// `(source, target)` id pairs. `o_key` of an `IRI_REF` row is the
    /// target's `s_id`, and the sweep filters to ref rows itself.
    pub(crate) fn out_typed(
        &self,
        p_id: u32,
        subjects: &[u64],
        out: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        let refs = fluree_db_binary_index::batched_lookup_predicate_refs(
            &self.store,
            self.g_id,
            p_id,
            subjects,
            self.to_t,
        )
        .map_err(|e| QueryError::Internal(format!("batched out-edges: {e}")))?;
        for (s_id, targets) in refs {
            for t in targets {
                out.push((s_id, t));
            }
        }
        Ok(())
    }

    /// Wildcard out-edges of `subjects` (any node→node edge except the
    /// reserved predicates), from base rows: `(source, target)` id pairs.
    pub(crate) fn out_wildcard(
        &self,
        subjects: &[u64],
        reserved_pids: &FxHashSet<u32>,
        out: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        let rows = fluree_db_binary_index::batched_lookup_subject_properties(
            &self.store,
            self.g_id,
            subjects,
            self.to_t,
        )
        .map_err(|e| QueryError::Internal(format!("batched out-edges: {e}")))?;
        for (s_id, props) in rows {
            for (p_id, o_type, o_key) in props {
                if reserved_pids.contains(&p_id) {
                    continue;
                }
                if !fluree_db_core::o_type::OType::from_u16(o_type).is_node_ref() {
                    continue;
                }
                out.push((s_id, o_key));
            }
        }
        Ok(())
    }

    /// In-edges pointing at `objects` from base rows, keeping only edges whose
    /// predicate passes `keep`: `(object, source-subject)` id pairs.
    pub(crate) fn in_edges(
        &self,
        objects: &[u64],
        keep: impl Fn(u32) -> bool,
        out: &mut Vec<(u64, u64)>,
    ) -> Result<()> {
        let inbound = fluree_db_binary_index::batched_lookup_inbound_refs(
            &self.store,
            self.g_id,
            objects,
            self.to_t,
        )
        .map_err(|e| QueryError::Internal(format!("batched in-edges: {e}")))?;
        for (o_key, edges) in inbound {
            for (p_id, s_id) in edges {
                if keep(p_id) {
                    out.push((o_key, s_id));
                }
            }
        }
        Ok(())
    }
}
