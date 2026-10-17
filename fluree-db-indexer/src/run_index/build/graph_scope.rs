//! Which graphs one build pass covers.
//!
//! A dedicated tree holds one graph and leaves the graph implicit; the pooled
//! tree holds every small named graph, each row carrying its own `g_id`.

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_core::GraphId;
use std::sync::Arc;

/// The graphs a pooled build covers, with O(1) membership.
#[derive(Debug)]
pub struct PoolGraphs {
    member: Vec<bool>,
    ids: Vec<GraphId>,
}

impl PoolGraphs {
    pub fn new(ids: impl IntoIterator<Item = GraphId>) -> Self {
        let mut ids: Vec<GraphId> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        let mut member = vec![false; ids.last().map_or(0, |&g| usize::from(g) + 1)];
        for &g in &ids {
            member[usize::from(g)] = true;
        }
        Self { member, ids }
    }

    #[inline]
    pub fn contains(&self, g_id: GraphId) -> bool {
        self.member.get(usize::from(g_id)).copied().unwrap_or(false)
    }

    /// Pooled graph ids, sorted.
    pub fn ids(&self) -> &[GraphId] {
        &self.ids
    }
}

/// One build pass's graphs.
#[derive(Debug, Clone)]
pub enum GraphScope {
    One(GraphId),
    Pool(Arc<PoolGraphs>),
}

impl GraphScope {
    #[inline]
    pub fn accepts(&self, g_id: GraphId) -> bool {
        match self {
            Self::One(g) => *g == g_id,
            Self::Pool(pool) => pool.contains(g_id),
        }
    }

    #[inline]
    pub fn is_pool(&self) -> bool {
        matches!(self, Self::Pool(_))
    }

    /// The order this pass builds for a dedicated order. The pool's SPOT
    /// stream comes from the `(g_id, SPOT)`-sorted commits, so it is GSPO.
    pub fn spot_order(&self) -> RunSortOrder {
        if self.is_pool() {
            RunSortOrder::Gspo
        } else {
            RunSortOrder::Spot
        }
    }

    /// The orders built from run files (everything but the SPOT stream).
    pub fn secondary_orders(&self) -> &'static [RunSortOrder] {
        if self.is_pool() {
            RunSortOrder::pool_secondary_orders()
        } else {
            RunSortOrder::secondary_orders()
        }
    }
}

/// Rows above which a named graph keeps a dedicated tree rather than sharing
/// the pooled one. A graph this large fills its own leaflets, and its own tree
/// keeps the fast paths and batched lanes that read a graph's leaf metadata.
pub const POOL_MAX_GRAPH_ROWS: u64 = 4096;

/// Split user named graphs into `(dedicated, pooled)` by their row counts in
/// the sorted commit files (`FLUREE_POOL_MAX_GRAPH_ROWS` overrides the
/// threshold).
pub fn place_named_graphs(
    named_g_ids: &[GraphId],
    infos: &[crate::run_index::runs::spool::SortedCommitInfo],
) -> (Vec<GraphId>, Vec<GraphId>) {
    let max_rows = std::env::var("FLUREE_POOL_MAX_GRAPH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(POOL_MAX_GRAPH_ROWS);
    let mut rows: std::collections::HashMap<GraphId, u64> = std::collections::HashMap::new();
    for info in infos {
        for &(g, n) in &info.graph_rows {
            *rows.entry(g).or_insert(0) += n;
        }
    }
    named_g_ids
        .iter()
        .partition(|g| rows.get(g).copied().unwrap_or(0) > max_rows)
}

/// Per-graph record counts of a graph-first sorted slice, ascending by graph.
pub fn graph_row_counts(graphs: impl IntoIterator<Item = GraphId>) -> Vec<(GraphId, u64)> {
    let mut out: Vec<(GraphId, u64)> = Vec::new();
    for g in graphs {
        match out.last_mut() {
            Some((last, n)) if *last == g => *n += 1,
            _ => out.push((g, 1)),
        }
    }
    out
}
