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
