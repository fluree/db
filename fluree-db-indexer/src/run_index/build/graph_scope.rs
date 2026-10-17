//! Which graphs one build pass covers.
//!
//! A dedicated tree holds one graph and leaves the graph implicit; the pooled
//! tree holds every small named graph, each row carrying its own `g_id`.

use fluree_db_binary_index::format::run_record::RunSortOrder;
use fluree_db_core::ledger_config::{GraphPlacement, GraphPlacementIntent};
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
        let mut member = vec![false; ids.last().map_or(0, |&g| g as usize + 1)];
        for &g in &ids {
            member[g as usize] = true;
        }
        Self { member, ids }
    }

    #[inline]
    pub fn contains(&self, g_id: GraphId) -> bool {
        self.member.get(g_id as usize).copied().unwrap_or(false)
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

/// Rows per leaf in the pooled tree: one leaflet. A window's writes scatter
/// across the pool's graph-last orders — a new document lands in every
/// predicate and value region it uses — and each touched leaf is rewritten
/// whole, sidecar included. Leaves this size keep a build's write cost at
/// (touched leaves x one leaflet) rather than growing with the pool.
pub fn pooled_leaf_rows(leaflet_rows: usize) -> usize {
    leaflet_rows.max(1)
}

/// Live rows above which a named graph keeps a dedicated tree rather than
/// sharing the pooled one. A graph this large fills its own leaflets, and its
/// own tree keeps the fast paths and batched lanes that read a graph's leaf
/// metadata.
pub const POOL_MAX_GRAPH_ROWS: u64 = 4096;

/// Retained history entries above which a named graph leaves the pool. Its
/// history rides in the sidecars of leaves it shares, so a write to any graph
/// beside it rewrites that history too. Current-state reads never decode it,
/// hence a looser bound than for rows.
pub const POOL_MAX_GRAPH_HISTORY: u64 = 4 * POOL_MAX_GRAPH_ROWS;

/// A named graph's size as placement weighs it: live rows and retained
/// history entries, kept apart so a small graph with a long history is not
/// mistaken for a large one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphSize {
    pub rows: u64,
    pub history: u64,
}

impl GraphSize {
    /// The size a graph's full event log will index to, before any build has
    /// resolved it: each retract removes a live row and leaves two history
    /// entries (the row's assert and the retract). Exact for logs without
    /// no-op events, which is every import.
    pub fn of_events(events: GraphEvents) -> Self {
        Self {
            rows: events.asserts.saturating_sub(events.retracts),
            history: events.retracts.saturating_mul(2),
        }
    }
}

/// Asserts and retracts of one graph in a slice of the commit log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphEvents {
    pub asserts: u64,
    pub retracts: u64,
}

/// Where every build — import, full rebuild and incremental — places user
/// named graphs: pooled while small, dedicated once large or when the
/// ledger config pins it so. The default graph and the reserved system
/// graphs always keep their own trees.
#[derive(Debug, Clone)]
pub struct PlacementPolicy {
    max_rows: u64,
    max_history: u64,
    default: GraphPlacement,
    overrides: std::collections::HashMap<GraphId, GraphPlacement>,
}

impl PlacementPolicy {
    /// `intent` resolved to graph ids through `graph_id`; an override naming
    /// a graph the ledger has not registered is moot for this build.
    /// `FLUREE_POOL_MAX_GRAPH_ROWS` / `FLUREE_POOL_MAX_GRAPH_HISTORY` override
    /// the size bounds.
    pub fn new(intent: &GraphPlacementIntent, graph_id: impl Fn(&str) -> Option<GraphId>) -> Self {
        let bound = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };
        Self {
            max_rows: bound("FLUREE_POOL_MAX_GRAPH_ROWS", POOL_MAX_GRAPH_ROWS),
            max_history: bound("FLUREE_POOL_MAX_GRAPH_HISTORY", POOL_MAX_GRAPH_HISTORY),
            default: intent.default,
            overrides: intent
                .overrides
                .iter()
                .filter_map(|(iri, p)| Some((graph_id(iri)?, *p)))
                .collect(),
        }
    }

    /// Placement with no ledger config: every user named graph by size.
    pub fn automatic() -> Self {
        Self::new(&GraphPlacementIntent::default(), |_| None)
    }

    /// Whether the config keeps `g_id` in its own trees whatever its size.
    pub fn pinned_dedicated(&self, g_id: GraphId) -> bool {
        self.overrides.get(&g_id).copied().unwrap_or(self.default) == GraphPlacement::Dedicated
    }

    /// Whether `g_id`, at `size`, belongs in the pool.
    pub fn pools(&self, g_id: GraphId, size: GraphSize) -> bool {
        g_id >= fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID
            && !self.pinned_dedicated(g_id)
            && size.rows <= self.max_rows
            && size.history <= self.max_history
    }
}

/// Split user named graphs into `(dedicated, pooled)` by `policy`, sizing
/// each from its events in the sorted commit files.
pub fn place_named_graphs(
    policy: &PlacementPolicy,
    named_g_ids: &[GraphId],
    infos: &[crate::run_index::runs::spool::SortedCommitInfo],
) -> (Vec<GraphId>, Vec<GraphId>) {
    let mut events: std::collections::HashMap<GraphId, GraphEvents> =
        std::collections::HashMap::new();
    for info in infos {
        for &(g, e) in &info.graph_events {
            let total = events.entry(g).or_default();
            total.asserts += e.asserts;
            total.retracts += e.retracts;
        }
    }
    named_g_ids.iter().partition(|&&g| {
        !policy.pools(
            g,
            GraphSize::of_events(events.get(&g).copied().unwrap_or_default()),
        )
    })
}

/// Per-graph events of a graph-first sorted slice of `(g_id, op)`, ascending
/// by graph.
pub fn graph_event_counts(
    records: impl IntoIterator<Item = (GraphId, u8)>,
) -> Vec<(GraphId, GraphEvents)> {
    let mut out: Vec<(GraphId, GraphEvents)> = Vec::new();
    for (g, op) in records {
        if !matches!(out.last(), Some((last, _)) if *last == g) {
            out.push((g, GraphEvents::default()));
        }
        let e = &mut out.last_mut().expect("pushed above").1;
        if op == 1 {
            e.asserts += 1;
        } else {
            e.retracts += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::graph_registry::FIRST_USER_GRAPH_ID;

    const G: GraphId = FIRST_USER_GRAPH_ID;

    fn size(rows: u64, history: u64) -> GraphSize {
        GraphSize { rows, history }
    }

    #[test]
    fn small_user_graphs_pool_and_the_rest_keep_their_trees() {
        let policy = PlacementPolicy::automatic();
        assert!(policy.pools(G, size(0, 0)));
        assert!(policy.pools(G, size(POOL_MAX_GRAPH_ROWS, POOL_MAX_GRAPH_HISTORY)));
        assert!(!policy.pools(G, size(POOL_MAX_GRAPH_ROWS + 1, 0)));
        assert!(
            !policy.pools(G, size(1, POOL_MAX_GRAPH_HISTORY + 1)),
            "a small graph with a long history leaves the pool"
        );
        for system in 0..FIRST_USER_GRAPH_ID {
            assert!(!policy.pools(system, size(1, 0)), "graph {system}");
        }
    }

    #[test]
    fn the_config_pins_graphs_by_iri_and_a_graph_override_wins() {
        let intent = GraphPlacementIntent {
            default: GraphPlacement::Dedicated,
            overrides: vec![
                ("http://ex/auto".into(), GraphPlacement::Auto),
                ("http://ex/unregistered".into(), GraphPlacement::Auto),
            ],
        };
        let policy = PlacementPolicy::new(&intent, |iri| match iri {
            "http://ex/auto" => Some(G + 1),
            _ => None,
        });
        assert!(policy.pinned_dedicated(G));
        assert!(!policy.pools(G, size(1, 0)));
        assert!(!policy.pinned_dedicated(G + 1));
        assert!(policy.pools(G + 1, size(1, 0)));

        let pin_one = GraphPlacementIntent {
            default: GraphPlacement::Auto,
            overrides: vec![("http://ex/pinned".into(), GraphPlacement::Dedicated)],
        };
        let policy = PlacementPolicy::new(&pin_one, |_| Some(G));
        assert!(!policy.pools(G, size(1, 0)));
        assert!(policy.pools(G + 1, size(1, 0)));
    }

    #[test]
    fn event_counts_size_a_graph_before_its_build() {
        let counts = graph_event_counts([(G, 1), (G, 1), (G, 0), (G + 1, 1)]);
        assert_eq!(
            counts,
            vec![
                (
                    G,
                    GraphEvents {
                        asserts: 2,
                        retracts: 1
                    }
                ),
                (
                    G + 1,
                    GraphEvents {
                        asserts: 1,
                        retracts: 0
                    }
                ),
            ]
        );
        assert_eq!(GraphSize::of_events(counts[0].1), size(1, 2));
    }
}
