//! What a scope reads through one ledger's binary index.
//!
//! A lane that reads the index learns which graphs it reads from
//! [`ExecutionContext::index_scope`] and matches on the [`IndexScope`] it gets.
//! A lane that handles one graph takes [`IndexScope::One`] and declines the
//! other arms, so reading one member of a default graph of several as if it
//! were the whole default graph cannot be written. The planner asks the same
//! question as a [`ScopeKind`], from the same function ([`ScopeKind::of`]).
//!
//! [`ExecutionContext::index_scope`]: crate::context::ExecutionContext::index_scope

use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::ids::GraphId;
use fluree_db_core::OverlayProvider;
use smallvec::SmallVec;

use crate::dataset::{ActiveGraph, DataSet};

/// How many graphs a scope's active graph reads, decided from the dataset
/// alone: the one definition the runtime asks as
/// [`ExecutionContext::scope_kind`] and the planner as
/// [`PlanningContext::index_scope_kind`].
///
/// [`ExecutionContext::scope_kind`]: crate::context::ExecutionContext::scope_kind
/// [`PlanningContext::index_scope_kind`]: crate::temporal_mode::PlanningContext::index_scope_kind
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ScopeKind {
    /// No dataset, a named graph the dataset holds, or a default graph of
    /// exactly one member.
    One,
    /// A default graph of two or more members, in a dataset of one ledger:
    /// the set union of the members' triples.
    Union,
    /// A default graph of no members (`FROM NAMED` alone): it holds nothing.
    Empty,
    /// Neither: a default graph of several members in a dataset that spans
    /// ledgers, or a named graph the dataset does not hold. Also what a
    /// planning context that was never given its scope plans for, so it
    /// plans no lane that reads an index.
    #[default]
    None,
}

impl ScopeKind {
    /// The kind of the scope whose active graph is `active` over `dataset`
    /// (`None`: no dataset, a ledger's own default graph).
    pub fn of(active: &ActiveGraph, dataset: Option<&DataSet<'_>>) -> Self {
        let Some(ds) = dataset else {
            return ScopeKind::One;
        };
        match active {
            ActiveGraph::Named(iri) if ds.named_graph(iri).is_some() => ScopeKind::One,
            ActiveGraph::Named(_) => ScopeKind::None,
            ActiveGraph::Default => match ds.default_graphs().len() {
                0 => ScopeKind::Empty,
                1 => ScopeKind::One,
                _ if ds.spans_multiple_ledgers() => ScopeKind::None,
                _ => ScopeKind::Union,
            },
        }
    }

    /// Whether a scope of this kind reads exactly one graph through that
    /// graph's binary index: one graph, a store attached (`has_store`), and a
    /// dataset that does not span ledgers (`spans_ledgers`). A dataset that
    /// spans ledgers reads every graph, named ones included, through the
    /// range scan: its bindings carry their ledger, which needs decoded
    /// terms, so the query path attaches no store to it.
    ///
    /// The one definition behind `ExecutionContext::has_binary_store` and
    /// `PlanningContext::reads_one_graph_index`, for a lane that has no
    /// fallback without the index (the geo index search).
    pub fn reads_one_graph_index(self, has_store: bool, spans_ledgers: bool) -> bool {
        has_store && self == ScopeKind::One && !spans_ledgers
    }
}

/// What a scope reads through one ledger's binary index. Built only by
/// [`ExecutionContext::index_scope`], which returns `None` where no index lane
/// may run.
///
/// [`ExecutionContext::index_scope`]: crate::context::ExecutionContext::index_scope
pub enum IndexScope<'a> {
    /// One graph: no dataset, a named graph, or a default graph of one member.
    One(IndexMember<'a>),
    /// A default graph of two or more graphs of one ledger, every one read
    /// through the same store and dictionary novelty: the set union of their
    /// triples.
    Union(MemberSet<'a>),
    /// A default graph of none (`FROM NAMED` alone): it answers nothing.
    Empty,
}

impl IndexScope<'_> {
    /// The [`ScopeKind`] this scope is.
    pub fn kind(&self) -> ScopeKind {
        match self {
            IndexScope::One(_) => ScopeKind::One,
            IndexScope::Union(_) => ScopeKind::Union,
            IndexScope::Empty => ScopeKind::Empty,
        }
    }
}

/// One graph of the ledger, as an index reader needs it: the store, the
/// graph, its novelty and its `t`. Built only by
/// [`ExecutionContext::index_scope`].
///
/// [`ExecutionContext::index_scope`]: crate::context::ExecutionContext::index_scope
#[derive(Clone, Copy)]
pub struct IndexMember<'a> {
    store: &'a Arc<BinaryIndexStore>,
    g_id: GraphId,
    overlay: &'a dyn OverlayProvider,
    to_t: i64,
}

impl<'a> IndexMember<'a> {
    pub(crate) fn new(
        store: &'a Arc<BinaryIndexStore>,
        g_id: GraphId,
        overlay: &'a dyn OverlayProvider,
        to_t: i64,
    ) -> Self {
        Self {
            store,
            g_id,
            overlay,
            to_t,
        }
    }

    /// The store this graph is read through.
    pub fn store(&self) -> &'a Arc<BinaryIndexStore> {
        self.store
    }

    /// The graph's id in [`Self::store`].
    pub fn g_id(&self) -> GraphId {
        self.g_id
    }

    /// The graph's novelty.
    pub fn overlay(&self) -> &'a dyn OverlayProvider {
        self.overlay
    }

    /// The `t` the graph is read at.
    pub fn to_t(&self) -> i64 {
        self.to_t
    }

    /// Whether the index alone answers for this graph: its overlay carries no
    /// novelty, and the index is at its `t`. What `fast_path_store` has
    /// always required.
    pub fn index_is_current(&self) -> bool {
        !overlay_carries_novelty(self.overlay) && self.to_t == self.store.max_t()
    }
}

/// Two or more graphs of one ledger, all read through one store. Never fewer
/// than two: [`Self::new`] refuses fewer.
pub struct MemberSet<'a> {
    store: &'a Arc<BinaryIndexStore>,
    members: SmallVec<[IndexMember<'a>; 4]>,
}

impl<'a> MemberSet<'a> {
    /// The set of `members`, each read through `store`, or `None` for fewer
    /// than two.
    pub(crate) fn new(
        store: &'a Arc<BinaryIndexStore>,
        members: SmallVec<[IndexMember<'a>; 4]>,
    ) -> Option<Self> {
        debug_assert!(members.iter().all(|m| Arc::ptr_eq(m.store, store)));
        (members.len() >= 2).then_some(Self { store, members })
    }

    /// The store every member is read through.
    pub fn store(&self) -> &'a Arc<BinaryIndexStore> {
        self.store
    }

    /// The members, in the dataset's order.
    pub fn members(&self) -> &[IndexMember<'a>] {
        &self.members
    }

    /// Whether the index alone answers for every member
    /// ([`IndexMember::index_is_current`]).
    pub fn all_current(&self) -> bool {
        self.members.iter().all(IndexMember::index_is_current)
    }
}

/// Whether `overlay` can still contribute flakes.
///
/// `epoch()` is a monotonic mutation counter that never resets, so
/// `epoch() == 0` means "never had novelty since this overlay was created";
/// it stays non-zero on a long-lived cached ledger even after an index swap
/// drains every segment, which `is_effectively_empty()` reports.
#[inline]
pub fn overlay_carries_novelty(overlay: &dyn OverlayProvider) -> bool {
    overlay.epoch() != 0 && !overlay.is_effectively_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::GraphRef;
    use fluree_db_core::{LedgerSnapshot, NoOverlay};

    fn graph<'a>(snapshot: &'a LedgerSnapshot, g_id: GraphId, ledger: &str) -> GraphRef<'a> {
        GraphRef::new(snapshot, g_id, &NoOverlay, 0, ledger)
    }

    /// `ScopeKind::of` over every dataset shape the scope type distinguishes.
    #[test]
    fn scope_kind_of_every_dataset_shape() {
        let snap = LedgerSnapshot::genesis("l:main");
        let default = ActiveGraph::Default;
        let named = |iri: &str| ActiveGraph::named(iri);

        // No dataset: the ledger's own graph, default or named.
        assert_eq!(ScopeKind::of(&default, None), ScopeKind::One);
        assert_eq!(ScopeKind::of(&named("urn:g1"), None), ScopeKind::One);

        // FROM NAMED alone: a default graph of none.
        let ds = DataSet::new().with_named_graph("urn:g1", graph(&snap, 3, "l:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::Empty);
        assert_eq!(ScopeKind::of(&named("urn:g1"), Some(&ds)), ScopeKind::One);
        assert_eq!(
            ScopeKind::of(&named("urn:nope"), Some(&ds)),
            ScopeKind::None
        );

        // FROM one graph.
        let ds = DataSet::new().with_default_graph(graph(&snap, 3, "l:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::One);

        // FROM two and three graphs of one ledger.
        let ds = DataSet::new()
            .with_default_graph(graph(&snap, 3, "l:main"))
            .with_default_graph(graph(&snap, 4, "l:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::Union);
        let ds = ds.with_default_graph(graph(&snap, 0, "l:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::Union);

        // Two ledgers in the default graph; and one ledger's two graphs in a
        // dataset whose named graph is another ledger's.
        let ds = DataSet::new()
            .with_default_graph(graph(&snap, 0, "a:main"))
            .with_default_graph(graph(&snap, 0, "b:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::None);
        let ds = DataSet::new()
            .with_default_graph(graph(&snap, 3, "a:main"))
            .with_default_graph(graph(&snap, 4, "a:main"))
            .with_named_graph("urn:b", graph(&snap, 0, "b:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::None);

        // One default graph beside another ledger's named graph reads one
        // graph, as `reads_exactly_one_graph` always said; it has no index to
        // read it through.
        let ds = DataSet::new()
            .with_default_graph(graph(&snap, 0, "a:main"))
            .with_named_graph("urn:b", graph(&snap, 0, "b:main"));
        assert_eq!(ScopeKind::of(&default, Some(&ds)), ScopeKind::One);
        assert!(!ScopeKind::One.reads_one_graph_index(true, ds.spans_multiple_ledgers()));
    }

    #[test]
    fn a_planning_context_never_given_its_scope_reads_no_index() {
        assert_eq!(ScopeKind::default(), ScopeKind::None);
        assert!(!ScopeKind::default().reads_one_graph_index(true, false));
    }
}
