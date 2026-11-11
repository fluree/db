//! The data a focus node is checked against: the graph being validated read
//! together with its `f:additionalDataSource` graphs as one graph.

use crate::error::Result;
use fluree_db_core::{Flake, GraphDbRef, GraphId, IndexType, RangeMatch, RangeTest};
use std::collections::HashSet;

/// A data graph's `f:additionalDataSource` list with the data graph split out:
/// it is always read, so listing it changes only which graphs the `sh:class`
/// subclass walk reads.
#[derive(Debug)]
pub(crate) struct DataSources {
    /// The listed graphs other than the data graph, sorted and distinct.
    others: Vec<GraphId>,
    /// Whether the data graph lists itself.
    lists_itself: bool,
}

impl DataSources {
    pub(crate) fn new(g_id: GraphId, mut listed: Vec<GraphId>) -> Self {
        let lists_itself = listed.contains(&g_id);
        listed.retain(|&other| other != g_id);
        listed.sort_unstable();
        listed.dedup();
        Self {
            others: listed,
            lists_itself,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.others.is_empty() && !self.lists_itself
    }
}

/// The graph being validated plus the graphs read with it.
///
/// Every read made while checking a node goes through [`DataView::range`],
/// which unions the graphs and keeps a fact stored in several of them once,
/// so a path can step from one graph into another and a fact counts once.
/// Which nodes are checked, and against which shapes, is decided from
/// `graph` alone.
#[derive(Clone, Copy)]
pub(crate) struct DataView<'a> {
    /// The graph being validated.
    pub(crate) graph: GraphDbRef<'a>,
    /// Graphs read with it; never contains `graph.g_id`.
    pub(crate) additional: &'a [GraphId],
    /// Whether `graph` is itself listed as a source.
    lists_itself: bool,
}

impl<'a> DataView<'a> {
    pub(crate) fn new(graph: GraphDbRef<'a>, sources: Option<&'a DataSources>) -> Self {
        Self {
            graph,
            additional: sources.map_or(&[][..], |s| s.others.as_slice()),
            lists_itself: sources.is_some_and(|s| s.lists_itself),
        }
    }

    /// The graphs read, the validated graph first.
    pub(crate) fn g_ids(&self) -> impl Iterator<Item = GraphId> + '_ {
        std::iter::once(self.graph.g_id).chain(self.additional.iter().copied())
    }

    /// The listed graphs: `additional`, plus the validated graph when it lists
    /// itself.
    pub(crate) fn listed_g_ids(&self) -> impl Iterator<Item = GraphId> + '_ {
        self.lists_itself
            .then_some(self.graph.g_id)
            .into_iter()
            .chain(self.additional.iter().copied())
    }

    /// `range` over the union, in `index` order. A fact is the same fact in
    /// every graph (`Flake` equality ignores the graph and `t`).
    pub(crate) async fn range(
        &self,
        index: IndexType,
        test: RangeTest,
        match_val: RangeMatch,
    ) -> Result<Vec<Flake>> {
        if self.additional.is_empty() {
            return Ok(self.graph.range(index, test, match_val).await?);
        }
        let mut flakes = self.graph.range(index, test, match_val.clone()).await?;
        let mut extra = Vec::new();
        for &g_id in self.additional {
            // Copy and re-point rather than `GraphDbRef::new`, which would
            // drop the tracker and the runtime dictionaries.
            let mut source = self.graph;
            source.g_id = g_id;
            extra.extend(source.range(index, test, match_val.clone()).await?);
        }
        if extra.is_empty() {
            return Ok(flakes);
        }
        let mut seen: HashSet<Flake> = flakes.iter().cloned().collect();
        for flake in extra {
            if seen.insert(flake.clone()) {
                flakes.push(flake);
            }
        }
        flakes.sort_by(|a, b| index.compare(a, b));
        Ok(flakes)
    }
}
