//! An RDF dataset: a default graph plus named graphs.

use crate::{Graph, Term};
use std::collections::BTreeMap;

/// A default graph and any number of named graphs, each a [`Graph`] with its
/// own triples and reifier attachments. Named graphs are kept in graph-name
/// order, so output built from a dataset is deterministic.
#[derive(Clone, Debug, Default)]
pub struct Dataset {
    /// The default graph.
    pub default: Graph,
    /// Named graphs, keyed by graph name (an IRI or a blank node).
    pub named: BTreeMap<Term, Graph>,
}

impl Dataset {
    /// An empty dataset.
    pub fn new() -> Self {
        Self::default()
    }

    /// The graph named `name` (`None`: the default graph), created if absent.
    pub fn graph_mut(&mut self, name: Option<&Term>) -> &mut Graph {
        match name {
            None => &mut self.default,
            Some(name) => self.named.entry(name.clone()).or_default(),
        }
    }

    /// Whether the dataset has no named graphs, so it can be written in a
    /// triples-only format.
    pub fn is_default_only(&self) -> bool {
        self.named.is_empty()
    }

    /// Sort and dedupe every graph (see [`Graph::canonicalize`]).
    pub fn canonicalize(&mut self) {
        self.default.canonicalize();
        self.named.values_mut().for_each(Graph::canonicalize);
    }

    /// Total triples across all graphs.
    pub fn len(&self) -> usize {
        self.default.len() + self.named.values().map(Graph::len).sum::<usize>()
    }

    /// Whether every graph is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The default graph, then each named graph with its name.
    pub fn graphs(&self) -> impl Iterator<Item = (Option<&Term>, &Graph)> {
        std::iter::once((None, &self.default)).chain(self.named.iter().map(|(n, g)| (Some(n), g)))
    }
}

impl From<Graph> for Dataset {
    fn from(default: Graph) -> Self {
        Self {
            default,
            named: BTreeMap::new(),
        }
    }
}
