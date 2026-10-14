//! An RDF dataset: a default graph plus named graphs.

use crate::{Graph, Term};
use fluree_vocab::rdf;
use std::collections::BTreeMap;
use std::sync::Arc;

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

    /// Add the quad `(s, p, o)` to the graph named `graph` (`None`: the
    /// default graph). `r rdf:reifies <<( s p o )>>` is the reification it
    /// is in RDF 1.2 (see [`Graph::add_reification`]), so it is written as
    /// an annotation where the format has one; any other quad is a triple.
    pub fn add_quad(&mut self, s: Term, p: Term, o: Term, graph: Option<&Term>) {
        let target = self.graph_mut(graph);
        match o {
            Term::TripleTerm(triple) if p.as_iri() == Some(rdf::REIFIES) => {
                let [ts, tp, to] = Arc::unwrap_or_clone(triple);
                target.add_reification(ts, tp, to, s);
            }
            o => target.add_triple(s, p, o),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reifies_quad_is_a_reification() {
        let (s, p, o) = (
            Term::iri("http://ex/s"),
            Term::iri("http://ex/p"),
            Term::iri("http://ex/o"),
        );
        let (r, g) = (Term::blank("r"), Term::iri("http://ex/g"));
        let reifies = Term::iri(rdf::REIFIES);
        let mut dataset = Dataset::new();
        dataset.add_quad(
            r.clone(),
            reifies.clone(),
            Term::triple(s.clone(), p.clone(), o.clone()),
            Some(&g),
        );
        dataset.add_quad(r.clone(), reifies, s.clone(), None);

        let named = &dataset.named[&g];
        assert!(named.is_empty(), "a reification does not assert its triple");
        assert_eq!(named.reifications()[0].reifier, r);
        assert_eq!(named.reifications()[0].triple.o, o);
        assert_eq!(
            dataset.default.len(),
            1,
            "rdf:reifies an IRI is just a triple"
        );
    }
}
