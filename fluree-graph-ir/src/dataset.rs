//! RDF dataset — a default graph plus zero or more named graphs
//!
//! A [`Dataset`] is to [`Quad`] what [`Graph`] is to [`Triple`]. Each graph
//! inside it is an ordinary [`Graph`] and keeps that type's semantics
//! exactly: `Vec`-backed bag storage, insertion order preserved,
//! deduplication only when asked for.

use crate::{Graph, Quad, Term, Triple};
use std::collections::BTreeMap;

/// A collection of RDF graphs: one default graph, plus named graphs keyed by
/// their graph term.
///
/// # Design decisions
///
/// - **The default graph is a plain [`Graph`]**, with all of that type's
///   semantics. A dataset with no named graphs behaves exactly like the graph
///   it wraps, which is what makes triple-only input (Turtle, N-Triples) and
///   quad input agree on the default graph.
/// - **Named graphs are a `BTreeMap` keyed by [`Term`]**, so iteration order
///   is deterministic and independent of the order graphs were first
///   mentioned — the same reasoning that made [`Graph::prefixes`] a
///   `BTreeMap`. Document order within each graph is still preserved.
/// - **Directives are document-scoped and live on the default graph.**
///   `@base` and `@prefix` describe the source document, not one graph in it,
///   so [`Dataset::base`] and [`Dataset::prefixes`] read and write the
///   default graph's fields. The [`Graph`]s returned for *named* graphs carry
///   no base or prefixes and none should be read from them.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Dataset, Quad, Term, Triple};
///
/// let mut dataset = Dataset::new();
/// let t = Triple::new(
///     Term::iri("http://example.org/alice"),
///     Term::iri("http://xmlns.com/foaf/0.1/name"),
///     Term::string("Alice"),
/// );
///
/// dataset.add_quad(Quad::in_default_graph(t.clone()));
/// dataset.add_quad(Quad::in_named_graph(t, Term::iri("http://example.org/g")));
///
/// assert_eq!(dataset.len(), 2);
/// assert_eq!(dataset.named_graph_count(), 1);
/// ```
#[derive(Clone, Debug, Default)]
pub struct Dataset {
    /// The default graph, and the home of the document's base/prefix
    /// directives
    default_graph: Graph,
    /// Named graphs, in deterministic graph-term order
    named_graphs: BTreeMap<Term, Graph>,
}

impl Dataset {
    /// Create an empty dataset
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a dataset with a base IRI
    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            default_graph: Graph::with_base(base),
            named_graphs: BTreeMap::new(),
        }
    }

    /// Wrap an existing graph as a dataset's default graph
    pub fn from_graph(graph: Graph) -> Self {
        Self {
            default_graph: graph,
            named_graphs: BTreeMap::new(),
        }
    }

    /// The document's base IRI, if one was declared
    pub fn base(&self) -> Option<&str> {
        self.default_graph.base.as_deref()
    }

    /// Set the document's base IRI
    pub fn set_base(&mut self, base: impl Into<String>) {
        self.default_graph.set_base(base);
    }

    /// The document's prefix mappings
    pub fn prefixes(&self) -> &BTreeMap<String, String> {
        &self.default_graph.prefixes
    }

    /// Add a prefix mapping
    pub fn add_prefix(&mut self, prefix: impl Into<String>, namespace: impl Into<String>) {
        self.default_graph.add_prefix(prefix, namespace);
    }

    /// The default graph
    pub fn default_graph(&self) -> &Graph {
        &self.default_graph
    }

    /// The default graph, mutably
    pub fn default_graph_mut(&mut self) -> &mut Graph {
        &mut self.default_graph
    }

    /// A named graph, if it exists
    pub fn named_graph(&self, name: &Term) -> Option<&Graph> {
        self.named_graphs.get(name)
    }

    /// A named graph, mutably, if it exists
    pub fn named_graph_mut(&mut self, name: &Term) -> Option<&mut Graph> {
        self.named_graphs.get_mut(name)
    }

    /// Whether a named graph exists — including one that exists but is empty
    pub fn contains_named_graph(&self, name: &Term) -> bool {
        self.named_graphs.contains_key(name)
    }

    /// The graph a quad with this name belongs in, creating it if needed.
    ///
    /// `None` selects the default graph, which always exists. This is the
    /// routing primitive a quad-consuming sink needs.
    pub fn graph_mut(&mut self, name: Option<&Term>) -> &mut Graph {
        match name {
            None => &mut self.default_graph,
            // One `Arc` bump per call to own the key; negligible beside the
            // term clones `add` already performs.
            Some(name) => self.named_graphs.entry(name.clone()).or_default(),
        }
    }

    /// Remove a named graph, returning it if it existed
    pub fn remove_named_graph(&mut self, name: &Term) -> Option<Graph> {
        self.named_graphs.remove(name)
    }

    /// Iterate over named graphs in graph-term order
    pub fn named_graphs(&self) -> impl Iterator<Item = (&Term, &Graph)> {
        self.named_graphs.iter()
    }

    /// The graph names present, in order
    pub fn graph_names(&self) -> impl Iterator<Item = &Term> {
        self.named_graphs.keys()
    }

    /// How many named graphs exist (the default graph is not counted)
    pub fn named_graph_count(&self) -> usize {
        self.named_graphs.len()
    }

    /// Add a quad, creating its named graph if needed
    pub fn add_quad(&mut self, quad: Quad) {
        let Quad { triple, graph } = quad;
        self.graph_mut(graph.as_ref()).add(triple);
    }

    /// Add a triple to the default graph
    pub fn add_triple(&mut self, s: Term, p: Term, o: Term) {
        self.default_graph.add_triple(s, p, o);
    }

    /// Total number of statements across every graph
    pub fn len(&self) -> usize {
        self.default_graph.len() + self.named_graphs.values().map(Graph::len).sum::<usize>()
    }

    /// Whether the dataset holds no statements.
    ///
    /// An *empty named graph* still counts as no statements — use
    /// [`Dataset::named_graph_count`] to tell the two apart.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over every statement as `(graph name, triple)`, default graph
    /// first, then named graphs in graph-term order.
    ///
    /// Borrows rather than yielding owned [`Quad`]s so that walking a large
    /// dataset costs no clones; use [`Dataset::into_quads`] when ownership is
    /// wanted.
    pub fn quads(&self) -> impl Iterator<Item = (Option<&Term>, &Triple)> {
        self.default_graph.iter().map(|t| (None, t)).chain(
            self.named_graphs
                .iter()
                .flat_map(|(name, graph)| graph.iter().map(move |triple| (Some(name), triple))),
        )
    }

    /// Consume the dataset into owned quads, in the same order as
    /// [`Dataset::quads`]
    pub fn into_quads(self) -> impl Iterator<Item = Quad> {
        self.default_graph
            .into_iter()
            .map(Quad::in_default_graph)
            .chain(self.named_graphs.into_iter().flat_map(|(name, graph)| {
                graph
                    .into_iter()
                    .map(move |triple| Quad::in_named_graph(triple, name.clone()))
            }))
    }

    /// Sort every graph by SPO for deterministic output
    pub fn sort(&mut self) {
        self.default_graph.sort();
        for graph in self.named_graphs.values_mut() {
            graph.sort();
        }
    }

    /// Remove duplicate statements within each graph.
    ///
    /// Duplicates are per-graph: the same triple in two graphs is two
    /// distinct quads and both survive.
    pub fn dedupe(&mut self) {
        self.default_graph.dedupe();
        for graph in self.named_graphs.values_mut() {
            graph.dedupe();
        }
    }

    /// Sort and dedupe in one pass
    pub fn canonicalize(&mut self) {
        self.dedupe(); // dedupe already sorts
    }
}

impl From<Graph> for Dataset {
    fn from(graph: Graph) -> Self {
        Self::from_graph(graph)
    }
}

impl FromIterator<Quad> for Dataset {
    fn from_iter<T: IntoIterator<Item = Quad>>(iter: T) -> Self {
        let mut dataset = Dataset::new();
        dataset.extend(iter);
        dataset
    }
}

impl Extend<Quad> for Dataset {
    fn extend<T: IntoIterator<Item = Quad>>(&mut self, iter: T) {
        for quad in iter {
            self.add_quad(quad);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn triple(o: &str) -> Triple {
        Triple::new(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string(o),
        )
    }

    fn g(name: &str) -> Term {
        Term::iri(name)
    }

    #[test]
    fn a_dataset_with_no_named_graphs_is_its_default_graph() {
        let mut graph = Graph::new();
        graph.add(triple("a"));
        graph.add_prefix("ex", "http://example.org/");
        graph.set_base("http://example.org/");

        let dataset = Dataset::from_graph(graph);
        assert_eq!(dataset.len(), 1);
        assert_eq!(dataset.named_graph_count(), 0);
        assert_eq!(dataset.base(), Some("http://example.org/"));
        assert_eq!(
            dataset.prefixes().get("ex"),
            Some(&"http://example.org/".to_string())
        );
    }

    #[test]
    fn quads_round_trip_through_the_dataset() {
        let quads = vec![
            Quad::in_default_graph(triple("d")),
            Quad::in_named_graph(triple("a"), g("http://example.org/g1")),
            Quad::in_named_graph(triple("b"), g("http://example.org/g1")),
            Quad::in_named_graph(triple("c"), g("http://example.org/g2")),
        ];

        let dataset: Dataset = quads.iter().cloned().collect();
        assert_eq!(dataset.len(), 4);
        assert_eq!(dataset.named_graph_count(), 2);

        // Default graph first, then named graphs in term order; within a
        // graph, insertion order.
        let round_tripped: Vec<Quad> = dataset.into_quads().collect();
        assert_eq!(round_tripped, quads);
    }

    #[test]
    fn borrowed_and_owned_iteration_agree() {
        let dataset: Dataset = vec![
            Quad::in_default_graph(triple("d")),
            Quad::in_named_graph(triple("a"), g("http://example.org/g1")),
        ]
        .into_iter()
        .collect();

        let borrowed: Vec<Quad> = dataset
            .quads()
            .map(|(name, t)| Quad::new(t.clone(), name.cloned()))
            .collect();
        let owned: Vec<Quad> = dataset.into_quads().collect();
        assert_eq!(borrowed, owned);
    }

    #[test]
    fn the_same_triple_in_two_graphs_stays_two_quads() {
        let mut dataset = Dataset::new();
        dataset.add_quad(Quad::in_default_graph(triple("x")));
        dataset.add_quad(Quad::in_named_graph(triple("x"), g("http://example.org/g")));
        dataset.add_quad(Quad::in_named_graph(triple("x"), g("http://example.org/g")));

        assert_eq!(dataset.len(), 3);
        // Dedupe is per-graph: the two in `g` collapse, the default one does
        // not join them.
        dataset.dedupe();
        assert_eq!(dataset.len(), 2);
        assert_eq!(dataset.default_graph().len(), 1);
        assert_eq!(
            dataset
                .named_graph(&g("http://example.org/g"))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn named_graph_iteration_order_is_term_order_not_insertion_order() {
        let mut dataset = Dataset::new();
        dataset.add_quad(Quad::in_named_graph(triple("z"), g("http://example.org/z")));
        dataset.add_quad(Quad::in_named_graph(triple("a"), g("http://example.org/a")));

        let names: Vec<&str> = dataset.graph_names().filter_map(Term::as_iri).collect();
        assert_eq!(names, vec!["http://example.org/a", "http://example.org/z"]);
    }

    #[test]
    fn an_empty_named_graph_exists_without_holding_statements() {
        let mut dataset = Dataset::new();
        // `<g> {}` in TriG: the graph is mentioned but asserts nothing.
        dataset.graph_mut(Some(&g("http://example.org/g")));

        assert!(dataset.is_empty(), "no statements");
        assert_eq!(dataset.named_graph_count(), 1, "but the graph is present");
        assert!(dataset.contains_named_graph(&g("http://example.org/g")));

        assert!(dataset
            .remove_named_graph(&g("http://example.org/g"))
            .is_some());
        assert_eq!(dataset.named_graph_count(), 0);
    }

    #[test]
    fn directives_live_on_the_default_graph_only() {
        let mut dataset = Dataset::new();
        dataset.set_base("http://example.org/");
        dataset.add_prefix("ex", "http://example.org/");
        let named = dataset.graph_mut(Some(&g("http://example.org/g")));

        assert_eq!(named.base, None, "named graphs carry no directives");
        assert!(named.prefixes.is_empty());
        assert_eq!(dataset.base(), Some("http://example.org/"));
        assert_eq!(
            dataset.default_graph().base.as_deref(),
            Some("http://example.org/")
        );
    }

    #[test]
    fn sorting_is_per_graph_and_leaves_graph_order_alone() {
        let mut dataset = Dataset::new();
        dataset.add_quad(Quad::in_named_graph(triple("b"), g("http://example.org/g")));
        dataset.add_quad(Quad::in_named_graph(triple("a"), g("http://example.org/g")));
        dataset.add_quad(Quad::in_default_graph(triple("b")));
        dataset.add_quad(Quad::in_default_graph(triple("a")));

        dataset.sort();

        let objects: Vec<String> = dataset.quads().map(|(_, t)| t.o.to_string()).collect();
        assert_eq!(objects, vec![r#""a""#, r#""b""#, r#""a""#, r#""b""#]);
    }
}
