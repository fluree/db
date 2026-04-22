//! RDF graph - a collection of triples
//!
//! The `Graph` type uses `Vec<Triple>` to preserve duplicates (bag semantics).
//! Call `dedupe()` explicitly if you want set semantics.

use crate::{Term, Triple};
use std::collections::BTreeMap;

/// A collection of RDF triples
///
/// # Design Decisions
///
/// - **Vec storage**: Uses `Vec<Triple>` instead of `BTreeSet` to preserve
///   duplicates from template instantiation.
/// - **Explicit deduplication**: Call `dedupe()` if you want set semantics.
/// - **Deterministic output**: Call `sort()` before formatting for stable output.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Graph, Term, Triple};
///
/// let mut graph = Graph::new();
///
/// graph.add_triple(
///     Term::iri("http://example.org/alice"),
///     Term::iri("http://xmlns.com/foaf/0.1/name"),
///     Term::string("Alice"),
/// );
///
/// // Sort for deterministic output
/// graph.sort();
///
/// // Or canonicalize (sort + dedupe)
/// graph.canonicalize();
/// ```
#[derive(Clone, Debug, Default)]
pub struct Graph {
    /// The triples in this graph
    triples: Vec<Triple>,
    /// Base IRI from parsing (for reconstruction)
    pub base: Option<String>,
    /// Prefix mappings from parsing (deterministic order via BTreeMap)
    pub prefixes: BTreeMap<String, String>,
}

impl Graph {
    /// Create an empty graph
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a graph with a base IRI
    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            base: Some(base.into()),
            ..Default::default()
        }
    }

    /// Set the base IRI
    pub fn set_base(&mut self, base: impl Into<String>) {
        self.base = Some(base.into());
    }

    /// Add a prefix mapping
    pub fn add_prefix(&mut self, prefix: impl Into<String>, namespace: impl Into<String>) {
        self.prefixes.insert(prefix.into(), namespace.into());
    }

    /// Add a triple to the graph
    pub fn add(&mut self, triple: Triple) {
        self.triples.push(triple);
    }

    /// Add a triple by components
    pub fn add_triple(&mut self, s: Term, p: Term, o: Term) {
        self.add(Triple::new(s, p, o));
    }

    /// Add a list item triple (with list index)
    ///
    /// The `index` is the 0-based position in the list for this (subject, predicate).
    pub fn add_list_item(&mut self, s: Term, p: Term, o: Term, index: i32) {
        self.add(Triple::with_list_index(s, p, o, index));
    }

    /// Get the number of triples
    pub fn len(&self) -> usize {
        self.triples.len()
    }

    /// Check if the graph is empty
    pub fn is_empty(&self) -> bool {
        self.triples.is_empty()
    }

    /// Iterate over triples
    pub fn iter(&self) -> impl Iterator<Item = &Triple> {
        self.triples.iter()
    }

    /// Get a mutable iterator over triples
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Triple> {
        self.triples.iter_mut()
    }

    /// Sort triples by SPO for deterministic output
    ///
    /// This enables stable, reproducible formatting regardless of insertion order.
    pub fn sort(&mut self) {
        self.triples.sort();
    }

    /// Remove duplicate triples (apply set semantics)
    ///
    /// Preserves the first occurrence of each triple.
    /// Call `sort()` first if you want deterministic results.
    pub fn dedupe(&mut self) {
        // Sort first to group duplicates
        self.triples.sort();
        self.triples.dedup();
    }

    /// Sort and dedupe in one pass (canonicalize)
    ///
    /// This is the standard way to prepare a graph for output when you
    /// want both deterministic ordering and set semantics.
    pub fn canonicalize(&mut self) {
        self.dedupe(); // dedupe already sorts
    }

    /// Check if the graph is sorted
    pub fn is_sorted(&self) -> bool {
        self.triples.windows(2).all(|w| w[0] <= w[1])
    }

    /// Get all triples (consuming the graph)
    pub fn into_triples(self) -> Vec<Triple> {
        self.triples
    }

    /// Get a reference to the triples
    pub fn triples(&self) -> &[Triple] {
        &self.triples
    }

    /// Group triples by subject
    ///
    /// Returns an iterator yielding (subject_term, triples_for_subject).
    /// The graph should be sorted first for consistent grouping.
    pub fn group_by_subject(&self) -> SubjectGroups<'_> {
        SubjectGroups {
            triples: &self.triples,
            index: 0,
        }
    }

    /// Get all unique subjects in the graph
    pub fn subjects(&self) -> Vec<&Term> {
        let mut subjects: Vec<&Term> = self.triples.iter().map(|t| &t.s).collect();
        subjects.sort();
        subjects.dedup();
        subjects
    }
}

impl IntoIterator for Graph {
    type Item = Triple;
    type IntoIter = std::vec::IntoIter<Triple>;

    fn into_iter(self) -> Self::IntoIter {
        self.triples.into_iter()
    }
}

impl<'a> IntoIterator for &'a Graph {
    type Item = &'a Triple;
    type IntoIter = std::slice::Iter<'a, Triple>;

    fn into_iter(self) -> Self::IntoIter {
        self.triples.iter()
    }
}

impl FromIterator<Triple> for Graph {
    fn from_iter<T: IntoIterator<Item = Triple>>(iter: T) -> Self {
        Graph {
            triples: iter.into_iter().collect(),
            base: None,
            prefixes: BTreeMap::new(),
        }
    }
}

impl Extend<Triple> for Graph {
    fn extend<T: IntoIterator<Item = Triple>>(&mut self, iter: T) {
        self.triples.extend(iter);
    }
}

/// Iterator over triples grouped by subject
///
/// Assumes the graph is sorted.
pub struct SubjectGroups<'a> {
    triples: &'a [Triple],
    index: usize,
}

impl<'a> Iterator for SubjectGroups<'a> {
    type Item = (&'a Term, &'a [Triple]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.triples.len() {
            return None;
        }

        let start = self.index;
        let subject = &self.triples[start].s;

        // Find the end of this subject's triples
        while self.index < self.triples.len() && self.triples[self.index].s == *subject {
            self.index += 1;
        }

        Some((subject, &self.triples[start..self.index]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_graph() -> Graph {
        let mut graph = Graph::new();

        // Add triples in non-sorted order
        graph.add_triple(
            Term::iri("http://example.org/bob"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Bob"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        graph.add_triple(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/age"),
            Term::integer(30),
        );

        graph
    }

    #[test]
    fn test_graph_creation() {
        let graph = Graph::new();
        assert!(graph.is_empty());
        assert_eq!(graph.len(), 0);
    }

    #[test]
    fn test_graph_add() {
        let mut graph = Graph::new();
        graph.add_triple(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("o"),
        );
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_graph_sort() {
        let mut graph = make_test_graph();

        assert!(!graph.is_sorted());
        graph.sort();
        assert!(graph.is_sorted());

        // Alice should come before Bob
        let first = graph.iter().next().unwrap();
        assert_eq!(first.s.as_iri(), Some("http://example.org/alice"));
    }

    #[test]
    fn test_graph_dedupe() {
        let mut graph = Graph::new();

        // Add duplicate triples
        let triple = Triple::new(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("o"),
        );

        graph.add(triple.clone());
        graph.add(triple.clone());
        graph.add(triple);

        assert_eq!(graph.len(), 3);

        graph.dedupe();
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_graph_prefixes() {
        let mut graph = Graph::new();
        graph.add_prefix("foaf", "http://xmlns.com/foaf/0.1/");
        graph.add_prefix("ex", "http://example.org/");

        assert_eq!(graph.prefixes.len(), 2);
        assert_eq!(
            graph.prefixes.get("foaf"),
            Some(&"http://xmlns.com/foaf/0.1/".to_string())
        );
    }

    #[test]
    fn test_group_by_subject() {
        let mut graph = make_test_graph();
        graph.sort();

        let groups: Vec<_> = graph.group_by_subject().collect();

        // Should have 2 subjects: alice and bob
        assert_eq!(groups.len(), 2);

        // Alice should be first (sorted), with 2 triples
        assert_eq!(groups[0].0.as_iri(), Some("http://example.org/alice"));
        assert_eq!(groups[0].1.len(), 2);

        // Bob second, with 1 triple
        assert_eq!(groups[1].0.as_iri(), Some("http://example.org/bob"));
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn test_from_iterator() {
        let triples = vec![Triple::new(
            Term::iri("http://example.org/s"),
            Term::iri("http://example.org/p"),
            Term::string("o"),
        )];

        let graph: Graph = triples.into_iter().collect();
        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_subjects() {
        let graph = make_test_graph();
        let subjects = graph.subjects();

        assert_eq!(subjects.len(), 2);
    }
}
