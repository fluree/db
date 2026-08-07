//! RDF quad — a triple plus the graph it belongs to
//!
//! A quad is the unit of an RDF *dataset*: the same subject-predicate-object
//! statement, qualified by which graph asserts it. `graph: None` means the
//! default graph, which is what every triple-only syntax (Turtle,
//! N-Triples) produces and what a [`Triple`] alone already means.
//!
//! # Why not a fourth `Term` field
//!
//! [`Quad`] wraps a whole [`Triple`] rather than flattening s/p/o alongside a
//! graph term. Everything that already consumes triples — ordering, list
//! indices, `is_rdf_type`, the formatters — keeps working on `quad.triple`
//! unchanged, and converting a triple stream into a quad stream costs no
//! rewrite of that code.

use crate::{Term, Triple};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// An RDF quad: a [`Triple`] and the graph that asserts it.
///
/// # Invariants
///
/// - The triple's own invariants apply unchanged (see [`Triple`]).
/// - `graph` is an IRI or a blank node when present — never a literal. Like
///   [`Triple`], this is *not* enforced at construction time; the collecting
///   sink ([`crate::DatasetCollectorSink`]) is the layer that refuses a
///   literal graph name, because it keys storage by that term.
/// - `graph: None` is the default graph. It is a distinct graph, not "no
///   graph": `<s> <p> <o> .` and `<s> <p> <o> <g> .` are two different quads.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{Quad, Term, Triple};
///
/// let triple = Triple::new(
///     Term::iri("http://example.org/alice"),
///     Term::iri("http://xmlns.com/foaf/0.1/name"),
///     Term::string("Alice"),
/// );
///
/// let default = Quad::in_default_graph(triple.clone());
/// assert!(default.is_default_graph());
///
/// let named = Quad::in_named_graph(triple, Term::iri("http://example.org/g"));
/// assert_eq!(named.graph_name().and_then(Term::as_iri), Some("http://example.org/g"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Quad {
    /// The statement itself
    pub triple: Triple,
    /// The graph asserting it, or `None` for the default graph
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub graph: Option<Term>,
}

impl Quad {
    /// Create a quad from a triple and an optional graph name
    pub fn new(triple: Triple, graph: Option<Term>) -> Self {
        Self { triple, graph }
    }

    /// Create a quad in the default graph
    pub fn in_default_graph(triple: Triple) -> Self {
        Self {
            triple,
            graph: None,
        }
    }

    /// Create a quad in a named graph
    pub fn in_named_graph(triple: Triple, graph: Term) -> Self {
        Self {
            triple,
            graph: Some(graph),
        }
    }

    /// Whether this quad is in the default graph
    pub fn is_default_graph(&self) -> bool {
        self.graph.is_none()
    }

    /// The graph name, or `None` for the default graph
    pub fn graph_name(&self) -> Option<&Term> {
        self.graph.as_ref()
    }

    /// Get the subject
    pub fn subject(&self) -> &Term {
        &self.triple.s
    }

    /// Get the predicate
    pub fn predicate(&self) -> &Term {
        &self.triple.p
    }

    /// Get the object
    pub fn object(&self) -> &Term {
        &self.triple.o
    }

    /// Discard the graph name, keeping the statement
    pub fn into_triple(self) -> Triple {
        self.triple
    }
}

impl PartialOrd for Quad {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Quad {
    /// Graph-major ordering: graph first, then the triple's own SPO order.
    ///
    /// Sorting quads therefore groups each graph's statements together, which
    /// is what every dataset serialization wants (TriG emits one block per
    /// graph; N-Quads is readable that way). The default graph sorts first
    /// because `None < Some(_)`.
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.graph, &self.triple).cmp(&(&other.graph, &other.triple))
    }
}

impl std::fmt::Display for Quad {
    /// N-Quads line shape: `s p o g .`, or `s p o .` in the default graph.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Triple { s, p, o, .. } = &self.triple;
        write!(f, "{s} {p} {o}")?;
        if let Some(g) = &self.graph {
            write!(f, " {g}")?;
        }
        write!(f, " .")
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

    #[test]
    fn default_and_named_graphs_are_distinguishable() {
        let d = Quad::in_default_graph(triple("x"));
        let n = Quad::in_named_graph(triple("x"), Term::iri("http://example.org/g"));

        assert!(d.is_default_graph());
        assert!(!n.is_default_graph());
        assert_eq!(d.graph_name(), None);
        assert_eq!(n.graph_name(), Some(&Term::iri("http://example.org/g")));

        // Same statement, different graph — NOT the same quad. This is the
        // whole point of the type: folding these together is the data-loss
        // bug the quad-capable path exists to prevent.
        assert_ne!(d, n);
    }

    #[test]
    fn ordering_is_graph_major_with_the_default_graph_first() {
        let g1 = Term::iri("http://example.org/g1");
        let g2 = Term::iri("http://example.org/g2");

        let mut quads = [
            Quad::in_named_graph(triple("b"), g2.clone()),
            Quad::in_named_graph(triple("a"), g1.clone()),
            Quad::in_default_graph(triple("z")),
            Quad::in_named_graph(triple("a"), g2.clone()),
        ];
        quads.sort();

        let names: Vec<Option<&str>> = quads
            .iter()
            .map(|q| q.graph_name().and_then(Term::as_iri))
            .collect();
        assert_eq!(
            names,
            vec![
                None,
                Some("http://example.org/g1"),
                Some("http://example.org/g2"),
                Some("http://example.org/g2"),
            ]
        );

        // Within a graph, the triple's own SPO order still decides.
        assert_eq!(quads[2].object(), &Term::string("a"));
        assert_eq!(quads[3].object(), &Term::string("b"));
    }

    #[test]
    fn display_is_an_n_quads_line() {
        let d = Quad::in_default_graph(triple("x"));
        assert_eq!(
            d.to_string(),
            r#"<http://example.org/s> <http://example.org/p> "x" ."#
        );

        let n = Quad::in_named_graph(triple("x"), Term::iri("http://example.org/g"));
        assert_eq!(
            n.to_string(),
            r#"<http://example.org/s> <http://example.org/p> "x" <http://example.org/g> ."#
        );

        // A blank-node graph name is legal and serializes as a blank.
        let b = Quad::in_named_graph(triple("x"), Term::blank("g0"));
        assert!(b.to_string().ends_with("_:g0 ."), "{b}");
    }

    #[test]
    fn the_default_graph_round_trips_through_serde_without_a_graph_key() {
        let d = Quad::in_default_graph(triple("x"));
        let json = serde_json::to_string(&d).unwrap();
        assert!(!json.contains("graph"), "{json}");
        assert_eq!(serde_json::from_str::<Quad>(&json).unwrap(), d);

        let n = Quad::in_named_graph(triple("x"), Term::iri("http://example.org/g"));
        let json = serde_json::to_string(&n).unwrap();
        assert_eq!(serde_json::from_str::<Quad>(&json).unwrap(), n);
    }
}
