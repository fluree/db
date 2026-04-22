//! RDF triple (statement)
//!
//! A triple represents a single RDF statement: subject-predicate-object.
//!
//! # List support
//!
//! Triples can optionally carry a `list_index` for representing ordered lists.
//! This avoids RDF's rdf:first/rest/nil chains by storing list elements as normal
//! triples with positional metadata:
//!
//! - `list_index: None` - normal multi-valued predicate (unordered)
//! - `list_index: Some(i)` - list element at position `i` for this (subject, predicate)
//!
//! This aligns with Fluree's FlakeMeta.i storage model.

use crate::Term;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// An RDF triple (subject-predicate-object)
///
/// # Invariants
///
/// - Subject can be IRI or blank node (not literal)
/// - Predicate must be IRI (not blank node or literal)
/// - Object can be IRI, blank node, or literal
///
/// These invariants are not enforced at construction time for flexibility,
/// but formatters may produce invalid output if violated.
///
/// # List elements
///
/// For list elements, `list_index` contains the position (0-based).
/// When formatting, triples with the same (subject, predicate) and non-None
/// list_index are grouped and output as `{"@list": [...]}` in JSON-LD.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Triple {
    /// Subject (IRI or blank node)
    pub s: Term,
    /// Predicate (IRI only)
    pub p: Term,
    /// Object (IRI, blank node, or literal)
    pub o: Term,
    /// Optional list index for ordered collections
    ///
    /// - `None`: normal triple (unordered multi-value)
    /// - `Some(i)`: list element at position `i`
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub list_index: Option<i32>,
}

impl Triple {
    /// Create a new triple (unordered, not a list element)
    pub fn new(s: Term, p: Term, o: Term) -> Self {
        Self {
            s,
            p,
            o,
            list_index: None,
        }
    }

    /// Create a triple that is part of an ordered list
    ///
    /// The `index` is the 0-based position in the list.
    pub fn with_list_index(s: Term, p: Term, o: Term, index: i32) -> Self {
        Self {
            s,
            p,
            o,
            list_index: Some(index),
        }
    }

    /// Get the subject
    pub fn subject(&self) -> &Term {
        &self.s
    }

    /// Get the predicate
    pub fn predicate(&self) -> &Term {
        &self.p
    }

    /// Get the object
    pub fn object(&self) -> &Term {
        &self.o
    }

    /// Get the list index, if this is a list element
    pub fn list_index(&self) -> Option<i32> {
        self.list_index
    }

    /// Check if this triple is a list element
    pub fn is_list_element(&self) -> bool {
        self.list_index.is_some()
    }

    /// Check if the predicate is rdf:type
    pub fn is_rdf_type(&self) -> bool {
        matches!(&self.p, Term::Iri(iri) if iri.as_ref() == crate::datatype::iri::RDF_TYPE)
    }
}

impl PartialOrd for Triple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Triple {
    /// SPO + list_index ordering
    ///
    /// Triples are ordered by subject, predicate, list_index (None < Some), then object.
    /// This ensures list elements with the same (subject, predicate) appear consecutively
    /// and in list order.
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.s, &self.p, &self.list_index, &self.o).cmp(&(
            &other.s,
            &other.p,
            &other.list_index,
            &other.o,
        ))
    }
}

impl std::fmt::Display for Triple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {} .", self.s, self.p, self.o)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_vocab::rdf;

    #[test]
    fn test_triple_creation() {
        let t = Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        assert!(t.subject().is_iri());
        assert!(t.predicate().is_iri());
        assert!(t.object().is_literal());
    }

    #[test]
    fn test_triple_ordering() {
        let t1 = Triple::new(
            Term::iri("http://a.org"),
            Term::iri("http://p.org"),
            Term::string("x"),
        );

        let t2 = Triple::new(
            Term::iri("http://a.org"),
            Term::iri("http://p.org"),
            Term::string("y"),
        );

        let t3 = Triple::new(
            Term::iri("http://b.org"),
            Term::iri("http://p.org"),
            Term::string("x"),
        );

        // Same S, same P, different O
        assert!(t1 < t2);

        // Different S
        assert!(t1 < t3);
        assert!(t2 < t3);
    }

    #[test]
    fn test_is_rdf_type() {
        let type_triple = Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri(rdf::TYPE),
            Term::iri("http://xmlns.com/foaf/0.1/Person"),
        );
        assert!(type_triple.is_rdf_type());

        let other_triple = Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );
        assert!(!other_triple.is_rdf_type());
    }

    #[test]
    fn test_triple_display() {
        let t = Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri("http://xmlns.com/foaf/0.1/name"),
            Term::string("Alice"),
        );

        let display = format!("{t}");
        assert!(display.contains("<http://example.org/alice>"));
        assert!(display.contains("<http://xmlns.com/foaf/0.1/name>"));
        assert!(display.contains("\"Alice\""));
        assert!(display.ends_with(" ."));
    }

    #[test]
    fn test_list_index() {
        // Normal triple has no list index
        let t = Triple::new(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/likes"),
            Term::string("cats"),
        );
        assert!(!t.is_list_element());
        assert_eq!(t.list_index(), None);

        // List element has index
        let t_list = Triple::with_list_index(
            Term::iri("http://example.org/alice"),
            Term::iri("http://example.org/likes"),
            Term::string("cats"),
            0,
        );
        assert!(t_list.is_list_element());
        assert_eq!(t_list.list_index(), Some(0));
    }

    #[test]
    fn test_list_ordering() {
        // Same (s, p) but different list indices
        let s = Term::iri("http://example.org/alice");
        let p = Term::iri("http://example.org/list");

        // Normal triple (None) comes before list elements
        let normal = Triple::new(s.clone(), p.clone(), Term::string("other"));

        let list_0 = Triple::with_list_index(s.clone(), p.clone(), Term::string("a"), 0);
        let list_1 = Triple::with_list_index(s.clone(), p.clone(), Term::string("b"), 1);
        let list_2 = Triple::with_list_index(s.clone(), p.clone(), Term::string("c"), 2);

        // None < Some(0) < Some(1) < Some(2)
        assert!(normal < list_0);
        assert!(list_0 < list_1);
        assert!(list_1 < list_2);

        // Sorting should preserve list order
        let mut triples = [
            list_2.clone(),
            list_0.clone(),
            list_1.clone(),
            normal.clone(),
        ];
        triples.sort();

        assert_eq!(triples[0], normal);
        assert_eq!(triples[1], list_0);
        assert_eq!(triples[2], list_1);
        assert_eq!(triples[3], list_2);
    }
}
