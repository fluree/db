//! GraphSink trait for event-driven graph construction
//!
//! This module provides a Raphael-style event interface for parsers to emit
//! graph events without knowing the concrete sink type.
//!
//! # Design
//!
//! Parsers call methods like `term_iri()` and `emit_triple()` on a sink.
//! The sink can be:
//! - `GraphCollectorSink`: Collects events into a `Graph`
//! - Future: `FlureeIngestSink`: Converts events to transaction IR
//! - Future: `StreamingSink`: Writes triples directly to output

use crate::{Datatype, Graph, LiteralValue, Term, Triple};
use std::collections::HashMap;

/// Opaque term identifier for efficient triple emission
///
/// `TermId` is only valid within a single sink session. It allows parsers
/// to reference terms efficiently without repeated string allocations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TermId(pub(crate) u32);

impl TermId {
    /// Create a new TermId from a raw index.
    ///
    /// This is intended for `GraphSink` implementations outside this crate
    /// that need to allocate term IDs.
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the raw index value.
    pub fn index(self) -> u32 {
        self.0
    }
}

/// Event-driven interface for RDF graph construction
///
/// Inspired by the Raphael library's generator pattern, this trait allows
/// parsers to emit graph events without knowing the concrete sink type.
///
/// # Example
///
/// ```
/// use fluree_graph_ir::{GraphSink, GraphCollectorSink, Datatype};
///
/// let mut sink = GraphCollectorSink::new();
///
/// // Declare prefixes
/// sink.on_prefix("foaf", "http://xmlns.com/foaf/0.1/");
///
/// // Create terms
/// let alice = sink.term_iri("http://example.org/alice");
/// let name = sink.term_iri("http://xmlns.com/foaf/0.1/name");
/// let alice_name = sink.term_literal("Alice", Datatype::xsd_string(), None);
///
/// // Emit triple
/// sink.emit_triple(alice, name, alice_name);
///
/// // Get the resulting graph
/// let graph = sink.finish();
/// assert_eq!(graph.len(), 1);
/// ```
pub trait GraphSink {
    /// Called when a base IRI is declared
    ///
    /// In Turtle: `@base <http://example.org/> .`
    /// In JSON-LD: `"@base": "http://example.org/"`
    fn on_base(&mut self, base_iri: &str);

    /// Called when a prefix is declared
    ///
    /// In Turtle: `@prefix foaf: <http://xmlns.com/foaf/0.1/> .`
    /// In JSON-LD: `"foaf": "http://xmlns.com/foaf/0.1/"` in @context
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str);

    /// Create an IRI term and return its ID
    ///
    /// The IRI should be fully expanded (not prefixed).
    fn term_iri(&mut self, iri: &str) -> TermId;

    /// Create a blank node term and return its ID
    ///
    /// If `label` is Some, the blank node has that label (for consistent
    /// identity across references). If None, generate a fresh blank node.
    fn term_blank(&mut self, label: Option<&str>) -> TermId;

    /// Create a literal term from a string value
    ///
    /// The value is the lexical form of the literal.
    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId;

    /// Create a literal term from a native value
    ///
    /// Use this for non-string values (boolean, integer, double, JSON).
    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId;

    /// Emit a triple using previously created term IDs
    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId);

    /// Emit a list item (triple with list index)
    ///
    /// The `index` is the 0-based position in the list for this (subject, predicate).
    /// List items will be formatted as `{"@list": [...]}` in JSON-LD output.
    ///
    /// Default implementation falls back to `emit_triple` (ignoring index).
    fn emit_list_item(&mut self, subject: TermId, predicate: TermId, object: TermId, index: i32) {
        // Default: fall back to regular triple (losing index info)
        // Implementations that support lists should override this
        let _ = index;
        self.emit_triple(subject, predicate, object);
    }
}

/// A sink that collects triples into a Graph
///
/// This is the standard sink for building an in-memory graph from parser events.
#[derive(Debug)]
pub struct GraphCollectorSink {
    /// The graph being built
    graph: Graph,
    /// Terms indexed by TermId
    terms: Vec<Term>,
    /// Counter for generating blank node IDs
    blank_counter: u32,
    /// Cache for blank node labels to TermId mapping
    blank_labels: HashMap<String, TermId>,
}

impl GraphCollectorSink {
    /// Create a new collector sink
    pub fn new() -> Self {
        Self {
            graph: Graph::new(),
            terms: Vec::new(),
            blank_counter: 0,
            blank_labels: HashMap::new(),
        }
    }

    /// Create a sink with a pre-configured base IRI
    pub fn with_base(base: impl Into<String>) -> Self {
        Self {
            graph: Graph::with_base(base),
            terms: Vec::new(),
            blank_counter: 0,
            blank_labels: HashMap::new(),
        }
    }

    /// Finish building and return the graph
    ///
    /// Consumes the sink.
    pub fn finish(self) -> Graph {
        self.graph
    }

    /// Get the current graph (non-consuming)
    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    /// Get the current graph mutably
    pub fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    /// Get a term by its ID
    fn get_term(&self, id: TermId) -> &Term {
        &self.terms[id.0 as usize]
    }

    /// Add a term and return its ID
    fn add_term(&mut self, term: Term) -> TermId {
        let id = TermId(self.terms.len() as u32);
        self.terms.push(term);
        id
    }
}

impl Default for GraphCollectorSink {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSink for GraphCollectorSink {
    fn on_base(&mut self, base_iri: &str) {
        self.graph.set_base(base_iri);
    }

    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.graph.add_prefix(prefix, namespace_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        self.add_term(Term::iri(iri))
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            Some(l) => {
                // Check if we've seen this label before
                if let Some(&id) = self.blank_labels.get(l) {
                    return id;
                }

                // Create new blank node with this label
                let id = self.add_term(Term::blank(l));
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                // Generate a fresh blank node ID
                self.blank_counter += 1;
                let label = format!("b{}", self.blank_counter);
                self.add_term(Term::blank(label))
            }
        }
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        let term = match language {
            Some(lang) => Term::lang_string(value, lang),
            None if datatype.is_xsd_string() => Term::string(value),
            None => Term::typed(value, datatype),
        };
        self.add_term(term)
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        let term = Term::Literal {
            value,
            datatype,
            language: None,
        };
        self.add_term(term)
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.graph.add(Triple::new(s, p, o));
    }

    fn emit_list_item(&mut self, subject: TermId, predicate: TermId, object: TermId, index: i32) {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.graph.add_list_item(s, p, o, index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_sink_basic() {
        let mut sink = GraphCollectorSink::new();

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://xmlns.com/foaf/0.1/name");
        let o = sink.term_literal("Alice", Datatype::xsd_string(), None);

        sink.emit_triple(s, p, o);

        let graph = sink.finish();
        assert_eq!(graph.len(), 1);

        let triple = graph.iter().next().unwrap();
        assert_eq!(triple.s.as_iri(), Some("http://example.org/alice"));
        assert_eq!(triple.p.as_iri(), Some("http://xmlns.com/foaf/0.1/name"));
    }

    #[test]
    fn test_collector_sink_blank_nodes() {
        let mut sink = GraphCollectorSink::new();

        // Same label should produce same TermId
        let b1 = sink.term_blank(Some("b0"));
        let b2 = sink.term_blank(Some("b0"));
        assert_eq!(b1, b2);

        // Different labels should produce different TermIds
        let b3 = sink.term_blank(Some("b1"));
        assert_ne!(b1, b3);

        // Anonymous blank nodes get sequential IDs
        let anon1 = sink.term_blank(None);
        let anon2 = sink.term_blank(None);
        assert_ne!(anon1, anon2);
    }

    #[test]
    fn test_collector_sink_prefixes() {
        let mut sink = GraphCollectorSink::new();

        sink.on_base("http://example.org/");
        sink.on_prefix("foaf", "http://xmlns.com/foaf/0.1/");

        let graph = sink.finish();

        assert_eq!(graph.base, Some("http://example.org/".to_string()));
        assert_eq!(
            graph.prefixes.get("foaf"),
            Some(&"http://xmlns.com/foaf/0.1/".to_string())
        );
    }

    #[test]
    fn test_collector_sink_language_literal() {
        let mut sink = GraphCollectorSink::new();

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://xmlns.com/foaf/0.1/name");
        let o = sink.term_literal("Alicia", Datatype::rdf_lang_string(), Some("es"));

        sink.emit_triple(s, p, o);

        let graph = sink.finish();
        let triple = graph.iter().next().unwrap();

        if let Term::Literal {
            language, datatype, ..
        } = &triple.o
        {
            assert_eq!(language.as_deref(), Some("es"));
            assert!(datatype.is_lang_string());
        } else {
            panic!("Expected literal");
        }
    }

    #[test]
    fn test_collector_sink_literal_values() {
        let mut sink = GraphCollectorSink::new();

        let s = sink.term_iri("http://example.org/test");
        let p = sink.term_iri("http://example.org/value");

        // Boolean
        let bool_val =
            sink.term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean());
        sink.emit_triple(s, p, bool_val);

        // Integer
        let int_val = sink.term_literal_value(LiteralValue::Integer(42), Datatype::xsd_integer());
        sink.emit_triple(s, p, int_val);

        // Double
        let double_val =
            sink.term_literal_value(LiteralValue::Double(3.13), Datatype::xsd_double());
        sink.emit_triple(s, p, double_val);

        let graph = sink.finish();
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn test_collector_sink_list_items() {
        let mut sink = GraphCollectorSink::new();

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/friends");

        // Emit list items out of order
        let o2 = sink.term_literal("Charlie", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o2, 2);

        let o0 = sink.term_literal("Alice", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o0, 0);

        let o1 = sink.term_literal("Bob", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o1, 1);

        let mut graph = sink.finish();
        assert_eq!(graph.len(), 3);

        // All triples should have list_index
        for triple in graph.iter() {
            assert!(triple.is_list_element());
        }

        // After sorting, should be in list order (0, 1, 2)
        graph.sort();
        let indices: Vec<_> = graph.iter().map(|t| t.list_index().unwrap()).collect();
        assert_eq!(indices, vec![0, 1, 2]);
    }
}
