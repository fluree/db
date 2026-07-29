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

/// Error returned by a [`GraphSink`] when it cannot accept an event.
///
/// Emission is fallible so a sink that writes somewhere (a file, a socket,
/// a pipe) can report failure at the point it happens and have the producer
/// stop immediately. That is what makes `convert big.ttl | head -5` cost five
/// statements instead of a full parse against a dead pipe.
///
/// Producers MUST propagate this error rather than continuing to emit; a sink
/// that has failed once is not required to behave sensibly afterwards.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    /// The sink's downstream writer failed (broken pipe, disk full, …).
    #[error("sink I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The sink refused the event: an unsupported capability, or an
    /// invariant the event would violate.
    #[error("sink rejected event: {0}")]
    Rejected(String),
}

impl SinkError {
    /// Build a [`SinkError::Rejected`] from any message.
    pub fn rejected(message: impl Into<String>) -> Self {
        Self::Rejected(message.into())
    }

    /// Whether this error is a broken pipe.
    ///
    /// CLI drivers use this to exit quietly (status 0) when a downstream
    /// consumer such as `head` closes the pipe, rather than reporting the
    /// normal, expected shutdown as a conversion failure.
    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Io(e) if e.kind() == std::io::ErrorKind::BrokenPipe)
    }
}

/// Result alias for [`GraphSink`] emission methods.
pub type SinkResult = std::result::Result<(), SinkError>;

/// Opaque term identifier for efficient triple emission
///
/// `TermId` is only valid within a single sink session. It allows parsers
/// to reference terms efficiently without repeated string allocations.
///
/// # Lifetime
///
/// Term IDs fall into two lifetime classes, and producers must respect the
/// difference so that sinks are free to reclaim storage (see
/// [`GraphSink::end_statement`]):
///
/// - IDs from [`GraphSink::term_iri`] and [`GraphSink::term_blank`] are valid
///   for the whole sink session. Parsers cache and reuse them across
///   statements.
/// - IDs from [`GraphSink::term_literal`] and
///   [`GraphSink::term_literal_value`] are **statement-scoped**: valid only
///   until the next [`GraphSink::end_statement`] call. Literals are minted per
///   occurrence and never deduplicated, so retaining them across statements is
///   what makes a long document's term table grow without bound.
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
/// # Fallibility
///
/// `term_*` methods are infallible: they only intern a term and hand back an
/// id. `emit_*` methods return [`SinkResult`] because that is where a writing
/// sink actually touches its output. Producers must propagate emission errors
/// immediately (riot-style early termination) instead of parsing on against a
/// sink that has already failed.
///
/// # Finishing
///
/// [`GraphSink::finish`] is the *protocol* finish: flush and finalize, called
/// by whoever owns the sink once no more events are coming. It takes
/// `&mut self` and returns no value, because a sink may be fed by several
/// producer calls (bulk import parses one sink's worth of events per chunk).
/// Sinks that also *produce* something — a `Graph`, a `Vec<Flake>`, a commit
/// writer — expose that separately as a consuming `into_*` method, so that
/// `finish()` always means "flush", never "give me your contents".
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
/// sink.emit_triple(alice, name, alice_name).unwrap();
/// sink.finish().unwrap();
///
/// // Get the resulting graph
/// let graph = sink.into_graph();
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
    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult;

    /// Emit a list item (triple with list index)
    ///
    /// The `index` is the 0-based position in the list for this (subject, predicate).
    /// List items will be formatted as `{"@list": [...]}` in JSON-LD output.
    ///
    /// Default implementation falls back to `emit_triple` (ignoring index).
    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        // Default: fall back to regular triple (losing index info)
        // Implementations that support lists should override this
        let _ = index;
        self.emit_triple(subject, predicate, object)
    }

    /// Whether this sink can consume quad events ([`Self::emit_quad`]).
    ///
    /// Producers of named-graph syntaxes (N-Quads, TriG, JSON-LD `@graph`)
    /// MUST check this before emitting any quad and reject the input with a
    /// clear error when it returns `false`. Defaults to `false`: the IR is
    /// triple-only today, and *silently* folding named graphs into the
    /// default graph is a data-loss bug, not a fallback.
    fn supports_quads(&self) -> bool {
        false
    }

    /// Emit a quad: the triple `(subject, predicate, object)` in the named
    /// graph `graph`.
    ///
    /// Only called when [`Self::supports_quads`] returns `true`. Unlike
    /// [`Self::emit_reified_triple`] — whose producers are all guarded by an
    /// explicit parser-side capability check — the default body **errors**
    /// rather than no-ops, because no producer emits quads yet and this
    /// backstop is what keeps a future quad producer from silently dropping
    /// graph names against a triple-only sink.
    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        let _ = (subject, predicate, object, graph);
        debug_assert!(
            self.supports_quads(),
            "emit_quad called on a sink that does not support quads"
        );
        Err(SinkError::rejected(
            "this sink is triple-only and cannot represent named graphs",
        ))
    }

    /// Called by the producer at each statement terminator, after the
    /// statement has parsed and emitted successfully.
    ///
    /// Two uses, both of which need the same boundary:
    ///
    /// - **Term lifetime.** Statement-scoped literal ids (see [`TermId`]) are
    ///   dead after this call, so a sink may reclaim their storage. Without
    ///   it, one live term-table entry accumulates per literal *occurrence*
    ///   for the length of the document.
    /// - **Statement-scoped buffering.** A sink that buffers output per
    ///   statement (needed for `--continue-on-error`, because triples are
    ///   emitted during descent, before the terminating `.` proves the
    ///   statement valid) commits its buffer here. A statement that fails to
    ///   parse never reaches this call, so it contributes nothing.
    ///
    /// Default: no-op.
    fn end_statement(&mut self) {}

    /// Flush and finalize. Called once by whoever owns the sink, after the
    /// last producer has finished emitting.
    ///
    /// Producers do NOT call this — a single sink may be fed by many producer
    /// calls (one per chunk on the bulk-import path), and flushing per chunk
    /// would be wrong for a streaming writer.
    ///
    /// Default: no-op, for sinks that hold no external resource.
    fn finish(&mut self) -> SinkResult {
        Ok(())
    }

    /// Whether this sink can consume RDF 1.2 reified-triple events
    /// ([`Self::emit_reified_triple`]).
    ///
    /// Parsers MUST check this before emitting any reified-triple event
    /// and reject the input with a clear "deferred / unsupported on this
    /// path" error when it returns `false`. Defaults to `false` so
    /// existing sinks (graph collectors, directive preludes, …) keep
    /// rejecting Turtle-star input instead of silently dropping the
    /// reifier semantics.
    fn supports_reified_triples(&self) -> bool {
        false
    }

    /// Emit an RDF 1.2 reified-triple event: `reifier` reifies the base
    /// triple `(subject, predicate, object)`.
    ///
    /// Contract:
    /// - The parser has ALREADY emitted the base triple via
    ///   [`Self::emit_triple`] (Fluree's edge-annotation model reifies an
    ///   asserted edge). This event only records the reifier attachment.
    /// - The parser mints a FRESH blank-node reifier per anonymous
    ///   occurrence (`<< s p o >>` / `{| … |}` without `~ reifier`) and
    ///   never deduplicates reifiers by base-triple identity; sinks must
    ///   not dedup either.
    /// - Annotation-body properties (`{| p o |}`) arrive as ordinary
    ///   [`Self::emit_triple`] events with `reifier` as the subject.
    ///
    /// Only called when [`Self::supports_reified_triples`] returns `true`;
    /// the default implementation is a no-op guarded by a debug assert
    /// (every producer of these events already refuses the input up-front
    /// when the capability probe is `false`, so the default body is
    /// unreachable rather than a silent fallback).
    fn emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> SinkResult {
        let _ = (subject, predicate, object, reifier);
        debug_assert!(
            self.supports_reified_triples(),
            "emit_reified_triple called on a sink that does not support reified triples"
        );
        Ok(())
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

    /// Consume the sink and return the collected graph.
    ///
    /// This is the sink's *product*, distinct from the protocol's
    /// [`GraphSink::finish`] (flush/finalize) — see the trait docs.
    pub fn into_graph(self) -> Graph {
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

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.graph.add(Triple::new(s, p, o));
        Ok(())
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        let s = self.get_term(subject).clone();
        let p = self.get_term(predicate).clone();
        let o = self.get_term(object).clone();
        self.graph.add_list_item(s, p, o, index);
        Ok(())
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

        sink.emit_triple(s, p, o).unwrap();

        let graph = sink.into_graph();
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

        let graph = sink.into_graph();

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

        sink.emit_triple(s, p, o).unwrap();

        let graph = sink.into_graph();
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
        sink.emit_triple(s, p, bool_val).unwrap();

        // Integer
        let int_val = sink.term_literal_value(LiteralValue::Integer(42), Datatype::xsd_integer());
        sink.emit_triple(s, p, int_val).unwrap();

        // Double
        let double_val =
            sink.term_literal_value(LiteralValue::Double(3.13), Datatype::xsd_double());
        sink.emit_triple(s, p, double_val).unwrap();

        let graph = sink.into_graph();
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn test_collector_sink_list_items() {
        let mut sink = GraphCollectorSink::new();

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/friends");

        // Emit list items out of order
        let o2 = sink.term_literal("Charlie", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o2, 2).unwrap();

        let o0 = sink.term_literal("Alice", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o0, 0).unwrap();

        let o1 = sink.term_literal("Bob", Datatype::xsd_string(), None);
        sink.emit_list_item(s, p, o1, 1).unwrap();

        let mut graph = sink.into_graph();
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

    // =====================================================================
    // Protocol: capability probes, fallible emission, lifecycle
    // =====================================================================

    /// A sink that claims quad support but never overrides `emit_quad`.
    ///
    /// Exists to reach the trait's default `emit_quad` body past its
    /// `debug_assert` — that body is the release-build backstop against a
    /// future quad producer silently folding graph names into the default
    /// graph, so it must be an error, not a no-op.
    #[derive(Default)]
    struct QuadClaimingSink {
        terms: u32,
    }

    impl GraphSink for QuadClaimingSink {
        fn on_base(&mut self, _base_iri: &str) {}
        fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {}
        fn term_iri(&mut self, _iri: &str) -> TermId {
            self.terms += 1;
            TermId::new(self.terms)
        }
        fn term_blank(&mut self, _label: Option<&str>) -> TermId {
            TermId::new(0)
        }
        fn term_literal(
            &mut self,
            _value: &str,
            _datatype: Datatype,
            _language: Option<&str>,
        ) -> TermId {
            TermId::new(0)
        }
        fn term_literal_value(&mut self, _value: LiteralValue, _datatype: Datatype) -> TermId {
            TermId::new(0)
        }
        fn emit_triple(&mut self, _s: TermId, _p: TermId, _o: TermId) -> SinkResult {
            Ok(())
        }
        fn supports_quads(&self) -> bool {
            true
        }
    }

    #[test]
    fn triple_only_sinks_do_not_claim_quad_support() {
        // The capability probe producers of N-Quads/TriG/`@graph` must
        // consult before emitting.
        let sink = GraphCollectorSink::new();
        assert!(!sink.supports_quads());
        assert!(!sink.supports_reified_triples());
    }

    #[test]
    fn default_emit_quad_refuses_instead_of_dropping_the_graph_name() {
        let mut sink = QuadClaimingSink::default();
        let g = sink.term_iri("http://example.org/g");
        let s = sink.term_iri("http://example.org/s");
        let p = sink.term_iri("http://example.org/p");
        let o = sink.term_iri("http://example.org/o");

        let err = sink
            .emit_quad(s, p, o, g)
            .expect_err("the default quad body must refuse, never silently fall back");
        assert!(matches!(err, SinkError::Rejected(_)), "{err:?}");
        assert!(err.to_string().contains("named graphs"), "{err}");
    }

    #[test]
    fn protocol_finish_defaults_to_a_no_op_and_leaves_the_product_alone() {
        // `finish()` flushes; `into_graph()` produces. Calling the former
        // must not disturb the latter — the two are deliberately separate
        // methods so `finish()` can never mean "give me your contents".
        let mut sink = GraphCollectorSink::new();
        let s = sink.term_iri("http://example.org/s");
        let p = sink.term_iri("http://example.org/p");
        let o = sink.term_literal("v", Datatype::xsd_string(), None);
        sink.emit_triple(s, p, o).unwrap();

        sink.finish().expect("default finish is infallible");
        sink.finish().expect("and idempotent");

        assert_eq!(sink.into_graph().len(), 1);
    }

    #[test]
    fn broken_pipe_is_distinguishable_from_other_sink_failures() {
        // CLI drivers exit quietly on a closed downstream (`… | head -5`)
        // but must still report real failures.
        let pipe = SinkError::Io(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "closed",
        ));
        assert!(pipe.is_broken_pipe());

        let full = SinkError::Io(std::io::Error::other("no space left on device"));
        assert!(!full.is_broken_pipe());

        let rejected = SinkError::rejected("nope");
        assert!(!rejected.is_broken_pipe());
    }
}
