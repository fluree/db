//! FlakeSink — a `GraphSink` that converts parser events directly to `Vec<Flake>`
//!
//! This bypasses the Graph → JSON-LD → Txn IR → FlakeGenerator pipeline for
//! Turtle INSERT, converting parsed triples directly into assertion flakes.

use crate::generate::infer_datatype;
use crate::namespace::{NamespaceRegistry, NsAllocator};
use crate::value_convert::{convert_native_literal, convert_string_literal};
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// ResolvedTerm — internal term representation
// ---------------------------------------------------------------------------

/// A term resolved to its Flake-ready form.
///
/// Blank nodes are eagerly skolemized at `term_blank` time, so they are stored
/// as `Sid` just like IRIs. Only literals need a separate variant.
enum ResolvedTerm {
    /// IRI or blank node (already resolved to a Sid)
    Sid(Sid),
    /// Literal value with datatype constraint
    Literal {
        value: FlakeValue,
        dtc: DatatypeConstraint,
    },
}

// ---------------------------------------------------------------------------
// FlakeSink
// ---------------------------------------------------------------------------

/// A `GraphSink` that converts parser events directly to `Vec<Flake>`.
///
/// Used by the direct Turtle → Flakes INSERT path to avoid intermediate
/// Graph, JSON-LD, and Txn IR representations.
///
/// # Example
///
/// ```ignore
/// let mut ns = NamespaceRegistry::from_db(&db);
/// let mut sink = FlakeSink::new(&mut ns, new_t, txn_id);
/// fluree_graph_turtle::parse(ttl, &mut sink)?;
/// let flakes = sink.finish();
/// ```
pub struct FlakeSink<'a> {
    /// Resolved terms indexed by TermId
    terms: Vec<ResolvedTerm>,
    /// Labeled blank node → TermId cache (same label = same identity)
    blank_labels: HashMap<String, TermId>,
    /// Counter for anonymous blank nodes
    blank_counter: u32,
    /// Accumulated assertion flakes
    flakes: Vec<Flake>,
    /// Namespace registry for IRI → Sid conversion and code allocation
    ns_registry: &'a mut NamespaceRegistry,
    /// Transaction time stamp for all generated flakes
    t: i64,
    /// Transaction ID for blank node skolemization
    txn_id: String,
}

impl<'a> FlakeSink<'a> {
    /// Create a new FlakeSink.
    ///
    /// # Arguments
    /// * `ns_registry` — namespace registry (seeded from the ledger DB)
    /// * `t` — transaction time (`ledger.t() + 1`)
    /// * `txn_id` — unique ID for blank node skolemization
    pub fn new(ns_registry: &'a mut NamespaceRegistry, t: i64, txn_id: String) -> Self {
        Self {
            terms: Vec::new(),
            blank_labels: HashMap::new(),
            blank_counter: 0,
            flakes: Vec::new(),
            ns_registry,
            t,
            txn_id,
        }
    }

    /// Consume the sink and return the accumulated flakes.
    pub fn finish(self) -> Vec<Flake> {
        self.flakes
    }

    // -- helpers -------------------------------------------------------------

    fn add_term(&mut self, term: ResolvedTerm) -> TermId {
        let id = TermId::new(self.terms.len() as u32);
        self.terms.push(term);
        id
    }

    /// Skolemize a blank node label into a stable Sid.
    fn skolemize(&mut self, local: &str) -> Sid {
        let unique_id = format!("{}-{}", self.txn_id, local);
        self.ns_registry.blank_node_sid(&unique_id)
    }

    /// Resolve a TermId that must be a Sid (subject or predicate position).
    fn resolve_sid(&self, id: TermId) -> Option<Sid> {
        match &self.terms[id.index() as usize] {
            ResolvedTerm::Sid(sid) => Some(sid.clone()),
            ResolvedTerm::Literal { .. } => None, // literals invalid here
        }
    }

    /// Resolve a TermId in object position → (FlakeValue, DatatypeConstraint).
    fn resolve_object(&self, id: TermId) -> Option<(FlakeValue, DatatypeConstraint)> {
        match &self.terms[id.index() as usize] {
            ResolvedTerm::Sid(sid) => {
                let val = FlakeValue::Ref(sid.clone());
                let dt = infer_datatype(&val);
                Some((val, DatatypeConstraint::Explicit(dt)))
            }
            ResolvedTerm::Literal { value, dtc } => Some((value.clone(), dtc.clone())),
        }
    }

    /// Build a Flake from resolved subject/predicate/object with optional list index.
    fn build_flake(
        &self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        list_index: Option<i32>,
    ) -> Option<Flake> {
        let s = self.resolve_sid(subject)?;
        let p = self.resolve_sid(predicate)?;
        let (o, dtc) = self.resolve_object(object)?;

        let dt = dtc.datatype().clone();
        let lang = dtc.lang_tag().map(std::string::ToString::to_string);

        let meta = match (&lang, list_index) {
            (Some(l), Some(i)) => Some(FlakeMeta {
                lang: Some(l.clone()),
                i: Some(i),
            }),
            (Some(l), None) => Some(FlakeMeta::with_lang(l)),
            (None, Some(i)) => Some(FlakeMeta::with_index(i)),
            (None, None) => None,
        };

        Some(Flake::new(s, p, o, dt, self.t, true, meta))
    }
}

// ---------------------------------------------------------------------------
// GraphSink implementation
// ---------------------------------------------------------------------------

impl GraphSink for FlakeSink<'_> {
    fn on_base(&mut self, _base_iri: &str) {
        // No-op — the parser resolves relative IRIs before calling term_iri
    }

    fn on_prefix(&mut self, _prefix: &str, namespace_iri: &str) {
        // Pre-register the namespace IRI to ensure consistent code allocation
        self.ns_registry.get_or_allocate(namespace_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        let sid = self.ns_registry.sid_for_iri(iri);
        self.add_term(ResolvedTerm::Sid(sid))
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        match label {
            Some(l) => {
                // Dedup: same label within a transaction → same Sid
                if let Some(&id) = self.blank_labels.get(l) {
                    return id;
                }
                let sid = self.skolemize(l);
                let id = self.add_term(ResolvedTerm::Sid(sid));
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                // Anonymous blank node — unique counter-based label
                self.blank_counter += 1;
                let label = format!("b{}", self.blank_counter);
                let sid = self.skolemize(&label);
                self.add_term(ResolvedTerm::Sid(sid))
            }
        }
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        let dt_iri = datatype.as_iri();
        let (flake_value, dt_sid) =
            convert_string_literal(value, dt_iri, &mut NsAllocator::Exclusive(self.ns_registry));

        let dtc = match language {
            Some(lang) => DatatypeConstraint::LangTag(Arc::from(lang)),
            None => DatatypeConstraint::Explicit(dt_sid),
        };

        self.add_term(ResolvedTerm::Literal {
            value: flake_value,
            dtc,
        })
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        let flake_value = convert_native_literal(&value);
        let dt_sid = if datatype.is_json() {
            Sid::new(fluree_vocab::namespaces::RDF, "JSON")
        } else {
            infer_datatype(&flake_value)
        };

        self.add_term(ResolvedTerm::Literal {
            value: flake_value,
            dtc: DatatypeConstraint::Explicit(dt_sid),
        })
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) {
        if let Some(flake) = self.build_flake(subject, predicate, object, None) {
            self.flakes.push(flake);
        }
    }

    fn emit_list_item(&mut self, subject: TermId, predicate: TermId, object: TermId, index: i32) {
        if let Some(flake) = self.build_flake(subject, predicate, object, Some(index)) {
            self.flakes.push(flake);
        }
    }
}

// Value conversion helpers live in crate::value_convert (shared with ImportSink).

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::Datatype;
    use fluree_vocab::xsd;

    fn make_sink() -> (NamespaceRegistry, i64, String) {
        (NamespaceRegistry::new(), 1, "test-txn".to_string())
    }

    #[test]
    fn test_basic_iri_triple() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/name");
        let o = sink.term_literal("Alice", Datatype::xsd_string(), None);
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        let f = &flakes[0];
        assert!(f.op); // assertion
        assert_eq!(f.t, 1);
        assert!(matches!(&f.o, FlakeValue::String(s) if s == "Alice"));
        assert!(f.m.is_none());
    }

    #[test]
    fn test_integer_literal() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/age");
        let o = sink.term_literal_value(LiteralValue::Integer(30), Datatype::xsd_integer());
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Long(30)));
    }

    #[test]
    fn test_double_literal() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/x");
        let p = sink.term_iri("http://example.org/val");
        let o = sink.term_literal_value(LiteralValue::Double(3.13), Datatype::xsd_double());
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Double(d) if (*d - 3.13).abs() < f64::EPSILON));
    }

    #[test]
    fn test_boolean_literal() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/x");
        let p = sink.term_iri("http://example.org/active");
        let o = sink.term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean());
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Boolean(true)));
    }

    #[test]
    fn test_language_tagged_string() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/name");
        let o = sink.term_literal("Alice", Datatype::rdf_lang_string(), Some("en"));
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        let f = &flakes[0];
        assert!(matches!(&f.o, FlakeValue::String(s) if s == "Alice"));
        assert_eq!(f.dt, Sid::new(fluree_vocab::namespaces::RDF, "langString"));
        let meta = f.m.as_ref().expect("should have meta");
        assert_eq!(meta.lang.as_deref(), Some("en"));
        assert_eq!(meta.i, None);
    }

    #[test]
    fn test_blank_node_skolemization() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let b1 = sink.term_blank(Some("foo"));
        let b2 = sink.term_blank(Some("foo"));
        let b3 = sink.term_blank(Some("bar"));
        let b4 = sink.term_blank(None);

        // Same label → same TermId
        assert_eq!(b1, b2);
        // Different label → different TermId
        assert_ne!(b1, b3);
        // Anonymous → always unique
        assert_ne!(b3, b4);
    }

    #[test]
    fn test_iri_object_as_ref() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/knows");
        let o = sink.term_iri("http://example.org/bob");
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        let f = &flakes[0];
        assert!(matches!(&f.o, FlakeValue::Ref(_)));
        // dt should be $id (JSON_LD namespace, "id")
        assert_eq!(f.dt, Sid::new(fluree_vocab::namespaces::JSON_LD, "id"));
    }

    #[test]
    fn test_list_items() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/scores");
        let o0 = sink.term_literal_value(LiteralValue::Integer(10), Datatype::xsd_integer());
        let o1 = sink.term_literal_value(LiteralValue::Integer(20), Datatype::xsd_integer());
        let o2 = sink.term_literal_value(LiteralValue::Integer(30), Datatype::xsd_integer());
        sink.emit_list_item(s, p, o0, 0);
        sink.emit_list_item(s, p, o1, 1);
        sink.emit_list_item(s, p, o2, 2);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 3);
        for (i, f) in flakes.iter().enumerate() {
            let meta = f.m.as_ref().expect("list items should have meta");
            assert_eq!(meta.i, Some(i as i32));
            assert_eq!(meta.lang, None);
        }
    }

    #[test]
    fn test_typed_string_datetime() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/event");
        let p = sink.term_iri("http://example.org/date");
        let o = sink.term_literal("2024-01-15T10:30:00Z", Datatype::xsd_date_time(), None);
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::DateTime(_)));
    }

    #[test]
    fn test_typed_string_integer() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/x");
        let p = sink.term_iri("http://example.org/count");
        let o = sink.term_literal("42", Datatype::xsd_integer(), None);
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Long(42)));
    }

    #[test]
    fn test_typed_string_preserves_declared_datatype() {
        // "42"^^xsd:long should preserve xsd:long, not normalize to xsd:integer
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/x");
        let p = sink.term_iri("http://example.org/count");
        let o = sink.term_literal("42", Datatype::xsd_long(), None);
        sink.emit_triple(s, p, o);

        let flakes = sink.finish();
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Long(42)));
        // dt must be xsd:long (declared), not xsd:integer (inferred)
        let expected_dt = ns.sid_for_iri(xsd::LONG);
        assert_eq!(flakes[0].dt, expected_dt);
    }
}
