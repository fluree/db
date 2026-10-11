//! FlakeSink — a `GraphSink` that converts parser events directly to `Vec<Flake>`
//!
//! This bypasses the Graph → JSON-LD → Txn IR → FlakeGenerator pipeline for
//! Turtle INSERT, converting parsed triples directly into assertion flakes.

use crate::error::TransactError;
use crate::generate::{infer_datatype, validate_value_dt_pair};
use crate::namespace::{NamespaceRegistry, NsAllocator};
use crate::value_convert::{convert_native_literal, convert_string_literal};
use fluree_db_core::DatatypeConstraint;
use fluree_db_core::{Flake, FlakeMeta, FlakeValue, Sid};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkError, SinkResult, TermId};
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
/// let flakes = sink.into_flakes().expect("no invariant violation");
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
    /// First storage-invariant violation observed during parse, if any.
    /// Surfaced by `finish()` so the caller fails the whole transaction
    /// rather than silently committing a partial flake set.
    invariant_error: Option<TransactError>,
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
            invariant_error: None,
        }
    }

    /// Consume the sink and return the accumulated flakes, or surface the
    /// first storage-invariant violation observed during parsing as a hard
    /// error. Mirrors
    /// [`ImportSink::into_parts`](crate::import_sink::ImportSink::into_parts)
    /// — bad input must fail the transaction, not silently omit a triple.
    ///
    /// Distinct from the protocol's [`GraphSink::finish`] (flush/finalize):
    /// this is the sink's *product*. Deferred-error semantics are unchanged
    /// by the fallible protocol — `emit_*` still records the first violation
    /// and keeps going, and this method is where it becomes a hard error.
    pub fn into_flakes(self) -> Result<Vec<Flake>, TransactError> {
        if let Some(err) = self.invariant_error {
            return Err(err);
        }
        Ok(self.flakes)
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
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        list_index: Option<i32>,
    ) -> Option<Flake> {
        let s = self.resolve_sid(subject)?;
        let p = self.resolve_sid(predicate)?;

        // Reserved-predicate firewall (mirrors the JSON-LD and SPARQL UPDATE
        // surfaces): a user-authored `f:reifies*` statement must not reach
        // stage. These predicates are the retired attachment encoding; the
        // RDF 1.2 annotation syntax (`~` / `{| |}` / `<< >>`) arrives via
        // `emit_reified_triple` and writes the `rdf:reifies` link.
        // Bulk import (`ImportSink`) is the administrative bootstrap path and
        // deliberately stays permissive so an export round-trips.
        if fluree_db_core::is_reserved_reifies_predicate(&p) {
            let iri = format!(
                "{}{}",
                self.ns_registry.get_prefix(p.namespace_code).unwrap_or(""),
                p.name
            );
            let e = TransactError::UnsupportedFeature(format!(
                "'{iri}' is a system-controlled predicate; use the RDF 1.2 annotation \
                 syntax (`~ <reifier> {{| ... |}}` or `<< s p o >>`) instead of \
                 writing f:reifies* triples by hand"
            ));
            tracing::error!("FlakeSink: reserved predicate, aborting — {e}");
            if self.invariant_error.is_none() {
                self.invariant_error = Some(e);
            }
            return None;
        }

        let (o, dtc) = self.resolve_object(object)?;

        // `rdf:reifies` names a triple term; any other object is a data
        // error, and one the link lowering would read as a reifier of
        // nothing. The reified-triple forms never reach here: the parser
        // hands them to `emit_reified_triple`.
        if fluree_db_core::is_rdf_reifies(&p) && !matches!(o, FlakeValue::TripleTerm(_)) {
            let e = TransactError::UnsupportedFeature(
                "'rdf:reifies' takes a triple term as its object; write the reified-triple \
                 form (`<< s p o >>` or `~ <reifier>`) rather than an ordinary object"
                    .to_string(),
            );
            tracing::error!("FlakeSink: rdf:reifies with a non-term object, aborting — {e}");
            if self.invariant_error.is_none() {
                self.invariant_error = Some(e);
            }
            return None;
        }

        let dt = dtc.datatype().clone();
        let lang = dtc.lang_tag().map(std::string::ToString::to_string);

        // Late hard guard: refuse to emit (FlakeValue, dt) shapes that would
        // produce corrupt flakes. Capture the first violation on the sink so
        // `finish()` can surface it as a hard transaction error — silently
        // dropping triples would let `stage_turtle_insert` succeed with
        // partial data, which is worse than failing loudly.
        if let Err(e) = validate_value_dt_pair(&o, &dt) {
            tracing::error!("FlakeSink: invariant violation, aborting — {e}");
            if self.invariant_error.is_none() {
                self.invariant_error = Some(e);
            }
            return None;
        }

        let meta = FlakeMeta::from_parts(lang.as_deref(), list_index);

        Some(Flake::new(s, p, o, dt, self.t, true, meta))
    }
}

/// The triple-term value of a resolved `<<( s p o )>>`.
pub(crate) fn triple_term_value(
    s: Option<Sid>,
    p: Option<Sid>,
    o: Option<(FlakeValue, DatatypeConstraint)>,
) -> Result<FlakeValue, SinkError> {
    let (Some(s), Some(p), Some((o, dtc))) = (s, p, o) else {
        return Err(SinkError::rejected(
            "a triple term's subject and predicate must be IRIs or blank nodes",
        ));
    };
    Ok(FlakeValue::TripleTerm(Box::new(
        fluree_db_core::TripleTermValue {
            s,
            p,
            o,
            dt: dtc.datatype().clone(),
            lang: dtc.lang_tag().map(str::to_string),
        },
    )))
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
                // Stable Fluree blank-node ids (`fdb-...`) address the
                // existing stored node; other labels skolemize fresh.
                let sid = crate::namespace::stable_blank_node_sid_from_label(l)
                    .unwrap_or_else(|| self.skolemize(l));
                let id = self.add_term(ResolvedTerm::Sid(sid));
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                // Anonymous blank node (`[]`, bare `~` reifiers, `{| … |}`
                // blocks) — unique counter-based label. The leading '-'
                // keeps the minted namespace disjoint from every
                // user-written label: BLANK_NODE_LABEL must start with
                // PN_CHARS_U | [0-9], so `_:-b1` can never lex (`_:b1` +
                // an anonymous mint used to skolemize identically and
                // silently merge). '-' stays legal medially, so the full
                // skolemized `fdb-{txn}--b{N}` label still serializes and
                // re-imports as the same stored node.
                self.blank_counter += 1;
                let label = format!("-b{}", self.blank_counter);
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

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        if let Some(flake) = self.build_flake(subject, predicate, object, None) {
            self.flakes.push(flake);
        }
        Ok(())
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        if let Some(flake) = self.build_flake(subject, predicate, object, Some(index)) {
            self.flakes.push(flake);
        }
        Ok(())
    }

    fn supports_reified_triples(&self) -> bool {
        true
    }

    fn supports_triple_terms(&self) -> bool {
        true
    }

    fn term_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<TermId, SinkError> {
        let term = triple_term_value(
            self.resolve_sid(subject),
            self.resolve_sid(predicate),
            self.resolve_object(object),
        )?;
        Ok(self.add_term(ResolvedTerm::Literal {
            value: term,
            dtc: DatatypeConstraint::Explicit(fluree_db_core::triple_term_datatype_sid().clone()),
        }))
    }

    /// The reified triple's link; the parser has already emitted the base
    /// triple through `emit_triple`.
    fn emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> SinkResult {
        let Some(s) = self.resolve_sid(subject) else {
            return Ok(());
        };
        let Some(p) = self.resolve_sid(predicate) else {
            return Ok(());
        };
        let Some((o, dtc)) = self.resolve_object(object) else {
            return Ok(());
        };
        let Some(ann) = self.resolve_sid(reifier) else {
            return Ok(());
        };

        match crate::generate::flakes::reified_triple_link(None, s, p, o, &dtc, &ann, self.t) {
            Ok(link) => self.flakes.push(link),
            Err(e) => {
                tracing::error!("FlakeSink: invariant violation in reified triple, aborting — {e}");
                if self.invariant_error.is_none() {
                    self.invariant_error = Some(e);
                }
            }
        }
        Ok(())
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
    fn test_anonymous_mints_disjoint_from_user_label_namespace() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        // A user-written `_:b1` and the first anonymous mint used to
        // skolemize to the SAME Sid (`{txn}-b1`), silently merging user
        // data into system-minted `[]`/reifier nodes. TermId inequality
        // (above) never caught it — the collision was at the Sid level.
        let user = sink.term_blank(Some("b1"));
        let anon = sink.term_blank(None);
        let user_sid = sink.resolve_sid(user).expect("user blank is a Sid");
        let anon_sid = sink.resolve_sid(anon).expect("anon blank is a Sid");
        assert_ne!(
            user_sid, anon_sid,
            "anonymous mint must never share a Sid with a user `_:b1`"
        );
    }

    #[test]
    fn test_iri_object_as_ref() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/knows");
        let o = sink.term_iri("http://example.org/bob");
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_list_item(s, p, o0, 0).unwrap();
        sink.emit_list_item(s, p, o1, 1).unwrap();
        sink.emit_list_item(s, p, o2, 2).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
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
        sink.emit_triple(s, p, o).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
        assert_eq!(flakes.len(), 1);
        assert!(matches!(&flakes[0].o, FlakeValue::Long(42)));
        // dt must be xsd:long (declared), not xsd:integer (inferred)
        let expected_dt = ns.sid_for_iri(xsd::LONG);
        assert_eq!(flakes[0].dt, expected_dt);
    }

    #[test]
    fn user_authored_reifies_predicate_is_rejected() {
        // The reserved-predicate firewall: a Turtle statement that names an
        // `f:reifies*` predicate directly must fail the whole transaction,
        // exactly like the JSON-LD and SPARQL UPDATE surfaces. Only the
        // parser's reifier path (`emit_reified_triple`) may mint bundles.
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let claim = sink.term_iri("http://example.org/claim1");
        let p = sink.term_iri("https://ns.flur.ee/db#reifiesSubject");
        let alice = sink.term_iri("http://example.org/alice");
        sink.emit_triple(claim, p, alice).unwrap();

        let err = sink
            .into_flakes()
            .expect_err("a hand-written f:reifiesSubject triple must be rejected");
        assert!(
            matches!(&err, TransactError::UnsupportedFeature(m) if m.contains("reifiesSubject")),
            "expected the reserved-predicate error naming the predicate, got {err:?}"
        );
    }

    /// The base flake and the term its link names.
    fn base_and_term(flakes: &[Flake]) -> (&Flake, &fluree_db_core::TripleTermValue) {
        assert_eq!(flakes.len(), 2, "base + link: {flakes:?}");
        let (base, link) = (&flakes[0], &flakes[1]);
        assert!(fluree_db_core::is_rdf_reifies(&link.p));
        assert_eq!(link.s.name.as_ref(), "reifier");
        assert_eq!(link.dt, *fluree_db_core::triple_term_datatype_sid());
        assert!(link.op && link.t == base.t && link.g.is_none());
        match &link.o {
            FlakeValue::TripleTerm(term) => (base, term),
            other => panic!("link object is not a triple term: {other:?}"),
        }
    }

    #[test]
    fn test_reified_triple_emits_link() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/worksFor");
        let o = sink.term_iri("http://example.org/acme");
        let r = sink.term_iri("http://example.org/reifier");
        sink.emit_triple(s, p, o).unwrap();
        sink.emit_reified_triple(s, p, o, r).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
        let (base, term) = base_and_term(&flakes);
        assert_eq!(
            (&term.s, &term.p, &term.o, &term.dt, &term.lang),
            (&base.s, &base.p, &base.o, &base.dt, &None)
        );
    }

    #[test]
    fn test_reified_lang_literal_link_carries_lang() {
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/alice");
        let p = sink.term_iri("http://example.org/label");
        let o = sink.term_literal("chat", Datatype::rdf_lang_string(), Some("fr"));
        let r = sink.term_iri("http://example.org/reifier");
        sink.emit_triple(s, p, o).unwrap();
        sink.emit_reified_triple(s, p, o, r).unwrap();

        let flakes = sink.into_flakes().expect("no invariant violation");
        let (base, term) = base_and_term(&flakes);
        assert_eq!((&term.o, &term.dt), (&base.o, &base.dt));
        assert_eq!(term.lang.as_deref(), Some("fr"));
    }

    #[test]
    fn test_invalid_vector_lexical_aborts_finish() {
        // A Turtle literal `"not-a-vector"^^f:embeddingVector` parses through
        // convert_string_literal → falls back to FlakeValue::String. The late
        // guard in build_flake must capture the (String, embeddingVector)
        // mismatch and surface it from finish() rather than silently dropping
        // the bad triple — otherwise stage_turtle_insert would commit a
        // partial flake set with the bad data omitted.
        let (mut ns, t, txn_id) = make_sink();
        let mut sink = FlakeSink::new(&mut ns, t, txn_id);

        let s = sink.term_iri("http://example.org/doc1");
        let p = sink.term_iri("http://example.org/embedding");
        let o = sink.term_literal(
            "not-a-vector",
            Datatype::from_iri(fluree_vocab::fluree::EMBEDDING_VECTOR),
            None,
        );
        sink.emit_triple(s, p, o).unwrap();

        let err = sink
            .into_flakes()
            .expect_err("invariant violation must abort");
        let msg = err.to_string();
        assert!(
            msg.contains("embeddingVector"),
            "expected embeddingVector in error, got: {msg}"
        );
    }
}
