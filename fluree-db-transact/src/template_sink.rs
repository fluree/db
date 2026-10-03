//! TemplateSink — a `GraphSink` that turns parsed RDF text into transaction
//! templates.
//!
//! RDF text (Turtle, TriG, N-Triples) is interpreted once, by
//! `fluree-graph-turtle`'s parser, and every verb that writes it — upsert,
//! the TriG insert fallback, graph sync, graph insert — stages from this
//! sink's templates. It replaces a round trip through JSON-LD that lost
//! list order, blank-node `rdf:type` objects and literal `rdf:type` values,
//! rejected IRIs the JSON-LD compact-IRI guard mistook for prefixed names,
//! and parsed literals more strictly than insert did.
//!
//! Literals convert exactly as [`FlakeSink`](crate::flake_sink::FlakeSink)
//! converts them (`value_convert`, lenient: an ill-typed lexical form is
//! stored as a string with its declared datatype), so a document means the
//! same thing on every RDF-text lane.
//!
//! The driver (`parse::rdf_text`) feeds one sink across a TriG document's
//! segments, setting the graph scope per segment, so labeled blank nodes are
//! document-scoped and anonymous ones are numbered across the whole
//! document.

use crate::error::TransactError;
use crate::generate::{bundle_templates, infer_datatype, validate_value_dt_pair};
use crate::ir::{TemplateTerm, TripleTemplate};
use crate::namespace::{NamespaceRegistry, NsAllocator};
use crate::value_convert::{convert_native_literal, convert_string_literal};
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3;

/// The graph the statements being parsed belong to.
#[derive(Clone, Debug)]
pub(crate) enum Scope {
    /// The default graph.
    Default,
    /// A named graph, by IRI.
    Named(Arc<str>),
    /// The `<#txn-meta>` block: commit metadata, not graph data.
    TxnMeta,
}

/// A parsed term.
enum SinkTerm {
    /// An IRI or blank node.
    Node {
        term: TemplateTerm,
        /// The IRI, for IRIs (a quad's graph, a txn-meta subject check).
        iri: Option<Arc<str>>,
        hash: u128,
    },
    /// A literal, converted for storage.
    Literal {
        value: FlakeValue,
        dtc: DatatypeConstraint,
        hash: u128,
        /// The txn-meta form of the literal, when parsed in the txn-meta
        /// scope.
        meta: Option<Result<TxnMetaValue, String>>,
    },
}

/// What a parsed RDF text document stages.
#[derive(Debug)]
pub struct RdfTextParts {
    /// One template per statement, graph set, plus the `f:reifies*`
    /// bundle templates of any RDF 1.2 annotations.
    pub templates: Vec<TripleTemplate>,
    /// Every named graph the document writes to (its labeled blocks).
    pub write_graphs: BTreeSet<String>,
    /// The `<#txn-meta>` block's entries.
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Order-insensitive hash of the document's data statements (see
    /// [`TemplateSink`]).
    pub content_id: u128,
}

/// A `GraphSink` that builds [`TripleTemplate`]s.
///
/// # Content identity
///
/// `content_id` is the wrapping sum of a 128-bit hash of every data
/// statement — graph, subject, predicate, object (lexical form, datatype
/// and language for literals; IRI or label for nodes) and list position —
/// and of every reifier attachment. A sum is commutative, so the id does
/// not depend on statement order, and it is computed in one pass with O(1)
/// memory. Upsert derives its blank-node skolem scope from it, so the same
/// document upserted again addresses the same blank nodes. `<#txn-meta>`
/// entries are commit metadata and do not contribute.
pub struct TemplateSink<'a> {
    ns: &'a mut NamespaceRegistry,
    terms: Vec<SinkTerm>,
    /// Labeled blank nodes, document-scoped.
    blank_labels: HashMap<String, TermId>,
    /// Anonymous blank nodes minted so far (`-b{N}`, as `FlakeSink`).
    anon: u32,
    scope: Scope,
    templates: Vec<TripleTemplate>,
    write_graphs: BTreeSet<String>,
    txn_meta: Vec<TxnMetaEntry>,
    content_id: u128,
    /// Data statements accepted so far (not `<#txn-meta>` entries, not
    /// annotation bundles).
    statements: usize,
    /// First error; surfaced by [`Self::into_parts`] so a bad document fails
    /// the whole transaction rather than committing part of it.
    error: Option<TransactError>,
}

impl<'a> TemplateSink<'a> {
    pub fn new(ns: &'a mut NamespaceRegistry) -> Self {
        Self {
            ns,
            terms: Vec::new(),
            blank_labels: HashMap::new(),
            anon: 0,
            scope: Scope::Default,
            templates: Vec::new(),
            write_graphs: BTreeSet::new(),
            txn_meta: Vec::new(),
            content_id: 0,
            statements: 0,
            error: None,
        }
    }

    /// Set the graph the next statements belong to. A named graph is
    /// written even when its block holds no statements.
    pub(crate) fn set_scope(&mut self, scope: Scope) {
        if let Scope::Named(iri) = &scope {
            self.write_graphs.insert(iri.to_string());
        }
        self.scope = scope;
    }

    /// Data statements accepted so far.
    pub(crate) fn statements(&self) -> usize {
        self.statements
    }

    /// The templates, graphs, txn-meta and content id, or the first error
    /// the document raised.
    pub fn into_parts(self) -> Result<RdfTextParts, TransactError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        crate::parse::trig_meta::validate_limits(&self.txn_meta)?;
        Ok(RdfTextParts {
            templates: self.templates,
            write_graphs: self.write_graphs,
            txn_meta: self.txn_meta,
            content_id: self.content_id,
        })
    }

    fn fail(&mut self, message: impl Into<String>) {
        if self.error.is_none() {
            self.error = Some(TransactError::Parse(message.into()));
        }
    }

    fn add(&mut self, term: SinkTerm) -> TermId {
        let id = TermId::new(self.terms.len() as u32);
        self.terms.push(term);
        id
    }

    fn term(&self, id: TermId) -> &SinkTerm {
        &self.terms[id.index() as usize]
    }

    fn hash(&self, id: TermId) -> u128 {
        match self.term(id) {
            SinkTerm::Node { hash, .. } | SinkTerm::Literal { hash, .. } => *hash,
        }
    }

    /// The node at `id`, or `None` for a literal.
    fn node(&self, id: TermId) -> Option<TemplateTerm> {
        match self.term(id) {
            SinkTerm::Node { term, .. } => Some(term.clone()),
            SinkTerm::Literal { .. } => None,
        }
    }

    /// An object: a node, or a literal value with its datatype constraint.
    fn object(&self, id: TermId) -> (TemplateTerm, Option<DatatypeConstraint>) {
        match self.term(id) {
            SinkTerm::Node { term, .. } => (term.clone(), None),
            SinkTerm::Literal { value, dtc, .. } => {
                (TemplateTerm::Value(value.clone()), Some(dtc.clone()))
            }
        }
    }

    /// The graph's contribution to a statement hash.
    fn graph_hash(&self) -> u128 {
        match &self.scope {
            Scope::Named(iri) => hash_parts(b"G", &[iri.as_bytes()]),
            Scope::Default | Scope::TxnMeta => 0,
        }
    }

    fn record(&mut self, parts: &[&[u8]]) {
        self.content_id = self.content_id.wrapping_add(hash_parts(b"S", parts));
    }

    /// Emit `template` in the current scope: the stand-in for `GraphScope::emit`.
    fn emit(&mut self, template: TripleTemplate) {
        match &self.scope {
            Scope::Default => self.templates.push(template),
            Scope::Named(iri) => {
                let iri = Arc::clone(iri);
                graph_scope_emit(&iri, template, self.ns, &mut self.templates);
            }
            Scope::TxnMeta => unreachable!("txn-meta statements are not templates"),
        }
    }

    fn push_statement(&mut self, s: TermId, p: TermId, o: TermId, list_index: Option<i32>) {
        if matches!(self.scope, Scope::TxnMeta) {
            if list_index.is_some() {
                self.fail("lists are not allowed in the txn-meta graph");
                return;
            }
            self.push_txn_meta(s, p, o);
            return;
        }
        let Some(subject) = self.node(s) else {
            self.fail("a literal cannot be a subject");
            return;
        };
        let predicate = match self.node(p) {
            Some(TemplateTerm::Sid(p)) => p,
            _ => {
                self.fail("a predicate must be an IRI");
                return;
            }
        };
        // Reserved-predicate firewall, as `FlakeSink` and the JSON-LD and
        // SPARQL UPDATE surfaces: a hand-written `f:reifies*` statement is
        // refused; annotations are minted only through the RDF 1.2
        // annotation syntax, which arrives via `emit_reified_triple`.
        if fluree_db_core::is_reserved_reifies_predicate(&predicate) {
            let iri = format!(
                "{}{}",
                self.ns.get_prefix(predicate.namespace_code).unwrap_or(""),
                predicate.name
            );
            let e = TransactError::UnsupportedFeature(format!(
                "'{iri}' is a system-controlled predicate; use the RDF 1.2 annotation \
                 syntax (`~ <reifier> {{| ... |}}` or `<< s p o >>`) instead of \
                 writing f:reifies* triples by hand"
            ));
            if self.error.is_none() {
                self.error = Some(e);
            }
            return;
        }
        let (object, dtc) = self.object(o);
        if let (TemplateTerm::Value(v), Some(dtc)) = (&object, &dtc) {
            if let Err(e) = validate_value_dt_pair(v, dtc.datatype()) {
                if self.error.is_none() {
                    self.error = Some(e);
                }
                return;
            }
        }
        let (gh, sh, ph, oh) = (self.graph_hash(), self.hash(s), self.hash(p), self.hash(o));
        let index = list_index.map_or([0u8; 5], |i| {
            let b = i.to_le_bytes();
            [1, b[0], b[1], b[2], b[3]]
        });
        self.record(&[
            &gh.to_le_bytes(),
            &sh.to_le_bytes(),
            &ph.to_le_bytes(),
            &oh.to_le_bytes(),
            &index,
        ]);

        let mut template = TripleTemplate::new(subject, TemplateTerm::Sid(predicate), object);
        if let Some(dtc) = dtc {
            template = template.with_dtc(dtc);
        }
        if let Some(i) = list_index {
            template = template.with_list_index(i);
        }
        self.statements += 1;
        self.emit(template);
    }

    fn push_txn_meta(&mut self, s: TermId, p: TermId, o: TermId) {
        match self.term(s) {
            SinkTerm::Node { iri: Some(iri), .. } if is_commit_this_iri(iri) => {}
            SinkTerm::Node { iri: Some(iri), .. } => {
                let iri = iri.clone();
                self.fail(format!(
                    "txn-meta subject must be fluree:commit:this, found: {iri}"
                ));
                return;
            }
            SinkTerm::Node { iri: None, .. } => {
                self.fail("blank nodes not allowed as txn-meta subject");
                return;
            }
            SinkTerm::Literal { .. } => {
                self.fail("a literal cannot be a subject");
                return;
            }
        }
        let predicate = match self.node(p) {
            Some(TemplateTerm::Sid(p)) => p,
            _ => {
                self.fail("a txn-meta predicate must be an IRI");
                return;
            }
        };
        let value = match self.term(o) {
            SinkTerm::Node {
                term: TemplateTerm::Sid(sid),
                iri: Some(_),
                ..
            } => Ok(TxnMetaValue::Ref {
                ns: sid.namespace_code,
                name: sid.name.to_string(),
            }),
            SinkTerm::Node { .. } => Err("blank nodes not allowed in txn-meta objects".to_string()),
            SinkTerm::Literal { meta, .. } => meta
                .clone()
                .unwrap_or_else(|| Err("unsupported txn-meta literal".to_string())),
        };
        match value {
            Ok(value) => self.txn_meta.push(TxnMetaEntry::new(
                predicate.namespace_code,
                predicate.name.to_string(),
                value,
            )),
            Err(message) => self.fail(message),
        }
    }

    /// The txn-meta form of a literal parsed in the txn-meta scope.
    fn meta_literal(&mut self, lexical: &str, dt_iri: &str, lang: Option<&str>) -> TxnMetaValue {
        if let Some(lang) = lang {
            return TxnMetaValue::LangString {
                value: lexical.to_string(),
                lang: lang.to_string(),
            };
        }
        if dt_iri == fluree_vocab::xsd::STRING {
            return TxnMetaValue::String(lexical.to_string());
        }
        let dt = self.ns.sid_for_iri(dt_iri);
        TxnMetaValue::TypedLiteral {
            value: lexical.to_string(),
            dt_ns: dt.namespace_code,
            dt_name: dt.name.to_string(),
        }
    }
}

/// The one rule that puts a template in its graph and, when it writes
/// `f:reifiesSubject` in a named graph, adds the bundle's `f:reifiesGraph`
/// anchor, so a template bundle never states its own anchor. It stands in
/// for `GraphScope::emit`, which is to own this rule for every template
/// source; when that lands, the sink routes through it and this goes.
fn graph_scope_emit(
    iri: &Arc<str>,
    template: TripleTemplate,
    ns: &mut NamespaceRegistry,
    out: &mut Vec<TripleTemplate>,
) {
    let anchors = matches!(
        &template.predicate,
        TemplateTerm::Sid(p) if fluree_db_core::is_reifies_subject(p)
    );
    let template = template.in_graph(Arc::clone(iri));
    if anchors {
        let anchor = TripleTemplate::new(
            template.subject.clone(),
            TemplateTerm::Sid(fluree_db_core::namespaces::reifies_graph_sid().clone()),
            TemplateTerm::Sid(ns.sid_for_iri(iri)),
        )
        .in_graph(Arc::clone(iri));
        out.push(anchor);
    }
    out.push(template);
}

fn is_commit_this_iri(iri: &str) -> bool {
    iri == fluree_vocab::fluree::COMMIT_THIS_HTTP || iri == fluree_vocab::fluree::COMMIT_THIS_SCHEME
}

/// A 128-bit hash of a tagged sequence of parts. Stable across builds and
/// platforms: it hashes bytes, never a `Debug` rendering or a `Hash` impl.
fn hash_parts(tag: &[u8], parts: &[&[u8]]) -> u128 {
    let mut h = Xxh3::new();
    h.update(tag);
    for part in parts {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    h.digest128()
}

impl GraphSink for TemplateSink<'_> {
    fn on_base(&mut self, _base_iri: &str) {
        // The parser resolves relative IRIs before calling term_iri.
    }

    fn on_prefix(&mut self, _prefix: &str, namespace_iri: &str) {
        // Pre-register the namespace, as FlakeSink does, so codes are
        // allocated in declaration order.
        self.ns.get_or_allocate(namespace_iri);
    }

    fn term_iri(&mut self, iri: &str) -> TermId {
        let sid = self.ns.sid_for_iri(iri);
        let hash = hash_parts(b"I", &[iri.as_bytes()]);
        self.add(SinkTerm::Node {
            term: TemplateTerm::Sid(sid),
            iri: Some(Arc::from(iri)),
            hash,
        })
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        // A blank node in the txn-meta block fails where it is used, with a
        // message naming the position (subject or object).
        match label {
            Some(l) => {
                if let Some(&id) = self.blank_labels.get(l) {
                    return id;
                }
                // Stable Fluree blank-node ids (`fdb-…`) address the stored
                // node; other labels skolemize at staging.
                let term = crate::namespace::stable_blank_node_sid_from_label(l)
                    .map_or_else(|| TemplateTerm::BlankNode(l.to_string()), TemplateTerm::Sid);
                let hash = hash_parts(b"B", &[l.as_bytes()]);
                let id = self.add(SinkTerm::Node {
                    term,
                    iri: None,
                    hash,
                });
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                // The leading '-' keeps minted labels out of the
                // user-writable BLANK_NODE_LABEL space (see FlakeSink).
                self.anon += 1;
                let label = format!("-b{}", self.anon);
                let hash = hash_parts(b"B", &[label.as_bytes()]);
                self.add(SinkTerm::Node {
                    term: TemplateTerm::BlankNode(label),
                    iri: None,
                    hash,
                })
            }
        }
    }

    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        let dt_iri = datatype.as_iri();
        let (flake_value, dt_sid) =
            convert_string_literal(value, dt_iri, &mut NsAllocator::Exclusive(self.ns));
        let dtc = match language {
            Some(lang) => DatatypeConstraint::LangTag(Arc::from(lang)),
            None => DatatypeConstraint::Explicit(dt_sid),
        };
        let hash = hash_parts(
            b"L",
            &[
                value.as_bytes(),
                dt_iri.as_bytes(),
                language.unwrap_or("").as_bytes(),
            ],
        );
        let meta = matches!(self.scope, Scope::TxnMeta)
            .then(|| Ok(self.meta_literal(value, dt_iri, language)));
        self.add(SinkTerm::Literal {
            value: flake_value,
            dtc,
            hash,
            meta,
        })
    }

    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        let flake_value = convert_native_literal(&value);
        let dt_sid = if datatype.is_json() {
            Sid::new(fluree_vocab::namespaces::RDF, "JSON")
        } else {
            infer_datatype(&flake_value)
        };
        let (kind, text): (&[u8], String) = match &value {
            LiteralValue::Integer(i) => (b"i", i.to_string()),
            LiteralValue::Double(d) => (b"d", format!("{:016x}", d.to_bits())),
            LiteralValue::Boolean(b) => (b"b", b.to_string()),
            LiteralValue::String(s) => (b"s", s.to_string()),
            LiteralValue::Json(s) => (b"j", s.to_string()),
        };
        let hash = hash_parts(b"N", &[kind, text.as_bytes(), datatype.as_iri().as_bytes()]);
        let meta = matches!(self.scope, Scope::TxnMeta).then(|| match &value {
            LiteralValue::Integer(i) => Ok(TxnMetaValue::Long(*i)),
            LiteralValue::Double(d) if d.is_finite() => Ok(TxnMetaValue::Double(*d)),
            LiteralValue::Double(_) => {
                Err("txn-meta does not support non-finite double values".to_string())
            }
            LiteralValue::Boolean(b) => Ok(TxnMetaValue::Boolean(*b)),
            LiteralValue::String(s) => Ok(TxnMetaValue::String(s.to_string())),
            LiteralValue::Json(s) => Ok(TxnMetaValue::TypedLiteral {
                value: s.to_string(),
                dt_ns: dt_sid.namespace_code,
                dt_name: dt_sid.name.to_string(),
            }),
        });
        self.add(SinkTerm::Literal {
            value: flake_value,
            dtc: DatatypeConstraint::Explicit(dt_sid),
            hash,
            meta,
        })
    }

    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.push_statement(subject, predicate, object, None);
        Ok(())
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        self.push_statement(subject, predicate, object, Some(index));
        Ok(())
    }

    fn supports_quads(&self) -> bool {
        true
    }

    /// A quad: the triple in the named graph `graph`, for a parser that reads
    /// TriG itself.
    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        let iri = match self.term(graph) {
            SinkTerm::Node { iri: Some(iri), .. } => Arc::clone(iri),
            _ => {
                self.fail("a graph name must be an IRI");
                return Ok(());
            }
        };
        let outer = std::mem::replace(&mut self.scope, Scope::Default);
        self.set_scope(Scope::Named(iri));
        self.push_statement(subject, predicate, object, None);
        self.scope = outer;
        Ok(())
    }

    fn supports_reified_triples(&self) -> bool {
        true
    }

    /// An RDF 1.2 reifier attachment: the `f:reifies*` bundle templates, in
    /// the current graph. The parser has already emitted the base triple.
    fn emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> SinkResult {
        if matches!(self.scope, Scope::TxnMeta) {
            self.fail(
                "RDF 1.2 reifiers and annotations are not allowed in the txn-meta graph; \
                 its triples become commit metadata, not graph edges",
            );
            return Ok(());
        }
        let (Some(s), Some(p), Some(ann)) =
            (self.node(subject), self.node(predicate), self.node(reifier))
        else {
            self.fail("a reified triple needs node subject, predicate and reifier");
            return Ok(());
        };
        let (o, dtc) = self.object(object);
        let (gh, sh, ph, oh, rh) = (
            self.graph_hash(),
            self.hash(subject),
            self.hash(predicate),
            self.hash(object),
            self.hash(reifier),
        );
        self.content_id = self.content_id.wrapping_add(hash_parts(
            b"R",
            &[
                &gh.to_le_bytes(),
                &sh.to_le_bytes(),
                &ph.to_le_bytes(),
                &oh.to_le_bytes(),
                &rh.to_le_bytes(),
            ],
        ));
        for template in bundle_templates(s, p, o, dtc, ann) {
            self.emit(template);
        }
        Ok(())
    }
}
