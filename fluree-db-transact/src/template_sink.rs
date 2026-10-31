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
use crate::generate::{infer_datatype, validate_value_dt_pair};
use crate::ir::{TemplateTerm, TemplateTripleTerm, TripleTemplate};
use crate::namespace::{NamespaceRegistry, NsAllocator};
use crate::parse::txn_meta::check_user_predicate;
use crate::value_convert::{convert_native_literal, convert_string_literal};
use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue};
use fluree_graph_ir::syntax::reserved_blank_label;
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkError, SinkResult, TermId};
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
    /// A triple term `<<( s p o )>>`: an object only.
    Triple { term: TemplateTerm, hash: u128 },
}

/// What a parsed RDF text document stages.
#[derive(Debug)]
pub struct RdfTextParts {
    /// One template per statement, graph set, plus the `rdf:reifies` link
    /// of each RDF 1.2 reified triple.
    pub templates: Vec<TripleTemplate>,
    /// Every named graph the document writes to (its labeled blocks).
    pub write_graphs: BTreeSet<String>,
    /// The `<#txn-meta>` block's entries.
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Order-insensitive hash of the document's data statements, when the
    /// sink was asked for it and the document has blank nodes for it to
    /// scope (see [`TemplateSink`]).
    pub content_id: Option<u128>,
}

/// A `GraphSink` that builds [`TripleTemplate`]s.
///
/// # Content identity
///
/// `content_id` is the wrapping sum of a 128-bit hash of every distinct
/// data statement — graph, subject, predicate, object and list position —
/// and of every distinct reifier attachment. A literal object is hashed as
/// it is stored: its value, datatype and language tag, the tag
/// lowercased ([`literal_hash`]), so `1` and `"1"^^xsd:integer` are one
/// object. An IRI is hashed in full, however it was written; a blank node
/// by its label. A statement written twice counts once, as a graph holds it
/// once. A sum is commutative, so the id does not depend on statement order.
/// Upsert derives its blank-node skolem scope from it, so the same document
/// upserted again addresses the same blank nodes. `<#txn-meta>` entries are
/// commit metadata and do not contribute.
///
/// The identity is computed only by a sink made with
/// [`Self::with_content_identity`], and only for a document with a blank
/// node, the one thing it scopes. A sink made with [`Self::new`] hashes
/// nothing, since no other lane uses it.
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
    /// The hash of each data statement and reifier attachment, for
    /// `content_id`; `None` when the identity is not asked for, and then no
    /// term or statement is hashed.
    statement_hashes: Option<Vec<u128>>,
    /// Whether the document holds a blank node: the identity scopes blank
    /// nodes, so a document with none has no identity to compute.
    blank_nodes: bool,
    /// Data statements accepted so far (not `<#txn-meta>` entries, not
    /// reifier links).
    statements: usize,
    /// Lists begun so far, and the one each subject and predicate is adding
    /// items to, for [`TripleTemplate::list_id`].
    lists: u32,
    open_lists: HashMap<(TermId, TermId), u32>,
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
            statement_hashes: None,
            blank_nodes: false,
            statements: 0,
            lists: 0,
            open_lists: HashMap::new(),
            error: None,
        }
    }

    /// A sink that also computes the document's content identity.
    pub fn with_content_identity(ns: &'a mut NamespaceRegistry) -> Self {
        let mut sink = Self::new(ns);
        sink.statement_hashes = Some(Vec::new());
        sink
    }

    /// Whether this sink computes the content identity.
    fn identity(&self) -> bool {
        self.statement_hashes.is_some()
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
    pub fn into_parts(mut self) -> Result<RdfTextParts, TransactError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        crate::parse::trig_meta::validate_limits(&self.txn_meta)?;
        // Each distinct statement once; nothing to scope without a blank node.
        let content_id = match self.statement_hashes.take() {
            Some(mut hashes) if self.blank_nodes => {
                hashes.sort_unstable();
                hashes.dedup();
                Some(hashes.iter().fold(0u128, |id, h| id.wrapping_add(*h)))
            }
            _ => None,
        };
        Ok(RdfTextParts {
            templates: self.templates,
            write_graphs: self.write_graphs,
            txn_meta: self.txn_meta,
            content_id,
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
            SinkTerm::Node { hash, .. }
            | SinkTerm::Literal { hash, .. }
            | SinkTerm::Triple { hash, .. } => *hash,
        }
    }

    /// The node at `id`, or `None` for a literal.
    fn node(&self, id: TermId) -> Option<TemplateTerm> {
        match self.term(id) {
            SinkTerm::Node { term, .. } => Some(term.clone()),
            SinkTerm::Literal { .. } | SinkTerm::Triple { .. } => None,
        }
    }

    /// An object: a node, or a literal value with its datatype constraint.
    fn object(&self, id: TermId) -> (TemplateTerm, Option<DatatypeConstraint>) {
        match self.term(id) {
            SinkTerm::Node { term, .. } | SinkTerm::Triple { term, .. } => (term.clone(), None),
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
        if let Some(hashes) = &mut self.statement_hashes {
            hashes.push(hash_parts(b"S", parts));
        }
    }

    /// Emit `template` in the current scope.
    fn emit(&mut self, template: TripleTemplate) {
        match &self.scope {
            Scope::Default => self.templates.push(template),
            Scope::Named(iri) => self.templates.push(template.in_graph(Arc::clone(iri))),
            Scope::TxnMeta => unreachable!("txn-meta statements are not templates"),
        }
    }

    fn push_statement(
        &mut self,
        s: TermId,
        p: TermId,
        o: TermId,
        list_index: Option<i32>,
        list_id: Option<u32>,
    ) {
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
        // `rdf:reifies` names a triple term, as in `FlakeSink`; the
        // reified-triple forms arrive via `emit_reified_triple`.
        if fluree_db_core::is_rdf_reifies(&predicate)
            && !matches!(object, TemplateTerm::TripleTerm(_))
        {
            if self.error.is_none() {
                self.error = Some(TransactError::UnsupportedFeature(
                    "'rdf:reifies' takes a triple term as its object; write the reified-triple \
                     form (`<< s p o >>` or `~ <reifier>`) rather than an ordinary object"
                        .to_string(),
                ));
            }
            return;
        }
        if let (TemplateTerm::Value(v), Some(dtc)) = (&object, &dtc) {
            if let Err(e) = validate_value_dt_pair(v, dtc.datatype()) {
                if self.error.is_none() {
                    self.error = Some(e);
                }
                return;
            }
        }
        if self.identity() {
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
        }

        let mut template = TripleTemplate::new(subject, TemplateTerm::Sid(predicate), object);
        if let Some(dtc) = dtc {
            template = template.with_dtc(dtc);
        }
        if let Some(i) = list_index {
            template = template.with_list_index(i);
        }
        template.list_id = list_id;
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
            SinkTerm::Literal { .. } | SinkTerm::Triple { .. } => {
                self.fail("a literal cannot be a subject");
                return;
            }
        }
        let predicate = match self.term(p) {
            SinkTerm::Node {
                term: TemplateTerm::Sid(sid),
                iri: Some(iri),
                ..
            } => match check_user_predicate(sid.namespace_code, &sid.name, &format!("<{iri}>")) {
                Ok(()) => sid.clone(),
                Err(e) => {
                    self.error.get_or_insert(e);
                    return;
                }
            },
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
            SinkTerm::Triple { .. } => {
                Err("txn-meta does not support triple-term values".to_string())
            }
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

fn is_commit_this_iri(iri: &str) -> bool {
    iri == fluree_vocab::fluree::COMMIT_THIS_HTTP || iri == fluree_vocab::fluree::COMMIT_THIS_SCHEME
}

/// A literal's part of a statement hash: the value it is stored as, its
/// datatype, and its language tag lowercased, as tags compare. So the
/// spellings of one stored literal — `1` and `"1"^^xsd:integer`, `true` and
/// `"true"^^xsd:boolean`, `1.0` and `1.00`, `@en-US` and `@en-us` — hash
/// alike. Text is hashed as is, a decimal by its normalized form, any other
/// value by `FlakeValue::canonical_hash`, whose bytes are fixed.
fn literal_hash(value: &FlakeValue, dt_iri: &str, language: Option<&str>) -> u128 {
    let lowered;
    let lang: &[u8] = match language {
        None => b"",
        Some(tag) if tag.bytes().any(|b| b.is_ascii_uppercase()) => {
            lowered = tag.to_ascii_lowercase();
            lowered.as_bytes()
        }
        Some(tag) => tag.as_bytes(),
    };
    let dt = dt_iri.as_bytes();
    match value {
        FlakeValue::String(text) | FlakeValue::Json(text) => {
            hash_parts(b"L", &[b"s", text.as_bytes(), dt, lang])
        }
        FlakeValue::Decimal(d) => decimal_hash(d, dt, lang),
        other => hash_parts(
            b"L",
            &[b"h", &other.canonical_hash().to_le_bytes(), dt, lang],
        ),
    }
}

/// [`literal_hash`] of a decimal, by value: its mantissa with no trailing
/// zero, and its scale, so `1.0` and `1.00` are one decimal. A mantissa that
/// fits an `i64`, the common case, is hashed as its eight little-endian
/// bytes with no allocation; a larger one is normalized and hashed as its
/// signed bytes, which are never eight long.
fn decimal_hash(d: &bigdecimal::BigDecimal, dt: &[u8], lang: &[u8]) -> u128 {
    use bigdecimal::ToPrimitive;
    let hash = |mantissa: &[u8], scale: i64| {
        hash_parts(b"L", &[b"d", mantissa, &scale.to_le_bytes(), dt, lang])
    };
    let (int_val, mut scale) = d.as_bigint_and_scale();
    if let Some(mut m) = int_val.to_i64() {
        if m == 0 {
            return hash(&0i64.to_le_bytes(), 0);
        }
        while m % 10 == 0 {
            m /= 10;
            scale -= 1;
        }
        return hash(&m.to_le_bytes(), scale);
    }
    let normal = d.normalized();
    let (int_val, scale) = normal.as_bigint_and_scale();
    match int_val.to_i64() {
        Some(m) => hash(&m.to_le_bytes(), scale),
        None => hash(&int_val.to_signed_bytes_le(), scale),
    }
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
        let hash = if self.identity() {
            hash_parts(b"I", &[iri.as_bytes()])
        } else {
            0
        };
        self.add(SinkTerm::Node {
            term: TemplateTerm::Sid(sid),
            iri: Some(Arc::from(iri)),
            hash,
        })
    }

    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.blank_nodes = true;
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
                let hash = if self.identity() {
                    hash_parts(b"B", &[l.as_bytes()])
                } else {
                    0
                };
                let id = self.add(SinkTerm::Node {
                    term,
                    iri: None,
                    hash,
                });
                self.blank_labels.insert(l.to_string(), id);
                id
            }
            None => {
                self.anon += 1;
                let label = reserved_blank_label("b", self.anon);
                let hash = if self.identity() {
                    hash_parts(b"B", &[label.as_bytes()])
                } else {
                    0
                };
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
        let hash = if self.identity() {
            literal_hash(&flake_value, dt_iri, language)
        } else {
            0
        };
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
        let hash = if self.identity() {
            literal_hash(&flake_value, datatype.as_iri(), None)
        } else {
            0
        };
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
        self.push_statement(subject, predicate, object, None, None);
        Ok(())
    }

    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        let list = match self.open_lists.get(&(subject, predicate)) {
            Some(&list) if index != 0 => list,
            _ => {
                let list = self.lists;
                self.lists += 1;
                self.open_lists.insert((subject, predicate), list);
                list
            }
        };
        self.push_statement(subject, predicate, object, Some(index), Some(list));
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
        self.push_statement(subject, predicate, object, None, None);
        self.scope = outer;
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
        let (Some(s), Some(p @ TemplateTerm::Sid(_))) = (self.node(subject), self.node(predicate))
        else {
            return Err(SinkError::rejected(
                "a triple term's subject must be an IRI or blank node, and its predicate an IRI",
            ));
        };
        let (o, dtc) = self.object(object);
        let hash = hash_parts(
            b"T",
            &[
                &self.hash(subject).to_le_bytes(),
                &self.hash(predicate).to_le_bytes(),
                &self.hash(object).to_le_bytes(),
            ],
        );
        let term = TemplateTerm::TripleTerm(Box::new(TemplateTripleTerm { s, p, o, dtc }));
        Ok(self.add(SinkTerm::Triple { term, hash }))
    }

    /// An RDF 1.2 reified triple: its `rdf:reifies` link, in the current
    /// graph. Only the annotation syntax asserts the base triple, which the
    /// parser has already emitted.
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
        if self.identity() {
            let (gh, sh, ph, oh, rh) = (
                self.graph_hash(),
                self.hash(subject),
                self.hash(predicate),
                self.hash(object),
                self.hash(reifier),
            );
            let hash = hash_parts(
                b"R",
                &[
                    &gh.to_le_bytes(),
                    &sh.to_le_bytes(),
                    &ph.to_le_bytes(),
                    &oh.to_le_bytes(),
                    &rh.to_le_bytes(),
                ],
            );
            if let Some(hashes) = &mut self.statement_hashes {
                hashes.push(hash);
            }
        }
        if let (TemplateTerm::Value(v), Some(dtc)) = (&o, &dtc) {
            if let Err(e) = validate_value_dt_pair(v, dtc.datatype()) {
                if self.error.is_none() {
                    self.error = Some(e);
                }
                return Ok(());
            }
        }
        let term = TemplateTripleTerm { s, p, o, dtc };
        self.emit(TripleTemplate::new(
            ann,
            TemplateTerm::Sid(fluree_db_core::rdf_reifies_sid().clone()),
            TemplateTerm::TripleTerm(Box::new(term)),
        ));
        Ok(())
    }
}
