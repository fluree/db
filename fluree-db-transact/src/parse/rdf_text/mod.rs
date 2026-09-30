//! RDF text, located once and parsed once.
//!
//! Every write verb that takes RDF text — upsert, the TriG insert fallback,
//! graph sync and graph insert — reads it here, with the conformant Turtle
//! parser, into one [`TemplateSink`]. Nothing re-encodes the text as
//! JSON-LD, reorders it, or reads block contents with a second parser.
//!
//! A TriG document is Turtle statements interleaved with graph blocks. The
//! locator (the `locate` module) reads tokens only: it finds each block's
//! label and extent and never interprets a term, so it cannot disagree with
//! the parser about what the text means. The driver then parses each segment
//! in document order, carrying the prefix and base declarations forward from
//! where they appear: a redefinition applies to what follows it and to
//! nothing before it. Block labels are resolved by the parser too, under the
//! declarations in force where the block appears. One sink serves the whole
//! document, so a blank node label means one node everywhere in it.
//!
//! Plain Turtle — no `{` and no `GRAPH` anywhere — takes a single parse.

use crate::error::{Result, TransactError};
use crate::ir::{GraphSel, Txn, TxnOpts, TxnType};
use crate::namespace::NamespaceRegistry;
use crate::parse::trig_meta::{might_contain_graph_block, TXN_META_GRAPH_IRI};
use crate::template_sink::{RdfTextParts, Scope, TemplateSink};
use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, SinkResult, TermId};
use fluree_graph_turtle::{ParserOptions, TurtleError};
use std::sync::Arc;

mod locate;

pub use locate::has_graph_blocks;
use locate::{locate, SegmentKind};

/// Where a document's statements land.
#[derive(Clone, Copy, Debug)]
pub enum Placement<'a> {
    /// As written: default-graph statements in the default graph, each
    /// block's statements in the block's graph.
    AsWritten,
    /// Every statement in one graph (`None`: the default graph), for a write
    /// scoped to a graph the request names. A block must name that graph,
    /// and a document holds the graph's statements either in blocks or as
    /// default-graph statements, not both.
    Into(Option<&'a str>),
}

/// A document parsed for one transaction: the templates and graphs it
/// writes, its `<#txn-meta>` entries, and what it held.
#[derive(Debug)]
pub struct RdfText {
    pub parts: RdfTextParts,
    /// Data statements in the document (not counting `<#txn-meta>`).
    pub statements: usize,
}

/// What [`parse_rdf_text_txn`] read, beside the transaction it built.
#[derive(Clone, Copy, Debug)]
pub struct RdfTextSummary {
    /// See [`RdfTextParts::content_id`].
    pub content_id: u128,
    /// Data statements in the document (not counting `<#txn-meta>`).
    pub statements: usize,
}

/// Parse `text` into a [`Txn`] of `txn_type`.
///
/// Upsert gets a blank-node skolem scope derived from the document's
/// content (see [`TemplateSink`]'s content identity) unless `opts` already
/// names one, so the same document upserted again addresses the same blank
/// nodes. A graph-scoped [`Placement::Into`] write registers its target
/// graph; a sync additionally gets `sync_graph`.
pub fn parse_rdf_text_txn(
    text: &str,
    txn_type: TxnType,
    placement: Placement<'_>,
    sync: bool,
    mut opts: TxnOpts,
    ns: &mut NamespaceRegistry,
) -> Result<(Txn, RdfTextSummary)> {
    let RdfText { parts, statements } = parse_rdf_text(text, placement, ns)?;
    let summary = RdfTextSummary {
        content_id: parts.content_id,
        statements,
    };
    if txn_type == TxnType::Upsert && opts.skolem_txn_id.is_none() {
        let id = parts.content_id;
        let folded = (id as u64) ^ ((id >> 64) as u64);
        opts.skolem_txn_id = Some(format!(
            "upsert{}",
            fluree_db_core::skolem::doc_scope(folded)
        ));
    }
    let mut txn = match txn_type {
        TxnType::Upsert => Txn::upsert(),
        TxnType::Insert => Txn::insert(),
        TxnType::Update => {
            return Err(TransactError::Parse(
                "RDF text cannot express an update; use SPARQL UPDATE or JSON-LD".to_string(),
            ))
        }
    }
    .with_opts(opts);
    txn.insert_templates = parts.templates;
    txn.write_graphs = parts.write_graphs;
    txn.txn_meta = parts.txn_meta;
    if sync {
        txn.sync_graph = Some(match placement {
            Placement::Into(Some(iri)) => GraphSel::Graph(iri.to_string()),
            Placement::Into(None) | Placement::AsWritten => GraphSel::Default,
        });
    }
    Ok((txn, summary))
}

/// Parse `text` into templates under `placement`.
pub fn parse_rdf_text(
    text: &str,
    placement: Placement<'_>,
    ns: &mut NamespaceRegistry,
) -> Result<RdfText> {
    let mut sink = TemplateSink::new(ns);
    let target_scope = |sink_target: Option<&str>| match sink_target {
        Some(iri) => Scope::Named(Arc::from(iri)),
        None => Scope::Default,
    };
    let default_scope = match placement {
        Placement::AsWritten => Scope::Default,
        Placement::Into(target) => target_scope(target),
    };

    if !might_contain_graph_block(text) {
        sink.set_scope(default_scope);
        fluree_graph_turtle::parse(text, &mut sink)?;
        let statements = sink.statements();
        return Ok(RdfText {
            parts: sink.into_parts()?,
            statements,
        });
    }

    let located = locate(text)?;
    let mut prefixes: Vec<(String, String)> = Vec::new();
    let mut base: Option<String> = None;
    let mut default_statements = 0usize;
    let mut named_blocks = 0usize;
    for segment in &located.segments {
        let before = sink.statements();
        match &segment.kind {
            SegmentKind::Default | SegmentKind::DefaultBlock => {
                sink.set_scope(default_scope.clone());
            }
            SegmentKind::Named { label, at } => {
                let iri = resolve_label(label, *at, &prefixes, base.as_deref())?;
                if iri == TXN_META_GRAPH_IRI {
                    sink.set_scope(Scope::TxnMeta);
                } else {
                    named_blocks += 1;
                    match placement {
                        Placement::AsWritten => sink.set_scope(Scope::Named(Arc::from(iri))),
                        Placement::Into(target) if target == Some(iri.as_str()) => {
                            sink.set_scope(default_scope.clone());
                        }
                        Placement::Into(target) => {
                            return Err(TransactError::PayloadGraphMismatch(format!(
                                "the request targets one graph, {}; the body also has a \
                                 GRAPH block for <{iri}>",
                                describe_target(target)
                            )));
                        }
                    }
                }
            }
        }
        let slice = &located.text[segment.range.clone()];
        let seeded_prefixes = prefixes.clone();
        let seeded_base = base.clone();
        let mut capture = Capture {
            sink: &mut sink,
            prefixes: &mut prefixes,
            base: &mut base,
        };
        fluree_graph_turtle::parse_with_prefixes_base_options(
            slice,
            &mut capture,
            &seeded_prefixes,
            seeded_base.as_deref(),
            ParserOptions::default(),
        )
        .map_err(|e| shift(e, segment.range.start))?;
        if matches!(
            segment.kind,
            SegmentKind::Default | SegmentKind::DefaultBlock
        ) {
            default_statements += sink.statements() - before;
        }
    }
    if let Placement::Into(target) = placement {
        if default_statements > 0 && named_blocks > 0 {
            let target = describe_target(target);
            return Err(TransactError::PayloadGraphMismatch(format!(
                "a TriG body holds {target}'s triples either in GRAPH {target} blocks or as \
                 default-graph triples, not both"
            )));
        }
    }
    let statements = sink.statements();
    Ok(RdfText {
        parts: sink.into_parts()?,
        statements,
    })
}

fn describe_target(target: Option<&str>) -> String {
    match target {
        Some(iri) => format!("<{iri}>"),
        None => "the default graph".to_string(),
    }
}

/// Move a parser error's byte position from a segment into the document.
fn shift(e: TurtleError, offset: usize) -> TransactError {
    TransactError::Turtle(match e {
        TurtleError::Parse { position, message } => TurtleError::Parse {
            position: position + offset,
            message,
        },
        TurtleError::Lexer { position, message } => TurtleError::Lexer {
            position: position + offset,
            message,
        },
        other => other,
    })
}

/// Resolve a block label with the parser, under the declarations in force
/// where the block appears: exactly the expansion a subject IRI gets there
/// (prefixed-name unescaping, relative references against the base).
///
/// `<#txn-meta>` with no base in force is the txn-meta block, as it always
/// was; under a base it resolves like any relative reference.
fn resolve_label(
    label: &str,
    at: usize,
    prefixes: &[(String, String)],
    base: Option<&str>,
) -> Result<String> {
    if base.is_none() && label == "<#txn-meta>" {
        return Ok(TXN_META_GRAPH_IRI.to_string());
    }
    let probe = format!("{label} <urn:fluree:probe> <urn:fluree:probe> .");
    let mut sink = LabelProbe(None);
    fluree_graph_turtle::parse_with_prefixes_base_options(
        &probe,
        &mut sink,
        prefixes,
        base,
        ParserOptions::default(),
    )
    .map_err(|e| {
        let message = match e {
            TurtleError::Parse { message, .. } | TurtleError::Lexer { message, .. } => message,
            other => other.to_string(),
        };
        TransactError::Turtle(TurtleError::Parse {
            position: at,
            message: format!("graph label {label}: {message}"),
        })
    })?;
    sink.0.ok_or_else(|| {
        TransactError::Turtle(TurtleError::parse(
            at,
            format!("graph label {label} is not an IRI"),
        ))
    })
}

/// Records the first IRI a probe statement names: the label.
struct LabelProbe(Option<String>);

impl GraphSink for LabelProbe {
    fn on_base(&mut self, _: &str) {}
    fn on_prefix(&mut self, _: &str, _: &str) {}
    fn term_iri(&mut self, iri: &str) -> TermId {
        if self.0.is_none() {
            self.0 = Some(iri.to_string());
        }
        TermId::new(0)
    }
    fn term_blank(&mut self, _: Option<&str>) -> TermId {
        TermId::new(0)
    }
    fn term_literal(&mut self, _: &str, _: Datatype, _: Option<&str>) -> TermId {
        TermId::new(0)
    }
    fn term_literal_value(&mut self, _: LiteralValue, _: Datatype) -> TermId {
        TermId::new(0)
    }
    fn emit_triple(&mut self, _: TermId, _: TermId, _: TermId) -> SinkResult {
        Ok(())
    }
}

/// Forwards every event to the sink and records the prefix and base
/// declarations a segment makes, so the next segment is seeded with them.
struct Capture<'s, 'n> {
    sink: &'s mut TemplateSink<'n>,
    prefixes: &'s mut Vec<(String, String)>,
    base: &'s mut Option<String>,
}

impl GraphSink for Capture<'_, '_> {
    fn on_base(&mut self, base_iri: &str) {
        *self.base = Some(base_iri.to_string());
        self.sink.on_base(base_iri);
    }
    fn on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        match self.prefixes.iter_mut().find(|(p, _)| p == prefix) {
            Some(entry) => entry.1 = namespace_iri.to_string(),
            None => self
                .prefixes
                .push((prefix.to_string(), namespace_iri.to_string())),
        }
        self.sink.on_prefix(prefix, namespace_iri);
    }
    fn term_iri(&mut self, iri: &str) -> TermId {
        self.sink.term_iri(iri)
    }
    fn term_blank(&mut self, label: Option<&str>) -> TermId {
        self.sink.term_blank(label)
    }
    fn term_literal(&mut self, value: &str, datatype: Datatype, language: Option<&str>) -> TermId {
        self.sink.term_literal(value, datatype, language)
    }
    fn term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.sink.term_literal_value(value, datatype)
    }
    fn emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) -> SinkResult {
        self.sink.emit_triple(subject, predicate, object)
    }
    fn emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> SinkResult {
        self.sink.emit_list_item(subject, predicate, object, index)
    }
    fn supports_quads(&self) -> bool {
        self.sink.supports_quads()
    }
    fn emit_quad(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        graph: TermId,
    ) -> SinkResult {
        self.sink.emit_quad(subject, predicate, object, graph)
    }
    fn end_statement(&mut self) {
        self.sink.end_statement();
    }
    fn abort_statement(&mut self) {
        self.sink.abort_statement();
    }
    fn supports_reified_triples(&self) -> bool {
        self.sink.supports_reified_triples()
    }
    fn emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> SinkResult {
        self.sink
            .emit_reified_triple(subject, predicate, object, reifier)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{TemplateGraph, TemplateTerm, TripleTemplate};
    use fluree_db_core::{DatatypeConstraint, FlakeValue, Sid};
    use fluree_db_novelty::TxnMetaValue;

    const EX: &str = "@prefix ex: <http://example.org/> .\n";
    const XSD_STRING: &str = "^^<http://www.w3.org/2001/XMLSchema#string>";

    fn iri(ns: &NamespaceRegistry, sid: &Sid) -> String {
        format!(
            "{}{}",
            ns.get_prefix(sid.namespace_code).unwrap_or("?"),
            sid.name
        )
    }

    fn term(ns: &NamespaceRegistry, t: &TemplateTerm) -> String {
        match t {
            TemplateTerm::Sid(sid) => format!("<{}>", iri(ns, sid)),
            TemplateTerm::BlankNode(label) => format!("_:{label}"),
            TemplateTerm::Value(FlakeValue::String(s)) => format!("{s:?}"),
            TemplateTerm::Value(v) => format!("{v:?}"),
            TemplateTerm::Var(v) => format!("?{v:?}"),
        }
    }

    /// `[graph] s p o[@lang|^^dt][#i]`
    fn render(ns: &NamespaceRegistry, t: &TripleTemplate) -> String {
        let graph = match &t.graph {
            TemplateGraph::Default => String::new(),
            TemplateGraph::Iri(g) => format!("[{g}] "),
            TemplateGraph::Var(v) => format!("[?{v:?}] "),
        };
        let dtc = match &t.dtc {
            None => String::new(),
            Some(DatatypeConstraint::LangTag(l)) => format!("@{l}"),
            Some(DatatypeConstraint::Explicit(dt)) => format!("^^<{}>", iri(ns, dt)),
        };
        let i = t.list_index.map(|i| format!("#{i}")).unwrap_or_default();
        format!(
            "{graph}{} {} {}{dtc}{i}",
            term(ns, &t.subject),
            term(ns, &t.predicate),
            term(ns, &t.object)
        )
    }

    fn parse_ok(text: &str, placement: Placement<'_>) -> (RdfText, NamespaceRegistry) {
        let mut ns = NamespaceRegistry::new();
        let parsed = parse_rdf_text(text, placement, &mut ns).unwrap_or_else(|e| panic!("{e}"));
        (parsed, ns)
    }

    fn rendered(text: &str) -> Vec<String> {
        let (parsed, ns) = parse_ok(text, Placement::AsWritten);
        let mut out: Vec<String> = parsed
            .parts
            .templates
            .iter()
            .map(|t| render(&ns, t))
            .collect();
        out.sort();
        out
    }

    fn err(text: &str, placement: Placement<'_>) -> String {
        let mut ns = NamespaceRegistry::new();
        parse_rdf_text(text, placement, &mut ns)
            .expect_err("should be refused")
            .to_string()
    }

    #[test]
    fn a_parse_error_in_a_block_reports_the_document_offset() {
        let doc = format!(
            "{EX}GRAPH <http://g/1> {{ ex:a ex:p 1 }}\nGRAPH <http://g/2> {{ ex:b ex:p ; }}\n"
        );
        let e = err(&doc, Placement::AsWritten);
        let semicolon = doc.rfind(';').unwrap();
        let position: usize = e
            .split("position ")
            .nth(1)
            .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("no position in {e}"));
        let second_block = doc.find("GRAPH <http://g/2>").unwrap();
        assert!(
            position >= second_block && position <= semicolon + 2,
            "the offset points into the second block of the document: {e}"
        );
    }

    // ---- Directives in document order (U1) -----------------------------

    #[test]
    fn a_redefined_prefix_or_base_applies_only_after_it() {
        let doc = "@prefix ex: <http://a.org/> .\n\
                   ex:x ex:p \"mentions the graph word\" .\n\
                   GRAPH <http://g/1> { ex:y ex:p \"1\" . }\n\
                   @prefix ex: <http://b.org/> .\n\
                   ex:z ex:p \"2\" .\n\
                   GRAPH <http://g/2> { ex:w ex:p \"3\" . }\n";
        assert_eq!(
            rendered(doc),
            vec![
                format!(
                    "<http://a.org/x> <http://a.org/p> \"mentions the graph word\"{XSD_STRING}"
                ),
                format!("<http://b.org/z> <http://b.org/p> \"2\"{XSD_STRING}"),
                format!("[http://g/1] <http://a.org/y> <http://a.org/p> \"1\"{XSD_STRING}"),
                format!("[http://g/2] <http://b.org/w> <http://b.org/p> \"3\"{XSD_STRING}"),
            ]
        );
        let doc = "@base <http://a.org/> .\n<x> <p> \"1\" .\nGRAPH <g1> { <y> <p> \"2\" . }\n\
                   @base <http://b.org/> .\n<z> <p> \"3\" .\nGRAPH <g2> { <w> <p> \"4\" . }\n";
        assert_eq!(
            rendered(doc),
            vec![
                format!("<http://a.org/x> <http://a.org/p> \"1\"{XSD_STRING}"),
                format!("<http://b.org/z> <http://b.org/p> \"3\"{XSD_STRING}"),
                format!("[http://a.org/g1] <http://a.org/y> <http://a.org/p> \"2\"{XSD_STRING}"),
                format!("[http://b.org/g2] <http://b.org/w> <http://b.org/p> \"4\"{XSD_STRING}"),
            ]
        );
    }

    #[test]
    fn relative_references_need_a_base() {
        let e = err(
            "GRAPH <http://g> { <s> <http://p> 1 . }",
            Placement::AsWritten,
        );
        assert!(e.contains("relative"), "{e}");
        let e = err(
            "GRAPH <g> { <http://s> <http://p> 1 . }",
            Placement::AsWritten,
        );
        assert!(e.contains("graph label"), "{e}");
        // Outside a block, the same rule.
        let e = err("<s> <http://p> 1 .", Placement::AsWritten);
        assert!(e.contains("relative"), "{e}");
    }

    // ---- Block contents: the full grammar -------------------------------

    #[test]
    fn blocks_accept_the_full_turtle_grammar_and_match_the_default_graph() {
        let body = "ex:s ex:p [ ex:q \"x\" ] ; ex:list ( \"c\" \"a\" \"b\" \"a\" ) ; \
                    ex:nested ( ( 1 ) ) ; ex:empty () ; ex:anon [] ; a _:k , \"lit\" .\n\
                    ex:s ex:r ex:o ~ ex:reifier {| ex:source \"wiki\" |} .\n";
        let default = rendered(&format!("{EX}{body}"));
        let block = rendered(&format!("{EX}GRAPH <http://g> {{\n{body}}}\n"));
        let mut stripped: Vec<String> = block
            .iter()
            .filter(|t| !t.contains("reifiesGraph"))
            .map(|t| t.trim_start_matches("[http://g] ").to_string())
            .collect();
        stripped.sort();
        assert_eq!(
            stripped, default,
            "block contents parse exactly as default-graph Turtle"
        );
        assert!(
            block
                .iter()
                .any(|t| t.contains("reifiesGraph") && t.ends_with("<http://g>")),
            "a named-graph bundle carries its anchor: {block:?}"
        );
        assert!(
            !default.iter().any(|t| t.contains("reifiesGraph")),
            "a default-graph bundle has none"
        );
        // Collections keep order and duplicates.
        for (i, v) in ["c", "a", "b", "a"].iter().enumerate() {
            let needle = format!("\"{v}\"{XSD_STRING}#{i}");
            assert!(
                default
                    .iter()
                    .any(|t| t.contains("/list>") && t.ends_with(&needle)),
                "{needle}: {default:?}"
            );
        }
        // `rdf:type` takes a blank node and a literal like any predicate.
        let rdf_type = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
        assert!(default
            .iter()
            .any(|t| t.contains(rdf_type) && t.ends_with("_:k")));
        assert!(default
            .iter()
            .any(|t| t.contains(rdf_type) && t.contains("\"lit\"")));
    }

    #[test]
    fn blank_node_labels_are_document_scoped_and_anonymous_nodes_are_fresh() {
        let (parsed, ns) = parse_ok(
            &format!(
                "{EX}_:b ex:p 1 .\nGRAPH <http://g/1> {{ _:b ex:q 2 . [] ex:r 3 . }}\n\
                 GRAPH <http://g/2> {{ _:b ex:q 4 . [] ex:r 5 . }}\n"
            ),
            Placement::AsWritten,
        );
        let subjects: Vec<String> = parsed
            .parts
            .templates
            .iter()
            .map(|t| term(&ns, &t.subject))
            .collect();
        assert_eq!(
            subjects.iter().filter(|s| *s == "_:b").count(),
            3,
            "one label, one node, across the default graph and both blocks: {subjects:?}"
        );
        let anon: Vec<&String> = subjects.iter().filter(|s| s.starts_with("_:-b")).collect();
        assert_eq!(anon.len(), 2);
        assert_ne!(anon[0], anon[1], "`[]` in two blocks is two nodes");
    }

    /// A stable Fluree blank-node id (`_:fdb-…`) in a block addresses the
    /// stored node; any other label stays a blank node, minted at staging.
    #[test]
    fn a_stable_blank_node_id_in_a_block_addresses_the_stored_node() {
        let (parsed, ns) = parse_ok(
            "GRAPH <http://g/1> { _:fdb-1234-0-b0 <http://example.org/knows> _:other . }",
            Placement::AsWritten,
        );
        let [template] = &parsed.parts.templates[..] else {
            panic!("one template: {:?}", parsed.parts.templates);
        };
        assert!(
            matches!(&template.subject, TemplateTerm::Sid(sid) if *sid == ns.blank_node_sid("1234-0-b0")),
            "{:?}",
            template.subject
        );
        assert!(
            matches!(&template.object, TemplateTerm::BlankNode(label) if label == "other"),
            "{:?}",
            template.object
        );
    }

    /// A hand-written `f:reifies*` statement is refused in a block as in the
    /// default graph: annotation bundles come only from the annotation syntax.
    #[test]
    fn a_reserved_reifies_predicate_is_refused_in_a_block() {
        for doc in [
            "GRAPH <http://g/1> { <http://example.org/claim> \
             <https://ns.flur.ee/db#reifiesSubject> <http://example.org/evil> . }",
            "<http://example.org/claim> <https://ns.flur.ee/db#reifiesSubject> \
             <http://example.org/evil> .",
        ] {
            let mut ns = NamespaceRegistry::new();
            let e =
                parse_rdf_text(doc, Placement::AsWritten, &mut ns).expect_err("reserved predicate");
            assert!(
                matches!(e, TransactError::UnsupportedFeature(_))
                    && e.to_string().contains("system-controlled predicate"),
                "{doc}: {e}"
            );
        }
    }

    #[test]
    fn literals_convert_as_insert_converts_them() {
        // An ill-typed lexical form is kept, with its declared datatype, as
        // `FlakeSink` keeps it: one conversion on every RDF-text lane.
        let got = rendered(&format!(
            "{EX}@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
             ex:s ex:n \"abc\"^^xsd:integer ; ex:m \"12\"^^xsd:integer .\n"
        ));
        assert!(
            got.iter()
                .any(|t| t.contains("\"abc\"^^<http://www.w3.org/2001/XMLSchema#integer>")),
            "{got:?}"
        );
        assert!(got.iter().any(|t| t.contains("Long(12)")), "{got:?}");
    }

    // ---- txn-meta ------------------------------------------------------

    fn meta(doc: &str) -> Vec<(String, TxnMetaValue)> {
        let (parsed, _) = parse_ok(doc, Placement::AsWritten);
        parsed
            .parts
            .txn_meta
            .iter()
            .map(|e| (e.predicate_name.clone(), e.value.clone()))
            .collect()
    }

    const META_PREFIXES: &str = "@prefix ex: <http://example.org/> .\n\
                                 @prefix fluree: <https://ns.flur.ee/db#> .\n\
                                 @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";

    #[test]
    fn txn_meta_blocks_become_commit_metadata() {
        let doc = format!(
            "{META_PREFIXES}ex:alice ex:name \"Alice\" .\n\
             GRAPH <#txn-meta> {{\n\
               fluree:commit:this ex:machine \"server-01\" ; ex:batchId 42 ;\n\
                 ex:validated true ; ex:ratio 1.5e0 ; ex:author ex:alice ;\n\
                 ex:description \"Mise a jour\"@fr ;\n\
                 ex:timestamp \"2025-01-15T10:30:00Z\"^^xsd:dateTime ;\n\
                 ex:tags \"a\", \"b\" .\n\
             }}\n"
        );
        let entries = meta(&doc);
        let find = |p: &str| -> Vec<&TxnMetaValue> {
            entries
                .iter()
                .filter(|(n, _)| n == p)
                .map(|(_, v)| v)
                .collect()
        };
        assert!(matches!(find("machine")[..], [TxnMetaValue::String(s)] if s == "server-01"));
        assert!(matches!(find("batchId")[..], [TxnMetaValue::Long(42)]));
        assert!(matches!(
            find("validated")[..],
            [TxnMetaValue::Boolean(true)]
        ));
        assert!(matches!(find("ratio")[..], [TxnMetaValue::Double(d)] if (*d - 1.5).abs() < 1e-9));
        assert!(matches!(find("author")[..], [TxnMetaValue::Ref { name, .. }] if name == "alice"));
        assert!(matches!(find("description")[..],
            [TxnMetaValue::LangString { value, lang }] if value == "Mise a jour" && lang == "fr"));
        assert!(matches!(find("timestamp")[..],
            [TxnMetaValue::TypedLiteral { value, dt_name, .. }]
                if value == "2025-01-15T10:30:00Z" && dt_name == "dateTime"));
        assert_eq!(find("tags").len(), 2);
        // The default-graph triple is data, not metadata.
        let (parsed, _) = parse_ok(&doc, Placement::AsWritten);
        assert_eq!(parsed.statements, 1);
        // The scheme spelling, a full IRI subject, the compact block form and
        // SPARQL-style prefixes all read the same.
        for doc in [
            "PREFIX ex: <http://example.org/>\n<#txn-meta> { <fluree:commit:this> ex:k \"v\" }\n",
            "@prefix ex: <http://example.org/> .\n\
             GRAPH <#txn-meta> { <https://ns.flur.ee/db#commit:this> ex:k \"v\" . }\n",
        ] {
            assert!(
                matches!(&meta(doc)[..], [(k, TxnMetaValue::String(v))] if k == "k" && v == "v"),
                "{doc}"
            );
        }
    }

    #[test]
    fn txn_meta_refuses_what_commit_metadata_cannot_hold() {
        for (body, needle) in [
            ("ex:alice ex:machine \"server-01\" .", "fluree:commit:this"),
            ("_:b1 ex:machine \"server-01\" .", "blank nodes not allowed"),
            (
                "fluree:commit:this ex:source _:b1 .",
                "blank nodes not allowed",
            ),
            (
                "fluree:commit:this ex:source [ ex:p 1 ] .",
                "blank nodes not allowed",
            ),
            (
                "fluree:commit:this ex:items ( 1 2 ) .",
                "lists are not allowed",
            ),
            ("fluree:commit:this ex:p ex:o ~ ex:r .", "reifiers"),
        ] {
            let doc = format!("{META_PREFIXES}GRAPH <#txn-meta> {{ {body} }}\n");
            let e = err(&doc, Placement::AsWritten);
            assert!(e.contains(needle), "{body}: {e}");
        }
        // Under a base, `<#txn-meta>` resolves like any relative reference.
        let (parsed, _) = parse_ok(
            "@base <http://b.org/doc> .\nGRAPH <#txn-meta> { <http://s> <http://p> 1 . }\n",
            Placement::AsWritten,
        );
        assert!(parsed.parts.txn_meta.is_empty());
        assert!(parsed
            .parts
            .write_graphs
            .contains("http://b.org/doc#txn-meta"));
    }

    // ---- Content identity ------------------------------------------------

    fn content_id(doc: &str) -> u128 {
        parse_ok(doc, Placement::AsWritten).0.parts.content_id
    }

    #[test]
    fn content_identity_ignores_statement_order_but_not_content() {
        let a = format!(
            "{EX}ex:s ex:p [ ex:q \"x\" ] .\nex:t ex:p \"y\" , \"z\" .\nex:l ex:p ( 1 2 ) .\n"
        );
        let reordered = format!(
            "{EX}ex:l ex:p ( 1 2 ) .\nex:t ex:p \"z\" , \"y\" .\nex:s ex:p [ ex:q \"x\" ] .\n"
        );
        assert_eq!(content_id(&a), content_id(&reordered));
        let edited = a.replace("\"y\"", "\"y2\"");
        assert_ne!(content_id(&a), content_id(&edited));
        let list_order = a.replace("( 1 2 )", "( 2 1 )");
        assert_ne!(
            content_id(&a),
            content_id(&list_order),
            "list order is content"
        );
        let meta_only = format!(
            "{a}@prefix fluree: <https://ns.flur.ee/db#> .\n\
             GRAPH <#txn-meta> {{ fluree:commit:this ex:at \"t1\" }}\n"
        );
        assert_eq!(
            content_id(&a),
            content_id(&meta_only),
            "commit metadata does not change the data's identity"
        );
    }

    // ---- Placement ---------------------------------------------------------

    #[test]
    fn a_graph_scoped_write_homes_every_statement_and_checks_the_blocks() {
        let target = "http://g/t";
        let in_target =
            |t: &TripleTemplate| matches!(&t.graph, TemplateGraph::Iri(g) if &**g == target);
        let (parsed, ns) = parse_ok(
            &format!("{EX}GRAPH <{target}> {{ ex:a ex:p 1 ~ ex:r . }}\n"),
            Placement::Into(Some(target)),
        );
        assert!(parsed.parts.templates.iter().all(in_target));
        assert!(parsed
            .parts
            .templates
            .iter()
            .any(|t| render(&ns, t).contains("reifiesGraph")));
        let (parsed, _) = parse_ok(
            &format!("{EX}ex:a ex:p 1 .\n"),
            Placement::Into(Some(target)),
        );
        assert!(parsed.parts.templates.iter().all(in_target));
        assert!(parsed.parts.write_graphs.contains(target));

        let e = err(
            &format!("{EX}GRAPH <http://g/other> {{ ex:a ex:p 1 }}\n"),
            Placement::Into(Some(target)),
        );
        assert!(e.contains("<http://g/other>"), "{e}");
        let e = err(
            &format!("{EX}ex:b ex:p 2 .\nGRAPH <{target}> {{ ex:a ex:p 1 }}\n"),
            Placement::Into(Some(target)),
        );
        assert!(e.contains("not both"), "{e}");
        let e = err(
            &format!("{EX}GRAPH <{target}> {{ ex:a ex:p 1 }}\n"),
            Placement::Into(None),
        );
        assert!(e.contains("the default graph"), "{e}");
    }
}
