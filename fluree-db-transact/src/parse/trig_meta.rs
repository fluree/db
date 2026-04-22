//! TriG parser with named graph support
//!
//! This module provides a TriG parser that supports:
//! - Default graph triples (passed through to Turtle parser)
//! - Transaction metadata from the txn-meta graph (`fluree:commit:this` subject)
//! - Named graphs for user data (arbitrary graph IRIs)
//!
//! # Supported Syntax
//!
//! ```trig
//! @prefix ex: <http://example.org/> .
//! @prefix fluree: <https://ns.flur.ee/db#> .
//!
//! # Default graph triples (passed through to Turtle parser)
//! ex:alice ex:name "Alice" .
//!
//! # Transaction metadata block (special handling)
//! GRAPH <#txn-meta> {
//!     fluree:commit:this ex:machine "server-01" ;
//!                        ex:batchId 42 .
//! }
//!
//! # User-defined named graph
//! GRAPH <http://example.org/products> {
//!     ex:product1 ex:name "Widget" ;
//!                 ex:price 19.99 .
//! }
//! ```
//!
//! # Graph Types
//!
//! - **txn-meta graph** (`#txn-meta`): Subject must be
//!   `fluree:commit:this`. These triples become commit metadata (stored in envelope).
//! - **Named graphs**: Any other graph IRI. These triples are stored with the
//!   allocated g_id for that graph.
//!
//! # Constraints
//!
//! - Blank nodes are rejected in txn-meta blocks
//! - Blank nodes in named graph blocks are allowed (skolemized during ingest)

use crate::error::{Result, TransactError};
use crate::namespace::NamespaceRegistry;
use fluree_db_novelty::{TxnMetaEntry, TxnMetaValue, MAX_TXN_META_BYTES, MAX_TXN_META_ENTRIES};
use fluree_graph_turtle::{tokenize, Token, TokenKind};
use rustc_hash::FxHashMap;

/// IRI reference for the transaction metadata named graph.
///
/// This is intentionally a **fragment-only** IRI reference so it can be interpreted
/// in the context of a ledger reference (e.g., `<mydb:main#txn-meta>`).
pub const TXN_META_GRAPH_IRI: &str = "#txn-meta";

/// Check if an IRI represents the "this commit" placeholder.
fn is_commit_this_iri(iri: &str) -> bool {
    iri == fluree_vocab::fluree::COMMIT_THIS_HTTP || iri == fluree_vocab::fluree::COMMIT_THIS_SCHEME
}

/// Result of extracting transaction metadata from a TriG document.
#[derive(Debug)]
pub struct TrigMetaResult {
    /// Turtle content for the default graph (GRAPH blocks removed).
    /// This should be passed to the normal Turtle parser.
    pub turtle: String,
    /// Extracted transaction metadata entries (from txn-meta graph).
    pub txn_meta: Vec<TxnMetaEntry>,
    /// Named graph blocks (non-txn-meta graphs).
    /// Each entry is (graph_iri, triples).
    pub named_graphs: Vec<NamedGraphBlock>,
}

/// A named graph block with its IRI and triples.
#[derive(Debug, Clone)]
pub struct NamedGraphBlock {
    /// The graph IRI.
    pub iri: String,
    /// Triples in this graph.
    pub triples: Vec<RawTriple>,
    /// Prefix mappings from the TriG document (for IRI expansion).
    pub prefixes: FxHashMap<String, String>,
}

// =============================================================================
// Two-phase extraction API
// =============================================================================
//
// For callers that don't have a NamespaceRegistry at parse time (e.g., the
// transaction builder), we provide a two-phase API:
//
// Phase 1: parse_trig_phase1() - Parse TriG, extract GRAPH block, return cleaned
//          Turtle and raw metadata (IRIs as strings, not namespace codes)
//
// Phase 2: resolve_trig_meta() - Convert raw metadata to TxnMetaEntry using
//          the NamespaceRegistry

/// Result of Phase 1 TriG parsing (before namespace resolution).
#[derive(Debug, Clone)]
pub struct TrigPhase1Result {
    /// Cleaned Turtle content (GRAPH blocks removed).
    pub turtle: String,
    /// Raw metadata entries from txn-meta graph (if present).
    pub raw_meta: Option<RawTrigMeta>,
    /// Named graph blocks (non-txn-meta graphs).
    pub named_graphs: Vec<NamedGraphBlock>,
}

/// Intermediate TriG metadata representation (before namespace resolution).
///
/// This holds the parsed triples from the GRAPH block with IRIs as strings.
/// Use `resolve_trig_meta()` to convert to `TxnMetaEntry` with namespace codes.
#[derive(Debug, Clone)]
pub struct RawTrigMeta {
    /// Prefix mappings from the TriG document.
    pub prefixes: FxHashMap<String, String>,
    /// Parsed triples from the txn-meta GRAPH block.
    pub triples: Vec<RawTriple>,
}

/// A parsed triple from the GRAPH block (before namespace resolution).
#[derive(Debug, Clone)]
pub struct RawTriple {
    /// Subject (IRI, prefixed name, or blank node).
    /// For txn-meta triples this is None (always fluree:commit:this).
    /// For named graph triples this is Some(...).
    pub subject: Option<RawTerm>,
    /// Predicate (must be an IRI or prefixed name).
    pub predicate: RawTerm,
    /// Object values.
    pub objects: Vec<RawObject>,
}

/// A term (IRI or prefixed name) before namespace resolution.
#[derive(Debug, Clone)]
pub enum RawTerm {
    /// Full IRI.
    Iri(String),
    /// Prefixed name (e.g., "ex:machine").
    PrefixedName { prefix: String, local: String },
}

/// An object value before namespace resolution.
#[derive(Debug, Clone)]
pub enum RawObject {
    /// Full IRI.
    Iri(String),
    /// Prefixed name.
    PrefixedName { prefix: String, local: String },
    /// Plain string literal.
    String(String),
    /// Integer literal.
    Integer(i64),
    /// Double literal.
    Double(f64),
    /// Boolean literal.
    Boolean(bool),
    /// Typed literal with datatype IRI.
    TypedLiteral { value: String, datatype: String },
    /// Language-tagged string.
    LangString { value: String, lang: String },
}

/// Phase 1: Parse TriG input and extract GRAPH blocks (no namespace resolution).
///
/// This function parses the TriG input, validates the GRAPH block structure,
/// and returns:
/// - Cleaned Turtle content (with GRAPH blocks removed)
/// - Raw metadata entries (if txn-meta GRAPH block was present)
/// - Named graph blocks (for non-txn-meta graphs)
///
/// The raw metadata can be converted to `TxnMetaEntry` using `resolve_trig_meta()`
/// once a `NamespaceRegistry` is available.
pub fn parse_trig_phase1(input: &str) -> Result<TrigPhase1Result> {
    // Check if input contains GRAPH keyword - if not, pass through as-is
    if !contains_graph_keyword(input) {
        return Ok(TrigPhase1Result {
            turtle: input.to_string(),
            raw_meta: None,
            named_graphs: Vec::new(),
        });
    }

    // Tokenize the input
    let tokens = tokenize(input).map_err(|e| TransactError::Parse(e.to_string()))?;

    // Parse into structured form
    let mut parser = TrigMetaParser::new(input, &tokens);
    parser.parse()?;

    // Extract phase 1 result (no namespace resolution)
    parser.extract_phase1()
}

/// Phase 2: Resolve raw TriG metadata to TxnMetaEntry using namespace registry.
///
/// This converts the intermediate `RawTrigMeta` representation to final
/// `TxnMetaEntry` values with namespace codes.
pub fn resolve_trig_meta(
    raw: &RawTrigMeta,
    ns_registry: &mut NamespaceRegistry,
) -> Result<Vec<TxnMetaEntry>> {
    let mut entries = Vec::new();

    for triple in &raw.triples {
        // Expand predicate to IRI
        let predicate_iri = match &triple.predicate {
            RawTerm::Iri(iri) => iri.clone(),
            RawTerm::PrefixedName { prefix, local } => {
                expand_prefixed_name(&raw.prefixes, prefix, local)?
            }
        };

        // Convert predicate to namespace code + name
        let pred_sid = ns_registry.sid_for_iri(&predicate_iri);

        // Convert each object to TxnMetaEntry
        for obj in &triple.objects {
            let value = raw_object_to_txn_meta_value(obj, &raw.prefixes, ns_registry)?;
            entries.push(TxnMetaEntry::new(
                pred_sid.namespace_code,
                pred_sid.name.to_string(),
                value,
            ));
        }
    }

    validate_limits(&entries)?;
    Ok(entries)
}

/// Expand a prefixed name to a full IRI using the prefix map.
fn expand_prefixed_name(
    prefixes: &FxHashMap<String, String>,
    prefix: &str,
    local: &str,
) -> Result<String> {
    let ns = prefixes
        .get(prefix)
        .ok_or_else(|| TransactError::Parse(format!("undefined prefix: {prefix}")))?;
    Ok(format!("{ns}{local}"))
}

/// Convert a RawObject to TxnMetaValue using the namespace registry.
fn raw_object_to_txn_meta_value(
    obj: &RawObject,
    prefixes: &FxHashMap<String, String>,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TxnMetaValue> {
    match obj {
        RawObject::String(s) => Ok(TxnMetaValue::String(s.clone())),
        RawObject::Integer(n) => Ok(TxnMetaValue::Long(*n)),
        RawObject::Double(n) => {
            if !n.is_finite() {
                return Err(TransactError::Parse(
                    "txn-meta does not support non-finite double values".to_string(),
                ));
            }
            Ok(TxnMetaValue::Double(*n))
        }
        RawObject::Boolean(b) => Ok(TxnMetaValue::Boolean(*b)),
        RawObject::Iri(iri) => {
            let sid = ns_registry.sid_for_iri(iri);
            Ok(TxnMetaValue::Ref {
                ns: sid.namespace_code,
                name: sid.name.to_string(),
            })
        }
        RawObject::PrefixedName { prefix, local } => {
            let iri = expand_prefixed_name(prefixes, prefix, local)?;
            let sid = ns_registry.sid_for_iri(&iri);
            Ok(TxnMetaValue::Ref {
                ns: sid.namespace_code,
                name: sid.name.to_string(),
            })
        }
        RawObject::LangString { value, lang } => Ok(TxnMetaValue::LangString {
            value: value.clone(),
            lang: lang.clone(),
        }),
        RawObject::TypedLiteral { value, datatype } => {
            let dt_sid = ns_registry.sid_for_iri(datatype);
            Ok(TxnMetaValue::TypedLiteral {
                value: value.clone(),
                dt_ns: dt_sid.namespace_code,
                dt_name: dt_sid.name.to_string(),
            })
        }
    }
}

/// Extract transaction metadata and named graphs from a TriG document.
///
/// This function:
/// 1. Parses @prefix/@base directives into a shared prefix map
/// 2. Finds all `GRAPH <iri> { ... }` blocks
/// 3. For txn-meta graph: extracts triples where subject is `fluree:commit:this`
/// 4. For other graphs: returns the triples for later processing
/// 5. Returns the default graph content as Turtle + extracted metadata + named graphs
///
/// # Errors
///
/// Returns an error if:
/// - Subject in txn-meta GRAPH block is not `fluree:commit:this`
/// - Blank nodes appear in txn-meta block
/// - Entry count or size limits exceeded
pub fn extract_trig_txn_meta(
    input: &str,
    ns_registry: &mut NamespaceRegistry,
) -> Result<TrigMetaResult> {
    // Check if input contains GRAPH keyword - if not, pass through as-is
    if !contains_graph_keyword(input) {
        return Ok(TrigMetaResult {
            turtle: input.to_string(),
            txn_meta: Vec::new(),
            named_graphs: Vec::new(),
        });
    }

    // Tokenize the input
    let tokens = tokenize(input).map_err(|e| TransactError::Parse(e.to_string()))?;

    // Parse into structured form
    let mut parser = TrigMetaParser::new(input, &tokens);
    parser.parse()?;

    // Extract txn-meta, named graphs, and rebuild Turtle
    let (turtle, txn_meta, named_graphs) = parser.extract(ns_registry)?;

    validate_limits(&txn_meta)?;

    Ok(TrigMetaResult {
        turtle,
        txn_meta,
        named_graphs,
    })
}

/// Quick check if input might contain a GRAPH block.
fn contains_graph_keyword(input: &str) -> bool {
    // Case-insensitive check for GRAPH keyword
    let upper = input.to_ascii_uppercase();
    upper.contains("GRAPH")
}

/// Parser state for TriG metadata extraction.
struct TrigMetaParser<'a> {
    input: &'a str,
    tokens: &'a [Token],
    pos: usize,
    /// Prefix mappings: prefix -> namespace IRI
    prefixes: FxHashMap<String, String>,
    /// Base IRI
    base: Option<String>,
    /// Collected directives (for reconstructing Turtle output)
    directives: Vec<(usize, usize)>, // (start, end) byte ranges
    /// Default graph triple ranges
    default_triples: Vec<(usize, usize)>,
    /// All GRAPH blocks (supports multiple named graphs)
    graph_blocks: Vec<GraphBlock>,
}

/// Information about a GRAPH block.
struct GraphBlock {
    /// The graph IRI
    iri: String,
    /// Triples inside the GRAPH block
    triples: Vec<ParsedTriple>,
}

/// A parsed triple (subject, predicate, objects).
struct ParsedTriple {
    subject: TermValue,
    predicate: TermValue,
    objects: Vec<ObjectValue>,
}

/// A term value (IRI or blank node).
#[derive(Clone)]
enum TermValue {
    Iri(String),
    PrefixedName {
        prefix: String,
        local: String,
    },
    #[allow(dead_code)] // Used for rejection/validation
    BlankNode(String),
}

/// An object value (literal or IRI reference).
#[derive(Clone)]
enum ObjectValue {
    Iri(String),
    PrefixedName {
        prefix: String,
        local: String,
    },
    #[allow(dead_code)] // Used for rejection/validation
    BlankNode(String),
    String(String),
    Integer(i64),
    Double(f64),
    Boolean(bool),
    TypedLiteral {
        value: String,
        datatype: String,
    },
    LangString {
        value: String,
        lang: String,
    },
}

impl<'a> TrigMetaParser<'a> {
    fn new(input: &'a str, tokens: &'a [Token]) -> Self {
        Self {
            input,
            tokens,
            pos: 0,
            prefixes: FxHashMap::default(),
            base: None,
            directives: Vec::new(),
            default_triples: Vec::new(),
            graph_blocks: Vec::new(),
        }
    }

    fn parse(&mut self) -> Result<()> {
        while !self.is_at_end() {
            self.parse_statement()?;
        }
        Ok(())
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.tokens.len() || matches!(self.tokens[self.pos].kind, TokenKind::Eof)
    }

    fn current(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn advance(&mut self) {
        if !self.is_at_end() {
            self.pos += 1;
        }
    }

    fn check(&self, kind: &TokenKind) -> bool {
        if self.is_at_end() {
            return false;
        }
        std::mem::discriminant(&self.tokens[self.pos].kind) == std::mem::discriminant(kind)
    }

    fn span_text(&self, start: u32, end: u32) -> &'a str {
        &self.input[start as usize..end as usize]
    }

    fn iri_content(&self, start: u32, end: u32) -> &'a str {
        &self.input[(start as usize + 1)..(end as usize - 1)]
    }

    fn prefix_ns_content(&self, start: u32, end: u32) -> &'a str {
        &self.input[start as usize..(end as usize - 1)]
    }

    fn parse_statement(&mut self) -> Result<()> {
        let start_pos = self.current().start as usize;

        match &self.tokens[self.pos].kind {
            TokenKind::KwPrefix | TokenKind::KwSparqlPrefix => {
                self.parse_prefix_directive()?;
                let end_pos = self.tokens[self.pos.saturating_sub(1)].end as usize;
                self.directives.push((start_pos, end_pos));
            }
            TokenKind::KwBase | TokenKind::KwSparqlBase => {
                self.parse_base_directive()?;
                let end_pos = self.tokens[self.pos.saturating_sub(1)].end as usize;
                self.directives.push((start_pos, end_pos));
            }
            TokenKind::KwGraph => {
                self.parse_graph_block()?;
            }
            TokenKind::Eof => {}
            _ => {
                // Default graph triple
                self.parse_default_triple(start_pos)?;
            }
        }
        Ok(())
    }

    fn parse_prefix_directive(&mut self) -> Result<()> {
        let is_sparql = matches!(self.current().kind, TokenKind::KwSparqlPrefix);
        self.advance(); // consume PREFIX/@prefix

        // Get prefix name
        let prefix = match &self.current().kind {
            TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                self.prefix_ns_content(s, e).to_string()
            }
            _ => {
                return Err(TransactError::Parse(
                    "expected prefix namespace in directive".to_string(),
                ))
            }
        };
        self.advance();

        // Get namespace IRI
        let namespace = self.parse_iri()?;

        // Register prefix
        self.prefixes.insert(prefix, namespace);

        // Consume trailing dot if not SPARQL style
        if !is_sparql && self.check(&TokenKind::Dot) {
            self.advance();
        }

        Ok(())
    }

    fn parse_base_directive(&mut self) -> Result<()> {
        let is_sparql = matches!(self.current().kind, TokenKind::KwSparqlBase);
        self.advance(); // consume BASE/@base

        let base_iri = self.parse_iri()?;
        self.base = Some(base_iri);

        if !is_sparql && self.check(&TokenKind::Dot) {
            self.advance();
        }

        Ok(())
    }

    fn parse_iri(&mut self) -> Result<String> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.advance();
                Ok(self.resolve_iri(iri))
            }
            TokenKind::IriEscaped(iri) => {
                self.advance();
                Ok(self.resolve_iri(&iri))
            }
            _ => Err(TransactError::Parse(format!(
                "expected IRI, found {:?}",
                self.current().kind
            ))),
        }
    }

    fn resolve_iri(&self, iri: &str) -> String {
        // Simple resolution - if relative and base is set, resolve against base
        if let Some(base) = &self.base {
            if !iri.contains(':') {
                return format!("{base}{iri}");
            }
        }
        iri.to_string()
    }

    fn expand_prefixed_name(&self, prefix: &str, local: &str) -> Result<String> {
        if let Some(namespace) = self.prefixes.get(prefix) {
            Ok(format!("{namespace}{local}"))
        } else {
            Err(TransactError::Parse(format!("undefined prefix: {prefix}")))
        }
    }

    fn parse_graph_block(&mut self) -> Result<()> {
        self.advance(); // consume GRAPH

        // Parse graph IRI
        let graph_iri = match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e).to_string();
                self.advance();
                self.resolve_iri(&iri)
            }
            TokenKind::IriEscaped(iri) => {
                self.advance();
                self.resolve_iri(&iri)
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                let span = self.span_text(s, e);
                let (prefix, local) = split_prefixed_name(span);
                self.advance();
                self.expand_prefixed_name(prefix, local)?
            }
            _ => {
                return Err(TransactError::Parse(format!(
                    "expected graph IRI, found {:?}",
                    self.current().kind
                )))
            }
        };

        // Expect opening brace
        if !self.check(&TokenKind::LBrace) {
            return Err(TransactError::Parse(
                "expected '{' after GRAPH IRI".to_string(),
            ));
        }
        self.advance();

        // Parse triples inside the GRAPH block
        let mut triples = Vec::new();
        while !self.check(&TokenKind::RBrace) && !self.is_at_end() {
            let parsed = self.parse_triple()?;
            triples.extend(parsed);
        }

        // Expect closing brace
        if !self.check(&TokenKind::RBrace) {
            return Err(TransactError::Parse(
                "expected '}' to close GRAPH block".to_string(),
            ));
        }
        self.advance();

        // Store the graph block (supports multiple GRAPH blocks)
        self.graph_blocks.push(GraphBlock {
            iri: graph_iri,
            triples,
        });

        Ok(())
    }

    fn parse_triple(&mut self) -> Result<Vec<ParsedTriple>> {
        // Parse subject
        let subject = self.parse_subject()?;

        // Parse predicate-object list
        let mut triples = Vec::new();
        let predicate = self.parse_predicate()?;
        let objects = self.parse_object_list()?;

        triples.push(ParsedTriple {
            subject: subject.clone(),
            predicate,
            objects,
        });

        // Handle semicolon-separated predicate-object pairs
        while self.check(&TokenKind::Semicolon) {
            self.advance();
            if self.check(&TokenKind::Dot) || self.check(&TokenKind::RBrace) || self.is_at_end() {
                break;
            }
            let predicate = self.parse_predicate()?;
            let objects = self.parse_object_list()?;
            triples.push(ParsedTriple {
                subject: subject.clone(),
                predicate,
                objects,
            });
        }

        // Expect dot
        if self.check(&TokenKind::Dot) {
            self.advance();
        }

        Ok(triples)
    }

    fn parse_subject(&mut self) -> Result<TermValue> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e).to_string();
                self.advance();
                Ok(TermValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::IriEscaped(iri) => {
                self.advance();
                Ok(TermValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                let span = self.span_text(s, e);
                let (prefix, local) = split_prefixed_name(span);
                self.advance();
                Ok(TermValue::PrefixedName {
                    prefix: prefix.to_string(),
                    local: local.to_string(),
                })
            }
            TokenKind::BlankNodeLabel => {
                let s = self.current().start;
                let e = self.current().end;
                let label = &self.input[(s as usize + 2)..e as usize];
                self.advance();
                Ok(TermValue::BlankNode(label.to_string()))
            }
            _ => Err(TransactError::Parse(format!(
                "expected subject, found {:?}",
                self.current().kind
            ))),
        }
    }

    fn parse_predicate(&mut self) -> Result<TermValue> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e).to_string();
                self.advance();
                Ok(TermValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::IriEscaped(iri) => {
                self.advance();
                Ok(TermValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                let span = self.span_text(s, e);
                let (prefix, local) = split_prefixed_name(span);
                self.advance();
                Ok(TermValue::PrefixedName {
                    prefix: prefix.to_string(),
                    local: local.to_string(),
                })
            }
            TokenKind::KwA => {
                self.advance();
                Ok(TermValue::Iri(fluree_vocab::rdf::TYPE.to_string()))
            }
            _ => Err(TransactError::Parse(format!(
                "expected predicate, found {:?}",
                self.current().kind
            ))),
        }
    }

    fn parse_object_list(&mut self) -> Result<Vec<ObjectValue>> {
        let mut objects = vec![self.parse_object()?];

        while self.check(&TokenKind::Comma) {
            self.advance();
            objects.push(self.parse_object()?);
        }

        Ok(objects)
    }

    fn parse_object(&mut self) -> Result<ObjectValue> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e).to_string();
                self.advance();
                Ok(ObjectValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::IriEscaped(iri) => {
                self.advance();
                Ok(ObjectValue::Iri(self.resolve_iri(&iri)))
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                let span = self.span_text(s, e);
                let (prefix, local) = split_prefixed_name(span);
                self.advance();
                Ok(ObjectValue::PrefixedName {
                    prefix: prefix.to_string(),
                    local: local.to_string(),
                })
            }
            TokenKind::BlankNodeLabel => {
                let s = self.current().start;
                let e = self.current().end;
                let label = &self.input[(s as usize + 2)..e as usize];
                self.advance();
                Ok(ObjectValue::BlankNode(label.to_string()))
            }
            TokenKind::String | TokenKind::LongString => self.parse_string_literal(),
            TokenKind::StringEscaped(value) => {
                let value = value.to_string();
                self.advance();
                self.parse_string_literal_suffix(value)
            }
            TokenKind::Integer(n) => {
                let val = n;
                self.advance();
                Ok(ObjectValue::Integer(val))
            }
            TokenKind::Double(n) => {
                let val = n;
                self.advance();
                if !val.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values".to_string(),
                    ));
                }
                Ok(ObjectValue::Double(val))
            }
            TokenKind::Decimal => {
                // Treat decimal as double for simplicity
                let s = self.current().start;
                let e = self.current().end;
                let text = self.span_text(s, e);
                self.advance();
                let n: f64 = text
                    .parse()
                    .map_err(|_| TransactError::Parse(format!("invalid decimal: {text}")))?;
                Ok(ObjectValue::Double(n))
            }
            TokenKind::KwTrue => {
                self.advance();
                Ok(ObjectValue::Boolean(true))
            }
            TokenKind::KwFalse => {
                self.advance();
                Ok(ObjectValue::Boolean(false))
            }
            _ => Err(TransactError::Parse(format!(
                "expected object, found {:?}",
                self.current().kind
            ))),
        }
    }

    fn parse_string_literal(&mut self) -> Result<ObjectValue> {
        let (s, e) = (self.current().start, self.current().end);
        let is_long = matches!(self.current().kind, TokenKind::LongString);
        let quote_len = if is_long { 3 } else { 1 };
        let value = self.input[(s as usize + quote_len)..(e as usize - quote_len)].to_string();
        self.advance();
        self.parse_string_literal_suffix(value)
    }

    fn parse_string_literal_suffix(&mut self, value: String) -> Result<ObjectValue> {
        match &self.current().kind {
            TokenKind::LangTag => {
                let s = self.current().start;
                let e = self.current().end;
                let lang = self.input[(s as usize + 1)..e as usize].to_string();
                self.advance();
                Ok(ObjectValue::LangString { value, lang })
            }
            TokenKind::DoubleCaret => {
                self.advance();
                let datatype = match self.current().kind.clone() {
                    TokenKind::Iri => {
                        let s = self.current().start;
                        let e = self.current().end;
                        let iri = self.iri_content(s, e).to_string();
                        self.advance();
                        self.resolve_iri(&iri)
                    }
                    TokenKind::IriEscaped(iri) => {
                        self.advance();
                        self.resolve_iri(&iri)
                    }
                    TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                        let s = self.current().start;
                        let e = self.current().end;
                        let span = self.span_text(s, e);
                        let (prefix, local) = split_prefixed_name(span);
                        self.advance();
                        self.expand_prefixed_name(prefix, local)?
                    }
                    _ => {
                        return Err(TransactError::Parse(format!(
                            "expected datatype IRI, found {:?}",
                            self.current().kind
                        )))
                    }
                };
                Ok(ObjectValue::TypedLiteral { value, datatype })
            }
            _ => Ok(ObjectValue::String(value)),
        }
    }

    fn parse_default_triple(&mut self, start_pos: usize) -> Result<()> {
        // Skip to end of triple (dot terminator)
        while !self.check(&TokenKind::Dot) && !self.is_at_end() {
            self.advance();
        }
        if self.check(&TokenKind::Dot) {
            self.advance();
        }
        let end_pos = self.tokens[self.pos.saturating_sub(1)].end as usize;
        self.default_triples.push((start_pos, end_pos));
        Ok(())
    }

    /// Extract txn-meta entries, named graphs, and reconstruct Turtle content.
    fn extract(
        self,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<(String, Vec<TxnMetaEntry>, Vec<NamedGraphBlock>)> {
        let mut txn_meta = Vec::new();
        let mut named_graphs = Vec::new();

        // Process GRAPH blocks
        for block in &self.graph_blocks {
            if block.iri == TXN_META_GRAPH_IRI {
                // txn-meta graph: extract as TxnMetaEntry
                for triple in &block.triples {
                    // Validate subject is fluree:commit:this
                    let subject_iri = match &triple.subject {
                        TermValue::Iri(iri) => iri.clone(),
                        TermValue::PrefixedName { prefix, local } => {
                            self.expand_prefixed_name(prefix, local)?
                        }
                        TermValue::BlankNode(_) => {
                            return Err(TransactError::Parse(
                                "blank nodes not allowed as txn-meta subject".to_string(),
                            ))
                        }
                    };

                    if !is_commit_this_iri(&subject_iri) {
                        return Err(TransactError::Parse(format!(
                            "txn-meta subject must be fluree:commit:this, found: {subject_iri}"
                        )));
                    }

                    // Get predicate IRI
                    let predicate_iri = match &triple.predicate {
                        TermValue::Iri(iri) => iri.clone(),
                        TermValue::PrefixedName { prefix, local } => {
                            self.expand_prefixed_name(prefix, local)?
                        }
                        TermValue::BlankNode(_) => {
                            return Err(TransactError::Parse(
                                "blank nodes not allowed as txn-meta predicate".to_string(),
                            ))
                        }
                    };

                    // Convert predicate to ns_code + name
                    let pred_sid = ns_registry.sid_for_iri(&predicate_iri);

                    // Convert each object to TxnMetaEntry
                    for obj in &triple.objects {
                        let value = self.object_to_txn_meta_value(obj, ns_registry)?;
                        txn_meta.push(TxnMetaEntry::new(
                            pred_sid.namespace_code,
                            pred_sid.name.to_string(),
                            value,
                        ));
                    }
                }
            } else {
                // Named graph: convert to RawTriples for later processing
                let raw_triples = self.convert_to_raw_triples(&block.triples)?;
                named_graphs.push(NamedGraphBlock {
                    iri: block.iri.clone(),
                    triples: raw_triples,
                    prefixes: self.prefixes.clone(),
                });
            }
        }

        // Reconstruct Turtle content (directives + default triples)
        let mut turtle = String::new();

        // Add directives
        for (start, end) in &self.directives {
            turtle.push_str(&self.input[*start..*end]);
            turtle.push('\n');
        }

        // Add default graph triples
        for (start, end) in &self.default_triples {
            turtle.push_str(&self.input[*start..*end]);
            turtle.push('\n');
        }

        Ok((turtle, txn_meta, named_graphs))
    }

    /// Convert ParsedTriples to RawTriples for named graph blocks.
    fn convert_to_raw_triples(&self, triples: &[ParsedTriple]) -> Result<Vec<RawTriple>> {
        let mut result = Vec::new();
        for triple in triples {
            // Convert subject - include it in the RawTriple (needed for named graphs)
            let subject = match &triple.subject {
                TermValue::Iri(iri) => RawTerm::Iri(iri.clone()),
                TermValue::PrefixedName { prefix, local } => RawTerm::PrefixedName {
                    prefix: prefix.clone(),
                    local: local.clone(),
                },
                TermValue::BlankNode(label) => {
                    // Blank nodes are allowed in named graphs (will be skolemized)
                    RawTerm::Iri(format!("_:{label}"))
                }
            };

            // Convert predicate
            let predicate = match &triple.predicate {
                TermValue::Iri(iri) => RawTerm::Iri(iri.clone()),
                TermValue::PrefixedName { prefix, local } => RawTerm::PrefixedName {
                    prefix: prefix.clone(),
                    local: local.clone(),
                },
                TermValue::BlankNode(_) => {
                    return Err(TransactError::Parse(
                        "blank nodes not allowed as predicate".to_string(),
                    ))
                }
            };

            // Convert objects
            let objects: Vec<RawObject> = triple
                .objects
                .iter()
                .map(|obj| self.convert_object_to_raw(obj))
                .collect::<Result<Vec<_>>>()?;

            result.push(RawTriple {
                subject: Some(subject),
                predicate,
                objects,
            });
        }
        Ok(result)
    }

    /// Convert an ObjectValue to RawObject.
    fn convert_object_to_raw(&self, obj: &ObjectValue) -> Result<RawObject> {
        match obj {
            ObjectValue::String(s) => Ok(RawObject::String(s.clone())),
            ObjectValue::Integer(n) => Ok(RawObject::Integer(*n)),
            ObjectValue::Double(n) => {
                if !n.is_finite() {
                    return Err(TransactError::Parse(
                        "non-finite double values not supported".to_string(),
                    ));
                }
                Ok(RawObject::Double(*n))
            }
            ObjectValue::Boolean(b) => Ok(RawObject::Boolean(*b)),
            ObjectValue::Iri(iri) => Ok(RawObject::Iri(iri.clone())),
            ObjectValue::PrefixedName { prefix, local } => Ok(RawObject::PrefixedName {
                prefix: prefix.clone(),
                local: local.clone(),
            }),
            ObjectValue::BlankNode(label) => {
                // Blank nodes in objects are allowed in named graphs
                Ok(RawObject::Iri(format!("_:{label}")))
            }
            ObjectValue::LangString { value, lang } => Ok(RawObject::LangString {
                value: value.clone(),
                lang: lang.clone(),
            }),
            ObjectValue::TypedLiteral { value, datatype } => Ok(RawObject::TypedLiteral {
                value: value.clone(),
                datatype: datatype.clone(),
            }),
        }
    }

    fn object_to_txn_meta_value(
        &self,
        obj: &ObjectValue,
        ns_registry: &mut NamespaceRegistry,
    ) -> Result<TxnMetaValue> {
        match obj {
            ObjectValue::String(s) => Ok(TxnMetaValue::String(s.clone())),
            ObjectValue::Integer(n) => Ok(TxnMetaValue::Long(*n)),
            ObjectValue::Double(n) => {
                if !n.is_finite() {
                    return Err(TransactError::Parse(
                        "txn-meta does not support non-finite double values".to_string(),
                    ));
                }
                Ok(TxnMetaValue::Double(*n))
            }
            ObjectValue::Boolean(b) => Ok(TxnMetaValue::Boolean(*b)),
            ObjectValue::Iri(iri) => {
                let sid = ns_registry.sid_for_iri(iri);
                Ok(TxnMetaValue::Ref {
                    ns: sid.namespace_code,
                    name: sid.name.to_string(),
                })
            }
            ObjectValue::PrefixedName { prefix, local } => {
                let iri = self.expand_prefixed_name(prefix, local)?;
                let sid = ns_registry.sid_for_iri(&iri);
                Ok(TxnMetaValue::Ref {
                    ns: sid.namespace_code,
                    name: sid.name.to_string(),
                })
            }
            ObjectValue::BlankNode(_) => Err(TransactError::Parse(
                "blank nodes not allowed in txn-meta objects".to_string(),
            )),
            ObjectValue::LangString { value, lang } => Ok(TxnMetaValue::LangString {
                value: value.clone(),
                lang: lang.clone(),
            }),
            ObjectValue::TypedLiteral { value, datatype } => {
                let dt_sid = ns_registry.sid_for_iri(datatype);
                Ok(TxnMetaValue::TypedLiteral {
                    value: value.clone(),
                    dt_ns: dt_sid.namespace_code,
                    dt_name: dt_sid.name.to_string(),
                })
            }
        }
    }

    /// Phase 1 extraction: return raw triples without namespace resolution.
    fn extract_phase1(self) -> Result<TrigPhase1Result> {
        // Reconstruct Turtle content (directives + default triples)
        let mut turtle = String::new();

        // Add directives
        for (start, end) in &self.directives {
            turtle.push_str(&self.input[*start..*end]);
            turtle.push('\n');
        }

        // Add default graph triples
        for (start, end) in &self.default_triples {
            turtle.push_str(&self.input[*start..*end]);
            turtle.push('\n');
        }

        let mut raw_meta: Option<RawTrigMeta> = None;
        let mut named_graphs: Vec<NamedGraphBlock> = Vec::new();

        // Process all GRAPH blocks
        for block in &self.graph_blocks {
            if block.iri == TXN_META_GRAPH_IRI {
                // txn-meta graph: convert to RawTrigMeta
                let mut triples = Vec::new();

                for triple in &block.triples {
                    // Validate subject is fluree:commit:this
                    let subject_iri = match &triple.subject {
                        TermValue::Iri(iri) => iri.clone(),
                        TermValue::PrefixedName { prefix, local } => {
                            self.expand_prefixed_name(prefix, local)?
                        }
                        TermValue::BlankNode(_) => {
                            return Err(TransactError::Parse(
                                "blank nodes not allowed as txn-meta subject".to_string(),
                            ))
                        }
                    };

                    if !is_commit_this_iri(&subject_iri) {
                        return Err(TransactError::Parse(format!(
                            "txn-meta subject must be fluree:commit:this, found: {subject_iri}"
                        )));
                    }

                    // Convert predicate to RawTerm
                    let predicate = match &triple.predicate {
                        TermValue::Iri(iri) => RawTerm::Iri(iri.clone()),
                        TermValue::PrefixedName { prefix, local } => RawTerm::PrefixedName {
                            prefix: prefix.clone(),
                            local: local.clone(),
                        },
                        TermValue::BlankNode(_) => {
                            return Err(TransactError::Parse(
                                "blank nodes not allowed as txn-meta predicate".to_string(),
                            ))
                        }
                    };

                    // Convert objects to RawObject
                    let objects: Vec<RawObject> = triple
                        .objects
                        .iter()
                        .map(|obj| match obj {
                            ObjectValue::String(s) => Ok(RawObject::String(s.clone())),
                            ObjectValue::Integer(n) => Ok(RawObject::Integer(*n)),
                            ObjectValue::Double(n) => {
                                if !n.is_finite() {
                                    return Err(TransactError::Parse(
                                        "txn-meta does not support non-finite double values"
                                            .to_string(),
                                    ));
                                }
                                Ok(RawObject::Double(*n))
                            }
                            ObjectValue::Boolean(b) => Ok(RawObject::Boolean(*b)),
                            ObjectValue::Iri(iri) => Ok(RawObject::Iri(iri.clone())),
                            ObjectValue::PrefixedName { prefix, local } => {
                                Ok(RawObject::PrefixedName {
                                    prefix: prefix.clone(),
                                    local: local.clone(),
                                })
                            }
                            ObjectValue::BlankNode(_) => Err(TransactError::Parse(
                                "blank nodes not allowed in txn-meta objects".to_string(),
                            )),
                            ObjectValue::LangString { value, lang } => Ok(RawObject::LangString {
                                value: value.clone(),
                                lang: lang.clone(),
                            }),
                            ObjectValue::TypedLiteral { value, datatype } => {
                                Ok(RawObject::TypedLiteral {
                                    value: value.clone(),
                                    datatype: datatype.clone(),
                                })
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    triples.push(RawTriple {
                        subject: None, // txn-meta subject is always fluree:commit:this
                        predicate,
                        objects,
                    });
                }

                raw_meta = Some(RawTrigMeta {
                    prefixes: self.prefixes.clone(),
                    triples,
                });
            } else {
                // Named graph: convert to NamedGraphBlock
                let raw_triples = self.convert_to_raw_triples(&block.triples)?;
                named_graphs.push(NamedGraphBlock {
                    iri: block.iri.clone(),
                    triples: raw_triples,
                    prefixes: self.prefixes.clone(),
                });
            }
        }

        Ok(TrigPhase1Result {
            turtle,
            raw_meta,
            named_graphs,
        })
    }
}

/// Split a prefixed name into prefix and local parts.
fn split_prefixed_name(span: &str) -> (&str, &str) {
    match span.find(':') {
        Some(pos) => (&span[..pos], &span[pos + 1..]),
        None => (span, ""),
    }
}

/// Validate txn-meta limits.
fn validate_limits(entries: &[TxnMetaEntry]) -> Result<()> {
    if entries.len() > MAX_TXN_META_ENTRIES {
        return Err(TransactError::Parse(format!(
            "txn-meta entry count {} exceeds maximum {}",
            entries.len(),
            MAX_TXN_META_ENTRIES
        )));
    }

    // Estimate encoded size
    let mut estimated_bytes: usize = 0;
    for entry in entries {
        estimated_bytes += 6 + entry.predicate_name.len();
        estimated_bytes += 1 + estimate_value_size(&entry.value);
    }

    if estimated_bytes > MAX_TXN_META_BYTES {
        return Err(TransactError::Parse(format!(
            "txn-meta estimated size {estimated_bytes} bytes exceeds maximum {MAX_TXN_META_BYTES} bytes"
        )));
    }

    Ok(())
}

fn estimate_value_size(value: &TxnMetaValue) -> usize {
    match value {
        TxnMetaValue::String(s) => 4 + s.len(),
        TxnMetaValue::Long(_) => 8,
        TxnMetaValue::Double(_) => 8,
        TxnMetaValue::Boolean(_) => 1,
        TxnMetaValue::Ref { name, .. } => 6 + name.len(),
        TxnMetaValue::LangString { value, lang } => 8 + value.len() + lang.len(),
        TxnMetaValue::TypedLiteral { value, dt_name, .. } => 10 + value.len() + dt_name.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry() -> NamespaceRegistry {
        NamespaceRegistry::new()
    }

    #[test]
    fn test_no_graph_passthrough() {
        let mut ns = test_registry();
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice" .
        "#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert_eq!(result.turtle, input);
    }

    #[test]
    fn test_basic_txn_meta_extraction() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

ex:alice ex:name "Alice" .

GRAPH <#txn-meta> {
    fluree:commit:this ex:machine "server-01" ;
                       ex:batchId 42 .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();

        // Should have extracted metadata
        assert_eq!(result.txn_meta.len(), 2);

        // Find machine entry
        let machine = result
            .txn_meta
            .iter()
            .find(|e| e.predicate_name == "machine")
            .unwrap();
        assert!(matches!(&machine.value, TxnMetaValue::String(s) if s == "server-01"));

        // Find batchId entry
        let batch = result
            .txn_meta
            .iter()
            .find(|e| e.predicate_name == "batchId")
            .unwrap();
        assert!(matches!(&batch.value, TxnMetaValue::Long(42)));

        // Turtle should contain prefixes and default graph triples
        assert!(result.turtle.contains("@prefix ex:"));
        assert!(result.turtle.contains("ex:alice ex:name"));
        // But not the GRAPH block
        assert!(!result.turtle.contains("GRAPH"));
    }

    #[test]
    fn test_named_graph_accepted() {
        // Named graphs (non-txn-meta) are now accepted and returned in named_graphs
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <http://example.org/my-graph> {
    ex:alice ex:name "Alice" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty()); // No txn-meta
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "http://example.org/my-graph");
        assert_eq!(result.named_graphs[0].triples.len(), 1);

        // Subject should be included for named graphs
        let triple = &result.named_graphs[0].triples[0];
        assert!(triple.subject.is_some());
    }

    #[test]
    fn test_multiple_named_graphs() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

ex:bob ex:name "Bob" .

GRAPH <http://example.org/products> {
    ex:widget ex:name "Widget" ;
              ex:price 19 .
}

GRAPH <http://example.org/orders> {
    ex:order1 ex:item ex:widget ;
              ex:qty 5 .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert_eq!(result.named_graphs.len(), 2);

        // Find each graph by IRI
        let products = result
            .named_graphs
            .iter()
            .find(|g| g.iri == "http://example.org/products")
            .expect("products graph");
        let orders = result
            .named_graphs
            .iter()
            .find(|g| g.iri == "http://example.org/orders")
            .expect("orders graph");

        assert_eq!(products.triples.len(), 2); // name and price
        assert_eq!(orders.triples.len(), 2); // item and qty

        // Default graph should have bob
        assert!(result.turtle.contains("ex:bob ex:name"));
    }

    #[test]
    fn test_mixed_txn_meta_and_named_graph() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:machine "server-01" .
}

GRAPH <http://example.org/data> {
    ex:alice ex:name "Alice" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();

        // Should have txn-meta
        assert_eq!(result.txn_meta.len(), 1);
        assert_eq!(result.txn_meta[0].predicate_name, "machine");

        // And a named graph
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "http://example.org/data");
    }

    #[test]
    fn test_reject_wrong_subject() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <#txn-meta> {
    ex:alice ex:machine "server-01" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("fluree:commit:this"));
    }

    #[test]
    fn test_reject_blank_node_subject() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <#txn-meta> {
    _:b1 ex:machine "server-01" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("blank nodes not allowed"));
    }

    #[test]
    fn test_reject_blank_node_object() {
        let mut ns = test_registry();
        let input = r"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:source _:b1 .
}
";

        let result = extract_trig_txn_meta(input, &mut ns);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("blank nodes not allowed"));
    }

    #[test]
    fn test_typed_literal() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:timestamp "2025-01-15T10:30:00Z"^^xsd:dateTime .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);

        if let TxnMetaValue::TypedLiteral { value, dt_name, .. } = &result.txn_meta[0].value {
            assert_eq!(value, "2025-01-15T10:30:00Z");
            assert_eq!(dt_name, "dateTime");
        } else {
            panic!("Expected typed literal");
        }
    }

    #[test]
    fn test_language_tagged_string() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:description "Mise a jour"@fr .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);

        if let TxnMetaValue::LangString { value, lang } = &result.txn_meta[0].value {
            assert_eq!(value, "Mise a jour");
            assert_eq!(lang, "fr");
        } else {
            panic!("Expected lang string");
        }
    }

    #[test]
    fn test_iri_reference_object() {
        let mut ns = test_registry();
        let input = r"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:author ex:alice .
}
";

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);

        if let TxnMetaValue::Ref { name, .. } = &result.txn_meta[0].value {
            assert_eq!(name, "alice");
        } else {
            panic!("Expected IRI ref");
        }
    }

    #[test]
    fn test_boolean_values() {
        let mut ns = test_registry();
        let input = r"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:validated true .
}
";

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);
        assert!(matches!(
            &result.txn_meta[0].value,
            TxnMetaValue::Boolean(true)
        ));
    }

    #[test]
    fn test_sparql_style_prefix() {
        let mut ns = test_registry();
        let input = r#"
PREFIX ex: <http://example.org/>
PREFIX fluree: <https://ns.flur.ee/db#>

ex:alice ex:name "Alice" .

GRAPH <#txn-meta> {
    fluree:commit:this ex:source "import" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);
        assert!(matches!(&result.txn_meta[0].value, TxnMetaValue::String(s) if s == "import"));
    }

    #[test]
    fn test_full_iri_subject() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <#txn-meta> {
    <https://ns.flur.ee/db#commit:this> ex:note "test" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);
    }

    #[test]
    fn test_scheme_based_commit_this() {
        // Users can use fluree:commit:this directly (scheme-based form)
        // without needing a @prefix fluree: definition.
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <#txn-meta> {
    <fluree:commit:this> ex:note "scheme form" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);
        assert!(matches!(&result.txn_meta[0].value, TxnMetaValue::String(s) if s == "scheme form"));
    }

    #[test]
    fn test_multiple_objects_comma() {
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

GRAPH <#txn-meta> {
    fluree:commit:this ex:tags "a", "b", "c" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 3);
        assert!(result.txn_meta.iter().all(|e| e.predicate_name == "tags"));
    }
}
