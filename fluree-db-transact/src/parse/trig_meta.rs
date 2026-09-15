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
//! # RDF 1.2 (TriG-star)
//!
//! Named graph blocks accept the asserting star forms — `{| … |}`, `~ r`,
//! `<< s p o ~ r >>` in either position, `r rdf:reifies <<( s p o )>>` —
//! and surface each as a [`RawReifiedTriple`] on the block, with the base
//! triple asserted alongside. Consumers emit the `f:reifies*` bundle in the
//! block's graph. Default-graph statements are handed to the streaming
//! Turtle parser, which handles the same forms itself.
//!
//! # Constraints
//!
//! - Blank nodes are rejected in txn-meta blocks
//! - Blank nodes in named graph blocks are allowed (skolemized during ingest)
//! - Star constructs are rejected in txn-meta blocks

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
    /// RDF 1.2 reifier attachments (TriG-star) in this graph. The reified
    /// base triple is also present in `triples` (Fluree asserts it), so
    /// consumers emit the `f:reifies*` bundle from here and nothing else.
    pub reified: Vec<RawReifiedTriple>,
    /// Prefix mappings from the TriG document (for IRI expansion).
    pub prefixes: FxHashMap<String, String>,
}

/// One RDF 1.2 reifier attachment parsed inside a GRAPH block: `reifier`
/// reifies the edge `(subject, predicate, object)`. Produced by every
/// TriG-star spelling (`{| … |}`, `~ r`, `<< s p o ~ r >>` in either
/// position, `r rdf:reifies <<( s p o )>>`) so the consumers see one shape.
#[derive(Debug, Clone)]
pub struct RawReifiedTriple {
    pub subject: RawTerm,
    pub predicate: RawTerm,
    pub object: RawObject,
    pub reifier: RawTerm,
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
    // Check if input might contain a graph block - if not, pass through as-is
    if !might_contain_graph_block(input) {
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
    // Check if input might contain a graph block - if not, pass through as-is
    if !might_contain_graph_block(input) {
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

/// Quick check if input might contain a graph block — either the SPARQL-style
/// `GRAPH <iri> { ... }` keyword form or the W3C-compliant compact `<iri> { ... }`
/// form (the `GRAPH` keyword is optional per the TriG grammar).
///
/// A `{` triggers parsing because every graph block (both forms) contains one,
/// while plain Turtle has no block syntax — the only way a `{` reaches here in
/// non-TriG content is inside a string literal, and such inputs round-trip
/// unchanged through the parser (the brace stays within a `String` token).
/// The `GRAPH` keyword is still checked so a malformed `GRAPH <iri>` (missing
/// braces) is routed to the parser for a proper error rather than passed
/// through silently.
fn might_contain_graph_block(input: &str) -> bool {
    // Cheap, common-case-first: a brace is present in every graph block.
    if input.contains('{') {
        return true;
    }
    // Case-insensitive check for the GRAPH keyword.
    input.to_ascii_uppercase().contains("GRAPH")
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
    /// Triples of the statement currently being parsed inside a GRAPH
    /// block — the statement's own predicate-object pairs plus the base
    /// triples asserted by star constructs and annotation-body triples.
    stmt_triples: Vec<ParsedTriple>,
    /// Reifier attachments of the GRAPH block currently being parsed.
    reified: Vec<ParsedReified>,
    /// Counter for fresh anonymous reifiers (`{| … |}` without `~`, bare
    /// `~`, reifier-less `<< … >>`). The leading `-` keeps the label out
    /// of the user-writable BLANK_NODE_LABEL space, as the Turtle sink does.
    anon_reifiers: u32,
    /// Nesting depth of `<< … >>` reified triples (bounded by [`MAX_STAR_DEPTH`]).
    star_depth: u32,
    /// Non-zero while parsing a `{| … |}` body: star constructs there are
    /// the deferred annotation-of-annotation shape.
    annotation_depth: u32,
}

/// Information about a GRAPH block.
struct GraphBlock {
    /// The graph IRI
    iri: String,
    /// Triples inside the GRAPH block
    triples: Vec<ParsedTriple>,
    /// Reifier attachments inside the GRAPH block (TriG-star)
    reified: Vec<ParsedReified>,
}

/// A reifier attachment before namespace resolution (see [`RawReifiedTriple`]).
struct ParsedReified {
    subject: TermValue,
    predicate: TermValue,
    object: ObjectValue,
    reifier: TermValue,
}

/// Bound on `<< << … >> … >>` nesting so adversarial input errors instead
/// of overflowing the stack.
const MAX_STAR_DEPTH: u32 = 32;

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
            stmt_triples: Vec::new(),
            reified: Vec::new(),
            anon_reifiers: 0,
            star_depth: 0,
            annotation_depth: 0,
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

    /// Kind of the token `offset` positions ahead of the cursor, if any.
    /// The tokenizer emits no whitespace/comment tokens, so the immediate
    /// successor is `offset == 1`.
    fn peek_kind(&self, offset: usize) -> Option<&TokenKind> {
        self.tokens.get(self.pos + offset).map(|t| &t.kind)
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
            // `VERSION "1.2"` has no trailing dot, so the default-triple
            // skipper would swallow the next directive with it.
            TokenKind::KwVersion | TokenKind::KwSparqlVersion => {
                self.parse_version_directive()?;
                let end_pos = self.tokens[self.pos.saturating_sub(1)].end as usize;
                self.directives.push((start_pos, end_pos));
            }
            TokenKind::KwGraph => {
                self.advance(); // consume the GRAPH keyword
                self.parse_graph_block()?;
            }
            // W3C-compliant compact graph block: `<iri> { ... }` or
            // `prefix:name { ... }` (the GRAPH keyword is optional per the TriG
            // grammar). A bare IRI / prefixed name followed by `{` can only be a
            // labeled graph block — in a default-graph triple the subject is
            // always followed by a predicate, never a brace.
            TokenKind::Iri
            | TokenKind::IriEscaped(_)
            | TokenKind::PrefixedName
            | TokenKind::PrefixedNameNs
                if matches!(self.peek_kind(1), Some(TokenKind::LBrace)) =>
            {
                self.parse_graph_block()?;
            }
            // Anonymous default-graph wrapped block `{ ... }`. Valid W3C TriG
            // (it denotes the default graph), but unsupported here: this parser
            // separates default-graph Turtle from labeled graph blocks. Reject
            // cleanly rather than letting the brace leak into the reconstructed
            // Turtle and surface as a misleading low-level parse error.
            TokenKind::LBrace => {
                return Err(TransactError::Parse(
                    "anonymous default-graph block `{ ... }` is not supported; \
                     write default-graph triples directly (outside any block), \
                     or use a labeled graph block `<iri> { ... }`"
                        .to_string(),
                ));
            }
            // Blank-node graph label (`_:b { ... }`). Valid W3C TriG, but not
            // supported here in either form (the keyword form rejects it too).
            // Emit a clear error instead of a silent mis-parse.
            TokenKind::BlankNodeLabel if matches!(self.peek_kind(1), Some(TokenKind::LBrace)) => {
                return Err(TransactError::Parse(
                    "blank-node graph labels are not supported; \
                     use an IRI graph label, e.g. `<iri> { ... }`"
                        .to_string(),
                ));
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

    /// Skip the RDF 1.2 version directive; the specifier is kept in the
    /// reconstructed Turtle, whose parser validates it.
    fn parse_version_directive(&mut self) -> Result<()> {
        let is_sparql = matches!(self.current().kind, TokenKind::KwSparqlVersion);
        self.advance(); // consume VERSION/@version

        if !matches!(
            self.current().kind,
            TokenKind::String | TokenKind::StringEscaped(_)
        ) {
            return Err(TransactError::Parse(format!(
                "expected a quoted version specifier such as \"1.2\", found {}",
                self.current().kind
            )));
        }
        self.advance();

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

    /// Resolve a potentially relative IRI reference against `@base` per
    /// RFC 3986 §5, via the shared `fluree_vocab::iri` resolver — the same
    /// semantics as the Turtle parser and SPARQL prologue handling. The old
    /// `!iri.contains(':')` + concat heuristic mis-resolved sibling
    /// references (`<sibling.ttl>` was appended, not merged), `<//host/x>`,
    /// `</rooted>`, and dot-segments, and mis-classified colon-in-path
    /// relatives (`<a/b:c>`) as absolute. Without a `@base`, relative
    /// references stay as written (ledger-local names) — this import path's
    /// historical behavior, which also keeps the relative `<#txn-meta>`
    /// sentinel matching `TXN_META_GRAPH_IRI` exactly.
    fn resolve_iri(&self, iri: &str) -> String {
        match &self.base {
            Some(base) if !fluree_vocab::iri::is_absolute_iri(iri) => {
                fluree_vocab::iri::resolve_iri(base, iri)
            }
            _ => iri.to_string(),
        }
    }

    fn expand_prefixed_name(&self, prefix: &str, local: &str) -> Result<String> {
        if let Some(namespace) = self.prefixes.get(prefix) {
            Ok(format!("{namespace}{local}"))
        } else {
            Err(TransactError::Parse(format!("undefined prefix: {prefix}")))
        }
    }

    /// Parse a graph block starting at the graph label (the optional `GRAPH`
    /// keyword, if present, must already be consumed by the caller). Handles
    /// both `GRAPH <iri> { ... }` and the compact `<iri> { ... }` form.
    fn parse_graph_block(&mut self) -> Result<()> {
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

        let reified = std::mem::take(&mut self.reified);
        // Only the `<#txn-meta>` sidecar spelling is known here; a write to the
        // ledger's full txn-meta IRI is refused in `stage()`.
        if graph_iri == TXN_META_GRAPH_IRI && !reified.is_empty() {
            return Err(TransactError::Parse(
                "RDF 1.2 reifiers and annotations are not allowed in the txn-meta graph; \
                 its triples become commit metadata, not graph edges"
                    .to_string(),
            ));
        }

        // Store the graph block (supports multiple GRAPH blocks)
        self.graph_blocks.push(GraphBlock {
            iri: graph_iri,
            triples,
            reified,
        });

        Ok(())
    }

    // ---------------------------------------------------------------------
    // RDF 1.2 star constructs inside GRAPH blocks (TriG-star)
    // ---------------------------------------------------------------------
    //
    // Mirrors the streaming Turtle parser's asserting forms: the reified
    // base triple is asserted, each anonymous occurrence mints a fresh
    // reifier, `<<( … )>>` is a value only as the object of `rdf:reifies`,
    // and star constructs inside an annotation body are deferred.

    fn triple_term_value_error(&self) -> TransactError {
        TransactError::Parse(
            "RDF 1.2 triple terms as values ('<<( … )>>') are deferred; inside a TriG \
             GRAPH block a triple term is accepted only as the object of rdf:reifies"
                .to_string(),
        )
    }

    fn annotation_of_annotation_error(&self) -> TransactError {
        TransactError::Parse(
            "RDF 1.2 star constructs nested inside an annotation body ('{| … |}') are \
             the deferred annotation-of-annotation shape; annotate the base triple instead"
                .to_string(),
        )
    }

    fn fresh_reifier(&mut self) -> TermValue {
        self.anon_reifiers += 1;
        TermValue::BlankNode(format!("-r{}", self.anon_reifiers))
    }

    fn predicate_is_reifies(&self, predicate: &TermValue) -> Result<bool> {
        Ok(match predicate {
            TermValue::Iri(iri) => iri == fluree_vocab::rdf::REIFIES,
            TermValue::PrefixedName { prefix, local } => {
                self.expand_prefixed_name(prefix, local)? == fluree_vocab::rdf::REIFIES
            }
            TermValue::BlankNode(_) => false,
        })
    }

    fn attach_reifier(
        &mut self,
        subject: &TermValue,
        predicate: &TermValue,
        object: &ObjectValue,
        reifier: &TermValue,
    ) {
        self.reified.push(ParsedReified {
            subject: subject.clone(),
            predicate: predicate.clone(),
            object: object.clone(),
            reifier: reifier.clone(),
        });
    }

    /// Assert the reified base triple (Fluree's documented divergence from
    /// RDF 1.2's non-asserting `<< … >>` / `rdf:reifies`).
    fn assert_base_triple(
        &mut self,
        subject: &TermValue,
        predicate: &TermValue,
        object: &ObjectValue,
    ) {
        self.stmt_triples.push(ParsedTriple {
            subject: subject.clone(),
            predicate: predicate.clone(),
            objects: vec![object.clone()],
        });
    }

    /// `<< rtSubject predicate rtObject ( ~ reifier )? >>` — returns the
    /// reifier term, which is what the construct denotes in its position.
    fn parse_reified_triple(&mut self) -> Result<TermValue> {
        if self.annotation_depth > 0 {
            return Err(self.annotation_of_annotation_error());
        }
        if self.star_depth >= MAX_STAR_DEPTH {
            return Err(TransactError::Parse(format!(
                "nesting of reified triples ('<< … >>') exceeds the maximum depth of {MAX_STAR_DEPTH}"
            )));
        }
        self.star_depth += 1;
        let result = self.parse_reified_triple_inner();
        self.star_depth -= 1;
        result
    }

    fn parse_reified_triple_inner(&mut self) -> Result<TermValue> {
        self.advance(); // `<<`
        let subject = self.parse_subject()?;
        let predicate = self.parse_predicate()?;
        let object = match self.current().kind {
            TokenKind::TripleTermStart => return Err(self.triple_term_value_error()),
            _ => self.parse_object()?,
        };
        let reifier = if self.check(&TokenKind::Tilde) {
            self.advance();
            self.parse_reifier_term()?
        } else {
            self.fresh_reifier()
        };
        if !self.check(&TokenKind::ReifiedTripleEnd) {
            return Err(TransactError::Parse(format!(
                "expected '>>' to close reified triple, found {:?}",
                self.current().kind
            )));
        }
        self.advance();
        self.assert_base_triple(&subject, &predicate, &object);
        self.attach_reifier(&subject, &predicate, &object, &reifier);
        Ok(reifier)
    }

    /// `r rdf:reifies <<( ttSubject predicate ttObject )>>` — the `<<(`
    /// token is current, `reifier` is the statement subject.
    fn parse_reifies_triple_term(&mut self, reifier: &TermValue) -> Result<()> {
        if self.annotation_depth > 0 {
            return Err(self.annotation_of_annotation_error());
        }
        self.advance(); // `<<(`
        let subject = match self.current().kind {
            TokenKind::ReifiedTripleStart | TokenKind::TripleTermStart => {
                return Err(TransactError::Parse(
                    "expected triple-term subject (IRI or blank node)".to_string(),
                ))
            }
            _ => self.parse_subject()?,
        };
        let predicate = self.parse_predicate()?;
        let object = match self.current().kind {
            TokenKind::TripleTermStart => return Err(self.triple_term_value_error()),
            TokenKind::ReifiedTripleStart => {
                return Err(TransactError::Parse(
                    "reified triples ('<< … >>') are not allowed inside a triple term".to_string(),
                ))
            }
            _ => self.parse_object()?,
        };
        if !self.check(&TokenKind::TripleTermEnd) {
            return Err(TransactError::Parse(format!(
                "expected ')>>' to close triple term, found {:?}",
                self.current().kind
            )));
        }
        self.advance();
        if matches!(
            self.current().kind,
            TokenKind::Tilde | TokenKind::AnnotationOpen
        ) {
            return Err(TransactError::Parse(
                "an annotation tail on an 'rdf:reifies <<( … )>>' statement would reify \
                 the reification itself (annotation-of-annotation), which is deferred; \
                 annotate the base triple instead"
                    .to_string(),
            ));
        }
        self.assert_base_triple(&subject, &predicate, &object);
        self.attach_reifier(&subject, &predicate, &object, reifier);
        Ok(())
    }

    /// `reifier ::= '~' (iri | BlankNode)?` — the `~` is already consumed;
    /// a bare `~` mints a fresh anonymous reifier.
    fn parse_reifier_term(&mut self) -> Result<TermValue> {
        match self.current().kind {
            TokenKind::Iri
            | TokenKind::IriEscaped(_)
            | TokenKind::PrefixedName
            | TokenKind::PrefixedNameNs
            | TokenKind::BlankNodeLabel => self.parse_subject(),
            _ => Ok(self.fresh_reifier()),
        }
    }

    /// `annotation ::= (reifier | annotationBlock)*` after an object. A
    /// `~ r` attaches `r` and stays pending so a following `{| … |}`
    /// describes the same reifier; a block without a pending reifier
    /// mints a fresh one. Body triples are about the reifier.
    fn parse_annotation_tail(
        &mut self,
        subject: &TermValue,
        predicate: &TermValue,
        object: &ObjectValue,
    ) -> Result<()> {
        let mut pending: Option<TermValue> = None;
        loop {
            match self.current().kind {
                TokenKind::Tilde => {
                    if self.annotation_depth > 0 {
                        return Err(self.annotation_of_annotation_error());
                    }
                    self.advance();
                    let reifier = self.parse_reifier_term()?;
                    self.attach_reifier(subject, predicate, object, &reifier);
                    pending = Some(reifier);
                }
                TokenKind::AnnotationOpen => {
                    if self.annotation_depth > 0 {
                        return Err(self.annotation_of_annotation_error());
                    }
                    self.advance();
                    let reifier = match pending.take() {
                        Some(r) => r,
                        None => {
                            let r = self.fresh_reifier();
                            self.attach_reifier(subject, predicate, object, &r);
                            r
                        }
                    };
                    if !self.check(&TokenKind::AnnotationClose) {
                        self.annotation_depth += 1;
                        let body = self.parse_predicate_object_list(&reifier);
                        self.annotation_depth -= 1;
                        body?;
                    }
                    if !self.check(&TokenKind::AnnotationClose) {
                        return Err(TransactError::Parse(format!(
                            "expected '|}}' to close annotation block, found {:?}",
                            self.current().kind
                        )));
                    }
                    self.advance();
                }
                _ => break,
            }
        }
        Ok(())
    }

    fn parse_triple(&mut self) -> Result<Vec<ParsedTriple>> {
        let subject = self.parse_subject()?;
        self.parse_predicate_object_list(&subject)?;

        // Expect dot
        if self.check(&TokenKind::Dot) {
            self.advance();
        }

        Ok(std::mem::take(&mut self.stmt_triples))
    }

    /// `predicateObjectList ::= verb objectList (';' (verb objectList)?)*`
    /// — pushes each pair onto `stmt_triples`. Shared by statements and
    /// annotation bodies (where `|}` also terminates a trailing `;`).
    fn parse_predicate_object_list(&mut self, subject: &TermValue) -> Result<()> {
        loop {
            let predicate = self.parse_predicate()?;
            let objects = self.parse_object_list(subject, &predicate)?;
            // Empty only for `rdf:reifies <<( … )>>`, whose base triple and
            // attachment were recorded directly.
            if !objects.is_empty() {
                self.stmt_triples.push(ParsedTriple {
                    subject: subject.clone(),
                    predicate,
                    objects,
                });
            }

            if !self.check(&TokenKind::Semicolon) {
                break;
            }
            self.advance();
            if self.check(&TokenKind::Dot)
                || self.check(&TokenKind::RBrace)
                || self.check(&TokenKind::AnnotationClose)
                || self.is_at_end()
            {
                break;
            }
        }
        Ok(())
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
            TokenKind::ReifiedTripleStart => self.parse_reified_triple(),
            TokenKind::TripleTermStart => Err(self.triple_term_value_error()),
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

    /// `objectList ::= object annotation? (',' object annotation?)*`, with
    /// the `rdf:reifies <<( … )>>` object form handled in place.
    fn parse_object_list(
        &mut self,
        subject: &TermValue,
        predicate: &TermValue,
    ) -> Result<Vec<ObjectValue>> {
        let mut objects = Vec::with_capacity(1);
        loop {
            if self.check(&TokenKind::TripleTermStart) {
                if !self.predicate_is_reifies(predicate)? {
                    return Err(self.triple_term_value_error());
                }
                self.parse_reifies_triple_term(subject)?;
            } else {
                let object = self.parse_object()?;
                if matches!(
                    self.current().kind,
                    TokenKind::Tilde | TokenKind::AnnotationOpen
                ) {
                    self.parse_annotation_tail(subject, predicate, &object)?;
                }
                objects.push(object);
            }

            if self.check(&TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
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
            TokenKind::IntegerOverflow => {
                // Beyond i64: keep the lexical so downstream promotes to BigInt.
                let s = self.current().start;
                let e = self.current().end;
                let text = self.span_text(s, e).to_string();
                self.advance();
                Ok(ObjectValue::TypedLiteral {
                    value: text,
                    datatype: fluree_vocab::xsd::INTEGER.to_string(),
                })
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
                // Bare decimal literals are xsd:decimal per the Turtle grammar.
                // Keep the exact lexical and route through the typed-literal
                // lane (shared exact parser downstream) — going through f64
                // here both corrupts the value and re-types it xsd:double,
                // diverging from the default-graph Turtle path.
                let s = self.current().start;
                let e = self.current().end;
                let text = self.span_text(s, e).to_string();
                self.advance();
                Ok(ObjectValue::TypedLiteral {
                    value: text,
                    datatype: fluree_vocab::xsd::DECIMAL.to_string(),
                })
            }
            TokenKind::KwTrue => {
                self.advance();
                Ok(ObjectValue::Boolean(true))
            }
            TokenKind::KwFalse => {
                self.advance();
                Ok(ObjectValue::Boolean(false))
            }
            // A reified triple in object position denotes its reifier node.
            TokenKind::ReifiedTripleStart => Ok(match self.parse_reified_triple()? {
                TermValue::Iri(iri) => ObjectValue::Iri(iri),
                TermValue::PrefixedName { prefix, local } => {
                    ObjectValue::PrefixedName { prefix, local }
                }
                TermValue::BlankNode(label) => ObjectValue::BlankNode(label),
            }),
            TokenKind::TripleTermStart => Err(self.triple_term_value_error()),
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
                let reified = self.convert_reified_to_raw(&block.reified)?;
                named_graphs.push(NamedGraphBlock {
                    iri: block.iri.clone(),
                    triples: raw_triples,
                    reified,
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

    /// Convert a subject/reifier term (blank nodes allowed, skolemized later).
    fn convert_node_to_raw(term: &TermValue) -> RawTerm {
        match term {
            TermValue::Iri(iri) => RawTerm::Iri(iri.clone()),
            TermValue::PrefixedName { prefix, local } => RawTerm::PrefixedName {
                prefix: prefix.clone(),
                local: local.clone(),
            },
            TermValue::BlankNode(label) => RawTerm::Iri(format!("_:{label}")),
        }
    }

    fn convert_reified_to_raw(&self, reified: &[ParsedReified]) -> Result<Vec<RawReifiedTriple>> {
        reified
            .iter()
            .map(|r| {
                let predicate = match &r.predicate {
                    TermValue::BlankNode(_) => {
                        return Err(TransactError::Parse(
                            "blank nodes not allowed as predicate".to_string(),
                        ))
                    }
                    other => Self::convert_node_to_raw(other),
                };
                Ok(RawReifiedTriple {
                    subject: Self::convert_node_to_raw(&r.subject),
                    predicate,
                    object: self.convert_object_to_raw(&r.object)?,
                    reifier: Self::convert_node_to_raw(&r.reifier),
                })
            })
            .collect()
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
                let reified = self.convert_reified_to_raw(&block.reified)?;
                named_graphs.push(NamedGraphBlock {
                    iri: block.iri.clone(),
                    triples: raw_triples,
                    reified,
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
    fn test_base_resolves_relative_references_rfc3986() {
        // bplatz's PR-1454 case: this is the production TriG import path,
        // and a document-IRI @base with a sibling relative reference must
        // MERGE paths per RFC 3986 §5 — the old concat heuristic produced
        // "http://ex.org/data/doc.trigsibling.ttl".
        let mut ns = test_registry();
        let input = r#"
@base <http://ex.org/data/doc.trig> .
@prefix ex: <http://example.org/> .

GRAPH <sibling.ttl> {
    ex:alice ex:name "Alice" .
}

GRAPH <#annotations> {
    ex:alice ex:note "fragment-named graph" .
}

GRAPH </rooted> {
    ex:alice ex:note "root-relative graph" .
}

GRAPH <urn:absolute:g> {
    ex:alice ex:note "absolute passthrough" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        let iris: Vec<&str> = result.named_graphs.iter().map(|g| g.iri.as_str()).collect();
        assert_eq!(
            iris,
            vec![
                "http://ex.org/data/sibling.ttl",
                "http://ex.org/data/doc.trig#annotations",
                "http://ex.org/rooted",
                "urn:absolute:g",
            ],
            "RFC 3986 §5 resolution on the TriG import path"
        );
    }

    #[test]
    fn test_relative_references_without_base_stay_as_written() {
        // Ledger-local names: without @base, relative references pass
        // through verbatim (also keeps the `<#txn-meta>` sentinel intact —
        // covered by the txn-meta tests above).
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

GRAPH <local-graph> {
    ex:alice ex:name "Alice" .
}
"#;
        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "local-graph");
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

    // ---- W3C-compliant compact graph block form: `<iri> { ... }` (issue #1278) ----

    #[test]
    fn test_compact_named_graph() {
        // The compact form omits the `GRAPH` keyword. The graph IRI here does
        // NOT contain the substring "graph", so it also exercises the gate.
        let mut ns = test_registry();
        let input = r"
@prefix ex: <http://example.org/> .

<urn:g1> {
    ex:alice ex:knows ex:bob .
}
";

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "urn:g1");
        assert_eq!(result.named_graphs[0].triples.len(), 1);
        // The graph block must NOT leak into the default-graph Turtle.
        assert!(!result.turtle.contains('{'));
        assert!(!result.turtle.contains("ex:alice"));
    }

    #[test]
    fn test_compact_named_graph_prefixed() {
        // Compact form with a prefixed-name graph label.
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

ex:g1 {
    ex:alice ex:name "Alice" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "http://example.org/g1");
        assert_eq!(result.named_graphs[0].triples.len(), 1);
    }

    #[test]
    fn test_compact_and_keyword_forms_equivalent() {
        // The compact form and the keyword form must produce identical results.
        let compact = r#"
@prefix ex: <http://example.org/> .
<http://example.org/data> {
    ex:alice ex:name "Alice" ;
             ex:age 30 .
}
"#;
        let keyword = r#"
@prefix ex: <http://example.org/> .
GRAPH <http://example.org/data> {
    ex:alice ex:name "Alice" ;
             ex:age 30 .
}
"#;

        let mut ns_c = test_registry();
        let mut ns_k = test_registry();
        let rc = extract_trig_txn_meta(compact, &mut ns_c).unwrap();
        let rk = extract_trig_txn_meta(keyword, &mut ns_k).unwrap();

        assert_eq!(rc.named_graphs.len(), rk.named_graphs.len());
        assert_eq!(rc.named_graphs.len(), 1);
        assert_eq!(rc.named_graphs[0].iri, rk.named_graphs[0].iri);
        assert_eq!(
            rc.named_graphs[0].triples.len(),
            rk.named_graphs[0].triples.len()
        );
        assert_eq!(rc.named_graphs[0].triples.len(), 2);
    }

    #[test]
    fn test_compact_txn_meta() {
        // The txn-meta graph in compact form must still be extracted.
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
@prefix fluree: <https://ns.flur.ee/db#> .

ex:alice ex:name "Alice" .

<#txn-meta> {
    fluree:commit:this ex:machine "server-01" .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.txn_meta.len(), 1);
        assert_eq!(result.txn_meta[0].predicate_name, "machine");
        assert!(result.turtle.contains("ex:alice ex:name"));
        assert!(!result.turtle.contains('{'));
    }

    #[test]
    fn test_compact_mixed_with_default_and_keyword() {
        // Default triples, a keyword-form graph, and a compact-form graph mixed.
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .

ex:bob ex:name "Bob" .

GRAPH <http://example.org/products> {
    ex:widget ex:price 19 .
}

<urn:orders> {
    ex:order1 ex:qty 5 .
}
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert_eq!(result.named_graphs.len(), 2);
        assert!(result
            .named_graphs
            .iter()
            .any(|g| g.iri == "http://example.org/products"));
        assert!(result.named_graphs.iter().any(|g| g.iri == "urn:orders"));
        assert!(result.turtle.contains("ex:bob ex:name"));
        assert!(!result.turtle.contains('{'));
    }

    #[test]
    fn test_compact_named_graph_comment_before_brace() {
        // Whitespace and comments between the graph label and `{` must not
        // defeat compact-form detection (the lexer skips both).
        let mut ns = test_registry();
        let input = "@prefix ex: <http://example.org/> .\n\
                     <urn:g1>   # the audit graph\n\
                     {\n    ex:a ex:b ex:c .\n}\n";

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.named_graphs.len(), 1);
        assert_eq!(result.named_graphs[0].iri, "urn:g1");
        assert_eq!(result.named_graphs[0].triples.len(), 1);
    }

    #[test]
    fn test_anonymous_default_graph_block_clean_error() {
        // Anonymous `{ ... }` is valid W3C TriG but unsupported here; it must
        // produce a clear error, not a silent mis-parse / misleading downstream
        // Turtle error.
        let mut ns = test_registry();
        let input = "@prefix ex: <http://example.org/> .\n{\n    ex:a ex:b ex:c .\n}\n";

        let err = extract_trig_txn_meta(input, &mut ns)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("anonymous default-graph block"),
            "expected a clear anonymous-block error, got: {err}"
        );
    }

    #[test]
    fn test_blank_node_graph_label_clean_error() {
        // Blank-node graph labels are unsupported; the compact form must report
        // it clearly rather than silently mis-parsing.
        let mut ns = test_registry();
        let input = "@prefix ex: <http://example.org/> .\n_:b {\n    ex:a ex:b ex:c .\n}\n";

        let err = extract_trig_txn_meta(input, &mut ns)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("blank-node graph labels are not supported"),
            "expected a clear blank-node-label error, got: {err}"
        );
    }

    #[test]
    fn test_brace_in_string_literal_roundtrips() {
        // A `{` inside a string literal must not be mistaken for a graph block;
        // the input round-trips unchanged through the TriG parser gate.
        let mut ns = test_registry();
        let input = r#"
@prefix ex: <http://example.org/> .
ex:alice ex:note "value with a { brace" .
"#;

        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert!(result.txn_meta.is_empty());
        assert!(result.named_graphs.is_empty());
        assert!(result.turtle.contains("value with a { brace"));
    }

    // =====================================================================
    // TriG-star — RDF 1.2 asserting forms inside GRAPH blocks
    // =====================================================================

    fn star_block(input: &str) -> NamedGraphBlock {
        let mut ns = test_registry();
        let mut result = extract_trig_txn_meta(input, &mut ns).expect("TriG-star must parse");
        assert_eq!(result.named_graphs.len(), 1);
        result.named_graphs.remove(0)
    }

    fn raw_iri(t: &RawTerm) -> String {
        match t {
            RawTerm::Iri(i) => i.clone(),
            RawTerm::PrefixedName { prefix, local } => format!("{prefix}:{local}"),
        }
    }

    fn triple_strs(block: &NamedGraphBlock) -> Vec<(String, String, String)> {
        block
            .triples
            .iter()
            .flat_map(|t| {
                let s = raw_iri(t.subject.as_ref().unwrap());
                let p = raw_iri(&t.predicate);
                t.objects
                    .iter()
                    .map(move |o| (s.clone(), p.clone(), format!("{o:?}")))
            })
            .collect()
    }

    const STAR_PREFIX: &str = "@prefix ex: <http://example.org/> .\n\
                               @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n";

    #[test]
    fn test_trig_star_every_spelling_yields_one_attachment_and_asserts_the_base() {
        for (label, body) in [
            ("annotation block", "ex:s ex:p ex:o {| ex:q ex:z |} ."),
            ("tilde reifier", "ex:s ex:p ex:o ~ ex:r ."),
            ("tilde + block", "ex:s ex:p ex:o ~ ex:r {| ex:q ex:z |} ."),
            ("reified subject", "<< ex:s ex:p ex:o ~ ex:r >> ex:q ex:z ."),
            ("reified object", "ex:z ex:q << ex:s ex:p ex:o ~ ex:r >> ."),
            (
                "rdf:reifies triple term",
                "ex:r rdf:reifies <<( ex:s ex:p ex:o )>> .",
            ),
        ] {
            let block = star_block(&format!("{STAR_PREFIX}GRAPH ex:g {{ {body} }}\n"));
            assert_eq!(block.reified.len(), 1, "[{label}]");
            let r = &block.reified[0];
            assert_eq!(raw_iri(&r.subject), "ex:s", "[{label}]");
            assert_eq!(raw_iri(&r.predicate), "ex:p", "[{label}]");
            assert!(matches!(&r.object, RawObject::PrefixedName { local, .. } if local == "o"));
            let triples = triple_strs(&block);
            assert!(
                triples.iter().any(|(s, p, _)| s == "ex:s" && p == "ex:p"),
                "[{label}] base triple must be asserted: {triples:?}"
            );
            assert!(
                !triples.iter().any(|(_, p, _)| p == "rdf:reifies"),
                "[{label}] rdf:reifies must not be stored as an ordinary triple: {triples:?}"
            );
        }
    }

    #[test]
    fn test_trig_star_named_reifier_and_body_triples() {
        let block = star_block(&format!(
            "{STAR_PREFIX}GRAPH ex:g {{ ex:s ex:p ex:o ~ ex:r {{| ex:q ex:z ; ex:n 1 |}} . }}\n"
        ));
        assert_eq!(raw_iri(&block.reified[0].reifier), "ex:r");
        let triples = triple_strs(&block);
        assert!(triples.iter().any(|(s, p, _)| s == "ex:r" && p == "ex:q"));
        assert!(triples.iter().any(|(s, p, _)| s == "ex:r" && p == "ex:n"));
        assert_eq!(triples.len(), 3, "{triples:?}");
    }

    #[test]
    fn test_trig_star_anonymous_reifiers_are_fresh_and_outside_user_label_space() {
        let block = star_block(&format!(
            "{STAR_PREFIX}GRAPH ex:g {{\n\
               ex:s ex:p ex:o {{| ex:q ex:z |}} .\n\
               ex:s ex:p ex:o {{| ex:q ex:z2 |}} .\n\
               ex:a ex:b _:r1 .\n\
             }}\n"
        ));
        assert_eq!(block.reified.len(), 2);
        let r1 = raw_iri(&block.reified[0].reifier);
        let r2 = raw_iri(&block.reified[1].reifier);
        assert_ne!(r1, r2, "two occurrences mint two reifiers");
        for r in [&r1, &r2] {
            assert!(
                r.starts_with("_:-"),
                "anonymous reifier label must not be lexable as a user label: {r}"
            );
        }
    }

    #[test]
    fn test_trig_star_reified_subject_carries_reifier_properties() {
        let block = star_block(&format!(
            "{STAR_PREFIX}GRAPH ex:g {{ << ex:s ex:p \"v\"@en >> ex:q ex:z . }}\n"
        ));
        let r = &block.reified[0];
        assert!(
            matches!(&r.object, RawObject::LangString { value, lang } if value == "v" && lang == "en")
        );
        let reifier = raw_iri(&r.reifier);
        let triples = triple_strs(&block);
        assert!(triples.iter().any(|(s, p, _)| *s == reifier && p == "ex:q"));
    }

    #[test]
    fn test_trig_star_nested_reified_triple() {
        // Inner reifier is the subject of the outer base triple.
        let block = star_block(&format!(
            "{STAR_PREFIX}GRAPH ex:g {{ << << ex:s ex:p ex:o >> ex:p2 ex:z >> ex:q ex:o2 . }}\n"
        ));
        assert_eq!(block.reified.len(), 2);
        let inner = raw_iri(&block.reified[0].reifier);
        assert_eq!(raw_iri(&block.reified[1].subject), inner);
    }

    #[test]
    fn test_trig_star_deferred_shapes_reject_cleanly() {
        let mut ns = test_registry();
        for (label, body, needle) in [
            (
                "triple term as value",
                "ex:a ex:q <<( ex:s ex:p ex:o )>> .",
                "triple terms as values",
            ),
            (
                "nested triple term",
                "ex:r rdf:reifies <<( ex:s ex:p <<( ex:x ex:y ex:z )>> )>> .",
                "triple terms as values",
            ),
            (
                "star inside annotation body",
                "ex:s ex:p ex:o {| ex:q ex:z {| ex:n 1 |} |} .",
                "annotation-of-annotation",
            ),
            (
                "annotation on reification",
                "ex:r rdf:reifies <<( ex:s ex:p ex:o )>> {| ex:q ex:z |} .",
                "annotation-of-annotation",
            ),
        ] {
            let err =
                extract_trig_txn_meta(&format!("{STAR_PREFIX}GRAPH ex:g {{ {body} }}\n"), &mut ns)
                    .expect_err(label)
                    .to_string();
            assert!(err.contains(needle), "[{label}] got: {err}");
        }
    }

    #[test]
    fn test_trig_star_rejected_in_txn_meta_graph() {
        let mut ns = test_registry();
        let input = "@prefix ex: <http://example.org/> .\n\
                     @prefix fluree: <https://ns.flur.ee/db#> .\n\
                     GRAPH <#txn-meta> { fluree:commit:this ex:by ex:bob {| ex:q ex:z |} . }\n";
        let err = extract_trig_txn_meta(input, &mut ns)
            .expect_err("txn-meta annotations must be rejected")
            .to_string();
        assert!(err.contains("txn-meta"), "{err}");
    }

    /// Star constructs in the DEFAULT-graph portion of a TriG document are
    /// plain Turtle-star: the gate must pass them through untouched (byte
    /// range → reconstructed Turtle) for the streaming parser to handle.
    #[test]
    fn test_turtle_star_outside_graph_blocks_passes_through() {
        let mut ns = test_registry();
        let input = "@prefix ex: <http://example.org/> .\n\
             ex:a ex:b ex:c {| ex:q ex:z |} .\n\
             GRAPH ex:g { ex:s ex:p ex:o . }\n";
        let result = extract_trig_txn_meta(input, &mut ns).unwrap();
        assert_eq!(result.named_graphs.len(), 1);
        assert!(
            result.turtle.contains("{| ex:q ex:z |}"),
            "default-graph star text must round-trip: {}",
            result.turtle
        );
    }

    #[test]
    fn test_trig_version_directive_does_not_swallow_the_next_directive() {
        for version in ["VERSION \"1.2\"", "@version \"1.2\" ."] {
            let mut ns = test_registry();
            let input = format!(
                "{version}\n{STAR_PREFIX}ex:a ex:b ex:c .\n\
                 GRAPH ex:g {{ ex:s ex:p ex:o {{| ex:q ex:z |}} . }}\n"
            );
            let result = extract_trig_txn_meta(&input, &mut ns)
                .unwrap_or_else(|e| panic!("[{version}] {e}"));
            assert_eq!(result.named_graphs.len(), 1, "[{version}]");
            assert_eq!(result.named_graphs[0].reified.len(), 1, "[{version}]");
            assert!(
                result.turtle.starts_with(version),
                "[{version}] directive must be kept: {}",
                result.turtle
            );
        }

        let mut ns = test_registry();
        let err = extract_trig_txn_meta("VERSION 1.2\nGRAPH <http://g> { }\n", &mut ns)
            .expect_err("unquoted version specifier")
            .to_string();
        assert!(err.contains("version specifier"), "{err}");
    }
}
