//! Turtle parser that emits to GraphSink.
//!
//! Parses Turtle syntax and emits triple events to a GraphSink implementation.
//! Uses span-based token access: most tokens carry no data, and the parser
//! extracts content from the source input via byte offsets.

use std::sync::Arc;

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use fluree_vocab::rdf;
use rustc_hash::FxHashMap;

use crate::error::{Result, TurtleError};
use crate::lex::{StreamingLexer, Token, TokenKind};

/// RDF well-known IRIs (imported from vocab crate)
const RDF_TYPE: &str = rdf::TYPE;
const RDF_FIRST: &str = rdf::FIRST;
const RDF_REST: &str = rdf::REST;
const RDF_NIL: &str = rdf::NIL;

/// Turtle parser state.
pub struct Parser<'a, 'input, S> {
    /// Source input for span extraction.
    input: &'input str,
    /// Streaming lexer — produces tokens on demand (no Vec<Token>).
    lexer: StreamingLexer<'input>,
    /// The current token (most recently lexed).
    current_token: Token,
    sink: &'a mut S,
    /// Cache of fully-expanded IRI string -> TermId (per-parse, in-memory).
    ///
    /// Keyed by `Arc<str>` so lookups can borrow `&str` without allocations.
    /// Uses FxHashMap for faster hashing than SipHash on IRI strings.
    iri_term_cache: FxHashMap<Arc<str>, TermId>,
    /// Cache of prefixed name span text -> TermId.
    ///
    /// Keyed by the raw span text (e.g., `"ex:name"` or `"ex:"`), which uniquely
    /// identifies the expanded IRI for a given prefix mapping. Handles both
    /// PrefixedName and PrefixedNameNs tokens in one cache.
    prefixed_term_cache: FxHashMap<Arc<str>, TermId>,
    /// Cache hit/miss counters (recorded on `turtle_parse_events` span).
    iri_cache_hits: u64,
    iri_cache_misses: u64,
    prefixed_cache_hits: u64,
    prefixed_cache_misses: u64,
    /// Cached common RDF term IDs (computed lazily).
    rdf_type_term: Option<TermId>,
    rdf_nil_term: Option<TermId>,
    rdf_first_term: Option<TermId>,
    rdf_rest_term: Option<TermId>,
    /// Prefix mappings (prefix -> namespace IRI)
    prefixes: FxHashMap<String, String>,
    /// Base IRI for relative IRI resolution
    base: Option<String>,
}

impl<'a, 'input, S: GraphSink> Parser<'a, 'input, S> {
    /// Create a new parser.
    pub fn new(input: &'input str, sink: &'a mut S) -> Result<Self> {
        let mut lexer = StreamingLexer::new(input);
        let current_token = lexer.next_token()?;

        // Pre-size caches based on input length. ~20 bytes per token on
        // average in Turtle, ~3 tokens per unique term → ~60 bytes per
        // unique term. Cap at 2M to avoid reserving hundreds of MB for
        // very large chunks.
        let est_unique = (input.len() / 60).min(2_000_000);
        let mut iri_term_cache = FxHashMap::default();
        iri_term_cache.reserve(est_unique);
        let mut prefixed_term_cache = FxHashMap::default();
        prefixed_term_cache.reserve(est_unique);

        Ok(Self {
            input,
            lexer,
            current_token,
            sink,
            iri_term_cache,
            prefixed_term_cache,
            iri_cache_hits: 0,
            iri_cache_misses: 0,
            prefixed_cache_hits: 0,
            prefixed_cache_misses: 0,
            rdf_type_term: None,
            rdf_nil_term: None,
            rdf_first_term: None,
            rdf_rest_term: None,
            prefixes: FxHashMap::default(),
            base: None,
        })
    }

    /// Parse the entire Turtle document.
    pub fn parse(mut self) -> Result<()> {
        let span = tracing::debug_span!(
            "turtle_parse_events",
            statement_count = tracing::field::Empty,
            iri_cache_hits = tracing::field::Empty,
            iri_cache_misses = tracing::field::Empty,
            prefixed_cache_hits = tracing::field::Empty,
            prefixed_cache_misses = tracing::field::Empty,
            iri_cache_size = tracing::field::Empty,
            prefixed_cache_size = tracing::field::Empty,
        );
        let _g = span.enter();

        let mut statement_count: u64 = 0;
        while !self.is_at_end() {
            self.parse_statement()?;
            statement_count += 1;
        }
        span.record("statement_count", statement_count);
        span.record("iri_cache_hits", self.iri_cache_hits);
        span.record("iri_cache_misses", self.iri_cache_misses);
        span.record("prefixed_cache_hits", self.prefixed_cache_hits);
        span.record("prefixed_cache_misses", self.prefixed_cache_misses);
        span.record("iri_cache_size", self.iri_term_cache.len() as u64);
        span.record("prefixed_cache_size", self.prefixed_term_cache.len() as u64);

        Ok(())
    }

    // =========================================================================
    // Span extraction helpers
    // =========================================================================
    //
    // These return `&'input str` (borrowing from the source input, not from
    // `&self`), so the caller can mutate `self` afterwards without conflict.

    /// Extract IRI content from an Iri token span (strips `<>`).
    #[inline]
    fn iri_content(&self, start: u32, end: u32) -> &'input str {
        &self.input[(start as usize + 1)..(end as usize - 1)]
    }

    /// Extract language tag from a LangTag token span (strips `@`).
    #[inline]
    fn lang_content(&self, start: u32, end: u32) -> &'input str {
        &self.input[(start as usize + 1)..end as usize]
    }

    /// Extract blank node label from a BlankNodeLabel token span (strips `_:`).
    #[inline]
    fn blank_label(&self, start: u32, end: u32) -> &'input str {
        &self.input[(start as usize + 2)..end as usize]
    }

    /// Extract decimal text from a Decimal token span.
    #[inline]
    fn decimal_content(&self, start: u32, end: u32) -> &'input str {
        &self.input[start as usize..end as usize]
    }

    /// Extract prefix from a PrefixedNameNs token span (strips trailing `:`).
    #[inline]
    fn prefix_ns_content(&self, start: u32, end: u32) -> &'input str {
        &self.input[start as usize..(end as usize - 1)]
    }

    /// Extract full span text for a token.
    #[inline]
    fn span_text(&self, start: u32, end: u32) -> &'input str {
        &self.input[start as usize..end as usize]
    }

    // =========================================================================
    // Sink wrappers
    // =========================================================================

    #[inline]
    fn sink_on_prefix(&mut self, prefix: &str, namespace_iri: &str) {
        self.sink.on_prefix(prefix, namespace_iri);
    }

    #[inline]
    fn sink_on_base(&mut self, base_iri: &str) {
        self.sink.on_base(base_iri);
    }

    #[inline]
    fn sink_term_iri(&mut self, iri: &str) -> TermId {
        // Parser-level cache: avoid repeating sink work for the same IRI.
        if let Some(&id) = self.iri_term_cache.get(iri) {
            self.iri_cache_hits += 1;
            return id;
        }
        self.iri_cache_misses += 1;
        let id = self.sink.term_iri(iri);
        self.iri_term_cache.insert(Arc::<str>::from(iri), id);
        id
    }

    #[inline]
    fn sink_term_blank(&mut self, label: Option<&str>) -> TermId {
        self.sink.term_blank(label)
    }

    #[inline]
    fn sink_term_literal(
        &mut self,
        value: &str,
        datatype: Datatype,
        language: Option<&str>,
    ) -> TermId {
        self.sink.term_literal(value, datatype, language)
    }

    #[inline]
    fn sink_term_literal_value(&mut self, value: LiteralValue, datatype: Datatype) -> TermId {
        self.sink.term_literal_value(value, datatype)
    }

    #[inline]
    fn sink_emit_triple(&mut self, subject: TermId, predicate: TermId, object: TermId) {
        self.sink.emit_triple(subject, predicate, object);
    }

    #[inline]
    fn sink_emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) {
        self.sink.emit_list_item(subject, predicate, object, index);
    }

    // =========================================================================
    // Term caching helpers
    // =========================================================================

    /// Resolve an IRI string and look up / register as a term.
    #[inline]
    fn resolve_iri_term(&mut self, iri: &str) -> Result<TermId> {
        if self.base.is_none() && is_absolute_iri(iri) {
            Ok(self.sink_term_iri(iri))
        } else {
            let resolved = self.resolve_iri(iri)?;
            Ok(self.sink_term_iri(&resolved))
        }
    }

    /// Look up a prefixed name (PrefixedName or PrefixedNameNs) by span text.
    ///
    /// The span text (e.g., `"ex:name"` or `"ex:"`) uniquely identifies the
    /// expanded IRI for the current prefix mappings, so it serves as the cache key.
    fn resolve_prefixed_term(&mut self, start: u32, end: u32) -> Result<TermId> {
        let span = self.span_text(start, end);
        if let Some(&id) = self.prefixed_term_cache.get(span) {
            self.prefixed_cache_hits += 1;
            return Ok(id);
        }
        self.prefixed_cache_misses += 1;

        // Split on first ':' to get prefix and local
        let colon_pos = span.find(':').unwrap_or(span.len());
        let prefix = &span[..colon_pos];
        let local = &span[colon_pos + 1..];

        // Handle rare local name escapes (\x sequences)
        let iri = if local.contains('\\') {
            let unescaped = unescape_pn_local(local);
            self.expand_prefixed_name(prefix, &unescaped)?
        } else {
            self.expand_prefixed_name(prefix, local)?
        };
        let id = self.sink_term_iri(&iri);
        // Cache with span text as key — avoids allocation on cache hits
        let span = self.span_text(start, end);
        self.prefixed_term_cache.insert(Arc::from(span), id);
        Ok(id)
    }

    #[inline]
    fn rdf_type(&mut self) -> TermId {
        if let Some(id) = self.rdf_type_term {
            return id;
        }
        let id = self.sink_term_iri(RDF_TYPE);
        self.rdf_type_term = Some(id);
        id
    }

    #[inline]
    fn rdf_nil(&mut self) -> TermId {
        if let Some(id) = self.rdf_nil_term {
            return id;
        }
        let id = self.sink_term_iri(RDF_NIL);
        self.rdf_nil_term = Some(id);
        id
    }

    #[inline]
    fn rdf_first(&mut self) -> TermId {
        if let Some(id) = self.rdf_first_term {
            return id;
        }
        let id = self.sink_term_iri(RDF_FIRST);
        self.rdf_first_term = Some(id);
        id
    }

    #[inline]
    fn rdf_rest(&mut self) -> TermId {
        if let Some(id) = self.rdf_rest_term {
            return id;
        }
        let id = self.sink_term_iri(RDF_REST);
        self.rdf_rest_term = Some(id);
        id
    }

    // =========================================================================
    // Token navigation
    // =========================================================================

    /// Check if we're at the end of input.
    fn is_at_end(&self) -> bool {
        matches!(self.current_token.kind, TokenKind::Eof)
    }

    /// Get the current token.
    #[inline]
    fn current(&self) -> &Token {
        &self.current_token
    }

    /// Advance to the next token.
    #[inline]
    fn advance(&mut self) -> Result<()> {
        if !self.is_at_end() {
            self.current_token = self.lexer.next_token()?;
        }
        Ok(())
    }

    /// Check if the current token matches the expected kind.
    fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(&self.current_token.kind) == std::mem::discriminant(kind)
    }

    /// Consume a token of the expected kind, or return an error.
    fn expect(&mut self, kind: &TokenKind) -> Result<()> {
        if self.check(kind) {
            self.advance()?;
            Ok(())
        } else {
            Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected {:?}, found {:?}", kind, self.current().kind),
            ))
        }
    }

    // =========================================================================
    // Parsing
    // =========================================================================

    /// Parse a single statement (directive or triples).
    fn parse_statement(&mut self) -> Result<()> {
        match self.current().kind {
            TokenKind::KwPrefix | TokenKind::KwSparqlPrefix => self.parse_prefix_directive(),
            TokenKind::KwBase | TokenKind::KwSparqlBase => self.parse_base_directive(),
            TokenKind::Eof => Ok(()),
            _ => self.parse_triples(),
        }
    }

    /// Parse @prefix or PREFIX directive.
    fn parse_prefix_directive(&mut self) -> Result<()> {
        let is_sparql_style = matches!(self.current().kind, TokenKind::KwSparqlPrefix);
        self.advance()?; // consume @prefix or PREFIX

        // Get prefix name (must be PrefixedNameNs)
        let prefix = match self.current().kind {
            TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                self.prefix_ns_content(s, e).to_string()
            }
            _ => {
                return Err(TurtleError::parse(
                    self.current().start as usize,
                    "expected prefix namespace",
                ))
            }
        };
        self.advance()?;

        // Get namespace IRI
        let namespace = match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.resolve_iri(iri)?
            }
            TokenKind::IriEscaped(iri) => self.resolve_iri(&iri)?,
            _ => {
                return Err(TurtleError::parse(
                    self.current().start as usize,
                    "expected IRI for prefix namespace",
                ))
            }
        };
        self.advance()?;

        // Register prefix
        self.sink_on_prefix(&prefix, &namespace);
        self.prefixes.insert(prefix, namespace);

        // Consume trailing dot (required for @prefix, not for PREFIX)
        if !is_sparql_style {
            self.expect(&TokenKind::Dot)?;
        }

        Ok(())
    }

    /// Parse @base or BASE directive.
    fn parse_base_directive(&mut self) -> Result<()> {
        let is_sparql_style = matches!(self.current().kind, TokenKind::KwSparqlBase);
        self.advance()?; // consume @base or BASE

        // Get base IRI
        let base_iri = match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                self.iri_content(s, e).to_string()
            }
            TokenKind::IriEscaped(iri) => iri.to_string(),
            _ => {
                return Err(TurtleError::parse(
                    self.current().start as usize,
                    "expected IRI for base",
                ))
            }
        };
        self.advance()?;

        // Set base
        self.sink_on_base(&base_iri);
        self.base = Some(base_iri);

        // Consume trailing dot (required for @base, not for BASE)
        if !is_sparql_style {
            self.expect(&TokenKind::Dot)?;
        }

        Ok(())
    }

    /// Parse a triple statement.
    fn parse_triples(&mut self) -> Result<()> {
        let subject = self.parse_subject()?;
        self.parse_predicate_object_list(subject)?;
        self.expect(&TokenKind::Dot)?;
        Ok(())
    }

    /// Parse a subject term.
    fn parse_subject(&mut self) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.advance()?;
                self.resolve_iri_term(iri)
            }
            TokenKind::IriEscaped(iri) => {
                self.advance()?;
                self.resolve_iri_term(&iri)
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                self.advance()?;
                self.resolve_prefixed_term(s, e)
            }
            TokenKind::BlankNodeLabel => {
                let label = self.blank_label(self.current().start, self.current().end);
                self.advance()?;
                Ok(self.sink_term_blank(Some(label)))
            }
            TokenKind::Anon => {
                self.advance()?;
                Ok(self.sink_term_blank(None))
            }
            TokenKind::LBracket => self.parse_blank_node_property_list(),
            TokenKind::LParen => self.parse_collection(),
            TokenKind::Nil => {
                self.advance()?;
                Ok(self.rdf_nil())
            }
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected subject, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a predicate-object list.
    fn parse_predicate_object_list(&mut self, subject: TermId) -> Result<()> {
        loop {
            let predicate = self.parse_predicate()?;
            self.parse_object_list(subject, predicate)?;

            if matches!(self.current().kind, TokenKind::Semicolon) {
                self.advance()?;
                if matches!(
                    self.current().kind,
                    TokenKind::Dot | TokenKind::RBracket | TokenKind::Eof
                ) {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Parse a predicate.
    fn parse_predicate(&mut self) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.advance()?;
                self.resolve_iri_term(iri)
            }
            TokenKind::IriEscaped(iri) => {
                self.advance()?;
                self.resolve_iri_term(&iri)
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                self.advance()?;
                self.resolve_prefixed_term(s, e)
            }
            TokenKind::KwA => {
                self.advance()?;
                Ok(self.rdf_type())
            }
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected predicate, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse an object list (comma-separated objects).
    ///
    /// Collections in object position are emitted as indexed list items via
    /// `emit_list_item()` instead of rdf:first/rdf:rest linked lists.
    fn parse_object_list(&mut self, subject: TermId, predicate: TermId) -> Result<()> {
        loop {
            match self.current().kind {
                TokenKind::LParen => {
                    self.parse_collection_as_list(subject, predicate)?;
                }
                TokenKind::Nil => {
                    self.advance()?;
                }
                _ => {
                    let object = self.parse_object()?;
                    self.sink_emit_triple(subject, predicate, object);
                }
            }

            if matches!(self.current().kind, TokenKind::Comma) {
                self.advance()?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Parse a collection in object position as indexed list items.
    fn parse_collection_as_list(&mut self, subject: TermId, predicate: TermId) -> Result<()> {
        self.expect(&TokenKind::LParen)?;
        let mut index: i32 = 0;
        while !matches!(self.current().kind, TokenKind::RParen) {
            let item = self.parse_object()?;
            self.sink_emit_list_item(subject, predicate, item, index);
            index += 1;
        }
        self.expect(&TokenKind::RParen)?;
        Ok(())
    }

    /// Parse an object term.
    fn parse_object(&mut self) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.advance()?;
                self.resolve_iri_term(iri)
            }
            TokenKind::IriEscaped(iri) => {
                self.advance()?;
                self.resolve_iri_term(&iri)
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                self.advance()?;
                self.resolve_prefixed_term(s, e)
            }
            TokenKind::BlankNodeLabel => {
                let label = self.blank_label(self.current().start, self.current().end);
                self.advance()?;
                Ok(self.sink_term_blank(Some(label)))
            }
            TokenKind::Anon => {
                self.advance()?;
                Ok(self.sink_term_blank(None))
            }
            TokenKind::LBracket => self.parse_blank_node_property_list(),
            TokenKind::LParen => self.parse_collection(),
            TokenKind::Nil => {
                self.advance()?;
                Ok(self.rdf_nil())
            }
            TokenKind::String | TokenKind::LongString | TokenKind::StringEscaped(_) => {
                self.parse_literal()
            }
            TokenKind::Integer(_) | TokenKind::Decimal | TokenKind::Double(_) => {
                self.parse_literal()
            }
            TokenKind::KwTrue | TokenKind::KwFalse => self.parse_literal(),
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected object, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a literal (string with optional language tag or datatype).
    fn parse_literal(&mut self) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::String => {
                let s = self.current().start;
                let e = self.current().end;
                self.advance()?;
                self.parse_string_suffix(s, e, 1)
            }
            TokenKind::LongString => {
                let s = self.current().start;
                let e = self.current().end;
                self.advance()?;
                self.parse_string_suffix(s, e, 3)
            }
            TokenKind::StringEscaped(value) => {
                self.advance()?;
                self.parse_string_suffix_escaped(&value)
            }
            TokenKind::Integer(n) => {
                self.advance()?;
                Ok(self.sink_term_literal_value(LiteralValue::Integer(n), Datatype::xsd_integer()))
            }
            TokenKind::Decimal => {
                let s = self.current().start;
                let e = self.current().end;
                let text = self.decimal_content(s, e);
                self.advance()?;
                Ok(self.sink_term_literal(text, Datatype::xsd_decimal(), None))
            }
            TokenKind::Double(n) => {
                self.advance()?;
                Ok(self.sink_term_literal_value(LiteralValue::Double(n), Datatype::xsd_double()))
            }
            TokenKind::KwTrue => {
                self.advance()?;
                Ok(self
                    .sink_term_literal_value(LiteralValue::Boolean(true), Datatype::xsd_boolean()))
            }
            TokenKind::KwFalse => {
                self.advance()?;
                Ok(self
                    .sink_term_literal_value(LiteralValue::Boolean(false), Datatype::xsd_boolean()))
            }
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected literal, found {:?}", self.current().kind),
            )),
        }
    }

    /// Handle the optional `@lang` or `^^datatype` suffix after a span-based string literal.
    ///
    /// `quote_len` is 1 for short strings, 3 for long strings.
    fn parse_string_suffix(
        &mut self,
        str_start: u32,
        str_end: u32,
        quote_len: usize,
    ) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::LangTag => {
                let ls = self.current().start;
                let le = self.current().end;
                self.advance()?;
                let value =
                    &self.input[(str_start as usize + quote_len)..(str_end as usize - quote_len)];
                let lang = self.lang_content(ls, le);
                Ok(self.sink_term_literal(value, Datatype::rdf_lang_string(), Some(lang)))
            }
            TokenKind::DoubleCaret => {
                self.advance()?;
                let datatype_iri = self.parse_datatype_iri()?;
                let value =
                    &self.input[(str_start as usize + quote_len)..(str_end as usize - quote_len)];
                let datatype = Datatype::from_iri(&datatype_iri);
                Ok(self.sink_term_literal(value, datatype, None))
            }
            _ => {
                let value =
                    &self.input[(str_start as usize + quote_len)..(str_end as usize - quote_len)];
                Ok(self.sink_term_literal(value, Datatype::xsd_string(), None))
            }
        }
    }

    /// Handle the optional `@lang` or `^^datatype` suffix after an escaped string literal.
    fn parse_string_suffix_escaped(&mut self, value: &str) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::LangTag => {
                let ls = self.current().start;
                let le = self.current().end;
                self.advance()?;
                let lang = self.lang_content(ls, le);
                Ok(self.sink_term_literal(value, Datatype::rdf_lang_string(), Some(lang)))
            }
            TokenKind::DoubleCaret => {
                self.advance()?;
                let datatype_iri = self.parse_datatype_iri()?;
                let datatype = Datatype::from_iri(&datatype_iri);
                Ok(self.sink_term_literal(value, datatype, None))
            }
            _ => Ok(self.sink_term_literal(value, Datatype::xsd_string(), None)),
        }
    }

    /// Parse a datatype IRI after ^^.
    fn parse_datatype_iri(&mut self) -> Result<String> {
        match self.current().kind.clone() {
            TokenKind::Iri => {
                let s = self.current().start;
                let e = self.current().end;
                let iri = self.iri_content(s, e);
                self.advance()?;
                if self.base.is_none() && is_absolute_iri(iri) {
                    Ok(iri.to_string())
                } else {
                    self.resolve_iri(iri)
                }
            }
            TokenKind::IriEscaped(iri) => {
                self.advance()?;
                if self.base.is_none() && is_absolute_iri(&iri) {
                    Ok(iri.to_string())
                } else {
                    self.resolve_iri(&iri)
                }
            }
            TokenKind::PrefixedName | TokenKind::PrefixedNameNs => {
                let s = self.current().start;
                let e = self.current().end;
                let span = self.span_text(s, e);
                let colon_pos = span.find(':').unwrap_or(span.len());
                let prefix = &span[..colon_pos];
                let local = &span[colon_pos + 1..];
                self.advance()?;
                if local.contains('\\') {
                    let unescaped = unescape_pn_local(local);
                    self.expand_prefixed_name(prefix, &unescaped)
                } else {
                    self.expand_prefixed_name(prefix, local)
                }
            }
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected datatype IRI, found {:?}", self.current().kind),
            )),
        }
    }

    /// Parse a blank node property list: `[ predicate object ; ... ]`
    fn parse_blank_node_property_list(&mut self) -> Result<TermId> {
        self.expect(&TokenKind::LBracket)?;

        let bnode = self.sink_term_blank(None);

        if !matches!(self.current().kind, TokenKind::RBracket) {
            self.parse_predicate_object_list(bnode)?;
        }

        self.expect(&TokenKind::RBracket)?;

        Ok(bnode)
    }

    /// Parse a collection (RDF list): `( item1 item2 ... )`
    fn parse_collection(&mut self) -> Result<TermId> {
        self.expect(&TokenKind::LParen)?;

        if matches!(self.current().kind, TokenKind::RParen) {
            self.advance()?;
            return Ok(self.rdf_nil());
        }

        let rdf_first = self.rdf_first();
        let rdf_rest = self.rdf_rest();
        let rdf_nil = self.rdf_nil();

        let first_node = self.sink_term_blank(None);
        let mut current_node = first_node;

        loop {
            let item = self.parse_object()?;
            self.sink_emit_triple(current_node, rdf_first, item);

            if matches!(self.current().kind, TokenKind::RParen) {
                self.sink_emit_triple(current_node, rdf_rest, rdf_nil);
                break;
            }
            let next_node = self.sink_term_blank(None);
            self.sink_emit_triple(current_node, rdf_rest, next_node);
            current_node = next_node;
        }

        self.expect(&TokenKind::RParen)?;

        Ok(first_node)
    }

    /// Resolve a potentially relative IRI against the base (RFC3986).
    fn resolve_iri(&self, reference: &str) -> Result<String> {
        if reference.is_empty() {
            return match &self.base {
                Some(base) => Ok(base.clone()),
                None => Err(TurtleError::IriResolution(
                    "empty IRI reference without base".to_string(),
                )),
            };
        }

        if let Some(colon_pos) = reference.find(':') {
            let potential_scheme = &reference[..colon_pos];
            if !potential_scheme.is_empty()
                && potential_scheme
                    .chars()
                    .next()
                    .unwrap()
                    .is_ascii_alphabetic()
                && potential_scheme
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
            {
                return Ok(reference.to_string());
            }
        }

        let base = match &self.base {
            Some(b) => b,
            None => {
                return Err(TurtleError::IriResolution(format!(
                    "relative IRI '{reference}' without base"
                )));
            }
        };

        let (base_scheme, base_authority, base_path, _base_query) = parse_iri_components(base);

        let (scheme, authority, path, query) = if let Some(rest) = reference.strip_prefix("//") {
            let (ref_authority, ref_path, ref_query) = parse_hier_part(rest);
            (
                base_scheme.to_string(),
                Some(ref_authority),
                remove_dot_segments(&ref_path),
                ref_query,
            )
        } else if reference.starts_with('/') {
            let (ref_path, ref_query) = split_path_query(reference);
            (
                base_scheme.to_string(),
                base_authority.map(std::string::ToString::to_string),
                remove_dot_segments(ref_path),
                ref_query.map(std::string::ToString::to_string),
            )
        } else if let Some(query_rest) = reference.strip_prefix('?') {
            (
                base_scheme.to_string(),
                base_authority.map(std::string::ToString::to_string),
                base_path.to_string(),
                Some(query_rest.to_string()),
            )
        } else if reference.starts_with('#') {
            (
                base_scheme.to_string(),
                base_authority.map(std::string::ToString::to_string),
                base_path.to_string(),
                None,
            )
        } else {
            let (ref_path, ref_query) = split_path_query(reference);
            let merged = if base_authority.is_some() && base_path.is_empty() {
                format!("/{ref_path}")
            } else {
                let base_dir = match base_path.rfind('/') {
                    Some(pos) => &base_path[..=pos],
                    None => "",
                };
                format!("{base_dir}{ref_path}")
            };
            (
                base_scheme.to_string(),
                base_authority.map(std::string::ToString::to_string),
                remove_dot_segments(&merged),
                ref_query.map(std::string::ToString::to_string),
            )
        };

        let mut result = scheme;
        result.push(':');
        if let Some(auth) = authority {
            result.push_str("//");
            result.push_str(&auth);
        }
        result.push_str(&path);
        if let Some(q) = query {
            result.push('?');
            result.push_str(&q);
        }

        Ok(result)
    }

    /// Expand a prefixed name to a full IRI.
    fn expand_prefixed_name(&self, prefix: &str, local: &str) -> Result<String> {
        if let Some(namespace) = self.prefixes.get(prefix) {
            Ok(format!("{namespace}{local}"))
        } else {
            Err(TurtleError::UndefinedPrefix(prefix.to_string()))
        }
    }
}

/// Unescape local name escape sequences (`\x` → `x`).
///
/// Only called when `\` is detected in the local part (extremely rare).
fn unescape_pn_local(local: &str) -> String {
    let mut result = String::with_capacity(local.len());
    let mut chars = local.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(escaped) = chars.next() {
                result.push(escaped);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[inline]
fn is_absolute_iri(reference: &str) -> bool {
    if let Some(colon_pos) = reference.find(':') {
        let potential_scheme = &reference[..colon_pos];
        !potential_scheme.is_empty()
            && potential_scheme
                .chars()
                .next()
                .unwrap()
                .is_ascii_alphabetic()
            && potential_scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
    } else {
        false
    }
}

// =============================================================================
// RFC3986 IRI Resolution Helpers
// =============================================================================

fn parse_iri_components(iri: &str) -> (&str, Option<&str>, &str, Option<&str>) {
    let (scheme, rest) = match iri.find(':') {
        Some(pos) => (&iri[..pos], &iri[pos + 1..]),
        None => return ("", None, iri, None),
    };

    let (authority, path_query) = if let Some(after_slashes) = rest.strip_prefix("//") {
        let auth_end = after_slashes
            .find(['/', '?', '#'])
            .unwrap_or(after_slashes.len());
        (Some(&after_slashes[..auth_end]), &after_slashes[auth_end..])
    } else {
        (None, rest)
    };

    let (path, query) = split_path_query(path_query);

    (scheme, authority, path, query)
}

fn parse_hier_part(s: &str) -> (String, String, Option<String>) {
    let auth_end = s.find(['/', '?', '#']).unwrap_or(s.len());
    let authority = s[..auth_end].to_string();
    let rest = &s[auth_end..];

    let (path, query) = split_path_query(rest);
    (
        authority,
        path.to_string(),
        query.map(std::string::ToString::to_string),
    )
}

fn split_path_query(s: &str) -> (&str, Option<&str>) {
    let s = match s.find('#') {
        Some(pos) => &s[..pos],
        None => s,
    };

    match s.find('?') {
        Some(pos) => (&s[..pos], Some(&s[pos + 1..])),
        None => (s, None),
    }
}

fn remove_dot_segments(path: &str) -> String {
    let mut output: Vec<&str> = Vec::new();

    for segment in path.split('/') {
        match segment {
            "." => {}
            ".." => {
                output.pop();
            }
            s => {
                output.push(s);
            }
        }
    }

    let result = output.join("/");
    if path.starts_with('/') && !result.starts_with('/') {
        format!("/{result}")
    } else {
        result
    }
}

/// Parse a Turtle document into GraphSink events.
pub fn parse<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Parser::new(input, sink)?.parse()
}

/// Parse Turtle input with a pre-seeded prefix map and optional base IRI.
///
/// This is useful when the caller has already extracted `@prefix` / `@base`
/// directives (e.g., from a file header) and wants to parse subsequent Turtle
/// fragments without re-prepending/re-parsing the directive text.
///
/// Notes:
/// - The provided `prefixes` and `base` affect **prefix expansion and IRI resolution**
///   inside the parser.
/// - This function does **not** emit `on_prefix` / `on_base` events to the sink.
///   Callers that need those events (e.g., to pre-register namespaces) should
///   do so explicitly.
pub fn parse_with_prefixes_base<S: GraphSink>(
    input: &str,
    sink: &mut S,
    prefixes: &[(String, String)],
    base: Option<&str>,
) -> Result<()> {
    let mut parser = Parser::new(input, sink)?;
    if let Some(base) = base {
        parser.base = Some(base.to_string());
    }
    if !prefixes.is_empty() {
        parser.prefixes.reserve(prefixes.len());
        for (prefix, namespace) in prefixes {
            parser.prefixes.insert(prefix.clone(), namespace.clone());
        }
    }
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_graph_ir::{Graph, GraphCollectorSink, Term};

    fn parse_to_graph(input: &str) -> Result<Graph> {
        let mut sink = GraphCollectorSink::new();
        parse(input, &mut sink)?;
        Ok(sink.finish())
    }

    #[test]
    fn test_simple_triple() {
        let input = r#"<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" ."#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(
            matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/name")
        );
    }

    #[test]
    fn test_prefix_directive() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            @prefix foaf: <http://xmlns.com/foaf/0.1/> .
            ex:alice foaf:name "Alice" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/alice"));
        assert!(
            matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == "http://xmlns.com/foaf/0.1/name")
        );
    }

    #[test]
    fn test_a_keyword() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice a ex:Person .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.p, Term::Iri(iri) if iri.as_ref() == RDF_TYPE));
    }

    #[test]
    fn test_semicolon_syntax() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice" ;
                     ex:age 30 .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_comma_syntax() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice ex:knows ex:bob, ex:charlie .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_blank_node() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            _:b1 ex:name "Bob" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::BlankNode(_)));
    }

    #[test]
    fn test_blank_node_property_list() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:knows [ ex:name "Bob" ] .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
    }

    #[test]
    fn test_typed_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            ex:alice ex:birthdate "2000-01-01"^^xsd:date .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { datatype, .. } = &triple.o {
            assert_eq!(datatype.as_iri(), "http://www.w3.org/2001/XMLSchema#date");
        } else {
            panic!("Expected literal");
        }
    }

    #[test]
    fn test_language_tagged_literal() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:alice ex:name "Alice"@en .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal { language, .. } = &triple.o {
            assert_eq!(language.as_deref(), Some("en"));
        } else {
            panic!("Expected literal");
        }
    }

    #[test]
    fn test_integer_literal() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice ex:age 30 .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal {
            value: LiteralValue::Integer(n),
            ..
        } = &triple.o
        {
            assert_eq!(*n, 30);
        } else {
            panic!("Expected integer literal");
        }
    }

    #[test]
    fn test_boolean_literal() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice ex:active true .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        if let Term::Literal {
            value: LiteralValue::Boolean(b),
            ..
        } = &triple.o
        {
            assert!(*b);
        } else {
            panic!("Expected boolean literal");
        }
    }

    #[test]
    fn test_collection() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice ex:friends ( ex:bob ex:charlie ) .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);
        for triple in graph.iter() {
            assert!(triple.is_list_element());
        }
    }

    #[test]
    fn test_empty_collection() {
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:alice ex:friends () .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 0);
    }

    #[test]
    fn test_sparql_prefix_syntax() {
        let input = r#"
            PREFIX ex: <http://example.org/>
            ex:alice ex:name "Alice" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
    }

    #[test]
    fn test_base_iri_resolution() {
        let input = r#"
            @base <http://example.org/path/> .
            <alice> <name> "Alice" .
            <../bob> <name> "Bob" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 2);

        let triples: Vec<_> = graph.iter().collect();

        let alice_triple = triples.iter().find(|t| {
            matches!(&t.o, Term::Literal { value, .. } if matches!(value, fluree_graph_ir::LiteralValue::String(s) if s.as_ref() == "Alice"))
        }).unwrap();
        assert!(
            matches!(&alice_triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/path/alice")
        );
        assert!(
            matches!(&alice_triple.p, Term::Iri(iri) if iri.as_ref() == "http://example.org/path/name")
        );

        let bob_triple = triples.iter().find(|t| {
            matches!(&t.o, Term::Literal { value, .. } if matches!(value, fluree_graph_ir::LiteralValue::String(s) if s.as_ref() == "Bob"))
        }).unwrap();
        assert!(
            matches!(&bob_triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/bob")
        );
    }

    #[test]
    fn test_base_iri_absolute_path() {
        let input = r#"
            @base <http://example.org/a/b/c> .
            </d/e> <name> "test" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/d/e"));
    }

    #[test]
    fn test_empty_iri_resolves_to_base() {
        let input = r#"
            @base <http://example.org/doc> .
            <> <name> "The Document" .
        "#;
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        assert!(matches!(&triple.s, Term::Iri(iri) if iri.as_ref() == "http://example.org/doc"));
    }
}
