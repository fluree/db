//! Turtle parser that emits to GraphSink.
//!
//! Parses Turtle syntax and emits triple events to a GraphSink implementation.
//! Uses span-based token access: most tokens carry no data, and the parser
//! extracts content from the source input via byte offsets.

use std::sync::Arc;

use fluree_graph_ir::{Datatype, GraphSink, LiteralValue, TermId};
use fluree_vocab::iri::is_absolute_iri;
use fluree_vocab::rdf;
use rustc_hash::FxHashMap;

use crate::error::{Result, TurtleError};
use crate::lex::{StreamingLexer, Token, TokenKind};
use crate::options::{CollectionStyle, NumericStyle, ParserOptions};

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
    /// Nesting depth of `{| … |}` annotation bodies currently being parsed.
    /// Non-zero means we are inside an annotation body, where further star
    /// constructs (annotation-of-annotation, reified triples) are the
    /// deferred v1 shapes — mirrors the JSON-LD `@annotation` lowering,
    /// which rejects the same nesting.
    annotation_depth: u32,
    /// Combined nesting depth of the recursive constructs (property lists,
    /// collections, reified triples), bounded by
    /// [`crate::error::MAX_NESTING_DEPTH`] so adversarial nesting errors
    /// instead of overflowing the stack.
    nesting_depth: u32,
    /// Conformance knobs; [`ParserOptions::default`] is the ingest shape.
    options: ParserOptions,
    /// Emissions so far. Compared against its value at the statement's start
    /// to decide whether a failed statement needs
    /// [`GraphSink::abort_statement`] — a statement that failed before
    /// emitting has nothing to roll back.
    emit_count: u64,
}

impl<'a, 'input, S: GraphSink> Parser<'a, 'input, S> {
    /// Create a new parser with the default (ingest) options.
    pub fn new(input: &'input str, sink: &'a mut S) -> Result<Self> {
        Self::with_options(input, sink, ParserOptions::default())
    }

    /// Create a new parser with explicit conformance options.
    pub fn with_options(
        input: &'input str,
        sink: &'a mut S,
        options: ParserOptions,
    ) -> Result<Self> {
        crate::error::check_input_len(input.len())?;
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
            annotation_depth: 0,
            nesting_depth: 0,
            options,
            emit_count: 0,
        })
    }

    /// Run `f` one nesting level deeper, erroring past
    /// [`crate::error::MAX_NESTING_DEPTH`]. Owns both sides of the depth
    /// bookkeeping so call sites cannot increment without the matching
    /// decrement.
    fn with_nesting<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if self.nesting_depth >= crate::error::MAX_NESTING_DEPTH {
            return Err(TurtleError::parse(
                self.current().start as usize,
                format!(
                    "nesting of property lists, collections, and reified triples \
                     exceeds the maximum depth of {}",
                    crate::error::MAX_NESTING_DEPTH
                ),
            ));
        }
        self.nesting_depth += 1;
        let result = f(self);
        self.nesting_depth -= 1;
        result
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
            let emitted_before = self.emit_count;
            match self.parse_statement() {
                // Committed at its own terminator; anything else (the
                // SPARQL-style directives) commits here.
                Ok(committed) => {
                    if !committed {
                        self.sink.end_statement();
                    }
                    statement_count += 1;
                }
                Err(e) => {
                    // The parser emits during descent, so a statement can
                    // fail having already pushed triples. Tell the sink to
                    // drop them, so "a rejected statement contributes
                    // nothing" holds at the sink and not just in the error
                    // return. Nothing emitted means nothing to roll back.
                    //
                    // A statement that already committed and then failed on
                    // the lookahead advance lands here too; its rollback is a
                    // no-op because the commit moved the sink's mark past it.
                    if self.emit_count > emitted_before {
                        self.sink.abort_statement();
                    }
                    return Err(e);
                }
            }
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

    /// Emit a triple, mapping a sink refusal into a parse-level error so the
    /// caller stops immediately (broken-pipe / early-termination semantics).
    #[inline]
    fn sink_emit_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<()> {
        self.sink.emit_triple(subject, predicate, object)?;
        self.emit_count += 1;
        Ok(())
    }

    #[inline]
    fn sink_emit_list_item(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        index: i32,
    ) -> Result<()> {
        self.sink
            .emit_list_item(subject, predicate, object, index)?;
        self.emit_count += 1;
        Ok(())
    }

    #[inline]
    fn sink_emit_reified_triple(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
        reifier: TermId,
    ) -> Result<()> {
        self.sink
            .emit_reified_triple(subject, predicate, object, reifier)?;
        self.emit_count += 1;
        Ok(())
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

    /// Consume the `.` terminating a statement, committing the statement to
    /// the sink BEFORE advancing.
    ///
    /// The commit has to happen before the advance because the parser keeps
    /// one token of lookahead: advancing past the `.` lexes the first token
    /// of the NEXT statement, and a lexical error there would otherwise be
    /// reported while this statement is still in flight — rolling back a
    /// statement that was in fact complete and valid.
    fn end_statement_at_dot(&mut self) -> Result<()> {
        if !self.check(&TokenKind::Dot) {
            return Err(TurtleError::parse(
                self.current().start as usize,
                format!("expected Dot, found {:?}", self.current().kind),
            ));
        }
        self.sink.end_statement();
        self.advance()
    }

    /// Parse a single statement (directive or triples).
    ///
    /// Returns whether the statement already committed itself to the sink via
    /// [`Self::end_statement_at_dot`]; the SPARQL-style `PREFIX`/`BASE` forms
    /// have no terminator, so the caller commits those.
    fn parse_statement(&mut self) -> Result<bool> {
        match self.current().kind {
            TokenKind::KwPrefix | TokenKind::KwSparqlPrefix => self.parse_prefix_directive(),
            TokenKind::KwBase | TokenKind::KwSparqlBase => self.parse_base_directive(),
            TokenKind::Eof => Ok(false),
            _ => self.parse_triples().map(|()| true),
        }
    }

    /// Parse @prefix or PREFIX directive.
    fn parse_prefix_directive(&mut self) -> Result<bool> {
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
            self.end_statement_at_dot()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Parse @base or BASE directive.
    fn parse_base_directive(&mut self) -> Result<bool> {
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
            self.end_statement_at_dot()?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Parse a triple statement.
    fn parse_triples(&mut self) -> Result<()> {
        let bnode_list_subject = matches!(self.current().kind, TokenKind::LBracket);
        let subject = self.parse_subject()?;
        // Turtle grammar: `blankNodePropertyList predicateObjectList? '.'` —
        // the predicate-object list is optional when the subject is a
        // `[...]` property list (its triples were emitted inside the list).
        if !(bnode_list_subject && matches!(self.current().kind, TokenKind::Dot)) {
            self.parse_predicate_object_list(subject)?;
        }
        self.end_statement_at_dot()
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
            TokenKind::ReifiedTripleStart => self.parse_reified_triple(),
            TokenKind::TripleTermStart => Err(self.triple_term_deferred_error()),
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
                    TokenKind::Dot
                        | TokenKind::RBracket
                        | TokenKind::Eof
                        | TokenKind::AnnotationClose
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
    /// Under [`CollectionStyle::IndexedItems`] (the default), collections in
    /// object position are emitted as indexed list items via
    /// `emit_list_item()` instead of rdf:first/rdf:rest linked lists, and an
    /// object-position `()` emits nothing. Under [`CollectionStyle::Spine`]
    /// both fall through to the ordinary object path, which already builds a
    /// spine for `( … )` and resolves `()` to `rdf:nil` — so the two
    /// positions share one code path and one grammar.
    ///
    /// Each object may carry an RDF 1.2 annotation tail (`~ reifier` and/or
    /// `{| … |}` blocks) — see [`Self::parse_annotation_tail`].
    fn parse_object_list(&mut self, subject: TermId, predicate: TermId) -> Result<()> {
        let indexed_lists = self.options.collections == CollectionStyle::IndexedItems;
        loop {
            match self.current().kind {
                TokenKind::LParen if indexed_lists => {
                    self.parse_collection_as_list(subject, predicate)?;
                    self.reject_annotation_on_collection()?;
                }
                TokenKind::Nil if indexed_lists => {
                    self.advance()?;
                    self.reject_annotation_on_collection()?;
                }
                _ => {
                    let object = self.parse_object()?;
                    self.sink_emit_triple(subject, predicate, object)?;
                    if matches!(
                        self.current().kind,
                        TokenKind::Tilde | TokenKind::AnnotationOpen
                    ) {
                        self.parse_annotation_tail(subject, predicate, object)?;
                    }
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
        self.with_nesting(|p| {
            p.expect(&TokenKind::LParen)?;
            let mut index: i32 = 0;
            while !matches!(p.current().kind, TokenKind::RParen) {
                let item = p.parse_object()?;
                p.sink_emit_list_item(subject, predicate, item, index)?;
                index += 1;
            }
            p.expect(&TokenKind::RParen)?;
            Ok(())
        })
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
            TokenKind::Integer(_)
            | TokenKind::IntegerOverflow
            | TokenKind::Decimal
            | TokenKind::Double(_) => self.parse_literal(),
            TokenKind::KwTrue | TokenKind::KwFalse => self.parse_literal(),
            TokenKind::ReifiedTripleStart => self.parse_reified_triple(),
            TokenKind::TripleTermStart => Err(self.triple_term_deferred_error()),
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
            // Branch on the option BEFORE touching the span: the default arm
            // must stay exactly the work it did before options existed, since
            // this is the ingest hot path.
            TokenKind::Integer(n) => match self.options.numerics {
                NumericStyle::Canonicalize => {
                    self.advance()?;
                    Ok(self
                        .sink_term_literal_value(LiteralValue::Integer(n), Datatype::xsd_integer()))
                }
                // PreserveLexical routes through the typed-string lane, the
                // same one `IntegerOverflow` and `Decimal` already use, so
                // `+1` / `01` / `1` stay distinguishable instead of all
                // collapsing to `Integer(1)`.
                NumericStyle::PreserveLexical => {
                    let s = self.current().start;
                    let e = self.current().end;
                    let text = self.decimal_content(s, e);
                    self.advance()?;
                    Ok(self.sink_term_literal(text, Datatype::xsd_integer(), None))
                }
            },
            TokenKind::IntegerOverflow => {
                // Beyond i64: keep the lexical so downstream promotes to BigInt.
                let s = self.current().start;
                let e = self.current().end;
                let text = self.decimal_content(s, e);
                self.advance()?;
                Ok(self.sink_term_literal(text, Datatype::xsd_integer(), None))
            }
            TokenKind::Decimal => {
                let s = self.current().start;
                let e = self.current().end;
                let text = self.decimal_content(s, e);
                self.advance()?;
                Ok(self.sink_term_literal(text, Datatype::xsd_decimal(), None))
            }
            TokenKind::Double(n) => match self.options.numerics {
                NumericStyle::Canonicalize => {
                    self.advance()?;
                    Ok(self
                        .sink_term_literal_value(LiteralValue::Double(n), Datatype::xsd_double()))
                }
                // `1e0`, `1.0e0`, and `1.0E0` are three spellings of one
                // value; canonicalizing keeps only the value.
                NumericStyle::PreserveLexical => {
                    let s = self.current().start;
                    let e = self.current().end;
                    let text = self.decimal_content(s, e);
                    self.advance()?;
                    Ok(self.sink_term_literal(text, Datatype::xsd_double(), None))
                }
            },
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
        self.with_nesting(|p| {
            p.expect(&TokenKind::LBracket)?;

            let bnode = p.sink_term_blank(None);

            if !matches!(p.current().kind, TokenKind::RBracket) {
                p.parse_predicate_object_list(bnode)?;
            }

            p.expect(&TokenKind::RBracket)?;

            Ok(bnode)
        })
    }

    /// Parse a collection (RDF list): `( item1 item2 ... )`
    fn parse_collection(&mut self) -> Result<TermId> {
        self.with_nesting(|p| {
            p.expect(&TokenKind::LParen)?;

            if matches!(p.current().kind, TokenKind::RParen) {
                p.advance()?;
                return Ok(p.rdf_nil());
            }

            let rdf_first = p.rdf_first();
            let rdf_rest = p.rdf_rest();
            let rdf_nil = p.rdf_nil();

            let first_node = p.sink_term_blank(None);
            let mut current_node = first_node;

            loop {
                let item = p.parse_object()?;
                p.sink_emit_triple(current_node, rdf_first, item)?;

                if matches!(p.current().kind, TokenKind::RParen) {
                    p.sink_emit_triple(current_node, rdf_rest, rdf_nil)?;
                    break;
                }
                let next_node = p.sink_term_blank(None);
                p.sink_emit_triple(current_node, rdf_rest, next_node)?;
                current_node = next_node;
            }

            p.expect(&TokenKind::RParen)?;

            Ok(first_node)
        })
    }

    // =========================================================================
    // RDF 1.2 (Turtle-star) — asserting forms only
    // =========================================================================
    //
    // Supported: reified triples `<< s p o >>` / `<< s p o ~ reifier >>`
    // (subject or object position, nesting via the reifier node) and
    // annotation blocks `s p o {| … |}` / `s p o ~ reifier {| … |}`.
    //
    // Deliberately rejected with specific deferred errors:
    // - `<<( … )>>` triple terms as values (no Fluree representation yet;
    //   the triple-term-as-value epic owns this),
    // - star constructs nested inside an annotation body
    //   (annotation-of-annotation — mirrors the JSON-LD `@annotation`
    //   lowering's v1 deferral),
    // - annotations on RDF collections (`( … ) {| … |}`) — collections are
    //   emitted as indexed list items with no single object term to reify.

    /// Error for `<<( … )>>` triple terms as values.
    fn triple_term_deferred_error(&self) -> TurtleError {
        TurtleError::parse(
            self.current().start as usize,
            "RDF 1.2 triple terms as values ('<<( … )>>') are deferred in Turtle \
             ingest; only the asserting forms are supported (reified triples \
             '<< s p o >>' with optional '~ reifier', and annotation blocks '{| … |}')",
        )
    }

    /// Guard shared by every star construct: the sink must support
    /// reified-triple events, and star constructs must not appear inside
    /// an annotation body (the deferred annotation-of-annotation shape).
    fn check_star_allowed(&self, construct: &str) -> Result<()> {
        if !self.sink.supports_reified_triples() {
            return Err(TurtleError::parse(
                self.current().start as usize,
                format!(
                    "Turtle-star {construct} is not supported on this ingest path \
                     (deferred); only direct Turtle insert/import paths accept \
                     RDF 1.2 asserting forms"
                ),
            ));
        }
        if self.annotation_depth > 0 {
            return Err(TurtleError::parse(
                self.current().start as usize,
                format!(
                    "Turtle-star {construct} nested inside an annotation body is the \
                     deferred annotation-of-annotation shape (v1) — mirrors the \
                     JSON-LD @annotation deferral"
                ),
            ));
        }
        Ok(())
    }

    /// Annotations after collection objects have no single object term to
    /// reify (collections are emitted as indexed list items); reject with a
    /// specific deferred error instead of a generic parse failure.
    fn reject_annotation_on_collection(&mut self) -> Result<()> {
        if matches!(
            self.current().kind,
            TokenKind::Tilde | TokenKind::AnnotationOpen
        ) {
            return Err(TurtleError::parse(
                self.current().start as usize,
                "RDF 1.2 annotations on collection objects ('( … ) {| … |}') are \
                 deferred; annotate a single-object triple instead",
            ));
        }
        Ok(())
    }

    /// Parse a reified triple: `<< rtSubject verb rtObject reifier? >>`.
    ///
    /// Emits the base triple, mints/resolves the reifier, signals the
    /// reifier attachment to the sink, and returns the reifier `TermId`
    /// (which stands in for the reified triple in the surrounding
    /// subject/object position).
    ///
    /// A FRESH blank-node reifier is minted per anonymous occurrence —
    /// two textual occurrences of the same `<< s p o >>` never share a
    /// reifier (mirrors the JSON-LD `_:fluree_ann_N` minting; W3C
    /// eval-triple-terms `pattern-3-nomatch` depends on this).
    fn parse_reified_triple(&mut self) -> Result<TermId> {
        self.with_nesting(|p| {
            p.check_star_allowed("reified triple ('<< … >>')")?;
            p.expect(&TokenKind::ReifiedTripleStart)?;

            let subject = p.parse_rt_subject()?;
            let predicate = p.parse_predicate()?;
            let object = p.parse_rt_object()?;

            // Optional reifier: `~` (iri | BlankNode)? — bare `~` mints fresh.
            let reifier = if matches!(p.current().kind, TokenKind::Tilde) {
                p.advance()?;
                p.parse_reifier_term()?
            } else {
                p.sink_term_blank(None)
            };

            p.expect(&TokenKind::ReifiedTripleEnd)?;

            // Fluree's edge-annotation model reifies an asserted edge: emit the
            // base triple, then the reifier attachment (documented divergence
            // from RDF 1.2's non-asserting `<< >>`; see the roadmap's construct
            // inventory).
            p.sink_emit_triple(subject, predicate, object)?;
            p.sink_emit_reified_triple(subject, predicate, object, reifier)?;

            Ok(reifier)
        })
    }

    /// rtSubject ::= iri | BlankNode | reifiedTriple
    fn parse_rt_subject(&mut self) -> Result<TermId> {
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
            TokenKind::ReifiedTripleStart => self.parse_reified_triple(),
            TokenKind::TripleTermStart => Err(self.triple_term_deferred_error()),
            _ => Err(TurtleError::parse(
                self.current().start as usize,
                format!(
                    "expected reified-triple subject (IRI, blank node, or nested \
                     '<< … >>'), found {:?}",
                    self.current().kind
                ),
            )),
        }
    }

    /// rtObject ::= iri | BlankNode | literal | tripleTerm | reifiedTriple
    ///
    /// Note: collections and blank-node property lists are NOT allowed
    /// inside a reified triple (per the RDF 1.2 Turtle grammar), so this
    /// does not reuse `parse_object`.
    fn parse_rt_object(&mut self) -> Result<TermId> {
        match self.current().kind.clone() {
            TokenKind::LBracket | TokenKind::LParen | TokenKind::Nil => Err(TurtleError::parse(
                self.current().start as usize,
                format!(
                    "collections and blank-node property lists are not allowed \
                     inside a reified triple, found {:?}",
                    self.current().kind
                ),
            )),
            TokenKind::TripleTermStart => Err(self.triple_term_deferred_error()),
            TokenKind::ReifiedTripleStart => self.parse_reified_triple(),
            _ => self.parse_object(),
        }
    }

    /// reifier ::= '~' (iri | BlankNode)? — the `~` is already consumed.
    /// A bare `~` (next token closes the construct or continues the
    /// statement) mints a fresh anonymous reifier.
    fn parse_reifier_term(&mut self) -> Result<TermId> {
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
            // Bare `~`: fresh anonymous reifier.
            _ => Ok(self.sink_term_blank(None)),
        }
    }

    /// Parse the annotation tail after an object:
    /// `annotation ::= (reifier | annotationBlock)*` where
    /// `annotationBlock ::= '{|' predicateObjectList '|}'`.
    ///
    /// - `~ reifier` attaches `reifier` to the `(subject, predicate,
    ///   object)` edge (reifier bundle emitted immediately) and stays
    ///   "pending" so an immediately following `{| … |}` describes that
    ///   same reifier instead of minting a second one.
    /// - `{| … |}` without a pending reifier mints a FRESH anonymous
    ///   reifier (never deduped by edge identity), emits its bundle, then
    ///   parses the body as ordinary triples about the reifier.
    fn parse_annotation_tail(
        &mut self,
        subject: TermId,
        predicate: TermId,
        object: TermId,
    ) -> Result<()> {
        // TERM-LIFETIME HAZARD, named here because this is the one place it
        // can bite. `object` may be a LITERAL id, and it stays live across the
        // annotation body — which mints further terms, including literals of
        // its own — before being handed to `emit_reified_triple` below.
        //
        // Everywhere else the parser emits a literal immediately after minting
        // it, so at most one literal id is live at a time and a sink may
        // recycle literal slots per statement (see `GraphSink::end_statement`)
        // without any live id being clobbered. Here that is not true: the
        // invariant this code relies on is that a sink does NOT reuse a
        // literal slot until `end_statement`, and the whole annotation tail is
        // inside one statement.
        //
        // Unreachable today — star constructs require
        // `supports_reified_triples()`, and the only recycling sink
        // (`GraphCollectorSink`) returns false, so no sink both recycles and
        // accepts these events. It becomes live the moment a writer sink
        // supports both. A sink that does must either keep literal ids valid
        // for the whole statement (what `end_statement` already promises) or
        // this function must materialize `object` before parsing the body.
        let mut pending: Option<TermId> = None;
        loop {
            match self.current().kind {
                TokenKind::Tilde => {
                    self.check_star_allowed("reifier ('~')")?;
                    self.advance()?;
                    let reifier = self.parse_reifier_term()?;
                    self.sink_emit_reified_triple(subject, predicate, object, reifier)?;
                    pending = Some(reifier);
                }
                TokenKind::AnnotationOpen => {
                    self.check_star_allowed("annotation block ('{| … |}')")?;
                    self.advance()?;
                    let reifier = match pending.take() {
                        Some(r) => r,
                        None => {
                            let r = self.sink_term_blank(None);
                            self.sink_emit_reified_triple(subject, predicate, object, r)?;
                            r
                        }
                    };
                    if !matches!(self.current().kind, TokenKind::AnnotationClose) {
                        self.annotation_depth += 1;
                        let body = self.parse_predicate_object_list(reifier);
                        self.annotation_depth -= 1;
                        body?;
                    }
                    self.expect(&TokenKind::AnnotationClose)?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    /// Resolve a potentially relative IRI against the base (RFC 3986 §5).
    ///
    /// Delegates to the shared resolver in `fluree_vocab::iri`; this wrapper
    /// only supplies the Turtle-specific "relative IRI without base" error.
    fn resolve_iri(&self, reference: &str) -> Result<String> {
        if is_absolute_iri(reference) {
            return Ok(reference.to_string());
        }

        let base = match &self.base {
            Some(b) => b,
            None => {
                return Err(TurtleError::IriResolution(format!(
                    "relative IRI '{reference}' without base"
                )));
            }
        };

        Ok(fluree_vocab::iri::resolve_iri(base, reference))
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

/// Parse a Turtle document into GraphSink events, with the ingest-shaped
/// [`ParserOptions::default`].
pub fn parse<S: GraphSink>(input: &str, sink: &mut S) -> Result<()> {
    Parser::new(input, sink)?.parse()
}

/// Parse a Turtle document into GraphSink events under explicit
/// [`ParserOptions`].
///
/// [`parse`] is this with [`ParserOptions::default`]; use
/// [`ParserOptions::conformant`] for the faithful-RDF shape (spine
/// collections, preserved numeric lexical forms).
pub fn parse_with_options<S: GraphSink>(
    input: &str,
    sink: &mut S,
    options: ParserOptions,
) -> Result<()> {
    Parser::with_options(input, sink, options)?.parse()
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
    parse_with_prefixes_base_options(input, sink, prefixes, base, ParserOptions::default())
}

/// [`parse_with_prefixes_base`] under explicit [`ParserOptions`].
///
/// This is the full entry point — every other `parse*` function is this one
/// with something defaulted — so the chunked reader paths, which need the
/// pre-seeded prelude, can opt into conformance options too.
pub fn parse_with_prefixes_base_options<S: GraphSink>(
    input: &str,
    sink: &mut S,
    prefixes: &[(String, String)],
    base: Option<&str>,
    options: ParserOptions,
) -> Result<()> {
    let mut parser = Parser::with_options(input, sink, options)?;
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
        Ok(sink.into_graph())
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
    fn test_integer_overflowing_i64_keeps_lexical_as_xsd_integer() {
        // xsd:integer is unbounded; a literal past i64 must keep its lexical
        // (typed-string lane promotes to BigInt downstream), never become 0.
        let input = r"
            @prefix ex: <http://example.org/> .
            ex:item ex:serial 123456789012345678901234567890 .
        ";
        let graph = parse_to_graph(input).unwrap();

        assert_eq!(graph.len(), 1);
        let triple = graph.iter().next().unwrap();
        match &triple.o {
            Term::Literal {
                value, datatype, ..
            } => {
                assert_eq!(
                    datatype.as_iri(),
                    "http://www.w3.org/2001/XMLSchema#integer"
                );
                assert_eq!(value.lexical(), "123456789012345678901234567890");
            }
            other => panic!("expected literal, got {other:?}"),
        }
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

    /// Collect every subject IRI in document order.
    fn subject_iris(input: &str) -> Vec<String> {
        let graph = parse_to_graph(input).unwrap();
        graph
            .iter()
            .filter_map(|t| match &t.s {
                Term::Iri(iri) => Some(iri.as_ref().to_string()),
                _ => None,
            })
            .collect()
    }

    /// Regression guard for issue #1395: `<#A>` and `<#B>` resolved against the
    /// same `@base` MUST yield two DISTINCT IRIs. The old `resolve_iri` dropped
    /// the fragment, collapsing every `<#Name>` to the bare base IRI (which in
    /// turn collapsed multi-`TriplesMap` R2RML mappings to one table).
    #[test]
    fn test_base_fragment_refs_resolve_to_distinct_iris() {
        let input = r#"
            @base <http://example.org/edw> .
            <#A> <p> "a" .
            <#B> <p> "b" .
        "#;
        let subjects = subject_iris(input);
        assert!(
            subjects.contains(&"http://example.org/edw#A".to_string()),
            "expected http://example.org/edw#A, got {subjects:?}"
        );
        assert!(
            subjects.contains(&"http://example.org/edw#B".to_string()),
            "expected http://example.org/edw#B, got {subjects:?}"
        );
        // The two references must NOT collapse to the same IRI.
        assert_ne!(subjects[0], subjects[1]);
    }

    /// A relative path reference that also carries a fragment: both the merged
    /// path and the reference fragment must survive (`<rel#frag>`).
    #[test]
    fn test_base_relative_path_with_fragment() {
        let input = r#"
            @base <http://example.org/edw> .
            <DimDate#col> <p> "x" .
        "#;
        let subjects = subject_iris(input);
        assert_eq!(subjects, vec!["http://example.org/DimDate#col".to_string()]);
    }

    /// When the base itself carries a fragment, a `<#frag>` reference replaces
    /// it (RFC 3986 §5.2.2: the resolved fragment is always the reference's).
    #[test]
    fn test_fragment_ref_against_base_with_fragment() {
        let input = r#"
            @base <http://example.org/path#oldfrag> .
            <#new> <p> "x" .
            <other#f> <p> "y" .
        "#;
        let subjects = subject_iris(input);
        assert!(
            subjects.contains(&"http://example.org/path#new".to_string()),
            "expected http://example.org/path#new, got {subjects:?}"
        );
        assert!(
            subjects.contains(&"http://example.org/other#f".to_string()),
            "expected http://example.org/other#f, got {subjects:?}"
        );
    }

    /// `<>` against a fragmented base resolves to the base MINUS its fragment.
    #[test]
    fn test_empty_ref_against_fragmented_base_drops_fragment() {
        let input = r#"
            @base <http://example.org/path#oldfrag> .
            <> <p> "x" .
        "#;
        let subjects = subject_iris(input);
        assert_eq!(subjects, vec!["http://example.org/path".to_string()]);
    }

    /// A reference with no fragment of its own must NOT inherit the base's
    /// fragment.
    #[test]
    fn test_ref_without_fragment_does_not_inherit_base_fragment() {
        let input = r#"
            @base <http://example.org/path#oldfrag> .
            <other> <p> "x" .
        "#;
        let subjects = subject_iris(input);
        assert_eq!(subjects, vec!["http://example.org/other".to_string()]);
    }

    /// RFC 3986 §5.4.1 examples covering query + fragment recomposition against
    /// a base that has both a query and (implicitly) no fragment.
    #[test]
    fn test_query_and_fragment_resolution_rfc3986() {
        // base = http://a/b/c/d;p?q  (path "/b/c/d;p", query "q")
        let cases = [
            ("<#s>", "http://a/b/c/d;p?q#s"),
            ("<?y#s>", "http://a/b/c/d;p?y#s"),
            ("<g#s>", "http://a/b/c/g#s"),
            ("<g?y#s>", "http://a/b/c/g?y#s"),
            ("<>", "http://a/b/c/d;p?q"),
        ];
        for (reference, expected) in cases {
            let input = format!("@base <http://a/b/c/d;p?q> .\n{reference} <p> \"v\" .\n");
            let subjects = subject_iris(&input);
            assert_eq!(
                subjects,
                vec![expected.to_string()],
                "reference {reference} should resolve to {expected}"
            );
        }
    }

    #[test]
    fn bare_blank_node_property_list_statement() {
        // Turtle grammar allows `[ ... ] .` with no outer predicate list.
        let turtle = r"
            @prefix ex: <http://example.org/> .
            [
              ex:p ex:A ;
              ex:p ex:B ;
            ].
        ";
        let mut sink = GraphCollectorSink::new();
        parse(turtle, &mut sink).expect("bare [ ... ] . statement must parse");
        assert_eq!(sink.into_graph().len(), 2);
    }

    // =========================================================================
    // RDF 1.2 (Turtle-star) — asserting forms
    // =========================================================================

    use fluree_graph_ir::GraphSink;

    /// A term as recorded by [`StarSink`] — enough identity to assert on.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum RecTerm {
        Iri(String),
        Blank(String),
        Literal(String),
    }

    /// Recording sink that supports reified-triple events.
    #[derive(Default)]
    struct StarSink {
        terms: Vec<RecTerm>,
        blank_counter: u32,
        blank_labels: std::collections::HashMap<String, TermId>,
        triples: Vec<(RecTerm, RecTerm, RecTerm)>,
        reified: Vec<(RecTerm, RecTerm, RecTerm, RecTerm)>,
    }

    impl StarSink {
        fn t(&self, id: TermId) -> RecTerm {
            self.terms[id.index() as usize].clone()
        }
        fn add(&mut self, term: RecTerm) -> TermId {
            let id = TermId::new(self.terms.len() as u32);
            self.terms.push(term);
            id
        }
    }

    impl GraphSink for StarSink {
        fn on_base(&mut self, _base_iri: &str) {}
        fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {}
        fn term_iri(&mut self, iri: &str) -> TermId {
            self.add(RecTerm::Iri(iri.to_string()))
        }
        fn term_blank(&mut self, label: Option<&str>) -> TermId {
            match label {
                Some(l) => {
                    if let Some(&id) = self.blank_labels.get(l) {
                        return id;
                    }
                    let id = self.add(RecTerm::Blank(l.to_string()));
                    self.blank_labels.insert(l.to_string(), id);
                    id
                }
                None => {
                    self.blank_counter += 1;
                    let label = format!("anon{}", self.blank_counter);
                    self.add(RecTerm::Blank(label))
                }
            }
        }
        fn term_literal(
            &mut self,
            value: &str,
            _datatype: Datatype,
            _language: Option<&str>,
        ) -> TermId {
            self.add(RecTerm::Literal(value.to_string()))
        }
        fn term_literal_value(&mut self, value: LiteralValue, _datatype: Datatype) -> TermId {
            self.add(RecTerm::Literal(value.lexical()))
        }
        fn emit_triple(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
        ) -> fluree_graph_ir::SinkResult {
            let t = (self.t(subject), self.t(predicate), self.t(object));
            self.triples.push(t);
            Ok(())
        }
        fn supports_reified_triples(&self) -> bool {
            true
        }
        fn emit_reified_triple(
            &mut self,
            subject: TermId,
            predicate: TermId,
            object: TermId,
            reifier: TermId,
        ) -> fluree_graph_ir::SinkResult {
            let r = (
                self.t(subject),
                self.t(predicate),
                self.t(object),
                self.t(reifier),
            );
            self.reified.push(r);
            Ok(())
        }
    }

    fn parse_star(input: &str) -> StarSink {
        let mut sink = StarSink::default();
        parse(input, &mut sink).expect("star input must parse");
        sink
    }

    fn iri(suffix: &str) -> RecTerm {
        RecTerm::Iri(format!("http://example/{suffix}"))
    }

    const P: &str = "PREFIX : <http://example/>\n";

    #[test]
    fn star_reified_triple_subject_position() {
        // data-1 shape: assert base, mint anon reifier, reifier gets props.
        let sink = parse_star(&format!("{P}<<:a :b :c>> :q :z ."));
        assert_eq!(sink.reified.len(), 1);
        let (s, p, o, r) = &sink.reified[0];
        assert_eq!((s, p, o), (&iri("a"), &iri("b"), &iri("c")));
        assert!(matches!(r, RecTerm::Blank(_)), "anon reifier: {r:?}");
        // Base triple asserted + reifier property triple.
        assert!(sink.triples.contains(&(iri("a"), iri("b"), iri("c"))));
        assert!(sink.triples.contains(&(r.clone(), iri("q"), iri("z"))));
        assert_eq!(sink.triples.len(), 2);
    }

    #[test]
    fn star_reified_triple_object_position_and_named_reifier() {
        // data-2 pattern-3 shape: named reifier shared across occurrences.
        let sink = parse_star(&format!(
            "{P}:a1 :b <<:s :p1 :o ~ :reifier >> .\n<<:s :p1 :o ~ :reifier >> :b :a2 ."
        ));
        // Two reified events, both naming :reifier for the same base triple.
        assert_eq!(sink.reified.len(), 2);
        for (s, p, o, r) in &sink.reified {
            assert_eq!((s, p, o), (&iri("s"), &iri("p1"), &iri("o")));
            assert_eq!(r, &iri("reifier"));
        }
        // The reifier is a queryable node on both sides.
        assert!(sink
            .triples
            .contains(&(iri("a1"), iri("b"), iri("reifier"))));
        assert!(sink
            .triples
            .contains(&(iri("reifier"), iri("b"), iri("a2"))));
    }

    #[test]
    fn star_fresh_reifier_per_anonymous_occurrence() {
        // pattern-3-nomatch depends on this: two textual occurrences of the
        // SAME `<< s p o >>` must mint DISTINCT reifiers (never dedup by
        // base-triple identity).
        let sink = parse_star(&format!(
            "{P}:a1 :b2 <<:s :p1 :o >> .\n<<:s :p1 :o >> :b2 :a2 ."
        ));
        assert_eq!(sink.reified.len(), 2);
        let r1 = &sink.reified[0].3;
        let r2 = &sink.reified[1].3;
        assert_ne!(
            r1, r2,
            "anonymous reifiers must be fresh per occurrence, never deduped by edge"
        );
    }

    #[test]
    fn star_annotation_block() {
        // data-3/data-8 shape: `:a :b :c {| :q :z |} .`
        let sink = parse_star(&format!("{P}:a :b :c {{| :q :z |}} ."));
        assert_eq!(sink.reified.len(), 1);
        let (s, p, o, r) = &sink.reified[0];
        assert_eq!((s, p, o), (&iri("a"), &iri("b"), &iri("c")));
        assert!(matches!(r, RecTerm::Blank(_)));
        // Base + annotation-body property about the reifier.
        assert!(sink.triples.contains(&(iri("a"), iri("b"), iri("c"))));
        assert!(sink.triples.contains(&(r.clone(), iri("q"), iri("z"))));
    }

    #[test]
    fn star_tilde_reifier_then_annotation_block_shares_reifier() {
        // `:a :b :c ~ :r {| :q :z |}` — the block describes :r; exactly ONE
        // reified event is emitted.
        let sink = parse_star(&format!("{P}:a :b :c ~ :r {{| :q :z |}} ."));
        assert_eq!(sink.reified.len(), 1);
        assert_eq!(sink.reified[0].3, iri("r"));
        assert!(sink.triples.contains(&(iri("r"), iri("q"), iri("z"))));
    }

    #[test]
    fn star_bare_tilde_mints_fresh_reifier() {
        let sink = parse_star(&format!("{P}:a :b :c ~ ."));
        assert_eq!(sink.reified.len(), 1);
        assert!(matches!(sink.reified[0].3, RecTerm::Blank(_)));
    }

    #[test]
    fn star_nested_reified_triple() {
        // data-2 pattern-6 shape: `<< <<:s :p2 :o>> :p3 :z >> :q :o .`
        // Inner reifier becomes the subject of the outer base triple.
        let sink = parse_star(&format!("{P}<< <<:s :p2 :o>> :p3 :z >> :q :o ."));
        assert_eq!(sink.reified.len(), 2);
        let (is_, ip, io, ir) = sink.reified[0].clone();
        assert_eq!((is_, ip, io), (iri("s"), iri("p2"), iri("o")));
        let (os_, op, oo, or_) = sink.reified[1].clone();
        assert_eq!(os_, ir, "outer base subject is the inner reifier");
        assert_eq!((op, oo), (iri("p3"), iri("z")));
        assert!(sink.triples.contains(&(or_, iri("q"), iri("o"))));
    }

    #[test]
    fn star_bnode_reifier_dedups_by_label() {
        // data-7 shape: `~ _:bnodereifier` twice = same reifier node.
        let sink = parse_star(&format!(
            "{P}:x10 :left << :a :b 9 ~ _:bnodereifier >> .\n\
             :x10 :right << :a :b 9 ~ _:bnodereifier >> ."
        ));
        assert_eq!(sink.reified.len(), 2);
        assert_eq!(sink.reified[0].3, sink.reified[1].3);
        assert!(matches!(sink.reified[0].3, RecTerm::Blank(_)));
    }

    #[test]
    fn star_annotation_trailing_semicolon() {
        let sink = parse_star(&format!("{P}:a :b :c {{| :q :z ; |}} ."));
        assert_eq!(sink.reified.len(), 1);
    }

    #[test]
    fn star_triple_term_rejected_with_deferred_error() {
        let mut sink = StarSink::default();
        let err = parse(&format!("{P}:x1 :left <<( :a :b 123 )>> ."), &mut sink)
            .expect_err("triple terms as values must be rejected");
        let msg = err.to_string();
        assert!(msg.contains("triple terms as values"), "{msg}");
        assert!(msg.contains("deferred"), "{msg}");
    }

    #[test]
    fn star_triple_term_in_reified_triple_rejected() {
        let mut sink = StarSink::default();
        let err = parse(
            &format!("{P}:f :g << :s :p <<(:x2 :y3 123 )>> >> ."),
            &mut sink,
        )
        .expect_err("nested triple term must be rejected");
        assert!(err.to_string().contains("triple terms as values"));
    }

    #[test]
    fn star_annotation_of_annotation_rejected() {
        let mut sink = StarSink::default();
        let err = parse(
            &format!("{P}:a :b :c {{| :q :z {{| :q2 :z2 |}} |}} ."),
            &mut sink,
        )
        .expect_err("annotation-of-annotation is deferred");
        let msg = err.to_string();
        assert!(msg.contains("annotation-of-annotation"), "{msg}");
    }

    #[test]
    fn star_annotation_on_collection_rejected() {
        let mut sink = StarSink::default();
        let err = parse(&format!("{P}:a :b ( :c :d ) {{| :q :z |}} ."), &mut sink)
            .expect_err("annotations on collections are deferred");
        assert!(err.to_string().contains("collection objects"));
    }

    #[test]
    fn star_rejected_on_unsupporting_sink() {
        // GraphCollectorSink does not support reified triples — the parser
        // must reject with a clear deferred error, never silently drop
        // reifier semantics.
        let mut sink = GraphCollectorSink::new();
        let err = parse(&format!("{P}<<:a :b :c>> :q :z ."), &mut sink)
            .expect_err("collector sink must reject star input");
        let msg = err.to_string();
        assert!(msg.contains("not supported on this ingest path"), "{msg}");

        let mut sink = GraphCollectorSink::new();
        let err = parse(&format!("{P}:a :b :c {{| :q :z |}} ."), &mut sink)
            .expect_err("collector sink must reject annotation blocks");
        assert!(err
            .to_string()
            .contains("not supported on this ingest path"));
    }

    #[test]
    fn star_full_data2_corpus_parses() {
        // The complete W3C eval-triple-terms data-2.ttl (all asserting
        // forms: anon + named reifiers, object position, nesting). The
        // pattern-3 caveat: these two tests only go green if the WHOLE
        // file ingests.
        let data2 = format!(
            "{P}:s :p1 :o .\n\
             <<:s :p1 :o>> :q :z .\n\
             :a1 :b <<:s :p1 :o ~ :reifier >>  .\n\
             <<:s :p1 :o  ~ :reifier >> :b :a2 .\n\
             :a1 :b2 <<:s :p1 :o >>  .\n\
             <<:s :p1 :o >> :b2 :a2 .\n\
             :s :p2 :o .\n\
             <<:s :p2 :o>> :sym <<:s :p2 :o>> .\n\
             <<:s :p2 :o>> :p3 :z .\n\
             << <<:s :p2 :o>> :p3 :z >> :q :o .\n\
             <<:s :p2 :o ~ :reifier2 >> :p4 :z .\n\
             << <<:s :p2 :o  ~ :reifier2 >> :p4 :z >> :q :o .\n"
        );
        let sink = parse_star(&data2);
        // 13 reified occurrences in the file (nested `<< << … >> … >>`
        // lines contribute two each).
        assert_eq!(sink.reified.len(), 13);
        // pattern-3 join exists…
        assert!(sink
            .triples
            .contains(&(iri("a1"), iri("b"), iri("reifier"))));
        assert!(sink
            .triples
            .contains(&(iri("reifier"), iri("b"), iri("a2"))));
        // …and the pattern-3-nomatch pair uses two distinct anon reifiers.
        let b2_object = sink
            .triples
            .iter()
            .find(|(s, p, _)| s == &iri("a1") && p == &iri("b2"))
            .map(|(_, _, o)| o.clone())
            .expect("a1 :b2 triple");
        let b2_subject = sink
            .triples
            .iter()
            .find(|(s, p, o)| matches!(s, RecTerm::Blank(_)) && p == &iri("b2") && o == &iri("a2"))
            .map(|(s, _, _)| s.clone())
            .expect(":b2 a2 triple");
        assert_ne!(b2_object, b2_subject, "pattern-3-nomatch must stay empty");
    }
    // =========================================================================
    // Recursion depth ceiling (issue #1480)
    // =========================================================================

    /// `[ ex:p [ ex:p … ex:o … ] ] .` nested `depth` levels.
    fn nested_property_lists(depth: usize) -> String {
        format!(
            "@prefix ex: <http://example.org/> .\n{}ex:o{} .",
            "[ ex:p ".repeat(depth),
            " ]".repeat(depth)
        )
    }

    /// `ex:s ex:p ( ( … ( ex:o ) … ) ) .` nested `depth` levels. The
    /// innermost collection holds a real item — a bare `()` lexes as a
    /// single `Nil` token and would not count as a nesting level.
    fn nested_collections(depth: usize) -> String {
        format!(
            "@prefix ex: <http://example.org/> .\nex:s ex:p {}ex:o{} .",
            "( ".repeat(depth),
            " )".repeat(depth)
        )
    }

    /// `<< … << ex:s ex:p ex:o >> … ex:p ex:o >> ex:p ex:o .` nested
    /// `depth` levels via the reified-triple subject position.
    fn nested_reified_triples(depth: usize) -> String {
        format!(
            "@prefix ex: <http://example.org/> .\n{}ex:s ex:p ex:o{} .",
            "<< ".repeat(depth),
            " >> ex:p ex:o".repeat(depth)
        )
    }

    fn assert_depth_err(err: TurtleError) {
        assert!(
            err.to_string().contains("maximum depth"),
            "expected the nesting-depth error, got: {err}"
        );
    }

    /// The ceiling is inclusive: depth == MAX_NESTING_DEPTH parses, pinning
    /// the boundary against a stricter-by-one guard regression.
    #[test]
    fn nesting_at_the_ceiling_parses() {
        let depth = crate::error::MAX_NESTING_DEPTH as usize;
        parse_to_graph(&nested_property_lists(depth)).expect("property lists");
        parse_to_graph(&nested_collections(depth)).expect("collections");
        parse_star(&nested_reified_triples(depth));
    }

    #[test]
    fn nesting_past_the_ceiling_errors_cleanly() {
        let depth = (crate::error::MAX_NESTING_DEPTH + 1) as usize;
        assert_depth_err(parse_to_graph(&nested_property_lists(depth)).expect_err("lists"));
        assert_depth_err(parse_to_graph(&nested_collections(depth)).expect_err("collections"));
        let mut sink = StarSink::default();
        assert_depth_err(parse(&nested_reified_triples(depth), &mut sink).expect_err("reified"));
    }

    /// The original DoS shape: a small document with adversarial nesting
    /// depth must fail with a clean parse error, not a stack overflow.
    #[test]
    fn deeply_nested_property_lists_do_not_overflow_the_stack() {
        assert_depth_err(parse_to_graph(&nested_property_lists(100_000)).expect_err("deep"));
    }

    /// End-to-end guard for the statement-scoped literal recycling that
    /// `end_statement` enables in `GraphCollectorSink`: a later statement
    /// reuses an earlier statement's literal slot, so if any triple held a
    /// slot reference instead of a clone, the earlier values would be
    /// overwritten here.
    #[test]
    fn recycled_literal_slots_do_not_corrupt_earlier_statements() {
        let input = r#"
            @prefix ex: <http://example.org/> .
            ex:a ex:p "one" .
            ex:b ex:p "two" .
            ex:c ex:p "three", "four" .
        "#;
        let graph = parse_to_graph(input).unwrap();
        assert_eq!(graph.len(), 4);

        let mut pairs: Vec<(String, String)> = graph
            .iter()
            .map(|t| {
                let subject = t.s.as_iri().expect("IRI subject").to_string();
                let object = match &t.o {
                    Term::Literal { value, .. } => value.lexical(),
                    other => panic!("expected literal, got {other:?}"),
                };
                (subject, object)
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("http://example.org/a".to_string(), "one".to_string()),
                ("http://example.org/b".to_string(), "two".to_string()),
                ("http://example.org/c".to_string(), "four".to_string()),
                ("http://example.org/c".to_string(), "three".to_string()),
            ]
        );
    }

    // =========================================================================
    // Blank-node namespace disjointness
    // =========================================================================

    /// Count the DISTINCT blank-node identities in a graph.
    fn distinct_blanks(graph: &Graph) -> Vec<String> {
        let mut labels: Vec<String> = graph
            .iter()
            .flat_map(|t| [&t.s, &t.o])
            .filter_map(|t| match t {
                Term::BlankNode(b) => Some(b.as_str().to_string()),
                _ => None,
            })
            .collect();
        labels.sort();
        labels.dedup();
        labels
    }

    /// User-written labels and parser-minted anonymous nodes must never
    /// collide. An isomorphism check structurally CANNOT catch this: merged
    /// nodes are isomorphic to themselves, so the merged graph looks
    /// well-formed. Only counting distinct identities finds it.
    ///
    /// Both mint sites are covered, because they fail independently:
    /// Spine-mode collections (systematic — every collection mints spine
    /// nodes) and `[ … ]` property lists (which mint in EVERY mode, so a
    /// spine-only test would pass while the bracket path stayed broken).
    #[test]
    fn minted_blanks_never_collide_with_user_written_labels() {
        // Site 1: Spine collection. `( ex:a ex:b )` mints two spine nodes,
        // which a `b{N}` counter would name `b1`/`b2` — exactly the labels
        // this document already uses. Four distinct RDF nodes must stay four.
        let spine_doc = r#"
            @prefix ex: <http://example.org/> .
            _:b1 ex:p "user-one" .
            _:b2 ex:p "user-two" .
            ex:s ex:list ( ex:a ex:b ) .
        "#;
        let mut sink = GraphCollectorSink::new();
        parse_with_options(spine_doc, &mut sink, ParserOptions::conformant()).expect("spine parse");
        let graph = sink.into_graph();
        let blanks = distinct_blanks(&graph);
        assert_eq!(
            blanks.len(),
            4,
            "2 user labels + 2 spine nodes = 4 distinct nodes, got {blanks:?}"
        );

        // Site 2: blank-node property list, in the DEFAULT mode — `[ … ]`
        // mints regardless of collection style, so this path is not covered
        // by the spine case above. Two distinct nodes must stay two.
        let bracket_doc = r#"
            @prefix ex: <http://example.org/> .
            _:b1 ex:p "user-one" .
            ex:t ex:has [ ex:q "anon" ] .
        "#;
        let mut sink = GraphCollectorSink::new();
        parse_with_options(bracket_doc, &mut sink, ParserOptions::default())
            .expect("default parse");
        let graph = sink.into_graph();
        let blanks = distinct_blanks(&graph);
        assert_eq!(
            blanks.len(),
            2,
            "1 user label + 1 bracket node = 2 distinct nodes, got {blanks:?}"
        );

        // And in both modes: each user label owns exactly its own triple, and
        // no minted label can lex as a user label — which is what makes the
        // disjointness structural rather than lucky.
        let doc = r#"
            @prefix ex: <http://example.org/> .
            _:b1 ex:p "user-one" .
            _:b2 ex:p "user-two" .
            ex:s ex:list ( ex:a ex:b ) .
            ex:t ex:has [ ex:q "anon" ] .
        "#;
        for (mode, options) in [
            ("spine", ParserOptions::conformant()),
            ("default", ParserOptions::default()),
        ] {
            let mut sink = GraphCollectorSink::new();
            parse_with_options(doc, &mut sink, options).expect("parses");
            let graph = sink.into_graph();

            for (label, object) in [("b1", "user-one"), ("b2", "user-two")] {
                let owned: Vec<String> = graph
                    .iter()
                    .filter(|t| matches!(&t.s, Term::BlankNode(b) if b.as_str() == label))
                    .map(|t| format!("{} {} {}", t.s, t.p, t.o))
                    .collect();
                assert_eq!(
                    owned.len(),
                    1,
                    "[{mode}] _:{label} must own only its own triple; extra triples mean a \
                     minted node merged into it: {owned:#?}"
                );
                assert!(owned[0].contains(object), "[{mode}] {owned:?}");
            }

            for label in distinct_blanks(&graph) {
                if label == "b1" || label == "b2" {
                    continue;
                }
                assert!(
                    label.starts_with('-'),
                    "[{mode}] minted label {label} can lex as BLANK_NODE_LABEL and so can collide"
                );
            }
        }
    }

    // =========================================================================
    // ParserOptions — collection + numeric conformance
    // =========================================================================
    //
    // Two shapes out of one grammar: the DEFAULT is Fluree's ingest shape and
    // is pinned bit-for-bit by the pre-existing tests above (`test_collection`,
    // `test_empty_collection`, `test_integer_literal`) plus the explicit
    // default-mode tests here; `ParserOptions::conformant()` is the RDF the
    // document actually denotes, which is what the W3C Turtle suite tests.

    const EX: &str = "http://example.org/";

    fn parse_conformant(input: &str) -> Graph {
        let mut sink = GraphCollectorSink::new();
        parse_with_options(input, &mut sink, ParserOptions::conformant())
            .expect("conformant parse");
        sink.into_graph()
    }

    fn parse_spine(input: &str) -> Graph {
        let mut sink = GraphCollectorSink::new();
        parse_with_options(
            input,
            &mut sink,
            ParserOptions::new().with_collections(CollectionStyle::Spine),
        )
        .expect("spine parse");
        sink.into_graph()
    }

    /// Every triple as `(subject, predicate, object)` in `Term`'s N-Triples-ish
    /// display form.
    fn rendered(graph: &Graph) -> Vec<(String, String, String)> {
        graph
            .iter()
            .map(|t| (t.s.to_string(), t.p.to_string(), t.o.to_string()))
            .collect()
    }

    /// Follow an `rdf:first`/`rdf:rest` spine from `head`, returning each
    /// item's rendered form. Panics if the chain is not well-formed, which is
    /// the point: a partial spine must not read as a short list.
    fn walk_spine(graph: &Graph, head: &Term) -> Vec<String> {
        let mut items = Vec::new();
        let mut node = head.clone();
        loop {
            if matches!(&node, Term::Iri(iri) if iri.as_ref() == RDF_NIL) {
                return items;
            }
            let first = graph
                .iter()
                .find(|t| t.s == node && matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_FIRST))
                .unwrap_or_else(|| panic!("no rdf:first for {node}"));
            items.push(first.o.to_string());
            let rest = graph
                .iter()
                .find(|t| t.s == node && matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_REST))
                .unwrap_or_else(|| panic!("no rdf:rest for {node}"));
            node = rest.o.clone();
        }
    }

    /// The object of the single `ex:s ex:p ?o` triple.
    fn object_of_p(graph: &Graph) -> Term {
        let matches: Vec<_> = graph
            .iter()
            .filter(|t| matches!(&t.p, Term::Iri(p) if p.as_ref() == format!("{EX}p")))
            .collect();
        assert_eq!(matches.len(), 1, "expected exactly one ex:p triple");
        matches[0].o.clone()
    }

    // -- Spine mode ---------------------------------------------------------

    /// W3C `turtle-syntax-*` shape: a one-item object collection denotes
    /// three triples. The default mode emits one `emit_list_item` event for
    /// the same input.
    #[test]
    fn spine_object_collection_of_one_emits_three_triples() {
        let graph = parse_spine(&format!("@prefix ex: <{EX}> .\nex:s ex:p ( ex:o ) ."));
        assert_eq!(graph.len(), 3, "{:#?}", rendered(&graph));

        let head = object_of_p(&graph);
        assert!(head.is_blank(), "collection head must be a blank node");
        assert_eq!(walk_spine(&graph, &head), vec![format!("<{EX}o>")]);
        assert!(
            graph.iter().all(|t| !t.is_list_element()),
            "spine mode must not emit indexed list items"
        );
    }

    #[test]
    fn spine_object_collection_of_two_chains_through_a_second_node() {
        let graph = parse_spine(&format!("@prefix ex: <{EX}> .\nex:s ex:p ( ex:a ex:b ) ."));
        // 2 items x (first + rest) + the ex:p edge.
        assert_eq!(graph.len(), 5, "{:#?}", rendered(&graph));

        let head = object_of_p(&graph);
        assert_eq!(
            walk_spine(&graph, &head),
            vec![format!("<{EX}a>"), format!("<{EX}b>")]
        );
    }

    /// The silent-triple-loss bug: `()` in object position emitted NOTHING.
    /// In spine mode it denotes `rdf:nil`, as RDF says.
    #[test]
    fn spine_empty_object_collection_is_rdf_nil() {
        let graph = parse_spine(&format!("@prefix ex: <{EX}> .\nex:s ex:p () ."));
        assert_eq!(
            rendered(&graph),
            vec![(
                format!("<{EX}s>"),
                format!("<{EX}p>"),
                format!("<{RDF_NIL}>")
            )]
        );
    }

    /// Subject position already emitted a spine before this change; the point
    /// of `Spine` is that both positions now share the one code path.
    #[test]
    fn spine_subject_and_object_collections_agree() {
        let subject_side = parse_spine(&format!("@prefix ex: <{EX}> .\n( ex:a ) ex:p ex:o ."));
        let object_side = parse_spine(&format!("@prefix ex: <{EX}> .\nex:s ex:p ( ex:a ) ."));
        // Same shape either way: 1 first + 1 rest + 1 edge.
        assert_eq!(subject_side.len(), 3);
        assert_eq!(object_side.len(), 3);

        let spine_of = |g: &Graph| {
            let head = g
                .iter()
                .find(|t| matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_FIRST))
                .map(|t| t.s.clone())
                .expect("a spine node");
            walk_spine(g, &head)
        };
        assert_eq!(spine_of(&subject_side), vec![format!("<{EX}a>")]);
        assert_eq!(spine_of(&object_side), vec![format!("<{EX}a>")]);
    }

    #[test]
    fn spine_nested_collections_nest_their_spines() {
        let graph = parse_spine(&format!(
            "@prefix ex: <{EX}> .\nex:s ex:p ( ( ex:a ) ex:b ) ."
        ));
        // outer: 2 nodes x 2 + inner: 1 node x 2 + the ex:p edge.
        assert_eq!(graph.len(), 7, "{:#?}", rendered(&graph));

        let outer = object_of_p(&graph);
        let outer_items = walk_spine(&graph, &outer);
        assert_eq!(outer_items.len(), 2);
        assert_eq!(outer_items[1], format!("<{EX}b>"));

        let inner_head = graph
            .iter()
            .find(|t| {
                matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_FIRST)
                    && t.s == outer
                    && t.o.is_blank()
            })
            .map(|t| t.o.clone())
            .expect("inner collection head");
        assert_eq!(walk_spine(&graph, &inner_head), vec![format!("<{EX}a>")]);
    }

    /// An empty collection nested inside a collection is `rdf:nil` as an item.
    #[test]
    fn spine_nested_empty_collection_is_a_nil_item() {
        let graph = parse_spine(&format!("@prefix ex: <{EX}> .\nex:s ex:p ( () ) ."));
        assert_eq!(graph.len(), 3, "{:#?}", rendered(&graph));
        let head = object_of_p(&graph);
        assert_eq!(walk_spine(&graph, &head), vec![format!("<{RDF_NIL}>")]);
    }

    /// In `IndexedItems` a collection object has no single term to reify, so
    /// annotations on it are refused. In `Spine` it has one — the head — so
    /// the refusal does not apply.
    #[test]
    fn spine_mode_admits_annotations_on_collections() {
        let mut sink = StarSink::default();
        parse_with_options(
            &format!("{P}:s :p ( :a ) {{| :q :z |}} ."),
            &mut sink,
            ParserOptions::new().with_collections(CollectionStyle::Spine),
        )
        .expect("a spine collection has a head term to reify");
        assert_eq!(sink.reified.len(), 1);

        // Same input, default mode: still refused.
        let mut sink = StarSink::default();
        let err = parse(&format!("{P}:s :p ( :a ) {{| :q :z |}} ."), &mut sink)
            .expect_err("indexed items leave nothing to reify");
        assert!(err.to_string().contains("collection objects"), "{err}");
    }

    // -- Default (IndexedItems) mode ----------------------------------------

    /// The default shape, pinned explicitly: one indexed event per item, no
    /// spine triples at all.
    #[test]
    fn default_object_collection_stays_indexed_with_no_spine() {
        let graph =
            parse_to_graph(&format!("@prefix ex: <{EX}> .\nex:s ex:p ( ex:a ex:b ) .")).unwrap();
        assert_eq!(graph.len(), 2);
        assert!(graph.iter().all(fluree_graph_ir::Triple::is_list_element));
        assert!(
            !graph
                .iter()
                .any(|t| matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_FIRST
                    || p.as_ref() == RDF_REST)),
            "default mode must not emit a spine"
        );
    }

    /// Documented (lossy) default: an object-position `()` emits nothing.
    /// Changing this is exactly what `Spine` is for.
    #[test]
    fn default_empty_object_collection_still_emits_nothing() {
        let graph = parse_to_graph(&format!("@prefix ex: <{EX}> .\nex:s ex:p () .")).unwrap();
        assert_eq!(graph.len(), 0);
    }

    /// Subject-position collections emitted a spine before this change and
    /// still do in the default mode — the change is scoped to object position.
    #[test]
    fn default_subject_collection_still_emits_a_spine() {
        let graph = parse_to_graph(&format!("@prefix ex: <{EX}> .\n( ex:a ) ex:p ex:o .")).unwrap();
        assert_eq!(graph.len(), 3);
        assert!(graph
            .iter()
            .any(|t| matches!(&t.p, Term::Iri(p) if p.as_ref() == RDF_FIRST)));
    }

    // -- Numerics -----------------------------------------------------------

    /// `+1`, `01`, `1e0`, `1.0e0` are distinct lexical forms of two values.
    /// Canonicalizing makes them unrepresentable; several W3C files depend on
    /// them surviving.
    #[test]
    fn preserve_lexical_keeps_the_source_spelling_of_numerics() {
        let cases = [
            ("+1", "http://www.w3.org/2001/XMLSchema#integer"),
            ("01", "http://www.w3.org/2001/XMLSchema#integer"),
            ("-0", "http://www.w3.org/2001/XMLSchema#integer"),
            ("1e0", "http://www.w3.org/2001/XMLSchema#double"),
            ("1.0e0", "http://www.w3.org/2001/XMLSchema#double"),
            ("1.0E0", "http://www.w3.org/2001/XMLSchema#double"),
            ("+1.0e-1", "http://www.w3.org/2001/XMLSchema#double"),
        ];
        for (lexical, datatype) in cases {
            let graph = parse_conformant(&format!("@prefix ex: <{EX}> .\nex:s ex:p {lexical} ."));
            assert_eq!(graph.len(), 1, "{lexical}");
            let triple = graph.iter().next().unwrap();
            match &triple.o {
                Term::Literal {
                    value, datatype: d, ..
                } => {
                    assert_eq!(value.lexical(), lexical, "lexical form of {lexical}");
                    assert_eq!(d.as_iri(), datatype, "datatype of {lexical}");
                }
                other => panic!("expected literal for {lexical}, got {other:?}"),
            }
        }
    }

    /// The default: the value survives, the spelling does not.
    #[test]
    fn canonicalize_is_the_default_and_discards_the_spelling() {
        let graph =
            parse_to_graph(&format!("@prefix ex: <{EX}> .\nex:s ex:p +1 ; ex:q 1e0 .")).unwrap();
        let mut objects: Vec<_> = graph.iter().map(|t| t.o.clone()).collect();
        objects.sort();
        assert!(
            objects.iter().any(|o| matches!(
                o,
                Term::Literal {
                    value: LiteralValue::Integer(1),
                    ..
                }
            )),
            "+1 must canonicalize to Integer(1): {objects:?}"
        );
        assert!(
            objects.iter().any(|o| matches!(
                o,
                Term::Literal { value: LiteralValue::Double(d), .. } if *d == 1.0
            )),
            "1e0 must canonicalize to Double(1.0): {objects:?}"
        );
    }

    /// Decimals and i64-overflowing integers already preserved their lexical
    /// form; `PreserveLexical` extends that lane rather than adding a second
    /// one, so these must be identical under both modes.
    #[test]
    fn decimal_and_bigint_lexicals_are_mode_independent() {
        let doc =
            format!("@prefix ex: <{EX}> .\nex:s ex:p 1.50 ; ex:q 123456789012345678901234567890 .");
        let render = |g: &Graph| {
            let mut r = rendered(g);
            r.sort();
            r
        };
        assert_eq!(
            render(&parse_to_graph(&doc).unwrap()),
            render(&parse_conformant(&doc))
        );
        let graph = parse_conformant(&doc);
        let lexicals: Vec<String> = graph
            .iter()
            .map(|t| match &t.o {
                Term::Literal { value, .. } => value.lexical(),
                other => panic!("expected literal, got {other:?}"),
            })
            .collect();
        assert!(lexicals.contains(&"1.50".to_string()), "{lexicals:?}");
        assert!(
            lexicals.contains(&"123456789012345678901234567890".to_string()),
            "{lexicals:?}"
        );
    }

    /// Booleans, strings, typed and language-tagged literals are untouched by
    /// `NumericStyle` — the knob is numerics only.
    #[test]
    fn non_numeric_literals_are_identical_under_both_modes() {
        let doc = format!(
            r#"@prefix ex: <{EX}> .
               @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
               ex:s ex:a true ; ex:b "plain" ; ex:c "tagged"@en ;
                    ex:d "2000-01-01"^^xsd:date ."#
        );
        let render = |g: &Graph| {
            let mut r = rendered(g);
            r.sort();
            r
        };
        assert_eq!(
            render(&parse_to_graph(&doc).unwrap()),
            render(&parse_conformant(&doc))
        );
    }

    // -- Default-mode pinning across the whole surface ----------------------

    /// One document exercising every construct the options touch, asserted
    /// triple-for-triple in the DEFAULT mode. This is the regression guard
    /// for "existing callers are unchanged": if adding options moved anything
    /// on the ingest path, it moves here.
    #[test]
    fn default_mode_output_is_pinned_triple_for_triple() {
        let doc = format!(
            r#"@prefix ex: <{EX}> .
               ex:s ex:int 30 ; ex:dbl 1.5e3 ; ex:dec 2.25 ; ex:list ( ex:a ex:b ) ;
                    ex:empty () ; ex:str "v" ."#
        );
        let mut got = rendered(&parse_to_graph(&doc).unwrap());
        got.sort();

        let xsd = "http://www.w3.org/2001/XMLSchema#";
        let mut want = vec![
            (
                format!("<{EX}s>"),
                format!("<{EX}int>"),
                format!("\"30\"^^<{xsd}integer>"),
            ),
            (
                format!("<{EX}s>"),
                format!("<{EX}dbl>"),
                format!("\"1.5E3\"^^<{xsd}double>"),
            ),
            (
                format!("<{EX}s>"),
                format!("<{EX}dec>"),
                format!("\"2.25\"^^<{xsd}decimal>"),
            ),
            (
                format!("<{EX}s>"),
                format!("<{EX}list>"),
                format!("<{EX}a>"),
            ),
            (
                format!("<{EX}s>"),
                format!("<{EX}list>"),
                format!("<{EX}b>"),
            ),
            (
                format!("<{EX}s>"),
                format!("<{EX}str>"),
                "\"v\"".to_string(),
            ),
        ];
        want.sort();
        assert_eq!(got, want);
    }

    /// And the same document under `conformant()`, so the two shapes are
    /// visibly different in exactly the intended places.
    #[test]
    fn conformant_mode_differs_only_in_collections_and_numerics() {
        let doc = format!(
            r#"@prefix ex: <{EX}> .
               ex:s ex:int 30 ; ex:dbl 1.5e3 ; ex:list ( ex:a ) ; ex:empty () ;
                    ex:str "v" ."#
        );
        let graph = parse_conformant(&doc);
        let xsd = "http://www.w3.org/2001/XMLSchema#";
        let rows = rendered(&graph);

        // Numerics keep their spelling.
        assert!(rows.contains(&(
            format!("<{EX}s>"),
            format!("<{EX}int>"),
            format!("\"30\"^^<{xsd}integer>")
        )));
        assert!(rows.contains(&(
            format!("<{EX}s>"),
            format!("<{EX}dbl>"),
            format!("\"1.5e3\"^^<{xsd}double>")
        )));
        // `()` is rdf:nil rather than nothing.
        assert!(rows.contains(&(
            format!("<{EX}s>"),
            format!("<{EX}empty>"),
            format!("<{RDF_NIL}>")
        )));
        // Non-numeric, non-collection terms are untouched.
        assert!(rows.contains(&(
            format!("<{EX}s>"),
            format!("<{EX}str>"),
            "\"v\"".to_string()
        )));
        // 4 scalar edges + 1 collection edge + 2 spine triples.
        assert_eq!(graph.len(), 7, "{rows:#?}");
    }

    /// Options ride through the pre-seeded-prelude entry point too — that is
    /// the one the chunked/parallel readers use.
    #[test]
    fn options_apply_on_the_prefixes_base_entry_point() {
        let mut sink = GraphCollectorSink::new();
        parse_with_prefixes_base_options(
            "ex:s ex:p ( ex:a ) .",
            &mut sink,
            &[("ex".to_string(), EX.to_string())],
            None,
            ParserOptions::conformant(),
        )
        .expect("prelude-seeded conformant parse");
        assert_eq!(sink.into_graph().len(), 3);

        let mut sink = GraphCollectorSink::new();
        parse_with_prefixes_base(
            "ex:s ex:p ( ex:a ) .",
            &mut sink,
            &[("ex".to_string(), EX.to_string())],
            None,
        )
        .expect("prelude-seeded default parse");
        assert_eq!(sink.into_graph().len(), 1, "default entry point unchanged");
    }

    // =========================================================================
    // Sink protocol: early termination + statement lifecycle
    // =========================================================================

    /// A sink that accepts `budget` triples and then fails, standing in for a
    /// writer whose downstream pipe closed (`fluree rdf convert f.ttl | head`).
    struct FailingSink {
        budget: usize,
        emitted: usize,
        statements: usize,
        finished: usize,
    }

    impl FailingSink {
        fn new(budget: usize) -> Self {
            Self {
                budget,
                emitted: 0,
                statements: 0,
                finished: 0,
            }
        }
    }

    impl GraphSink for FailingSink {
        fn on_base(&mut self, _base_iri: &str) {}
        fn on_prefix(&mut self, _prefix: &str, _namespace_iri: &str) {}
        fn term_iri(&mut self, _iri: &str) -> TermId {
            TermId::new(0)
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
        fn emit_triple(
            &mut self,
            _subject: TermId,
            _predicate: TermId,
            _object: TermId,
        ) -> fluree_graph_ir::SinkResult {
            if self.emitted >= self.budget {
                return Err(fluree_graph_ir::SinkError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "downstream closed",
                )));
            }
            self.emitted += 1;
            Ok(())
        }
        fn end_statement(&mut self) {
            self.statements += 1;
        }
        fn finish(&mut self) -> fluree_graph_ir::SinkResult {
            self.finished += 1;
            Ok(())
        }
    }

    fn many_statements(n: usize) -> String {
        let mut doc = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..n {
            doc.push_str(&format!("ex:s{i} ex:p ex:o{i} .\n"));
        }
        doc
    }

    /// The `| head -5` case: once the sink refuses, parsing stops at that
    /// statement instead of running to EOF against a dead pipe.
    #[test]
    fn sink_failure_terminates_the_parse_immediately() {
        let mut sink = FailingSink::new(5);
        let err = parse(&many_statements(10_000), &mut sink)
            .expect_err("a refusing sink must fail the parse");

        assert!(
            matches!(&err, TurtleError::Sink(e) if e.is_broken_pipe()),
            "the sink's error must survive, not be flattened into a generic parse error: {err}"
        );
        assert_eq!(sink.emitted, 5, "no emission past the budget");
        // The `@prefix` directive plus five completed triple statements. The
        // sixth failed mid-flight and so was never terminated — exactly what
        // statement-scoped buffering needs.
        assert_eq!(sink.statements, 6);
    }

    /// `end_statement` fires once per completed statement, and directives
    /// count as statements (Turtle grammar: `statement ::= directive |
    /// triples '.'`).
    #[test]
    fn end_statement_fires_once_per_completed_statement() {
        let mut sink = FailingSink::new(usize::MAX);
        parse(&many_statements(3), &mut sink).unwrap();
        // 1 `@prefix` directive + 3 triple statements.
        assert_eq!(sink.statements, 4);
        assert_eq!(sink.emitted, 3);
    }

    /// A multi-triple statement is ONE statement: the boundary is the `.`,
    /// not the triple, so statement-scoped literal ids stay live across the
    /// whole predicate-object list.
    #[test]
    fn predicate_object_list_is_a_single_statement() {
        let mut sink = FailingSink::new(usize::MAX);
        parse(
            r#"@prefix ex: <http://example.org/> .
               ex:s ex:p "a", "b" ; ex:q "c" .
            "#,
            &mut sink,
        )
        .unwrap();
        assert_eq!(sink.emitted, 3);
        assert_eq!(sink.statements, 2, "one directive + one triple statement");
    }

    /// The parser never calls the protocol `finish()`: a sink can be fed by
    /// many `parse()` calls (one per chunk on the bulk-import path), so
    /// flushing is the owner's call, not the producer's.
    #[test]
    fn the_parser_does_not_call_protocol_finish() {
        let mut sink = FailingSink::new(usize::MAX);
        parse(&many_statements(3), &mut sink).unwrap();
        parse(&many_statements(3), &mut sink).unwrap();
        assert_eq!(sink.finished, 0);
        assert_eq!(sink.emitted, 6, "both parses fed the same sink");
    }

    /// Siblings at the same level do not accumulate depth: only the nesting
    /// chain counts, so wide-but-shallow documents stay parseable.
    #[test]
    fn sibling_nesting_does_not_accumulate_depth() {
        let mut input = String::from("@prefix ex: <http://example.org/> .\n");
        for i in 0..(crate::error::MAX_NESTING_DEPTH as usize * 4) {
            input.push_str(&format!("ex:s{i} ex:p [ ex:q ( ex:o ) ] .\n"));
        }
        parse_to_graph(&input).expect("sibling nesting must not accumulate");
    }
}
