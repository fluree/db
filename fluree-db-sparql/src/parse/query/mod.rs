//! SPARQL query parsing.
//!
//! This module parses SPARQL queries from tokens into AST nodes.

mod construct;
mod describe;
mod modifier;
mod pattern;
mod select;
mod term;
mod update;

#[cfg(test)]
mod tests;

use crate::ast::path::PropertyPath;
use crate::ast::{
    BaseDecl, GraphPattern, Pragmas, PrefixDecl, Prologue, QueryBody, SparqlAst, TriplePattern,
};
use crate::diag::{DiagCode, Diagnostic, ParseOutput};
use crate::lex::{tokenize_with_comments, TokenKind};
use crate::span::SourceSpan;

// Re-export sub-module dependencies for use via `super::` in child modules.
use super::expr;
use super::path;

/// A verb in a triple pattern: either a simple predicate or a property path.
enum Verb {
    /// Simple predicate (variable, IRI, or `a`)
    Simple(crate::ast::PredicateTerm),
    /// Property path expression
    Path(PropertyPath),
}

/// Parse a SPARQL query string into an AST.
///
/// Returns a `ParseOutput` containing the AST (if parsing succeeded) and
/// any diagnostics (errors or warnings).
pub fn parse_sparql(input: &str) -> ParseOutput<SparqlAst> {
    let (tokens, comments) = tokenize_with_comments(input);

    // Check for lexer errors first
    let lex_errors: Vec<_> = tokens
        .iter()
        .filter(|t| matches!(&t.kind, TokenKind::Error(_)))
        .collect();

    if !lex_errors.is_empty() {
        let diagnostics = lex_errors
            .into_iter()
            .map(|t| {
                if let TokenKind::Error(msg) = &t.kind {
                    Diagnostic::error(DiagCode::ExpectedToken, msg.to_string(), t.span)
                } else {
                    unreachable!()
                }
            })
            .collect();

        return ParseOutput::with_diagnostics(None, diagnostics);
    }

    let mut stream = super::stream::TokenStream::new(tokens);
    let mut parser = Parser::with_source(&mut stream, input);

    match parser.parse_query() {
        Some(mut ast) => {
            ast.pragmas = extract_pragmas(&comments);

            // Trailing-token / EOF assertion: after a complete Query or
            // Update parses, every remaining non-EOF token is an
            // error-severity diagnostic. Without this, anything after the
            // parsed operation was silently discarded — most damagingly a
            // standard multi-operation UPDATE request (`INSERT ...; DELETE
            // ...`), which committed only the first operation: silent data
            // loss (issue #1438). PR-U2 replaces the multi-op rejection with
            // real ';'-loop support and reuses this single entry-point
            // assertion (D-10a interim guard).
            let mut unconsumed_input = false;
            if !stream.is_eof() {
                if matches!(ast.body, QueryBody::Update(_)) && stream.check(&TokenKind::Semicolon) {
                    // Update ::= Prologue ( Update1 ( ';' Update )? )? — the
                    // recursive Update may be empty, so one trailing ';'
                    // after the last operation is valid SPARQL.
                    stream.advance();
                    if !stream.is_eof() {
                        unconsumed_input = true;
                        let span = stream.current_span();
                        stream.add_diagnostic(Diagnostic::error(
                            DiagCode::ExpectedToken,
                            "multi-operation SPARQL UPDATE requests (';'-separated) are not \
                             supported yet; only the first operation was parsed — submit each \
                             operation as its own request"
                                .to_string(),
                            span,
                        ));
                    }
                } else {
                    unconsumed_input = true;
                    stream
                        .error_at_current("unexpected trailing tokens after the end of the query");
                }
            }

            let diagnostics = stream.take_diagnostics();
            // Parse-time *semantic* rejections (currently the V5 BIND-scope
            // check) and unconsumed trailing input must prevent AST
            // production: unlike recovered syntax errors — where returning a
            // best-effort AST keeps diagnostics flowing for IDE/LSP use —
            // these produce an AST that does not faithfully represent the
            // request (V5: parsed completely but spec-invalid; trailing
            // input: the AST covers only a prefix of the input). Callers
            // that only check for an AST — most dangerously external users
            // of the public `lower_sparql_update_ast` entry, which takes the
            // AST without seeing these diagnostics — would otherwise execute
            // it; for a multi-operation UPDATE that re-opens exactly the
            // #1438 silent-data-loss window the guard above closes (roadmap
            // D-4: hard errors, no diagnostic-swallowing).
            let semantic_reject = diagnostics
                .iter()
                .any(|d| d.code == DiagCode::BindTargetAlreadyInScope && d.is_error());
            if semantic_reject || unconsumed_input {
                ParseOutput::with_diagnostics(None, diagnostics)
            } else {
                ParseOutput::with_diagnostics(Some(ast), diagnostics)
            }
        }
        None => ParseOutput::with_diagnostics(None, stream.take_diagnostics()),
    }
}

/// Extract Fluree `# PRAGMA ...` directives from the query's comments.
///
/// Comments are sourced from the lexer (`tokenize_with_comments`), so `#`
/// characters inside string literals or IRIs can never be misread as
/// directives, and the query stays valid SPARQL for standard tooling.
/// Comparison is case-insensitive on the `PRAGMA` keyword and pragma name;
/// the value is split on commas and whitespace. Unrecognized pragma names
/// are ignored (they are ordinary comments).
///
/// Supported:
/// - `# PRAGMA reasoning: owl2rl` (also `rdfs`, `owl2ql`, `datalog`,
///   `owl-datalog`, `none`, or a comma-separated combination)
/// - `# PRAGMA reasoning-max-facts: 20000000` — OWL2-RL materialization budget
/// - `# PRAGMA reasoning-max-seconds: 300` — OWL2-RL materialization budget
fn extract_pragmas(comments: &[String]) -> Pragmas {
    let mut pragmas = Pragmas::default();

    for comment in comments {
        let Some(rest) = strip_keyword_ci(comment, "PRAGMA") else {
            continue;
        };

        // `strip_keyword_ci` requires a word boundary, so plain `reasoning`
        // never matches the `reasoning-max-*` directives.
        if let Some(value) = strip_keyword_ci(rest, "reasoning-max-facts") {
            // Last pragma wins; the raw value is preserved (even if empty) so
            // lowering can reject an invalid number with a proper error.
            pragmas.reasoning_max_facts = Some(pragma_scalar_value(value));
        } else if let Some(value) = strip_keyword_ci(rest, "reasoning-max-seconds") {
            pragmas.reasoning_max_seconds = Some(pragma_scalar_value(value));
        } else if let Some(value) = strip_keyword_ci(rest, "reasoning") {
            let value = value.trim_start().strip_prefix(':').unwrap_or(value);
            let modes: Vec<String> = value
                .split([',', ' ', '\t'])
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            // Last pragma wins if repeated; an empty mode list is preserved so
            // lowering can reject `# PRAGMA reasoning:` with no value.
            pragmas.reasoning = Some(modes);
        }
    }

    pragmas
}

/// Extract a single trimmed scalar value after an optional `:`.
fn pragma_scalar_value(value: &str) -> String {
    let value = value.trim_start();
    let value = value.strip_prefix(':').unwrap_or(value);
    value.trim().to_string()
}

/// Strip a case-insensitive keyword prefix followed by a word boundary
/// (whitespace, `:`, or end of input). Returns the remainder.
fn strip_keyword_ci<'a>(input: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = input.trim_start();
    if trimmed.len() < keyword.len() || !trimmed.is_char_boundary(keyword.len()) {
        return None;
    }
    let (head, rest) = trimmed.split_at(keyword.len());
    if !head.eq_ignore_ascii_case(keyword) {
        return None;
    }
    match rest.chars().next() {
        None => Some(rest),
        Some(c) if c.is_whitespace() || c == ':' => Some(rest),
        Some(_) => None,
    }
}

/// Parse a group graph pattern from a token stream.
///
/// This is used by the expression parser for EXISTS/NOT EXISTS patterns.
/// Expects the stream to be positioned at the opening `{`.
pub fn parse_group_graph_pattern(
    stream: &mut super::stream::TokenStream,
) -> Result<GraphPattern, String> {
    // Expect opening brace
    if !stream.check(&TokenKind::LBrace) {
        return Err(format!(
            "Expected '{{' at position {}",
            stream.current_span().start
        ));
    }
    stream.advance(); // consume {

    let mut parser = Parser::new(stream);
    parser
        .parse_group_graph_pattern()
        .ok_or_else(|| "Failed to parse group graph pattern".to_string())
}

/// The SPARQL parser.
struct Parser<'a> {
    stream: &'a mut super::stream::TokenStream,
    /// Raw query text, when available. Used for checks that need the
    /// token's original lexeme (e.g. the VERSION specifier must be a
    /// *short* quoted string — the cooked `TokenKind::String` value
    /// cannot distinguish `"1.2"` from `"""1.2"""`). `None` for
    /// sub-parsers spun up without source access (EXISTS groups),
    /// which never parse a prologue.
    source: Option<&'a str>,
    /// Monotonic counter minting unique labels for blank-node property lists
    /// (`[ :p ?o ]`). Each list desugars to a fresh labeled blank node so the
    /// node and its nested triples share one join variable.
    bnode_counter: usize,
    /// Triples produced by a blank-node property list while parsing an
    /// object/subject term. The enclosing object-list / triples-block parser
    /// drains these into its BGP once the term that produced them is placed.
    pending_bnpl_triples: Vec<TriplePattern>,
    /// Non-triple graph patterns produced while parsing an object/subject
    /// term: a property path used as the verb inside a blank-node property
    /// list (`[ :p|:q ?x ]`) emits a `GraphPattern::Path`, which cannot ride
    /// the `pending_bnpl_triples` channel. Drained alongside the triples by
    /// the enclosing triples-block / path-object-list parser; template
    /// contexts (CONSTRUCT, INSERT/DELETE) reject a non-empty channel since
    /// paths are not legal there.
    pending_bnpl_patterns: Vec<GraphPattern>,
}

impl<'a> Parser<'a> {
    fn new(stream: &'a mut super::stream::TokenStream) -> Self {
        Self {
            stream,
            source: None,
            bnode_counter: 0,
            pending_bnpl_triples: Vec::new(),
            pending_bnpl_patterns: Vec::new(),
        }
    }

    fn with_source(stream: &'a mut super::stream::TokenStream, source: &'a str) -> Self {
        Self {
            stream,
            source: Some(source),
            bnode_counter: 0,
            pending_bnpl_triples: Vec::new(),
            pending_bnpl_patterns: Vec::new(),
        }
    }

    /// Parse a complete SPARQL query.
    fn parse_query(&mut self) -> Option<SparqlAst> {
        let start_span = self.stream.current_span();

        // Parse prologue (BASE and PREFIX declarations)
        let prologue = self.parse_prologue();

        // Parse query body
        let body = self.parse_query_body()?;

        // Calculate total span
        let end_span = self.stream.previous_span();
        let span = start_span.union(end_span);

        Some(SparqlAst::new(prologue, body, span))
    }

    /// Parse the prologue (BASE and PREFIX declarations).
    fn parse_prologue(&mut self) -> Prologue {
        let mut prologue = Prologue::new();

        loop {
            if self.stream.check_keyword(TokenKind::KwBase) {
                if let Some(base) = self.parse_base_decl() {
                    prologue = prologue.with_base(base);
                }
            } else if self.stream.check_keyword(TokenKind::KwPrefix) {
                if let Some(prefix) = self.parse_prefix_decl() {
                    prologue = prologue.with_prefix(prefix);
                }
            } else if self.stream.check_keyword(TokenKind::KwVersion) {
                self.parse_version_decl();
            } else {
                break;
            }
        }

        prologue
    }

    /// Parse and discard a SPARQL 1.2 `VERSION "1.2"` declaration.
    ///
    /// Fluree runs the RDF 1.2 / SPARQL 1.2 surface ungated, so the
    /// declaration is purely informational — we lex-and-accept it so a
    /// conformant 1.2 client that emits the mandated `VERSION "1.2"`
    /// pragma parses instead of hard-failing. The version string is not
    /// validated against a specific value (a future version would still
    /// parse).
    fn parse_version_decl(&mut self) {
        self.stream.advance(); // consume VERSION
        match self.stream.consume_string() {
            None => {
                self.stream
                    .error_at_current("expected a version string after VERSION (e.g. \"1.2\")");
            }
            Some((_, span)) => {
                // Grammar: VersionSpecifier ::= STRING_LITERAL1 |
                // STRING_LITERAL2 — long (triple-quoted) strings are not
                // legal here (W3C negative tests version-bad-01/02).
                if let Some(source) = self.source {
                    let raw = source.get(span.start..span.end).unwrap_or("");
                    if raw.starts_with("\"\"\"") || raw.starts_with("'''") {
                        self.stream.error_at(
                            "VERSION requires a short quoted string (e.g. \"1.2\"); \
                             long (triple-quoted) strings are not allowed",
                            span,
                        );
                    }
                }
            }
        }
    }

    /// Parse a BASE declaration.
    fn parse_base_decl(&mut self) -> Option<BaseDecl> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume BASE

        // Expect an IRI
        if let Some((iri, iri_span)) = self.stream.consume_iri() {
            let span = start.union(iri_span);
            Some(BaseDecl::new(iri.as_ref(), span))
        } else {
            self.stream.error_at_current("expected IRI after BASE");
            None
        }
    }

    /// Parse a PREFIX declaration.
    fn parse_prefix_decl(&mut self) -> Option<PrefixDecl> {
        let start = self.stream.current_span();
        self.stream.advance(); // consume PREFIX

        // Expect prefix namespace (e.g., "ex:" or ":")
        let prefix = if let Some((prefix, _)) = self.stream.consume_prefixed_name_ns() {
            prefix
        } else {
            self.stream
                .error_at_current("expected prefix namespace (e.g., 'ex:')");
            return None;
        };

        // Expect an IRI
        if let Some((iri, iri_span)) = self.stream.consume_iri() {
            let span = start.union(iri_span);
            Some(PrefixDecl::new(prefix.as_ref(), iri.as_ref(), span))
        } else {
            self.stream
                .error_at_current("expected IRI after prefix namespace");
            None
        }
    }

    /// Parse the query body (SELECT, CONSTRUCT, ASK, DESCRIBE, or UPDATE).
    fn parse_query_body(&mut self) -> Option<QueryBody> {
        match &self.stream.peek().kind {
            TokenKind::KwSelect => {
                let query = self.parse_select_query()?;
                Some(QueryBody::Select(query))
            }
            TokenKind::KwAsk => {
                let query = self.parse_ask_query()?;
                Some(QueryBody::Ask(query))
            }
            TokenKind::KwDescribe => {
                let query = self.parse_describe_query()?;
                Some(QueryBody::Describe(query))
            }
            TokenKind::KwConstruct => {
                let query = self.parse_construct_query()?;
                Some(QueryBody::Construct(query))
            }
            // SPARQL Update operations
            TokenKind::KwInsert | TokenKind::KwDelete | TokenKind::KwWith => {
                let update = self.parse_update_operation()?;
                Some(QueryBody::Update(update))
            }
            _ => {
                if self.stream.is_eof() {
                    self.stream.error_unexpected_eof("query or update form");
                } else {
                    self.stream
                        .error_at_current("expected query form (SELECT, CONSTRUCT, ASK, DESCRIBE) or update (INSERT, DELETE)");
                }
                None
            }
        }
    }
}

// =========================================================================
// Free helper functions
// =========================================================================

/// Flush accumulated triples into the pattern list as a BGP.
///
/// This is a common pattern used throughout the parser when transitioning
/// from triple accumulation to a keyword-based pattern (OPTIONAL, FILTER, etc.).
fn flush_current_triples(
    current_triples: &mut Vec<TriplePattern>,
    patterns: &mut Vec<GraphPattern>,
) {
    if !current_triples.is_empty() {
        let bgp_span = span_of_triples(current_triples);
        patterns.push(GraphPattern::bgp(std::mem::take(current_triples), bgp_span));
    }
}

/// Calculate the span covering a list of triple patterns.
fn span_of_triples(triples: &[TriplePattern]) -> SourceSpan {
    if triples.is_empty() {
        SourceSpan::point(0)
    } else {
        let first = triples.first().unwrap().span;
        let last = triples.last().unwrap().span;
        first.union(last)
    }
}

/// Combine multiple patterns into a single pattern.
///
/// - If empty, returns an empty BGP
/// - If one pattern, returns it directly
/// - If multiple patterns, wraps them in a Group
fn collect_patterns_into_one(
    patterns: Vec<GraphPattern>,
    fallback_span: SourceSpan,
) -> GraphPattern {
    match patterns.len() {
        0 => GraphPattern::empty_bgp(fallback_span),
        1 => patterns.into_iter().next().unwrap(),
        _ => {
            let span = patterns
                .iter()
                .map(super::super::ast::pattern::GraphPattern::span)
                .reduce(super::super::span::SourceSpan::union)
                .unwrap_or(fallback_span);
            GraphPattern::group(patterns, span)
        }
    }
}
