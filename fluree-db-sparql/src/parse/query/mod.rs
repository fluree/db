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
    BaseDecl, GraphPattern, PrefixDecl, Prologue, QueryBody, SparqlAst, TriplePattern,
};
use crate::diag::{DiagCode, Diagnostic, ParseOutput};
use crate::lex::{tokenize, TokenKind};
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
    let tokens = tokenize(input);

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
    let mut parser = Parser::new(&mut stream);

    match parser.parse_query() {
        Some(ast) => ParseOutput::with_diagnostics(Some(ast), stream.take_diagnostics()),
        None => ParseOutput::with_diagnostics(None, stream.take_diagnostics()),
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
}

impl<'a> Parser<'a> {
    fn new(stream: &'a mut super::stream::TokenStream) -> Self {
        Self { stream }
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
            } else {
                break;
            }
        }

        prologue
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
