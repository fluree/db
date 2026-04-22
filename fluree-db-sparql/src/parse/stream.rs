//! Token stream for parsing.
//!
//! The `TokenStream` wraps the lexer output and provides:
//! - Lookahead (peeking) without consuming tokens
//! - Position tracking for error recovery
//! - Convenient matching and consuming methods
//! - Diagnostic collection for errors

use crate::diag::{DiagCode, Diagnostic};
use crate::lex::{Token, TokenKind};
use crate::span::SourceSpan;
use std::sync::Arc;

/// A stream of tokens for parsing.
///
/// Provides lookahead, matching, and error recovery utilities.
#[derive(Debug)]
pub struct TokenStream {
    /// The tokens to parse
    tokens: Vec<Token>,
    /// Current position in the token stream
    pos: usize,
    /// Collected diagnostics
    diagnostics: Vec<Diagnostic>,
}

impl TokenStream {
    /// Create a new token stream from a vector of tokens.
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            diagnostics: Vec::new(),
        }
    }

    /// Get the current position in the stream.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Restore position for backtracking.
    pub fn restore(&mut self, pos: usize) {
        self.pos = pos;
    }

    /// Get collected diagnostics.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// Take the collected diagnostics.
    pub fn take_diagnostics(&mut self) -> Vec<Diagnostic> {
        std::mem::take(&mut self.diagnostics)
    }

    /// Add a diagnostic.
    pub fn add_diagnostic(&mut self, diag: Diagnostic) {
        self.diagnostics.push(diag);
    }

    /// Check if at end of stream (only EOF remains).
    pub fn is_eof(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    /// Peek at the current token without consuming it.
    pub fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or_else(|| {
            self.tokens
                .last()
                .expect("Token stream should have at least EOF")
        })
    }

    /// Peek at the nth token ahead (0 = current).
    pub fn peek_n(&self, n: usize) -> &Token {
        self.tokens.get(self.pos + n).unwrap_or_else(|| {
            self.tokens
                .last()
                .expect("Token stream should have at least EOF")
        })
    }

    /// Get the span of the current token.
    pub fn current_span(&self) -> SourceSpan {
        self.peek().span
    }

    /// Get the span of the previous token (for error recovery).
    pub fn previous_span(&self) -> SourceSpan {
        if self.pos > 0 {
            self.tokens[self.pos - 1].span
        } else {
            SourceSpan::point(0)
        }
    }

    /// Advance to the next token, returning the current one.
    pub fn advance(&mut self) -> &Token {
        let token = self.peek();
        if !matches!(token.kind, TokenKind::Eof) {
            self.pos += 1;
        }
        &self.tokens[self.pos.saturating_sub(1).min(self.tokens.len() - 1)]
    }

    /// Consume the current token and return it (owned).
    pub fn consume(&mut self) -> Token {
        self.advance().clone()
    }

    /// Check if the current token matches the expected kind.
    pub fn check(&self, kind: &TokenKind) -> bool {
        std::mem::discriminant(&self.peek().kind) == std::mem::discriminant(kind)
    }

    /// Check if the current token is a specific keyword.
    pub fn check_keyword(&self, kw: TokenKind) -> bool {
        self.peek().kind == kw
    }

    /// Consume the current token if it matches, returning true.
    pub fn match_token(&mut self, kind: &TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Consume the current token if it's the expected keyword.
    pub fn match_keyword(&mut self, kw: TokenKind) -> bool {
        if self.check_keyword(kw) {
            self.advance();
            true
        } else {
            false
        }
    }

    /// Expect and consume a specific token kind, or emit an error.
    ///
    /// Returns the token if matched, or None if error.
    pub fn expect(&mut self, kind: &TokenKind, message: &str) -> Option<Token> {
        if self.check(kind) {
            Some(self.consume())
        } else {
            self.error_at_current(message);
            None
        }
    }

    /// Expect and consume a specific keyword, or emit an error.
    pub fn expect_keyword(&mut self, kw: TokenKind, name: &str) -> Option<Token> {
        if self.check_keyword(kw) {
            Some(self.consume())
        } else {
            self.error_at_current(&format!("expected '{name}'"));
            None
        }
    }

    /// Add an error at the current token position.
    pub fn error_at_current(&mut self, message: &str) {
        let span = self.current_span();
        self.add_diagnostic(Diagnostic::error(
            DiagCode::ExpectedToken,
            message.to_string(),
            span,
        ));
    }

    /// Add an error at a specific span.
    pub fn error_at(&mut self, message: &str, span: SourceSpan) {
        self.add_diagnostic(Diagnostic::error(
            DiagCode::ExpectedToken,
            message.to_string(),
            span,
        ));
    }

    /// Add an error for unexpected end of file.
    pub fn error_unexpected_eof(&mut self, expected: &str) {
        let span = self.current_span();
        self.add_diagnostic(
            Diagnostic::error(
                DiagCode::UnexpectedEof,
                format!("unexpected end of input, expected {expected}"),
                span,
            )
            .with_help("The query appears to be incomplete."),
        );
    }

    /// Skip tokens until we find one of the recovery points.
    ///
    /// Used for error recovery to resync the parser.
    pub fn synchronize(&mut self, recovery_tokens: &[TokenKind]) {
        while !self.is_eof() {
            let current = &self.peek().kind;
            for recovery in recovery_tokens {
                if std::mem::discriminant(current) == std::mem::discriminant(recovery) {
                    return;
                }
            }
            self.advance();
        }
    }

    /// Try to parse something, restoring position on failure.
    ///
    /// Returns `Some(result)` if the parser succeeds, `None` if it fails.
    /// On failure, the stream position is restored to before the attempt.
    pub fn try_parse<T, F>(&mut self, f: F) -> Option<T>
    where
        F: FnOnce(&mut Self) -> Option<T>,
    {
        let start_pos = self.pos;
        let start_diag_count = self.diagnostics.len();

        match f(self) {
            Some(result) => Some(result),
            None => {
                // Restore position and remove any diagnostics added during the attempt
                self.pos = start_pos;
                self.diagnostics.truncate(start_diag_count);
                None
            }
        }
    }

    // =========================================================================
    // Convenience methods for common token patterns
    // =========================================================================

    /// Consume and return a variable name if the current token is a variable.
    pub fn consume_var(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::Var(_) => {
                let token = self.consume();
                if let TokenKind::Var(name) = token.kind {
                    Some((name, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return an IRI if the current token is an IRI.
    pub fn consume_iri(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::Iri(_) => {
                let token = self.consume();
                if let TokenKind::Iri(iri) = token.kind {
                    Some((iri, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a prefixed name if the current token is one.
    pub fn consume_prefixed_name(&mut self) -> Option<(Arc<str>, Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::PrefixedName { .. } => {
                let token = self.consume();
                if let TokenKind::PrefixedName { prefix, local } = token.kind {
                    Some((prefix, local, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a prefixed name namespace if the current token is one.
    pub fn consume_prefixed_name_ns(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::PrefixedNameNs(_) => {
                let token = self.consume();
                if let TokenKind::PrefixedNameNs(prefix) = token.kind {
                    Some((prefix, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return an integer if the current token is one.
    pub fn consume_integer(&mut self) -> Option<(i64, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::Integer(_) => {
                let token = self.consume();
                if let TokenKind::Integer(n) = token.kind {
                    Some((n, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a string if the current token is one.
    pub fn consume_string(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::String(_) => {
                let token = self.consume();
                if let TokenKind::String(s) = token.kind {
                    Some((s, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a decimal if the current token is one.
    pub fn consume_decimal(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::Decimal(_) => {
                let token = self.consume();
                if let TokenKind::Decimal(s) = token.kind {
                    Some((s, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a double if the current token is one.
    pub fn consume_double(&mut self) -> Option<(f64, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::Double(_) => {
                let token = self.consume();
                if let TokenKind::Double(n) = token.kind {
                    Some((n, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Consume and return a language tag if the current token is one.
    pub fn consume_lang_tag(&mut self) -> Option<(Arc<str>, SourceSpan)> {
        match &self.peek().kind {
            TokenKind::LangTag(_) => {
                let token = self.consume();
                if let TokenKind::LangTag(s) = token.kind {
                    Some((s, token.span))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Check if current token is a keyword that can start a query form.
    pub fn is_query_form_start(&self) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::KwSelect | TokenKind::KwConstruct | TokenKind::KwDescribe | TokenKind::KwAsk
        )
    }

    /// Skip tokens until a balanced closing delimiter is found.
    ///
    /// Assumes the opening delimiter has already been consumed (depth starts
    /// at 1). Tracks nesting and advances past the matching closing delimiter.
    /// No-ops at EOF without panicking.
    pub fn skip_balanced(&mut self, open: &TokenKind, close: &TokenKind) {
        let mut depth = 1u32;
        while depth > 0 && !self.is_eof() {
            if self.check(open) {
                depth += 1;
            } else if self.check(close) {
                depth -= 1;
            }
            self.advance();
        }
    }

    /// Check if current token can start a term (subject, predicate, or object).
    pub fn is_term_start(&self) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::Var(_)
                | TokenKind::Iri(_)
                | TokenKind::PrefixedName { .. }
                | TokenKind::PrefixedNameNs(_)
                | TokenKind::String(_)
                | TokenKind::Integer(_)
                | TokenKind::Decimal(_)
                | TokenKind::Double(_)
                | TokenKind::BlankNodeLabel(_)
                | TokenKind::Anon
                | TokenKind::KwTrue
                | TokenKind::KwFalse
                | TokenKind::KwA
                | TokenKind::LBracket  // Property list syntax
                | TokenKind::LParen    // Collection syntax (non-empty)
                | TokenKind::Nil       // Collection syntax (empty list)
                | TokenKind::TripleStart // RDF-star quoted triple
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lex::tokenize;

    fn stream_from(source: &str) -> TokenStream {
        TokenStream::new(tokenize(source))
    }

    #[test]
    fn test_peek_and_advance() {
        let mut stream = stream_from("SELECT ?x");

        assert!(matches!(stream.peek().kind, TokenKind::KwSelect));
        stream.advance();
        assert!(matches!(stream.peek().kind, TokenKind::Var(_)));
        stream.advance();
        assert!(stream.is_eof());
    }

    #[test]
    fn test_check_and_match() {
        let mut stream = stream_from("SELECT ?x");

        assert!(stream.check_keyword(TokenKind::KwSelect));
        assert!(!stream.check_keyword(TokenKind::KwWhere));

        assert!(stream.match_keyword(TokenKind::KwSelect));
        assert!(!stream.match_keyword(TokenKind::KwSelect)); // Already consumed
    }

    #[test]
    fn test_consume_var() {
        let mut stream = stream_from("?name");

        let (name, span) = stream.consume_var().expect("should consume var");
        assert_eq!(name.as_ref(), "name");
        assert_eq!(span, SourceSpan::new(0, 5));
    }

    #[test]
    fn test_consume_iri() {
        let mut stream = stream_from("<http://example.org/>");

        let (iri, _span) = stream.consume_iri().expect("should consume IRI");
        assert_eq!(iri.as_ref(), "http://example.org/");
    }

    #[test]
    fn test_try_parse_success() {
        let mut stream = stream_from("SELECT ?x");

        let result = stream.try_parse(|s| {
            if s.match_keyword(TokenKind::KwSelect) {
                Some("found SELECT")
            } else {
                None
            }
        });

        assert_eq!(result, Some("found SELECT"));
        // Position should be after SELECT
        assert!(matches!(stream.peek().kind, TokenKind::Var(_)));
    }

    #[test]
    fn test_try_parse_failure() {
        let mut stream = stream_from("SELECT ?x");

        let result = stream.try_parse(|s| {
            if s.match_keyword(TokenKind::KwWhere) {
                Some("found WHERE")
            } else {
                None
            }
        });

        assert!(result.is_none());
        // Position should be restored
        assert!(matches!(stream.peek().kind, TokenKind::KwSelect));
    }

    #[test]
    fn test_expect_success() {
        let mut stream = stream_from("SELECT ?x");

        let token = stream.expect_keyword(TokenKind::KwSelect, "SELECT");
        assert!(token.is_some());
        assert!(stream.diagnostics().is_empty());
    }

    #[test]
    fn test_expect_failure() {
        let mut stream = stream_from("SELECT ?x");

        let token = stream.expect_keyword(TokenKind::KwWhere, "WHERE");
        assert!(token.is_none());
        assert_eq!(stream.diagnostics().len(), 1);
    }

    #[test]
    fn test_synchronize() {
        let mut stream = stream_from("garbage tokens WHERE { }");

        // Skip to WHERE
        stream.synchronize(&[TokenKind::KwWhere, TokenKind::LBrace]);
        assert!(stream.check_keyword(TokenKind::KwWhere));
    }

    #[test]
    fn test_peek_n() {
        let stream = stream_from("SELECT ?x WHERE");

        assert!(matches!(stream.peek_n(0).kind, TokenKind::KwSelect));
        assert!(matches!(stream.peek_n(1).kind, TokenKind::Var(_)));
        assert!(matches!(stream.peek_n(2).kind, TokenKind::KwWhere));
    }

    #[test]
    fn test_is_term_start() {
        let stream = stream_from("?x");
        assert!(stream.is_term_start());

        let stream = stream_from("<http://example.org>");
        assert!(stream.is_term_start());

        let stream = stream_from("ex:foo");
        assert!(stream.is_term_start());

        let stream = stream_from("WHERE");
        assert!(!stream.is_term_start());
    }
}
