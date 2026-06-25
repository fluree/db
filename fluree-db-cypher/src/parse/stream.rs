//! Token stream wrapper with peek/eat/expect helpers.

use crate::diag::{DiagCode, Diagnostic, Severity};
use crate::lex::{Token, TokenKind};
use crate::span::SourceSpan;

/// Maximum parser recursion depth. The recursive-descent parser (and the
/// AST walkers that later recurse over its output) descend one frame per level
/// of expression/statement nesting; without a bound, hostile input such as
/// `RETURN ((((…50k…))))` overflows the thread stack, which aborts the whole
/// process (a Rust stack overflow is not catchable). Capping the AST depth here
/// also bounds the depth of every downstream walker (lowering, param
/// substitution). 256 is far beyond any real query yet leaves ample stack
/// headroom even with the ~12-frame precedence chain per level.
const MAX_PARSE_DEPTH: u32 = 256;

pub struct TokenStream {
    tokens: Vec<Token>,
    pos: usize,
    depth: u32,
}

impl TokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            pos: 0,
            depth: 0,
        }
    }

    /// Enter one level of recursion, erroring (rather than overflowing the
    /// stack) past [`MAX_PARSE_DEPTH`]. Every successful `enter_recursion` MUST
    /// be paired with a [`Self::leave_recursion`] on the success path; the
    /// counter is incremented only when the limit is not exceeded, so an error
    /// return (which aborts the whole parse) need not unwind it.
    pub fn enter_recursion(&mut self) -> Result<(), Diagnostic> {
        if self.depth >= MAX_PARSE_DEPTH {
            return Err(self.error(
                DiagCode::NestingTooDeep,
                format!("query nesting exceeds the maximum depth of {MAX_PARSE_DEPTH}"),
            ));
        }
        self.depth += 1;
        Ok(())
    }

    /// Leave one level of recursion. Pairs with [`Self::enter_recursion`].
    pub fn leave_recursion(&mut self) {
        self.depth = self.depth.saturating_sub(1);
    }

    pub fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    /// Snapshot the current position for speculative parsing.
    pub fn mark(&self) -> usize {
        self.pos
    }

    /// Restore a position captured by [`Self::mark`] (backtracking).
    pub fn reset(&mut self, mark: usize) {
        self.pos = mark;
    }

    pub fn peek_kind(&self) -> &TokenKind {
        &self.tokens[self.pos].kind
    }

    pub fn peek_at(&self, offset: usize) -> &TokenKind {
        let idx = self.pos + offset;
        if idx < self.tokens.len() {
            &self.tokens[idx].kind
        } else {
            &self.tokens[self.tokens.len() - 1].kind
        }
    }

    pub fn peek_span(&self) -> SourceSpan {
        self.tokens[self.pos].span
    }

    pub fn is_eof(&self) -> bool {
        matches!(self.peek_kind(), TokenKind::Eof)
    }

    pub fn advance(&mut self) -> Token {
        let tok = self.tokens[self.pos].clone();
        if self.pos < self.tokens.len() - 1 {
            self.pos += 1;
        }
        tok
    }

    /// If the current token's kind matches `expected` (by discriminant),
    /// consume and return its span. Otherwise return None.
    pub fn eat(&mut self, expected: &TokenKind) -> Option<SourceSpan> {
        if std::mem::discriminant(self.peek_kind()) == std::mem::discriminant(expected) {
            let span = self.peek_span();
            self.advance();
            Some(span)
        } else {
            None
        }
    }

    pub fn expect(&mut self, expected: &TokenKind) -> Result<SourceSpan, Diagnostic> {
        if let Some(s) = self.eat(expected) {
            Ok(s)
        } else {
            Err(self.error(
                DiagCode::UnexpectedToken,
                format!("expected `{}` but got `{}`", expected, self.peek_kind()),
            ))
        }
    }

    pub fn error(&self, code: DiagCode, message: impl Into<String>) -> Diagnostic {
        Diagnostic {
            code,
            severity: Severity::Error,
            message: message.into(),
            span: self.peek_span(),
            help: None,
        }
    }

    pub fn error_at(
        &self,
        code: DiagCode,
        message: impl Into<String>,
        span: SourceSpan,
    ) -> Diagnostic {
        Diagnostic {
            code,
            severity: Severity::Error,
            message: message.into(),
            span,
            help: None,
        }
    }
}
