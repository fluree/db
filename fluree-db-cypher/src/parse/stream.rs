//! Token stream wrapper with peek/eat/expect helpers.

use crate::diag::{DiagCode, Diagnostic, Severity};
use crate::lex::{Token, TokenKind};
use crate::span::SourceSpan;

pub struct TokenStream {
    tokens: Vec<Token>,
    pos: usize,
}

impl TokenStream {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    pub fn peek(&self) -> &Token {
        &self.tokens[self.pos]
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
                format!(
                    "expected `{}` but got `{}`",
                    expected,
                    self.peek_kind()
                ),
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

    pub fn error_at(&self, code: DiagCode, message: impl Into<String>, span: SourceSpan) -> Diagnostic {
        Diagnostic {
            code,
            severity: Severity::Error,
            message: message.into(),
            span,
            help: None,
        }
    }
}
