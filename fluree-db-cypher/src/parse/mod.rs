//! Cypher parser — tokens → AST.

mod expr;
mod pattern;
mod stmt;
mod stream;

use crate::ast::CypherAst;
use crate::diag::{DiagCode, Diagnostic, ParseOutput, Severity};
use crate::lex::tokenize;
use crate::span::SourceSpan;

pub use stream::TokenStream;

/// Parse a Cypher source string into a `ParseOutput` containing an
/// optional AST and a list of diagnostics.
pub fn parse_cypher(src: &str) -> ParseOutput {
    let tokens = match tokenize(src) {
        Ok(t) => t,
        Err(e) => {
            let span = e.span();
            let diag = Diagnostic {
                code: DiagCode::UnexpectedToken,
                severity: Severity::Error,
                message: e.to_string(),
                span,
                help: None,
            };
            return ParseOutput {
                ast: None,
                diagnostics: vec![diag],
            };
        }
    };

    let mut stream = TokenStream::new(tokens);
    let mut diagnostics = Vec::new();

    match stmt::parse_statement(&mut stream) {
        Ok(stmt) => {
            // Reject trailing tokens.
            if !stream.is_eof() {
                let span = stream.peek_span();
                diagnostics.push(Diagnostic {
                    code: DiagCode::UnexpectedToken,
                    severity: Severity::Error,
                    message: format!(
                        "unexpected token after end of statement: {}",
                        stream.peek_kind()
                    ),
                    span,
                    help: Some(
                        "multi-statement scripts (semicolon-separated) are deferred; submit one statement per request"
                            .to_string(),
                    ),
                });
                ParseOutput {
                    ast: None,
                    diagnostics,
                }
            } else {
                let span = SourceSpan::new(0, src.len());
                ParseOutput {
                    ast: Some(CypherAst {
                        statement: stmt,
                        span,
                    }),
                    diagnostics,
                }
            }
        }
        Err(d) => {
            diagnostics.push(d);
            ParseOutput {
                ast: None,
                diagnostics,
            }
        }
    }
}
