//! SPARQL Lexical Analysis.
//!
//! This module handles tokenization of SPARQL queries, producing a stream
//! of tokens with source spans. The parser then consumes these tokens.
//!
//! ## Design
//!
//! SPARQL lexing is non-trivial due to:
//! - Comments (single-line `#` style)
//! - String escaping (single/double quotes, long strings)
//! - IRIs (absolute and relative, with escapes)
//! - Prefixed names (PN_CHARS rules, namespace:local)
//! - Keyword vs. prefix ambiguity (`a` is both keyword and valid prefix)
//! - Numeric formats (integer, decimal, double, exponent notation)
//!
//! ## Implementation
//!
//! Uses winnow for all tokenization. The lexer produces `Token` values
//! with source spans for precise diagnostic locations.
//!
//! ## Usage
//!
//! ```
//! use fluree_db_sparql::lex::tokenize;
//!
//! let tokens = tokenize("SELECT ?x WHERE { ?x a :Person }");
//! for token in tokens {
//!     println!("{:?} at {:?}", token.kind, token.span);
//! }
//! ```

mod chars;
mod lexer;
mod token;

pub use lexer::{tokenize, Lexer};
pub use token::{keyword_from_str, Token, TokenKind};
