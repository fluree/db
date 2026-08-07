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

pub use lexer::{tokenize, tokenize_with_comments, Lexer};
pub use token::{keyword_from_str, Token, TokenKind};

/// This crate's transcription of the `IRIREF` exclusion set.
///
/// Exported for ONE reason: it is the fourth independent copy of that set in
/// this workspace, and the differential that binds the other three lives in a
/// light crate which cannot depend on the SPARQL engine. The binding test sits
/// in `fluree-db-cli`, which already depends on both sides, and needs to name
/// this function to compare it. Re-exported rather than made a public module so
/// the widening is exactly one item wide.
pub use chars::is_iri_char;
