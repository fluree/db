//! Turtle lexer module.
//!
//! Tokenizes Turtle input using winnow.

/// Turtle character-class predicates.
///
/// These live in `fluree-graph-ir` so that the parser and the writers share
/// one transcription of the grammar's character productions rather than two
/// that can drift. Re-exported here because `lex::chars` is the path this
/// crate's own code and callers already use.
pub use fluree_graph_ir::chars;
pub mod lexer;
pub mod token;

pub use lexer::{tokenize, Lexer, StreamingLexer};
pub use token::{Token, TokenKind};
