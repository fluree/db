//! Turtle lexer module.
//!
//! Tokenizes Turtle input using winnow.

pub mod chars;
pub mod lexer;
pub mod token;

pub use lexer::{tokenize, Lexer, StreamingLexer};
pub use token::{Token, TokenKind};
