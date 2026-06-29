//! Cypher lexer (winnow-based, hand-written).

mod lexer;
mod token;

pub use lexer::{tokenize, LexError};
pub use token::{Token, TokenKind};
