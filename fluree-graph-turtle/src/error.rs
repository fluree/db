//! Error types for Turtle parsing

/// Error type for Turtle parsing operations
#[derive(Debug, thiserror::Error)]
pub enum TurtleError {
    /// Lexer error (invalid token)
    #[error("Lexer error at position {position}: {message}")]
    Lexer { position: usize, message: String },

    /// Parser error (unexpected token or invalid structure)
    #[error("Parse error at position {position}: {message}")]
    Parse { position: usize, message: String },

    /// IRI resolution error (relative IRI without base)
    #[error("IRI resolution error: {0}")]
    IriResolution(String),

    /// Prefix not defined
    #[error("Undefined prefix: {0}")]
    UndefinedPrefix(String),

    /// Invalid escape sequence
    #[error("Invalid escape sequence: {0}")]
    InvalidEscape(String),
}

/// Result type for Turtle operations
pub type Result<T> = std::result::Result<T, TurtleError>;

impl TurtleError {
    /// Create a lexer error
    pub fn lexer(position: usize, message: impl Into<String>) -> Self {
        Self::Lexer {
            position,
            message: message.into(),
        }
    }

    /// Create a parse error
    pub fn parse(position: usize, message: impl Into<String>) -> Self {
        Self::Parse {
            position,
            message: message.into(),
        }
    }
}
