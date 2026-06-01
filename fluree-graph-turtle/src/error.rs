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

/// Maximum input size, in bytes, that the lexer/parser can address in a single
/// call. Token spans are stored as `u32` byte offsets (see [`crate::Token`]),
/// so any `&str` handed to the tokenizer must fit in `u32::MAX` bytes.
///
/// Bulk import bounds every chunk far below this; the guard exists only to turn
/// an oversized whole-file input into a clean error instead of a silently
/// wrapped offset that would later panic when slicing the source.
pub const MAX_INPUT_BYTES: usize = u32::MAX as usize;

/// Reject input too large for `u32` token-span offsets.
///
/// Called at every lexer/parser entry point. Compares via `u64` so the check is
/// well-formed (and lint-clean) on 32-bit targets where `usize == u32`.
pub fn check_input_len(len: usize) -> Result<()> {
    if len as u64 > MAX_INPUT_BYTES as u64 {
        return Err(TurtleError::Lexer {
            position: MAX_INPUT_BYTES,
            message: format!(
                "input is {len} bytes, exceeding the {MAX_INPUT_BYTES}-byte limit \
                 (token spans are u32); split the input into smaller chunks before parsing"
            ),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_input_up_to_the_u32_limit() {
        assert!(check_input_len(0).is_ok());
        assert!(check_input_len(1024).is_ok());
        assert!(check_input_len(MAX_INPUT_BYTES).is_ok());
    }

    #[test]
    fn rejects_input_past_the_u32_limit() {
        // One byte over the limit would overflow a u32 offset — must error,
        // not silently wrap (the >4 GiB whole-file import panic). Checked via a
        // fabricated length so the test allocates nothing.
        let err = check_input_len(MAX_INPUT_BYTES + 1).unwrap_err();
        assert!(matches!(err, TurtleError::Lexer { .. }));
        assert!(err.to_string().contains("exceeding"));
    }
}
