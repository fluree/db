//! Error types for storage encryption operations.
//!
//! These errors are designed to:
//! - Provide useful debugging information without leaking secrets
//! - Support cause chains when mapped to higher-level error types
//! - Distinguish between different failure modes (format, key, crypto)

use thiserror::Error;

/// Result type alias for encryption operations.
pub type Result<T> = std::result::Result<T, EncryptionError>;

/// Errors that can occur during encryption/decryption operations.
///
/// Note: Error messages intentionally avoid including sensitive data
/// like key material or plaintext content.
#[derive(Error, Debug)]
pub enum EncryptionError {
    /// The ciphertext envelope format is invalid.
    ///
    /// This occurs when:
    /// - Data is too short to contain a valid header
    /// - Magic bytes don't match (data may not be encrypted)
    /// - Unsupported version or algorithm
    #[error("Invalid encryption format: {context}")]
    InvalidFormat {
        /// Description of what was wrong with the format.
        context: &'static str,
    },

    /// The key ID in the ciphertext doesn't match any known key.
    ///
    /// This typically means:
    /// - The data was encrypted with a rotated-out key
    /// - The wrong key provider is configured
    #[error("Unknown encryption key ID: {key_id}")]
    UnknownKeyId {
        /// The key ID found in the ciphertext header.
        key_id: u32,
    },

    /// Encryption operation failed.
    ///
    /// This is rare with AES-GCM and typically indicates
    /// a programming error or resource exhaustion.
    #[error("Encryption failed: {context}")]
    EncryptFailed {
        /// Description of what went wrong.
        context: &'static str,
    },

    /// Decryption operation failed.
    ///
    /// This occurs when:
    /// - Wrong key is used
    /// - Ciphertext has been tampered with or corrupted
    /// - Authentication tag verification failed
    #[error("Decryption failed: {context}")]
    DecryptFailed {
        /// Description of what went wrong.
        context: &'static str,
    },

    /// Invalid key material provided.
    ///
    /// This occurs when:
    /// - Key is not exactly 32 bytes
    /// - Base64 decoding fails
    #[error("Invalid key: {context}")]
    InvalidKey {
        /// Description of what was wrong with the key.
        context: &'static str,
    },
}

impl EncryptionError {
    /// Create an invalid format error.
    pub fn invalid_format(context: &'static str) -> Self {
        Self::InvalidFormat { context }
    }

    /// Create an unknown key ID error.
    pub fn unknown_key_id(key_id: u32) -> Self {
        Self::UnknownKeyId { key_id }
    }

    /// Create an encryption failed error.
    pub fn encrypt_failed(context: &'static str) -> Self {
        Self::EncryptFailed { context }
    }

    /// Create a decryption failed error.
    pub fn decrypt_failed(context: &'static str) -> Self {
        Self::DecryptFailed { context }
    }

    /// Create an invalid key error.
    pub fn invalid_key(context: &'static str) -> Self {
        Self::InvalidKey { context }
    }
}

// Conversion to fluree-db-core error type
impl From<EncryptionError> for fluree_db_core::error::Error {
    fn from(err: EncryptionError) -> Self {
        fluree_db_core::error::Error::storage(err.to_string())
    }
}
