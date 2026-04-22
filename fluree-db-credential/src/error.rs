//! Error types for credential verification

use thiserror::Error;

/// Error type for credential verification operations
#[derive(Error, Debug)]
pub enum CredentialError {
    /// Invalid JWS format (not 3 dot-separated parts)
    #[error("Invalid JWS format: {0}")]
    InvalidJwsFormat(String),

    /// Invalid JWS header (malformed JSON or missing fields)
    #[error("Invalid JWS header: {0}")]
    InvalidJwsHeader(String),

    /// Unsupported signing algorithm (only EdDSA supported)
    #[error("Unsupported algorithm: {0}. Expected EdDSA")]
    UnsupportedAlgorithm(String),

    /// Ed25519 signature verification failed
    #[error("Invalid signature: {0}")]
    InvalidSignature(String),

    /// Could not extract identity from credential
    #[error("Could not verify identity: {0}")]
    NoIdentity(String),

    /// Invalid DID format
    #[error("Invalid DID format: {0}")]
    InvalidDid(String),

    /// Invalid public key format or length
    #[error("Invalid public key: {0}")]
    InvalidPublicKey(String),

    /// Base64 decoding error
    #[error("Base64 decode error: {0}")]
    Base64Decode(String),

    /// Base58 decoding error
    #[error("Base58 decode error: {0}")]
    Base58Decode(String),

    /// JSON parsing error
    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    /// Missing required field in credential
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// VC verification not enabled (requires "vc" feature)
    #[error("VerifiableCredential verification not enabled (requires 'vc' feature)")]
    VcNotEnabled,

    /// OIDC JWT verification error (JWKS path)
    #[error("JWT error: {0}")]
    JwtError(String),

    /// Key not found in JWKS for the given kid
    #[error("Key not found in JWKS: kid={0}")]
    KeyNotFound(String),
}

impl CredentialError {
    /// HTTP status code for this error
    ///
    /// - 400: Invalid format, signature, algorithm, etc.
    /// - 401: Could not verify identity
    pub fn status_code(&self) -> u16 {
        match self {
            Self::NoIdentity(_) => 401,
            _ => 400,
        }
    }
}

/// Result type alias for credential operations
pub type Result<T> = std::result::Result<T, CredentialError>;
