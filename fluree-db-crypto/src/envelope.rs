//! Ciphertext envelope format for Fluree encrypted storage.
//!
//! ## Envelope Layout
//!
//! ```text
//! ┌───────────┬─────────┬────────┬────────┬───────────┬────────────────────────┐
//! │   Magic   │ Version │  Alg   │ Key ID │   Nonce   │   Ciphertext + Tag     │
//! │  4 bytes  │ 1 byte  │ 1 byte │ 4 bytes│  12 bytes │       N bytes          │
//! └───────────┴─────────┴────────┴────────┴───────────┴────────────────────────┘
//!          ↑________________________↑
//!                     AAD (authenticated but not encrypted)
//! ```
//!
//! - **Magic**: `FLU\0` - identifies this as a Fluree encrypted object
//! - **Version**: Envelope format version (currently `0x01`)
//! - **Alg**: Algorithm ID (`0x01` = AES-256-GCM)
//! - **Key ID**: 4-byte identifier for key rotation support
//! - **Nonce**: 12-byte random nonce (unique per encryption)
//! - **Ciphertext + Tag**: AES-GCM output (plaintext length + 16 bytes auth tag)
//!
//! The entire header (22 bytes) is used as AAD (Additional Authenticated Data),
//! ensuring the header cannot be tampered with without detection.

use crate::error::{EncryptionError, Result};

// ============================================================================
// Constants
// ============================================================================

/// Magic bytes identifying a Fluree encrypted object.
pub const MAGIC: &[u8; 4] = b"FLU\x00";

/// Current envelope format version.
pub const VERSION: u8 = 0x01;

/// Algorithm ID for AES-256-GCM.
pub const ALG_AES256_GCM: u8 = 0x01;

/// Size of the nonce (IV) for AES-GCM.
pub const NONCE_LEN: usize = 12;

/// Size of the authentication tag for AES-GCM.
pub const TAG_LEN: usize = 16;

/// Required key size for AES-256.
pub const KEY_LEN: usize = 32;

// Header field offsets and sizes
const MAGIC_OFFSET: usize = 0;
const MAGIC_LEN: usize = 4;
const VERSION_OFFSET: usize = MAGIC_OFFSET + MAGIC_LEN; // 4
const VERSION_LEN: usize = 1;
const ALG_OFFSET: usize = VERSION_OFFSET + VERSION_LEN; // 5
const ALG_LEN: usize = 1;
const KEY_ID_OFFSET: usize = ALG_OFFSET + ALG_LEN; // 6
const KEY_ID_LEN: usize = 4;
const NONCE_OFFSET: usize = KEY_ID_OFFSET + KEY_ID_LEN; // 10
                                                        // NONCE_LEN = 12

/// Total header length (magic + version + alg + key_id + nonce).
pub const HEADER_LEN: usize = NONCE_OFFSET + NONCE_LEN; // 22

/// Minimum valid envelope size (header + auth tag, with empty plaintext).
pub const MIN_ENVELOPE_LEN: usize = HEADER_LEN + TAG_LEN; // 38

// ============================================================================
// Header Construction
// ============================================================================

/// Build the envelope header for encryption.
///
/// The header is used both as the prefix of the ciphertext envelope
/// and as the AAD for authenticated encryption.
pub fn build_header(key_id: u32, nonce: &[u8; NONCE_LEN]) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];

    // Magic bytes
    header[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LEN].copy_from_slice(MAGIC);

    // Version
    header[VERSION_OFFSET] = VERSION;

    // Algorithm
    header[ALG_OFFSET] = ALG_AES256_GCM;

    // Key ID (little-endian)
    header[KEY_ID_OFFSET..KEY_ID_OFFSET + KEY_ID_LEN].copy_from_slice(&key_id.to_le_bytes());

    // Nonce
    header[NONCE_OFFSET..NONCE_OFFSET + NONCE_LEN].copy_from_slice(nonce);

    header
}

// ============================================================================
// Header Parsing
// ============================================================================

/// Parsed header from an encrypted envelope.
#[derive(Debug, Clone)]
pub struct ParsedHeader {
    /// Key ID used for encryption.
    pub key_id: u32,
    /// Nonce used for encryption.
    pub nonce: [u8; NONCE_LEN],
}

/// Parse and validate the header from an encrypted envelope.
///
/// Returns the parsed header fields and validates:
/// - Magic bytes match
/// - Version is supported
/// - Algorithm is supported
///
/// # Errors
///
/// Returns `EncryptionError::InvalidFormat` if:
/// - Envelope is too short
/// - Magic bytes don't match (data may not be encrypted)
/// - Version is unsupported
/// - Algorithm is unsupported
pub fn parse_header(envelope: &[u8]) -> Result<ParsedHeader> {
    // Check minimum length
    if envelope.len() < MIN_ENVELOPE_LEN {
        return Err(EncryptionError::invalid_format(
            "ciphertext too short for valid envelope",
        ));
    }

    // Validate magic bytes
    if &envelope[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LEN] != MAGIC {
        return Err(EncryptionError::invalid_format(
            "not a Fluree encrypted object (magic mismatch)",
        ));
    }

    // Validate version
    let version = envelope[VERSION_OFFSET];
    if version != VERSION {
        return Err(EncryptionError::invalid_format(
            "unsupported envelope version",
        ));
    }

    // Validate algorithm
    let alg = envelope[ALG_OFFSET];
    if alg != ALG_AES256_GCM {
        return Err(EncryptionError::invalid_format(
            "unsupported encryption algorithm",
        ));
    }

    // Extract key ID
    let key_id_bytes: [u8; KEY_ID_LEN] = envelope[KEY_ID_OFFSET..KEY_ID_OFFSET + KEY_ID_LEN]
        .try_into()
        .expect("slice length verified");
    let key_id = u32::from_le_bytes(key_id_bytes);

    // Extract nonce
    let nonce: [u8; NONCE_LEN] = envelope[NONCE_OFFSET..NONCE_OFFSET + NONCE_LEN]
        .try_into()
        .expect("slice length verified");

    Ok(ParsedHeader { key_id, nonce })
}

/// Extract the ciphertext portion (after header) from an envelope.
///
/// # Panics
///
/// Panics if the envelope is shorter than `HEADER_LEN`. Always call
/// `parse_header` first to validate the envelope.
pub fn ciphertext_slice(envelope: &[u8]) -> &[u8] {
    &envelope[HEADER_LEN..]
}

/// Extract the header portion (for AAD) from an envelope.
///
/// # Panics
///
/// Panics if the envelope is shorter than `HEADER_LEN`. Always call
/// `parse_header` first to validate the envelope.
pub fn header_slice(envelope: &[u8]) -> &[u8] {
    &envelope[..HEADER_LEN]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let key_id = 42u32;
        let nonce = [1u8; NONCE_LEN];

        let header = build_header(key_id, &nonce);

        // Verify we can parse what we built
        // Create a minimal "envelope" by adding fake ciphertext
        let mut envelope = header.to_vec();
        envelope.extend_from_slice(&[0u8; TAG_LEN]); // Minimal ciphertext (just tag)

        let parsed = parse_header(&envelope).unwrap();
        assert_eq!(parsed.key_id, key_id);
        assert_eq!(parsed.nonce, nonce);
    }

    #[test]
    fn test_header_constants() {
        // Verify our offset math is correct
        assert_eq!(HEADER_LEN, 22);
        assert_eq!(MIN_ENVELOPE_LEN, 38);

        // Build a header and verify layout
        let header = build_header(0x1234_5678, &[0xAA; NONCE_LEN]);

        // Magic
        assert_eq!(&header[0..4], b"FLU\x00");
        // Version
        assert_eq!(header[4], 0x01);
        // Algorithm
        assert_eq!(header[5], 0x01);
        // Key ID (little-endian)
        assert_eq!(&header[6..10], &[0x78, 0x56, 0x34, 0x12]);
        // Nonce
        assert_eq!(&header[10..22], &[0xAA; 12]);
    }

    #[test]
    fn test_invalid_magic() {
        let mut envelope = vec![0u8; MIN_ENVELOPE_LEN];
        envelope[0..4].copy_from_slice(b"XXXX");

        let err = parse_header(&envelope).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidFormat { .. }));
    }

    #[test]
    fn test_too_short() {
        let envelope = vec![0u8; 10];
        let err = parse_header(&envelope).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidFormat { .. }));
    }
}
