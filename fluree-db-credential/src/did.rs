//! DID:key encoding and decoding
//!
//! Implements the did:key method for Ed25519 public keys.
//! Format: did:key:z6Mk... where z6Mk... is multibase base58btc with 0xed01 prefix

use crate::error::{CredentialError, Result};

/// Ed25519 multicodec prefix (0xed01)
const ED25519_MULTICODEC_PREFIX: [u8; 2] = [0xed, 0x01];

/// Derive a did:key identifier from an Ed25519 public key
///
/// # Format
/// `did:key:z{base58btc(0xed01 || pubkey)}`
///
/// The 'z' prefix indicates base58btc encoding (multibase)
/// The 0xed01 prefix indicates Ed25519 public key (multicodec)
///
/// # Example
/// ```ignore
/// let pubkey = [0u8; 32]; // 32-byte Ed25519 public key
/// let did = did_from_pubkey(&pubkey);
/// // did:key:z6Mk...
/// ```
pub fn did_from_pubkey(pubkey: &[u8; 32]) -> String {
    // Concatenate multicodec prefix with public key
    let mut bytes = Vec::with_capacity(34);
    bytes.extend_from_slice(&ED25519_MULTICODEC_PREFIX);
    bytes.extend_from_slice(pubkey);

    // Base58btc encode
    let base58 = bs58::encode(&bytes).into_string();

    // Add multibase 'z' prefix and did:key: scheme
    format!("did:key:z{base58}")
}

/// Extract an Ed25519 public key from a did:key identifier
///
/// # Format
/// Expects `did:key:z{base58btc(0xed01 || pubkey)}`
///
/// # Errors
/// - `InvalidDid` if the format is not did:key:z...
/// - `Base58Decode` if base58 decoding fails
/// - `InvalidPublicKey` if the multicodec prefix is wrong or key length is not 32
pub fn pubkey_from_did(did: &str) -> Result<[u8; 32]> {
    // Strip did:key:z prefix
    let key_part = did.strip_prefix("did:key:z").ok_or_else(|| {
        CredentialError::InvalidDid(format!("DID must start with 'did:key:z', got: {did}"))
    })?;

    // Base58btc decode
    let bytes = bs58::decode(key_part)
        .into_vec()
        .map_err(|e| CredentialError::Base58Decode(e.to_string()))?;

    // Verify multicodec prefix (0xed01)
    if bytes.len() < 2 || bytes[0] != 0xed || bytes[1] != 0x01 {
        return Err(CredentialError::InvalidPublicKey(
            "Missing or invalid Ed25519 multicodec prefix (0xed01)".to_string(),
        ));
    }

    // Extract the 32-byte public key
    let key_bytes = &bytes[2..];
    if key_bytes.len() != 32 {
        return Err(CredentialError::InvalidPublicKey(format!(
            "Ed25519 public key must be 32 bytes, got {}",
            key_bytes.len()
        )));
    }

    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(key_bytes);
    Ok(pubkey)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test vectors from fluree.crypto cross_platform_test.cljc
    const TEST_PUBKEY_HEX: &str =
        "a8def12ad736f8840f836a46c66c9f3e2015d1ea2c69d546c050fef746bd63b3";
    const TEST_DID: &str = "did:key:z6MkqpTi7zUDy5nnSfpLf7SPGsepMNJAxRiH1jbCZbuaZoEz";

    fn hex_to_bytes(hex: &str) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let s = std::str::from_utf8(chunk).unwrap();
            bytes[i] = u8::from_str_radix(s, 16).unwrap();
        }
        bytes
    }

    #[test]
    fn test_did_from_pubkey() {
        let pubkey = hex_to_bytes(TEST_PUBKEY_HEX);
        let did = did_from_pubkey(&pubkey);
        assert_eq!(did, TEST_DID);
    }

    #[test]
    fn test_pubkey_from_did() {
        let pubkey = pubkey_from_did(TEST_DID).unwrap();
        let expected = hex_to_bytes(TEST_PUBKEY_HEX);
        assert_eq!(pubkey, expected);
    }

    #[test]
    fn test_roundtrip() {
        let original = hex_to_bytes(TEST_PUBKEY_HEX);
        let did = did_from_pubkey(&original);
        let recovered = pubkey_from_did(&did).unwrap();
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_invalid_did_prefix() {
        let result = pubkey_from_did("did:web:example.com");
        assert!(matches!(result, Err(CredentialError::InvalidDid(_))));
    }

    #[test]
    fn test_invalid_multicodec_prefix() {
        // Create a DID with wrong multicodec prefix
        let wrong_prefix = bs58::encode([0x00, 0x01]).into_string();
        let bad_did = format!("did:key:z{wrong_prefix}");
        let result = pubkey_from_did(&bad_did);
        assert!(matches!(result, Err(CredentialError::InvalidPublicKey(_))));
    }
}
