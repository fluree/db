//! Ed25519 signature signing and verification

use crate::error::{CredentialError, Result};
pub use ed25519_dalek::SigningKey;
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};

/// Sign a message with an Ed25519 signing key
///
/// # Arguments
/// * `signing_key` - Ed25519 signing key (32-byte secret + derived public key)
/// * `message` - Message bytes to sign (raw bytes, not a hex-encoded digest)
///
/// # Returns
/// 64-byte Ed25519 signature
pub fn sign_ed25519(signing_key: &SigningKey, message: &[u8]) -> [u8; 64] {
    signing_key.sign(message).to_bytes()
}

/// Verify an Ed25519 signature
///
/// # Arguments
/// * `pubkey` - 32-byte Ed25519 public key
/// * `message` - Message that was signed
/// * `signature` - 64-byte Ed25519 signature
///
/// # Errors
/// - `InvalidPublicKey` if the public key is invalid
/// - `InvalidSignature` if the signature is invalid or verification fails
pub fn verify_ed25519(pubkey: &[u8; 32], message: &[u8], signature: &[u8]) -> Result<()> {
    // Parse public key
    let verifying_key = VerifyingKey::from_bytes(pubkey)
        .map_err(|e| CredentialError::InvalidPublicKey(e.to_string()))?;

    // Parse signature (must be 64 bytes)
    if signature.len() != 64 {
        return Err(CredentialError::InvalidSignature(format!(
            "Ed25519 signature must be 64 bytes, got {}",
            signature.len()
        )));
    }

    let sig = Signature::from_slice(signature)
        .map_err(|e| CredentialError::InvalidSignature(e.to_string()))?;

    // Verify
    verifying_key
        .verify(message, &sig)
        .map_err(|e| CredentialError::InvalidSignature(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    #[test]
    fn test_verify_valid_signature() {
        // Generate a keypair
        let secret = [0u8; 32]; // deterministic for testing
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();

        // Sign a message
        let message = b"Hello, world!";
        let signature = signing_key.sign(message);

        // Verify
        let result = verify_ed25519(&pubkey, message, &signature.to_bytes());
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_invalid_signature() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();

        let message = b"Hello, world!";
        let mut signature = signing_key.sign(message).to_bytes();

        // Tamper with signature
        signature[0] ^= 0xff;

        let result = verify_ed25519(&pubkey, message, &signature);
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_verify_wrong_message() {
        let secret = [0u8; 32];
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key().to_bytes();

        let message = b"Hello, world!";
        let signature = signing_key.sign(message);

        // Verify with different message
        let result = verify_ed25519(&pubkey, b"Goodbye, world!", &signature.to_bytes());
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_verify_wrong_length_signature() {
        let pubkey = [0u8; 32];
        let message = b"Hello";
        let short_sig = [0u8; 32]; // Too short

        let result = verify_ed25519(&pubkey, message, &short_sig);
        assert!(matches!(result, Err(CredentialError::InvalidSignature(_))));
    }

    #[test]
    fn test_sign_and_verify_round_trip() {
        let secret = [42u8; 32];
        let key = SigningKey::from_bytes(&secret);
        let pubkey = key.verifying_key().to_bytes();

        let message = b"commit digest bytes";
        let signature = sign_ed25519(&key, message);

        assert_eq!(signature.len(), 64);
        assert!(verify_ed25519(&pubkey, message, &signature).is_ok());
    }

    #[test]
    fn test_sign_different_messages_produce_different_sigs() {
        let key = SigningKey::from_bytes(&[1u8; 32]);
        let sig1 = sign_ed25519(&key, b"message A");
        let sig2 = sign_ed25519(&key, b"message B");
        assert_ne!(sig1, sig2);
    }
}
