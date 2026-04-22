//! Encryption key types and key provider traits.
//!
//! This module provides:
//! - [`EncryptionKey`]: A 32-byte AES-256 key with automatic zeroization
//! - [`KeyProvider`]: Trait for looking up keys (supports rotation)
//! - [`StaticKeyProvider`]: Simple single-key implementation

use crate::envelope::KEY_LEN;
use crate::error::{EncryptionError, Result};
use base64::prelude::*;
use std::sync::Arc;
use zeroize::{Zeroize, ZeroizeOnDrop};

// ============================================================================
// EncryptionKey
// ============================================================================

/// A 32-byte AES-256 encryption key with automatic zeroization.
///
/// This type wraps the key material and:
/// - Prevents accidental exposure through `Debug` or `Display`
/// - Automatically zeroizes memory when dropped
///
/// # Security
///
/// - Key material is zeroized on drop
/// - No `Debug` implementation to prevent logging keys
/// - Use [`expose_secret`](Self::expose_secret) only when needed for crypto operations
pub struct EncryptionKey {
    /// The secret key bytes, zeroized on drop.
    bytes: KeyBytes,
    /// Key identifier for rotation support.
    id: u32,
}

/// Fixed-size array for the key, with zeroization on drop.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
struct KeyBytes([u8; KEY_LEN]);

// Safe Debug impl that doesn't expose key material
impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionKey")
            .field("id", &self.id)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

impl EncryptionKey {
    /// Create a new encryption key from raw bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Exactly 32 bytes of key material
    /// * `id` - Key identifier (for rotation, use 0 if not rotating)
    pub fn new(bytes: [u8; KEY_LEN], id: u32) -> Self {
        Self {
            bytes: KeyBytes(bytes),
            id,
        }
    }

    /// Create an encryption key from a base64-encoded string.
    ///
    /// # Arguments
    ///
    /// * `encoded` - Base64-encoded key (standard or URL-safe)
    /// * `id` - Key identifier (for rotation, use 0 if not rotating)
    ///
    /// # Errors
    ///
    /// Returns `EncryptionError::InvalidKey` if:
    /// - Base64 decoding fails
    /// - Decoded length is not exactly 32 bytes
    pub fn from_base64(encoded: &str, id: u32) -> Result<Self> {
        // Try standard base64 first, then URL-safe
        let decoded = BASE64_STANDARD
            .decode(encoded.trim())
            .or_else(|_| BASE64_URL_SAFE.decode(encoded.trim()))
            .map_err(|_| EncryptionError::invalid_key("invalid base64 encoding"))?;

        if decoded.len() != KEY_LEN {
            return Err(EncryptionError::invalid_key(
                "key must be exactly 32 bytes when decoded",
            ));
        }

        let mut bytes = [0u8; KEY_LEN];
        bytes.copy_from_slice(&decoded);

        // Zeroize the decoded Vec
        let mut decoded = decoded;
        decoded.zeroize();

        Ok(Self::new(bytes, id))
    }

    /// Get the key identifier.
    ///
    /// This is used to match ciphertext to the correct key during decryption.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Expose the secret key bytes for cryptographic operations.
    ///
    /// # Security
    ///
    /// Only call this when you need to pass the key to a crypto primitive.
    /// Do not store, log, or transmit the returned bytes.
    pub(crate) fn expose_secret(&self) -> &[u8; KEY_LEN] {
        &self.bytes.0
    }
}

// Intentionally no Debug impl to prevent accidental logging
// Intentionally no Clone impl to prevent accidental copies

// ============================================================================
// KeyProvider
// ============================================================================

/// Trait for providing encryption keys.
///
/// This trait supports key rotation by:
/// - Providing the current key for new encryptions via [`current_key`](Self::current_key)
/// - Looking up keys by ID for decryption via [`key_by_id`](Self::key_by_id)
///
/// # Implementors
///
/// - [`StaticKeyProvider`]: Simple single-key implementation
/// - Custom implementations can load keys from KMS, HSM, or config services
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to work with async storage operations.
pub trait KeyProvider: Send + Sync {
    /// Get the current key to use for new encryptions.
    ///
    /// This should return the "active" key. When keys are rotated,
    /// this returns the newest key while old keys remain available
    /// via [`key_by_id`](Self::key_by_id) for decryption.
    fn current_key(&self) -> Arc<EncryptionKey>;

    /// Look up a key by its ID for decryption.
    ///
    /// Returns `None` if the key ID is unknown, which happens when:
    /// - Data was encrypted with a rotated-out key
    /// - Wrong key provider is configured
    /// - Key ID was corrupted/tampered
    fn key_by_id(&self, id: u32) -> Option<Arc<EncryptionKey>>;
}

// ============================================================================
// StaticKeyProvider
// ============================================================================

/// A simple key provider with a single static key.
///
/// This is the most common implementation for applications that
/// don't need key rotation. The same key is used for all operations.
///
/// # Example
///
/// ```ignore
/// use fluree_db_crypto::{EncryptionKey, StaticKeyProvider};
///
/// let key = EncryptionKey::new([0u8; 32], 0);
/// let provider = StaticKeyProvider::new(key);
/// ```
pub struct StaticKeyProvider {
    key: Arc<EncryptionKey>,
}

impl StaticKeyProvider {
    /// Create a new static key provider.
    ///
    /// # Arguments
    ///
    /// * `key` - The encryption key to use for all operations
    pub fn new(key: EncryptionKey) -> Self {
        Self { key: Arc::new(key) }
    }

    /// Create from a base64-encoded key string.
    ///
    /// Convenience constructor that creates the key with ID 0.
    ///
    /// # Errors
    ///
    /// Returns `EncryptionError::InvalidKey` if base64 decoding fails
    /// or the decoded key is not exactly 32 bytes.
    pub fn from_base64(encoded: &str) -> Result<Self> {
        let key = EncryptionKey::from_base64(encoded, 0)?;
        Ok(Self::new(key))
    }
}

impl KeyProvider for StaticKeyProvider {
    fn current_key(&self) -> Arc<EncryptionKey> {
        Arc::clone(&self.key)
    }

    fn key_by_id(&self, id: u32) -> Option<Arc<EncryptionKey>> {
        if id == self.key.id() {
            Some(Arc::clone(&self.key))
        } else {
            None
        }
    }
}

// Intentionally no Debug impl to prevent accidental key exposure

#[cfg(test)]
mod tests {
    use super::*;

    // Test key (32 bytes of 0x42)
    const TEST_KEY: [u8; 32] = [0x42; 32];

    #[test]
    fn test_key_creation() {
        let key = EncryptionKey::new(TEST_KEY, 123);
        assert_eq!(key.id(), 123);
        assert_eq!(key.expose_secret(), &TEST_KEY);
    }

    #[test]
    fn test_key_from_base64() {
        // Base64 of 32 zero bytes
        let encoded = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let key = EncryptionKey::from_base64(encoded, 0).unwrap();
        assert_eq!(key.expose_secret(), &[0u8; 32]);
    }

    #[test]
    fn test_key_from_base64_url_safe() {
        // URL-safe base64 should also work
        let encoded = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let key = EncryptionKey::from_base64(encoded, 0).unwrap();
        assert_eq!(key.expose_secret(), &[0u8; 32]);
    }

    #[test]
    fn test_key_from_base64_wrong_length() {
        let encoded = "AQID"; // Only 3 bytes decoded
        let err = EncryptionKey::from_base64(encoded, 0).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidKey { .. }));
    }

    #[test]
    fn test_key_from_base64_invalid() {
        let encoded = "not valid base64!!!";
        let err = EncryptionKey::from_base64(encoded, 0).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidKey { .. }));
    }

    #[test]
    fn test_static_provider() {
        let key = EncryptionKey::new(TEST_KEY, 42);
        let provider = StaticKeyProvider::new(key);

        // Current key should work
        let current = provider.current_key();
        assert_eq!(current.id(), 42);

        // Lookup by ID should work for matching ID
        assert!(provider.key_by_id(42).is_some());

        // Lookup by wrong ID should return None
        assert!(provider.key_by_id(999).is_none());
    }

    #[test]
    fn test_static_provider_from_base64() {
        let encoded = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let provider = StaticKeyProvider::from_base64(encoded).unwrap();

        // Default key ID is 0
        assert_eq!(provider.current_key().id(), 0);
    }
}
