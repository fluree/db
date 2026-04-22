//! Storage encryption layer for Fluree DB using AES-256-GCM.
//!
//! This crate provides transparent encryption for any Fluree storage backend
//! by wrapping storage implementations with [`EncryptedStorage`].
//!
//! # Features
//!
//! - **AES-256-GCM authenticated encryption**: Provides both confidentiality
//!   and integrity protection
//! - **Portable ciphertext format**: Same encrypted format regardless of
//!   storage backend (file, S3, memory)
//! - **Key rotation support**: Key IDs in the envelope allow reading data
//!   encrypted with previous keys
//! - **Secure key handling**: Keys are zeroized on drop via the `zeroize` crate
//!
//! # Quick Start
//!
//! ```ignore
//! use fluree_db_core::MemoryStorage;
//! use fluree_db_crypto::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
//!
//! // Create any storage backend
//! let storage = MemoryStorage::new();
//!
//! // Create a key (in production, load from secure storage)
//! let key = EncryptionKey::new([0u8; 32], 0);
//! let provider = StaticKeyProvider::new(key);
//!
//! // Wrap with encryption
//! let encrypted = EncryptedStorage::new(storage, provider);
//!
//! // Use normally - encryption is transparent
//! encrypted.write_bytes("data", b"secret").await?;
//! let plaintext = encrypted.read_bytes("data").await?;
//! ```
//!
//! # Envelope Format
//!
//! All encrypted data uses a consistent envelope format:
//!
//! ```text
//! ┌───────────┬─────────┬────────┬────────┬───────────┬────────────────────────┐
//! │   Magic   │ Version │  Alg   │ Key ID │   Nonce   │   Ciphertext + Tag     │
//! │  4 bytes  │ 1 byte  │ 1 byte │ 4 bytes│  12 bytes │       N bytes          │
//! └───────────┴─────────┴────────┴────────┴───────────┴────────────────────────┘
//! ```
//!
//! - **Magic**: `FLU\0` - identifies this as a Fluree encrypted object
//! - **Version**: Envelope format version (currently `0x01`)
//! - **Alg**: Algorithm ID (`0x01` = AES-256-GCM)
//! - **Key ID**: Identifies which key was used (for rotation)
//! - **Nonce**: Random 12-byte IV (unique per encryption)
//! - **Ciphertext + Tag**: Encrypted data with 16-byte auth tag
//!
//! The header is authenticated via GCM's AAD mechanism, preventing tampering.
//!
//! # Key Management
//!
//! Keys are managed through the [`KeyProvider`] trait:
//!
//! - [`StaticKeyProvider`]: Single key, no rotation (simplest)
//! - Custom implementations can load keys from KMS, HSM, or config services
//!
//! ```ignore
//! // From raw bytes
//! let key = EncryptionKey::new([0u8; 32], 0);
//!
//! // From base64 (useful for configuration)
//! let key = EncryptionKey::from_base64("base64-encoded-key", 0)?;
//!
//! // From environment variable
//! let encoded = std::env::var("FLUREE_ENCRYPTION_KEY")?;
//! let provider = StaticKeyProvider::from_base64(&encoded)?;
//! ```
//!
//! # Error Handling
//!
//! Encryption errors are reported through [`EncryptionError`]:
//!
//! - `InvalidFormat`: Data is not encrypted or has invalid header
//! - `UnknownKeyId`: Key ID not found (key rotation issue)
//! - `DecryptFailed`: Wrong key or data corrupted/tampered
//! - `InvalidKey`: Key material is invalid (wrong length, bad base64)
//!
//! # Thread Safety
//!
//! All types are `Send + Sync` when their type parameters are, making them
//! safe for use with async runtimes like Tokio.

mod encrypted;
mod envelope;
mod error;
mod key;

// Re-export main types
pub use encrypted::EncryptedStorage;
pub use error::{EncryptionError, Result};
pub use key::{EncryptionKey, KeyProvider, StaticKeyProvider};

// Re-export envelope constants for advanced use cases
pub use envelope::{
    ALG_AES256_GCM, HEADER_LEN, KEY_LEN, MAGIC, MIN_ENVELOPE_LEN, NONCE_LEN, TAG_LEN, VERSION,
};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use fluree_db_core::prelude::*;

    /// Test that data encrypted with one storage can be decrypted
    /// by another storage with the same key (portability).
    #[tokio::test]
    async fn test_portability_between_storages() {
        // Encrypt with one storage instance
        let storage1 = MemoryStorage::new();
        let encrypted1 = EncryptedStorage::new(
            storage1.clone(),
            StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 1)),
        );

        let plaintext = b"portable data";
        encrypted1.write_bytes("test", plaintext).await.unwrap();

        // Get the raw encrypted bytes
        let raw = storage1.read_bytes("test").await.unwrap();

        // Create a completely new storage and decrypt
        let storage2 = MemoryStorage::new();
        storage2.write_bytes("test", &raw).await.unwrap();

        let encrypted2 = EncryptedStorage::new(
            storage2,
            StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 1)),
        );

        // Should decrypt successfully
        let decrypted = encrypted2.read_bytes("test").await.unwrap();
        assert_eq!(decrypted, plaintext);
    }

    /// Test that nonces are different for each encryption.
    #[tokio::test]
    async fn test_nonces_are_unique() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(
            storage.clone(),
            StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 1)),
        );

        // Write the same data twice
        encrypted.write_bytes("test1", b"same").await.unwrap();
        encrypted.write_bytes("test2", b"same").await.unwrap();

        // Get raw encrypted data
        let raw1 = storage.read_bytes("test1").await.unwrap();
        let raw2 = storage.read_bytes("test2").await.unwrap();

        // Nonce is at offset 10-22, ciphertext should be different
        // (even though plaintext is the same) due to different nonces
        assert_ne!(raw1, raw2);

        // But both should decrypt to the same thing
        let dec1 = encrypted.read_bytes("test1").await.unwrap();
        let dec2 = encrypted.read_bytes("test2").await.unwrap();
        assert_eq!(dec1, dec2);
    }
}
