//! Encrypted storage wrapper that transparently encrypts/decrypts data.
//!
//! [`EncryptedStorage`] wraps any storage implementation and provides
//! transparent AES-256-GCM encryption for all read/write operations.
//!
//! # Content-Addressed Storage Compatibility
//!
//! This wrapper is designed to work with Fluree's content-addressed storage model:
//!
//! - **Addresses are computed from plaintext** by higher layers *before* calling
//!   `write_bytes`. This means `address = hash(plaintext)` is deterministic.
//! - **Ciphertext is non-deterministic** because each encryption uses a fresh
//!   random nonce for security. Writing the same plaintext twice produces
//!   different ciphertext.
//! - **This is correct behavior**: The address (content hash) is stable, only
//!   the on-disk bytes vary. Reads always decrypt to the same plaintext.
//!
//! The storage flow is:
//! ```text
//! Higher layers:     address = hash(plaintext)
//!                         ↓
//! EncryptedStorage:  ciphertext = encrypt(plaintext, random_nonce)
//!                    inner.write_bytes(address, ciphertext)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use fluree_db_core::MemoryStorage;
//! use fluree_db_crypto::{EncryptedStorage, EncryptionKey, StaticKeyProvider};
//!
//! let storage = MemoryStorage::new();
//! let key = EncryptionKey::new([0u8; 32], 0);
//! let provider = StaticKeyProvider::new(key);
//! let encrypted = EncryptedStorage::new(storage, provider);
//!
//! // Now all writes are encrypted, all reads are decrypted
//! encrypted.write_bytes("data", b"secret").await?;
//! let plaintext = encrypted.read_bytes("data").await?;
//! ```

use crate::envelope::{
    build_header, ciphertext_slice, header_slice, parse_header, HEADER_LEN, NONCE_LEN,
};
use crate::error::{EncryptionError, Result};
use crate::key::KeyProvider;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use async_trait::async_trait;
use fluree_db_core::{
    sha256_hex, ContentAddressedWrite, ContentKind, ContentWriteResult, StorageRead, StorageWrite,
};
use rand_core::{OsRng, RngCore};
use std::fmt::{self, Debug};
use std::sync::Arc;

/// A storage wrapper that encrypts data on write and decrypts on read.
///
/// This wraps any [`Storage`] implementation and provides transparent
/// AES-256-GCM authenticated encryption. The same encrypted format is
/// used regardless of the underlying storage backend (file, S3, memory),
/// making data portable between backends.
///
/// # Type Parameters
///
/// * `S` - The underlying storage implementation
/// * `K` - The key provider (typically [`StaticKeyProvider`](crate::StaticKeyProvider))
///
/// # Thread Safety
///
/// `EncryptedStorage` is `Send + Sync` when both `S` and `K` are.
///
/// # Error Behavior
///
/// - **Reading unencrypted data with encryption enabled**: Returns
///   `EncryptionError::InvalidFormat` ("not a Fluree encrypted object").
///   This is intentional - if encryption is configured, all data should
///   be encrypted.
///
/// - **Reading with wrong key**: Returns `EncryptionError::DecryptFailed`
///   or `EncryptionError::UnknownKeyId` depending on whether the key ID
///   is recognized.
pub struct EncryptedStorage<S, K> {
    inner: S,
    keys: Arc<K>,
}

impl<S, K> EncryptedStorage<S, K>
where
    K: KeyProvider,
{
    /// Create a new encrypted storage wrapper.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying storage to wrap
    /// * `keys` - Key provider for encryption/decryption
    pub fn new(inner: S, keys: K) -> Self {
        Self {
            inner,
            keys: Arc::new(keys),
        }
    }

    /// Create with an existing Arc'd key provider.
    ///
    /// Useful when sharing a key provider across multiple storage instances.
    pub fn with_arc_keys(inner: S, keys: Arc<K>) -> Self {
        Self { inner, keys }
    }

    /// Get a reference to the underlying storage.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Encrypt plaintext and build the complete envelope.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = self.keys.current_key();

        // Create cipher from key
        let cipher =
            Aes256Gcm::new_from_slice(key.expose_secret()).expect("key is always 32 bytes");

        // Generate random nonce
        let mut nonce_bytes = [0u8; NONCE_LEN];
        OsRng.fill_bytes(&mut nonce_bytes);

        // Build header (used as AAD)
        let header = build_header(key.id(), &nonce_bytes);

        // Encrypt with header as AAD
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plaintext,
                    aad: &header,
                },
            )
            .map_err(|_| EncryptionError::encrypt_failed("AES-GCM encryption failed"))?;

        // Build envelope: header || ciphertext
        let mut envelope = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        envelope.extend_from_slice(&header);
        envelope.extend_from_slice(&ciphertext);

        Ok(envelope)
    }

    /// Decrypt an envelope and return the plaintext.
    fn decrypt(&self, envelope: &[u8]) -> Result<Vec<u8>> {
        // Parse and validate header
        let header = parse_header(envelope)?;

        // Look up the key
        let key = self
            .keys
            .key_by_id(header.key_id)
            .ok_or_else(|| EncryptionError::unknown_key_id(header.key_id))?;

        // Create cipher from key
        let cipher =
            Aes256Gcm::new_from_slice(key.expose_secret()).expect("key is always 32 bytes");

        // Decrypt with header as AAD
        let nonce = Nonce::from_slice(&header.nonce);
        let aad = header_slice(envelope);
        let ciphertext = ciphertext_slice(envelope);

        cipher
            .decrypt(
                nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| {
                EncryptionError::decrypt_failed(
                    "decryption failed: wrong key or data corrupted/tampered",
                )
            })
    }
}

// Debug impl that doesn't expose key material
impl<S: Debug, K> Debug for EncryptedStorage<S, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptedStorage")
            .field("inner", &self.inner)
            .field("keys", &"<key provider>")
            .finish()
    }
}

// Clone when inner storage is cloneable
impl<S: Clone, K> Clone for EncryptedStorage<S, K> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            keys: Arc::clone(&self.keys),
        }
    }
}

// ============================================================================
// Storage Trait Implementations
// ============================================================================

impl<S, K> fluree_db_core::StorageMethod for EncryptedStorage<S, K>
where
    S: fluree_db_core::StorageMethod,
    K: KeyProvider,
{
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

#[async_trait]
impl<S, K> StorageRead for EncryptedStorage<S, K>
where
    S: StorageRead,
    K: KeyProvider,
{
    async fn read_bytes(&self, address: &str) -> fluree_db_core::error::Result<Vec<u8>> {
        // Read encrypted bytes from underlying storage
        let encrypted = self.inner.read_bytes(address).await?;

        // Decrypt
        self.decrypt(&encrypted).map_err(Into::into)
    }

    // read_byte_range: uses default (full read + slice). AES-GCM encryption
    // requires decrypting the entire blob — partial range reads on ciphertext
    // are not meaningful. The default calls read_bytes() → decrypt → slice.

    async fn exists(&self, address: &str) -> fluree_db_core::error::Result<bool> {
        // Pass through - existence check doesn't need decryption
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::error::Result<Vec<String>> {
        // Pass through - listing doesn't need encryption
        self.inner.list_prefix(prefix).await
    }
}

#[async_trait]
impl<S, K> StorageWrite for EncryptedStorage<S, K>
where
    S: StorageWrite,
    K: KeyProvider,
{
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::error::Result<()> {
        // Encrypt the plaintext
        let encrypted = self
            .encrypt(bytes)
            .map_err(fluree_db_core::error::Error::from)?;

        // Write encrypted bytes to underlying storage
        self.inner.write_bytes(address, &encrypted).await
    }

    async fn delete(&self, address: &str) -> fluree_db_core::error::Result<()> {
        // Pass through - deletion doesn't need encryption
        self.inner.delete(address).await
    }
}

#[async_trait]
impl<S, K> ContentAddressedWrite for EncryptedStorage<S, K>
where
    S: ContentAddressedWrite,
    K: KeyProvider,
{
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentWriteResult> {
        // Remember plaintext size for the result
        let plaintext_size = bytes.len();

        // Encrypt the plaintext
        let encrypted = self
            .encrypt(bytes)
            .map_err(fluree_db_core::error::Error::from)?;

        // Delegate to inner storage - it generates the address with its own method
        // (file, s3, memory, etc.) while we store encrypted bytes
        let mut result = self
            .inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, &encrypted)
            .await?;

        // Restore plaintext size (inner storage reports encrypted size)
        result.size_bytes = plaintext_size;

        Ok(result)
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentWriteResult> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash_hex, bytes)
            .await
    }
}

// ============================================================================
// Nameservice Trait Implementations (optional feature)
// ============================================================================

#[cfg(feature = "nameservice")]
mod nameservice_impls {
    use super::*;
    use fluree_db_core::{
        CasAction, CasOutcome, ListResult, StorageCas, StorageDelete, StorageExtError,
        StorageExtResult, StorageList,
    };

    /// StorageDelete passthrough - deletion doesn't need encryption
    #[async_trait]
    impl<S, K> StorageDelete for EncryptedStorage<S, K>
    where
        S: StorageDelete,
        K: KeyProvider,
    {
        async fn delete(&self, address: &str) -> StorageExtResult<()> {
            self.inner.delete(address).await
        }
    }

    /// StorageList passthrough - listing doesn't need encryption
    #[async_trait]
    impl<S, K> StorageList for EncryptedStorage<S, K>
    where
        S: StorageList,
        K: KeyProvider,
    {
        async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
            self.inner.list_prefix(prefix).await
        }

        async fn list_prefix_paginated(
            &self,
            prefix: &str,
            continuation_token: Option<String>,
            max_keys: usize,
        ) -> StorageExtResult<ListResult> {
            self.inner
                .list_prefix_paginated(prefix, continuation_token, max_keys)
                .await
        }
    }

    /// StorageCas with encryption - encrypts/decrypts data transparently
    #[async_trait]
    impl<S, K> StorageCas for EncryptedStorage<S, K>
    where
        S: StorageCas,
        K: KeyProvider,
    {
        async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
            let encrypted = self
                .encrypt(bytes)
                .map_err(|e| StorageExtError::other(e.to_string()))?;
            self.inner.insert(address, &encrypted).await
        }

        async fn compare_and_swap<T, F>(
            &self,
            address: &str,
            f: F,
        ) -> StorageExtResult<CasOutcome<T>>
        where
            F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError>
                + Send
                + Sync,
            T: Send,
        {
            self.inner
                .compare_and_swap(address, |current_encrypted| {
                    // Decrypt current value if present
                    let current_plaintext = current_encrypted
                        .map(|enc| {
                            self.decrypt(enc)
                                .map_err(|e| StorageExtError::other(e.to_string()))
                        })
                        .transpose()?;

                    let current_ref = current_plaintext.as_deref();
                    match f(current_ref)? {
                        CasAction::Write(plaintext) => {
                            let encrypted = self
                                .encrypt(&plaintext)
                                .map_err(|e| StorageExtError::other(e.to_string()))?;
                            Ok(CasAction::Write(encrypted))
                        }
                        CasAction::Abort(t) => Ok(CasAction::Abort(t)),
                    }
                })
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{EncryptionKey, StaticKeyProvider};
    use fluree_db_core::MemoryStorage;

    // Test key (32 bytes)
    fn test_key() -> EncryptionKey {
        EncryptionKey::new([0x42; 32], 1)
    }

    fn test_provider() -> StaticKeyProvider {
        StaticKeyProvider::new(test_key())
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        let plaintext = b"Hello, encrypted world!";

        // Write encrypted
        encrypted.write_bytes("test/data", plaintext).await.unwrap();

        // Read and decrypt
        let decrypted = encrypted.read_bytes("test/data").await.unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_encrypted_data_is_different() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());

        let plaintext = b"Hello, encrypted world!";

        // Write encrypted
        encrypted.write_bytes("test/data", plaintext).await.unwrap();

        // Read raw (unencrypted) from underlying storage
        let raw = storage.read_bytes("test/data").await.unwrap();

        // Raw data should be different (encrypted) and longer (header + tag)
        assert_ne!(raw.as_slice(), plaintext);
        assert!(raw.len() > plaintext.len());

        // Should start with magic bytes
        assert_eq!(&raw[0..4], b"FLU\x00");
    }

    #[tokio::test]
    async fn test_wrong_key_fails() {
        let storage = MemoryStorage::new();

        // Write with one key
        let key1 = EncryptionKey::new([0x01; 32], 1);
        let provider1 = StaticKeyProvider::new(key1);
        let encrypted1 = EncryptedStorage::new(storage.clone(), provider1);
        encrypted1
            .write_bytes("test/data", b"secret")
            .await
            .unwrap();

        // Try to read with different key (same ID)
        let key2 = EncryptionKey::new([0x02; 32], 1);
        let provider2 = StaticKeyProvider::new(key2);
        let encrypted2 = EncryptedStorage::new(storage.clone(), provider2);

        let result = encrypted2.read_bytes("test/data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unknown_key_id_fails() {
        let storage = MemoryStorage::new();

        // Write with key ID 1
        let key1 = EncryptionKey::new([0x01; 32], 1);
        let provider1 = StaticKeyProvider::new(key1);
        let encrypted1 = EncryptedStorage::new(storage.clone(), provider1);
        encrypted1
            .write_bytes("test/data", b"secret")
            .await
            .unwrap();

        // Try to read with different key ID
        let key2 = EncryptionKey::new([0x01; 32], 2); // Same bytes, different ID
        let provider2 = StaticKeyProvider::new(key2);
        let encrypted2 = EncryptedStorage::new(storage.clone(), provider2);

        let result = encrypted2.read_bytes("test/data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plaintext_fails_when_encryption_enabled() {
        let storage = MemoryStorage::new();

        // Write plaintext directly
        storage
            .write_bytes("test/plain", b"not encrypted")
            .await
            .unwrap();

        // Try to read with encryption enabled
        let encrypted = EncryptedStorage::new(storage, test_provider());
        let result = encrypted.read_bytes("test/plain").await;

        // Should fail because magic bytes don't match
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_exists_passthrough() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        // Write something
        encrypted.write_bytes("test/data", b"data").await.unwrap();

        // Exists should work
        assert!(encrypted.exists("test/data").await.unwrap());
        assert!(!encrypted.exists("test/nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_delete_passthrough() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        // Write something
        encrypted.write_bytes("test/data", b"data").await.unwrap();
        assert!(encrypted.exists("test/data").await.unwrap());

        // Delete should work
        encrypted.delete("test/data").await.unwrap();
        assert!(!encrypted.exists("test/data").await.unwrap());
    }

    #[tokio::test]
    async fn test_list_passthrough() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        // Write some files
        encrypted.write_bytes("prefix/a", b"a").await.unwrap();
        encrypted.write_bytes("prefix/b", b"b").await.unwrap();
        encrypted.write_bytes("other/c", b"c").await.unwrap();

        // List should work
        let mut files = encrypted.list_prefix("prefix/").await.unwrap();
        files.sort();
        assert_eq!(files, vec!["prefix/a", "prefix/b"]);
    }

    #[tokio::test]
    async fn test_empty_plaintext() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        // Empty data should work
        encrypted.write_bytes("test/empty", b"").await.unwrap();
        let decrypted = encrypted.read_bytes("test/empty").await.unwrap();
        assert!(decrypted.is_empty());
    }

    #[tokio::test]
    async fn test_large_plaintext() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage, test_provider());

        // Large data (1 MB)
        let plaintext: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        encrypted
            .write_bytes("test/large", &plaintext)
            .await
            .unwrap();
        let decrypted = encrypted.read_bytes("test/large").await.unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_tampered_data_fails() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());

        // Write encrypted data
        encrypted.write_bytes("test/data", b"secret").await.unwrap();

        // Tamper with the ciphertext
        let mut raw = storage.read_bytes("test/data").await.unwrap();
        if let Some(byte) = raw.last_mut() {
            *byte ^= 0xFF; // Flip bits
        }
        storage.write_bytes("test/data", &raw).await.unwrap();

        // Read should fail due to auth tag mismatch
        let result = encrypted.read_bytes("test/data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_content_addressed_write_uses_inner_storage_method() {
        use fluree_db_core::ContentKind;

        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());

        let plaintext = b"content for addressing";

        // Use content-addressed write
        let result = encrypted
            .content_write_bytes(ContentKind::Commit, "mydb:main", plaintext)
            .await
            .unwrap();

        // Address should use the inner storage's method (memory), not hardcoded "file"
        assert!(
            result.address.starts_with("fluree:memory://"),
            "Expected address to start with 'fluree:memory://', got: {}",
            result.address
        );

        // Verify the address format is correct
        assert!(result.address.contains("mydb/main/commit/"));
        assert!(result.address.ends_with(".fcv2"));

        // Verify size_bytes reports plaintext size, not encrypted size
        assert_eq!(result.size_bytes, plaintext.len());

        // Verify we can read the data back and decrypt it
        let decrypted = encrypted.read_bytes(&result.address).await.unwrap();
        assert_eq!(decrypted, plaintext);

        // Verify the underlying storage has encrypted data (not plaintext)
        let raw = storage.read_bytes(&result.address).await.unwrap();
        assert_ne!(raw.as_slice(), plaintext);
        assert!(raw.len() > plaintext.len()); // Encrypted data is larger
    }
}
