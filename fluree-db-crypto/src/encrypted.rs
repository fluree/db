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
    build_header, ciphertext_slice, envelope_version, header_slice, key_id_of_header, parse_header,
    HEADER_LEN, NONCE_LEN,
};
use crate::error::{EncryptionError, Result};
use crate::key::KeyProvider;
use crate::segmented;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use async_trait::async_trait;
use fluree_db_core::{
    sha256_hex, ContentAddressedWrite, ContentKind, ContentWriteResult, EncryptionAdmin,
    StorageRead, StorageWrite,
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
    /// Random per instance (shared by clones): scopes what a cache of
    /// decrypted bytes may serve to readers of this storage.
    context: [u8; 16],
    /// The envelope new writes use.
    write: WriteFormat,
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
        Self::with_arc_keys(inner, Arc::new(keys))
    }

    /// Create with an existing Arc'd key provider.
    ///
    /// Useful when sharing a key provider across multiple storage instances.
    pub fn with_arc_keys(inner: S, keys: Arc<K>) -> Self {
        let mut context = [0u8; 16];
        OsRng.fill_bytes(&mut context);
        Self {
            inner,
            keys,
            context,
            write: WriteFormat::Segmented,
        }
    }

    /// Write version 1 envelopes, as releases before version 2 did: for
    /// tests of stores those releases wrote, not a supported configuration.
    #[doc(hidden)]
    pub fn writing_version_1_envelopes(mut self) -> Self {
        self.write = WriteFormat::Whole;
        self
    }

    /// Get a reference to the underlying storage.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Encrypt plaintext and build the complete envelope.
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        Self::encrypt_with(&self.keys, self.write, plaintext)
    }

    /// [`Self::encrypt`] with only the key provider and format, for callers
    /// that cannot borrow the storage.
    fn encrypt_with(keys: &K, write: WriteFormat, plaintext: &[u8]) -> Result<Vec<u8>> {
        match write {
            WriteFormat::Whole => Self::encrypt_whole(keys, plaintext),
            WriteFormat::Segmented => {
                let key = keys.current_key();
                segmented::encrypt(
                    key.expose_secret(),
                    key.id(),
                    segmented::DEFAULT_SEGMENT_LOG2,
                    plaintext,
                )
            }
        }
    }

    /// A version 1 envelope: one AES-GCM message over the whole object.
    fn encrypt_whole(keys: &K, plaintext: &[u8]) -> Result<Vec<u8>> {
        let key = keys.current_key();

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
        Self::decrypt_with(&self.keys, envelope)
    }

    /// [`Self::decrypt`] with only the key provider, for callers that cannot
    /// borrow the storage.
    fn decrypt_with(keys: &K, envelope: &[u8]) -> Result<Vec<u8>> {
        if envelope_version(envelope)? == segmented::VERSION {
            return Self::object_key(keys, envelope)?.decrypt(envelope);
        }
        let plaintext = Self::decrypt_whole(keys, envelope)?;
        crate::stats::v1_object_decrypted(plaintext.len());
        Ok(plaintext)
    }

    /// A segmented envelope's object key, under the master key its header
    /// names.
    fn object_key(keys: &K, envelope: &[u8]) -> Result<segmented::ObjectKey> {
        let header = segmented::Header::parse(envelope)?;
        let key = keys
            .key_by_id(header.key_id)
            .ok_or_else(|| EncryptionError::unknown_key_id(header.key_id))?;
        Ok(segmented::ObjectKey::derive(key.expose_secret(), header))
    }

    /// A version 1 envelope's plaintext.
    fn decrypt_whole(keys: &K, envelope: &[u8]) -> Result<Vec<u8>> {
        // Parse and validate header
        let header = parse_header(envelope)?;

        // Look up the key
        let key = keys
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

/// The envelope new writes use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteFormat {
    /// Version 1, one AES-GCM message per object.
    Whole,
    /// Version 2, segments of the default size.
    Segmented,
}

/// A local segmented envelope read by range: the ciphertext stays as the
/// inner storage served it (a mapping, for a large file), and only the
/// segments a read touches are decrypted.
struct SegmentedSource {
    envelope: fluree_db_core::ContentBytes,
    key: segmented::ObjectKey,
    identity: u128,
}

impl Debug for SegmentedSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentedSource")
            .field("envelope", &self.envelope)
            .field("key", &self.key)
            .finish()
    }
}

impl fluree_db_core::RangeSource for SegmentedSource {
    fn len(&self) -> u64 {
        self.key.header().plaintext_len()
    }

    fn read_range(&self, range: std::ops::Range<u64>) -> fluree_db_core::error::Result<Vec<u8>> {
        self.key
            .read_range(&self.envelope, range)
            .map_err(Into::into)
    }

    fn read_unit(&self) -> u64 {
        self.key.header().segment_size()
    }

    /// The ciphertext as it is held, plus the expanded key.
    fn cache_weight(&self) -> usize {
        self.envelope.cache_weight() + std::mem::size_of::<segmented::ObjectKey>()
    }

    /// This storage instance and this envelope: its header carries the
    /// salt drawn for each encryption.
    fn cache_identity(&self) -> u128 {
        self.identity
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
            context: self.context,
            write: self.write,
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
    S: StorageRead + StorageWrite + Clone + Send + Sync + 'static,
    K: KeyProvider + 'static,
{
    async fn read_bytes(&self, address: &str) -> fluree_db_core::error::Result<Vec<u8>> {
        // Read encrypted bytes from underlying storage
        let encrypted = self.inner.read_bytes(address).await?;

        // Decrypt
        self.decrypt(&encrypted).map_err(Into::into)
    }

    /// The whole envelope is fetched either way. A version 1 envelope is
    /// decrypted whole and sliced; a segmented one decrypts only the segments
    /// the range touches.
    async fn read_byte_range(
        &self,
        address: &str,
        range: std::ops::Range<u64>,
    ) -> fluree_db_core::error::Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let envelope = self.inner.read_bytes(address).await?;
        if envelope_version(&envelope)? == segmented::VERSION {
            let key = Self::object_key(&self.keys, &envelope)?;
            segmented::check_envelope_len(key.header(), envelope.len() as u64)?;
            return Ok(key.read_range(&envelope, range)?);
        }
        let full = self.decrypt(&envelope)?;
        let start = (range.start as usize).min(full.len());
        let end = (range.end as usize).min(full.len());
        Ok(full[start..end].to_vec())
    }

    /// The inner storage's local ciphertext, decrypted. CPU work only, and
    /// the plaintext goes back to the caller, never into a cache file.
    fn get_local(
        &self,
        address: &str,
    ) -> fluree_db_core::error::Result<Option<fluree_db_core::ContentBytes>> {
        match self.inner.get_local(address)? {
            Some(envelope) => Ok(Some(self.decrypt(&envelope)?.into())),
            None => Ok(None),
        }
    }

    /// A segmented envelope on this machine, read by range. A version 1
    /// envelope can only be decrypted whole, so it answers `None` and the
    /// caller reads it through `get_local`.
    fn get_local_ranged(
        &self,
        address: &str,
    ) -> fluree_db_core::error::Result<Option<Arc<dyn fluree_db_core::RangeSource>>> {
        let Some(envelope) = self.inner.get_local(address)? else {
            return Ok(None);
        };
        if envelope_version(&envelope)? != segmented::VERSION {
            return Ok(None);
        }
        let key = Self::object_key(&self.keys, &envelope)?;
        segmented::check_envelope_len(key.header(), envelope.len() as u64)?;
        let identity = {
            use sha2::Digest;
            let digest = sha2::Sha256::new()
                .chain_update(b"fluree-db/segment-cache")
                .chain_update(self.context)
                .chain_update(&envelope[..segmented::HEADER_LEN])
                .finalize();
            u128::from_le_bytes(digest[..16].try_into().expect("16 of 32 bytes"))
        };
        Ok(Some(Arc::new(SegmentedSource {
            envelope,
            key,
            identity,
        })))
    }

    async fn exists(&self, address: &str) -> fluree_db_core::error::Result<bool> {
        // Pass through - existence check doesn't need decryption
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::error::Result<Vec<String>> {
        // Pass through - listing doesn't need encryption
        self.inner.list_prefix(prefix).await
    }

    /// Reads come back decrypted, so a read-through disk cache of them
    /// would be a plaintext copy of the ledger outside the encrypted tier.
    fn permits_plaintext_cache(&self) -> bool {
        false
    }

    fn is_remote(&self) -> bool {
        self.inner.is_remote()
    }

    fn encryption_admin(&self) -> Option<Arc<dyn EncryptionAdmin>> {
        Some(Arc::new(self.clone()))
    }
}

/// The rotation surface: classify a blob by the key its header names, and
/// re-envelope it under the current key in place.
#[async_trait]
impl<S, K> EncryptionAdmin for EncryptedStorage<S, K>
where
    S: StorageRead + StorageWrite + Clone + Send + Sync + 'static,
    K: KeyProvider + 'static,
{
    fn key_ids(&self) -> Vec<u32> {
        self.keys.key_ids()
    }

    fn current_key_id(&self) -> u32 {
        self.keys.current_key().id()
    }

    async fn key_id_at(&self, address: &str) -> fluree_db_core::error::Result<Option<u32>> {
        // Header only: a ranged read on the inner storage, no decryption.
        // An address that is empty or gone (a lock file, or a blob collected
        // between listing and this read) holds no envelope.
        let prefix = match self
            .inner
            .read_byte_range(address, 0..HEADER_LEN as u64)
            .await
        {
            Ok(prefix) => prefix,
            Err(fluree_db_core::error::Error::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        match key_id_of_header(&prefix) {
            Ok(id) => Ok(Some(id)),
            // Not an envelope: plaintext left by an unencrypted run, or a
            // nameservice record. Reported, never rewritten.
            Err(EncryptionError::InvalidFormat { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn reencrypt(&self, address: &str) -> fluree_db_core::error::Result<Option<u64>> {
        let envelope = self.inner.read_bytes(address).await?;
        let current = self.keys.current_key();
        if key_id_of_header(&envelope)? == current.id() {
            return Ok(None);
        }
        let plaintext = self.decrypt(&envelope)?;
        let rewritten = self.encrypt(&plaintext)?;
        self.inner.write_bytes(address, &rewritten).await?;

        // Read back through the same path a reader takes: the write must
        // decrypt under the current key to exactly what was there before.
        let check = self.inner.read_bytes(address).await?;
        if key_id_of_header(&check)? != current.id() || self.decrypt(&check)? != plaintext {
            return Err(fluree_db_core::error::Error::storage(format!(
                "re-encryption of {address} did not read back under the current key"
            )));
        }
        Ok(Some(plaintext.len() as u64))
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

    async fn delete_many(
        &self,
        addresses: &[String],
    ) -> Vec<(String, fluree_db_core::error::Error)> {
        self.inner.delete_many(addresses).await
    }

    async fn sync(&self) -> fluree_db_core::error::Result<()> {
        self.inner.sync().await
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
        namespace: &fluree_db_core::StorageNamespace,
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
            .content_write_bytes_with_hash(kind, namespace, content_hash_hex, &encrypted)
            .await?;

        // Restore plaintext size (inner storage reports encrypted size)
        result.size_bytes = plaintext_size;

        Ok(result)
    }

    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        namespace: &fluree_db_core::StorageNamespace,
        bytes: &[u8],
    ) -> fluree_db_core::error::Result<ContentWriteResult> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, namespace, &hash_hex, bytes)
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
        S: StorageCas + 'static,
        K: KeyProvider + 'static,
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
                + Sync
                + 'static,
            T: Send + 'static,
        {
            // The closure may run off this task, so it owns its keys.
            let keys = Arc::clone(&self.keys);
            let write = self.write;
            self.inner
                .compare_and_swap(address, move |current_encrypted| {
                    // Decrypt current value if present
                    let current_plaintext = current_encrypted
                        .map(|enc| {
                            Self::decrypt_with(&keys, enc)
                                .map_err(|e| StorageExtError::other(e.to_string()))
                        })
                        .transpose()?;

                    let current_ref = current_plaintext.as_deref();
                    match f(current_ref)? {
                        CasAction::Write(plaintext) => {
                            let encrypted = Self::encrypt_with(&keys, write, &plaintext)
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

    /// Local reads decrypt, like fetched ones: a reader handed the inner
    /// storage's envelope would parse ciphertext as an index artifact.
    #[tokio::test]
    async fn get_local_serves_plaintext() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());
        encrypted
            .write_bytes("test/data", b"leaf bytes")
            .await
            .unwrap();

        let local = encrypted.get_local("test/data").unwrap().expect("local");
        assert_eq!(local, b"leaf bytes");
        assert_ne!(
            storage.get_local("test/data").unwrap().unwrap(),
            b"leaf bytes",
            "the inner storage holds ciphertext"
        );
        assert!(encrypted.get_local("test/absent").unwrap().is_none());

        let other_key = EncryptedStorage::new(
            storage,
            StaticKeyProvider::new(EncryptionKey::new([0x02; 32], 1)),
        );
        assert!(
            other_key.get_local("test/data").is_err(),
            "a local copy that fails to decrypt is an error, not a miss"
        );
    }

    /// Mixed versions on one store: a version 1 envelope reads as before and
    /// has no ranged source; a segmented one serves ranges that match a full
    /// read, through both the local source and `read_byte_range`.
    #[tokio::test]
    async fn segmented_and_whole_envelopes_read_side_by_side() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());
        let plain: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();

        encrypted.write_bytes("seg", &plain).await.unwrap();
        let raw = storage.read_bytes("seg").await.unwrap();
        assert_eq!(raw[4], segmented::VERSION, "new writes are segmented");
        let v1 = EncryptedStorage::<MemoryStorage, StaticKeyProvider>::encrypt_whole(
            &encrypted.keys,
            &plain,
        )
        .unwrap();
        storage.write_bytes("whole", &v1).await.unwrap();

        for address in ["seg", "whole"] {
            assert_eq!(encrypted.read_bytes(address).await.unwrap(), plain);
            assert_eq!(encrypted.get_local(address).unwrap().unwrap(), plain);
            assert_eq!(
                encrypted
                    .read_byte_range(address, 70_000..140_123)
                    .await
                    .unwrap(),
                plain[70_000..140_123]
            );
        }
        assert!(encrypted.get_local_ranged("whole").unwrap().is_none());
        assert!(encrypted.get_local_ranged("absent").unwrap().is_none());

        let source = encrypted.get_local_ranged("seg").unwrap().expect("ranged");
        assert_eq!(source.len(), plain.len() as u64);
        assert_eq!(source.read_unit(), 1 << segmented::DEFAULT_SEGMENT_LOG2);
        for range in [0..10, 65_530..65_550, 299_990..u64::MAX, 400_000..400_001] {
            let end = (range.end as usize).min(plain.len());
            let start = (range.start as usize).min(end);
            assert_eq!(source.read_range(range.clone()).unwrap(), plain[start..end]);
        }
    }

    /// A cache keyed by source identity serves a reader only what was
    /// decrypted for that storage and that envelope: re-encryption and a
    /// second storage over the same bytes both get identities of their own.
    #[tokio::test]
    async fn ranged_source_identity_is_scoped_to_storage_and_envelope() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());
        encrypted.write_bytes("seg", &[1u8; 1000]).await.unwrap();
        let identity = |s: &EncryptedStorage<MemoryStorage, StaticKeyProvider>| {
            s.get_local_ranged("seg").unwrap().unwrap().cache_identity()
        };
        let first = identity(&encrypted);
        assert_eq!(identity(&encrypted), first, "stable for one envelope");
        assert_eq!(
            identity(&encrypted.clone()),
            first,
            "clones share a context"
        );
        let other = EncryptedStorage::new(storage.clone(), test_provider());
        assert_ne!(identity(&other), first, "another storage instance");
        encrypted.write_bytes("seg", &[1u8; 1000]).await.unwrap();
        assert_ne!(
            identity(&encrypted),
            first,
            "the same plaintext re-encrypted"
        );
    }

    /// A ranged source reads the envelope it opened: replacing the object
    /// (as re-encryption does) changes nothing for a reader mid-operation.
    #[tokio::test]
    async fn a_ranged_source_keeps_the_envelope_it_opened() {
        let dir = tempfile::tempdir().unwrap();
        let file = fluree_db_core::FileStorage::new(dir.path());
        let encrypted = EncryptedStorage::new(file, test_provider());
        let address = "fluree:file://blob";
        let first = vec![1u8; 200_000];
        encrypted.write_bytes(address, &first).await.unwrap();
        let source = encrypted
            .get_local_ranged(address)
            .unwrap()
            .expect("ranged");

        encrypted
            .write_bytes(address, &[2u8; 200_000])
            .await
            .unwrap();
        assert_eq!(source.read_range(100_000..100_010).unwrap(), [1u8; 10]);
        assert_eq!(
            encrypted.read_byte_range(address, 0..3).await.unwrap(),
            [2u8; 3]
        );
    }

    /// A local copy that fails to decrypt is an error, never a miss: a
    /// wrong key, and a segment corrupted under a range read.
    #[tokio::test]
    async fn ranged_reads_fail_loudly() {
        let storage = MemoryStorage::new();
        let encrypted = EncryptedStorage::new(storage.clone(), test_provider());
        encrypted.write_bytes("seg", &[7u8; 100_000]).await.unwrap();

        let other_key = EncryptedStorage::new(
            storage.clone(),
            StaticKeyProvider::new(EncryptionKey::new([0x02; 32], 1)),
        );
        let source = other_key.get_local_ranged("seg").unwrap().expect("opens");
        assert!(source.read_range(0..10).is_err());
        let unknown = EncryptedStorage::new(
            storage.clone(),
            StaticKeyProvider::new(EncryptionKey::new([0x42; 32], 9)),
        );
        assert!(unknown.get_local_ranged("seg").is_err(), "unknown key id");

        let mut raw = storage.read_bytes("seg").await.unwrap();
        raw[segmented::HEADER_LEN + 70_000] ^= 1;
        storage.write_bytes("seg", &raw).await.unwrap();
        let source = encrypted.get_local_ranged("seg").unwrap().expect("opens");
        assert_eq!(source.read_range(0..10).unwrap(), [7u8; 10]);
        assert!(source.read_range(69_990..70_010).is_err());
        assert!(encrypted.read_bytes("seg").await.is_err());

        raw.truncate(raw.len() - 1);
        storage.write_bytes("seg", &raw).await.unwrap();
        assert!(
            encrypted.get_local_ranged("seg").is_err(),
            "a truncated envelope does not open"
        );
    }

    /// Rotation re-envelopes segmented objects and reports them by key.
    #[tokio::test]
    async fn rotation_rewrites_a_segmented_envelope() {
        let storage = MemoryStorage::new();
        let old = || EncryptionKey::new([0x11; 32], 1);
        let new = EncryptionKey::new([0x22; 32], 2);
        let writer = EncryptedStorage::new(
            storage.clone(),
            crate::MultiKeyProvider::new(vec![old()], 1).unwrap(),
        );
        writer.write_bytes("seg", &[5u8; 70_000]).await.unwrap();

        let rotating = EncryptedStorage::new(
            storage.clone(),
            crate::MultiKeyProvider::new(vec![old(), new], 2).unwrap(),
        );
        assert_eq!(rotating.key_id_at("seg").await.unwrap(), Some(1));
        assert_eq!(rotating.reencrypt("seg").await.unwrap(), Some(70_000));
        assert_eq!(rotating.key_id_at("seg").await.unwrap(), Some(2));
        assert_eq!(rotating.reencrypt("seg").await.unwrap(), None);
        assert_eq!(rotating.read_bytes("seg").await.unwrap(), vec![5u8; 70_000]);
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
            .content_write_bytes(
                ContentKind::Commit,
                &fluree_db_core::StorageNamespace::parse_legacy("mydb:main").unwrap(),
                plaintext,
            )
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

    /// The disk cache is attached where a store stack is assembled, from
    /// the storage's answers; type erasure must not lose them. An encrypted
    /// remote stack gets its fetches coalesced and nothing written to disk;
    /// the same stack without the key is cached (the non-vacuity control).
    #[tokio::test]
    async fn an_encrypted_remote_backend_writes_nothing_to_the_disk_cache() {
        use fluree_db_core::{ContentKind, Storage, StorageBackend};
        use std::sync::Arc;

        let plain = MemoryStorage::new();
        assert!(plain.permits_plaintext_cache());
        let encrypted = EncryptedStorage::new(plain.clone(), test_provider());
        assert!(!encrypted.permits_plaintext_cache());
        let erased: Arc<dyn Storage> = Arc::new(encrypted);
        assert!(!erased.permits_plaintext_cache());

        for encrypt in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let remote = MemoryStorage::new().simulating_remote();
            let storage: Arc<dyn Storage> = if encrypt {
                Arc::new(EncryptedStorage::new(remote, test_provider()))
            } else {
                Arc::new(remote)
            };
            let backend = StorageBackend::Managed(storage).with_disk_cache(dir.path());
            let cs = backend
                .content_store(&fluree_db_core::StorageNamespace::parse_legacy("db:main").unwrap());
            let id = cs.put(ContentKind::IndexLeaf, b"leaf bytes").await.unwrap();
            assert_eq!(cs.get(&id).await.unwrap(), b"leaf bytes");

            let written = std::fs::read_dir(dir.path()).unwrap().count();
            if encrypt {
                assert_eq!(written, 0, "plaintext spilled to the disk cache");
            } else {
                assert_eq!(written, 1, "a plaintext remote read is cached");
            }
        }
    }
}
