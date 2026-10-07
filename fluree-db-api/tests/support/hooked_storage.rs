//! `MemoryStorage` with test hooks around its reads and writes.

use async_trait::async_trait;
use fluree_db_core::storage::ContentWriteResult;
use fluree_db_core::{
    ContentAddressedWrite, ContentKind, EncryptionAdmin, MemoryStorage, RemoteObject,
    StorageMethod, StorageRead, StorageWrite,
};
use std::fmt::Debug;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Hooks a [`HookedStorage`] runs around the operations it forwards.
///
/// Every hook defaults to doing nothing.
#[async_trait]
pub trait StorageHooks: Debug + Send + Sync + 'static {
    /// Runs before every read of `address`, whole or ranged.
    async fn before_read(&self, _address: &str) {}

    /// Runs before every content-addressed write of `kind`.
    async fn before_content_write(&self, _kind: ContentKind) {}

    /// Runs after every successful write, with the address written.
    fn after_write(&self, _address: &str) {}
}

/// `MemoryStorage` that runs `H` around the operations it forwards.
///
/// Clones share the storage and the hooks.
#[derive(Debug)]
pub struct HookedStorage<H> {
    inner: MemoryStorage,
    hooks: Arc<H>,
}

impl<H> Clone for HookedStorage<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            hooks: Arc::clone(&self.hooks),
        }
    }
}

impl<H: StorageHooks> HookedStorage<H> {
    /// Hooks over a new, empty `MemoryStorage`.
    pub fn new(hooks: H) -> Self {
        Self::over(MemoryStorage::new(), hooks)
    }

    /// Hooks over `inner`, which keeps sharing its data with its other clones.
    pub fn over(inner: MemoryStorage, hooks: H) -> Self {
        Self {
            inner,
            hooks: Arc::new(hooks),
        }
    }

    pub fn hooks(&self) -> &H {
        &self.hooks
    }
}

#[async_trait]
impl<H: StorageHooks> StorageRead for HookedStorage<H> {
    fn permits_plaintext_cache(&self) -> bool {
        self.inner.permits_plaintext_cache()
    }

    fn is_remote(&self) -> bool {
        self.inner.is_remote()
    }

    /// Not a fetch: the bytes are shared, not copied, so no hook runs.
    fn get_local(
        &self,
        address: &str,
    ) -> fluree_db_core::Result<Option<fluree_db_core::ContentBytes>> {
        self.inner.get_local(address)
    }

    fn encryption_admin(&self) -> Option<Arc<dyn EncryptionAdmin>> {
        self.inner.encryption_admin()
    }

    async fn read_bytes(&self, address: &str) -> fluree_db_core::Result<Vec<u8>> {
        self.hooks.before_read(address).await;
        self.inner.read_bytes(address).await
    }

    async fn exists(&self, address: &str) -> fluree_db_core::Result<bool> {
        self.inner.exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> fluree_db_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn read_byte_range(
        &self,
        address: &str,
        range: Range<u64>,
    ) -> fluree_db_core::Result<Vec<u8>> {
        self.hooks.before_read(address).await;
        self.inner.read_byte_range(address, range).await
    }

    fn supports_ranged_reads(&self) -> bool {
        self.inner.supports_ranged_reads()
    }

    async fn list_prefix_with_metadata(
        &self,
        prefix: &str,
    ) -> fluree_db_core::Result<Vec<RemoteObject>> {
        self.inner.list_prefix_with_metadata(prefix).await
    }
}

#[async_trait]
impl<H: StorageHooks> StorageWrite for HookedStorage<H> {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> fluree_db_core::Result<()> {
        self.inner.write_bytes(address, bytes).await?;
        self.hooks.after_write(address);
        Ok(())
    }

    async fn delete(&self, address: &str) -> fluree_db_core::Result<()> {
        self.inner.delete(address).await
    }
}

#[async_trait]
impl<H: StorageHooks> ContentAddressedWrite for HookedStorage<H> {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> fluree_db_core::Result<ContentWriteResult> {
        self.hooks.before_content_write(kind).await;
        let result = self
            .inner
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await?;
        self.hooks.after_write(&result.address);
        Ok(result)
    }
}

impl<H: StorageHooks> StorageMethod for HookedStorage<H> {
    fn storage_method(&self) -> &str {
        self.inner.storage_method()
    }
}

/// Storage hooks that count index artifact writes by the address written.
#[derive(Debug, Default)]
pub struct IndexWriteCounts {
    index_leaf_writes: AtomicU64,
    index_branch_writes: AtomicU64,
    index_root_writes: AtomicU64,
}

impl IndexWriteCounts {
    /// `(leaves, branches, roots)` written so far.
    pub fn snapshot_counts(&self) -> (u64, u64, u64) {
        (
            self.index_leaf_writes.load(Ordering::Relaxed),
            self.index_branch_writes.load(Ordering::Relaxed),
            self.index_root_writes.load(Ordering::Relaxed),
        )
    }
}

impl StorageHooks for IndexWriteCounts {
    fn after_write(&self, address: &str) {
        if address.contains("/index/objects/leaves/") {
            self.index_leaf_writes.fetch_add(1, Ordering::Relaxed);
        } else if address.contains("/index/objects/branches/") {
            self.index_branch_writes.fetch_add(1, Ordering::Relaxed);
        } else if address.contains("/index/roots/") {
            self.index_root_writes.fetch_add(1, Ordering::Relaxed);
        }
    }
}
