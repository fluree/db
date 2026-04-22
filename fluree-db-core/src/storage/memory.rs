//! In-memory storage backend
//!
//! Provides [`MemoryStorage`] (implements the low-level storage traits) and
//! [`MemoryContentStore`] (a CID-based content store backed by a `HashMap`).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use crate::error::Result;
use crate::{
    content_address, CasAction, CasOutcome, ContentAddressedWrite, ContentId, ContentKind,
    ContentStore, ContentWriteResult, StorageCas, StorageExtError, StorageExtResult, StorageMethod,
    StorageRead, StorageWrite,
};

/// Storage method for in-memory storage.
pub const STORAGE_METHOD_MEMORY: &str = "memory";

/// Simple in-memory storage
///
/// This implementation stores data in a HashMap with interior mutability
/// (via `Arc<RwLock<...>>`) to support both reading and writing.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    /// Create a new empty memory storage
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Insert data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert(&self, address: impl Into<String>, data: Vec<u8>) {
        self.data.write().insert(address.into(), data);
    }

    /// Insert JSON data at the given address
    ///
    /// Note: This method takes `&self` (not `&mut self`) due to interior mutability.
    pub fn insert_json<T: serde::Serialize>(
        &self,
        address: impl Into<String>,
        value: &T,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.insert(address, bytes);
        Ok(())
    }

    /// Remove data at the given address, if present.
    pub fn remove(&self, address: impl Into<String>) {
        self.data.write().remove(&address.into());
    }
}

#[async_trait]
impl StorageRead for MemoryStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        self.data
            .read()
            .get(address)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(address))
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        Ok(self.data.read().contains_key(address))
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let data = self.data.read();
        Ok(data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let data = self.data.read();
        let full = data
            .get(address)
            .ok_or_else(|| crate::error::Error::not_found(address))?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }
}

#[async_trait]
impl StorageWrite for MemoryStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        self.insert(address, bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, address: &str) -> Result<()> {
        // Idempotent: ok even if not found
        self.remove(address);
        Ok(())
    }
}

impl StorageMethod for MemoryStorage {
    fn storage_method(&self) -> &str {
        STORAGE_METHOD_MEMORY
    }
}

#[async_trait]
impl ContentAddressedWrite for MemoryStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let address = content_address(STORAGE_METHOD_MEMORY, kind, ledger_id, content_hash_hex);
        self.insert(&address, bytes.to_vec());
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

#[async_trait]
impl StorageCas for MemoryStorage {
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool> {
        let mut data = self.data.write();
        if data.contains_key(address) {
            Ok(false)
        } else {
            data.insert(address.into(), bytes.to_vec());
            Ok(true)
        }
    }

    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send,
    {
        let mut data = self.data.write();
        let current = data.get(address).map(std::vec::Vec::as_slice);
        match f(current)? {
            CasAction::Write(new_bytes) => {
                data.insert(address.to_string(), new_bytes);
                Ok(CasOutcome::Written)
            }
            CasAction::Abort(t) => Ok(CasOutcome::Aborted(t)),
        }
    }
}

/// In-memory content store keyed by `ContentId`.
///
/// This is the CID-first counterpart to [`MemoryStorage`].
#[derive(Debug, Clone)]
pub struct MemoryContentStore {
    data: Arc<RwLock<HashMap<ContentId, Vec<u8>>>>,
}

impl Default for MemoryContentStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryContentStore {
    /// Create a new empty in-memory content store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ContentStore for MemoryContentStore {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        Ok(self.data.read().contains_key(id))
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        self.data
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| crate::error::Error::not_found(id.to_string()))
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        self.data.write().insert(id.clone(), bytes.to_vec());
        Ok(id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        if !id.verify(bytes) {
            return Err(crate::error::Error::storage(format!(
                "CID verification failed: provided CID {id} does not match bytes"
            )));
        }
        self.data.write().insert(id.clone(), bytes.to_vec());
        Ok(())
    }

    async fn release(&self, id: &ContentId) -> Result<()> {
        self.data.write().remove(id);
        Ok(())
    }

    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let data = self.data.read();
        let full = data
            .get(id)
            .ok_or_else(|| crate::error::Error::not_found(id.to_string()))?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{decode_json, StorageContentStore};

    #[tokio::test]
    async fn test_memory_storage() {
        let storage = MemoryStorage::new();
        storage.insert("test/path", b"hello world".to_vec());

        let bytes = storage.read_bytes("test/path").await.unwrap();
        assert_eq!(bytes, b"hello world");

        assert!(storage.exists("test/path").await.unwrap());
        assert!(!storage.exists("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_not_found() {
        let storage = MemoryStorage::new();
        let result = storage.read_bytes("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_storage_json() {
        let storage = MemoryStorage::new();

        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestData {
            name: String,
            value: i32,
        }

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        storage.insert_json("test.json", &data).unwrap();

        let bytes = storage.read_bytes("test.json").await.unwrap();
        let parsed: TestData = decode_json(&bytes).unwrap();

        assert_eq!(parsed, data);
    }

    #[tokio::test]
    async fn test_memory_storage_write() {
        let storage = MemoryStorage::new();

        // Test StorageWrite trait
        storage
            .write_bytes("write/test", b"written data")
            .await
            .unwrap();

        let bytes = storage.read_bytes("write/test").await.unwrap();
        assert_eq!(bytes, b"written data");

        // Test idempotency - writing same content again should succeed
        storage
            .write_bytes("write/test", b"overwritten")
            .await
            .unwrap();
        let bytes = storage.read_bytes("write/test").await.unwrap();
        assert_eq!(bytes, b"overwritten");
    }

    #[tokio::test]
    async fn test_memory_storage_delete() {
        let storage = MemoryStorage::new();
        storage.insert("delete/test", b"data".to_vec());

        assert!(storage.exists("delete/test").await.unwrap());
        storage.delete("delete/test").await.unwrap();
        assert!(!storage.exists("delete/test").await.unwrap());

        // Idempotent: deleting non-existent is OK
        storage.delete("delete/test").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_storage_list_prefix() {
        let storage = MemoryStorage::new();
        storage.insert("prefix/a", b"a".to_vec());
        storage.insert("prefix/b", b"b".to_vec());
        storage.insert("other/c", b"c".to_vec());

        let mut results = storage.list_prefix("prefix/").await.unwrap();
        results.sort();
        assert_eq!(results, vec!["prefix/a", "prefix/b"]);
    }

    #[tokio::test]
    async fn test_memory_storage_content_write_layout() {
        let storage = MemoryStorage::new();
        let bytes = br#"{"hello":"world"}"#;
        let res = storage
            .content_write_bytes(ContentKind::Commit, "mydb:main", bytes)
            .await
            .unwrap();

        assert!(res.address.starts_with("fluree:memory://mydb/main/commit/"));
        assert!(res.address.ends_with(".fcv2"));
        assert_eq!(res.size_bytes, bytes.len());

        let roundtrip = storage.read_bytes(&res.address).await.unwrap();
        assert_eq!(roundtrip, bytes);
    }

    // ========================================================================
    // ContentStore tests (CID-first)
    // ========================================================================

    #[tokio::test]
    async fn test_memory_content_store_put_get() {
        let store = MemoryContentStore::new();
        let data = b"content store test";

        let id = store.put(ContentKind::Commit, data).await.unwrap();
        assert!(store.has(&id).await.unwrap());

        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_memory_content_store_not_found() {
        let store = MemoryContentStore::new();
        let fake_id = ContentId::new(ContentKind::Commit, b"nonexistent");
        assert!(!store.has(&fake_id).await.unwrap());
        assert!(store.get(&fake_id).await.is_err());
    }

    #[tokio::test]
    async fn test_memory_content_store_put_with_id() {
        let store = MemoryContentStore::new();
        let data = b"verified content";
        let id = ContentId::new(ContentKind::Txn, data);

        // Correct bytes should succeed
        store.put_with_id(&id, data).await.unwrap();
        assert_eq!(store.get(&id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_memory_content_store_put_with_id_rejects_mismatch() {
        let store = MemoryContentStore::new();
        let id = ContentId::new(ContentKind::Txn, b"original");

        // Wrong bytes should be rejected
        let result = store.put_with_id(&id, b"tampered").await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("CID verification failed"));
    }

    #[tokio::test]
    async fn test_bridge_adapter_roundtrip() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage.clone(), "mydb:main", "memory");

        let data = b"bridge test data";
        let id = store.put(ContentKind::Commit, data).await.unwrap();

        // Should be retrievable via ContentStore
        assert!(store.has(&id).await.unwrap());
        let retrieved = store.get(&id).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_bridge_adapter_put_with_id() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "mydb:main", "memory");

        let data = b"bridge put_with_id test";
        let id = ContentId::new(ContentKind::IndexRoot, data);

        store.put_with_id(&id, data).await.unwrap();
        assert_eq!(store.get(&id).await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_bridge_adapter_put_with_id_rejects_mismatch() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "mydb:main", "memory");

        let id = ContentId::new(ContentKind::IndexRoot, b"real data");
        let result = store.put_with_id(&id, b"fake data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bridge_adapter_cid_matches_content_id_new() {
        let storage = MemoryStorage::new();
        let store = StorageContentStore::new(storage, "test:main", "memory");

        let data = b"cid consistency check";
        let id_from_store = store.put(ContentKind::Commit, data).await.unwrap();
        let id_from_new = ContentId::new(ContentKind::Commit, data);

        assert_eq!(id_from_store, id_from_new);
    }
}
