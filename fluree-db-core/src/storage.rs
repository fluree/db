//! Storage traits and content-addressing utilities.
//!
//! This module defines the runtime-agnostic storage traits that backends must
//! implement, the content-addressed [`ContentStore`] abstraction built on top
//! of them, and shared helpers for address generation and hashing.
//!
//! ## Submodules
//!
//! - [`memory`]: In-memory backend ([`MemoryStorage`], [`MemoryContentStore`])
//! - [`file`]: Filesystem backend behind the `native` feature ([`FileStorage`])

/// When a write to `FileStorage` is reported complete.
///
/// Both settings are atomic — a reader never observes a partial file either
/// way. They differ in what survives the machine losing power.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Durability {
    /// Report complete once the bytes and the directory entry naming them have
    /// been flushed to the device. Survives power loss; costs an fsync of the
    /// staged file and one of its parent directory per write.
    #[default]
    Sync,
    /// Report complete once the bytes reach the OS page cache. Survives process
    /// death, but a power loss or kernel panic can lose writes already reported
    /// as committed.
    PageCache,
}

// The env/sync helpers are consumed only by the native `FileStorage` (gated
// on `native` + non-wasm); the enum itself is part of the portable config API.
#[cfg_attr(
    not(all(feature = "native", not(target_arch = "wasm32"))),
    allow(dead_code)
)]
impl Durability {
    /// Environment variable selecting the default for new `FileStorage`.
    pub const ENV_VAR: &'static str = "FLUREE_STORAGE_FSYNC";

    pub(crate) fn syncs(&self) -> bool {
        matches!(self, Durability::Sync)
    }

    /// Default read from [`Self::ENV_VAR`], falling back to [`Self::Sync`] when
    /// unset or unrecognized.
    ///
    /// Read once per storage construction rather than per write, so tests set
    /// the field through `FileStorage::with_durability` and never race on
    /// process environment.
    fn from_env() -> Self {
        Self::from_env_override().unwrap_or_default()
    }

    /// The setting [`Self::ENV_VAR`] asks for, or `None` when it is unset.
    ///
    /// Distinguishing "unset" from "set to on" is what lets configuration
    /// supply a value that an operator can still override for one run.
    pub fn from_env_override() -> Option<Self> {
        std::env::var(Self::ENV_VAR)
            .ok()
            .as_deref()
            .map(|v| Self::parse(Some(v)))
    }

    /// Resolve the setting for a storage instance.
    ///
    /// Environment beats configuration beats the default, matching the
    /// precedence documented in `docs/operations/configuration.md` — a checked-in
    /// config file never outranks an operator's one-off override.
    pub fn resolve(configured: Option<Self>) -> Self {
        Self::resolve_from(Self::from_env_override(), configured)
    }

    /// Pure half of [`Self::resolve`], so the precedence rule is testable
    /// without touching process environment.
    fn resolve_from(env: Option<Self>, configured: Option<Self>) -> Self {
        env.or(configured).unwrap_or_default()
    }

    /// Pure half of [`Self::from_env`], so the accepted spellings are testable
    /// without touching process environment.
    fn parse(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            Some(v) if matches!(v.as_str(), "0" | "false" | "off" | "no") => Durability::PageCache,
            _ => Durability::Sync,
        }
    }

    /// Parse a configuration mode name (`sync` / `page-cache`).
    ///
    /// Named modes rather than the environment variable's boolean: a config
    /// file is read to understand a deployment, and a name says what it does.
    /// Returns `None` for an unrecognized value so the caller can reject it
    /// rather than silently pick a durability the operator did not ask for.
    pub fn from_mode_name(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "sync" | "fsync" => Some(Durability::Sync),
            "page-cache" | "pagecache" => Some(Durability::PageCache),
            _ => None,
        }
    }
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
mod file;
mod memory;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use file::{FileStorage, STORAGE_METHOD_FILE};
pub use memory::{MemoryContentStore, MemoryStorage, STORAGE_METHOD_MEMORY};

use crate::address_path::{ledger_id_to_path_prefix, shared_prefix_for_path};
use crate::error::Result;
use async_trait::async_trait;
use sha2::Digest;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

// ============================================================================
// Read Hints
// ============================================================================

/// Hint to storage implementation about expected content type
///
/// This enum allows callers to signal format preferences to storage implementations.
/// The default implementation ignores the hint and returns raw bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadHint {
    /// Any bytes format (default, no special negotiation)
    ///
    /// Storage returns raw bytes as stored. This is the default behavior
    /// and matches the semantics of `read_bytes()`.
    #[default]
    AnyBytes,

    /// Prefer pre-parsed leaf flakes (FLKB format) if the address points to a leaf
    ///
    /// Storage implementations that support content negotiation (e.g., ProxyStorage)
    /// can use this hint to request policy-filtered flakes instead of raw bytes.
    /// If the address is not a leaf or the server doesn't support FLKB, falls back
    /// to raw bytes.
    PreferLeafFlakes,
}

// ============================================================================
// Core Traits
// ============================================================================

/// An object in remote storage with its size, returned by
/// [`StorageRead::list_prefix_with_metadata`].
///
/// `address` is a storage address accepted by [`StorageRead::read_bytes`] /
/// [`StorageRead::read_byte_range`] — it is opaque to callers and remains
/// backend-encapsulated.
#[derive(Debug, Clone)]
pub struct RemoteObject {
    pub address: String,
    pub size_bytes: u64,
}

/// Read-only storage operations
///
/// This trait provides all non-mutating storage operations: reading bytes,
/// checking existence, and listing by prefix.
#[async_trait]
pub trait StorageRead: Debug + Send + Sync {
    /// Read raw bytes from the given address
    ///
    /// The address format is typically:
    /// `fluree:{identifier}:{method}://{path}`
    ///
    /// Returns `Error::NotFound` if the resource doesn't exist.
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>>;

    /// Read bytes with a format hint
    ///
    /// This method allows callers to signal a format preference to storage
    /// implementations that support content negotiation (e.g., ProxyStorage).
    ///
    /// The default implementation ignores the hint and delegates to `read_bytes()`.
    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        let _ = hint; // Default implementation ignores hint
        self.read_bytes(address).await
    }

    /// Check if a resource exists at the given address
    async fn exists(&self, address: &str) -> Result<bool>;

    /// List all objects under a prefix
    ///
    /// Returns all matching keys. May be expensive for large prefixes.
    ///
    /// # Warning
    ///
    /// This can be expensive for large prefixes. Use only for:
    /// - Development/debugging
    /// - Admin operations
    /// - Small, bounded prefixes
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Resolve a CAS address to a local filesystem path, if available.
    ///
    /// Returns `Some(path)` for storage backends where data is already on
    /// the local filesystem (e.g., `FileStorage`). Returns `None` for
    /// remote or in-memory backends.
    ///
    /// Callers use this to avoid redundant copy-to-cache when the data
    /// is already locally accessible.
    fn resolve_local_path(&self, address: &str) -> Option<PathBuf> {
        let _ = address;
        None
    }

    /// Read a byte range from the object at the given address.
    ///
    /// The range is `[start, end)` in bytes. Returns the bytes within
    /// the range, which may be shorter than requested if the object is
    /// smaller than `range.end`.
    ///
    /// The default implementation fetches the full object and slices.
    /// StorageBackends that support native range reads (S3, HTTP) should override
    /// for efficiency.
    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        let full = self.read_bytes(address).await?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }

    /// Whether [`read_byte_range`](Self::read_byte_range) is a **native**
    /// ranged read. The default `read_byte_range` fetches the full object and
    /// slices, so callers that issue many windowed reads over one object
    /// (e.g. streaming import) must check this and fall back to a single
    /// whole-object read when it returns `false` — otherwise each window
    /// re-fetches the entire object.
    fn supports_ranged_reads(&self) -> bool {
        false
    }

    /// List objects under a prefix together with their byte sizes.
    ///
    /// Used by callers (e.g. the bulk-import remote source) that need to
    /// know object sizes up front without issuing a separate HEAD per object.
    ///
    /// Backends that support cheap metadata listing (S3, GCS, etc.) should
    /// override this. The default returns `Other("not supported")` so callers
    /// can fail fast and fall back to a caller-supplied object list.
    async fn list_prefix_with_metadata(&self, prefix: &str) -> Result<Vec<RemoteObject>> {
        let _ = prefix;
        Err(crate::error::Error::other(
            "list_prefix_with_metadata is not supported by this storage backend",
        ))
    }
}

/// Mutating storage operations
///
/// This trait provides basic write and delete operations.
#[async_trait]
pub trait StorageWrite: Debug + Send + Sync {
    /// Write bytes to the given address
    ///
    /// For content-addressed storage, this should be idempotent:
    /// if content already exists at address, this is a no-op.
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()>;

    /// Delete an object by address
    ///
    /// Returns `Ok(())` if the object was deleted or did not exist.
    /// This is idempotent: deleting a non-existent object succeeds.
    /// Only returns an error for actual failures (network, permissions, etc).
    async fn delete(&self, address: &str) -> Result<()>;
}

// ============================================================================
// Content-Addressed Write (Extension)
// ============================================================================

use crate::content_kind::{dict_kind_extension, ContentKind};

/// Result of a storage-owned content write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentWriteResult {
    /// Canonical address that should be persisted/referenced.
    pub address: String,
    /// Content hash (hex sha256 for built-in storages).
    pub content_hash: String,
    /// Number of bytes written (input length).
    pub size_bytes: usize,
}

/// Content-addressed write operations
///
/// This trait extends `StorageWrite` with the ability to write bytes while
/// letting storage determine the address based on content hash.
#[async_trait]
pub trait ContentAddressedWrite: StorageWrite {
    /// Write bytes using a caller-provided content hash (hex).
    ///
    /// This allows higher layers to control the hashing algorithm for certain
    /// content kinds (e.g. commit IDs that intentionally exclude non-deterministic
    /// fields like wall-clock timestamps), while keeping **storage in charge of
    /// layout** and returning the canonical address.
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult>;

    /// Write bytes, computing the content hash automatically (SHA-256).
    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let hash_hex = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash_hex, bytes)
            .await
    }
}

// ============================================================================
// Marker Trait
// ============================================================================

// Well-known storage method identifiers.
// Use these constants instead of bare string literals when matching on `storage_method()`.

/// Storage method for AWS S3 object storage.
pub const STORAGE_METHOD_S3: &str = "s3";

/// Storage method for IPFS (Kubo) content-addressed storage.
pub const STORAGE_METHOD_IPFS: &str = "ipfs";

/// Identifies the storage method/scheme for CID-to-address mapping.
///
/// Every storage backend must declare its method name (e.g.,
/// [`STORAGE_METHOD_FILE`], [`STORAGE_METHOD_MEMORY`], [`STORAGE_METHOD_S3`]).
/// This is used by [`StorageContentStore`] to map `ContentId` values to
/// physical storage addresses via [`content_address`].
///
/// This trait is a supertrait of [`Storage`], ensuring that any type-erased
/// `dyn Storage` (e.g., [`AnyStorage`]) automatically includes `storage_method()`.
pub trait StorageMethod {
    /// Return the storage method identifier (e.g., [`STORAGE_METHOD_FILE`]).
    fn storage_method(&self) -> &str;
}

/// Full storage capability marker
///
/// This trait combines `StorageRead`, `ContentAddressedWrite`, and `StorageMethod`,
/// providing a single bound for storage backends that support all operations.
///
/// Used for type erasure in `AnyStorage`.
pub trait Storage: StorageRead + ContentAddressedWrite + StorageMethod {}
impl<T: StorageRead + ContentAddressedWrite + StorageMethod> Storage for T {}

// ============================================================================
// Arc<dyn Storage> delegation (enables type-erased storage)
// ============================================================================

#[async_trait]
impl StorageRead for Arc<dyn Storage> {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        self.as_ref().read_bytes(address).await
    }

    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        self.as_ref().read_bytes_hint(address, hint).await
    }

    async fn read_byte_range(&self, address: &str, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        self.as_ref().read_byte_range(address, range).await
    }

    fn supports_ranged_reads(&self) -> bool {
        self.as_ref().supports_ranged_reads()
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        self.as_ref().exists(address).await
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.as_ref().list_prefix(prefix).await
    }

    async fn list_prefix_with_metadata(&self, prefix: &str) -> Result<Vec<RemoteObject>> {
        self.as_ref().list_prefix_with_metadata(prefix).await
    }

    fn resolve_local_path(&self, address: &str) -> Option<PathBuf> {
        self.as_ref().resolve_local_path(address)
    }
}

#[async_trait]
impl StorageWrite for Arc<dyn Storage> {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> {
        self.as_ref().write_bytes(address, bytes).await
    }

    async fn delete(&self, address: &str) -> Result<()> {
        self.as_ref().delete(address).await
    }
}

#[async_trait]
impl ContentAddressedWrite for Arc<dyn Storage> {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        self.as_ref()
            .content_write_bytes_with_hash(kind, ledger_id, content_hash_hex, bytes)
            .await
    }
}

impl StorageMethod for Arc<dyn Storage> {
    fn storage_method(&self) -> &str {
        self.as_ref().storage_method()
    }
}

// ============================================================================
// ContentStore Trait (CID-first storage abstraction)
// ============================================================================

use crate::content_id::ContentId;

/// Content-addressed store operating on `ContentId` (CIDv1).
///
/// This trait is the CID-first replacement for the address-string-based
/// `Storage` traits above. The API is purely `id → bytes`: physical
/// layout (ledger namespacing, codec subdirectories, etc.) is the
/// implementation's concern, configured at construction time.
///
/// During the migration period, [`StorageContentStore`] provides a bridge
/// from existing `S: Storage` implementations to this trait.
#[async_trait]
pub trait ContentStore: Debug + Send + Sync {
    /// Check if an object exists by CID.
    async fn has(&self, id: &ContentId) -> Result<bool>;

    /// Retrieve object bytes by CID.
    async fn get(&self, id: &ContentId) -> Result<Vec<u8>>;

    /// Store bytes, computing CID from kind + bytes. Returns the CID.
    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId>;

    /// Store bytes with a caller-provided CID (for imports/replication).
    ///
    /// Implementations MUST call `id.verify(bytes)` and reject mismatches.
    ///
    /// V4 commit blobs can be stored with this method since their CID
    /// is `SHA-256(full blob)`, matching `id.verify(bytes)`.
    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()>;

    /// Resolve a CID to a local filesystem path, if available.
    ///
    /// Returns `Some(path)` for storage backends where data is already on
    /// the local filesystem (e.g., `FileContentStore`). Returns `None` for
    /// remote or in-memory backends.
    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        let _ = id;
        None
    }

    /// Synchronous, non-blocking lookup of already-resident bytes for a CID.
    ///
    /// This is the sync residency tier for targets without a sync→async
    /// bridge (`wasm32`): the binary-index read path consults it instead of
    /// filesystem probes or a bridged CAS fetch, and surfaces a typed
    /// `NeedFetch` miss when it returns `None` so an async caller can fetch
    /// and retry. Implementations must not perform I/O or block — a hit is
    /// an O(1) map lookup returning a shared `Arc` clone (zero copy); on a
    /// backend with no resident tier the default returns `None`.
    ///
    /// Content is immutable (CID-addressed), so implementations may pin and
    /// serve entries indefinitely without invalidation.
    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<std::sync::Arc<[u8]>> {
        let _ = id;
        None
    }

    /// Signal that this content is no longer needed and may be reclaimed.
    ///
    /// Implementations should make a best effort to free the underlying
    /// resources associated with `id`. The content may or may not become
    /// immediately unavailable after this call, depending on the backend.
    ///
    /// Releasing a non-existent CID is **not** an error — implementations
    /// must be idempotent. This allows callers (e.g., GC) to retry without
    /// tracking which releases have already succeeded.
    async fn release(&self, id: &ContentId) -> Result<()>;

    /// Retrieve a byte range from an object by CID.
    ///
    /// The range is `[start, end)` in bytes. Returns the bytes within
    /// the range, which may be shorter than requested if the object is
    /// smaller than `range.end`.
    ///
    /// The default implementation fetches the full object and slices.
    /// StorageBackends that support native range reads (S3, HTTP) should override
    /// for efficiency.
    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let full = self.get(id).await?;
        let start = range.start as usize;
        let end = (range.end as usize).min(full.len());
        if start >= full.len() {
            return Ok(Vec::new());
        }
        Ok(full[start..end].to_vec())
    }
}

// Blanket `ContentStore` impl for `Arc<dyn ContentStore>`, so callers can pass
// a dynamically-dispatched content store anywhere a `C: ContentStore` bound is
// expected.
#[async_trait]
impl ContentStore for Arc<dyn ContentStore> {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        self.as_ref().has(id).await
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        self.as_ref().get(id).await
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        self.as_ref().put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        self.as_ref().put_with_id(id, bytes).await
    }

    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        self.as_ref().resolve_local_path(id)
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<std::sync::Arc<[u8]>> {
        self.as_ref().resolve_cached_bytes(id)
    }

    async fn release(&self, id: &ContentId) -> Result<()> {
        self.as_ref().release(id).await
    }

    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        self.as_ref().get_range(id, range).await
    }
}

// ============================================================================
// StorageContentStore (bridge adapter: existing Storage → ContentStore)
// ============================================================================

/// Bridge adapter that wraps an existing `S: Storage` to provide `ContentStore`.
///
/// This is the critical piece for incremental migration: code that already has
/// `S: Storage` can obtain a `ContentStore` without rewriting storage
/// implementations.
///
/// The adapter is constructed with a ledger scope (`ledger_id`) and a
/// storage method name (e.g., `"file"`, `"memory"`, `"s3"`), which together
/// determine the layout rule for mapping CIDs to legacy address strings.
#[derive(Debug, Clone)]
pub struct StorageContentStore<S: Storage> {
    storage: S,
    ledger_id: String,
    method: String,
}

impl<S: Storage> StorageContentStore<S> {
    /// Create a new bridge adapter.
    ///
    /// # Arguments
    ///
    /// * `storage` - The underlying legacy storage implementation
    /// * `ledger_id` - Ledger identifier (e.g., `"mydb:main"`)
    /// * `method` - Storage method name for address generation (e.g., `"file"`, `"memory"`)
    pub fn new(storage: S, ledger_id: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            storage,
            ledger_id: ledger_id.into(),
            method: method.into(),
        }
    }

    /// Map a CID to an address string using the current layout.
    fn cid_to_address(&self, id: &ContentId) -> Result<String> {
        let kind = id.content_kind().ok_or_else(|| {
            crate::error::Error::storage(format!("unknown codec {} in CID {}", id.codec(), id))
        })?;
        let hex_digest = id.digest_hex();
        let addr = content_address(&self.method, kind, &self.ledger_id, &hex_digest);
        Ok(addr)
    }

    /// For dict blobs, return the pre-global-dicts address where dicts lived
    /// under the per-branch namespace (`mydb/main/index/objects/dicts/{sha}.dict`).
    /// Returns `None` for non-dict CIDs.
    fn legacy_dict_address(&self, id: &ContentId) -> Option<String> {
        legacy_dict_address(&self.method, &self.ledger_id, id)
    }

    /// Index roots were stored with a `.json` extension before the switch to `.fir6`.
    /// Returns `None` for non-IndexRoot CIDs.
    fn legacy_index_root_address(&self, id: &ContentId) -> Option<String> {
        legacy_index_root_address(&self.method, &self.ledger_id, id)
    }
}

/// The pre-global-dicts address, where dict blobs lived under the per-branch
/// namespace (`mydb/main/index/objects/dicts/{sha}.dict`).
///
/// Returns `None` for non-dict CIDs.
pub fn legacy_dict_address(method: &str, ledger_id: &str, id: &ContentId) -> Option<String> {
    if id.codec() != crate::CODEC_FLUREE_DICT_BLOB {
        return None;
    }
    let prefix = ledger_id_prefix_for_path(ledger_id);
    let hex = id.digest_hex();
    Some(format!(
        "fluree:{method}://{prefix}/index/objects/dicts/{hex}.dict"
    ))
}

/// The index-root address from before the `.json` to `.fir6` rename.
///
/// Returns `None` for non-`IndexRoot` CIDs.
fn legacy_index_root_address(method: &str, ledger_id: &str, id: &ContentId) -> Option<String> {
    if id.codec() != crate::CODEC_FLUREE_INDEX_ROOT {
        return None;
    }
    let prefix = ledger_id_prefix_for_path(ledger_id);
    let hex = id.digest_hex();
    Some(format!("fluree:{method}://{prefix}/index/roots/{hex}.json"))
}

/// Every address at which a CID's blob could physically live, current layout
/// first.
///
/// The storage layout has changed twice — dict blobs moved from the per-branch
/// namespace to `@shared`, and index roots moved from `.json` to `.fir6` — and
/// [`ContentStore::get`] falls back through the older forms. Anything deciding
/// that a blob is unreferenced must account for all of them, or it will delete
/// a live blob that exists only at a legacy address.
///
/// Returns an empty vector when the CID's codec is unrecognised. Callers that
/// delete must treat that as "cannot locate this blob" and decline to act, not
/// as "this blob occupies no addresses".
pub fn candidate_addresses(method: &str, ledger_id: &str, id: &ContentId) -> Vec<String> {
    let mut addresses = Vec::new();
    if let Some(kind) = id.content_kind() {
        addresses.push(content_address(method, kind, ledger_id, &id.digest_hex()));
    }
    addresses.extend(legacy_dict_address(method, ledger_id, id));
    addresses.extend(legacy_index_root_address(method, ledger_id, id));
    addresses
}

#[async_trait]
impl<S: Storage + Send + Sync> ContentStore for StorageContentStore<S> {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        let address = self.cid_to_address(id)?;
        if self.storage.exists(&address).await? {
            return Ok(true);
        }
        // Fallback: dicts moved from per-branch to @shared namespace
        if let Some(legacy) = self.legacy_dict_address(id) {
            return self.storage.exists(&legacy).await;
        }
        // Fallback: index roots stored with .json before .fir6 rename
        if let Some(legacy) = self.legacy_index_root_address(id) {
            return self.storage.exists(&legacy).await;
        }
        Ok(false)
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        let address = self.cid_to_address(id)?;
        // Keep the primary miss's message rather than discarding it: it carries
        // the resolved path, and — for a zero-length blob — the reason the
        // backend called it absent. Rebuilding a bare `not_found(address)` here
        // throws both away, and this is the error a caller actually sees.
        let primary = match self.storage.read_bytes(&address).await {
            Ok(bytes) => return Ok(bytes),
            Err(crate::error::Error::NotFound(reason)) => reason,
            Err(e) => return Err(e),
        };
        // Fallback: dicts moved from per-branch to @shared namespace
        if let Some(legacy) = self.legacy_dict_address(id) {
            return self.storage.read_bytes(&legacy).await;
        }
        // Fallback: index roots stored with .json before .fir6 rename
        if let Some(legacy) = self.legacy_index_root_address(id) {
            return self.storage.read_bytes(&legacy).await;
        }
        Err(crate::error::Error::not_found(primary))
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        let id = ContentId::new(kind, bytes);
        let hex_digest = id.digest_hex();
        self.storage
            .content_write_bytes_with_hash(kind, &self.ledger_id, &hex_digest, bytes)
            .await?;
        Ok(id)
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        if !id.verify(bytes) {
            return Err(crate::error::Error::storage(format!(
                "CID verification failed: provided CID {id} does not match bytes"
            )));
        }
        let kind = id.content_kind().ok_or_else(|| {
            crate::error::Error::storage(format!("unknown codec {} in CID {}", id.codec(), id))
        })?;
        // Same address `cid_to_address` derives — both go through
        // `content_address` with this kind — but routed so the write *decides*
        // its durability from the content kind rather than inheriting the
        // instance default. Today's callers are all commits and txns, which
        // land on the same answer either way; a path that ingests index blobs
        // by CID would silently start fsyncing them.
        self.storage
            .content_write_bytes_with_hash(kind, &self.ledger_id, &id.digest_hex(), bytes)
            .await?;
        Ok(())
    }

    async fn release(&self, id: &ContentId) -> Result<()> {
        // Delete every address the blob could occupy, not just the current
        // layout: on a ledger predating the `@shared` dict move or the
        // `.fir6` rename, the only copy sits at a legacy address, and
        // releasing just the canonical one silently reclaims nothing.
        for address in candidate_addresses(&self.method, &self.ledger_id, id) {
            match self.storage.delete(&address).await {
                Ok(()) | Err(crate::error::Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        let address = self.cid_to_address(id).ok()?;
        if let Some(path) = self.storage.resolve_local_path(&address) {
            return Some(path);
        }
        // Fallback: dicts moved from per-branch to @shared namespace
        if let Some(legacy) = self.legacy_dict_address(id) {
            if let Some(path) = self.storage.resolve_local_path(&legacy) {
                return Some(path);
            }
        }
        // Fallback: index roots stored with .json before .fir6 rename
        let legacy = self.legacy_index_root_address(id)?;
        self.storage.resolve_local_path(&legacy)
    }

    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        let address = self.cid_to_address(id)?;
        // Same reason-preservation as `get` above.
        let primary = match self.storage.read_byte_range(&address, range.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(crate::error::Error::NotFound(reason)) => reason,
            Err(e) => return Err(e),
        };
        // Fallback: dicts moved from per-branch to @shared namespace
        if let Some(legacy) = self.legacy_dict_address(id) {
            match self.storage.read_byte_range(&legacy, range.clone()).await {
                Ok(bytes) => return Ok(bytes),
                Err(crate::error::Error::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        // Fallback: index roots stored with .json before .fir6 rename
        if let Some(legacy) = self.legacy_index_root_address(id) {
            return self.storage.read_byte_range(&legacy, range).await;
        }
        Err(crate::error::Error::not_found(primary))
    }
}

/// Convenience constructor for the `StorageContentStore` bridge adapter.
///
/// Wraps an existing `S: Storage` to provide `ContentStore` semantics.
/// The `method` string (e.g., `"file"`, `"memory"`, `"s3"`) must come from
/// the calling layer — typically from connection config or by parsing an
/// existing address via `parse_fluree_address().method`.
pub fn bridge_content_store<S: Storage>(
    storage: S,
    ledger_id: &str,
    method: &str,
) -> StorageContentStore<S> {
    StorageContentStore::new(storage, ledger_id, method)
}

/// Construct a `ContentStore` from a `Storage` backend using its declared method.
///
/// This is the preferred way to obtain a `ContentStore` — the method is derived
/// from the storage's [`StorageMethod::storage_method()`] implementation, so
/// callers never need to supply a method string manually.
///
/// The `namespace_id` is typically a ledger ID (e.g., `"mydb:main"`) or a
/// graph source ID (e.g., `"my-search:main"`) — it determines the CAS
/// namespace prefix for physical key layout.
pub fn content_store_for<S: Storage>(storage: S, namespace_id: &str) -> StorageContentStore<S> {
    let method = storage.storage_method().to_string();
    StorageContentStore::new(storage, namespace_id, method)
}

// ============================================================================
// StorageBackend (unified storage backend abstraction)
// ============================================================================

/// Unified storage backend.
///
/// Represents the two kinds of storage backends Fluree supports:
///
/// - **Managed**: Address-based backends (file, S3, memory) where Fluree
///   controls the full data lifecycle including deletion. These implement
///   the [`Storage`] trait and get content-addressing via
///   [`StorageContentStore`].
///
/// - **Permanent**: Natively content-addressed backends (IPFS) where data
///   is append-only and cannot be deleted. These implement [`ContentStore`]
///   directly.
///
/// # Content store access
///
/// Both variants can produce an `Arc<dyn ContentStore>` scoped to a ledger
/// via [`content_store`]. For `Managed`, this constructs a
/// [`StorageContentStore`] bridge; for `Permanent`, it returns the inner
/// store directly (the `namespace_id` is ignored since the backend handles
/// its own addressing).
///
/// # Admin operations
///
/// Only `Managed` backends support admin operations (deletion, listing).
/// Use [`admin_storage`] to get the underlying raw storage, if available.
///
/// [`content_store`]: StorageBackend::content_store
/// [`admin_storage`]: StorageBackend::admin_storage
pub enum StorageBackend {
    /// Storage with full lifecycle control including deletion (file, S3, memory).
    Managed(Arc<dyn Storage>),
    /// Append-only content-addressed storage (IPFS).
    Permanent(Arc<dyn ContentStore>),
    /// Namespace-routed composition: mounted alias prefixes read through
    /// their own backend, everything else uses the default backend.
    Routed(Arc<RoutedBackend>),
}

/// Namespace-prefix routing table for [`StorageBackend::Routed`].
///
/// Each mount claims a ledger-name prefix (e.g. `"acme"` routes
/// `acme/inventory:main` and every namespace under `acme/`). Store selection
/// happens in [`StorageBackend::content_store`], the single point where a
/// ledger's namespace is bound to a store — so ledger loading, branched-store
/// ancestry walks, and default-context reads all route without changes.
///
/// Admin operations (delete, list) apply only to the default backend; mounts
/// are read-only remote content.
pub struct RoutedBackend {
    default: StorageBackend,
    mounts: Vec<(String, StorageBackend)>,
}

impl RoutedBackend {
    /// Compose a default backend with `(prefix, backend)` mounts.
    pub fn new(default: StorageBackend, mounts: Vec<(String, StorageBackend)>) -> Self {
        Self { default, mounts }
    }

    /// Select the backend owning `namespace_id` (a ledger ID or name).
    fn backend_for(&self, namespace_id: &str) -> &StorageBackend {
        self.mounts
            .iter()
            .find(|(prefix, _)| {
                namespace_id
                    .strip_prefix(prefix.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
            })
            .map_or(&self.default, |(_, backend)| backend)
    }
}

impl Debug for RoutedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutedBackend")
            .field("default", &self.default)
            .field(
                "mounts",
                &self.mounts.iter().map(|(p, _)| p).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl StorageBackend {
    /// Create an `Arc<dyn ContentStore>` scoped to the given namespace
    /// (typically a ledger ID).
    ///
    /// For `Managed` backends, this constructs a [`StorageContentStore`] that
    /// maps CIDs to physical addresses under the namespace. For `Permanent`
    /// backends, the inner store is returned directly. For `Routed` backends,
    /// the namespace's owning backend (mount or default) is selected first.
    pub fn content_store(&self, namespace_id: &str) -> Arc<dyn ContentStore> {
        match self {
            StorageBackend::Managed(storage) => {
                Arc::new(content_store_for(storage.clone(), namespace_id))
            }
            StorageBackend::Permanent(store) => Arc::clone(store),
            StorageBackend::Routed(routed) => {
                routed.backend_for(namespace_id).content_store(namespace_id)
            }
        }
    }

    /// Get the underlying raw storage for admin operations (delete, list).
    ///
    /// Returns `Some` for `Managed` backends (and the default backend of
    /// `Routed`), `None` for `Permanent`.
    pub fn admin_storage(&self) -> Option<&dyn Storage> {
        match self {
            StorageBackend::Managed(storage) => Some(storage.as_ref()),
            StorageBackend::Permanent(_) => None,
            StorageBackend::Routed(routed) => routed.default.admin_storage(),
        }
    }

    /// Clone the admin storage as an owned `Arc<dyn Storage>`, if available.
    ///
    /// Returns `Some` for `Managed` backends (and the default backend of
    /// `Routed`), `None` for `Permanent`.
    pub fn admin_storage_cloned(&self) -> Option<Arc<dyn Storage>> {
        match self {
            StorageBackend::Managed(storage) => Some(Arc::clone(storage)),
            StorageBackend::Permanent(_) => None,
            StorageBackend::Routed(routed) => routed.default.admin_storage_cloned(),
        }
    }
}

impl Debug for StorageBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackend::Managed(s) => f.debug_tuple("Managed").field(s).finish(),
            StorageBackend::Permanent(s) => f.debug_tuple("Permanent").field(s).finish(),
            StorageBackend::Routed(s) => f.debug_tuple("Routed").field(s).finish(),
        }
    }
}

impl Clone for StorageBackend {
    fn clone(&self) -> Self {
        match self {
            StorageBackend::Managed(s) => StorageBackend::Managed(Arc::clone(s)),
            StorageBackend::Permanent(s) => StorageBackend::Permanent(Arc::clone(s)),
            StorageBackend::Routed(s) => StorageBackend::Routed(Arc::clone(s)),
        }
    }
}

// ============================================================================
// BranchedContentStore (fallback from branch namespace to parent namespace)
// ============================================================================

/// Content store for branched ledgers that reads from the branch namespace
/// first, falling back through a DAG of parent namespaces.
///
/// Writes always go to the branch's own namespace. Reads try the branch
/// namespace first, then recurse into parent stores. The recursive structure
/// supports both linear branching (branch from branch) and future merge
/// scenarios where a branch has multiple parents.
#[derive(Clone)]
pub struct BranchedContentStore {
    /// Store scoped to this branch's own namespace
    branch_store: Arc<dyn ContentStore>,
    /// Parent stores to fall back to on read misses. Typically one parent
    /// for a simple branch; multiple parents after a merge.
    parents: Vec<BranchedContentStore>,
}

impl BranchedContentStore {
    /// Create a leaf content store with no parents (equivalent to a root branch).
    pub fn leaf(store: Arc<dyn ContentStore>) -> Self {
        Self {
            branch_store: store,
            parents: Vec::new(),
        }
    }

    /// Create a branched content store with parent fallbacks.
    pub fn with_parents(store: Arc<dyn ContentStore>, parents: Vec<Self>) -> Self {
        Self {
            branch_store: store,
            parents,
        }
    }
}

impl Debug for BranchedContentStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BranchedContentStore")
            .field("branch_store", &self.branch_store)
            .field("parents", &self.parents)
            .finish()
    }
}

#[async_trait]
impl ContentStore for BranchedContentStore {
    async fn has(&self, id: &ContentId) -> Result<bool> {
        if self.branch_store.has(id).await? {
            return Ok(true);
        }
        for parent in &self.parents {
            if parent.has(id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn get(&self, id: &ContentId) -> Result<Vec<u8>> {
        match self.branch_store.get(id).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) if self.parents.is_empty() => return Err(e),
            Err(e) if !matches!(e, crate::error::Error::NotFound(_)) => return Err(e),
            Err(_) => {} // not-found with parents available — fall through
        }
        let mut last_err = None;
        for parent in &self.parents {
            match parent.get(id).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| crate::error::Error::not_found(id.to_string())))
    }

    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId> {
        self.branch_store.put(kind, bytes).await
    }

    async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> Result<()> {
        self.branch_store.put_with_id(id, bytes).await
    }

    async fn release(&self, id: &ContentId) -> Result<()> {
        self.branch_store.release(id).await
    }

    fn resolve_local_path(&self, id: &ContentId) -> Option<std::path::PathBuf> {
        self.branch_store
            .resolve_local_path(id)
            .or_else(|| self.parents.iter().find_map(|p| p.resolve_local_path(id)))
    }

    fn resolve_cached_bytes(&self, id: &ContentId) -> Option<std::sync::Arc<[u8]>> {
        self.branch_store
            .resolve_cached_bytes(id)
            .or_else(|| self.parents.iter().find_map(|p| p.resolve_cached_bytes(id)))
    }

    async fn get_range(&self, id: &ContentId, range: std::ops::Range<u64>) -> Result<Vec<u8>> {
        // Mirror `get` semantics: try this branch first, then walk parents on
        // NotFound. Forward `get_range` natively at every step so range-capable
        // backends (S3, file) keep their byte-range optimization across the
        // ancestry chain. Without this, the trait default falls back to a full
        // `get` + slice — which silently nullifies envelope-only probes
        // (`load_commit_envelope_by_id`) for branched ledgers.
        match self.branch_store.get_range(id, range.clone()).await {
            Ok(bytes) => return Ok(bytes),
            Err(e) if self.parents.is_empty() => return Err(e),
            Err(e) if !matches!(e, crate::error::Error::NotFound(_)) => return Err(e),
            Err(_) => {}
        }
        let mut last_err = None;
        for parent in &self.parents {
            match parent.get_range(id, range.clone()).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| crate::error::Error::not_found(id.to_string())))
    }
}

// ============================================================================
// Helper Functions (Public for use by other storage implementations)
// ============================================================================

/// Compute SHA-256 hash of bytes and return as hex string.
///
/// This is the standard hash function used for content-addressed storage.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

/// Convert a ledger ID to a path prefix.
///
/// Handles the standard ledger ID format (e.g., "mydb:main" -> "mydb/main").
pub fn ledger_id_prefix_for_path(ledger_id: &str) -> String {
    ledger_id_to_path_prefix(ledger_id).unwrap_or_else(|_| ledger_id.replace(':', "/"))
}

/// Leading path segment under which graph-source artifacts (snapshots and
/// mappings) are stored. Unlike every other content kind, these addresses do
/// NOT begin with the owning id — they begin with this literal segment and
/// carry the graph_source_id after it. Shared between the forward direction
/// ([`content_path`]) and reverse parsers (e.g. the remote-mount proxy
/// storage) so the two cannot drift.
pub const GRAPH_SOURCES_PATH_SEGMENT: &str = "graph-sources";

/// Build a storage path for content-addressed data.
///
/// This determines the directory structure for different content types:
/// - Commits: `{ledger_id}/commit/{hash}.fcv2`
/// - Index roots: `{ledger_id}/index/roots/{hash}.fir6`
/// - Graph sources: `graph-sources/{graph_source_id}/snapshots/{hash}.gssnap`
///   (note: keyed by graph_source_id, not a ledger id)
/// - etc.
pub fn content_path(kind: ContentKind, ledger_id: &str, hash_hex: &str) -> String {
    let prefix = ledger_id_prefix_for_path(ledger_id);
    match kind {
        ContentKind::Commit => format!("{prefix}/commit/{hash_hex}.fcv2"),
        ContentKind::Txn => format!("{prefix}/txn/{hash_hex}.json"),
        ContentKind::IndexRoot => format!("{prefix}/index/roots/{hash_hex}.fir6"),
        ContentKind::GarbageRecord => format!("{prefix}/index/garbage/{hash_hex}.json"),
        ContentKind::DictBlob { dict } => {
            // Dictionaries are global per ledger — shared across all branches.
            // Use the @shared namespace (can't collide with branch names since @ is forbidden).
            let shared = shared_prefix_for_path(ledger_id);
            let ext = dict_kind_extension(dict);
            format!("{shared}/dicts/{hash_hex}.{ext}")
        }
        ContentKind::IndexBranch => {
            format!("{prefix}/index/objects/branches/{hash_hex}.fbr")
        }
        ContentKind::IndexLeaf => format!("{prefix}/index/objects/leaves/{hash_hex}.fli"),
        ContentKind::LedgerConfig => format!("{prefix}/config/{hash_hex}.json"),
        ContentKind::StatsSketch => format!("{prefix}/index/stats/{hash_hex}.hll"),
        ContentKind::GraphSourceSnapshot => {
            format!("{GRAPH_SOURCES_PATH_SEGMENT}/{prefix}/snapshots/{hash_hex}.gssnap")
        }
        ContentKind::SpatialIndex => format!("{prefix}/index/spatial/{hash_hex}.bin"),
        ContentKind::HistorySidecar => {
            format!("{prefix}/index/objects/history/{hash_hex}.fhs1")
        }
        ContentKind::GraphSourceMapping => {
            format!("{GRAPH_SOURCES_PATH_SEGMENT}/{prefix}/mapping/{hash_hex}.ttl")
        }
        // Forward-compatibility: unknown kinds go to a generic blob directory
        #[allow(unreachable_patterns)]
        _ => format!("{prefix}/blob/{hash_hex}.bin"),
    }
}

/// Build a Fluree address for content-addressed data.
///
/// # Arguments
///
/// * `method` - Storage method identifier (e.g., "file", "s3", "memory")
/// * `kind` - The type of content being stored
/// * `ledger_id` - Ledger ID (e.g., "mydb:main")
/// * `hash_hex` - Content hash as hex string
///
/// # Returns
///
/// A Fluree address like `fluree:file://mydb/main/commit/{hash}.fcv2`
pub fn content_address(method: &str, kind: ContentKind, ledger_id: &str, hash_hex: &str) -> String {
    let path = content_path(kind, ledger_id, hash_hex);
    format!("fluree:{method}://{path}")
}

/// Deserialize a JSON byte slice into a typed value.
pub fn decode_json<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

// ============================================================================
// Extended Storage Traits (CAS, List, Delete)
// ============================================================================

/// Error type for extended storage operations
///
/// These errors have specific semantics important for storage operations:
/// - `PreconditionFailed` indicates CAS conflict (retry is appropriate)
/// - `Throttled` indicates rate limiting (back off and retry)
/// - Others are generally fatal for the operation
#[derive(Debug, Error)]
pub enum StorageExtError {
    /// I/O or network error
    #[error("I/O error: {0}")]
    Io(String),

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(String),

    /// Unauthorized - invalid credentials
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Forbidden - insufficient permissions
    #[error("Forbidden: {0}")]
    Forbidden(String),

    /// Throttled - rate limited
    ///
    /// Indicates the caller should back off and retry.
    #[error("Throttled: {0}")]
    Throttled(String),

    /// Precondition failed (CAS conflict)
    ///
    /// Indicates a concurrent modification was detected. The caller should
    /// retry with a fresh read.
    #[error("Precondition failed: {0}")]
    PreconditionFailed(String),

    /// Other error
    #[error("{0}")]
    Other(String),
}

impl StorageExtError {
    pub fn io(msg: impl Into<String>) -> Self {
        Self::Io(msg.into())
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::Unauthorized(msg.into())
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::Forbidden(msg.into())
    }

    pub fn throttled(msg: impl Into<String>) -> Self {
        Self::Throttled(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Result type for extended storage operations
pub type StorageExtResult<T> = std::result::Result<T, StorageExtError>;

/// Result of a paginated list operation
#[derive(Debug, Clone)]
pub struct ListResult {
    /// Object keys/addresses relative to storage root
    ///
    /// These are storage keys, not full Fluree addresses.
    /// The caller is responsible for building Fluree addresses from context.
    pub keys: Vec<String>,

    /// Continuation token for fetching the next page
    ///
    /// `None` if there are no more results.
    pub continuation_token: Option<String>,

    /// Whether there are more results available
    pub is_truncated: bool,
}

impl ListResult {
    /// Create a new list result
    pub fn new(keys: Vec<String>, continuation_token: Option<String>, is_truncated: bool) -> Self {
        Self {
            keys,
            continuation_token,
            is_truncated,
        }
    }

    /// Create an empty list result
    pub fn empty() -> Self {
        Self {
            keys: Vec::new(),
            continuation_token: None,
            is_truncated: false,
        }
    }
}

/// Delete stored objects
///
/// This trait is separate from `StorageWrite` because:
/// 1. Not all storage backends support deletion
/// 2. Deletion is rarely needed in normal operation (append-only data model)
/// 3. Deletion has security implications that warrant separate consideration
#[async_trait]
pub trait StorageDelete: Debug + Send + Sync {
    /// Delete an object by address
    ///
    /// Returns `Ok(())` if the object was deleted or did not exist.
    /// Only returns an error for actual failures (network, permissions, etc).
    async fn delete(&self, address: &str) -> StorageExtResult<()>;
}

/// List objects by prefix
///
/// This trait provides listing capabilities for storage backends.
///
/// # Warning
///
/// `list_prefix` can be expensive for large prefixes. Use only for:
/// - Development/debugging
/// - Admin operations
/// - Small, bounded prefixes
///
/// For production iteration over large datasets, use `list_prefix_paginated`.
#[async_trait]
pub trait StorageList: Debug + Send + Sync {
    /// List all objects under a prefix
    ///
    /// Returns all matching keys. May be expensive for large prefixes.
    ///
    /// # Warning
    ///
    /// This method loads all results into memory. For large prefixes,
    /// use `list_prefix_paginated` instead.
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>>;

    /// Paginated listing for production use
    ///
    /// Returns up to `max_keys` addresses and a continuation token.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to list under
    /// * `continuation_token` - Token from a previous call to continue listing
    /// * `max_keys` - Maximum number of keys to return (capped by backend limits)
    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<ListResult>;
}

/// What a `compare_and_swap` closure decided to do.
pub enum CasAction<T = ()> {
    /// Write these bytes back to storage.
    Write(Vec<u8>),
    /// Abort without writing; carry an application-level value out.
    Abort(T),
}

/// Outcome of a `compare_and_swap` call.
#[derive(Debug)]
pub enum CasOutcome<T = ()> {
    /// The write succeeded.
    Written,
    /// The closure chose to abort.
    Aborted(T),
}

/// Atomic storage operations
///
/// Provides insert-if-absent and read-modify-write (compare-and-swap) semantics.
/// Implementations choose their own concurrency mechanism:
/// - In-memory: hold a write lock across the operation
/// - Filesystem: file locking
/// - S3: ETag-based optimistic concurrency with internal retry
///
/// Callers never see ETags, version counters, or other concurrency tokens.
#[async_trait]
pub trait StorageCas: Debug + Send + Sync {
    /// Write bytes only if the key does not already exist.
    ///
    /// Returns `true` if the write succeeded (key was created),
    /// `false` if the key already existed (no write performed).
    async fn insert(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool>;

    /// Atomic read-modify-write.
    ///
    /// Reads the current value at `address` (or `None` if absent), passes it to
    /// the closure, and writes the result back atomically. If another writer
    /// modifies the value concurrently, the implementation re-reads and calls
    /// the closure again.
    ///
    /// The closure receives `Option<&[u8]>` and returns:
    /// - `Ok(CasAction::Write(bytes))` to write new bytes
    /// - `Ok(CasAction::Abort(t))` to stop without writing
    /// - `Err(e)` to stop with an error
    ///
    /// The closure should be a pure function of its input — it may be called
    /// multiple times on retry.
    async fn compare_and_swap<T, F>(&self, address: &str, f: F) -> StorageExtResult<CasOutcome<T>>
    where
        F: Fn(Option<&[u8]>) -> std::result::Result<CasAction<T>, StorageExtError> + Send + Sync,
        T: Send;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content_kind::DictKind;
    use crate::storage::memory::MemoryStorage;

    const LEDGER: &str = "mydb:main";

    /// `candidate_addresses` exists so that callers deciding a blob is
    /// unreferenced see every address `ContentStore::get` would resolve it to.
    /// A dict blob left at the pre-`@shared` address is readable, so it must
    /// also be listed — otherwise a sweep deletes a live blob.
    #[tokio::test]
    async fn candidate_addresses_cover_the_legacy_dict_fallback() {
        let storage = MemoryStorage::new();
        let id = ContentId::new(
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"dict bytes",
        );
        let legacy = legacy_dict_address(storage.storage_method(), LEDGER, &id)
            .expect("dict CIDs have a legacy address");
        storage.write_bytes(&legacy, b"dict bytes").await.unwrap();

        let store = content_store_for(storage.clone(), LEDGER);
        assert_eq!(
            store.get(&id).await.unwrap(),
            b"dict bytes",
            "reads fall back to the legacy dict address"
        );
        assert!(
            candidate_addresses(storage.storage_method(), LEDGER, &id).contains(&legacy),
            "the address reads fall back to must be listed as a candidate"
        );
    }

    /// Same coupling for index roots written before the `.json` to `.fir6`
    /// rename.
    #[tokio::test]
    async fn candidate_addresses_cover_the_legacy_index_root_fallback() {
        let storage = MemoryStorage::new();
        let id = ContentId::new(ContentKind::IndexRoot, b"root bytes");
        let legacy = legacy_index_root_address(storage.storage_method(), LEDGER, &id)
            .expect("index-root CIDs have a legacy address");
        storage.write_bytes(&legacy, b"root bytes").await.unwrap();

        let store = content_store_for(storage.clone(), LEDGER);
        assert_eq!(
            store.get(&id).await.unwrap(),
            b"root bytes",
            "reads fall back to the legacy index-root address"
        );
        assert!(
            candidate_addresses(storage.storage_method(), LEDGER, &id).contains(&legacy),
            "the address reads fall back to must be listed as a candidate"
        );
    }

    /// A garbage manifest names a CID, not an address. On a ledger predating
    /// the `@shared` dict move the only copy sits at the legacy address, so
    /// releasing just the canonical one reclaims nothing while reporting
    /// success — the leak GC exists to prevent.
    #[tokio::test]
    async fn release_reclaims_a_blob_at_its_legacy_address() {
        let storage = MemoryStorage::new();
        let id = ContentId::new(
            ContentKind::DictBlob {
                dict: DictKind::Graphs,
            },
            b"legacy dict",
        );
        let legacy = legacy_dict_address(storage.storage_method(), LEDGER, &id)
            .expect("dict CIDs have a legacy address");
        storage.write_bytes(&legacy, b"legacy dict").await.unwrap();

        let store = content_store_for(storage.clone(), LEDGER);
        store.release(&id).await.expect("release succeeds");

        assert!(
            !storage.exists(&legacy).await.unwrap(),
            "the legacy-located blob must actually be reclaimed"
        );
        assert!(!store.has(&id).await.unwrap(), "and be unreachable after");
    }

    /// The current-layout address always leads, so callers that only need the
    /// canonical location can take the first entry.
    #[test]
    fn candidate_addresses_lead_with_the_current_layout() {
        let id = ContentId::new(ContentKind::IndexLeaf, b"leaf");
        let addresses = candidate_addresses("memory", LEDGER, &id);
        assert_eq!(
            addresses.first().map(String::as_str),
            Some(
                content_address("memory", ContentKind::IndexLeaf, LEDGER, &id.digest_hex())
                    .as_str()
            )
        );
    }
}
