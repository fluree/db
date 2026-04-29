# Storage Traits Design

This document describes the storage trait architecture in Fluree DB, explaining the design rationale and providing guidance for implementing new storage backends.

## Overview

Fluree uses a layered storage abstraction that separates:
- **Content-addressed access** (`fluree-db-core`): The `ContentStore` trait provides get/put/has operations keyed by `ContentId` (CIDv1). This is the primary interface for all immutable artifact access (commits, index roots, leaves, dicts).
- **Physical storage traits** (`fluree-db-core`): Runtime-agnostic storage operations (`StorageRead`, `StorageWrite`, `ContentAddressedWrite`) with standard `Result<T>` error handling. These handle the physical I/O layer beneath ContentStore.
- **Extension traits** (`fluree-db-nameservice`): Nameservice-specific operations with `StorageExtResult<T>` for richer error semantics (CAS operations, pagination, etc.).

See [ContentId and ContentStore](content-id-and-contentstore.md) for the content-addressed identity model.

## Quick Start: The Prelude

For convenient imports, use the storage prelude:

```rust
use fluree_db_core::prelude::*;

// Now you have access to:
// - Storage, StorageRead, StorageWrite, ContentAddressedWrite (traits)
// - MemoryStorage, FileStorage (implementations)
// - ContentKind, ContentWriteResult, ReadHint (types)

async fn example<S: Storage>(storage: &S) -> Result<()> {
    let bytes = storage.read_bytes("some/address").await?;
    storage.write_bytes("other/address", &bytes).await?;
    Ok(())
}
```

For API consumers, `fluree-db-api` re-exports all storage traits:

```rust
use fluree_db_api::{Storage, StorageRead, MemoryStorage};
```

## Trait Hierarchy

```text
              ┌──────────────────────┐
              │    ContentStore      │  get(ContentId), put(ContentKind, bytes), has(ContentId)
              └──────────────────────┘
                    (primary interface for immutable artifacts)

              ┌─────────────────┐
              │   StorageRead   │  read_bytes, exists, list_prefix
              └────────┬────────┘
                       │
              ┌────────┴────────┐
              │  StorageWrite   │  write_bytes, delete
              └────────┬────────┘
                       │
        ┌──────────────┴──────────────┐
        │   ContentAddressedWrite     │  content_write_bytes[_with_hash]
        └──────────────┬──────────────┘
                       │
              ┌────────┴────────┐
              │     Storage     │  (marker trait - blanket impl)
              └─────────────────┘
                    (physical I/O layer)
```

`ContentStore` is the content-addressed layer that sits above the physical storage traits. It maps `ContentId` values to physical storage locations via the underlying `Storage` implementation.

## ContentStore (fluree-db-core)

The `ContentStore` trait is the primary interface for accessing immutable, content-addressed artifacts (commits, index roots, leaves, dictionaries, etc.).

```rust
#[async_trait]
pub trait ContentStore: Debug + Send + Sync {
    /// Retrieve bytes by content ID
    async fn get(&self, id: &ContentId) -> Result<Vec<u8>>;

    /// Store bytes, returning the computed ContentId
    async fn put(&self, kind: ContentKind, bytes: &[u8]) -> Result<ContentId>;

    /// Check whether an object exists
    async fn has(&self, id: &ContentId) -> Result<bool>;
}
```

**Design notes:**
- `ContentId` is a CIDv1 value encoding the hash function, digest, and content kind (multicodec). See [ContentId and ContentStore](content-id-and-contentstore.md).
- `ContentKind` enables routing to different storage tiers (commit store vs index store) without parsing URL paths.
- `put` computes the content hash and returns the derived `ContentId`.
- Implementations include `MemoryContentStore` (for testing) and `BridgeContentStore` (adapts a `Storage` backend).

## Physical Storage Traits (fluree-db-core)

The physical storage traits handle raw byte I/O against storage backends (filesystem, S3, memory). `ContentStore` implementations typically wrap these.

### StorageRead

Read-only storage operations. Implement this for any storage that can retrieve data.

```rust
#[async_trait]
pub trait StorageRead: Debug + Send + Sync {
    /// Read raw bytes from an address
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>>;

    /// Read with a hint for content type optimization
    /// Default implementation ignores the hint
    async fn read_bytes_hint(&self, address: &str, hint: ReadHint) -> Result<Vec<u8>> {
        self.read_bytes(address).await
    }

    /// Check if an address exists
    async fn exists(&self, address: &str) -> Result<bool>;

    /// List all addresses with a given prefix
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}
```

**Design notes:**
- `read_bytes_hint` enables optimizations like returning pre-encoded flakes for leaf nodes
- `list_prefix` is essential for garbage collection and administrative operations
- All methods return `fluree_db_core::Result<T>` (alias for `std::result::Result<T, Error>`)

### StorageWrite

Mutating storage operations. Implement alongside `StorageRead` for read-write storage.

```rust
#[async_trait]
pub trait StorageWrite: Debug + Send + Sync {
    /// Write raw bytes to an address
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()>;

    /// Delete data at an address
    async fn delete(&self, address: &str) -> Result<()>;
}
```

**Design notes:**
- `delete` is part of the core write trait (not separate) because any writable storage should support deletion
- Implementations should be idempotent: deleting a non-existent address succeeds silently

### ContentAddressedWrite

Extension trait for content-addressed (hash-based) writes. Extends `StorageWrite`.

```rust
#[async_trait]
pub trait ContentAddressedWrite: StorageWrite {
    /// Write bytes with a pre-computed content hash
    /// Returns the canonical address and metadata
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult>;

    /// Write bytes, computing the hash internally
    /// Default implementation computes SHA-256 and delegates
    async fn content_write_bytes(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        let hash = sha256_hex(bytes);
        self.content_write_bytes_with_hash(kind, ledger_id, &hash, bytes).await
    }
}
```

**Design notes:**
- `ContentKind` indicates whether data is a commit or index, enabling routing to different storage tiers
- The default `content_write_bytes` implementation handles hash computation, so most backends only need to implement `content_write_bytes_with_hash`
- Content-addressed storage enables deduplication and integrity verification

### Storage (Marker Trait)

A convenience marker trait indicating full storage capability.

```rust
/// Full storage capability: read + content-addressed write
pub trait Storage: StorageRead + ContentAddressedWrite {}

/// Blanket implementation for any type implementing both traits
impl<T: StorageRead + ContentAddressedWrite> Storage for T {}
```

**Usage:**
```rust
// Instead of this verbose bound:
fn process<S: StorageRead + StorageWrite + ContentAddressedWrite>(storage: &S)

// Use this:
fn process<S: Storage>(storage: &S)
```

## Extension Traits (fluree-db-nameservice)

The nameservice crate defines additional traits with `StorageExtResult<T>` for richer error handling (e.g., `PreconditionFailed` for CAS operations).

### StorageList

Paginated listing for large-scale storage backends.

```rust
#[async_trait]
pub trait StorageList {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>>;

    async fn list_prefix_paginated(
        &self,
        prefix: &str,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> StorageExtResult<ListResult>;
}
```

### StorageCas

Compare-and-swap operations for consistent distributed updates.

```rust
#[async_trait]
pub trait StorageCas {
    /// Write only if the address doesn't exist
    async fn write_if_absent(&self, address: &str, bytes: &[u8]) -> StorageExtResult<bool>;

    /// Write only if the current version matches expected_etag
    async fn write_if_match(
        &self,
        address: &str,
        bytes: &[u8],
        expected_etag: &str,
    ) -> StorageExtResult<String>;

    /// Read with version/etag for subsequent CAS operations
    async fn read_with_etag(&self, address: &str) -> StorageExtResult<(Vec<u8>, String)>;
}
```

### StorageDelete (nameservice)

Delete with nameservice error semantics.

```rust
#[async_trait]
pub trait StorageDelete {
    async fn delete(&self, address: &str) -> StorageExtResult<()>;
}
```

**Why separate from core `StorageWrite::delete`?**
- Nameservice operations need `StorageExtResult` for errors like `PreconditionFailed`
- Core operations use standard `Result` for simplicity
- Storage backends typically implement both, with the nameservice version delegating to core

## Implementing a Storage Backend

### Minimal Read-Only Backend

For a read-only backend (e.g., `ProxyStorage` that fetches via HTTP):

```rust
#[async_trait]
impl StorageRead for MyReadOnlyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> {
        // Fetch from remote
    }

    async fn exists(&self, address: &str) -> Result<bool> {
        // Check existence (can implement as try-read)
        match self.read_bytes(address).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_prefix(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(Error::storage("list_prefix not supported"))
    }
}

// Must also implement StorageWrite (with error stubs) and ContentAddressedWrite
// if you want to satisfy the Storage marker trait
#[async_trait]
impl StorageWrite for MyReadOnlyStorage {
    async fn write_bytes(&self, _: &str, _: &[u8]) -> Result<()> {
        Err(Error::storage("read-only storage"))
    }
    async fn delete(&self, _: &str) -> Result<()> {
        Err(Error::storage("read-only storage"))
    }
}

#[async_trait]
impl ContentAddressedWrite for MyReadOnlyStorage {
    async fn content_write_bytes_with_hash(&self, ...) -> Result<ContentWriteResult> {
        Err(Error::storage("read-only storage"))
    }
}
```

### Full Read-Write Backend

For a complete backend (e.g., S3, filesystem):

```rust
// 1. Implement core traits
#[async_trait]
impl StorageRead for MyStorage {
    async fn read_bytes(&self, address: &str) -> Result<Vec<u8>> { ... }
    async fn exists(&self, address: &str) -> Result<bool> { ... }
    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> { ... }
}

#[async_trait]
impl StorageWrite for MyStorage {
    async fn write_bytes(&self, address: &str, bytes: &[u8]) -> Result<()> { ... }
    async fn delete(&self, address: &str) -> Result<()> { ... }
}

#[async_trait]
impl ContentAddressedWrite for MyStorage {
    async fn content_write_bytes_with_hash(
        &self,
        kind: ContentKind,
        ledger_id: &str,
        content_hash_hex: &str,
        bytes: &[u8],
    ) -> Result<ContentWriteResult> {
        // Build address from kind + alias + hash
        let address = build_content_address(kind, ledger_id, content_hash_hex);
        self.write_bytes(&address, bytes).await?;
        Ok(ContentWriteResult {
            address,
            content_hash: content_hash_hex.to_string(),
            size_bytes: bytes.len(),
        })
    }
}

// Storage marker trait is automatically satisfied via blanket impl

// 2. Optionally implement nameservice traits for advanced features
#[async_trait]
impl StorageList for MyStorage {
    async fn list_prefix(&self, prefix: &str) -> StorageExtResult<Vec<String>> {
        // Delegate to core trait, convert error
        StorageRead::list_prefix(self, prefix)
            .await
            .map_err(|e| StorageExtError::Other(e.to_string()))
    }
    // ... paginated version
}
```

## BranchedContentStore (fluree-db-core)

`BranchedContentStore<S>` is a recursive `ContentStore` implementation that provides namespace-scoped fallback reads for branched ledgers. When a branch is created, it gets its own storage namespace for new writes, but needs to read pre-branch-point content (commits, dictionaries) from ancestor namespaces.

### Structure

```rust
pub struct BranchedContentStore<S: Storage> {
    branch_store: StorageContentStore<S>,
    parents: Vec<BranchedContentStore<S>>,
}
```

- **`branch_store`** — the branch's own namespace store; all writes go here
- **`parents`** — ancestor stores to fall back to for reads (recursive tree)

The recursive structure supports arbitrarily deep branch chains (main → dev → feature) and is designed to support future merge scenarios where a branch may have multiple parents (DAG ancestry).

### Constructors

```rust
// Root branch (e.g., main) — no parents
let store = BranchedContentStore::leaf(storage, "mydb:main");

// Branch with parent fallback
let parent = BranchedContentStore::leaf(storage, "mydb:main");
let store = BranchedContentStore::with_parents(storage, "mydb:dev", vec![parent]);
```

### Read Behavior

`get()` tries the branch's own namespace first, then recurses into parents:

1. Try `branch_store.get(id)` — if found, return immediately
2. If `NotFound` and parents exist, try each parent in order
3. If no parent finds it, return the last `NotFound` error
4. **Non-`NotFound` errors propagate immediately** — only `NotFound` triggers fallback

`has()` and `resolve_local_path()` follow the same fallback pattern.

### Write Behavior

`put()` and `put_with_id()` always write to `branch_store` — never to parents. This ensures branch isolation: new content is always scoped to the branch's own namespace.

### What Is and Isn't Copied at Branch Time

| Artifact | Copied? | Reason |
|----------|---------|--------|
| **Commits** | No | Immutable chain, never deleted; read via fallback |
| **Index structure files** (root, leaves, branches, arenas) | Yes | Source may GC old indexes after reindexing |
| **String dictionaries** | No | Stored globally in the `@shared` namespace; all branches read from the same location |

### Global Dictionary Storage (`@shared` Namespace)

String dictionaries (mappings between IRIs/strings and compact integer IDs) are the largest index artifact. Rather than copying them per-branch or relying on `BranchedContentStore` fallback reads, dictionaries are stored in a **global namespace** shared by all branches of a ledger.

The `content_path` function routes all `DictBlob` CIDs to a shared path:

```text
mydb/@shared/dicts/<sha256hex>.subject    # Subject dict
mydb/@shared/dicts/<sha256hex>.string     # String dict
mydb/@shared/dicts/<sha256hex>.predicate  # Predicate dict
...
```

The `@shared` prefix uses the `@` character, which is forbidden in branch names by `validate_branch_name`, so it cannot collide with any branch namespace. The constant is defined as `SHARED_NAMESPACE` in `fluree-db-core::address_path`.

**Legacy fallback:** Existing deployments may have dictionaries stored at the old per-branch path (e.g., `mydb/main/index/objects/dicts/<sha>.dict`). `StorageContentStore` automatically falls back to the legacy path when a dict CID is not found at the new `@shared` location. After the next index build, new writes go to the `@shared` path — no manual migration is needed.

### Building the Store Tree

`LedgerState::build_branched_store()` recursively walks the branch ancestry via nameservice `source_branch` metadata, constructing the `BranchedContentStore` tree. This uses `Box::pin` for the recursive async calls.

The actual ancestry walk lives in **`fluree-db-nameservice`** (`branched_store::build_branched_store`), and `LedgerState::build_branched_store` is a thin wrapper that delegates there. This keeps the helper available to crates that don't depend on `fluree-db-ledger` (notably `fluree-db-indexer`'s background worker).

### When to Use BranchedContentStore

Any code path that walks the commit chain or loads index blobs for a branched ledger MUST use a branch-aware content store. Per-query reads against an already-loaded `LedgerState` are fine — `LedgerState::load` already wires the branched store up.

Use the nameservice helpers, not the flat `StorageBackend::content_store(...)`:

| Helper | When to use |
|---|---|
| `fluree_db_nameservice::branched_content_store_for_record(backend, ns, &record)` | An `NsRecord` is in scope (no extra lookup) |
| `fluree_db_nameservice::branched_content_store_for_id(backend, ns, ledger_id)` | No `NsRecord` available — does one nameservice lookup |
| `Fluree::branched_content_store(&self, ledger_id)` | API / CLI callers — wraps `_for_id` |

Both helpers return the flat namespace store unchanged for non-branched ledgers, so adding them to non-branch code paths costs at most a single nameservice lookup.

A flat `backend.content_store(ledger_id)` on the commit-chain walk path will 404 the moment the walker steps past the fork point and tries to read an ancestor commit from the wrong namespace.

## Type Erasure with AnyStorage

For dynamic dispatch (e.g., runtime-selected storage backends), use `AnyStorage`:

```rust
/// Type-erased storage wrapper
pub struct AnyStorage {
    inner: Arc<dyn Storage>,
}

impl AnyStorage {
    pub fn new<S: Storage + 'static>(storage: S) -> Self {
        Self { inner: Arc::new(storage) }
    }
}
```

**When to use:**
- `FlureeClient` uses `AnyStorage` to support any backend at runtime
- Generic code should prefer concrete types (`S: Storage`) for better optimization
- Use `AnyStorage` when storage type is determined at runtime (e.g., from config)

## Wrapper Storages

Several wrapper types add functionality to underlying storage:

### TieredStorage

Routes commits and indexes to different backends:

```rust
pub struct TieredStorage<S> {
    commit_storage: S,
    index_storage: S,
}
```

### EncryptedStorage

Adds transparent encryption:

```rust
pub struct EncryptedStorage<S, K> {
    inner: S,
    key_provider: K,
}
```

### AddressIdentifierResolverStorage

Routes reads based on address format (e.g., different storage backends by identifier segment):

```rust
pub struct AddressIdentifierResolverStorage {
    default_storage: Arc<dyn Storage>,
    identifier_storages: HashMap<String, Arc<dyn Storage>>,
}
```

## Error Handling

### Core Errors (`fluree_db_core::Error`)

Standard errors for storage operations:
- `NotFound` - Address doesn't exist
- `Storage` - Generic storage failure
- `Io` - Underlying I/O error

### Nameservice Errors (`StorageExtError`)

Extended errors for nameservice operations:
- `NotFound` - Address doesn't exist
- `PreconditionFailed` - CAS condition not met
- `Other` - Generic error with message

## Summary

| Type | Crate | Purpose | Error Type |
|------|-------|---------|------------|
| `ContentStore` (trait) | core | Content-addressed get/put/has by `ContentId` | `Result<T>` |
| `BranchedContentStore` (struct) | core | Recursive `ContentStore` with namespace fallback for branches | `Result<T>` |
| `StorageRead` (trait) | core | Physical read operations | `Result<T>` |
| `StorageWrite` (trait) | core | Physical write + delete | `Result<T>` |
| `ContentAddressedWrite` (trait) | core | Hash-based physical writes | `Result<T>` |
| `Storage` (trait) | core | Marker (full physical capability) | - |
| `StorageList` (trait) | nameservice | Paginated listing | `StorageExtResult<T>` |
| `StorageCas` (trait) | nameservice | Compare-and-swap | `StorageExtResult<T>` |
| `StorageDelete` (trait) | nameservice | Delete with ext errors | `StorageExtResult<T>` |

Application code typically interacts with `ContentStore` for immutable artifact access. Storage backend implementors implement the physical traits (`StorageRead`, `StorageWrite`, `ContentAddressedWrite`) and the `Storage` marker trait is automatically satisfied. For branched ledgers, `BranchedContentStore` wraps the physical storage with recursive namespace fallback — see [BranchedContentStore](#branchedcontentstore-fluree-db-core) above.
