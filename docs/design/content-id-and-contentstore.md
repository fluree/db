# ContentId and ContentStore

This document describes the content-addressed identity and storage layer introduced by the storage-agnostic commits design. For the full design rationale, see [Storage-agnostic commits and sync](../design/storage-agnostic-commits-and-sync.md).

## Overview

Fluree's storage-agnostic architecture separates **identity** (what something is) from **location** (where its bytes live). Every immutable artifact—commit, transaction payload, index root, index leaf, dictionary blob—is identified by a **ContentId** (a CIDv1 value) and stored/retrieved via a **ContentStore** trait.

> **Identity is a content ID; location is a local configuration detail.**

## ContentId

`ContentId` is a CIDv1 (multiformats) value that encodes three things:

1. **Version**: CIDv1
2. **Multicodec**: identifies the *kind* of the bytes (e.g., Fluree commit, index root)
3. **Multihash**: identifies the *hash function + digest* (SHA-256)

### Multicodec assignments (private-use range)

Fluree uses the multicodec private-use range for type-tagged CIDs:

| Codec value | ContentKind | Description |
|-------------|-------------|-------------|
| `0x300001` | `Commit` | Commit payload |
| `0x300002` | `Txn` | Original transaction payload |
| `0x300003` | `IndexRoot` | Binary index root descriptor |
| `0x300004` | `IndexBranch` | Index branch manifest |
| `0x300005` | `IndexLeaf` | Index leaf file |
| `0x300006` | `DictBlob` | Dictionary artifact |
| `0x300007` | `DefaultContext` | Default JSON-LD @context |

### String representation

The canonical string form is **base32-lower multibase** (the familiar `bafy…` / `bafk…` prefixes from IPFS/IPLD). This is the form used in JSON APIs, logs, nameservice records, and CLI output.

```text
bafybeigdyr...   (commit CID)
bafkreihdwd...   (index root CID)
```

### Binary representation

The compact binary form (varint version + varint codec + multihash bytes) is used for:
- On-wire pack streams
- Internal caches and indexes
- Embedded references inside commit payloads

### Creating a ContentId

A ContentId is derived by hashing the **canonical bytes** of an artifact with SHA-256, then wrapping the digest as a CIDv1 with the appropriate multicodec:

```rust
use fluree_db_core::content_id::{ContentId, ContentKind};

let bytes: &[u8] = /* canonical commit bytes */;
let cid = ContentId::from_bytes(ContentKind::Commit, bytes);

// String form for JSON/logs
let s = cid.to_string(); // "bafybeig..."

// Parse back
let parsed = ContentId::from_str(&s)?;
assert_eq!(cid, parsed);
```

### ContentId in commit references

Commits reference parents and related artifacts by ContentId only—never by storage addresses:

```json
{
  "t": 42,
  "previous": "bafybeigdyr...commitParent",
  "txn": "bafkreihdwd...txnBlob",
  "index": "bafybeigdyr...indexRoot"
}
```

## ContentKind

`ContentKind` is an enum that maps 1:1 to multicodec values. It serves two purposes:

1. **Embedded in CIDs**: the multicodec tag lets stores, caches, and validators identify what an object is without parsing its bytes.
2. **Routing**: the ContentStore uses `ContentKind` to route objects to the appropriate storage tier (commit store vs index store).

```rust
pub enum ContentKind {
    Commit,
    Txn,
    IndexRoot,
    IndexBranch,
    IndexLeaf,
    DictBlob,
    DefaultContext,
}
```

### Routing by kind (replaces URL parsing)

Previously, storage routing parsed URL path segments (e.g., looking for `"/commit/"` in an address string). With ContentId, routing is explicit:

- `Commit` + `Txn` → commit-tier store(s)
- `IndexRoot` + `IndexBranch` + `IndexLeaf` + `DictBlob` → index-tier store(s)

## ContentStore trait

`ContentStore` provides content-addressed get/put operations keyed by `ContentId`:

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

### Relationship to Storage trait

`ContentStore` is the primary abstraction for immutable object access. The `Storage` / `StorageRead` / `ContentAddressedWrite` traits handle address-routed I/O for the underlying storage backends (filesystem, S3, etc.), while `ContentStore` provides the content-addressed layer on top.

### Implementations

- **`MemoryContentStore`**: In-memory `HashMap<ContentId, Vec<u8>>` for testing.
- **`BridgeContentStore`**: Adapter that wraps a `Storage` implementation, mapping ContentIds to physical storage addresses.
- **Filesystem / S3 / IPFS**: Direct implementations that store objects keyed by CID.

### Layered composition

ContentStore implementations can be layered:

```text
Local cache (filesystem)
    ↓ miss
Shared store (S3 / IPFS / shared filesystem)
```

Reads fall through from cache to shared store. Writes go to both (policy-configurable).

## How ContentId flows through the system

### Transaction path

1. Transactor produces commit bytes
2. `ContentId::from_bytes(ContentKind::Commit, &bytes)` computes the CID
3. `content_store.put(Commit, &bytes)` stores the blob
4. Nameservice head is updated: `commit_head_id = cid, commit_t = t`

### Index path

1. Indexer builds binary index, producing root descriptor bytes
2. `ContentId::from_bytes(ContentKind::IndexRoot, &root_bytes)` computes the CID
3. All artifacts (branches, leaves, dicts) are stored via `content_store.put()`
4. Nameservice index head is updated: `index_head_id = cid, index_t = t`

### Query path

1. Query engine reads nameservice to get `index_head_id`
2. `content_store.get(&index_head_id)` fetches the index root
3. Index root references branches/leaves/dicts by their ContentIds
4. Each artifact is fetched via `content_store.get()` (with caching)

### Replication path (clone/pull/push)

1. Client fetches remote nameservice heads (ContentIds + watermarks)
2. Client sends `have[]` / `want[]` roots to server
3. Server walks commit chain and (optionally) index graph to compute missing objects
4. Missing objects streamed as `(ContentId, bytes)` pairs
5. Client stores objects in local ContentStore and advances local nameservice heads

No address rewriting is needed because commits contain no storage addresses.

## Implementation status

- `ContentId` type and `ContentKind` enum: `fluree-db-core/src/content_id.rs`
- `ContentStore` trait + `MemoryContentStore` + bridge adapter: `fluree-db-core/src/storage.rs`
- `Commit` and `CommitRef` use `ContentId` for all references (index pointers are tracked exclusively via nameservice, not embedded in commits)
- Nameservice records use `head_commit_id` / `index_head_id` as ContentId values
- `IndexRoot` (FIR6) references all artifacts by ContentId
- Transact and indexer paths use `ContentStore` for all object I/O

## Related documentation

- [Storage-agnostic commits and sync](../design/storage-agnostic-commits-and-sync.md) — full design rationale
- [Storage traits](storage-traits.md) — existing storage trait hierarchy
- [Index format](index-format.md) — binary index format (IndexRoot / FIR6)
- [Nameservice schema v2](nameservice-schema-v2.md) — nameservice record schema
