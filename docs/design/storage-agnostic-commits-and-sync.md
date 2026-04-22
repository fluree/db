# Storage-agnostic commits and sync

Fluree uses **ContentId** (CIDv1) values as the primary identifiers for commits, index roots, and other immutable artifacts. This decouples the commit chain and nameservice references from any specific storage backend, enabling replication across different storage systems (filesystem, S3, IPFS, etc.) without rewriting commit data.

## Pack protocol (`fluree-pack-v1`)

The pack protocol enables efficient bulk transfer of CAS objects between Fluree instances. Instead of fetching each commit individually (one HTTP round-trip per commit), the pack protocol streams all missing objects in a single binary response.

### How it works

1. **Client** sends a `POST /pack/{ledger}` request with `want` (CIDs the client needs, typically the remote head) and `have` (CIDs the client already has, typically the local head). Optionally includes `include_indexes: true` with `want_index_root_id` / `have_index_root_id` to request binary index artifacts.
2. **Server** walks the commit chain from each `want` backward until it reaches a `have`, collecting all missing commits and their referenced txn blobs. When indexes are requested, computes the diff of index artifact CIDs between the want and have index roots.
3. **Server** streams commit + txn objects as binary data frames (oldest-first topological order), followed by a Manifest frame and index artifact data frames when indexes are included.
4. **Client** decodes frames incrementally via a `BytesMut` buffer, verifies integrity of each object, and writes to local CAS.

The CLI uses a **peek-then-ingest** pattern: it reads the Header frame first (via `peek_pack_header`) to inspect `estimated_total_bytes`, then prompts for confirmation on large transfers (>1 GiB) before consuming the rest of the stream via `ingest_pack_stream_with_header`.

### Wire format

```text
[Preamble: FPK1 + version(1)] [Header frame] [Data frames...] [Manifest frame]? [Data frames...]? [End frame]
```

| Frame    | Type byte | Content |
|----------|-----------|---------|
| Header   | `0x00`    | JSON metadata: protocol, capabilities, commit count, index artifact count, `estimated_total_bytes` |
| Data     | `0x01`    | CID binary + raw object bytes (commit, txn blob, or index artifact) |
| Error    | `0x02`    | UTF-8 error message (terminates stream) |
| Manifest | `0x03`    | JSON metadata for phase transitions (e.g. start of index artifact phase) |
| End      | `0xFF`    | End of stream |

### Client-side verification

Each data frame is verified before writing to CAS:
- **Commit blobs** (`FCV2` magic): SHA-256 of full blob via `verify_commit_blob()`
- **All other blobs** (txn, index artifacts, config): Full-bytes SHA-256 via `ContentId::verify()`

Integrity failure is terminal -- the entire ingest is aborted.

### Fallback

When the server does not support the pack endpoint (returns 404, 405, 406, or 501), CLI commands automatically fall back to:
- **Named-remote**: Paginated JSON export via `GET /commits/{ledger}`
- **Origin-based**: CID chain walk via `GET /storage/objects/{cid}`

### Implementation

| Component | Location |
|-----------|----------|
| Wire format (encode/decode), estimation constants | `fluree-db-core/src/pack.rs` |
| Server-side pack generation + index artifact diff | `fluree-db-api/src/pack.rs` |
| Server HTTP endpoint | `fluree-db-server/src/routes/pack.rs` |
| Client-side streaming ingest (`ingest_pack_stream`, `peek_pack_header`, `ingest_pack_stream_with_header`) | `fluree-db-nameservice-sync/src/pack_client.rs` |
| Origin fetcher pack methods | `fluree-db-nameservice-sync/src/origin.rs` |
| CLI pull/clone with index transfer + size confirmation | `fluree-db-cli/src/commands/sync.rs` |
| `set_index_head()` API method | `fluree-db-api/src/commit_transfer.rs` |

For the full design document including graph source packing and protocol evolution, see `STORAGE_AGNOSTIC_COMMITS_AND_SYNC.md` (repo root).
