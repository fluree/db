# Pack format: archive and restore

Fluree's `.flpack` format is a self-contained binary snapshot of an entire ledger -- commits, transaction payloads, and (optionally) binary index artifacts. It enables ledger portability: archive a ledger to cold storage, restore it later under the same or a different name, or move it between environments.

## Overview

The pack protocol (`fluree-pack-v1`) was designed for efficient bulk transfer between Fluree instances. The same format works equally well for file-based archive/restore workflows. Because all objects inside a pack are content-addressed (identified by `ContentId` / CIDv1), the ledger name only matters at the nameservice layer -- making rename-on-restore straightforward.

### What's in a `.flpack` file?

A `.flpack` file is a binary stream of frames:

```text
[Preamble: FPK1 + version(1)]
[Header frame]        -- JSON metadata (commit count, estimated size, etc.)
[Data frames...]      -- commits + txn blobs (oldest-first, topological order)
[Manifest frame]?     -- marks start of index artifact phase (if included)
[Data frames...]?     -- index branches, leaves, dict blobs, roots
[End frame]
```

Each data frame contains a CID (content identity) and the raw bytes of the object. On ingest, every frame is integrity-verified before being written to storage.

### With or without indexes

A pack can include just commits + txn blobs (compact, sufficient for full restore -- queries replay from commits), or it can also include binary index artifacts (larger, but the restored ledger is immediately queryable without reindexing).

## CLI usage

### Archive (export to `.flpack`)

The CLI does not yet have a dedicated `fluree export --format flpack` command. To produce a `.flpack` file today, use the pack HTTP endpoint directly or the Rust API (see below).

From the CLI, the closest equivalent is `fluree clone` which uses the pack protocol internally for transfer, then writes objects to local CAS.

### Restore (import from `.flpack`)

```bash
fluree create my-restored-ledger --from /path/to/archive.flpack
```

This reads the `.flpack` file, ingests all CAS objects, and creates a new ledger pointing at the imported commit chain. The ledger name (`my-restored-ledger`) is independent of whatever the original ledger was called.

## Rust API usage

All building blocks for archive/restore live in the API and core crates -- no CLI dependency required.

### Dependencies

```toml
[dependencies]
fluree-db-api = { version = "0.1", features = ["native"] }
fluree-db-core = "0.1"
fluree-db-nameservice-sync = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Archive: generate a `.flpack` file

Use `stream_pack()` from `fluree-db-api` to generate pack frames, then write them to a file (or S3, GCS, etc.).

```rust
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_api::pack::{full_ledger_pack_request, stream_pack};
use tokio::sync::mpsc;
use tokio::io::AsyncWriteExt;

async fn archive_ledger(
    fluree: &Fluree<impl Storage + Clone + Send + Sync + 'static, impl NameService + RefPublisher + Send + Sync>,
    ledger_id: &str,
    output_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let handle = fluree.ledger(ledger_id).await?;

    // Build a request that captures the current head commit (and index
    // root, if present). `include_indexes = true` gives the restored
    // ledger instant queryability; pass `false` for a smaller archive
    // that reindexes on import. Empty `want` is always rejected by
    // `stream_pack`, so always build via this helper.
    //
    // `full_ledger_pack_request` sets `include_txns = true` by default.
    // To produce an even smaller archive without original transaction
    // payloads (verifiable but not replayable), mutate the returned
    // request: `request.include_txns = false;`.
    let request = full_ledger_pack_request(&handle, /* include_indexes */ true).await?;

    let (tx, mut rx) = mpsc::channel(64);

    // Spawn the pack generator
    let fluree_clone = fluree.clone();
    let handle_clone = handle.clone();
    let req_clone = request.clone();
    tokio::spawn(async move {
        let _ = stream_pack(&fluree_clone, &handle_clone, &req_clone, tx).await;
    });

    // Write frames to file
    let mut file = tokio::fs::File::create(output_path).await?;
    while let Some(chunk) = rx.recv().await {
        file.write_all(&chunk.bytes).await?;
    }
    file.flush().await?;

    Ok(())
}
```

To archive to S3 instead of a local file, replace the file writer with your S3 upload (e.g., `aws_sdk_s3` multipart upload consuming chunks from `rx`).

### Restore: ingest a `.flpack` file

Use `ingest_pack_frame()` from `fluree-db-nameservice-sync` to write each object, then finalize the nameservice pointers with `set_commit_head()` / `set_index_head()`.

#### Streaming vs. memory-mapped reads

Pack files can be very large for production ledgers. There are two approaches to reading them:

- **Memory-mapped (mmap)**: The CLI uses `memmap2::Mmap` to map the entire file into virtual address space. This avoids heap allocation but still requires the OS to page the entire file through virtual memory. Suitable for files that fit comfortably in available address space.
- **Streaming**: For very large archives or when reading from a non-seekable source (S3 `GetObject`, HTTP response, pipe), decode frames incrementally from a buffered reader. The network ingestion path (`ingest_pack_stream`) already works this way -- it processes one frame at a time and never holds more than a single frame in memory.

For API consumers building archive/restore on large datasets, the streaming approach is recommended. The example below shows the mmap approach for simplicity; see `fluree-db-nameservice-sync::pack_client::ingest_pack_stream` for the streaming pattern using `BytesMut` + `decode_frame` in a loop.

```rust
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_core::pack::{decode_frame, read_stream_preamble, PackFrame, DEFAULT_MAX_PAYLOAD};
use fluree_db_core::{ContentKind, ContentStore};
use fluree_db_nameservice_sync::pack_client::ingest_pack_frame;

async fn restore_ledger(
    fluree: &Fluree<impl Storage + Clone + Send + Sync + 'static, impl NameService + RefPublisher + Send + Sync>,
    new_ledger_id: &str,
    flpack_bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create the target ledger (empty)
    fluree.create(new_ledger_id).await?;
    let handle = fluree.ledger(new_ledger_id).await?;

    // 2. Parse preamble
    let mut pos = read_stream_preamble(flpack_bytes)?;

    // 3. Decode frames and ingest each CAS object
    let storage = fluree.storage();
    let mut ns_manifest: Option<serde_json::Value> = None;

    loop {
        let (frame, consumed) = decode_frame(&flpack_bytes[pos..], DEFAULT_MAX_PAYLOAD)?;
        pos += consumed;

        match frame {
            PackFrame::Header(_header) => {
                // Metadata -- log or inspect as needed
            }
            PackFrame::Data { cid, payload } => {
                ingest_pack_frame(&cid, &payload, storage, new_ledger_id).await?;
            }
            PackFrame::Manifest(json) => {
                // The nameservice manifest contains commit/index head CIDs and t values
                if json.get("phase").and_then(|v| v.as_str()) == Some("nameservice") {
                    ns_manifest = Some(json);
                }
            }
            PackFrame::End => break,
            PackFrame::Error(msg) => {
                return Err(format!("pack error: {msg}").into());
            }
        }
    }

    // 4. Finalize nameservice pointers from the manifest
    let manifest = ns_manifest.ok_or("missing nameservice manifest in .flpack")?;

    if let Some(cid_str) = manifest.get("commit_head_id").and_then(|v| v.as_str()) {
        let commit_cid: fluree_db_core::ContentId = cid_str.parse()?;
        let commit_t = manifest.get("commit_t").and_then(|v| v.as_i64()).unwrap_or(0);
        fluree.set_commit_head(&handle, &commit_cid, commit_t).await?;
    }
    if let Some(cid_str) = manifest.get("index_head_id").and_then(|v| v.as_str()) {
        let index_cid: fluree_db_core::ContentId = cid_str.parse()?;
        let index_t = manifest.get("index_t").and_then(|v| v.as_i64()).unwrap_or(0);
        fluree.set_index_head(&handle, &index_cid, index_t).await?;
    }

    Ok(())
}
```

### Key points

- **Rename on restore**: The `new_ledger_id` parameter controls the ledger name. CAS objects are content-addressed and name-agnostic; only the nameservice pointer uses the name.
- **Integrity**: Every data frame is verified (SHA-256) before writing. A corrupted archive is detected immediately.
- **Indexes are optional**: Without indexes, the restored ledger is functional but will need to reindex (or replay from commits) before queries are efficient. With indexes, it's ready immediately.
- **Storage-agnostic**: The same `.flpack` file can be restored to file storage, S3, or any backend that implements the `Storage` trait. Archive from file, restore to S3 (or vice versa).

## Wire format reference

For full protocol details including frame encoding, see:

- [Storage-agnostic commits and sync](../design/storage-agnostic-commits-and-sync.md) -- design rationale and protocol overview
- [ContentId and ContentStore](../design/content-id-and-contentstore.md) -- CID encoding and verification
- `fluree-db-core/src/pack.rs` -- wire format constants and encode/decode functions

## Architecture

| Concern | Crate | Key file |
|---------|-------|----------|
| Wire format (FPK1 frames, encode/decode) | `fluree-db-core` | `src/pack.rs` |
| Pack stream generation (export) | `fluree-db-api` | `src/pack.rs` |
| HTTP endpoint (`POST /v1/fluree/pack/*`) | `fluree-db-server` | `src/routes/pack.rs` |
| Stream ingestion (import) | `fluree-db-nameservice-sync` | `src/pack_client.rs` |
| Commit/index head finalization | `fluree-db-api` | `src/commit_transfer.rs` |
| CLI `.flpack` file import | `fluree-db-cli` | `src/commands/create.rs` |
