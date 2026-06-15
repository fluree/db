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

```bash
# Local ledger
fluree export mydb --format ledger -o mydb.flpack

# Smaller archive without binary index artifacts (importer will reindex):
fluree export mydb --format ledger --no-indexes -o mydb.flpack

# Remote ledger (cold-archive a production ledger to local disk):
fluree export mydb --remote prod --format ledger -o mydb.flpack
```

`--format ledger` (alias `--format flpack`) writes the full `fluree-pack-v1` archive — commits, txn blobs, and (unless `--no-indexes`) index artifacts — plus a `phase: "nameservice"` manifest frame that lets the importer reconstruct commit/index head pointers.

`-o FILE` is required when stdout is a TTY (the archive is binary). Pipe-friendly forms work too: `fluree export mydb --format ledger > mydb.flpack`.

The local path calls `Fluree::archive_ledger`. The `--remote` path calls `GET /storage/ns/:ledger-id` to fetch the remote NsRecord, then streams `POST /pack/*ledger` and substitutes the nameservice manifest in place of the terminal End frame on the fly — so a remote-sourced archive is byte-compatible with a locally-generated one. Both endpoints require a Bearer token with `fluree.storage.*` permissions (same auth bracket as `fluree clone` / `pull`).

For non-CLI archive flows (S3 upload, custom storage), use `Fluree::archive_ledger` directly — see [Rust API usage](#rust-api-usage) below.

### Restore (import from `.flpack`)

```bash
# Restore into a new LOCAL ledger
fluree create my-restored-ledger --from /path/to/archive.flpack

# Restore onto a REMOTE server (streams the archive to POST /import)
fluree create my-restored-ledger --remote origin --from /path/to/archive.flpack
```

Both forms read the `.flpack` archive, ingest all CAS objects (verifying each), and create a new ledger pointing at the imported commit chain. The ledger name (`my-restored-ledger`) is independent of whatever the original ledger was called.

The remote form streams the file to the server's `POST /import/<ledger>` endpoint, so the server materializes the ledger itself — no local staging instance required. This makes `.flpack` the universal on-ramp for getting data onto a server: build a ledger locally in **any** supported format, export it, then import it wholesale.

```bash
# The generic "load any data onto a server" pattern
fluree create staging --from data.ttl          # any format: ttl, jsonld, nq, dir, jsonl, …
fluree export staging --format ledger -o snap.flpack
fluree create prod --remote origin --from snap.flpack
```

> **Restore vs. publish.** `fluree publish` folds a *local ledger's commits* into a remote, re-validated and reindexed (the `POST /push` path). `--remote --from <archive>.flpack` restores a *trusted snapshot* wholesale — byte-for-byte, index included, no replay. Use publish to merge ongoing work; use import to materialize a ledger from an archive.

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

`Fluree::restore_ledger` does the whole restore in one streaming call: it creates the target ledger, ingests and verifies every CAS object, finalizes the commit/index heads from the embedded nameservice manifest, and rolls back on any failure. It is the import counterpart of `Fluree::archive_ledger`.

It reads from any `AsyncRead`, so the **source** of the archive is open-ended — a local file, an S3 `GetObject` body, an HTTP response, or a pipe. The archive is decoded one frame at a time and never buffered whole, so multi-gigabyte production archives restore without exhausting memory.

```rust
use fluree_db_api::Fluree;

async fn restore_from_file(
    fluree: &Fluree,
    new_ledger_id: &str,
    path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::open(path).await?;
    let result = fluree.restore_ledger(new_ledger_id, &mut file).await?;
    println!(
        "restored {new_ledger_id}: {} commits, {} index artifacts",
        result.commits, result.index_artifacts,
    );
    Ok(())
}
```

To restore from a non-file source, adapt it into an `AsyncRead` and pass it to the same call:

```rust
// From an HTTP / S3 byte stream (Stream<Item = Result<Bytes, _>>):
use tokio_util::io::StreamReader;
let mut reader = StreamReader::new(byte_stream.map_err(std::io::Error::other));
fluree.restore_ledger(new_ledger_id, &mut reader).await?;

// From an aws-sdk-s3 GetObject body:
let mut reader = get_object_output.body.into_async_read();
fluree.restore_ledger(new_ledger_id, &mut reader).await?;
```

The server's `POST /import/<ledger>` endpoint is exactly this: it adapts the request body into an `AsyncRead` and calls `restore_ledger`.

### Key points

- **Rename on restore**: The `new_ledger_id` argument controls the ledger name (a bare name is normalized to `name:main`). CAS objects are content-addressed and name-agnostic; only the nameservice pointer uses the name. The one exception is the FIR6 **index root**, which carries the source `ledger_id` as an inline identity field: restoring under a new name re-stamps that field and re-writes the root under a fresh CID before pointing the index head at it. Without this, the restored ledger would query fine (cold load trusts the root's own name) but reject every write — `apply_loaded_db` asserts the loaded root's `ledger_id` matches the live ledger, so a mismatch loops writes as retryable and leaves the ledger silently read-only. Branches, leaves, and dict blobs are name-independent and ride along verbatim. (The historical txn-meta/config graph IRIs keep the source name — they live in the content-addressed dict tree, matching clone/pull semantics.)
- **Integrity**: Every data frame is verified (SHA-256) before writing, and the manifest's commit/index head CIDs are checked to be present in the archive before the heads are set — a corrupted, truncated, or mismatched archive is rejected rather than creating a dangling head.
- **Atomic on failure**: Any mid-stream error rolls back the half-created ledger, so a failed restore never leaves a live, partially-ingested ledger behind.
- **Indexes are optional**: Without indexes, the restored ledger is functional but replays from commits (or reindexes) before queries are efficient. With indexes, it's queryable immediately.
- **Default context preserved**: If the source ledger has a stored default JSON-LD context, its blob travels in the archive and the restored ledger keeps it — so queries that omit an inline `@context` and rely on the ledger's prefixes keep working. (Currently the local `archive_ledger` path; the remote `export --remote --format ledger` path does not yet carry it.)
- **Storage-agnostic destination**: The restored ledger can live on file storage, S3, or any backend that implements the `Storage` trait — independent of where the archive's bytes came from.
- **Large archives (> 5 GB) use multipart upload**: The `Fluree::restore_ledger` API itself streams an `AsyncRead` of any size with no cap. The size limit only appears in the *negotiated CLI upload* to a size-capped app server: a single S3 presigned PUT rejects bodies over 5 GiB, so the CLI/server negotiate a **multipart** upload (the app mints one presigned `UploadPart` URL per part, the CLI uploads parts, the app calls `CompleteMultipartUpload`). This is automatic — see the [Negotiated Upload Import Contract](../cli/server-integration.md#negotiated-upload-import-contract). Note that compressing the archive further is not a workaround: commit ops and index leaves are already compressed inside the `.flpack`, so an outer pass yields little.

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
| Archive export (`Fluree::archive_ledger`) | `fluree-db-api` | `src/lib.rs` |
| Streaming restore (`Fluree::restore_ledger`) + head finalization | `fluree-db-api` | `src/commit_transfer.rs` |
| HTTP export endpoint (`POST /v1/fluree/pack/*`) | `fluree-db-server` | `src/routes/pack.rs` |
| HTTP import endpoint (`POST /v1/fluree/import/*`) | `fluree-db-server` | `src/routes/import.rs` |
| Network sync ingestion (clone/pull) | `fluree-db-nameservice-sync` | `src/pack_client.rs` |
| CLI `.flpack` import (local + `--remote`) | `fluree-db-cli` | `src/commands/create.rs` |
