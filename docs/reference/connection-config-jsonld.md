# JSON-LD Connection Configuration (Rust)

This page documents the **JSON-LD connection config** supported by the Rust implementation.

This config uses the same `@context` + `@graph` model as other Fluree JSON-LD config surfaces.

## Using with the Fluree server

The server accepts a connection config file via `--connection-config`:

```bash
fluree server run --connection-config /path/to/connection.jsonld
```

This replaces `--storage-path` for S3, DynamoDB, and other non-filesystem backends. The server builds its storage and nameservice from the config file at startup. Server-level settings (`--cache-max-mb`, `--indexing-enabled`, etc.) override connection config defaults. See [Configuration](../operations/configuration.md#connection-configuration-s3-dynamodb-etc) for full details and examples.

## Entry points (Rust API)

All construction flows through `FlureeBuilder`:

- `FlureeBuilder::from_json_ld(&json)?` — parses JSON-LD config into builder settings
  - Then call `.build_client().await` for a type-erased `FlureeClient`
  - Or use typed terminal methods (`.build()`, `.build_memory()`, `.build_s3()`) for compile-time type safety
## JSON-LD shape

At minimum, your document contains:

- `@context` with `@base` and `@vocab`
- `@graph` with:
  - one `Connection` node
  - one or more `Storage` nodes
  - optional `Publisher` nodes (nameservice backends)

```json
{
  "@context": {
    "@base": "https://ns.flur.ee/config/connection/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    { "@id": "storage1", "@type": "Storage", "filePath": "./data" },
    {
      "@id": "connection",
      "@type": "Connection",
      "indexStorage": { "@id": "storage1" }
    }
  ]
}
```

## ConfigurationValue (env var indirection)

Many fields can be provided as direct literals **or** as a `ConfigurationValue` object:

```json
{
  "s3Bucket": { "envVar": "FLUREE_S3_BUCKET", "defaultVal": "my-bucket" },
  "cacheMaxMb": { "envVar": "FLUREE_CACHE_MAX_MB", "defaultVal": "1024" }
}
```

Notes:
- `envVar`: reads from environment (non-wasm targets)
- `defaultVal`: fallback string value
- `javaProp`: accepted for compatibility; Rust treats it like another env var key (best-effort)

## Connection node fields

Supported:
- `parallelism` (default 4)
- `cacheMaxMb` (supports `ConfigurationValue`)
- `indexStorage` (required): reference to a `Storage` node
- `commitStorage` (optional): reference to a `Storage` node
- `primaryPublisher` (optional): reference to a `Publisher` node
- `addressIdentifiers` (read routing): map of identifier → storage reference
- `defaults` (partial):
  - `defaults.indexing.reindexMinBytes` / `reindexMaxBytes` are applied as the default `IndexConfig` for writes
  - `defaults.indexing.indexingEnabled=false` suppresses background index triggers
  - `defaults.indexing.maxOldIndexes` sets the maximum number of old index versions to retain before GC (default: 5)
  - `defaults.indexing.gcMinTimeMins` sets the minimum age in minutes before an index can be garbage collected (default: 30)

### addressIdentifiers (read routing)

The `addressIdentifiers` field maps identifier strings to storage backends, enabling read routing based on the identifier segment in Fluree addresses.

```json
{
  "@id": "connection",
  "@type": "Connection",
  "indexStorage": {"@id": "indexS3"},
  "commitStorage": {"@id": "commitS3"},
  "addressIdentifiers": {
    "commit-storage": {"@id": "commitS3"},
    "index-storage": {"@id": "indexS3"}
  }
}
```

**Routing behavior:**
- `fluree:commit-storage:s3://db/commit/abc.fcv2` → routes to `commitS3`
- `fluree:index-storage:s3://db/index/xyz.json` → routes to `indexS3`
- `fluree:s3://db/index/xyz.json` (no identifier) → routes to default storage
- `fluree:unknown-id:s3://db/file.json` (unknown identifier) → fallback to default storage

**Notes:**
- Writes always go to the default storage (TieredStorage or indexStorage), regardless of identifier
- This is a read-only routing mechanism for addresses that already contain identifiers
- Use `addressIdentifier` (singular) on storage nodes to **write** addresses with identifier segments

Not yet supported (parsed/ignored or absent):
- `remoteSystems` — not supported

## Storage node fields

### Memory storage

```json
{ "@id": "mem", "@type": "Storage" }
```

### File storage (requires `native`)

Supported:
- `filePath`
- `AES256Key` (supports `ConfigurationValue`)

Notes:
- Rust expects `AES256Key` to be **base64-encoded** and decode to exactly 32 bytes.
- This encrypts the **index/commit blobs** written via the storage layer. The file-based
  nameservice remains plaintext, matching the existing builder behavior.

```json
{
  "@id": "fileStorage",
  "@type": "Storage",
  "filePath": "/var/lib/fluree",
  "AES256Key": { "envVar": "FLUREE_ENCRYPTION_KEY" }
}
```

### S3 storage (requires `aws`)

Supported fields (parsed and **applied** by Rust):
- `s3Bucket`
- `s3Prefix`
- `s3Endpoint` (optional; recommended **only** for LocalStack/MinIO/custom endpoints)
- `s3ReadTimeoutMs`, `s3WriteTimeoutMs`, `s3ListTimeoutMs`
  - Rust applies a single **operation timeout** of `max(read, write, list)`
- `s3MaxRetries`, `s3RetryBaseDelayMs`, `s3RetryMaxDelayMs`
  - Rust maps `s3MaxRetries` to AWS SDK `max_attempts = max_retries + 1`

#### Standard S3 (AWS)

```json
{
  "@id": "s3",
  "@type": "Storage",
  "s3Bucket": "fluree-prod-data",
  "s3Prefix": "fluree/"
}
```

#### LocalStack / MinIO (custom endpoint)

```json
{
  "@id": "s3",
  "@type": "Storage",
  "s3Bucket": "fluree-test",
  "s3Endpoint": "http://localhost:4566",
  "s3Prefix": "fluree/"
}
```

#### S3 Express One Zone

Rust relies on the AWS SDK’s native support for directory buckets. We also provide bucket-name
detection (`--x-s3` + `-azN`) for diagnostics.

```json
{
  "@id": "s3Express",
  "@type": "Storage",
  "s3Bucket": "my-index--use1-az1--x-s3",
  "s3Prefix": "indexes/"
}
```

Note: omit `s3Endpoint` for Express directory buckets and let the AWS SDK handle endpoint
resolution. `FlureeBuilder::s3()` is designed for standard and LocalStack
endpoints; for Express buckets, use `FlureeBuilder::from_json_ld()` with a config that omits
`s3Endpoint`.

Guidance:
- **Standard S3 in AWS**: omit `s3Endpoint` (let the SDK pick defaults)
- **Express One Zone**: omit `s3Endpoint`
- **LocalStack/MinIO/custom**: set `s3Endpoint`

#### addressIdentifier

Rust parses `addressIdentifier` on storage nodes and uses it to rewrite **published**
commit/index ContentIds so they include the identifier segment, e.g.:
`fluree:{addressIdentifier}:s3://...`.

This is mainly useful when you have multiple storage backends and want addresses to
carry an explicit storage identifier.

## Split commit vs index storage (tiered S3)

Rust supports the tiered `commitStorage` + `indexStorage` format via `FlureeBuilder::from_json_ld()` / `build_client()`.
Internally, Rust routes:
- `.../commit/...` and `.../txn/...` → commit storage
- everything else → index storage

```json
{
  "@context": {"@base": "https://ns.flur.ee/config/connection/", "@vocab": "https://ns.flur.ee/system#"},
  "@graph": [
    { "@id": "commitStorage", "@type": "Storage", "s3Bucket": "commits-bucket", "s3Prefix": "fluree-data/" },
    { "@id": "indexStorage",  "@type": "Storage", "s3Bucket": "index--use1-az1--x-s3" },
    { "@id": "publisher", "@type": "Publisher", "dynamodbTable": "fluree-nameservice", "dynamodbRegion": "us-east-1" },
    {
      "@id": "connection",
      "@type": "Connection",
      "commitStorage": {"@id": "commitStorage"},
      "indexStorage":  {"@id": "indexStorage"},
      "primaryPublisher": {"@id": "publisher"}
    }
  ]
}
```

### IPFS storage (requires `ipfs`)

Supported:
- `ipfsApiUrl` (default `http://127.0.0.1:5001`): Kubo HTTP RPC API base URL
- `ipfsPinOnPut` (default `true`): pin blocks after writing

```json
{
  "@id": "ipfsStorage",
  "@type": "Storage",
  "ipfsApiUrl": "http://127.0.0.1:5001",
  "ipfsPinOnPut": true
}
```

With env var indirection:

```json
{
  "@id": "ipfsStorage",
  "@type": "Storage",
  "ipfsApiUrl": { "envVar": "FLUREE_IPFS_API_URL", "defaultVal": "http://127.0.0.1:5001" },
  "ipfsPinOnPut": true
}
```

Notes:
- Requires a running Kubo node at the specified URL
- Fluree's CIDs (SHA-256 + private-use multicodec) are stored directly into IPFS
- No encryption support (`AES256Key` is not applicable)
- See [IPFS Storage Guide](../operations/ipfs-storage.md) for Kubo setup and operational details

## Publisher (nameservice) node fields

### Storage-backed nameservice

Supported:
- `storage` (reference to a `Storage` node)

```json
{
  "@id": "publisher",
  "@type": "Publisher",
  "storage": { "@id": "s3" }
}
```

### DynamoDB nameservice (requires `aws`)

Supported (and applied):
- `dynamodbTable`
- `dynamodbRegion`
- `dynamodbEndpoint`
- `dynamodbTimeoutMs`

## Compatibility notes

This Rust JSON-LD model is intended to stay aligned with existing Fluree docs:
- `../db/docs/S3_STORAGE_GUIDE.md`
- `../db/docs/FILE_STORAGE_GUIDE.md`
- `../db/docs/DYNAMODB_NAMESERVICE_GUIDE.md`

Current intentional gaps in Rust:
- `remoteSystems` not supported
- `defaults.identity` is parsed but not currently applied
- `defaults.indexing.trackClassStats` is parsed but not currently applied

