# Storage Encryption

Fluree supports transparent encryption of data at rest using AES-256-GCM authenticated encryption. When enabled, all data written to storage is automatically encrypted, and data is decrypted transparently when read.

## Overview

**Key Features:**
- **AES-256-GCM**: Industry-standard authenticated encryption with integrity protection
- **Transparent Operation**: Encryption/decryption happens automatically on read/write
- **All Storage Backends**: Works natively with file, S3, and memory storage (not IPFS)
- **Portable Ciphertext**: Encrypted data can be moved between storage backends (file ↔ S3)
- **Environment Variable Support**: Keys can be loaded from environment variables
- **Secure Key Handling**: Key material in `EncryptionKey` is zeroized on drop

## Quick Start

### Rust API

```rust
use fluree_db_api::FlureeBuilder;

// Option 1: Direct key (for testing)
let key: [u8; 32] = /* your 32-byte key */;
let fluree = FlureeBuilder::file("/data/fluree")
    .build_encrypted(key)?;

// Option 2: Base64-encoded key
let fluree = FlureeBuilder::file("/data/fluree")
    .with_encryption_key_base64("your-base64-encoded-32-byte-key")?
    .build_encrypted_from_config()?;

// Option 3: From JSON-LD config with env var (nodes are located by @id)
let config = serde_json::json!({
    "@context": {
        "@base": "https://ns.flur.ee/config/connection/",
        "@vocab": "https://ns.flur.ee/system#"
    },
    "@graph": [
        {
            "@id": "storage",
            "@type": "Storage",
            "filePath": "/data/fluree",
            "AES256Key": {"envVar": "FLUREE_ENCRYPTION_KEY"}
        },
        {"@id": "connection", "@type": "Connection", "indexStorage": {"@id": "storage"}}
    ]
});
let fluree = FlureeBuilder::from_json_ld(&config)?
    .build_encrypted_from_config()?;

// Option 4: any build path honours a configured key. This is what the
// server and embedders use; the storage type comes from the config.
let client = FlureeBuilder::from_json_ld(&config)?
    .build_client()
    .await?;
```

A key set on the builder (`with_encryption_key*()` or `AES256Key` in JSON-LD) is
applied by every terminal build method — `build()`, `build_memory()`, `build_s3()`,
`build_client()` and friends — on every backend. The `build_*_encrypted()` methods
remain for callers that want the key to be an explicit argument. Two build
methods are exceptions:

- `build_ipfs()` returns an error when a key is configured. IPFS storage cannot
  be wrapped for encryption, and it publishes to a content-addressed network, so
  silently writing plaintext is not an option.
- `build_with()` takes a storage you have already composed, and may already have
  wrapped in `EncryptedStorage`. It leaves the key to you and logs a warning when
  one is configured.

### Server Configuration

Set the encryption key via environment variable:

```bash
# Generate a secure 32-byte key and base64 encode it
export FLUREE_ENCRYPTION_KEY=$(openssl rand -base64 32)

# Start the server with JSON-LD config
./fluree-db-server --config config.jsonld
```

## Configuration

### JSON-LD Configuration

The encryption key is specified in the storage configuration using `AES256Key`:

```json
{
  "@context": {
    "@base": "https://example.org/config/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "indexStorage",
      "@type": "Storage",
      "filePath": "/var/lib/fluree/data",
      "AES256Key": {
        "envVar": "FLUREE_ENCRYPTION_KEY"
      }
    },
    {
      "@id": "mainConnection",
      "@type": "Connection",
      "indexStorage": {"@id": "indexStorage"},
      "cacheMaxMb": 2000
    }
  ]
}
```

### Configuration Options

| Field | Type | Description |
|-------|------|-------------|
| `AES256Key` | string or object | Base64-encoded 32-byte encryption key. A single key with id `0`. |
| `AES256Key.envVar` | string | Environment variable containing the key |
| `AES256Key.defaultVal` | string | Fallback key if env var is not set |
| `AES256Keys` | list | A key set for rotation: each entry a node with `keyId` (integer) and `AES256Key` (string or object as above). Every listed key decrypts. |
| `AES256CurrentKey` | integer | The `keyId` that encrypts new writes. Required with `AES256Keys`. |

`AES256Key` and `AES256Keys` are mutually exclusive. Key ids are written into
every envelope header, so once a key has an id, keep it.

### Environment Variable Indirection

You can load the encryption key from an environment variable:

```json
{
  "AES256Key": {
    "envVar": "FLUREE_ENCRYPTION_KEY"
  }
}
```

Or with a fallback default (not recommended for production):

```json
{
  "AES256Key": {
    "envVar": "FLUREE_ENCRYPTION_KEY",
    "defaultVal": "fallback-base64-key-for-dev-only"
  }
}
```

## Key Management

### Generating Keys

Generate a cryptographically secure 32-byte key:

```bash
# Using OpenSSL (recommended)
openssl rand -base64 32

# Using /dev/urandom
head -c 32 /dev/urandom | base64

# Example output: "K7gNU3sdo+OL0wNhqoVWhr3g6s1xYv72ol/pe/Unols="
```

### Key Storage Best Practices

1. **Never commit keys to version control**
2. **Use environment variables or secret managers**
3. **Rotate keys periodically** (see Key Rotation below)
4. **Limit access to key material**

Recommended secret management solutions:
- HashiCorp Vault
- AWS Secrets Manager
- Kubernetes Secrets
- Docker secrets

### Key Rotation

Keys rotate without export and import. Every envelope header records the id
of the key that encrypted it, so a storage configured with several keys reads
any of them while new writes use the current one. A rotation is then a
background sweep that re-envelopes every blob still on the retiring key. Two
properties make it safe to run over days while serving traffic:

- **Rewrites are in place.** Addresses are hashes of plaintext, so a blob's
  address does not change when its key does. Each rewrite is one atomic
  object write, verified by reading it back; a crash between blobs leaves each
  blob on exactly one key.
- **The blobs are the truth.** The progress record at
  `@maintenance/key-rotation.json` in the same storage caches where the sweep
  stood; resuming from a stale record only re-reads a few headers. Nothing
  removes a key automatically. Completion is a verification pass that finds
  zero blobs on the retiring key, after which an operator drops the key.

#### Rollout order

Nodes share storage, so the order matters more than the commands:

1. Add the new key to every node's configuration as a decrypt-only entry of
   `AES256Keys` and restart. Every node can now read blobs on either key.
2. Set `AES256CurrentKey` to the new key on every node and restart. All new
   writes now use it.
3. Run the sweep from one node (`fluree encryption rotate --retire <old>`).
4. Run `fluree encryption verify --retire <old>`. When it reports zero
   remaining, remove the old key from `AES256Keys` and restart.

A node restarted with the old key already removed cannot read blobs still on
it, and the sweep refuses to start or resume unless the process holds both the
retiring key and the current one.

```json
{
  "@id": "storage",
  "@type": "Storage",
  "filePath": "/var/lib/fluree",
  "AES256Keys": [
    {"keyId": 1, "AES256Key": {"envVar": "FLUREE_KEY_1"}},
    {"keyId": 2, "AES256Key": {"envVar": "FLUREE_KEY_2"}}
  ],
  "AES256CurrentKey": 2
}
```

#### Commands and endpoints

Every `fluree encryption` subcommand takes `--remote <name>` to run against a
server or `--connection-config <path>` to run directly against the storage the
config describes. `--json` prints the raw response.

| Command | Endpoint | What it does |
|---|---|---|
| `fluree encryption status` | `GET /v1/fluree/encryption`, `GET /v1/fluree/encryption/rotate/status` | Held key ids, the current key, and the progress record. Any node answers. |
| `fluree encryption rotate --retire N [--dry-run] [--ledger L] [--rate 50mb] [--wait]` | `POST /v1/fluree/encryption/rotate` | Start, or resume, the sweep. A dry run counts and writes nothing. `--wait` polls status until the sweep stops. |
| `fluree encryption resume [--wait]` | `POST /v1/fluree/encryption/rotate` | Continue the rotation the record describes. |
| `fluree encryption pause` / `cancel` | `POST /v1/fluree/encryption/rotate/pause` / `cancel` | Stop after the next blob. A paused sweep resumes from its cursor; a cancelled one starts over. |
| `fluree encryption verify --retire N` | `POST /v1/fluree/encryption/rotate/verify` | Count blobs still on key N across the whole store, by header, and stamp the record. |
| `fluree encryption generate-key` | — | Print a fresh base64 key. |

The write endpoints are admin-gated. Under Raft they are forwarded to the
leader, which is the only node that runs the sweep; on a leadership change the
outgoing leader releases the record and the new leader resumes it. A
standalone server resumes a `Running` record at startup. No operator action is
needed after a restart.

#### Reading status

The record reports `state` (`running`, `paused`, `cancelled`, `failed`,
`swept`, `completed`), the holder, the unit in progress (a ledger branch, a
ledger's shared dictionaries, or graph sources) with the last address done,
and counters: scanned, rewritten, already current, on other held keys, not
enveloped (nameservice records and lock files), and failed with the first
hundred failing addresses. `swept` means the sweep finished but verification
found stragglers; `resume` retries them. `stalled` is set when a `running`
record has not checkpointed for ten minutes: nobody is advancing it, and the
next `rotate` or a leader election takes it over.

Only `completed` — a verification stamp with zero remaining — licenses
removing the retiring key.

## Encryption Details

### Algorithm

- **Cipher**: AES-256-GCM (Galois/Counter Mode)
- **Key Size**: 256 bits (32 bytes)
- **Nonce Size**: 96 bits (12 bytes), randomly generated per write
- **Tag Size**: 128 bits (16 bytes)

### Ciphertext Envelope Format

All encrypted data uses a portable envelope format:

```
┌──────────────────────────────────────────────────────────────┐
│ Header (22 bytes)                                            │
├──────────┬─────────┬─────────┬──────────┬───────────────────┤
│ Magic    │ Version │ Alg     │ Key ID   │ Nonce             │
│ 4 bytes  │ 1 byte  │ 1 byte  │ 4 bytes  │ 12 bytes          │
│ "FLU\0"  │ 0x01    │ 0x01    │ uint32   │ random            │
├──────────┴─────────┴─────────┴──────────┴───────────────────┤
│ Ciphertext (variable length)                                 │
├──────────────────────────────────────────────────────────────┤
│ Authentication Tag (16 bytes)                                │
└──────────────────────────────────────────────────────────────┘
```

- **Magic bytes**: `FLU\0` (0x46 0x4C 0x55 0x00) for format detection
- **Version**: Format version (currently 0x01)
- **Algorithm**: 0x01 = AES-256-GCM
- **Key ID**: Identifier for key rotation support
- **Nonce**: Randomly generated per encryption operation
- **Authentication Tag**: GCM integrity tag (authenticates header + ciphertext)

### Security Properties

1. **Confidentiality**: AES-256 encryption protects data content
2. **Integrity**: GCM authentication tag detects tampering
3. **Authenticity**: Header is included in AAD (Additional Authenticated Data)
4. **Non-deterministic**: Random nonces mean same plaintext → different ciphertext

## Portability

Encrypted data is portable between storage backends:

```bash
# Encrypted files can be copied from local storage to S3
aws s3 sync /var/lib/fluree/data s3://my-bucket/fluree/

# And back again
aws s3 sync s3://my-bucket/fluree/ /var/lib/fluree/data
```

The same encryption key will decrypt data regardless of where it's stored.

## What Stays Plaintext

Encryption covers every blob written through the storage layer: commits,
transactions, index roots, branches, leaves, dictionaries and arenas. These
are outside it by design:

- **The nameservice.** The file nameservice under `ns@v2/` and the DynamoDB or
  S3 storage-backed nameservice hold ledger names, head commit ids and index
  root ids in plaintext. They contain no ledger content.
- **Nothing else on local disk, once a build is done.** Readers keep a
  read-through disk cache of index artifacts (`$TMPDIR/fluree_binary_cache` by
  default, or `LedgerManagerConfig::cache_dir`), and the indexer seeds it with
  artifacts it just built. With encryption enabled that cache is bypassed
  entirely: no decrypted leaf, branch, dictionary or vector shard is written
  outside the encrypted storage, and nothing already in the cache directory is
  consulted. Fetched artifacts are served from memory instead.

  **Upgrading from an earlier release.** Earlier releases did write decrypted
  index artifacts to this cache when encryption was enabled. This release no
  longer reads them, but it does not delete them. After upgrading, stop the
  server and delete the cache directory to remove those plaintext copies from
  disk. The same applies to a host that held a ledger unencrypted before it was
  re-imported with a key.
- **Peers reading through a proxy.** A peer in proxy mode fetches artifacts over
  HTTP from a server. It holds no key, and what it receives is plaintext even
  when that server encrypts at rest, so the peer's own disk cache holds
  plaintext. Encryption at rest covers the server's storage, not a peer's local
  disk. Protect a peer's cache directory as you would the ledger data itself.

### Index build staging

A full index rebuild is an external sort. While it runs it stages sorted
commit runs, dictionaries and leaves in plaintext under per-session
directories, `{data_dir}/{ledger}/tmp_import/{session}` and
`{data_dir}/{ledger}/index/{session}`. Those directories are removed on every
exit — success, error, or a panic in the build task — so nothing outlives the
build. Only a process killed mid-build leaves its session directories behind;
nothing removes those automatically, so clear `tmp_import` after a crash.

`data_dir` (`IndexerConfig::data_dir`; the server's indexer data directory)
defaults to `$TMPDIR/fluree-index`. For an encrypted deployment point it at a
directory on an encrypted volume; the rebuild logs a warning when encryption
is on and `data_dir` is unset.

A bulk import stages the same kind of plaintext, but not under `data_dir`: it
uses `$TMPDIR/fluree-import/{ledger}/tmp_import/{session}`, or the directory
named by `FLUREE_IMPORT_DIR`. For an encrypted deployment, point
`FLUREE_IMPORT_DIR` at an encrypted volume too. Import removes its session
directory when it finishes, on success or error, unless `cleanup_local_files`
is turned off. Unlike a rebuild, an import that panics or is cancelled leaves
its session directory behind
([#1950](https://github.com/fluree/db/issues/1950)).

## Performance Considerations

- **CPU overhead**: ~5-15% for encryption/decryption (depends on hardware AES support)
- **Storage overhead**: 22 bytes header + 16 bytes tag per object
- **Memory**: Keys are kept in memory while the connection is open
- **No disk cache**: because the read-through disk cache is bypassed (see above),
  a remote backend such as S3 re-fetches an index artifact whenever it falls out
  of the in-memory leaflet cache. Concurrent readers of the same artifact still
  share one fetch. Size that cache (`cacheMaxMb`) for the working set. File
  storage is unaffected: it reads and decrypts blobs in place.

Modern CPUs with AES-NI instructions provide hardware acceleration, minimizing the performance impact.

## Troubleshooting

### Common Errors

**"Invalid encryption format"**
- The data doesn't have the expected magic bytes
- Possible causes: trying to read unencrypted data with encryption enabled, or corrupted data

**"Unknown encryption key ID"**
- The data was encrypted with a different key than what's configured
- Check that the correct key is being used

**"Decryption failed"**
- The encryption key doesn't match
- The data may be corrupted
- The authentication tag verification failed (data was tampered with)

**"Encryption key must be 32 bytes"**
- The provided key is the wrong length
- Base64-decode your key and verify it's exactly 32 bytes

### Verifying Encryption

Check if a file is encrypted by looking for the magic bytes:

```bash
# Check first 4 bytes of a file
xxd -l 4 /var/lib/fluree/data/some-file
# Encrypted: 00000000: 464c 5500  FLU.
# Unencrypted: will show different bytes (likely JSON or Avro magic)
```

## Changing Encryption Settings

### Enabling Encryption on Existing Data

To encrypt existing unencrypted data:

1. **Export** all ledgers to JSON-LD
2. **Delete** the old unencrypted data directory
3. **Configure** encryption with a new key
4. **Import** the JSON-LD data

```bash
# 1. Export (while running without encryption)
fluree export mydb:main --format json-ld > mydb-export.jsonld

# 2. Stop server and backup/delete old data
mv /var/lib/fluree/data /var/lib/fluree/data-unencrypted-backup

# 3. Configure encryption key
export FLUREE_ENCRYPTION_KEY=$(openssl rand -base64 32)
echo "Save this key securely: $FLUREE_ENCRYPTION_KEY"

# 4. Start server with encryption config and import
./fluree-db-server --config encrypted-config.jsonld
fluree create mydb --from mydb-export.jsonld
```

### Disabling Encryption

> **Warning**: This exposes your data. Only do this if absolutely necessary.

Follow the same export/import process, but configure without an encryption key.

## Related Documentation

- [Storage Modes](../operations/storage.md) - Storage backend configuration
- [Configuration](../operations/configuration.md) - General configuration reference
- [Policy Model](policy-model.md) - Access control and authorization
