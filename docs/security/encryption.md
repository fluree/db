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

// Option 5: a key set for rotation, from the Rust API: (key id, key) pairs
// and the id that encrypts new writes.
let fluree = FlureeBuilder::file("/data/fluree")
    .with_encryption_keys(vec![(1, old_key), (2, new_key)], 2)?
    .build()?;
```

A key set on the builder (`with_encryption_key*()`, `with_encryption_keys()`,
or `AES256Key` / `AES256Keys` in JSON-LD) is applied by every terminal build method — `build()`, `build_memory()`, `build_s3()`,
`build_client()` and friends — on every backend. The `build_*_encrypted()` methods
remain for callers that want the key to be an explicit argument;
`build_encrypted(key)` replaces any configured key set with that one key, as
id `0`. Two build methods are exceptions:

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
fluree server run --connection-config config.jsonld
```

## Configuration

### JSON-LD Configuration

The encryption key is specified in the storage configuration using `AES256Key`,
on the node the connection names as `indexStorage`:

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
every envelope header, so once a key has an id, keep it. The key is standard or
URL-safe base64 and must decode to exactly 32 bytes. `keyId` and
`AES256CurrentKey` accept the same `envVar` indirection as the key itself.

**Where the key goes.** Put the key on the `indexStorage` node. With a separate
`commitStorage` (for example a second S3 bucket), the `indexStorage` key
encrypts both, and a `commitStorage` node carrying a different key is rejected
at startup rather than ignored. Storages listed under `addressIdentifiers` carry
their own keys.

### Environment Variable Indirection

You can load the encryption key from an environment variable:

```json
{
  "AES256Key": {
    "envVar": "FLUREE_ENCRYPTION_KEY"
  }
}
```

If the variable is unset or empty and no `defaultVal` is given, the connection
config fails to load: Fluree refuses to start a storage unencrypted because a
key did not resolve. `javaProp` is also accepted and read from the environment
variable of that name.

Or with a fallback default, used when the variable is unset or empty (not
recommended for production):

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

# Using the Fluree CLI
fluree encryption generate-key

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
3. Run the sweep from one node
   (`fluree encryption rotate --remote <name> --retire <old>`).
4. Run `fluree encryption verify --remote <name> --retire <old>`. When it
   reports zero remaining, remove the old key from `AES256Keys` and restart.

**Coming from a single `AES256Key`.** A single key has id `0`, and every blob
written under it records id `0`. List it under that id when you move to a key
set, then add the new key and retire `0`:

```json
"AES256Keys": [
  {"keyId": 0, "AES256Key": {"envVar": "FLUREE_KEY_OLD"}},
  {"keyId": 1, "AES256Key": {"envVar": "FLUREE_KEY_NEW"}}
],
"AES256CurrentKey": 0
```

That is step 1; step 2 sets `AES256CurrentKey` to `1`, and steps 3 and 4 retire
`0`. Giving the old key any other id makes every existing blob fail with
"Unknown encryption key ID: 0".

A node restarted with the old key already removed cannot read blobs still on
it, and the sweep refuses to start or resume unless the process holds both the
retiring key and the current one. Nothing checks this order across nodes: each
node reports only its own key ids, so following it is the operator's job
([#1952](https://github.com/fluree/db/issues/1952)).

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

Every `fluree encryption` subcommand except `generate-key` needs one of
`--remote <name>`, to run against a server, or `--connection-config <path>`, to
run directly against the storage that config describes. `--json` prints the raw
response. See [`fluree encryption`](../cli/encryption.md) for flags and output.

| Command | Endpoint | What it does |
|---|---|---|
| `fluree encryption status` | `GET /v1/fluree/encryption`, `GET /v1/fluree/encryption/rotate/status` | Held key ids, the current key, and the progress record. Any node answers. |
| `fluree encryption rotate --retire N [--dry-run] [--ledger L] [--rate 50mb] [--wait]` | `POST /v1/fluree/encryption/rotate` | Start, or resume, the sweep. A dry run counts and writes nothing. `--wait` polls status until the sweep stops. |
| `fluree encryption resume [--wait]` | `POST /v1/fluree/encryption/rotate` | Re-issue `rotate` for the record's retiring key and scope. |
| `fluree encryption pause` / `cancel` | `POST /v1/fluree/encryption/rotate/pause` / `cancel` | Stop once the blob in progress is done. A paused sweep resumes from its cursor; a cancelled one starts over. |
| `fluree encryption verify --retire N` | `POST /v1/fluree/encryption/rotate/verify` | Count blobs still on key N across the whole store, by header, and stamp the record. |
| `fluree encryption generate-key` | — | Print a fresh base64 key. |

All six endpoints, the two status reads included, sit behind the admin-token
gate, which is enforced only with `--admin-auth-mode required`. Under the
default (`none`) anyone who can reach the server can start, pause, cancel or
verify a rotation, so enable admin auth before exposing a server that encrypts.

Under Raft the writes are forwarded to the leader, which is the only node that
runs the sweep. The status reads are answered by whichever node receives them.

#### Resuming after a restart or leader change

A sweep resumes by itself only in some cases; otherwise
`fluree encryption resume` continues it.

- A standalone server tries once, at startup, to resume a `running` record. It
  succeeds when the record's holder is this same process identity (host name
  and process id, so in practice only when both repeat, as for a container
  running as pid 1) or when the record's last checkpoint is more than ten
  minutes old. After an ordinary restart the record usually shows `stalled`
  ten minutes later; run `resume` then.
- Under Raft, a leader that loses leadership cleanly releases the record, and
  the new leader takes it over when it starts. The holder is the Raft node id,
  so a crashed leader that is elected again continues its own record. If a
  different node's check ran before the release was written, or the old leader
  crashed, nobody advances the record: it shows `released` or, ten minutes after
  a crash, `stalled`. Run `resume` against the leader; a released record is
  taken over at once.
- A throttle set with `--rate` is not stored in the record. A sweep resumed any
  way other than `rotate --retire N --rate …` (same key and scope) runs
  unthrottled ([#1954](https://github.com/fluree/db/issues/1954)).

Automatic retries are tracked in
[#1953](https://github.com/fluree/db/issues/1953).

#### Reading status

The record reports:

- `state`: `running`, `paused`, `cancelled`, `failed`, `swept` or `completed`.
- The holder, `dry_run`, `ledger_scope`, and progress: `units_done` of
  `units_total` (each ledger branch, then each ledger's shared dictionaries,
  then graph sources), the unit in progress and the last address done.
- Counters: `scanned`, `rewritten`, `already_current`, `on_other_keys`,
  `not_enveloped` (lock files and any blob written before encryption was
  enabled), `on_retired` (blobs found on the retiring key; in a
  dry run, the work a real run would do), `bytes_rewritten`, and `failed` with
  the first hundred failing addresses.
- `last_error`, and `completion` (`verified_at`, `remaining_on_retired`) once
  verification has run.

The status view adds `active_here` (the sweep runs in the answering process),
`seconds_since_update`, `stalled` and `released`. The sweep checkpoints every
thousand blobs or thirty seconds while it rewrites. `stalled` is set when a
`running` record has not checkpointed for ten minutes, and `rotate` or `resume`
takes it over. Listing a unit and the closing verification write no checkpoint,
so on a large store a live sweep can show `stalled` while it lists or verifies:
check the holder first, and do not resume while the holder's own status shows
`active_here: true`. `released` is set when the last holder let go on a
leadership change; see
[Resuming after a restart or leader change](#resuming-after-a-restart-or-leader-change).

`swept` means the sweep finished but verification found blobs still on the
retiring key, or could not list the store (`last_error` says which); `resume`
retries them.

**Only `completed` with `completion.remaining_on_retired` equal to `0` licenses
removing the retiring key.** A dry run also ends as `completed`, with
`dry_run: true` and no `completion`: it counted, it did not verify, and it is
held only in the memory of the node that ran it (the leader, under Raft).
That node's status keeps showing the dry run until another sweep starts there,
and `resume` after a dry run starts a real sweep. Running `verify` for a
different key id than the record's replaces the record.

#### Scope and limits

- **Rotation re-encrypts; it does not encrypt.** A blob written before
  encryption was enabled has no envelope, and the sweep counts it as not
  enveloped and leaves it alone. To encrypt an existing plaintext store, see
  [Enabling Encryption on Existing Data](#enabling-encryption-on-existing-data).
- **`--ledger` narrows the sweep, not the verification.** A ledger name
  covers every branch of that ledger, a branch-qualified id only that branch;
  either way the ledger's shared dictionaries are included and graph sources
  are not. The scoped sweep's closing verification still counts the whole
  store, so it ends `swept` while anything else is still on the retiring key;
  an unscoped `rotate --retire N` finishes the rest.
- **Local runs.** With `--connection-config` the sweep runs inside the CLI
  process, which blocks until it stops, `--wait` or not. `pause` and `cancel`
  act on the process running the sweep, so they work only against a server.
  Interrupting a local sweep leaves the record `running`; `resume` takes it
  over once it shows `stalled`.
- **Storages reached through `addressIdentifiers` are not rotated.** This node
  writes and lists only its default storage, so that is what it rotates and
  what `GET /v1/fluree/encryption` reports. A routed storage is read-only here
  and has its own key configuration; rotate it from the deployment that writes
  it.
- **Cost.** The sweep lists each unit's addresses into memory before walking
  them. Verification, which runs at the end of every sweep and on each
  `verify`, lists the whole store at once and reads one envelope header per
  blob. Budget roughly 100–150 bytes of memory per blob in the store, and one
  ranged read per blob for each verification pass. Streaming the listing is
  tracked in [#1951](https://github.com/fluree/db/issues/1951).

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
│ "FLU\0"  │ 0x01    │ 0x01    │ u32 LE   │ random            │
├──────────┴─────────┴─────────┴──────────┴───────────────────┤
│ Ciphertext (variable length)                                 │
├──────────────────────────────────────────────────────────────┤
│ Authentication Tag (16 bytes)                                │
└──────────────────────────────────────────────────────────────┘
```

- **Magic bytes**: `FLU\0` (0x46 0x4C 0x55 0x00) for format detection
- **Version**: Format version (currently 0x01)
- **Algorithm**: 0x01 = AES-256-GCM
- **Key ID**: Identifier for key rotation support, little-endian
- **Nonce**: Randomly generated per encryption operation
- **Authentication Tag**: GCM integrity tag (authenticates header + ciphertext)

### Security Properties

1. **Confidentiality**: AES-256 encryption protects data content
2. **Integrity**: GCM authentication tag detects tampering
3. **Authenticity**: Header is included in AAD (Additional Authenticated Data)
4. **Non-deterministic**: Random nonces mean same plaintext → different ciphertext

Integrity is per blob. The tag detects a modified blob, but the envelope is not
bound to its address, so it does not detect one valid blob substituted for
another under the same key, nor a rollback of the plaintext nameservice.
Encryption protects confidentiality at rest; control write access to storage
separately.

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
  S3 storage-backed nameservice hold, in plaintext: ledger and branch names,
  head commit and index root ids, default-context and config ids, and
  graph-source definitions including their configuration (for example a BM25
  index's query, or an Iceberg or R2RML source's settings). They hold no
  flakes or commit data. Under Raft the same state is also in each node's Raft
  log and snapshots under `--raft-storage-path`; put that directory on an
  encrypted volume ([#1955](https://github.com/fluree/db/issues/1955)).
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
- **Uploaded import files.** With `--import-presign-enabled`, uploaded `.flpack`
  archives and source files are staged in plaintext under
  `--import-staging-dir` (`FLUREE_IMPORT_STAGING_DIR`, default
  `$TMPDIR/fluree-import-staging`) until the restore finishes. Put that
  directory on an encrypted volume.
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
nothing removes those automatically, so after a crash clear both
`{data_dir}/{ledger}/tmp_import/` and `{data_dir}/{ledger}/index/`.

`data_dir` defaults to `$TMPDIR/fluree-index`. Embedders set it with
`IndexerConfig::data_dir` through `FlureeBuilder::with_indexer_config`. The
server has no setting for it, so its rebuilds always stage under
`$TMPDIR/fluree-index`: for an encrypted server, set `TMPDIR` in the server's
environment to a directory on an encrypted volume
([#1955](https://github.com/fluree/db/issues/1955)). The rebuild logs a warning
when encryption is on and `data_dir` is unset.

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
  storage has no network round trip, but each cache miss re-reads and
  decrypts the whole file rather than mapping it.

Modern CPUs with AES-NI instructions provide hardware acceleration, minimizing the performance impact.

## Troubleshooting

### Common Errors

**"Invalid encryption format"**
- The data doesn't have the expected magic bytes
- Possible causes: trying to read unencrypted data with encryption enabled, or corrupted data

**"Unknown encryption key ID: N"**
- The blob was encrypted with key id N, which this node does not hold
- A key was removed from `AES256Keys` before `verify` reported zero remaining,
  or a node was not given the new key before another node made it current
- Moving from a single `AES256Key` to `AES256Keys`: the old key must keep id `0`

**"Decryption failed"**
- The encryption key doesn't match
- The data may be corrupted
- The authentication tag verification failed (data was tampered with)

**"Invalid encryption key: Invalid key: key must be exactly 32 bytes when decoded"**
(or "…: invalid base64 encoding"; "Invalid encryption key N: …" for an
`AES256Keys` entry)
- The key is not valid base64, or does not decode to exactly 32 bytes

**"AES256Key is configured but did not resolve to a key"**
- The `envVar` it names is unset or empty, and there is no `defaultVal`

**"commitStorage carries an encryption key that indexStorage does not"**
- Move the key to the `indexStorage` node; it encrypts commit storage too

**Key set errors at startup**
- "AES256Key and AES256Keys are mutually exclusive": use one form
- "AES256Keys entry requires AES256Key" / "AES256Keys entry requires keyId": an
  entry is missing its key (or its `envVar` did not resolve) or its id
- "AES256Keys requires AES256CurrentKey": name the key that encrypts new writes
- "AES256CurrentKey N is not among the AES256Keys ids": list that key

**Rotation refused**
- "key N is the current key" / "key N is not held": `--retire` must name a
  held key other than the current one
- 409 "a key rotation is running on …": another holder checkpointed within ten
  minutes; wait for `stalled`, or `resume` a `released` record
- 409 "no key rotation is running in this process": `pause` and `cancel` reach
  only a sweep running in the process you ask; use `--remote`

### Verifying Encryption

Check if a file is encrypted by looking for the magic bytes:

```bash
# Check the first 4 bytes of a blob, for example a commit
xxd -l 4 /var/lib/fluree/data/<ledger>/<branch>/commit/<file>
# Encrypted:   00000000: 464c 5500  FLU.
# Unencrypted: another format's magic, for example FCV2, FLI3 or FIR6, or JSON
```

Compare all four bytes: `FLI3` also starts with `46 4c`. Files under `ns@v2/`
and lock files are plaintext by design. After every start, confirm with
`fluree encryption status`, which lists the key ids a node holds, or
`GET /v1/fluree/encryption`, which returns `encrypted: true`.

## Changing Encryption Settings

### Enabling Encryption on Existing Data

Key rotation cannot do this: it re-encrypts enveloped blobs and leaves
plaintext ones alone. Move each ledger through a `.flpack` archive instead,
which keeps its commits and history:

1. **Export** each ledger from the unencrypted server as a ledger archive
2. **Stop** the server and move the unencrypted data aside
3. **Configure** encryption and start the server on a fresh storage path
4. **Restore** each archive into the encrypted server

```bash
# 1. Export (while running without encryption); one archive per branch
fluree export mydb:main --remote old --format ledger -o mydb.flpack

# 2. Stop the server and move the old data aside
mv /var/lib/fluree/data /var/lib/fluree/data-unencrypted-backup

# 3. Configure an encryption key and start the server with it
export FLUREE_ENCRYPTION_KEY=$(fluree encryption generate-key)
echo "Save this key securely: $FLUREE_ENCRYPTION_KEY"
fluree server run --connection-config encrypted-config.jsonld

# 4. From another shell, restore into the encrypted server
fluree create mydb --remote new --from mydb.flpack
```

Then delete what is still plaintext: the `.flpack` files, the moved-aside data
directory once you no longer need it, and the old read-through disk cache
(`$TMPDIR/fluree_binary_cache` by default). `fluree export --format json-ld`
also works but carries only the current state of the default graph, with no
history and no named graphs.

### Disabling Encryption

> **Warning**: This exposes your data. Only do this if absolutely necessary.

Follow the same export/import process, but configure without an encryption key.

## Related Documentation

- [`fluree encryption`](../cli/encryption.md) - Key rotation commands
- [Storage Modes](../operations/storage.md) - Storage backend configuration
- [Configuration](../operations/configuration.md) - General configuration reference
- [Policy Model](policy-model.md) - Access control and authorization
