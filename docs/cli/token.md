# fluree token

Manage JWS tokens for authentication with Fluree servers.

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `create` | Create a new JWS token |
| `keygen` | Generate a new Ed25519 keypair |
| `inspect` | Decode and verify a JWS token |

---

## fluree token create

Create a new JWS token for authenticating with Fluree servers.

### Usage

```bash
fluree token create --private-key <KEY> [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--private-key <KEY>` | **Required.** Ed25519 private key (hex, base58, `@filepath`, or `@-` for stdin) |
| `--expires-in <DUR>` | Token lifetime (default: `1h`). Supports `s`, `m`, `h`, `d`, `w` suffixes |
| `--subject <SUB>` | Subject claim (`sub`) - identity of the token holder |
| `--audience <AUD>` | Audience claim (`aud`) - repeatable for multiple audiences |
| `--identity <ID>` | Fluree identity claim (`fluree.identity`) - takes precedence over `sub` for policy |
| `--all` | Grant full access to all ledgers (events, storage, read, and write) |
| `--events-ledger <ALIAS>` | Grant events access to specific ledger (repeatable) |
| `--storage-ledger <ALIAS>` | Grant storage access to specific ledger (repeatable) |
| `--read-all` | Grant data API read access to all ledgers (`fluree.ledger.read.all=true`) |
| `--read-ledger <ALIAS>` | Grant data API read access to specific ledger (repeatable) |
| `--write-all` | Grant data API write access to all ledgers (`fluree.ledger.write.all=true`) |
| `--write-ledger <ALIAS>` | Grant data API write access to specific ledger (repeatable) |
| `--graph-source <ALIAS>` | Grant access to specific graph source (repeatable) |
| `--output <FMT>` | Output format: `token`, `json`, or `curl` (default: `token`) |
| `--print-claims` | Print decoded claims to stderr |

### Private Key Formats

| Format | Example |
|--------|---------|
| Hex | `0x<64 hex chars>` or `<64 hex chars>` |
| Base58 | `z<base58 string>` (multibase) or raw base58 |
| File | `@/path/to/keyfile` or `@~/.fluree/key` (tilde expansion) |
| Stdin | `@-` (read from stdin to avoid shell history) |

### Examples

```bash
# Create a token with full access
fluree token create --private-key 0x1234...abcd --all

# Create a token for specific ledgers (events/storage)
fluree token create --private-key @~/.fluree/key \
  --events-ledger mydb --storage-ledger mydb

# Create a token with data API read+write for specific ledgers
fluree token create --private-key @~/.fluree/key \
  --read-ledger mydb:main --write-ledger mydb:main

# Create a token with identity and audience
fluree token create --private-key @- \
  --identity did:example:alice \
  --audience https://api.example.com \
  --expires-in 7d

# Output as curl command
fluree token create --private-key 0x... --all --output curl

# View claims while creating
fluree token create --private-key 0x... --all --print-claims
```

---

## fluree token keygen

Generate a new Ed25519 keypair for signing tokens.

### Usage

```bash
fluree token keygen [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--format <FMT>` | Output format: `hex`, `base58`, or `json` (default: `hex`) |
| `-o, --output <PATH>` | Write private key to file (otherwise prints to stdout) |

### Examples

```bash
# Generate keypair in hex format
fluree token keygen

# Generate in JSON format with all representations
fluree token keygen --format json

# Save private key to file
fluree token keygen --output ~/.fluree/key

# Generate base58 format
fluree token keygen --format base58
```

### Output

Hex format:
```
Private key: 0x1234567890abcdef...
Public key:  0xabcdef1234567890...
DID:         did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
```

JSON format:
```json
{
  "private_key": {
    "hex": "0x1234...",
    "base58": "z..."
  },
  "public_key": {
    "hex": "0xabcd...",
    "base58": "z..."
  },
  "did": "did:key:z6Mk..."
}
```

---

## fluree token inspect

Decode and optionally verify a JWS token.

### Usage

```bash
fluree token inspect <TOKEN> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `<TOKEN>` | JWS token string or `@filepath` |

### Options

| Option | Description |
|--------|-------------|
| `--no-verify` | Skip signature verification (default: verify) |
| `--output <FMT>` | Output format: `pretty`, `json`, or `table` (default: `pretty`) |

### Examples

```bash
# Inspect and verify a token
fluree token inspect eyJhbGciOiJFZERTQSI...

# Inspect without verification
fluree token inspect eyJ... --no-verify

# Output as JSON
fluree token inspect eyJ... --output json

# Read token from file
fluree token inspect @token.txt
```

### Output

Pretty format:
```
Token Information
─────────────────────────────────────────────────────
Issuer:   did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
Subject:  test@example.com
Issued:   2024-01-15 10:30:00 UTC
Expires:  2024-01-15 11:30:00 UTC

Permissions:
  Events:  all ledgers
  Storage: all ledgers

Signature: ✓ Valid
```

## Token Scopes

Tokens can carry different permission scopes that control access to different server features:

| Scope | Claim | Controls |
|-------|-------|----------|
| Events (all) | `fluree.events.all` | SSE event stream for all ledgers |
| Events (specific) | `fluree.events.ledgers` | SSE event stream for listed ledgers |
| Storage (all) | `fluree.storage.all` | Storage proxy read access (all); also implies data API read |
| Storage (specific) | `fluree.storage.ledgers` | Storage proxy read access (listed); also implies data API read |
| Read (all) | `fluree.ledger.read.all` | Data API query access to all ledgers |
| Read (specific) | `fluree.ledger.read.ledgers` | Data API query access to listed ledgers |
| Write (all) | `fluree.ledger.write.all` | Data API write access to all ledgers |
| Write (specific) | `fluree.ledger.write.ledgers` | Data API write access to listed ledgers |

The `--all` flag sets events, storage, read, and write access for all ledgers.

**Back-compat:** `fluree.storage.*` claims also grant data API read access for the same ledgers.

## See Also

- [auth](auth.md) - Store/manage tokens on remotes
- [remote](remote.md) - Configure remote servers
- [Authentication](../security/authentication.md) - Auth model, modes, and token claims
- [fetch](fetch.md) - Fetch from remotes (requires auth token)
- [push](push.md) - Push to remotes (requires auth token)
