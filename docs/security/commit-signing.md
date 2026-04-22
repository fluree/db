# Commit Signing and Attestation

Fluree supports cryptographic signing at two levels:

1. **Transaction signatures** prove **who submitted** a transaction (user-facing). See [Signed Transactions](../transactions/signed-transactions.md).
2. **Commit signatures** prove **which node wrote** a commit (infrastructure-facing). This page covers commit signatures.

Both use **did:key** identifiers with **Ed25519** signatures, aligning with the credential infrastructure in `fluree-db-credential`.

**Note:** Requires the `credential` feature flag. See [Compatibility and Feature Flags](../reference/compatibility.md#fluree-db-api-features).

## Transaction Signatures vs Commit Signatures

These two signature types serve different purposes:

| | Transaction Signature | Commit Signature |
|---|---|---|
| **Proves** | Who submitted the transaction | Which node wrote the commit |
| **Signed by** | End user (client-side) | Fluree node (server-side) |
| **Trust model** | User authentication | Infrastructure integrity |
| **Format** | JWS / Verifiable Credential | Domain-separated Ed25519 over commit hash |
| **Stored in** | Commit envelope (`txn_signature`) | Trailing signature block after commit hash |

A single commit can have both: a transaction signature from the user who submitted it, and a commit signature from the node that wrote it.

## How Commit Signing Works

### Commit Digest

When a commit is written, its content is hashed with SHA-256 to produce a `commit_hash`. The signing digest is then computed with domain separation to prevent cross-protocol and cross-ledger replay:

```text
to_sign = SHA-256("fluree/commit/v1" || varint(ledger_id.len()) || ledger_id || commit_hash)
```

Where:
- `"fluree/commit/v1"` is a domain separator (18 bytes ASCII)
- `ledger_id` is the ledger ID (`name:branch`, length-prefixed)
- `commit_hash` is the 32-byte SHA-256 of the commit content

### Signature Block Layout

The signature block is appended **after** the commit hash and is not covered by it:

```text
+-------------------------------------+
| Header (32 bytes)                   |
|   flags: includes HAS_COMMIT_SIG    |
+-------------------------------------+
| Envelope + Ops + Dictionaries       |
+-------------------------------------+
| Footer (64 bytes)                   |
+-------------------------------------+
| commit_hash (32 bytes)              |
+-------------------------------------+
| Signature Block (optional)          |  <-- after hash boundary
|   sig_count: u16                    |
|   signatures: [CommitSignature]     |
+-------------------------------------+
```

This design means:
- `commit_hash` is stable regardless of signatures
- Signatures can be added without changing the commit's content address
- Existing verification (hash check) works unchanged

### Signature Entry Format

Each signature entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `signer` | String | Signer identity (`did:key:z6Mk...`) |
| `algo` | u8 | Signing algorithm (`0x01` = Ed25519) |
| `signature` | [u8; 64] | Ed25519 signature bytes |
| `timestamp` | i64 | Signing time (epoch millis, informational only) |
| `metadata` | Option\<Vec\<u8\>\> | Optional metadata (node_id, region, role for consensus) |

The `algo` byte provides forward compatibility for new signature algorithms. Unknown `algo` values are rejected on decode (not silently skipped).

The `timestamp` is informational only and is **not** part of the signed digest. Ordering is determined by the commit chain, not by signature timestamps.

The `metadata` field is reserved for future consensus features (multi-node signing, quorum sets). It allows nodes to include identifying information like node ID, region, or role. Currently unused but present in the format to avoid future versioning.

## Enabling Commit Signing (Rust API)

Commit signing is opt-in via `CommitOpts` when using the Rust API:

```rust
use std::sync::Arc;
use fluree_db_novelty::SigningKey;

// Load or generate an Ed25519 signing key
let signing_key = Arc::new(SigningKey::from_bytes(&key_bytes));

// Attach to commit options
let opts = CommitOpts::default()
    .with_signing_key(signing_key);
```

When a signing key is present, the commit writer:
1. Computes the domain-separated digest from the commit hash and ledger ID
2. Signs the digest with Ed25519
3. Appends the signature block after the commit hash
4. Sets the `FLAG_HAS_COMMIT_SIG` bit in the header

## Verifying Commit Signatures

Verification recomputes the domain-separated digest and checks the Ed25519 signature:

```rust
use fluree_db_credential::verify_commit_digest;

verify_commit_digest(
    &signer_did,       // "did:key:z6Mk..."
    &signature_bytes,  // [u8; 64]
    &commit_hash,      // [u8; 32]
    ledger_id,           // "mydb:main"
)?;
```

The verifier:
1. Extracts the Ed25519 public key from the `did:key` identifier
2. Recomputes `to_sign = SHA-256("fluree/commit/v1" || varint(ledger_id.len()) || ledger_id || commit_hash)`
3. Verifies the signature over `to_sign`

No external key registry is needed for `did:key` identifiers — the public key is embedded in the DID itself.

## Wire Format

Each `CommitSignature` is encoded as:

```
signer_len:   u16 (LE)          - length of signer string
signer:       [u8; signer_len]  - UTF-8 did:key identifier
algo:         u8                - signature algorithm (0x01 = Ed25519)
signature:    [u8; 64]          - Ed25519 signature bytes
timestamp:    i64 (LE)          - signing timestamp (epoch millis)
meta_len:     u16 (LE)          - metadata length (0 if none)
metadata:     [u8; meta_len]    - optional metadata bytes
```

The signature block is prefixed with `sig_count: u16` (LE) containing the number of signatures.

## Security Properties

### Replay Prevention

- **Cross-ledger:** The ledger ID is part of the signed digest, so a signature from ledger A cannot be replayed on ledger B
- **Cross-protocol:** The domain separator `"fluree/commit/v1"` prevents signatures meant for other systems from being accepted
- **Version upgrade:** Changing the domain separator (e.g., `v1` to `v2`) invalidates old signatures

### What Commit Signatures Do Not Provide

- **Transaction authorization:** Use [transaction signatures](../transactions/signed-transactions.md) and [policies](policy-model.md) for user-level access control
- **Consensus:** A single commit signature proves one node wrote it. Multi-node consensus requires attestation policies (see below)
- **Encryption:** Commit signing provides integrity and authenticity, not confidentiality. See [Storage Encryption](encryption.md) for data-at-rest protection

## Future: Attestations and Consensus Policy

The following capabilities are designed but not yet implemented.

### Detached Attestations

For multi-node deployments, signatures can be collected as separate attestation objects rather than embedded in the commit:

- Commit file remains immutable and content-addressed
- Signatures collected asynchronously from multiple nodes
- No coordination needed during commit write
- Attestations from different nodes can arrive at different times

### Consensus Policy

Consensus policy will define how many signatures are required for a commit to be accepted:

- **None:** No signatures required (default)
- **Single signer:** One designated writer must sign
- **Threshold (K-of-N):** At least K signatures from an allowlist of N signers
- **Quorum set:** At least one signature from each required group

Policy validation runs after commit hash integrity check, before accepting the commit.

## Related Documentation

- [Signed Transactions](../transactions/signed-transactions.md) — User-facing transaction signing (JWS/VC)
- [Verifiable Data](../concepts/verifiable-data.md) — Cryptographic verification concepts
- [Storage Encryption](encryption.md) — Data-at-rest encryption
- [Commit Receipts](../transactions/commit-receipts.md) — Commit metadata and content hashes
