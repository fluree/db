# Experimental journal foundation

Enabled only by `experimental-local-journal` on Unix. **This feature does not turn
on database WAL persistence.** Existing storage, transaction acknowledgments, and
CLI/server behavior are unchanged. The format is experimental and not yet a
supported database storage format.

## Implemented boundary

`Journal<I: JournalIo>` uses positioned reads/writes and explicit `sync_all`.
`FileIo` implements that same boundary with a Unix file and an exclusive advisory
inode lock. Creation uses `create_new`, syncs the new file and its parent directory,
then journal initialization writes and syncs the header. The parent and its ancestors
must already exist durably. This inode lock is not a database-root ownership lock.

The journal has a 56-byte header: eight-byte versioned magic `FLWAL001`, a
caller-supplied fresh 16-byte generation identity, and SHA-256 of those 24 bytes.
Each frame contains `FR01`, a little-endian u32 payload length, a little-endian u64
sequence starting at one, the preceding header/frame digest (32 bytes), a JSON
transition payload, and SHA-256 of the frame header and payload (32 bytes).

The payload stores ledger/generation, exact expected/resulting head bytes, and
immutable objects with relative storage keys and exact bytes. It has no dependency
on the nameservice crate. Payloads are capped at 16 MiB **after encoding**; the whole
journal is capped at 64 MiB. Byte arrays in this initial JSON codec add substantial
space/CPU overhead; this is a correctness prototype, not a latency result or the
final efficient binary encoding. Oversize input is rejected before file effects.

Every append returns a position/digest receipt only after a successful sync.
Short writes and interrupted I/O are retried; write/flush errors poison the writer.
An unsuccessful flush may still have persisted its complete record. Recovery scans
and verifies the entire stream before returning records, retains complete uncertain
records, and syncs the validated stream before allowing further appends.

`replay_chain` handles one contiguous ledger generation under exclusive startup
access. It checks the head chain and supplied generation before materialization,
checks/installs immutable objects through `ReplayTarget`, and publishes the final
head last. It can resume from any head in that chain and rechecks objects even when
the final head is already installed. The API oracle supplies a **test-only** target;
there is no production materializer yet. Multi-ledger dispatch is not implemented.

## Failure model and limits

The declared contract is that a successful sync preserves all previous bytes and
the length; later appends cannot destroy them. Crash images lose, tear, or reorder
unsynced writes. Bad headers, lengths, order, checksums, and incomplete tails fail
explicitly. **There is no automatic tail truncation.** A malformed tail alone does
not prove that its transactions were unacknowledged.

Tests also deliberately violate the storage contract: a write damages an earlier
acknowledged frame across a shared boundary, or required content disappears. They
verify corruption detection/the external oracle, not automatic repair. Losing a
whole valid suffix cannot be detected by the surviving checksums alone; the external
acknowledgment oracle detects it. Recovery of a deleted/truncated only durable copy
needs independent persistence. SHA-256 here is integrity checking, not authentication.

Before transaction integration, provide durable root ownership/format fencing,
startup recovery before all access, CAS reservation before journal acceptance,
complete required-content validation, and production atomic materialization/state
installation before ACK. Coordinate or reject every mutation path, including import,
push, index heads, configuration/lifecycle and GC. Explicitly reject unsupported
encryption, mixed backends, clusters, and oversized transactions before effects.

This slice has no checkpoint, truncation, rotation, online GC, submission idempotency,
group commit, transaction overlap, or remote preparation. At capacity it stops;
it does not remove recovery bytes. A durable checkpoint/retirement protocol and
storage qualification are required before an unrestricted WAL mode.

## Verification

Core tests use the production append/recovery code with a deterministic I/O model:
short/interrupted writes, write-zero/disk-full, both failed-flush outcomes, every
torn-append byte cut, reordered data, bytewise corruption, shared-boundary damage,
foreign-record splicing, initialization failure, bounds, and file ownership.

The API test wraps actual `FileIo` calls, recording writes and successful sync
barriers. Exact real Fluree commits and raw transactions are recovered into empty
database directories from those journal images. Receipts and expected query answers
live outside the fault image. It cuts each replay operation, resumes twice, and
checks exact commit identities, parent chains, raw bytes, and query results. Negative
tests remove required raw bytes, truncate a valid suffix, break the head chain,
change generation, and supply an unrelated head.

These are deterministic I/O-contract tests and local filesystem tests, not VM/block
fault injection or physical power-loss qualification. They establish no performance
improvement. The next performance comparison must keep the repaired durable baseline,
then measure an actually integrated WAL with the same Cypher workload.

```
cargo test -p fluree-db-core --features experimental-local-journal local_journal --lib
cargo test -p fluree-db-api --features experimental-local-journal --test grp_ledger it_file_recovery_oracle
```
