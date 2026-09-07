# Experimental journal foundation

Enabled only by `experimental-local-journal` on Unix. **This feature does not turn
on WAL for ordinary Fluree transactions.** Their acknowledgments remain unchanged.
The experimental owner can now accept exact transitions through a trusted embedding
interface; no CLI/server/Fluree transaction adapter calls it yet.
Ordinary Unix file opens now acquire a shared root lease and reject managed roots,
including when journal support is not compiled. The format is experimental and not yet a
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
the final head is already installed. The API oracle supplies a test-only target;
`LocalRoot` now also supplies an owned atomic file materializer. Multi-ledger
dispatch is not implemented.

## Root ownership and startup

`LocalRoot::initialize` accepts only an existing empty directory. It exclusively
locks the directory inode, syncs existing ancestors, creates/syncs the `.fluree-wal`
fence, initializes the journal, and durably publishes a manifest bound to its identity.
Managed materialized data lives under `.fluree-wal/data`, preserving the journal's
relative storage keys behind a reserved path. Ordinary storage/listing/sweeping
cannot enter that reserved namespace from a wider root. No populated-root conversion
or old-layout migration is implemented.

`LocalRoot::open` acquires the root lock, validates the manifest/journal generation,
validates the whole journal, and completes exact atomic materialization before
returning an owner. Canonical-path aliases share one ready `Arc` in-process;
another process is refused until the final owner drops or dies. The owner exposes
byte reads and the `accept_with` embedding seam; there is no Fluree transaction,
local-path/mmap, nameservice, or indexer adapter yet. Recovery stages immutable objects and renames them into place, rejects
conflicting bytes/symlink paths, and publishes the head last. Individual replay writes
omit fsync because the complete journal is retained; this is not a checkpoint.

`FileStorage::new` stays free of I/O. Its first access acquires and retains a shared
directory lease; this may create an absent empty ordinary root, including on a read.
The nearest existing ancestor is leased before creation to prevent initialization
races. FileStorage clones and its blocking I/O workers retain that lease. Ordinary
builders/connections, all FileNameService trait entrypoints, remote tracking operations,
range/local-path access, and startup sweeps check the fence. These checks are present
without the experimental feature. New directory locking requirements and startup
overhead need qualification on Linux and network filesystems before integration.

Roots/ancestors must not be renamed/replaced or symlinks retargeted while handles
live. Arbitrary filesystem access and old binaries that ignore the marker cannot be
fenced; do not run mixed versions. This is a cooperative local filesystem boundary,
not a security sandbox for hostile symlink routing through unrelated roots.
Interrupted initialization without a valid manifest fails closed and retains its
files for inspection. No transition could have been accepted before successful initialization.

## Serialized acceptance and reconciliation

`accept_with` holds the shared owner's mutex through expected-head/generation checks,
semantic validation, filesystem preflight, journal append/sync, immutable materialization,
head publication, and the state-installation hook. Only then does it advance completed
state and return a receipt. Readers take the same mutex, so no owner reader crosses
an incomplete installation. Same-base concurrent candidates have one CAS winner;
the loser writes no journal record or object.

`AcceptanceView` exposes only candidate bytes and objects in the retained accepted
journal. A merely readable file is not a durable prerequisite. Immutable key changes,
head/object collisions, reserved paths and file/directory collisions are rejected.
The bounded journal's object bytes are retained in memory for this prototype.

`AcceptanceValidator` and the installation hook are **trusted embedding interfaces**.
The core cannot interpret an opaque nameservice head or prove policy/query semantics;
callers must not supply arbitrary validators through a transaction request. The API's
`LinearCommitValidator` verifies a closed ns@v2 head shape, exact v4 commit CIDs,
linear transaction/parent continuity, and every referenced raw transaction CID.
It walks all ancestry to genesis and rejects unrelated candidate objects, indices,
configuration/lifecycle changes, branches/merges and commit/transaction signatures.
It does not validate policies or remote preparation; staging must do that.

Definite CAS/validation/capacity rejections remain distinct from
`AcceptanceUnresolved`. An I/O error after append begins is unresolved and blocks the
owner. If the flush succeeded but materialization/installation failed, that error
includes the durable journal receipt. A panic after append begins also leaves the
owner blocked. Installation hooks must not reenter the owner or send a response.

`recover_with` retains the root lock, reopens/verifies the bound journal, replays exact
accepted bytes, and calls a recovery-state installation hook before making the owner
available again. Failed recovery/installation leaves it blocked. Complete records
whose original response failed may recover; reconcile their identity before retrying.
This is not submission-level exactly-once delivery or automatic retry of CREATE.

No real LedgerState installer or transaction endpoint is connected yet. The linear
validator walks/hashes complete history on every acceptance and the payload codec
uses JSON byte arrays. Both are correctness prototypes with substantial overhead;
cache validated dependency closure and choose an efficient codec before performance
acceptance. No benchmark gain follows from these tests alone.

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

Before enabling ordinary transactions, finish the Fluree staging/state-installation
adapter, read/cache integration, mutation-path coverage, and ownership qualification.
Coordinate or reject import, push, index heads, configuration/lifecycle and GC.
Explicitly reject unsupported encryption, mixed backends, clusters, and oversized
transactions before effects.

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
