# Experimental journal foundation

Enabled only by `experimental-local-journal` on Unix. **This feature does not turn
on WAL for ordinary Fluree transactions.** Their acknowledgments remain unchanged.
The experimental owner can now accept exact transitions through a trusted embedding
interface; the limited embedded JSON-LD/Cypher adapter calls it, while ordinary
CLI/server persistence remains unchanged.
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
byte reads and the `accept_with` embedding seam. It exposes no general
local-path/mmap, nameservice, or indexer adapter. Recovery stages immutable objects and renames them into place, rejects
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

## Durable bootstrap checkpoint (core embedding only)

`LocalRoot::bootstrap` creates a v2 root from a pinned `CheckpointSpec` in an existing
empty directory. It does not convert ordinary files in place. A spec lists the exact
baseline head and strictly sorted object keys, byte lengths and SHA-256 hashes.
The core treats these as opaque bytes: the trusted callback must verify Fluree CIDs,
complete dependencies, supported configuration and an unchanged pinned source head.
A hash inventory alone is not a database-semantic proof.

Objects are streamed with a 64 KiB buffer into `.fluree-wal/checkpoint/objects`.
Each completed object is hash/length checked and file-synced; all created directories
are then synced from children to parents. The checkpoint descriptor binds the root
identity, ledger, generation, exact baseline head and inventory. Its bytes are
file-synced, atomically published and directory-synced before journal initialization.
The journal identity derives from the root identity and exact descriptor digest.
Only after validation and journal initialization does the ready v2 root manifest
publish. Errors retain the fenced partial generation; there is no in-place resume,
automatic deletion, hard-link sharing, or journal retirement.

The inventory is bounded to 100,000 objects and 16 MiB encoded descriptor bytes;
keys to 1,024 bytes, baseline head to 64 KiB, each object to 1 GiB, and total content
to 1 TiB. Startup rechecks every inventory object with bounded streaming memory.
Explicit reads allocate at most one bounded object and recheck its hash/length.
Baseline bytes never enter the coordinator's journal-object map. The journal's
existing 16 MiB encoded-record and 64 MiB total limits still apply to later writes.

The coordinator starts at the checkpoint head, so a synthetic genesis or stale
first transition fails CAS. Checkpoint immutable-key and file/directory collisions
are rejected. Missing head materialization is reconstructed from the verified
baseline before tail replay. A conflicting existing head fails closed. Startup also
syncs the control directory before returning, covering an earlier initializer that
failed after ready-manifest rename but before its directory sync.

`recover_with_checkpoint` revalidates the baseline and journal, then supplies a
read-only checkpoint handle and tail records to the trusted state-installation hook.
A retained checkpoint handle keeps the exclusive root lease alive. It does not grant
coordinator health or permit cached queries to bypass their operation gate. Legacy
`recover_with` hooks and validators without explicit checkpoint support reject v2
roots. The JSON-LD/Cypher adapter now explicitly validates and loads supported indexed
checkpoints. Opaque core checkpoints without that semantic proof still fail closed.
Core tests use opaque synthetic baselines; API tests use actual native imports.

Bootstrap tests interrupt every instrumented completed file/directory/publication
operation and reopen twice, requiring either the exact baseline or a fenced error.
They also cover missing/corrupt/symlinked dependencies, source-byte mismatch,
semantic-hook rejection, journal/manifest binding, immutable collisions, retained
ownership, baseline plus externally recorded tail receipts after erased
materialization, and unresolved installation followed by damaged-baseline recovery.
These are filesystem interruption tests, not simulated device-cache loss or physical
power-loss qualification. A generated object larger than the journal cap exercises
streaming bootstrap without putting the baseline in a journal frame.

## Serialized acceptance and reconciliation

`accept_with` holds the shared owner's mutex through expected-head/generation checks,
semantic validation, filesystem preflight, journal append/sync, immutable materialization,
head publication, and the state-installation hook. Only then does it advance completed
state and return a receipt. Readers take the same mutex, so no owner reader crosses
an incomplete installation. Same-base concurrent candidates have one CAS winner;
the loser writes no journal record or object.

`AcceptanceView` exposes candidate bytes, objects in the retained accepted
journal, and explicitly verified checkpoint prerequisites through `read_content`. A merely readable file is not a durable prerequisite. Immutable key changes,
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

The API's feature-gated `local_journal_ledger::JournalLedger` now connects real
JSON-LD insert/upsert/WHERE and Cypher staging to this coordinator. It journals exact raw and
commit bytes, materializes, and installs the real LedgerState before returning a
receipt. A private query/cache gate also checks coordinator health for memory-only
reads. Recovery revalidates database semantics and rebuilds LedgerState directly
from journal-covered bytes before clearing the root's unavailable flag. Cancelling
an awaiting write cannot release its gate while the blocking flush/install runs.
Independent adapter opens refresh their cached state when the accepted head changes.

This is a trusted embedded adapter for an unsigned default-graph ledger, initialized
empty or bootstrapped from a quiescent private ordinary file source with a fixed index. It exposes no general storage, nameservice, Fluree or cached-view handles.
Named/config graph writes are rejected; online index publication, lifecycle, policy
contexts, encryption and cluster entrypoints are not exposed. Raw transaction JSON
is always recorded; the adapter's 10 MB novelty backpressure limit also applies.
Head identity and immutable content reads support response-loss reconciliation;
recovery does not automatically resubmit requests or provide submission deduplication.
Ordinary constructors/CLI/server transaction paths still do not enable WAL.

`transact_cypher` accepts one statement plus parameters, including the existing
single-write, conditional MERGE and sequential multi-clause staging paths. All
probes run under the adapter gate. It rejects semicolon-separated scripts before
effects. RETURN rows are prepared in a private transient view before append and
exposed only after successful state installation (or a healthy no-op). Raw provenance
is `{"cypher": original_text, "params": map}` with absent parameters recorded as `{}`.
`query_cypher` formats its response under the same health-gated query lock. Existing
Cypher syntax limitations and the `FLUREE_CYPHER_ALLOW_FULL_SCAN` opt-in still apply.

Tests retain the eight exact write statements and hashes from the archived durable
Cypher suite. They compare against ordinary Fluree on a small unindexed fixture,
then check external receipts, parameter bytes, entity IDs and rows after erasing all
materialized files. This is workload compatibility/recovery coverage, not the full
medium Pokec benchmark or an indexed performance comparison.

The linear validator walks/hashes complete history on every acceptance and the payload codec
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

Before enabling ordinary transactions, extend the limited adapter's transaction and
mutation-path coverage, and finish ownership qualification.
Coordinate or reject import, push, index heads, configuration/lifecycle and GC.
Explicitly reject unsupported encryption, mixed backends, clusters, and oversized
transactions before effects.

The bootstrap checkpoint does not provide truncation, rotation, online GC, submission idempotency,
group commit, transaction overlap, or remote preparation. At capacity it stops;
it does not remove recovery bytes. A subsequent checkpoint/retirement protocol and
storage qualification are required before an unrestricted WAL mode.

## Indexed adapter bootstrap

`JournalLedger::bootstrap(destination, source, ledger, generation)` pins the exact
main and separate index nameservice bytes, applies the ordinary effective-index merge
rule, and preserves both source records in the checkpoint. It verifies full v4
commit/raw ancestry, the effective index root, all expanded index artifacts (including
transaction-metadata branches and annotation leaves), every retained previous index
root and each referenced garbage manifest. Missing historical dependencies fail;
there is no history pruning or GC. Garbage manifests are retained as metadata, not
instructions to copy obsolete objects that no retained root needs.

Native import publishes a static default-context blob. The adapter preserves its CID,
exact bytes and config watermark, allowing only string-valued absolute IRI mappings
and optional `@vocab`. It rejects broader/scoped/remote context forms, configuration
metadata and user named graphs. The reserved transaction-metadata index graph is
preserved; configuration-graph routing is rejected. Source heads are rechecked after
copying and before ready publication. This requires a quiescent private source and
is not a live migration protocol.

Recovery builds a semantic baseline proof bound to the exact checkpoint digest.
Live writes reuse a private process-local proof of the accepted prefix. It binds the
root-specific journal receipt/digest, generation, exact head and checkpoint digest.
Each candidate still checks its exact parent/time, dependency bytes/CIDs, default-graph
restriction and unchanged configuration/index fields. Candidate closure excludes
unrelated re-supplied ancestor objects; older raw dependencies remain hash-checked.
Proof extends only inside validated, successfully flushed state installation. The
cache gate and owner health prevent reuse after uncertain flush or installation
failure. Independent adapter caches compare the entire frontier and fully recover
before using changed state. The opaque core frontier alone is neither semantic proof
nor an acknowledgment. Full recovery views deliberately expose no reusable frontier.
Bootstrap/startup verify the full baseline and
currently decode full commit bodies, one object at a time. Core copying uses its
fixed buffer, but the API content-store interface buffers one source object; this is
not a constant-memory end-to-end import. A separate embedded medium diagnostic
qualifies this fixed-index path; it does not enable the original index-on benchmark.

The factored index attachment helper accepts the private read-only checkpoint/journal
store, restores dictionary watermarks and namespaces, populates novelty IDs and
attaches binary providers and annotation content access. Private temporary cache files
are disposable and hold no recovery authority. Store handles retain both root and
cache lifetimes. Both initial/recovery attachment and namespace reattachment use the
private engine's bounded shared leaflet/dictionary cache, with the ordinary memory-based
budget. Omitting this cache caused repeated dictionary file reads and minutes of
medium readback in the first diagnostic; a regression asserts the actual attachment
retains this cache across new-namespace writes and recovery. Static context metadata is installed in the private memory staging
engine so ordinary Cypher lowering/probes use the same IRI mapping; accepted data
still comes only from the supplied owned LedgerState. Queries use the context and
JSON-LD writes inherit it when they have no explicit context; raw provenance remains
the original request.

A namespace-introducing commit conservatively reattaches the binary store before ACK.
The pre-existing provider fallback could scan new namespace data but returned no rows
for a bound join in the indexed regression. Reattachment fixes that adapter path and
preserves unresolved-outcome handling on failure; its extra cost is unmeasured.

Tests compare the exact eight archived Cypher writes with ordinary Fluree using a
native-imported binary index and sealed annotation arenas (the CLI's follow-up reindex).
They remove the source and all target materialization, reopen twice, and compare exact
external acknowledgments, raw text/parameters, generated identities, reads and
relationship properties. Actual dictionary/leaf corruption, missing annotation leaves
and previous roots, source-head changes, unsupported metadata, new namespaces,
context inheritance and failed indexed installation are covered. The imported index
stays fixed; this is not the original medium index-on benchmark or a latency result.

## Private durable index builds

`LocalRoot::pin_index_build` captures a healthy accepted frontier while the embedding
holds its LedgerState gate. The opaque `IndexBuildPin` retains root ownership without
holding the acceptance mutex. `prepare` copies a strictly sorted inventory to a fresh
`.fluree-wal/index-builds/<random>/checkpoint` directory using the checkpoint format's
64 KiB stream buffer, hash/length checks and file/directory syncs. The trusted
validator must check index CIDs, complete dependencies and the built-through input.

The build descriptor binds the root identity, ledger/generation, exact input head and
journal prefix digest. Its optional `index_build` field distinguishes staged builds
from bootstrap checkpoints; ordinary descriptors omit it. A staged descriptor cannot
be opened as a bootstrap checkpoint. The same inventory/object/total limits apply;
index bytes stay outside the journal's 16 MiB record and 64 MiB total limits.

`PreparedIndex` exposes listed bytes and physical revalidation for the same owner,
with per-read checks and no raw-path capability. It retains its original input while
newer commits proceed. Neither preparation nor verification changes the accepted
head, journal or database read view. This is not a publication receipt or reusable
semantic proof. Publication validates under the acceptance gate as described below;
the database embedding must preserve newer commits.

Failures leave private files for inspection and permit a fresh-directory retry.
Restart ignores unreferenced staged files, including complete or corrupt abandoned
builds. Only an accepted journal publication authorizes reopening a build. There is
no arbitrary reopen-by-path, orphan promotion, cleanup or retirement API. Losing an
unpublished handle requires rebuilding. There is no cumulative disk-growth bound across repeated abandoned
builds, so this seam remains for controlled experiments. Streaming bounds the copy;
an embedding's semantic validation or ContentStore may still buffer one object.

Tests cover every instrumented staging cut with repeated restart/retry, newer commits
during copy, concurrent builders, an object larger than the journal limit, source and
manifest corruption, symlinked paths, foreign owners, purpose/prefix binding, and
unresolved-writer recovery. The indexed API fixture also copies an actual validated
native index while a real newer transaction commits. No Fluree indexer trigger or
active Fluree index pointer is connected by this slice.

## Journal-authorized index publication (core only)

`LocalRoot::publish_index` reopens and physically verifies a `PreparedIndex` for the
same owner, then merges against the latest accepted head under the coordinator gate.
The trusted embedding must hold its database-state gate, preserve commit/configuration
fields and newer novelty, and explicitly implement `validate_index_publication` to
check index CIDs, dependency closure, built-through ancestry and monotonic progress.
The default validator rejects publications. Callbacks must not reenter the root.

The distinct optional `index_publication` record field contains a private build ID,
manifest digest, exact input-prefix digest and input head. Publication has no inline
objects and uses normal latest-head CAS. The coordinator requires the input prefix
and head to occur in this root's accepted history (including a checkpoint's empty
journal prefix), rejects duplicate manifests and immutable/path collisions, flushes
the journal, materializes the head and calls installation before making the new
index readable through the owner or returning a receipt. An uncertain append/flush
or failed installation leaves the owner unavailable until explicit recovery.

Startup opens only builds named by journal records and checks all their physical
prerequisites and prefix lineage before replay. `recover_with_indexes` provides the
baseline, records and verified index stores to the embedding's validation/installation
hook. `validate_recovered_with_indexes` validates in journal order: preceding indexes
and the current publication's candidate are visible, future builds are not. Legacy
recovery hooks reject journals containing publications. Ordinary records still omit
the new field; older strict decoders reject publication records instead of silently
ignoring them. This experimental format has no downgrade/migration guarantee.

Publication tests use synthetic opaque heads, including an older build published
after newer commits, exact receipts across repeated root reopen, every publication
frame cut, both outcomes of failed flush, failed/panicking installation, missing or
corrupt prerequisites, forged prefixes, symlinks and stale/conflicting publications.
These test core ordering and durability mechanics, not Fluree index semantics.
The API adapter still rejects publication: actual index construction, semantic
validation, state attachment and server/index-trigger integration remain next work.

This correctness prototype physically rehashes the complete build under the owner
gate on publication, retains earlier accepted indexes, and keeps an accepted-prefix
head map bounded by the journal's existing cap. Publication latency and sustained
memory/disk costs have not been measured. No artifact retirement, orphan cleanup,
transaction overlap, group flush or cluster protocol is introduced.

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
cargo test -p fluree-db-api --features experimental-local-journal --lib local_journal_ledger
```

Adapter tests execute real writes/queries through the owned root, discard all
materialized files and recover exact externally acknowledged chains/raw bytes,
preserve generated subjects across replay, and exercise no-ops, cloned concurrent
writers, independent caches, failed installation, cancellation after durable flush,
unsupported semantics, and process death/takeover. SIGKILL is a process-interruption
test; the kernel page cache survives it. Neither that test nor erased materialized
files establishes device power-loss behavior.
