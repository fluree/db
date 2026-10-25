# Permanent erasure

Permanent erasure removes selected occurrences from the commits that hold them. Each affected commit is
replaced by a redacted copy, and a zero-knowledge proof shows the copy is the original commit less
exactly those occurrences. Every other operation, the commit's place in history and every commit ID
stay as they were.

[Targeted removal](history-retention.md#targeted-removal) hides occurrences from queries and keeps the
commit log intact for audit. Erasure goes further: once an erasure is published, nothing reads the
original commit again.

Once an erasure is published, a [purge](#purge) deletes the original commit files and every other copy
of what the request removed that this node's storage holds: transaction documents, index files,
derived search indexes and scratch files.

## Scope

- **Deployments:** a single node with file storage (`--storage-path`). On memory or S3 storage, in a
  Raft cluster, on a query peer, or with no prover configured, a request is refused with `501`.
- **Commits:** v4 commits with SHA2-256 CIDs, signed or unsigned. A request touching anything else is
  refused with `422`.
- **What can be erased:** a commit's operations (assertions and retractions). Transaction metadata and
  the stored transaction documents cannot be redacted (see [Limits](#limits)).
- **What is not erased:** IRIs. They are identifiers, so keep personal data out of them and use opaque
  ids (`ex:user-8f3a…`, not `mailto:alice@example.org`). See [Limits](#limits).

## Requesting an erasure

```http
POST /v1/fluree/erase/<ledger>
Authorization: Bearer <admin token>

{
  "matching": [
    {"subject": "https://example.org/alice", "predicate": "https://example.org/email",
     "object": "alice@example.org"}
  ]
}
```

A request selects occurrences the way a [targeted removal](history-retention.md#targeted-removal)
does: `matching` selects every occurrence of a fact, and `occurrences` names occurrences by commit and
slot. The admin token's identity is recorded as the requester. From Rust, use
`Fluree::erase_occurrences(ledger_id, request, requester)`.

Accepting a request does the following, in order:

1. **Removal on every branch that holds an occurrence.** Current values are retracted, and the removal
   journal refuses the history at once. A named occurrence also takes the one that bounds its span: an
   assertion takes the retraction that ended it, and a retraction takes the assertion it ended. Both
   carry the value. The removal's own retraction commits contain the value too, so they are erased as
   well.
2. **An authorization commit.** A control commit records the request ID, the requester and a SHA-256 of
   the manifest in its txn-meta (`f:erasureRequest`, `f:erasureRequester`, `f:erasureManifest`). It
   records no value, and anchors the authorization in the ledger's history.
3. **A job.** It is kept in the branch's retention registration and returned as a receipt:

```json
{
  "requestId": "erasure-3f1c…",
  "manifest": [{"commit": "bagaybqab…", "slot": 4}, {"commit": "bagaybqab…", "slot": 0}],
  "commits": 2,
  "authorizedT": 43,
  "stage": "proving",
  "removals": [{"ledgerId": "people:main", "removal": {"requestId": "removal-…", "…": "…"}}]
}
```

Repeating a request returns the same job.

## How it completes

The retention worker advances jobs in the background, one proving step at a time:

- **Proving.** For each affected commit, the prover (`fluree-prover`, a separate binary) runs the
  redaction in the RISC Zero zkVM. It returns the replacement and a succinct receipt.
- **Verification.** Fluree verifies each receipt in-process, before storing anything, against:
  - the guest images it pins, and only succinct receipts;
  - the original commit's CID;
  - the authorized slots;
  - the ledger its signatures were made for;
  - the replacement's CID.
- **Retries.** A prover that is unavailable, or a proof that does not verify, is retried with backoff.
  A commit the prover refuses fails the job, and nothing of that job is published.
- **Publishing.** Once every affected commit is proved and every branch that holds them has indexed
  them, the erasure is published. Publishing waits for the index for a reason: an erased commit is
  then never replayed as unindexed data, so an indexed assertion cannot lose the retraction that ended
  it. The branches checked are those that exist when it publishes, including branches made after the
  request. Creating a branch waits while a request is being accepted or published. Nothing is
  published while the ledger is dropped, since its branches and their indexes can be restored;
  restoring the ledger restores its requests, and pending ones resume.
- **Overlapping requests.** A request that shares commits with an earlier one still in progress waits
  for it, and proves only what the earlier one's published erasure leaves. A request is complete once
  the published erasure of each affected commit removes every occurrence it asked for. If the earlier
  request fails, the later one proves those occurrences itself.

Once published, every read of an affected commit reads its replacement. That includes ledger loads,
replays, reindexing, time travel, commit display and raw storage retrieval, across restarts. A
replacement keeps the original's CID as its identity, its parents, its `t`, its envelope and its
surviving operations under their original slot numbers. Its proofs are verified again once per process
before it is first read, and if they do not verify, the read fails rather than return the original.

Another Fluree instance reading the same storage, such as a query peer, reads the replacements within a
second of publication. A job reports `published` only after that second has passed.

A later request for the same commit adds a step: the prover redacts the current replacement, and the
proofs chain back to the original commit.

Replacements, receipts, the published mappings and each published request's authorization (its
control commit and the occurrences its manifest named, which [archives](#archives) carry) are kept
under `@retention/erasure/` in storage. That is outside every ledger's folder, so ledger drops and
backup restores do not touch them.

## Purge

After publication, the retention worker purges the erasure in the background, one pass at a time. A pass
resumes where an interrupted one stopped.

1. **Indexes.** Every branch that holds an erased commit has its index rewritten without the erased
   events. Usually only the leaves that hold them are rewritten. The branch is rebuilt from the
   replacements instead when an erased value lives outside the leaves (a big number, vector, geometry,
   full-text value or triple term), or is a string, JSON value or duration in a ledger with triple
   terms, which may spell it too. Full-text (BM25) and vector indexes over a holding branch that read
   an erased property (each request records the properties it erases when it is accepted) are rebuilt
   from scratch; one that reads none of them never held the value and keeps its index. Every such
   index loses the files its current index does not use, which an earlier index of the same name may
   have left, and one dropped before the purge loses all of its files. A soft-dropped ledger that
   holds an erased commit has its indexes deleted, and restoring it rebuilds them before it serves.
2. **Grace.** The older indexes stay for 15 minutes (the index collector's minimum age), so queries
   reading them can finish. They are then released.
3. **Scan.** Every holding branch's history, index, dictionaries and TTL schedules are checked for the
   erased values. A value that a surviving occurrence still uses stays.
4. **Deletion.** TTL schedules holding an erased fact are rewritten. Next, the affected commits'
   transaction documents and superseded replacements are deleted whole. The scan then runs again, and
   last, the original commit files are deleted wherever they are stored. Each deletion is checked.
5. **Durability.** The write-ahead log is checkpointed and every directory the pass deleted from is
   flushed, so no deletion is undone by a power loss. Staging files left by crashed writes are removed,
   and so are scratch files left by crashed index rebuilds, imports and proving steps.
6. **Caches.** This process's caches are cleared.

A pass that cannot go on records why in the job and tries again an hour later. The reasons are:

- a full-text or vector index over a dropped ledger that reads an erased property, which keeps its own
  copies (drop it first: the purge then deletes its files);
- an instance that does not hold its storage's lock;
- another process's write-ahead log under the storage;
- a scan that finds something it cannot account for.

A purged job lists, under `purge.unverified`, what this node cannot check. That is the memory of other
processes reading the same storage, such as query peers. Publication and the purge each announce a
refresh of the ledgers they change (`ns-refresh` on [`/events`](../operations/query-peers.md)): a query
peer following the server evicts what it cached of them, and one that missed the event while
disconnected evicts everything it cached when it reconnects. Restart any other process, and peers of
earlier releases.

## Archives

A `.flpack` archive of a ledger with erased commits (`fluree export --format ledger`,
`Fluree::archive_ledger`) carries each erased commit as its erasure, never as its original:

- its chain: every step's request, slots, replacement and receipt hash;
- its latest replacement and every step's receipt;
- what authorized each step's request: the request's control commit and the occurrences its manifest
  named, with the retractions joined at publication.

An erased commit's transaction document is not archived, and neither is the ledger's index: an index
may still hold an erased value. The restored ledger builds its own from the replacements.

A restore (`fluree create --from`, `POST /import`, `Fluree::restore_ledger`) checks every erasure
before anything reads the restored history. Each chain's receipts must verify against the pinned guest
images, bound to the original commit and its slots, and the control commit must record the request and
the hash of its manifest. Each step's slots must be among the occurrences that manifest names, or the
retractions joined to it. The commit must be in the archive's history. The restored ledger then reads
each erased commit as its replacement and refuses the history the archive's removal journal refuses. No
prover is needed to restore.

A proof shows that a commit was redacted correctly, and a control commit shows that a request was made.
Neither shows who was allowed to make it: control commits are unsigned. A restore trusts the archive's
erasures as it trusts the archive's data.

A ledger on this storage may already hold the original of a commit the archive erases: a copy restored
earlier, or a branch of one. Before the restored ledger becomes visible, the restore makes each such
branch read as the erasure does:

- the erased occurrences are recorded in its removal journal, against its own history, so its history
  over them is refused;
- they leave its index without a rebuild: leaf by leaf when they are only history there, and a value
  still current there as a retraction would remove it, its row and the index's statistics included,
  with no history left of it. A value outside the leaves (a big number, vector, geometry, full-text
  value or triple term) is rebuilt from the replacements instead;
- its full-text and vector indexes that read an erased property are rebuilt;
- nothing is committed to it: a retraction would hold the value, and only a prover could erase it.

Each such branch is write-locked from the checks until it no longer holds an erased value as current,
so no transaction can retract the value in a commit only a prover could erase; its queries wait
meanwhile. `RestoreResult::reconciled` lists those branches. Each holding ledger records a published
job per request, and its [purge](#purge) deletes the originals and their transaction documents, with
no prover. A soft-dropped ledger holding an original is left to the restored ledger's purge, which
releases its indexes, and to its own restore, which rebuilds them.

The restore records the activation before the first ledger here changes. One stopped after that, by
a crash or a failed step, completes when the retention worker next runs
(`Fluree::resume_received_erasures` from Rust), even if the restored ledger itself was discarded.
Without the ledger cache (`FlureeBuilder::without_ledger_caching`, as the CLI runs), nothing locks
out a transaction: one committed on a holder meanwhile fails the activation, which then resumes the
same way.

The archive's manifest is checked in full before anything acts on it. A restore is refused, and
writes nothing to the ledgers here, when:

- the manifest names what the archive does not hold, or an archive with erased commits names an
  index;
- the archive's removal journal does not record every erased occurrence: the restored ledger would
  serve its history as complete;
- an erasure fails a check;
- an erasure differs from the one this storage holds for the same commit (a chain that is the same, or
  one this storage has already extended, is accepted);
- a ledger here retracts an erased value in a commit of its own, which the archive does not erase
  (`409`; erase it there with a request);
- a ledger here has a full-text or vector index that reads an erased property and that this instance
  cannot rebuild (`409`; drop it first);
- the instance cannot hold erased history: memory or S3 storage, or a cluster. Versions of Fluree
  without erased history in archives refuse such an archive as malformed.

## Clones and pulls

`fluree clone` and `fluree pull` carry erased history through the pack protocol, to a CLI that takes
it from a remote designated an erasure authority (`fluree remote erasure-authority <name> true`).
The pack carries each erased commit as an archive does, and the source's removal journal; it carries
no index when the requested history holds an erased commit, even one the receiver already has.

A pull's request lists the erasure chains the receiver holds. The source sends the chain of every
other erased commit of the history, those the receiver already has included, before the new commits.
The receiver takes them as a restore does ([Archives](#archives)): it checks them, makes every ledger
holding an original read as they do (the pulled ledger among them, with its own journal, index and
search indexes, and nothing committed), then publishes them before the head moves. A pack request
that does not ask for erasures is refused once the history holds one, so an older client never pulls
past an erasure of history it holds. A pull with no new commits still asks for a pack of none, so an
erasure published after the receiver has every commit reaches it too.

A transaction staged on a holder before an activation, and committed after it, is refused (`409`)
when it retracts a value the activation took out of the current state: its commit would hold the
value again. Commits wait while an activation checks and changes a holder. A request's erasures may
arrive a part at a time, a fork's before its source's: the job the first part made takes the rest,
and its purge runs again. With only soft-dropped ledgers holding the originals, the receiving
ledger's purge deletes them; if a restore fails after publishing the erasures, that purge stays owed
until the same ledger is restored again, or until no soft-dropped ledger holds an original.

A clone or pull that stops partway completes at the next clone or pull from the CLI, or when a
server on this storage next runs its retention worker. Without a ledger cache, as the CLI runs, a
transaction committed on a holder during the activation fails it, and it resumes the same way.

An origin is no named remote: `fluree clone --origin` and a pull from a ledger's origins take erased
history only with `--erasure-authority`, which designates the origin for that transfer.

## Pushes

`fluree push` and `fluree publish` carry erased history to a server that advertises `push.erasures`
(a single server on file storage), from a principal with the authority erasing needs there (the
admin token `POST /erase` takes, when the server requires one). Each erased commit among those pushed
travels as its erasure: empty bytes under its CID, with its chain, replacement, receipts and
authorizations; its original and transaction document stay behind. The push also lists the chain
digest of every erased commit of the history the server already has. A server that holds the original
of one but not its erasure answers `409`, naming the commits whose erasures it needs; one that holds
another erasure of a commit refuses the push. The CLI pushes the erasures named in pushes of no
commits, each within the server's body limit and holding every chain of a request together (the
server refuses part of a request, which would leave a commit holding what it erased), then the push
again. The server takes each as it arrives, so a push stopped between them resumes asking only for
the rest. When the server already has every commit, `fluree push` sends the erasures and journal in a
push of no commits, so an erasure published after its request was pushed still arrives.

The server takes the erasures as a pull does, before it validates the pushed commits: it checks them,
makes every ledger it holds with an original read as they do (the pushed ledger among them), and
publishes them. An erased commit is then validated as its replacement, whose surviving operations
are staged like any pushed commit's, and stored as nothing. The push carries the ledger's removal
journal too; see [History retention](history-retention.md). A cluster refuses a push of erased
commits (`501`).

## Object requests

`GET /storage/objects/:cid`, which an origin-based clone walks when no pack is served, answers an
erased commit with its erasure: the ledger's journal and the commit's chain, as pack frames, under
`X-Fluree-Content-Kind: erasure`. It never answers with the replacement, which the CID does not
match, nor with `304`, since a chain can gain steps. The transaction document of an erased commit is
not served (`404`). A clone takes the erasures, with `--erasure-authority`, as it takes a pack's. It
asks for an erased commit even when its storage already reads it for another ledger: taking the
erasure brings the journal, and makes the clone a holder that later erasures reach.

## Status

`GET /v1/fluree/retention/<ledger>` lists the branch's erasure jobs under `registration.erasures`.

| Field | Meaning |
|---|---|
| `stage` | `proving`, `indexing` (proved, waiting for an index of its commits), `published`, `purging`, `purged`, or `failed` |
| `commits[].proved` | Per commit, once this request proved it: the slots it removed, the replacement it redacted further (`base`, absent for the original), the replacement, the receipt hash, input and replacement sizes, cycles, segments, proving time. Absent when an earlier request's erasure already removes the commit's requested occurrences. |
| `retries`, `retryAtMs`, `lastError` | Failed steps and when the next try runs |
| `acceptedMs`, `publishedMs` | When the request was accepted and published |
| `purge` | The purge's progress: `rebuiltMs`, `scannedMs`, `purgedMs`, files `deleted`, values left `inUse` by a surviving occurrence, `blocked` (why it cannot go on), `unverified` (what to restart) |

## Server configuration

| Flag | Default | Meaning |
|---|---|---|
| `--erasure-prover <path>` | none | The `fluree-prover` binary. Without it, requests are refused. |
| `--erasure-scratch-dir <dir>` | `<temp>/fluree-erasure` | The prover's temporary files. They hold the plaintext of commits being erased. Each step's directory is removed when the step ends, and a purge removes any that a crashed step left. |
| `--erasure-prover-threads <n>` | every core | Proving threads. Fewer threads leave transactions their cores. |
| `--erasure-prover-timeout-secs <n>` | 14400 | A step still running after this long is killed, with its process group, and retried. |

The prover runs at low priority (nice 10). Proving on a host that also serves transactions slows them
down unless it is limited, so give it its own cores or its own host. See
[`fluree-prover/README.md`](https://github.com/fluree/db/blob/main/fluree-prover/README.md) to build it.
Fluree accepts proofs only from the guest images its build pins. The prover's guest builds in a
pinned container, so a prover built from the same release's sources proves under the image that
release pins.

## Signatures

By default, the proof verifies a signed commit's signatures. A request can set `"signatures":
"expose"` instead: the proof then publishes the signatures and the signed body's hash, and Fluree
verifies them itself. That makes a signed commit as fast to prove as an unsigned one. The cost is
privacy. With the body hash, anyone who can rebuild every other byte of the body can test guesses at
an erased value, which matters when the value has few possible values. The original CID allows the
same test on an unsigned commit, whichever mode is used.

## Cost

On a 16-vCPU host, the measured proving times are:

| Commit | Proving time |
|---|---|
| 1,000 operations, unsigned or `expose` | about 4 minutes |
| 1,000 operations, signed (signatures verified in the proof) | about 6 minutes |
| 50,000 operations | about 3 hours |

A request typically touches two or three commits per branch: the assertion, its retraction, and the
removal's own retraction. The ZK prototype report (`prototypes/zk-redaction/README.md`) has the full
measurements.

## Limits

- **Until the purge completes**, the original commit files and index files built before publishing
  remain on disk, and no read path serves them.
- **IRIs stay in the index dictionaries.** Erasing every fact about a subject, or every reference to a
  node, removes those facts from the commits and the index. The purge leaves the IRIs in the index's
  dictionaries, though nothing else holds them once no fact uses them. Namespace prefixes stay in the
  ledger's namespace table regardless. To remove the IRIs too, run a full reindex after the purge
  (`POST /v1/fluree/reindex`). The new index is built from the replacements and does not hold them.
  The index it replaces still does, until index garbage collection releases it. By default that waits
  until five newer index versions exist and the old index is 15 minutes old (`--gc-max-old-indexes`,
  `--gc-min-time-mins`). From Rust, a reindex whose `IndexerConfig` sets both to 0 releases it at
  once, at the cost of queries still reading it.
- **Transaction documents are not redacted.** The stored transaction document of an affected commit
  holds the original request, and audit retrieval serves it until the purge deletes it whole. No
  transfer sends it: archives, packs and pushes leave it out, and an object request answers `404`.
- **Some transfers refuse erased history.** The paginated commit export refuses a ledger whose
  history holds an erased commit, and so does an origin-based pull's object-by-object fallback, which
  walks only new commits. Archives, local or remote (`fluree export --remote --format ledger`), carry
  erased history (see [Archives](#archives)); so do clone and pull through the pack protocol (see
  [Clones and pulls](#clones-and-pulls)), an origin-based clone's object walk, and push (see
  [Pushes](#pushes)), each to a receiver that takes erasures from that source.
- **Not covered:** copies outside this node's storage (backups, other clones, exports already made), the
  memory of other processes that follow no refresh until they restart, and values in the original CID
  itself (see
  [Signatures](#signatures)). Neither the proof nor the purge shows that copies elsewhere are gone.
