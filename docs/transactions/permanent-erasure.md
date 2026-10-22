# Permanent erasure

Permanent erasure removes selected occurrences from the commits that hold them. Each affected commit is
replaced by a redacted copy, and a zero-knowledge proof shows the copy is the original commit less
exactly those occurrences. Every other operation, the commit's place in history and every commit ID
stay as they were.

[Targeted removal](history-retention.md#targeted-removal) hides occurrences from queries and keeps the
commit log intact for audit. Erasure goes further: once an erasure is published, nothing reads the
original commit again.

**This release.** It runs the whole workflow through published, verified replacements, but the original
commit files stay on disk. Deleting them, and the other copies listed under [Limits](#limits), is the
next release. Until then, erasure stops every read path from serving the originals, but does not destroy
them.

## Scope

- **Deployments:** a single node with file storage (`--storage-path`). On memory or S3 storage, in a
  Raft cluster, on a query peer, or with no prover configured, a request is refused with `501`.
- **Commits:** v4 commits with SHA2-256 CIDs, signed or unsigned. A request touching anything else is
  refused with `422`.
- **What can be erased:** a commit's operations (assertions and retractions). Transaction metadata and
  the stored transaction documents cannot be redacted (see [Limits](#limits)).

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
  request. Creating a branch waits while a request is being accepted or published.
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

Replacements, receipts and the published mappings are kept under `@retention/erasure/` in storage. That
is outside every ledger's folder, so ledger drops and backup restores do not touch them.

## Status

`GET /v1/fluree/retention/<ledger>` lists the branch's erasure jobs under `registration.erasures`.

| Field | Meaning |
|---|---|
| `stage` | `proving`, `indexing` (proved, waiting for an index of its commits), `published`, or `failed` |
| `commits[].proved` | Per commit, once this request proved it: the slots it removed, the replacement it redacted further (`base`, absent for the original), the replacement, the receipt hash, input and replacement sizes, cycles, segments, proving time. Absent when an earlier request's erasure already removes the commit's requested occurrences. |
| `retries`, `retryAtMs`, `lastError` | Failed steps and when the next try runs |
| `acceptedMs`, `publishedMs` | When the request was accepted and published |

## Server configuration

| Flag | Default | Meaning |
|---|---|---|
| `--erasure-prover <path>` | none | The `fluree-prover` binary. Without it, requests are refused. |
| `--erasure-scratch-dir <dir>` | `<temp>/fluree-erasure` | The prover's temporary files. They hold the plaintext of commits being erased, and each step's directory is removed when the step ends. |
| `--erasure-prover-threads <n>` | every core | Proving threads. Fewer threads leave transactions their cores. |
| `--erasure-prover-timeout-secs <n>` | 14400 | A step still running after this long is killed, with its process group, and retried. |

The prover runs at low priority (nice 10). Proving on a host that also serves transactions slows them
down unless it is limited, so give it its own cores or its own host. See
[`fluree-prover/README.md`](https://github.com/fluree/db/blob/main/fluree-prover/README.md) to build it.
Fluree accepts proofs only from the guest images its build pins, so deploy the prover binary
recorded for the same release: a prover rebuilt elsewhere may prove under another image.

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

- **Originals are retained in this release.** The original commit files remain on disk, and no read
  path serves them.
- **Transaction documents are not redacted.** The stored transaction document of an affected commit
  still holds the original request, and audit retrieval still serves it. The next release deletes such
  documents whole.
- **Index files built before publishing** still hold the erased values' history. The removal journal
  refuses those reads, and a reindex rebuilds the index without them.
- **Transfers are refused.** Push, pull, clone, `.flpack` archives and pack exports refuse a ledger's
  erased commits by name.
- **Not covered:** copies outside this node's storage (backups, other clones, exports already made),
  and values in the original CID itself (see [Signatures](#signatures)). A proof does not show that
  every physical copy is gone.
