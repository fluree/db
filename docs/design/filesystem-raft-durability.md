# Filesystem Raft durability and throughput qualification

The primary production target for the durability work is shared filesystem payload
storage with a separate durable local Raft log on each voter. Standalone mode remains
useful for development and benchmark comparisons. S3 retains its durability and
compatibility requirements, but is not the latency/throughput optimization target.
Transaction preparation overlap belongs in a separate PR.

## Preserve the two authorities

The standalone experimental journal is an acceptance authority for a single local
ledger. It cannot replace or bypass the replicated nameservice in a Raft deployment.
Compiling `raft` and `experimental-local-journal` together is supported; enabling
`journal_root` on a Raft server remains rejected. The standalone 21.503 → 5.441 ms
measurement is not a Raft performance result.

In Raft mode, `QueuedTransactor` durably writes the full queued envelope to shared
CAS before proposing `EnqueueCommand`. The branch-owning worker writes referenced
content and the exact proposed commit to shared CAS before proposing `ApplyHead`.
Raft orders queue consumption and head acceptance. Replies follow replicated apply.
A node-local payload journal cannot substitute for shared payload durability: loss
of the worker and its volume must not leave a committed CID without its bytes.

Index construction and index artifact writes remain asynchronous. An index pointer
advance is a separate replicated operation. Neither transaction acknowledgment nor
payload retention depends on waiting for new index construction or publication.
Recovery can use a suitable available index; losing a disposable newer index must
still leave a correct recovery path from accepted source.

## File-log hardening

Newly created log/snapshot directory hierarchies are synchronized through their
existing parent before use. These initialization barriers do not move directory
work into steady-state appends.

The filesystem Raft store now uses one contiguous unpurged extent for both extent
reporting and range reads. Restart removes files below the durable purge boundary
and uncommitted orphan files beyond the first gap, then synchronizes those removals
before exposing the store. This prevents a later leader's append from filling a gap
and accidentally resurrecting the old tail. Missing/inconsistent history covered by
the persisted committed watermark fails opening before cleanup. The store also
checks the entry index against its filename and the committed log's complete id.

A small `log_start` file records whether a log starts at zero (openraft) or one
(older generic embeddings). It is durable before the first append and cached for
steady-state use. Existing roots without that marker infer zero when entry zero is
present, otherwise one, and persist the inferred convention during opening. A
legacy root already missing entry zero cannot be distinguished from a one-based
root by filename inspection alone; this is not a blanket repair claim for arbitrary
preexisting disk corruption. Root opening/recovery requires exclusive operational
ownership, before any running node uses that storage directory.

These changes retain the per-entry file format and its existing durability barriers.
They do not introduce batched transaction acknowledgment, speculative staging, or a
new Raft log backend. The origin marker adds a one-time durable initialization write;
it is not rewritten per entry. Local regression tests model interrupted filesystem
images; orderly Raft shutdown/restart and in-process multi-node failover are separate
evidence, not power-loss qualification.

## Performance work in this stream

1. Establish a filesystem Raft baseline with shared payloads and disjoint per-voter
   logs. Record the actual shared filesystem/mount and its durability semantics.
   Run the fixed Cypher writes at 1, 2, 4, 8 and 16 clients, on one ledger and on
   independent ledgers. Preserve raw envelopes, exact ACK receipts and all read
   comparisons. Measure response drain, failures and retries, not just submissions.
2. Attribute time and I/O separately to shared envelope persistence, enqueue quorum,
   worker queue/staging, shared commit persistence, head quorum and reply delivery.
   Trace each voter's log/vote/committed/snapshot I/O in a separate diagnostic run.
   Keep async indexing enabled and report contention without moving it into ACK.
3. Replace per-entry Raft file creation with a bounded append-oriented storage
   backend if the baseline supports that choice. This backend must preserve Raft's
   vote, truncate, purge, snapshot and log-flushed contracts; reuse journal framing
   and fault-testing ideas, not the standalone head-publication authority. Seal
   immutable segments and durably switch a small manifest instead of copying all
   historical entries. Compaction may remove log prefixes only after a suitable
   durable Raft snapshot. Preserve ordinary per-append durability before trying any
   cross-request flush grouping.
4. Optimize shared immutable payload writes separately if those flushes dominate.
   A worker-local receipt alone is insufficient. Any pack/segment scheme needs
   safe multi-writer publication, exact CID lookup, and availability after worker
   loss before Raft can accept a reference to it.
5. Repeat the matched Raft benchmark and failure matrix before claiming a gain.
   Include minority-node loss, leader/worker changes, lost replies, complete node
   restart, lagging-voter catch-up, snapshots/purge, disk-full and interrupted
   writes/renames/flushes. Retain an ACK oracle outside the failed node. Arbitrary
   shared-storage loss is a different failure domain from losing one Raft voter.

Throughput must improve without weakening durable acknowledgment, losing exact
accepted identities/raw bytes, increasing unbounded replay/queue growth, or coupling
indexing to transaction completion. Numeric targets for the Raft optimization should
be pinned against its own measured baseline; the standalone benchmark is context.

## Optional phase diagnostics

Enable `RUST_LOG=info,fluree_raft_timing=debug` for a separate diagnostic run.
The target records durations for shared queued-envelope writes, enqueue proposals,
worker attempts, staging/persistence, shared commit blobs, head proposals and each
node's Raft append batch. Disabled diagnostics avoid clocks and identity formatting.
The phases nest and overlap across nodes; do not sum their medians into request
latency. `ok` means the instrumented Rust call returned `Ok`, which does not by
itself prove the state machine accepted a transaction. Early errors or cancellation
can omit inner phase records. Use HTTP receipts and recovery checks as the oracle.

The Raft log already serves as a write-ahead log for replicated decisions. A second
WAL around it is unnecessary. Its current per-entry file syncs and per-batch directory
sync remain the baseline; an append-oriented backend would be a separate measured
optimization. Durable shared payload storage is still required before proposing
references to those payloads.

## Low-traffic worker loss

The controlled three-node NFS qualification exposed a separate liveness defect:
with the default 1000-entry healthy-lag window, losing the owner of an idle or
lightly used branch did not make it worker-ineligible. The quorum retained the
queue and prior accepted data, but new requests timed out until that node returned.

Standard `RaftIntegration::bootstrap` now observes successful responses from the
existing per-group Raft HTTP transport, including idle heartbeats. The leader's
monitor combines those responses with replication lag: no responses for the
configured `unreachable_after` (15 seconds by default) can trigger a replicated
worker-eligibility change even at zero lag. New monitor instances receive a fresh
grace period. Promotion requires fresh responses throughout the recovery window
and acceptable replication lag. No extra health RPCs or log flushes are introduced
on the transaction path. Eligibility changes still require Raft consensus; a
minority cannot independently reassign work.

Custom integrations built using `RaftIntegration::new` must share a `PeerActivity`
observer between their HTTP network factory and `with_peer_activity` to obtain
this transport-based detection. Without that observer, the legacy lag-only
fallback remains. This does not diagnose an application worker that hangs while
its Raft transport continues answering normally.

## Submission identity under concurrency

The concurrency-16 NFS qualification exposed a separate receipt-correlation bug:
identical envelopes can share a CID, while a CID-keyed single-waiter map displaced
callers and could attach a receipt to the wrong idempotency key. Timestamps are
optional and must never be treated as submission identity.

Each new queued envelope now carries a random 256-bit `submission_nonce`. This
metadata is ignored by transaction execution and excluded from every operation's
canonical idempotency body hash. Legacy JSON envelopes without it still decode;
older readers ignore the additional struct field. This also gives orphan cleanup
exclusive ownership of the new attempt's blob instead of deleting another pending
submission with identical content. Raft command and snapshot formats are unchanged.

Waiter interests additionally include branch and idempotency identity. Duplicate
callers joining one queue entry receive the same outcome; distinct keys cannot
cross-wire receipts. Unkeyed identical submissions each retain a separate queue
entry. A short synchronous mutex makes binding and cancellation atomic without
holding a lock during I/O or awaiting transaction work. Followers still retain no
unsolicited outcomes. Qualification must compare every receipt to its replicated
idempotency result, not only check that it names an existing unique commit.

## Envelope cleanup and cancellation

Envelope ownership lasts across internal proposal retries. Once an enqueue may
have accepted a CID, a later rejection or leader change cannot authorize deleting
it. Queue/cache lifecycle handles known referenced envelopes. A fresh duplicate
that joins an existing request has its own nonce/CID; release those unused bytes
only when the submission finishes, since an internal retry may still enqueue them.
Canonical body encoding completes before the envelope upload.

Cancellation or a fatal Raft error can leave an unknown proposal outcome. Retain
those bytes rather than risk deleting a committed request. Cancellation removes
local waiter interest, not replicated work. This is deliberately conservative:
ambiguous unreferenced uploads and failed release calls can leave orphan objects;
there is no general bounded orphan collector in this PR. Such a collector needs
replicated reachability or safe ownership leases and is separate follow-up work.
