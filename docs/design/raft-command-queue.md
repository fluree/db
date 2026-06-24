# Raft command queue and replicated state machine

How `fluree-db-consensus`'s Raft mode replicates writes across a cluster. The operations-facing recipe (cluster bootstrap, day-2 admin, security boundaries) lives in [Raft clusters (replicated writes)](../operations/raft-clusters.md); this doc covers the design choices behind the implementation.

## Goals and constraints

The transactional path on a Fluree server is non-trivial: a write has to parse JSON-LD / SPARQL Update, evaluate policy, generate flakes, resolve conflicts, write a commit blob to content-addressed storage (CAS), and update the branch head. Replicating this naively across a Raft cluster — by replaying the full work on every node — runs into three problems:

1. **The work isn't deterministic in the inputs the log carries.** Policy evaluation, conflict resolution, indexing side-effects, and CAS writes all depend on state the log doesn't replicate (cache contents, wall-clock ordering, storage backend addresses).
2. **The log would bloat.** Commit envelopes can be megabytes; replicating them through the log instead of CAS doubles the network and storage cost of every write for no benefit — every node already has CAS access.
3. **Stage time is unbounded.** Some transactions take seconds (large updates, complex policy, indexing). Blocking the openraft commit path on that work would extend leader heartbeat latency and degrade liveness.

The design splits the work: the Raft log replicates **decisions** (branch head moved to CID `x` at queue position `n`), and the CAS holds the **bytes** (envelopes, commit blobs, index artifacts). Only the leader stages, but every node observes the result.

## Component map

The Raft consensus crate (`fluree-db-consensus/src/raft/`) is structured as a set of cooperating components:

| Component                  | Lives where                                              | Job                                                                                                         |
| -------------------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `Command`, `Response`      | `state_machine.rs`                                       | The log entry types. ~20 variants spanning transaction flow, ledger lifecycle, and metadata.                |
| `NameServiceState`         | `state_machine.rs`                                       | The replicated in-memory state: branch heads, ledger registry, per-branch queues, idempotency cache.        |
| `StateMachineAdapter`      | `state_machine_adapter.rs`                               | openraft's `RaftStateMachine` impl. Applies entries, takes/installs snapshots, resolves waiters.            |
| `LogStore`, `SnapshotStore`| `log_adapter.rs`, `storage/{fs,memory}.rs`               | openraft's `RaftLog`/`RaftSnapshotBuilder`. Local-disk persistence.                                          |
| `HttpRaftNetworkFactory`   | `network.rs`                                             | Inter-node RPC (`/raft/vote`, `/raft/append-entries`, `/raft/install-snapshot`) over HTTP.                  |
| `RaftAdmin` / `/cluster/*` | `admin.rs`                                               | Operator-facing membership endpoints (`initialize`, `add-learner`, `change-membership`, `status`).         |
| Follower-forward middleware| `forward.rs`                                             | Axum middleware that proxies leader-only client requests to the current leader.                            |
| `QueuedTransactor`         | `queued_transactor.rs`                                   | Client-side proposer. Builds envelopes, writes to CAS, proposes `EnqueueCommand`, awaits the typed receipt. |
| `CommitWorker`             | `commit_worker.rs`                                       | Leader-only. Drains per-branch queues, stages work, proposes `ApplyHead`.                                  |
| `EvictionScheduler`        | `eviction_scheduler.rs`                                  | Leader-only. Periodically proposes `EvictIdempotency` to age out the cache.                                |
| `RaftNameService`          | `nameservice.rs`                                         | The replicated `NameService` impl. Reads observe `NameServiceState`; writes propose log entries.            |
| `WaiterMap`                | `waiter.rs`                                              | Per-process oneshot registry keyed by `queue_id`. Bridges propose and apply.                                |
| `StagedReceiptMap`         | `staged_receipt.rs`                                      | Per-process map carrying typed apply receipts (flake counts, tally, conflict resolution) from worker to transactor on the same node. |

Three of these (`CommitWorker`, `EvictionScheduler`, follower-forward middleware) are gated on leadership: the integration's leader watcher spawns / stops them in response to `current_leader()` changes.

## Submission flow in detail

Stages, traced end-to-end:

```
Client → POST /api/transact
   ↓ (any node)
[follower-forward middleware]
   ├─ this node is leader  → next.run() (continue locally)
   └─ this node is follower → HTTP forward to leader's client_addr
        ↓
[QueuedTransactor on leader]
   1. write QueuedRequest envelope to CAS  → envelope_cid
   2. register oneshot waiter on WaiterMap → rx
   3. propose Command::EnqueueCommand { envelope_cid, body_hash, kind, idempotency_key? }
        ↓
[Raft consensus]
   4. leader appends to log, replicates to quorum
   5. on quorum, state machine applies on every node:
        - state.queues[branch].push_back(QueueEntry { queue_id, envelope_cid, ... })
        - leader records waiter assignment
        ↓
[CommitWorker on leader]
   6. polls state.queues[branch].front()
   7. fetches envelope from CAS, stages via `Fluree` API
   8. writes commit blob to CAS → head_cid
   9. stashes AppliedReceipt in StagedReceiptMap[queue_id]
  10. propose Command::ApplyHead { branch, queue_id, head_cid, ... }
        ↓
[Raft consensus]
  11. leader appends, replicates to quorum
  12. on quorum, state machine applies on every node:
        - state.refs[branch].head = head_cid
        - state.queues[branch].pop_front()
        - leader: take StagedReceiptMap[queue_id] → resolve WaiterMap[queue_id] with receipt
        ↓
[QueuedTransactor on leader]
  13. rx returns receipt → return to client
        ↓
[follower-forward middleware (if forwarded)]
  14. relay response verbatim to client
```

Why two separate log entries (`EnqueueCommand` then `ApplyHead`) instead of one combined "apply this transaction"?

- The state machine must apply deterministically given the log entry alone. The output of staging (head CID, flake count, conflict outcome) depends on the leader's local state and CAS writes — neither is in the log. Putting the *result* on the log lets every node apply the same outcome without re-running the stage.
- An idempotent retry that lands while the original is still queued can be deduplicated at `EnqueueCommand` (the state machine sees the matching idempotency key and short-circuits) — without needing to re-stage.
- The queue is the unit of fairness. Multiple writers to the same branch land in a FIFO; the worker drains in order and `ApplyHead` references the `queue_id` it's draining. A racing admin operation that resets the branch head produces a `BranchHeadReset` poison record on the front entry rather than corrupting the queue.

### Receipts: replicated vs. process-local

Apply receipts come in two flavors:

- **`AppliedReceipt::Detailed { tally, conflict_outcome, ... }`** — the typed result of a successful stage, including flake counts, indexing status, and conflict resolution. Carried out-of-band in `StagedReceiptMap` on the leader and signaled to the local waiter. Not in the log.
- **`AppliedReceipt::Minimal { head, t }`** — head identity only. Synthesized on every node when applying `ApplyHead`, used by remote nodes and as a fallback on the leader when the staged receipt is missing (e.g. after a leader transition that stranded the receipt).

The split keeps the log encoding small (the heavy receipt fields don't replicate) while still giving the proposing client the rich result on the happy path. Followers that forwarded the original request see the leader's full response; followers reading the head independently see the minimal version.

## Log entry types

`Command` variants are grouped by purpose:

**Transaction flow:**

- `EnqueueCommand { branch, envelope_cid, body_hash, kind, idempotency_key? }` — append a queue entry.
- `ApplyHead { branch, queue_id, head_cid, ... }` — strict head advance, pop the queue front.
- `PoisonQueueEntry { branch, queue_id, reason }` — abandon a queued entry with a typed reason (`BranchHeadReset`, `BranchDropped`, `Poisoned { ... }`).
- `EvictIdempotency { cutoff_millis }` — age out idempotency cache entries older than the cutoff. Released CAS envelopes fan out per node.

**Ledger lifecycle:**

- `CreateLedger`, `CreateBranch`, `DropBranch`, `PurgeLedger`, `RetractLedger`, `ResetHead` — admin operations that mutate the registry. Each has companion entries to clear matching idempotency entries / queues.

**Metadata and refs:**

- `AdvanceIndexHead`, `RewriteIndexHead` — index pointer updates. `Advance` is strict-monotonic; `Rewrite` allows equal-or-different for admin rebinds.
- `CompareAndSetRef` — generic CAS over a named ref, gated by an expected-prior CID.
- `PushStatus`, `PushConfig`, `PublishGraphSource*` — peer / graph-source state.

The log can carry any of these; the state machine resolves them deterministically against the in-memory `NameServiceState`.

## Snapshot design

openraft snapshots the state machine every N entries (configured via `RaftConfig::snapshot_policy`). The adapter serializes `NameServiceState` via [postcard](https://github.com/jamesmunns/postcard) — chosen for its compact binary representation and stable schema-by-struct discipline — plus `last_applied` and `last_membership`.

Snapshots are stored as:

```
<raft_storage_path>/raft/snapshots/
   current             # plain-text snapshot id
   <id>.meta           # postcard-encoded SnapshotMeta
   <id>.data           # raw NameServiceState bytes
```

Atomic writes throughout: write to a temp file, fsync, rename, fsync the parent directory. A crash mid-snapshot leaves the previous snapshot intact via `current`.

On restart, `StateMachineAdapter::open` restores the latest snapshot first; openraft then replays the log from `last_applied + 1`. Without the restore step, log replay would have to start at index 0, which fails once the log has been compacted past it (i.e. always after the first snapshot).

Snapshot ids are validated against a path-traversal guard before any disk path is constructed: non-empty, ≤ 128 bytes, alphanumeric + `-_.` only, no `..`. This is enforced on receipt of an `install-snapshot` RPC — a malicious or buggy peer cannot push a snapshot id that would escape the snapshots subtree.

## Network transport

Inter-node RPC is plain HTTP over `reqwest`, with:

- `connect_timeout`: 250 ms (default)
- `rpc_timeout` (vote, append): 500 ms (default)
- `snapshot_timeout`: 30 s (default)
- Redirects disabled (SSRF guard against 302s to internal addresses).
- Per-route body size limits: vote 1 MiB, append-entries 64 MiB, install-snapshot 1 GiB.

These are exposed on `NetworkConfig` and can be overridden at integration time, though the server binary doesn't currently expose tuning knobs for them.

Why plain HTTP rather than gRPC or a custom protocol? Two reasons:

- The same axum router that hosts the inter-node port also hosts `/cluster/*` admin, which fits naturally in HTTP. Operators can curl the admin endpoints; tooling can intercept with standard HTTP proxies.
- openraft's `RaftNetworkFactory` trait abstracts the transport. HTTP is the lowest-overhead option that's portable across operator environments. Switching to gRPC later doesn't disturb the state machine.

## Why this design

A few alternatives we considered and rejected:

**Replicate commit blobs through the log.** Simplest design, but the log inflates with the size of every transaction. With a 64 MiB append-entries limit and large commits, the log churns through snapshots aggressively, and follower catch-up bandwidth scales with the total commit history rather than with the head set.

**Stage on every node (deterministic state machine apply).** Would let a follower verify the leader's work, but staging touches non-deterministic state: cache contents, wall-clock timestamps in indexing decisions, CAS write outcomes that depend on backend behavior. Forcing determinism would require reworking the entire commit path. The current design treats the leader as the source of truth for the *result* and replicates the result.

**Use openraft's built-in linearizable read API.** openraft offers a primitive for serving reads through the consensus log, but our reads have stricter latency requirements than that path provides. Reading directly from the locally-applied `NameServiceState` is much faster and is correct because the state machine applies under the same lock that the read path acquires.

**Single combined "apply commit" log entry.** Discussed under [Submission flow](#submission-flow-in-detail). The two-entry design (`EnqueueCommand` → `ApplyHead`) gives idempotent retries cheap deduplication and lets the staging work happen between the two replication rounds.

## See also

- [Raft clusters (replicated writes)](../operations/raft-clusters.md) — operator-facing recipe.
- [`fluree cluster` CLI](../cli/cluster.md) — admin subcommand reference.
- [Nameservice schema v2](nameservice-schema-v2.md) — the underlying nameservice model that the Raft variant replicates.
- [Storage-agnostic commits and sync](storage-agnostic-commits-and-sync.md) — the CAS layer the Raft state machine sits on top of.
