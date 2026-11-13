# Data lifecycle: expiry, history retention and erasure

By default Fluree keeps everything. A delete is a retraction: current queries stop seeing the value, and [time travel](time-travel.md), history queries and the commit log still hold it. That is what makes a ledger auditable, and it is the wrong behavior for some data: a session that should end, a counter nobody will ever read the past of, a secret pasted into the wrong field, a customer who asks to be forgotten.

Four features change how long a value lives. They are opt-in, and a ledger that uses none of them behaves exactly as before.

| Feature | What it does | Use it for |
|---|---|---|
| [Time to live (TTL)](../transactions/ttl.md) | Retracts a value automatically when its lifetime ends | Data with a known lifetime: sessions, leases, caches, offers, readings |
| [`f:noHistory`](../transactions/history-retention.md#fnohistory) | Keeps only a predicate's current values in the index | High-churn values whose past nobody queries: counters, heartbeats, cursors |
| [Targeted removal](../transactions/history-retention.md#targeted-removal) | Stops serving selected occurrences, now and in history | A value that must stop being readable today, with the commit log kept for audit |
| [Permanent erasure](../transactions/permanent-erasure.md) | Replaces the commits that hold a value and deletes the originals | A legal or contractual duty to destroy data; leaked secrets |

## Where a value lives

To pick between them, it helps to know the places a committed value is held:

1. **Current state.** What a query sees at the ledger's head.
2. **Served history.** What a query as of an earlier point, or a history range, can read. It is answered from the index.
3. **The commit log.** The commits themselves and any stored transaction documents. Clones, pulls, archives and audit reads copy it.
4. **Derived copies on this storage.** Older index versions, dictionaries, full-text and vector indexes, reasoning indexes, expiry schedules.
5. **Copies elsewhere.** Backups, clones, exports already made, and the memory of other processes.

Each feature reaches a different depth:

| | Current state | Served history | Commit log | Derived copies here | Copies elsewhere |
|---|---|---|---|---|---|
| Retraction (an ordinary delete) | Removed | Kept | Kept | Kept | Kept |
| TTL | Removed at the deadline | Kept | Kept | Kept | Kept |
| `f:noHistory` | Unchanged | Superseded values leave the index | Kept | History is not built into new index files | Kept |
| Targeted removal | Removed | Refused (`410`) | Kept, for audit readers only | Kept, for audit readers only | Kept |
| Permanent erasure | Removed | Refused (`410`) | Replaced by proved redactions | Purged | Not reached: see [what erasure does not guarantee](../transactions/permanent-erasure.md#what-erasure-does-not-guarantee) |

Read the table as a ladder. TTL automates what a delete does. `f:noHistory` trims what history costs. Targeted removal changes what the ledger will *serve*. Only permanent erasure changes what the ledger *holds*.

## Choosing

**The value has a natural end.** Use TTL. Put a lifetime on the value when you write it, on the whole transaction, or on the predicate in the ledger's config. A background worker retracts it with an ordinary commit, so everything that follows a delete (policy, history, replication) follows an expiry too. TTL is not a privacy tool: the value stays in history.

**The predicate changes constantly and its past is noise.** Use `f:noHistory`. A view counter updated a thousand times a day adds a thousand superseded values a day to the index, and time travel over it is rarely meaningful. With `f:noHistory` the index keeps the current value only. No read is refused; a read at an earlier point simply sees less. The commit log still has every change, so this is a storage and performance choice, not a deletion.

**A specific value must stop being readable, and you need the audit trail.** Use targeted removal. It retracts the value if it is still current, then refuses any historical read that could return it. The commits are untouched, and reading them raw needs an audit permission from then on. It works on every storage backend, takes effect when the request returns, and needs no prover. It is also the first thing a permanent erasure does.

A removal has a side effect to plan for. The ledger can no longer answer a historical read unless it can tell which predicates the read touches, so reads of the past with a variable predicate, graph crawls, Cypher queries and queries under reasoning are refused at any point a removal covers, for every subject. Reads of the past that name their predicates, and all reads of current state, are unaffected. The [retention cookbook](../guides/cookbook-retention.md#what-still-works-in-history-afterwards) has the full table.

**The data must be destroyed.** Use permanent erasure. Each commit that holds the value is replaced by a redacted copy, with a zero-knowledge proof that the copy is the original less exactly the erased operations. Nothing reads the original again, and a purge deletes it and every derived copy from the node's storage. The rest of the ledger, its commit IDs and its history are unchanged, so the chain still verifies. Erasure needs file storage and a [prover](../operations/erasure-prover.md), and takes minutes to hours.

They combine:

- A predicate entry can carry both `f:ttl` and `f:noHistory`: values expire, and once indexed their history goes too.
- A targeted removal can be followed by an erasure of the same occurrences later, for example once a retention period for audit has passed.
- TTL and erasure are independent. An expired value is still in the commit log; erase it if it must be destroyed.

## What each one costs

| | Who triggers it | Takes effect | Undone by | Needs |
|---|---|---|---|---|
| TTL | The writer, or ledger config | At the deadline, plus up to one retention pass (15 seconds by default) | Asserting the value again (history keeps the expired one) | A running maintenance owner: the server, or a scheduled pass |
| `f:noHistory` | Ledger config | At the retention worker's next pass, as index files are rewritten | Turning the rule off keeps history from then on; covered history stays out | A running maintenance owner |
| Targeted removal | An administrator (`POST /remove`) | When the request returns | Nothing | Admin access |
| Permanent erasure | An administrator (`POST /erase`) | Removal at once; replacement and purge in the background | Nothing | Admin access, file storage, a prover of the server's release |

A ledger that uses none of these pays nothing for them: no expiry check on writes or queries, no extra read on a load.

## Designing for erasure

Erasure is strongest on a ledger that was set up for it. If a ledger may ever hold data you will be asked to destroy, decide these before the data arrives:

- **Keep personal data out of IRIs.** Erasure removes values, not identifiers. Use `ex:user-8f3a…`, not `mailto:alice@example.org`.
- **Sign commits.** A commit's ID is a hash of its original bytes. On an unsigned commit, someone who can rebuild every other byte can test guesses at an erased value against that hash. A [signature](../security/commit-signing.md) puts the bytes out of reach.
- **Encrypt at rest.** A purge unlinks files; the disk may keep the blocks. With [storage encryption](../security/encryption.md) those blocks are unreadable, and so are backups.
- **Use file storage.** Erasure runs on a single node with file storage, or a Raft cluster whose members share it. Memory and S3 storage cannot hold an erasure.
- **Keep commits that hold erasable data under 50,000 operations.** A larger commit cannot be proved. Load data that may need erasing with ordinary transactions, not as one very large commit.
- **Plan for the copies you control.** Backups, clones and exports made before an erasure still hold the value. Decide how those are rotated or re-made.

## Related documentation

- [Time to live (TTL)](../transactions/ttl.md): every way to set a lifetime, renewal, and how expiration runs
- [History retention](../transactions/history-retention.md): `f:noHistory` and targeted removal
- [Permanent erasure](../transactions/permanent-erasure.md): the full reference
- [Cookbook: expiring data and trimming history](../guides/cookbook-retention.md)
- [Cookbook: permanent erasure](../guides/cookbook-erasure.md)
- [Running the erasure prover](../operations/erasure-prover.md)
- [Time travel](time-travel.md): what these features change about reading the past
