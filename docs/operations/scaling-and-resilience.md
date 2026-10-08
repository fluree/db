# Scaling and resilience architecture

Fluree separates database compute from durable data. Immutable commits and index
artifacts live in content-addressed storage, while the nameservice tracks mutable
ledger heads and indexing progress. This separation lets query capacity scale
independently, keeps transaction ordering explicit, and makes ledger history
portable across storage backends.

The practical deployment model is:

- scale reads horizontally with query peers or load-balanced servers,
- run transaction and indexing capacity independently when needed,
- use Raft for highly available writes within a low-latency network, and
- use content-addressed replication for cross-region or cross-cloud recovery.

Fluree does not currently provide active-active multi-cloud writes. Its
multi-cloud resilience model is globally distributable read compute plus
portable, replicable data and controlled disaster recovery.

## Elastic query scaling

Fluree's HTTP request boundary is stateless: a request carries the information
needed to authenticate and execute it, so query traffic can be distributed
across multiple processes behind a load balancer.

The processes themselves deliberately retain performance state. A query server
holds loaded ledger snapshots and memory caches, and can use a local disk
artifact cache. These caches contain immutable objects addressed by content ID
(CID), so they require no invalidation: a cached CID remains valid, and eviction
is always safe. A replacement query process has no unique durable ledger state
to recover, but cold requests pay the cost of loading metadata and fetching
uncached objects.

[Query peers](query-peers.md) provide the explicit read-scaling role. A peer:

- executes queries locally,
- learns nameservice changes from the transaction server's events stream,
- reads from the same storage backend or fetches CID-addressed objects through
  the storage proxy, and
- forwards write and administrative operations to the transaction server.

Peers may therefore run near applications and users without owning write
authority. With block access, they can also read directly from S3 using
short-lived, ledger-scoped credentials. Remote mounts provide a related model:
a consumer executes queries locally while fetching immutable ledger objects
from an origin on demand.

## Independent transaction, query, and indexing capacity

Commits are the durable source of truth. Indexes are derived artifacts that can
be rebuilt from commits, and background indexing can run separately from query
and transaction handling.

This role separation allows operators to:

- add query processes without adding writers,
- disable indexing on query-only processes when a dedicated worker owns it,
- size transaction capacity for commit and policy-evaluation work, and
- select different storage classes for durable commits and rebuildable indexes.

Queries combine the latest persisted index with commits that arrived after that
index, so they remain current while indexing runs asynchronously. Operators
should monitor the gap between `commit_t` and `index_t`: a large gap increases
the amount of catch-up work on the query path.

## Serverless and scale-to-zero

Fluree's embeddable Rust API and object-storage architecture support serverless
deployment for moderate, bounded workloads. Transaction handling, indexing, and
queries can run in separate functions or container workers. Standard S3 is the
recommended durable commit store; index storage can be selected independently
for durability, latency, and cost.

Local temporary storage is important even in a serverless deployment. Sizing a
function's `/tmp` space for the disk artifact cache allows warm invocations to
avoid repeated remote reads. Warm query performance can therefore be largely
independent of the remote index bucket.

This enables scale-to-zero compute and burst concurrency within the provider's
limits, but it does not remove physical limits:

- cold queries must fetch uncached index objects,
- the active working set must fit the function's memory and temporary storage,
- execution time limits bound large transactions and indexing work, and
- large imports and multi-gigabyte restores belong on long-lived workers.

See [Serverless storage choices](serverless-storage.md) for storage selection,
benchmark ranges, and tuning guidance.

## Highly available transactions with Raft

Raft mode provides a replicated transaction service. Clients may send traffic
to any cluster node; reads are served locally, while writes received by a
follower are forwarded to the elected leader.

The Raft log replicates coordination state rather than full database payloads:
branch heads, ledger lifecycle, transaction queues, index pointers, and
idempotency records. Commit envelopes, commit blobs, and index artifacts remain
in shared content-addressed storage reachable by every node. This keeps the
consensus log narrow while preserving ordered writes.

Recommended voter counts are:

- three voters to tolerate one failed voter, or
- five voters to tolerate two failed voters and span availability zones.

Quorum loss stops writes rather than electing a leader from a minority, avoiding
split brain. Surviving nodes can continue serving their last-applied read state,
which may become stale until quorum is restored. Raft storage is durable and
local to each node; the shared ledger object store is a separate failure domain.

Raft is intended for stable identities and a low-latency, private network. Its
inter-node RPC relies on network trust and its default timeouts are tuned for a
LAN or VPC. Stretching one voter group across distant regions or clouds is not a
documented production topology.

See [Raft clusters](raft-clusters.md) for bootstrap, membership, failure
handling, security boundaries, and rolling upgrades.

## Multi-cloud resilience

Content IDs identify immutable Fluree artifacts independently of their physical
location. Commits can move between filesystem, S3, IPFS, and other storage
implementations without rewriting commit history. Fluree's pack protocol
transfers missing commits and optional index artifacts as a verified stream;
clone and pull use this capability for eager replication. Remote mounts provide
the lazy, demand-driven counterpart.

A practical multi-cloud topology is:

1. Run a three- or five-node Raft transaction cluster across availability zones
   in the active region.
2. Store canonical commits in highly durable object storage accessible to all
   transaction nodes.
3. Place query peers near users, using shared storage, direct S3 access, proxy
   block reads, or replicated local storage.
4. Replicate immutable commits to a secondary cloud or region. Copy indexes for
   faster recovery, or rebuild them from commits after failover.
5. Back up the authoritative nameservice or Raft state separately and use a
   tested, controlled promotion procedure.

This model offers geographic read distribution, storage portability, and
cross-cloud recoverability without requiring a wide-area consensus round trip
for every write.

### Current boundary

Fluree does not currently implement:

- simultaneous active write leaders in multiple clouds,
- automatic promotion of a secondary cloud after total primary-region loss, or
- a production-tested, WAN-stretched Raft quorum.

Object-store replication alone is not active-active database replication. The
authoritative ledger heads and transaction ordering state must also be recovered
or promoted without allowing two independent writers.

## Failure domains

Plan and monitor these layers independently:

- **Query process:** replaceable; loses warm caches but no unique ledger
  history.
- **Raft voter:** recoverable from the surviving quorum by rejoining and
  receiving the log or a snapshot.
- **Raft quorum:** required for writes; minority recovery is intentionally not
  automatic.
- **Shared content store:** required for commit and index objects; protect it
  with the storage provider's durability, versioning, and replication features.
- **Nameservice or Raft state:** contains authoritative heads and must be part of
  the backup and disaster-recovery plan.
- **Index storage:** performance-critical but rebuildable from canonical
  commits.

## Related guides

- [Query peers and replication](query-peers.md)
- [Serverless storage choices](serverless-storage.md)
- [Raft clusters (replicated transaction servers)](raft-clusters.md)
- [Storage modes](storage.md)
- [Pack format: archive and restore](pack-archive-restore.md)
- [Sharing data with downstream consumers](../guides/sharing-data.md)
- [Remote mounts and serving tiers](../design/remote-mounts.md)
- [Storage-agnostic commits and sync](../design/storage-agnostic-commits-and-sync.md)
